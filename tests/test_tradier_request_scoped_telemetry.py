from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.tradier import TradierProvider
from app.services.deterministic_provider_runtime_service import (
    DeterministicProviderRuntimeService,
)
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
)
from app.services.senior_analyst_projection_v1 import DATASET_POLICIES


NOW = datetime(2026, 7, 30, 15, tzinfo=UTC)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "tradier-scope.sqlite",
        source_policy_path="config/source_policy.json",
        tradier_enabled=True,
        tradier_market_data_enabled=True,
        tradier_production_token="test-only-token",
        tradier_cross_asset_symbols="QQQ",
        enable_scheduler=False,
        ai_worker_enabled=False,
        enable_ai_researcher=False,
    )


def _transport(
    request: httpx.Request,
) -> httpx.Response:
    if request.url.path.endswith("/markets/options/expirations"):
        return httpx.Response(
            200,
            json={
                "expirations": {
                    "date": ["2026-07-31", "2026-08-07"]
                }
            },
            request=request,
        )
    return httpx.Response(
        200,
        json={"quotes": {"quote": []}},
        request=request,
    )


@pytest.mark.asyncio
async def test_force_bypasses_tradier_parametric_cache(
    tmp_path: Path,
) -> None:
    cfg = _settings(tmp_path)
    observed_requests: list[str] = []

    def transport(request: httpx.Request) -> httpx.Response:
        observed_requests.append(str(request.url))
        return _transport(request)

    provider = TradierProvider(
        ProviderCacheRepository(cfg.database_path),
        cfg,
        transport=httpx.MockTransport(transport),
        clock=lambda: NOW,
    )

    await provider.quotes(["QQQ"])
    await provider.quotes(["QQQ"])
    token = provider.begin_request_telemetry("force-request")
    await provider.quotes(["QQQ"], force=True)
    evidence = provider.end_request_telemetry(token)

    assert len(observed_requests) == 2
    force_detail = next(iter(evidence.values()))
    assert force_detail["cache_status"] == "REFRESHED_NO_DATA"
    assert force_detail["actual_provider_requests"] == 1
    assert force_detail["cache_hit"] == 0


def _collector(
    request_id: str,
) -> RequestProviderAccountingCollector:
    policies = [
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id
        in {"market_internals", "options_positioning"}
    ]
    return RequestProviderAccountingCollector(
        request_id=request_id,
        correlation_id=request_id,
        request_started_at=NOW - timedelta(seconds=1),
        policies=policies,
        clock=lambda: NOW,
    )


@pytest.mark.asyncio
async def test_tradier_scope_does_not_inherit_prior_endpoint_attempts(
    tmp_path: Path,
) -> None:
    cfg = _settings(tmp_path)
    provider = TradierProvider(
        ProviderCacheRepository(cfg.database_path),
        cfg,
        transport=httpx.MockTransport(_transport),
        clock=lambda: NOW,
    )

    first_token = provider.begin_request_telemetry("request-one")
    await provider.expirations("QQQ")
    first = provider.end_request_telemetry(first_token)

    second_token = provider.begin_request_telemetry("request-two")
    await provider.quotes(["QQQ"])
    second = provider.end_request_telemetry(second_token)

    assert {
        detail["endpoint_category"]
        for detail in first.values()
    } == {"option_expirations"}
    assert {
        detail["correlation_id"]
        for detail in first.values()
    } == {"request-one"}
    assert {
        detail["endpoint_category"]
        for detail in second.values()
    } == {"quotes"}
    assert {
        detail["correlation_id"]
        for detail in second.values()
    } == {"request-two"}
    assert not hasattr(provider, "last_telemetry")


@pytest.mark.asyncio
async def test_runtime_accounting_ignores_previous_tradier_attempts(
    tmp_path: Path,
) -> None:
    cfg = _settings(tmp_path)
    cache = ProviderCacheRepository(cfg.database_path)
    provider = TradierProvider(
        cache,
        cfg,
        transport=httpx.MockTransport(_transport),
        clock=lambda: NOW,
    )
    runtime = DeterministicProviderRuntimeService(
        cfg,
        providers={"tradier": provider},
        cache=cache,
        clock=lambda: NOW,
    )

    first = await runtime.enrich_market_context(
        {},
        refresh="force",
    )
    assert first["deterministic_domains"]["telemetry"][
        "actual_provider_requests"
    ] == 1

    cfg.tradier_enabled = False
    collector = _collector("request-two")
    second = await runtime.enrich_market_context(
        {},
        refresh="force",
        accounting_collector=collector,
    )

    telemetry = second["deterministic_domains"]["telemetry"]
    assert telemetry["tradier"] == {}
    assert telemetry["actual_provider_requests"] == 0
    rows = collector.manifest()["datasets"]
    assert {row["request_id"] for row in rows} == {"request-two"}
    assert all(
        row["primary_provider"]["called"] is False
        and row["primary_provider"]["attempts"] == 0
        and row["primary_provider"]["execution_origin"]
        == "OBSERVED_SKIP"
        for row in rows
    )


@pytest.mark.asyncio
async def test_runtime_accounting_propagates_canonical_lifecycle(
    tmp_path: Path,
) -> None:
    cfg = _settings(tmp_path)
    cache = ProviderCacheRepository(cfg.database_path)
    runtime = DeterministicProviderRuntimeService(
        cfg,
        providers={},
        cache=cache,
        clock=lambda: NOW,
    )
    for dataset_id in ("market_internals", "options_positioning"):
        runtime._persist_deterministic_section(
            dataset_id,
            {
                "status": "AVAILABLE",
                "provider": "TRADIER",
                "data_as_of": NOW.isoformat(),
                "warnings": [],
            },
        )
    collector = _collector("canonical-request")

    await runtime.enrich_market_context(
        {},
        refresh="force",
        accounting_collector=collector,
    )

    manifest = collector.manifest()
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    assert {
        row["database_lifecycle_status"]
        for row in manifest["datasets"]
    } == {"ACTIVE"}


@pytest.mark.asyncio
async def test_db_valid_dataset_is_not_included_in_other_tradier_acquisition(
    tmp_path: Path,
) -> None:
    cfg = _settings(tmp_path)
    cfg.deterministic_cross_asset_context_enabled = False
    quote_symbol_requests: list[str] = []

    def transport(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/markets/quotes"):
            symbols = str(request.url.params["symbols"])
            quote_symbol_requests.append(symbols)
            return httpx.Response(
                200,
                json={
                    "quotes": {
                        "quote": {
                            "symbol": "QQQ",
                            "last": 500.0,
                            "bid": 499.0,
                            "ask": 501.0,
                            "trade_date": 1785500000000,
                        }
                    }
                },
                request=request,
            )
        return _transport(request)

    cache = ProviderCacheRepository(cfg.database_path)
    provider = TradierProvider(
        cache,
        cfg,
        transport=httpx.MockTransport(transport),
        clock=lambda: NOW,
    )
    runtime = DeterministicProviderRuntimeService(
        cfg,
        providers={"tradier": provider},
        cache=cache,
        clock=lambda: NOW,
    )
    runtime._persist_deterministic_section(
        "market_internals",
        {
            "status": "AVAILABLE",
            "provider": "TRADIER",
            "data_as_of": NOW.isoformat(),
            "breadth": {"advancers": 1, "decliners": 0},
            "warnings": [],
        },
    )
    collector = _collector("separate-tradier-acquisitions")

    await runtime.enrich_market_context(
        {
            "nasdaq_context": {
                "qqq_holdings": {
                    "holdings": [
                        {"symbol": "AAPL", "weight": 1.0}
                    ]
                }
            }
        },
        refresh="force",
        accounting_collector=collector,
    )

    assert quote_symbol_requests == ["QQQ"]
    rows = {
        row["dataset_id"]: row
        for row in collector.manifest()["datasets"]
    }
    assert rows["market_internals"]["database_freshness_evaluation"] == (
        "VALID"
    )
    assert rows["market_internals"]["primary_provider"] == {
        "provider": "TRADIER",
        "called": False,
        "attempts": 0,
        "result": "NOT_CALLED",
        "not_called_reason": "VALID_DATABASE_RECORD_SELECTED",
        "execution_origin": "CACHE_DECISION",
    }
    assert rows["market_internals"]["shared_acquisition_dataset_ids"] == [
        "market_internals"
    ]
    assert rows["options_positioning"]["primary_provider"]["called"] is True
    assert rows["options_positioning"]["primary_provider"]["attempts"] == 4


@pytest.mark.asyncio
async def test_failed_tradier_transport_is_not_reported_as_not_called(
    tmp_path: Path,
) -> None:
    cfg = _settings(tmp_path)
    cfg.deterministic_cross_asset_context_enabled = False
    cfg.tradier_retry_attempts = 1
    observed_requests: list[str] = []

    def transport(request: httpx.Request) -> httpx.Response:
        observed_requests.append(str(request.url))
        return httpx.Response(
            500,
            json={"error": "controlled failure"},
            request=request,
        )

    cache = ProviderCacheRepository(cfg.database_path)
    provider = TradierProvider(
        cache,
        cfg,
        transport=httpx.MockTransport(transport),
        clock=lambda: NOW,
    )
    runtime = DeterministicProviderRuntimeService(
        cfg,
        providers={"tradier": provider},
        cache=cache,
        clock=lambda: NOW,
    )
    collector = _collector("failed-tradier-transport")

    await runtime.enrich_market_context(
        {
            "nasdaq_context": {
                "qqq_holdings": {
                    "holdings": [
                        {"symbol": "AAPL", "weight": 1.0}
                    ]
                }
            }
        },
        refresh="force",
        accounting_collector=collector,
    )

    assert len(observed_requests) == 2
    manifest = collector.manifest()
    assert manifest["evidence_status"] == "INCOMPLETE"
    for row in manifest["datasets"]:
        assert row["evidence_status"] == "INCOMPLETE"
        assert row["primary_provider"]["called"] is True
        assert row["primary_provider"]["attempts"] == 1
        assert row["primary_provider"]["result"] == (
            "ATTEMPT_EVIDENCE_INCOMPLETE"
        )
