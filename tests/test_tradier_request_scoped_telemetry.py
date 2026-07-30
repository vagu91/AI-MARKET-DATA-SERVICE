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
