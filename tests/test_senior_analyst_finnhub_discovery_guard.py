from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.services.deterministic_provider_runtime_service import (
    DeterministicProviderRuntimeService,
)
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
)
from app.services.senior_analyst_projection_v1 import DATASET_POLICIES


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)


class _ForbiddenFinnhub:
    def __init__(self) -> None:
        self.earnings_calls = 0
        self.news_calls = 0

    async def earnings_calendar(self, **_kwargs):
        self.earnings_calls += 1
        raise AssertionError("unaccounted Finnhub earnings call")

    async def company_news(self, *_args, **_kwargs):
        self.news_calls += 1
        raise AssertionError("unaccounted Finnhub news call")


class _ForbiddenOfficialProvider:
    def __init__(self, provider_id: str) -> None:
        self.provider_id = provider_id
        self.calls = 0

    async def fetch_safe(self, **_kwargs):
        self.calls += 1
        raise AssertionError(
            f"unaccounted {self.provider_id} retry"
        )

    async def fetch(self, **_kwargs):
        self.calls += 1
        raise AssertionError(
            f"unaccounted {self.provider_id} retry"
        )


def _settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "finnhub-guard.sqlite",
        finnhub_enabled=True,
        finnhub_api_key="configured-test-key",
        deterministic_earnings_intelligence_enabled=True,
        tradier_enabled=False,
        census_enabled=False,
        enable_scheduler=False,
        ai_worker_enabled=False,
        enable_ai_researcher=False,
    )


def _runtime(tmp_path):
    settings = _settings(tmp_path)
    finnhub = _ForbiddenFinnhub()
    runtime = DeterministicProviderRuntimeService(
        settings,
        providers={"finnhub": finnhub},
        cache=ProviderCacheRepository(settings.database_path),
        clock=lambda: NOW,
    )
    return runtime, finnhub


@pytest.mark.asyncio
async def test_senior_consumer_mode_never_calls_undelivered_finnhub(
    tmp_path,
) -> None:
    runtime, finnhub = _runtime(tmp_path)

    output = await runtime.enrich_market_context(
        {},
        refresh="force",
        include_candidate_discovery=False,
    )

    assert finnhub.earnings_calls == 0
    assert finnhub.news_calls == 0
    assert output["earnings_intelligence"]["warnings"] == [
        "candidate_discovery_not_delivered_for_audience"
    ]


@pytest.mark.asyncio
async def test_request_accounting_guard_skips_finnhub_and_missing_row_fails(
    tmp_path,
) -> None:
    runtime, finnhub = _runtime(tmp_path)
    policies = tuple(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id
        in {
            "market_internals",
            "options_positioning",
            "current_news",
        }
    )
    collector = RequestProviderAccountingCollector(
        request_id="finnhub-accounting-guard",
        correlation_id="finnhub-accounting-guard",
        request_started_at=NOW - timedelta(seconds=1),
        policies=policies,
        clock=lambda: NOW,
    )

    output = await runtime.enrich_market_context(
        {},
        refresh="force",
        accounting_collector=collector,
    )

    assert finnhub.earnings_calls == 0
    assert finnhub.news_calls == 0
    assert output["earnings_intelligence"]["warnings"] == [
        "candidate_discovery_has_no_request_accounting_policy"
    ]
    manifest = collector.manifest(
        request_completed_at=NOW + timedelta(seconds=1)
    )
    assert manifest["evidence_status"] == "INCOMPLETE"
    missing = next(
        row
        for row in manifest["datasets"]
        if row["dataset_id"] == "current_news"
    )
    assert missing["evidence_status"] == "INCOMPLETE"


@pytest.mark.asyncio
async def test_request_accounting_never_retries_macro_providers(
    tmp_path,
) -> None:
    settings = _settings(tmp_path)
    forbidden = {
        provider_id: _ForbiddenOfficialProvider(
            provider_id.upper()
        )
        for provider_id in ("fred", "bls", "bea", "census")
    }
    runtime = DeterministicProviderRuntimeService(
        settings,
        providers=forbidden,
        cache=ProviderCacheRepository(settings.database_path),
        clock=lambda: NOW,
    )
    policies = tuple(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id
        in {"market_internals", "options_positioning"}
    )
    collector = RequestProviderAccountingCollector(
        request_id="macro-retry-guard",
        correlation_id="macro-retry-guard",
        request_started_at=NOW - timedelta(seconds=1),
        policies=policies,
        clock=lambda: NOW,
    )
    contract = {
        "event_calendar": {
            "events": [
                {
                    "provider": "CENSUS",
                    "dataset": "NEWHOME",
                    "reference_period": "2026-06",
                }
            ]
        }
    }

    output = await runtime.enrich_market_context(
        contract,
        refresh="force",
        accounting_collector=collector,
    )

    assert {
        provider_id: provider.calls
        for provider_id, provider in forbidden.items()
    } == {
        "fred": 0,
        "bls": 0,
        "bea": 0,
        "census": 0,
    }
    assert output["macro_actuals"]["items"] == []
