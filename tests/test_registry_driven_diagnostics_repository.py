from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType

import pytest

import app.services.provider_capability_registry as provider_registry
from app.core.config import Settings
from app.services.data_freshness_service import DataFreshnessService
from app.services.diagnostics_service import (
    DiagnosticsService,
    _dataset_sla,
    _macro_repository_queries,
    _macro_requested_series,
    _nasdaq_repository_queries,
    _news_accounting_provider_names,
)


NOW = datetime(2026, 7, 31, 10, 0, tzinfo=UTC)


class _ObservedFacts:
    def __init__(self) -> None:
        self.fact_types: list[str] = []

    def get_valid_facts_by_type(
        self,
        fact_type: str,
        *,
        allow_stale: bool = False,
    ) -> list[dict[str, object]]:
        assert allow_stale is True
        self.fact_types.append(fact_type)
        return []


@pytest.mark.asyncio
async def test_macro_lookup_series_and_sla_follow_central_mutations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_query = (
        provider_registry.MARKET_FACT_REPOSITORY_DATASET_QUERIES[
            "treasury_rates"
        ]
    )
    mutated_query = replace(
        original_query,
        fact_types=("mutated_official_macro",),
        series_ids=("DGS30",),
    )
    monkeypatch.setattr(
        provider_registry,
        "MARKET_FACT_REPOSITORY_DATASET_QUERIES",
        MappingProxyType(
            {
                **provider_registry.MARKET_FACT_REPOSITORY_DATASET_QUERIES,
                "treasury_rates": mutated_query,
            }
        ),
    )
    original_policy = provider_registry.dataset_policy_by_id(
        "treasury_rates"
    )
    mutated_policy = replace(original_policy, sla_seconds=1234)
    monkeypatch.setattr(
        provider_registry,
        "DATASET_SOURCE_POLICIES",
        tuple(
            mutated_policy
            if policy.dataset_id == "treasury_rates"
            else policy
            for policy in provider_registry.DATASET_SOURCE_POLICIES
        ),
    )

    queries = _macro_repository_queries()
    assert queries["treasury_rates"] is mutated_query
    assert _dataset_sla("treasury_rates") == timedelta(
        seconds=1234
    )
    assert _macro_requested_series(
        {"treasury_rates"},
        queries=queries,
    ) == {"FRED": ("DGS30",)}

    service = object.__new__(DiagnosticsService)
    service.facts = _ObservedFacts()
    service.freshness = DataFreshnessService(
        Settings(_env_file=None),
        clock=lambda: NOW,
    )

    await service._macro_db_first(fetch_missing=False)

    assert "mutated_official_macro" in service.facts.fact_types


def test_nasdaq_queries_and_earnings_preloads_cover_every_registry_fact(
) -> None:
    queries = _nasdaq_repository_queries()
    assert "nasdaq_100_constituents" in queries[
        "nasdaq_100"
    ].fact_types
    assert "fmp_earnings_calendar" in queries[
        "earnings"
    ].fact_types

    service = object.__new__(DiagnosticsService)
    service.freshness = DataFreshnessService(
        Settings(_env_file=None),
        clock=lambda: NOW,
    )
    fmp_fact = {
        "fact_key": "earnings:fmp:2026-08-01",
        "fact_type": "fmp_earnings_calendar",
        "source": "Financial Modeling Prep Earnings Calendar",
        "retrieved_at": (NOW - timedelta(minutes=5)).isoformat(),
        "release_at": NOW.isoformat(),
        "valid_until": (NOW + timedelta(days=1)).isoformat(),
        "next_refresh_at": (NOW + timedelta(hours=1)).isoformat(),
        "lifecycle_status": "ACTIVE",
        "raw_payload": {
            "status": "found",
            "events": [{"symbol": "NVDA", "date": "2026-08-01"}],
        },
    }

    preloaded = service._earnings_preloaded_blocks(
        {
            fact_type: (
                [fmp_fact]
                if fact_type == "fmp_earnings_calendar"
                else []
            )
            for fact_type in queries["earnings"].fact_types
        }
    )

    assert set(preloaded) == {"fmp_earnings"}
    assert preloaded["fmp_earnings"]["cache_used"] is True
    assert preloaded["fmp_earnings"]["database_lookup"][
        "freshness"
    ] == "VALID"
    assert preloaded["fmp_earnings"]["events"] == [
        {"symbol": "NVDA", "date": "2026-08-01"}
    ]


def test_news_accounting_display_names_follow_runtime_policy_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = provider_registry.dataset_policy_by_id("current_news")
    original_order = (
        original.primary_provider,
        *original.fallback_providers,
    )
    reordered = replace(
        original,
        primary_provider=original_order[-1],
        fallback_providers=(
            original_order[0],
            *original_order[1:-1],
        ),
    )
    monkeypatch.setattr(
        provider_registry,
        "DATASET_SOURCE_POLICIES",
        tuple(
            reordered
            if policy.dataset_id == "current_news"
            else policy
            for policy in provider_registry.DATASET_SOURCE_POLICIES
        ),
    )

    names = _news_accounting_provider_names()

    assert tuple(names) == (
        reordered.primary_provider,
        *reordered.fallback_providers,
    )
    assert names["GOOGLE_NEWS_RSS"] == "Google News RSS"
