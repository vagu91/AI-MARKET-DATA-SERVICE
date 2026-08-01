from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType

import pytest

import app.services.provider_capability_registry as provider_registry
from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.services.deterministic_provider_runtime_service import (
    DeterministicProviderRuntimeService,
)


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)


def _settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "deterministic-registry.sqlite",
        tradier_enabled=False,
        census_enabled=False,
        enable_scheduler=False,
        ai_worker_enabled=False,
        enable_ai_researcher=False,
    )


def _runtime(tmp_path) -> DeterministicProviderRuntimeService:
    settings = _settings(tmp_path)
    return DeterministicProviderRuntimeService(
        settings,
        providers={},
        cache=ProviderCacheRepository(settings.database_path),
        clock=lambda: NOW,
    )


def test_persistence_uses_live_registry_fact_type_and_sla(
    tmp_path,
    monkeypatch,
) -> None:
    policies = tuple(
        replace(policy, sla_seconds=60 * 60)
        if policy.dataset_id == "market_internals"
        else policy
        for policy in provider_registry.DATASET_SOURCE_POLICIES
    )
    queries = dict(
        provider_registry
        .MARKET_FACT_REPOSITORY_DATASET_QUERIES
    )
    queries["market_internals"] = replace(
        queries["market_internals"],
        fact_types=("controlled_market_internals",),
    )
    monkeypatch.setattr(
        provider_registry,
        "DATASET_SOURCE_POLICIES",
        policies,
    )
    monkeypatch.setattr(
        provider_registry,
        "MARKET_FACT_REPOSITORY_DATASET_QUERIES",
        MappingProxyType(queries),
    )
    runtime = _runtime(tmp_path)

    runtime._persist_deterministic_section(
        "market_internals",
        {
            "status": "AVAILABLE",
            "provider": "TRADIER",
            "data_as_of": NOW.isoformat(),
        },
    )

    rows = runtime.facts.get_valid_facts_by_type(
        "controlled_market_internals",
        allow_stale=True,
    )
    assert len(rows) == 1
    assert rows[0]["valid_until"] == (
        NOW + timedelta(hours=1)
    ).isoformat()
    assert (
        runtime.facts.get_valid_facts_by_type(
            "deterministic_market_internals",
            allow_stale=True,
        )
        == []
    )


@pytest.mark.asyncio
async def test_provider_policy_mismatch_fails_before_runtime_call(
    tmp_path,
    monkeypatch,
) -> None:
    policies = tuple(
        replace(policy, primary_provider="CBOE")
        if policy.dataset_id == "market_internals"
        else policy
        for policy in provider_registry.DATASET_SOURCE_POLICIES
    )
    monkeypatch.setattr(
        provider_registry,
        "DATASET_SOURCE_POLICIES",
        policies,
    )
    runtime = _runtime(tmp_path)

    with pytest.raises(
        RuntimeError,
        match=(
            "DETERMINISTIC_RUNTIME_CAPABILITY_MISSING:"
            "market_internals:CBOE"
        ),
    ):
        await runtime.enrich_market_context(
            {},
            refresh="force",
        )
