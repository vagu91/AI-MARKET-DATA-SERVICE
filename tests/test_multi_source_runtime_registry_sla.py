from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

import app.services.multi_source_runtime_service as runtime_module
import app.services.provider_capability_registry as registry_module
from app.core.config import Settings


def _settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "runtime-sla.sqlite",
        enable_scheduler=False,
        ai_worker_enabled=False,
        enable_ai_researcher=False,
    )


def _set_dataset_sla(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dataset_id: str,
    sla_seconds: int,
) -> None:
    policies = tuple(
        replace(policy, sla_seconds=sla_seconds) if policy.dataset_id == dataset_id else policy
        for policy in registry_module.DATASET_SOURCE_POLICIES
    )
    monkeypatch.setattr(
        registry_module,
        "DATASET_SOURCE_POLICIES",
        policies,
    )


def _as_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_governed_runtime_cache_ages_resolve_from_dataset_registry() -> None:
    for name, dataset_id in runtime_module._POLICY_RUNTIME_DATASET_IDS.items():
        policy = registry_module.dataset_policy_by_id(dataset_id)

        assert runtime_module._provider_cache_max_age(name) == timedelta(seconds=policy.sla_seconds)

    assert set(runtime_module._LOCAL_PROVIDER_CACHE_MAX_AGE) == {
        "aaii_sentiment",
        "macromicro_aaii_crosscheck",
        "polymarket_prediction_markets",
        "quikstrike_review",
    }


@pytest.mark.asyncio
async def test_run_provider_cache_lookup_observes_mutated_dataset_sla(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    now = datetime.now(UTC).replace(microsecond=0)
    data_as_of = now - timedelta(hours=3)
    future = now + timedelta(days=1)
    service._save_fact(
        "investing_fed_rate_monitor",
        runtime_module.FACT_TYPES["investing_fed_rate_monitor"],
        {
            "status": "found",
            "source": "Investing.com Fed Rate Monitor",
            "retrieved_at": now.isoformat(),
            "data_as_of": data_as_of.isoformat(),
            "valid_until": future.isoformat(),
            "next_refresh_at": future.isoformat(),
            "meetings": [
                {
                    "meeting_date": "2026-09-16",
                    "updated_at": data_as_of.isoformat(),
                }
            ],
            "warnings": [],
            "errors": [],
        },
        source="Investing.com Fed Rate Monitor",
    )
    assert registry_module.dataset_policy_by_id("fomc_expectations").sla_seconds == 2 * 60 * 60
    _set_dataset_sla(
        monkeypatch,
        dataset_id="fomc_expectations",
        sla_seconds=4 * 60 * 60,
    )
    calls = 0

    async def forbidden_fetch() -> dict:
        nonlocal calls
        calls += 1
        raise AssertionError("mutated valid SLA must avoid provider")

    result = await service._run_provider(
        "investing_fed_rate_monitor",
        runtime_module.FACT_TYPES["investing_fed_rate_monitor"],
        forbidden_fetch,
        item_count=lambda payload: len(payload.get("meetings") or []),
        enabled=True,
        source="Investing.com Fed Rate Monitor",
        refresh="force",
    )

    assert calls == 0
    assert result["cache_used"] is True
    assert result["provider_calls"] == 0
    assert result["database_lookup"]["freshness"] == "VALID"


@pytest.mark.asyncio
async def test_run_provider_default_valid_until_observes_mutated_dataset_sla(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    _set_dataset_sla(
        monkeypatch,
        dataset_id="earnings",
        sla_seconds=137,
    )

    async def observed_fetch() -> dict:
        return {
            "status": "found",
            "source": "Nasdaq Earnings Calendar",
            "events": [{"symbol": "NVDA"}],
            "warnings": [],
            "errors": [],
        }

    before = datetime.now(UTC).replace(microsecond=0)
    result = await service._run_provider(
        "nasdaq_earnings",
        runtime_module.FACT_TYPES["nasdaq_earnings"],
        observed_fetch,
        item_count=lambda payload: len(payload.get("events") or []),
        enabled=True,
        source="Nasdaq Earnings Calendar",
        refresh="force",
    )
    after = datetime.now(UTC).replace(microsecond=0)

    valid_until = _as_datetime(result["valid_until"])
    assert before + timedelta(seconds=137) <= valid_until
    assert valid_until <= after + timedelta(seconds=137)
    persisted = service.facts.get_fact("multi_source:nasdaq_earnings:earnings_event:latest")
    assert persisted is not None
    assert _as_datetime(persisted["valid_until"]) == valid_until
    assert _as_datetime(persisted["raw_payload"]["content_valid_until"]) == (valid_until)


@pytest.mark.asyncio
async def test_unknown_runtime_dataset_mapping_fails_before_fetch(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    mapping = dict(runtime_module._POLICY_RUNTIME_DATASET_IDS)
    mapping["investing_fed_rate_monitor"] = "unregistered_dataset"
    monkeypatch.setattr(
        runtime_module,
        "_POLICY_RUNTIME_DATASET_IDS",
        mapping,
    )
    calls = 0

    async def forbidden_fetch() -> dict:
        nonlocal calls
        calls += 1
        return {"status": "found", "meetings": []}

    with pytest.raises(
        RuntimeError,
        match="RUNTIME_CACHE_POLICY_REGISTRY_MISSING",
    ):
        await service._run_provider(
            "investing_fed_rate_monitor",
            runtime_module.FACT_TYPES["investing_fed_rate_monitor"],
            forbidden_fetch,
            item_count=lambda payload: len(payload.get("meetings") or []),
            enabled=True,
            source="Investing.com Fed Rate Monitor",
            refresh="force",
        )

    assert calls == 0


@pytest.mark.asyncio
async def test_earnings_db_first_selects_valid_fmp_canonical_record_before_primary(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    now = datetime.now(UTC).replace(microsecond=0)
    valid_until = now + timedelta(days=1)
    service._save_fact(
        "fmp_earnings",
        runtime_module.FACT_TYPES["fmp_earnings"],
        {
            "status": "found",
            "source": "Financial Modeling Prep Earnings Calendar",
            "retrieved_at": now.isoformat(),
            "data_as_of": now.isoformat(),
            "valid_until": valid_until.isoformat(),
            "refresh_due_at": valid_until.isoformat(),
            "events": [
                {
                    "symbol": "AAPL",
                    "event_date": (now.date() + timedelta(days=2)).isoformat(),
                    "source": "Financial Modeling Prep Earnings Calendar",
                    "acquisition_provider": "FMP_EARNINGS_CALENDAR",
                }
            ],
            "warnings": [],
            "errors": [],
        },
        source="Financial Modeling Prep Earnings Calendar",
    )
    executions = 0

    async def forbidden_provider_execution(*_args, **_kwargs) -> dict:
        nonlocal executions
        executions += 1
        raise AssertionError("valid canonical fallback must avoid every provider")

    monkeypatch.setattr(
        service,
        "_run_provider",
        forbidden_provider_execution,
    )

    blocks = await service._earnings_chain(refresh="force")

    assert executions == 0
    assert sum(int(block["provider_calls"]) for block in blocks.values()) == 0
    selected = blocks["nasdaq_earnings"]
    assert selected["provider_id"] == "NASDAQ"
    assert selected["canonical_record_provider_id"] == "FMP_EARNINGS_CALENDAR"
    assert selected["source"] == "Financial Modeling Prep Earnings Calendar"
    assert selected["events"][0]["acquisition_provider"] == "FMP_EARNINGS_CALENDAR"
    assert selected["cache_used"] is True
    assert selected["database_lookup"]["performed"] is True
    assert selected["database_lookup"]["found"] is True
    assert selected["database_lookup"]["freshness"] == "VALID"
    fallback = blocks["fmp_earnings"]
    assert fallback["provider_id"] == "FMP_EARNINGS_CALENDAR"
    assert fallback["attempted"] is False
    assert fallback["reason"] == "VALID_CANONICAL_EARNINGS_RECORD_SELECTED"


@pytest.mark.asyncio
async def test_earnings_revalidates_preloaded_cache_at_selection_time(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    preflight_at = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)
    expired_at_selection = preflight_at + timedelta(minutes=2)
    service.freshness = runtime_module.DataFreshnessService(
        service.settings,
        clock=lambda: expired_at_selection,
    )
    deadline = preflight_at + timedelta(minutes=1)
    stale_preload = {
        "status": "found",
        "source": "Financial Modeling Prep Earnings Calendar",
        "events": [{"symbol": "AAPL"}],
        "cache_used": True,
        "attempted": False,
        "provider_calls": 0,
        "materialized_count": 1,
        "database_lookup": {
            "performed": True,
            "found": True,
            "data_as_of": preflight_at.isoformat(),
            "content_valid_until": deadline.isoformat(),
            "refresh_due_at": deadline.isoformat(),
            "lifecycle_status": "ACTIVE",
            "expired": False,
            "freshness": "VALID",
            "reason_code": "CANONICAL_RECORD_WITHIN_SLA",
        },
    }
    executions: list[str] = []

    async def observed_provider_execution(name, *_args, **_kwargs) -> dict:
        executions.append(name)
        return {
            "status": "not_found",
            "attempted": True,
            "provider_calls": 1,
            "fetched_count": 0,
            "materialized_count": 0,
        }

    monkeypatch.setattr(
        service,
        "_run_provider",
        observed_provider_execution,
    )

    await service._earnings_chain(
        refresh="force",
        preloaded_fallback=stale_preload,
    )

    assert executions == ["nasdaq_earnings", "fmp_earnings"]


def test_old_investing_fed_observation_is_not_materialized_as_current() -> None:
    observed_at = datetime.now(UTC).replace(microsecond=0)
    old_observation = observed_at - timedelta(hours=3)
    result = runtime_module._canonical_runtime_result(
        "investing_fed_rate_monitor",
        {
            "status": "found",
            "retrieved_at": observed_at.isoformat(),
            "valid_until": (
                observed_at + timedelta(minutes=30)
            ).isoformat(),
            "meetings": [
                {
                    "updated_at": old_observation.isoformat(),
                    "meeting_date": "2026-09-16",
                }
            ],
            "current_meeting": {
                "updated_at": old_observation.isoformat(),
            },
            "diagnostics": {},
            "warnings": [],
        },
    )

    assert result["data_as_of"] == old_observation.isoformat()
    assert result["status"] == "not_found"
    assert result["meetings"] == []
    assert result["current_meeting"] is None
    assert result["freshness_state"] == "SLA_EXPIRED"
    assert result["reason_code"] == (
        "CANONICAL_RECORD_OUTSIDE_DATASET_SLA"
    )
    assert (
        result["diagnostics"]["rejected_stale_observation_count"]
        == 1
    )


@pytest.mark.asyncio
async def test_earnings_prevalidates_fallback_leaf_before_primary_execution(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    service.fmp_earnings = service.nasdaq_100
    executions = 0

    async def observed_execution(*_args, **_kwargs) -> dict:
        nonlocal executions
        executions += 1
        return {"status": "not_found"}

    monkeypatch.setattr(service, "_run_provider", observed_execution)

    with pytest.raises(
        RuntimeError,
        match="RUNTIME_ADAPTER_CAPABILITY_MISMATCH:earnings:FMP_EARNINGS_CALENDAR",
    ):
        await service._earnings_chain(refresh="force")

    assert executions == 0


@pytest.mark.asyncio
async def test_nasdaq_constituents_leaf_is_rejected_for_nasdaq_earnings(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    service.nasdaq_earnings = service.nasdaq_100
    executions = 0

    async def observed_execution(*_args, **_kwargs) -> dict:
        nonlocal executions
        executions += 1
        return {"status": "not_found"}

    monkeypatch.setattr(service, "_run_provider", observed_execution)

    with pytest.raises(
        RuntimeError,
        match="RUNTIME_ADAPTER_CAPABILITY_MISMATCH:earnings:NASDAQ:earnings_event",
    ):
        await service._earnings_chain(refresh="force")

    assert executions == 0


@pytest.mark.asyncio
async def test_same_capability_does_not_allow_cross_provider_earnings_mapping(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    assert {
        capability.metric_id
        for provider_id in ("NASDAQ", "FMP_EARNINGS_CALENDAR")
        for capability in registry_module.provider_by_id(provider_id).capabilities
        if capability.dataset_id == "earnings"
    } == {"earnings_event"}
    monkeypatch.setattr(
        runtime_module,
        "_EARNINGS_RUNTIME_BLOCKS",
        {
            "NASDAQ": "fmp_earnings",
            "FMP_EARNINGS_CALENDAR": "nasdaq_earnings",
        },
    )
    executions = 0

    async def observed_execution(*_args, **_kwargs) -> dict:
        nonlocal executions
        executions += 1
        return {"status": "not_found"}

    monkeypatch.setattr(service, "_run_provider", observed_execution)

    with pytest.raises(
        RuntimeError,
        match=("RUNTIME_ADAPTER_CAPABILITY_MISMATCH:earnings:NASDAQ:earnings_event"),
    ):
        await service._earnings_chain(refresh="force")

    assert executions == 0


@pytest.mark.asyncio
async def test_registry_adapter_mutation_cannot_substitute_same_capability_provider(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(_settings(tmp_path))
    fmp_path = registry_module.provider_by_id("FMP_EARNINGS_CALENDAR").adapter_path
    mutated_registry = tuple(
        replace(
            provider,
            capabilities=tuple(
                replace(capability, probe_adapter_path=fmp_path)
                if capability.dataset_id == "earnings"
                else capability
                for capability in provider.capabilities
            ),
        )
        if provider.provider_id == "NASDAQ"
        else provider
        for provider in registry_module.PROVIDER_REGISTRY
    )
    monkeypatch.setattr(
        registry_module,
        "PROVIDER_REGISTRY",
        mutated_registry,
    )
    executions = 0

    async def observed_execution(*_args, **_kwargs) -> dict:
        nonlocal executions
        executions += 1
        return {"status": "not_found"}

    monkeypatch.setattr(service, "_run_provider", observed_execution)

    with pytest.raises(
        RuntimeError,
        match=("RUNTIME_ADAPTER_CAPABILITY_MISMATCH:earnings:NASDAQ:earnings_event"),
    ):
        await service._earnings_chain(refresh="force")

    assert executions == 0
