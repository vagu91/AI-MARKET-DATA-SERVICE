from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import app.services.multi_source_runtime_service as runtime_module
import app.services.provider_capability_registry as registry_module
import app.services.senior_analyst_projection_v1 as projection_module
from app.core.config import Settings
from app.services.diagnostics_service import DiagnosticsService
from app.services.provider_capability_registry import (
    DATASET_SOURCE_POLICIES,
    dataset_policy_by_id,
    provider_by_id,
)
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
)
from app.services.senior_analyst_projection_v1 import (
    build_senior_analyst_payload_v1,
    validate_senior_analyst_payload_v1,
)


def _service(tmp_path: Path) -> runtime_module.MultiSourceRuntimeService:
    return runtime_module.MultiSourceRuntimeService(
        Settings(
            environment="test",
            database_path=tmp_path / "market-schedule-groups.sqlite",
        )
    )


def _provider_result(*, succeeded: bool) -> dict[str, Any]:
    return {
        "status": "found" if succeeded else "not_found",
        "fetched_count": 1 if succeeded else 0,
        "materialized_count": 1 if succeeded else 0,
    }


NOW = datetime(2026, 7, 30, 18, 30, tzinfo=UTC)
REQUEST_ID = "market-schedule-capability-accounting"
PROJECTION_DATASET_POLICIES = projection_module.DATASET_POLICIES


def _lookup(
    state: str,
) -> dict[str, Any]:
    if state == "VALID":
        return {
            "performed": True,
            "found": True,
            "data_as_of": (NOW - timedelta(hours=1)).isoformat(),
            "content_valid_until": (NOW + timedelta(days=2)).isoformat(),
            "refresh_due_at": (NOW + timedelta(days=1)).isoformat(),
            "lifecycle_status": "CURRENT",
            "expired": False,
            "freshness": "VALID",
            "reason_code": "CANONICAL_RECORD_WITHIN_SLA",
        }
    if state == "EXPIRED":
        return {
            "performed": True,
            "found": True,
            "data_as_of": (NOW - timedelta(days=2)).isoformat(),
            "content_valid_until": (NOW - timedelta(minutes=1)).isoformat(),
            "refresh_due_at": (NOW + timedelta(hours=1)).isoformat(),
            "lifecycle_status": "CURRENT",
            "expired": True,
            "freshness": "EXPIRED_CONTENT_VALID_UNTIL",
            "reason_code": "CANONICAL_CONTENT_VALID_UNTIL_EXPIRED",
        }
    if state == "NOT_FOUND":
        return {
            "performed": True,
            "found": False,
            "data_as_of": None,
            "content_valid_until": None,
            "refresh_due_at": None,
            "lifecycle_status": None,
            "expired": False,
            "freshness": "NOT_FOUND",
            "reason_code": "CANONICAL_RECORD_NOT_FOUND",
        }
    if state == "NOT_LOOKED_UP":
        return {
            "performed": False,
            "found": None,
            "data_as_of": None,
            "content_valid_until": None,
            "refresh_due_at": None,
            "lifecycle_status": None,
            "expired": None,
            "freshness": "NOT_LOOKED_UP",
            "reason_code": (
                "PRIOR_SCHEDULE_CAPABILITY_PROVIDER_SUCCEEDED"
            ),
        }
    raise AssertionError(state)


def _schedule_block(
    provider_id: str,
    *,
    lookup_state: str,
    called: bool,
    status: str,
    reason: str | None = None,
) -> dict[str, Any]:
    metric_id = next(
        capability.metric_id
        for capability in provider_by_id(provider_id).capabilities
        if capability.dataset_id == "market_schedule"
    )
    return {
        "dataset_id": "market_schedule",
        "provider_id": provider_id,
        "capability_metric_id": metric_id,
        "status": status,
        "attempted": called,
        "provider_calls": int(called),
        "cache_used": lookup_state == "VALID",
        "fetched_count": int(called and status == "found"),
        "materialized_count": int(
            status == "found" and (called or lookup_state == "VALID")
        ),
        "reason": reason,
        "database_lookup": _lookup(lookup_state),
    }


def _observed_schedule_blocks() -> dict[str, dict[str, Any]]:
    return {
        "nasdaq_market_info": _schedule_block(
            "NASDAQ_MARKET_INFO",
            lookup_state="VALID",
            called=False,
            status="found",
        ),
        "cme_market_schedule": _schedule_block(
            "CME",
            lookup_state="EXPIRED",
            called=True,
            status="found",
        ),
        "investing_holidays": _schedule_block(
            "INVESTING_HOLIDAYS",
            lookup_state="NOT_FOUND",
            called=True,
            status="found",
        ),
        "marketbeat_holidays": _schedule_block(
            "MARKETBEAT",
            lookup_state="NOT_LOOKED_UP",
            called=False,
            status="not_called",
            reason="PRIOR_SCHEDULE_CAPABILITY_PROVIDER_SUCCEEDED",
        ),
    }


class _ScheduleCollectorAdapter:
    """Expose all policies to Diagnostics but retain only schedule evidence."""

    def __init__(self) -> None:
        policy = _projection_policy()
        self.policies = {
            item.dataset_id: item
            for item in PROJECTION_DATASET_POLICIES
        }
        self.inner = RequestProviderAccountingCollector(
            request_id=REQUEST_ID,
            correlation_id=REQUEST_ID,
            request_started_at=NOW - timedelta(minutes=1),
            policies=[policy],
            clock=lambda: NOW,
        )

    def record(self, dataset_id: str, **values: Any) -> None:
        if dataset_id == "market_schedule":
            self.inner.record(dataset_id, **values)


def _schedule_acquisition_manifest(
    blocks: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    adapter = _ScheduleCollectorAdapter()
    service = object.__new__(DiagnosticsService)
    not_found = _lookup("NOT_FOUND")
    service._record_multi_source_accounting(
        adapter,
        blocks=blocks or _observed_schedule_blocks(),
        risk_context={},
        vix_database_lookup=not_found,
        vix_provider_evidence=None,
        fed_database_lookup=not_found,
        risk_database_lookup=not_found,
    )
    return adapter.inner.manifest(request_completed_at=NOW)


def _project_schedule_manifest(
    monkeypatch: pytest.MonkeyPatch,
    manifest: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = _projection_policy()
    monkeypatch.setattr(
        projection_module,
        "DATASET_POLICIES",
        (policy,),
    )
    source = {
        "symbol": "MNQ",
        "generated_at": NOW.isoformat(),
        "sections": {"market_schedule": {}},
        "request_scoped_provider_accounting": manifest,
    }
    payload = build_senior_analyst_payload_v1(
        source,
        now=NOW,
        request_id=REQUEST_ID,
        request_refresh_mode="force",
    )
    validation = validate_senior_analyst_payload_v1(
        payload,
        now=NOW,
        require_recent_response=True,
    )
    return payload, validation


def _projection_policy() -> Any:
    return next(
        policy
        for policy in PROJECTION_DATASET_POLICIES
        if policy.dataset_id == "market_schedule"
    )


@pytest.mark.asyncio
async def test_nasdaq_success_does_not_skip_complementary_schedule_capabilities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    calls: list[str] = []

    async def run_provider(
        name: str,
        *_: Any,
        **__: Any,
    ) -> dict[str, Any]:
        calls.append(name)
        return _provider_result(succeeded=name == "nasdaq_market_info")

    monkeypatch.setattr(service, "_run_provider", run_provider)

    output = await service._market_schedule_chain(refresh="force")

    assert calls == [
        "nasdaq_market_info",
        "cme_market_schedule",
        "investing_holidays",
        "marketbeat_holidays",
    ]
    assert output["nasdaq_market_info"]["capability_metric_id"] == (
        provider_by_id("NASDAQ_MARKET_INFO").capabilities[0].metric_id
    )
    assert output["cme_market_schedule"]["capability_metric_id"] == (
        provider_by_id("CME").capabilities[0].metric_id
    )


@pytest.mark.asyncio
async def test_investing_success_skips_only_its_marketbeat_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    calls: list[str] = []

    async def run_provider(
        name: str,
        *_: Any,
        **__: Any,
    ) -> dict[str, Any]:
        calls.append(name)
        return _provider_result(
            succeeded=name
            in {
                "nasdaq_market_info",
                "cme_market_schedule",
                "investing_holidays",
            }
        )

    monkeypatch.setattr(service, "_run_provider", run_provider)

    output = await service._market_schedule_chain(refresh="force")

    assert calls == [
        "nasdaq_market_info",
        "cme_market_schedule",
        "investing_holidays",
    ]
    skipped = output["marketbeat_holidays"]
    assert skipped["status"] == "not_called"
    assert skipped["reason"] == (
        "PRIOR_SCHEDULE_CAPABILITY_PROVIDER_SUCCEEDED"
    )
    assert skipped["capability_metric_id"] == output[
        "investing_holidays"
    ]["capability_metric_id"]


@pytest.mark.asyncio
async def test_schedule_capability_groups_follow_mutated_policy_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    policy = dataset_policy_by_id("market_schedule")
    reordered = replace(
        policy,
        primary_provider="CME",
        fallback_providers=(
            "NASDAQ_MARKET_INFO",
            "INVESTING_HOLIDAYS",
            "MARKETBEAT",
        ),
    )
    monkeypatch.setattr(
        registry_module,
        "DATASET_SOURCE_POLICIES",
        tuple(
            reordered if item.dataset_id == "market_schedule" else item
            for item in DATASET_SOURCE_POLICIES
        ),
    )
    calls: list[str] = []

    async def run_provider(
        name: str,
        *_: Any,
        **__: Any,
    ) -> dict[str, Any]:
        calls.append(name)
        return _provider_result(succeeded=True)

    monkeypatch.setattr(service, "_run_provider", run_provider)

    output = await service._market_schedule_chain(refresh="force")

    assert calls == [
        "cme_market_schedule",
        "nasdaq_market_info",
        "investing_holidays",
    ]
    assert output["marketbeat_holidays"]["status"] == "not_called"


@pytest.mark.asyncio
async def test_schedule_unmapped_runtime_fails_before_any_provider_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    calls: list[str] = []

    async def run_provider(
        name: str,
        *_: Any,
        **__: Any,
    ) -> dict[str, Any]:
        calls.append(name)
        return _provider_result(succeeded=True)

    specs = dict(runtime_module._MARKET_SCHEDULE_RUNTIME_SPECS)
    specs.pop("CME")
    monkeypatch.setattr(
        runtime_module,
        "_MARKET_SCHEDULE_RUNTIME_SPECS",
        specs,
    )
    monkeypatch.setattr(service, "_run_provider", run_provider)

    with pytest.raises(RuntimeError, match="RUNTIME_POLICY_MAPPING_MISMATCH"):
        await service._market_schedule_chain(refresh="force")

    assert calls == []


@pytest.mark.asyncio
async def test_incongruent_fallback_capability_fails_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(tmp_path)
    calls: list[str] = []
    original_provider_by_id = runtime_module.provider_by_id
    marketbeat = provider_by_id("MARKETBEAT")
    mutated_capability = replace(
        next(
            capability
            for capability in marketbeat.capabilities
            if capability.dataset_id == "market_schedule"
        ),
        frequency="intraday",
    )
    mutated_marketbeat = replace(
        marketbeat,
        capabilities=tuple(
            mutated_capability
            if capability.dataset_id == "market_schedule"
            else capability
            for capability in marketbeat.capabilities
        ),
    )

    def mutated_provider_by_id(provider_id: str):
        if provider_id == "MARKETBEAT":
            return mutated_marketbeat
        return original_provider_by_id(provider_id)

    async def run_provider(
        name: str,
        *_: Any,
        **__: Any,
    ) -> dict[str, Any]:
        calls.append(name)
        return _provider_result(succeeded=True)

    monkeypatch.setattr(
        runtime_module,
        "provider_by_id",
        mutated_provider_by_id,
    )
    monkeypatch.setattr(service, "_run_provider", run_provider)

    with pytest.raises(
        RuntimeError,
        match="MARKET_SCHEDULE_RUNTIME_CAPABILITY_GROUP_INCONGRUENT",
    ):
        await service._market_schedule_chain(refresh="force")

    assert calls == []


def test_schedule_capability_evidence_is_emitted_and_mutations_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _schedule_acquisition_manifest()

    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    row = manifest["datasets"][0]
    assert row["evidence_status"] == "ACQUISITION_COMPLETE"
    assert row["database_freshness_evaluation"] == "CAPABILITY_SCOPED"
    assert [
        acquisition["capability_metric_id"]
        for acquisition in row["capability_acquisitions"]
    ] == [
        "nasdaq_cash_session",
        "mnq_futures_session",
        "market_holidays",
    ]
    assert [
        item["freshness"]
        for acquisition in row["capability_acquisitions"]
        for item in acquisition["database_lookups"]
    ] == [
        "VALID",
        "EXPIRED_CONTENT_VALID_UNTIL",
        "NOT_FOUND",
        "NOT_LOOKED_UP",
    ]
    assert row["primary_provider"]["execution_origin"] == "CACHE_DECISION"
    assert row["fallbacks"][0]["execution_origin"] == "PROVIDER_CALL"
    assert row["fallbacks"][1]["execution_origin"] == "PROVIDER_CALL"
    assert row["fallbacks"][2]["execution_origin"] == "OBSERVED_SKIP"

    payload, validation = _project_schedule_manifest(
        monkeypatch,
        manifest,
    )
    assert payload["request"]["same_request_provider_accounting"] is True
    assert payload["provider_accounting"][0]["evidence_status"] == "COMPLETE"
    assert validation["checks"]["provider_accounting_valid"] is True

    copied_lookup = deepcopy(_observed_schedule_blocks())
    copied_lookup["cme_market_schedule"]["database_lookup"] = deepcopy(
        copied_lookup["nasdaq_market_info"]["database_lookup"]
    )

    invented_marketbeat = deepcopy(_observed_schedule_blocks())
    invented_marketbeat["marketbeat_holidays"] = _schedule_block(
        "MARKETBEAT",
        lookup_state="NOT_FOUND",
        called=True,
        status="found",
    )

    missing_capability = deepcopy(_observed_schedule_blocks())
    missing_capability.pop("cme_market_schedule")

    missing_lookup_reason = deepcopy(_observed_schedule_blocks())
    missing_lookup_reason["nasdaq_market_info"][
        "database_lookup"
    ].pop("reason_code")

    mutations = [
        ("cme_lookup_copied_from_nasdaq", copied_lookup),
        ("marketbeat_lookup_and_call_invented", invented_marketbeat),
        ("cme_capability_missing", missing_capability),
        ("nasdaq_lookup_reason_missing", missing_lookup_reason),
    ]

    for mutation_name, mutated_blocks in mutations:
        mutated = _schedule_acquisition_manifest(mutated_blocks)
        assert mutated["evidence_status"] == "INCOMPLETE", mutation_name
        assert (
            mutated["datasets"][0]["evidence_status"] == "INCOMPLETE"
        ), mutation_name
        mutated_payload, mutated_validation = _project_schedule_manifest(
            monkeypatch,
            mutated,
        )
        assert (
            mutated_payload["provider_accounting"][0]["evidence_status"]
            == "INCOMPLETE"
        ), mutation_name
        assert (
            mutated_payload["request"]["same_request_provider_accounting"]
            is False
        ), mutation_name
        assert (
            mutated_validation["checks"]["provider_accounting_valid"]
            is False
        ), mutation_name
