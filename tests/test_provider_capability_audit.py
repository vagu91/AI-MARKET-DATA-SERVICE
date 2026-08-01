from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from app.services.provider_audit_artifacts import ProviderAuditArtifactWriter
from app.services.provider_capability_audit import (
    AuditFilters,
    CapturedHttpExchange,
    DatabaseBundleGuard,
    FallbackObservation,
    HealthStatus,
    ProbeOutcome,
    ProviderCapabilityAuditEngine,
    _bound_lineage_valid,
    _bound_occurrence_valid,
    _recomputed_acquisition_request_key,
    build_probe_requests,
    evaluate_fallback_chain,
    select_capability_targets,
    stable_sha256,
    validate_capability_result_derivations,
    verify_policy_fallback_chains,
)
from app.services.provider_capability_registry import (
    CapabilityRegistration,
    DATASET_SOURCE_POLICIES,
    PROVIDER_REGISTRY,
)


ALL_TRUE = {
    "transport_valid": True,
    "schema_valid": True,
    "completeness_valid": True,
    "freshness_valid": True,
    "semantic_mapping_valid": True,
    "occurrence_match_valid": True,
    "lineage_valid": True,
}


def _write_http_exchange_artifacts(
    root: Path,
    *,
    url: str,
    body: bytes,
    sequence: int = 1,
) -> tuple[list[dict[str, Any]], str]:
    root.mkdir(parents=True, exist_ok=True)
    body_path = root / f"http-{sequence}.body"
    headers_path = root / f"http-{sequence}.headers.json"
    body_path.write_bytes(body)
    headers_path.write_text(
        json.dumps(
            {
                "request": {"method": "GET", "url": url},
                "response": {"status_code": 200, "headers": []},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    body_sha256 = hashlib.sha256(body).hexdigest()
    headers_bytes = headers_path.read_bytes()
    return (
        [
            {
                "kind": "http_exchange_body",
                "sequence": sequence,
                "path": body_path.name,
                "sha256": body_sha256,
                "original_sha256": body_sha256,
                "size_bytes": len(body),
                "exact_bytes_saved": True,
                "redaction_applied": False,
            },
            {
                "kind": "http_exchange_headers",
                "sequence": sequence,
                "path": headers_path.name,
                "sha256": hashlib.sha256(headers_bytes).hexdigest(),
                "size_bytes": len(headers_bytes),
            },
        ],
        body_sha256,
    )


def _terminal_policy_results(
    dataset_id: str,
    *,
    health_by_provider: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    policy = next(
        item
        for item in DATASET_SOURCE_POLICIES
        if item.dataset_id == dataset_id
    )
    source_ids = {
        policy.canonical_repository,
        policy.primary_provider,
        *policy.fallback_providers,
        *policy.ai_fallback_providers,
    }
    health_by_provider = health_by_provider or {}
    rows: list[dict[str, Any]] = []
    for provider in PROVIDER_REGISTRY:
        if provider.provider_id not in source_ids:
            continue
        health = health_by_provider.get(provider.provider_id, "HEALTHY")
        eligible = health in {"HEALTHY", "DEGRADED"}
        for capability in provider.capabilities:
            if capability.dataset_id != dataset_id:
                continue
            rows.append(
                {
                    "capability_id": (
                        f"{provider.provider_id}|{dataset_id}|"
                        f"{capability.metric_id}"
                    ),
                    "provider_id": provider.provider_id,
                    "dataset_id": dataset_id,
                    "metric_id": capability.metric_id,
                    "supported_fields": list(
                        capability.supported_fields
                    ),
                    "field_results": {
                        field_name: {"health_status": health}
                        for field_name in capability.supported_fields
                    },
                    "health_status": health,
                    "eligible_as_primary": eligible,
                    "eligible_as_fallback": eligible,
                }
            )
    return rows


def _provider(
    *,
    provider_id: str = "TEST",
    provider_type: str = "API",
    capabilities: list[dict[str, Any]] | None = None,
    credentials: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "provider_id": provider_id,
        "provider_type": provider_type,
        "adapter_path": "tests.fake:Provider",
        "capabilities": capabilities
        or [
            {
                "dataset_id": "macro",
                "metric_id": "headline_pce_yoy",
                "supported_fields": ("actual",),
                "frequency": "monthly",
                "transformation": "identity",
                "probe_id": f"probe.{provider_id.casefold()}.macro",
            }
        ],
        "allowed_roles": ("PRIMARY", "FALLBACK"),
        "credential_requirements": credentials,
        "max_attempts": 2,
        "timeout": 3,
        "probe_id": f"probe.{provider_id.casefold()}",
        "probe_enabled": True,
    }


@pytest.mark.asyncio
async def test_healthy_probe_has_auditable_100_point_score_and_terminal_row(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    async def probe(request: Any) -> ProbeOutcome:
        calls.append(request.request_key)
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            raw_response=b'{"actual":2.6}',
            normalized_response={"actual": 2.6},
            attempts=1,
            checks=ALL_TRUE,
            evidence={
                "real_adapter_invoked": True,
                "probe_dispatch_status": "REAL_ADAPTER",
                "fields": {
                    "actual": {
                        "publisher": "Official Publisher",
                        "acquisition_provider": "TEST",
                    }
                },
            },
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
        clock=lambda: datetime(2026, 7, 31, tzinfo=UTC),
    ).run(sandbox_root=tmp_path)

    assert execution.audit_status == "COMPLETED"
    assert execution.system_health == "HEALTHY"
    assert len(calls) == 1
    result = execution.report["results"][0]
    assert result["health_status"] == "HEALTHY"
    assert result["quality_score"] == 100
    assert sum(
        component["weight"]
        for component in result["field_results"]["actual"]["score_components"].values()
    ) == 100
    assert result["eligible_as_primary"] is True
    assert execution.report["terminal_rows_complete"] is True
    assert execution.report["real_adapter_probes"] == 1
    acquisition = execution.report["acquisitions"][0]
    assert acquisition["run_id"] == execution.run_id
    assert acquisition["provider_id"] == "TEST"
    assert acquisition["target_ids"] == [result["capability_id"]]
    assert acquisition["probe_ids"] == ["probe.test.macro"]
    assert acquisition["probe_adapter_paths"] == ["tests.fake:Provider"]
    assert acquisition["configured"] is True
    assert acquisition["network_exchange_count"] == 0
    assert acquisition["reason_codes"] == []
    assert result["acquisition_id"] == acquisition["acquisition_id"]
    assert result["request_key"] == acquisition["request_key"]
    assert result["checked_at"] == acquisition["checked_at"]
    registration = _provider()
    assert validate_capability_result_derivations(
        result,
        registration["capabilities"][0],
        registration,
    ) == ()

    forged = json.loads(json.dumps(result))
    forged["field_results"]["actual"]["checks"] = {}
    forged["field_results"]["actual"]["quality_score"] = 100
    forged["field_results"]["actual"]["health_status"] = "HEALTHY"
    forged["quality_score"] = 100
    forged["health_status"] = "HEALTHY"
    forged["eligible_as_primary"] = True
    forged["eligible_as_fallback"] = True

    forged_errors = validate_capability_result_derivations(
        forged,
        registration["capabilities"][0],
        registration,
    )
    assert "actual:FIELD_CHECKS_INVALID" in forged_errors
    assert "FIELD_DERIVATION_INCOMPLETE" in forged_errors


@pytest.mark.asyncio
async def test_http_500_cannot_be_healthy_even_with_forged_true_checks(
    tmp_path: Path,
) -> None:
    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            http_status=500,
            attempts=1,
            checks=ALL_TRUE,
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
    ).run(sandbox_root=tmp_path)

    row = execution.report["results"][0]
    assert row["checks"]["transport_valid"] is False
    assert row["health_status"] == "DOWN"
    assert row["eligible_as_primary"] is False
    assert "TRANSPORT_VALID_FAILED" in row["reason_codes"]


@pytest.mark.asyncio
async def test_declared_partial_freshness_produces_auditable_degraded_row(
    tmp_path: Path,
) -> None:
    provider = _provider()
    provider["capabilities"][0]["degradable_quality_checks"] = (
        "freshness_valid",
    )

    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            attempts=1,
            checks={**ALL_TRUE, "freshness_valid": False},
        )

    execution = await ProviderCapabilityAuditEngine(
        (provider,),
        probe,
        settings={},
    ).run(sandbox_root=tmp_path)

    row = execution.report["results"][0]
    assert row["health_status"] == "DEGRADED"
    assert row["quality_score"] == 85
    assert row["eligible_as_primary"] is False
    assert "FRESHNESS_VALID_PARTIAL" in row["reason_codes"]


@pytest.mark.asyncio
async def test_canonical_repository_role_is_eligible_as_db_primary(
    tmp_path: Path,
) -> None:
    provider = _provider()
    provider["allowed_roles"] = ("CANONICAL_REPOSITORY",)

    async def probe(request: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            normalized_response={"actual": 2.6},
            attempts=1,
            checks=ALL_TRUE,
            evidence={
                "real_adapter_invoked": True,
                "probe_dispatch_status": "REAL_ADAPTER",
                "adapter_path": request.adapter_path,
                "fields": {
                    "actual": {
                        "publisher": "Canonical DB",
                    }
                },
            },
        )

    execution = await ProviderCapabilityAuditEngine(
        (provider,),
        probe,
        settings={},
    ).run(sandbox_root=tmp_path)

    result = execution.report["results"][0]
    assert result["eligible_as_primary"] is True
    assert result["eligible_as_fallback"] is False
    assert result["recommendations"] == ["KEEP_PRIMARY"]
    assert validate_capability_result_derivations(
        result,
        provider["capabilities"][0],
        provider,
    ) == ()


@pytest.mark.asyncio
async def test_explicit_request_group_deduplicates_call_but_not_capability_rows(
    tmp_path: Path,
) -> None:
    capabilities = [
        {
            "dataset_id": "macro",
            "metric_id": metric,
            "supported_fields": ("actual",),
            "frequency": "monthly",
            "transformation": "identity",
            "probe_id": "probe.test.batch",
            "request_group": "fred-monthly-batch",
        }
        for metric in ("headline_pce_yoy", "core_pce_yoy")
    ]
    calls = 0

    async def probe(request: Any) -> ProbeOutcome:
        nonlocal calls
        calls += 1
        assert len(request.targets) == 2
        assert request.leaf_request_count == 2
        assert request.call_budget == 2
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            normalized_response={"actual": 1},
            attempts=1,
            checks=ALL_TRUE,
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(capabilities=capabilities),),
        probe,
        settings={},
    ).run(sandbox_root=tmp_path)

    assert calls == 1
    assert execution.report["acquisitions_executed"] == 1
    assert execution.report["capabilities_tested"] == 2
    assert execution.report["deduplicated_capability_count"] == 1
    assert len({row["capability_id"] for row in execution.report["results"]}) == 2


def test_registered_multistep_probe_has_bounded_leaf_call_budget(
    tmp_path: Path,
) -> None:
    targets = select_capability_targets(
        PROVIDER_REGISTRY,
        AuditFilters.from_values(
            providers=("CBOE",),
            metrics=("vix_futures",),
        ),
        settings={},
    )
    request = build_probe_requests(
        targets,
        run_id="offline-call-budget",
        sandbox_root=tmp_path,
        database_snapshot_path=None,
        settings={},
    )[0]

    assert request.max_attempts == 1
    assert request.leaf_request_count == 23
    assert request.call_budget == 23


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("transport_status", "configured", "expected"),
    [
        ("DOWN", True, "DOWN"),
        ("AUTH_FAILED", True, "AUTH_FAILED"),
        ("RATE_LIMITED", True, "RATE_LIMITED"),
        ("UNUSABLE", True, "UNUSABLE"),
        ("NOT_CONFIGURED", False, "NOT_CONFIGURED"),
    ],
)
async def test_every_explicit_transport_status_is_terminal(
    tmp_path: Path,
    transport_status: str,
    configured: bool,
    expected: str,
) -> None:
    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            configured=configured,
            transport_status=transport_status,
            reason_codes=(f"OBSERVED_{transport_status}",),
            checks={name: None for name in ALL_TRUE},
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
    ).run(sandbox_root=tmp_path)

    row = execution.report["results"][0]
    assert row["health_status"] == expected
    assert f"OBSERVED_{transport_status}" in row["reason_codes"]
    assert execution.audit_status == "COMPLETED"


@pytest.mark.asyncio
async def test_missing_evidence_is_unknown_and_semantic_mismatch_is_unusable(
    tmp_path: Path,
) -> None:
    outcomes = iter(
        (
            ProbeOutcome(
                transport_status="OK",
                http_status=200,
                normalized_response={"actual": 1},
                attempts=1,
                checks={},
            ),
            ProbeOutcome(
                transport_status="OK",
                http_status=200,
                normalized_response={"actual": 1},
                attempts=1,
                checks={**ALL_TRUE, "semantic_mapping_valid": False},
            ),
        )
    )

    async def probe(_: Any) -> ProbeOutcome:
        return next(outcomes)

    engine = ProviderCapabilityAuditEngine((_provider(),), probe, settings={})
    unknown = await engine.run(run_id="unknown", sandbox_root=tmp_path / "one")
    unusable = await engine.run(run_id="unusable", sandbox_root=tmp_path / "two")

    assert unknown.report["results"][0]["health_status"] == "UNKNOWN"
    assert unusable.report["results"][0]["health_status"] == "UNUSABLE"
    assert (
        "SEMANTIC_MAPPING_VALID_FAILED"
        in unusable.report["results"][0]["reason_codes"]
    )


@pytest.mark.asyncio
async def test_missing_credential_is_terminal_without_calling_executor(
    tmp_path: Path,
) -> None:
    called = False

    async def probe(_: Any) -> ProbeOutcome:
        nonlocal called
        called = True
        return ProbeOutcome()

    execution = await ProviderCapabilityAuditEngine(
        (_provider(credentials=("api_key",)),),
        probe,
        settings={"api_key": None},
    ).run(sandbox_root=tmp_path)

    assert called is False
    row = execution.report["results"][0]
    assert row["health_status"] == "NOT_CONFIGURED"
    assert "CREDENTIAL_NOT_CONFIGURED:api_key" in row["reason_codes"]


@pytest.mark.asyncio
async def test_ai_filter_excludes_ai_without_claiming_it_was_tested(
    tmp_path: Path,
) -> None:
    providers = (
        _provider(provider_id="HTTP"),
        _provider(provider_id="AI", provider_type="AI"),
    )

    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            checks=ALL_TRUE,
            attempts=1,
        )

    execution = await ProviderCapabilityAuditEngine(
        providers,
        probe,
        settings={},
    ).run(
        filters=AuditFilters.from_values(include_ai=False),
        sandbox_root=tmp_path,
    )

    assert execution.report["providers_tested"] == 1
    assert {row["provider_id"] for row in execution.report["results"]} == {"HTTP"}


def _ai_field_evidence(**updates: Any) -> dict[str, Any]:
    value_sha256 = stable_sha256(2.6)
    value = {
        "field": "actual",
        "value": 2.6,
        "value_present": True,
        "value_sha256": value_sha256,
        "freshness_verified": True,
        "semantic_mapping_verified": True,
        "occurrence_verified": True,
        "request_correlation_verified": True,
        "reference_period_verified": True,
        "source_url": "https://bea.gov/release/pce",
        "source_url_reachable": True,
        "source_content_sha256": "a" * 64,
        "publisher": "BEA",
        "distributor": "BEA",
        "acquisition_provider": "AI_RESEARCHER",
        "verification_origin": "AUDIT_TRANSPORT",
        "field_lineage_verified": True,
        "lineage_evidence": {
            "field": "actual",
            "value_sha256": value_sha256,
            "publisher": "BEA",
            "distributor": "BEA",
            "acquisition_provider": "AI_RESEARCHER",
            "source_url": "https://bea.gov/release/pce",
            "source_content_sha256": "a" * 64,
        },
        "invented": False,
        "model_knowledge_only": False,
    }
    value.update(updates)
    if value.get("value") is None:
        value.update(
            {
                "field_observed": True,
                "explicit_null": True,
                "explicit_null_verified": bool(value.get("null_reason")),
                "value_sha256": None,
                "field_lineage_verified": False,
            }
        )
    return value


@pytest.mark.asyncio
async def test_request_key_and_ai_occurrence_are_unique_to_single_run(
    tmp_path: Path,
) -> None:
    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome.not_configured("CONTROLLED_OFFLINE_PROBE")

    registration = _provider(provider_id="AI", provider_type="AI")
    engine = ProviderCapabilityAuditEngine(
        (registration,),
        probe,
        settings={},
    )
    first = await engine.run(
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "first",
    )
    second = await engine.run(
        run_id="20260731T120001Z",
        sandbox_root=tmp_path / "second",
    )
    first_acquisition = first.report["acquisitions"][0]
    second_acquisition = second.report["acquisitions"][0]
    target_id = first_acquisition["target_ids"][0]
    first_correlation = first_acquisition["request_correlations"][target_id]
    second_correlation = second_acquisition["request_correlations"][target_id]

    assert first_acquisition["run_id"] != second_acquisition["run_id"]
    assert first_acquisition["request_key"] != second_acquisition["request_key"]
    assert (
        first_correlation["expected_occurrence_id"]
        != second_correlation["expected_occurrence_id"]
    )
    assert first_correlation["request_key"] == first_acquisition["request_key"]
    assert second_correlation["request_key"] == second_acquisition["request_key"]
    reused_for_other_run = {
        **first_acquisition,
        "run_id": second_acquisition["run_id"],
    }
    assert (
        _recomputed_acquisition_request_key(reused_for_other_run, registration)
        != reused_for_other_run["request_key"]
    )


def test_accepting_verifier_rejects_source_only_or_unbound_lineage() -> None:
    owner = {
        "actual": 2.6,
        "metric_id": "headline_pce_yoy",
        "provider_id": "DIRECT",
        "lineage": [{"field": "actual", "source": "DIRECT"}],
        "actual_source": "DIRECT",
    }
    forged = {
        "field": "actual",
        "value_sha256": stable_sha256(2.6),
        "field_lineage_verified": True,
        "lineage_evidence": {
            "field": "actual",
            "value_sha256": stable_sha256(2.6),
            "publisher": "BEA",
            "distributor": "DIRECT",
            "acquisition_provider": "DIRECT",
            "source_url": "https://api.example.test/value",
        },
    }

    valid, errors = _bound_lineage_valid(
        forged,
        owner=owner,
        field_name="actual",
        value=2.6,
        expected_provider_id="DIRECT",
        acquisition={
            "capture_mode_expected": "HTTPX",
            "artifact_bindings": [
                {
                    "kind": "http_exchange_body",
                    "sha256": "a" * 64,
                }
            ],
        },
        target={"provider_type": "API", "runtime_source_domains": ("example.test",)},
        registration={"provider_type": "API", "source_domains": ("example.test",)},
        expected_correlation={},
        artifact_root=None,
    )

    assert valid is False
    assert "LINEAGE_EVIDENCE_NOT_REPRODUCIBLE" in errors


def test_accepting_verifier_reloads_exact_http_body_bytes(
    tmp_path: Path,
) -> None:
    value_sha256 = stable_sha256(2.6)
    bindings, source_sha256 = _write_http_exchange_artifacts(
        tmp_path,
        url="https://api.example.test/value",
        body=b"exact provider response bytes",
    )
    lineage = {
        "field": "actual",
        "value_sha256": value_sha256,
        "publisher": "BEA",
        "distributor": "DIRECT",
        "acquisition_provider": "DIRECT",
        "source_url": "https://api.example.test/value",
    }
    source_evidence = {
        **lineage,
        "field_lineage_verified": True,
        "source_url_reachable": True,
        "source_content_sha256": source_sha256,
        "verification_origin": "AUDIT_TRANSPORT",
        "lineage_evidence": {
            **lineage,
            "source_url_reachable": True,
            "source_content_sha256": source_sha256,
            "verification_origin": "AUDIT_TRANSPORT",
            "local_record_locator": None,
        },
    }

    valid, errors = _bound_lineage_valid(
        source_evidence,
        owner={
            "actual": 2.6,
            "provider_id": "DIRECT",
            "lineage": [lineage],
        },
        field_name="actual",
        value=2.6,
        expected_provider_id="DIRECT",
        acquisition={
            "capture_mode_expected": "HTTPX",
            "artifact_bindings": bindings,
        },
        target={"provider_type": "API", "runtime_source_domains": ("example.test",)},
        registration={"provider_type": "API", "source_domains": ("example.test",)},
        expected_correlation={},
        artifact_root=tmp_path,
    )

    assert valid is True
    assert errors == ()


def test_subprocess_lineage_requires_server_captured_source_body(
    tmp_path: Path,
) -> None:
    value_sha256 = stable_sha256(2.6)
    occurrence_id = "provider-audit:AI_RESEARCHER:macro:headline_pce_yoy:key"
    reference_period = "2026-06"
    evidence_text = (
        "headline pce year over year monthly actual 2.6 percent 2026-06"
    )
    lineage = {
        "field": "actual",
        "value_sha256": value_sha256,
        "publisher": "BEA",
        "distributor": "AI_RESEARCHER",
        "acquisition_provider": "AI_RESEARCHER",
        "source_url": "https://bea.gov/release/pce",
        "evidence_text": evidence_text,
        "metric_id": "headline_pce_yoy",
        "frequency": "monthly",
        "unit": "percent",
        "occurrence_id": occurrence_id,
        "reference_period": reference_period,
    }
    declared_only = {
        **lineage,
        "field_lineage_verified": True,
        "source_url_reachable": True,
        "source_content_sha256": "b" * 64,
        "verification_origin": "SOURCE_GATEWAY",
        "lineage_evidence": {
            **lineage,
            "source_content_sha256": "b" * 64,
        },
    }
    owner = {"actual": 2.6, "actual_lineage": lineage}

    valid, errors = _bound_lineage_valid(
        declared_only,
        owner=owner,
        field_name="actual",
        value=2.6,
        expected_provider_id="AI_RESEARCHER",
        acquisition={
            "capture_mode_expected": "SUBPROCESS",
            "artifact_bindings": [],
        },
        target={"provider_type": "AI", "runtime_source_domains": ("bea.gov",)},
        registration={"provider_type": "AI", "source_domains": ("bea.gov",)},
        expected_correlation={
            "expected_occurrence_id": occurrence_id,
            "expected_reference_period": reference_period,
        },
        artifact_root=tmp_path,
    )

    assert valid is False
    assert "LINEAGE_EVIDENCE_NOT_REPRODUCIBLE" in errors

    bindings, source_sha256 = _write_http_exchange_artifacts(
        tmp_path,
        url="https://bea.gov/release/pce",
        body=evidence_text.encode("utf-8"),
    )
    captured = {
        **declared_only,
        "source_content_sha256": source_sha256,
        "verification_origin": "AUDIT_SOURCE_URL_GET",
        "lineage_evidence": {
            **declared_only["lineage_evidence"],
            "source_content_sha256": source_sha256,
            "verification_origin": "AUDIT_SOURCE_URL_GET",
        },
    }
    valid, errors = _bound_lineage_valid(
        captured,
        owner=owner,
        field_name="actual",
        value=2.6,
        expected_provider_id="AI_RESEARCHER",
        acquisition={
            "capture_mode_expected": "SUBPROCESS",
            "artifact_bindings": bindings,
        },
        target={"provider_type": "AI", "runtime_source_domains": ("bea.gov",)},
        registration={"provider_type": "AI", "source_domains": ("bea.gov",)},
        expected_correlation={
            "expected_occurrence_id": occurrence_id,
            "expected_reference_period": reference_period,
        },
        artifact_root=tmp_path,
    )

    assert valid is True
    assert errors == ()


def test_accepting_verifier_rejects_same_period_wrong_occurrence_or_provider() -> None:
    target = {
        "metric_id": "headline_pce_yoy",
        "frequency": "monthly",
    }
    correlation = {
        "provider_id": "DIRECT",
        "expected_occurrence_id": "direct:event-a:2026-06",
        "expected_reference_period": "2026-06",
    }
    wrong_occurrence = {
        "provider_id": "DIRECT",
        "metric_id": "headline_pce_yoy",
        "occurrence_id": "direct:event-b:2026-06",
        "reference_period": "2026-06",
    }
    wrong_provider = {
        **wrong_occurrence,
        "provider_id": "OTHER_PROVIDER",
        "occurrence_id": "direct:event-a:2026-06",
    }
    wrong_owner_with_valid_sibling = {
        "actual": 2.6,
        **wrong_occurrence,
        "other_event": {
            "provider_id": "DIRECT",
            "metric_id": "headline_pce_yoy",
            "occurrence_id": "direct:event-a:2026-06",
            "reference_period": "2026-06",
        },
    }

    assert _bound_occurrence_valid(
        wrong_occurrence,
        target=target,
        normalized_response=wrong_occurrence,
        expected_correlation=correlation,
    ) is False
    assert _bound_occurrence_valid(
        wrong_provider,
        target=target,
        normalized_response=wrong_provider,
        expected_correlation=correlation,
    ) is False
    assert _bound_occurrence_valid(
        wrong_owner_with_valid_sibling,
        target=target,
        normalized_response=wrong_owner_with_valid_sibling,
        expected_correlation=correlation,
        field_name="actual",
    ) is False


@pytest.mark.asyncio
async def test_ai_capability_is_certified_only_with_complete_transport_evidence(
    tmp_path: Path,
) -> None:
    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            attempts=1,
            checks=ALL_TRUE,
            evidence={"fields": {"actual": _ai_field_evidence()}},
        )

    registration = _provider(provider_id="AI", provider_type="AI")
    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        probe,
        settings={},
    ).run(sandbox_root=tmp_path)

    row = execution.report["results"][0]
    assert row["health_status"] == "HEALTHY"
    assert row["eligible_as_primary"] is True
    assert row["eligible_as_fallback"] is True
    assert validate_capability_result_derivations(
        row,
        registration["capabilities"][0],
        registration,
    ) == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("updates", "expected_reason"),
    [
        ({"source_url": None}, "AI_SOURCE_URL_MISSING"),
        ({"source_url_reachable": False}, "AI_SOURCE_NOT_VERIFIED"),
        ({"occurrence_verified": False}, "AI_OCCURRENCE_AMBIGUOUS"),
        (
            {"invented": True, "model_knowledge_only": True},
            "AI_VALUE_INVENTED_OR_MODEL_KNOWLEDGE",
        ),
        (
            {
                "value": None,
                "value_present": False,
                "null_reason": "NO_OCCURRENCE_SPECIFIC_EVIDENCE",
            },
            "NO_OCCURRENCE_SPECIFIC_EVIDENCE",
        ),
        ({"field_lineage_verified": False}, "AI_FIELD_LINEAGE_UNVERIFIED"),
    ],
    ids=[
        "url-missing",
        "source-unreachable",
        "occurrence-ambiguous",
        "invented",
        "correct-null",
        "lineage-incomplete",
    ],
)
async def test_ai_invalid_or_null_field_is_excluded_per_capability(
    tmp_path: Path,
    updates: dict[str, Any],
    expected_reason: str,
) -> None:
    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            attempts=1,
            checks=ALL_TRUE,
            evidence={
                "fields": {
                    "actual": _ai_field_evidence(**updates),
                }
            },
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(provider_id="AI", provider_type="AI"),),
        probe,
        settings={},
    ).run(sandbox_root=tmp_path)

    row = execution.report["results"][0]
    assert row["health_status"] == "UNUSABLE"
    assert row["eligible_as_primary"] is False
    assert row["eligible_as_fallback"] is False
    assert expected_reason in row["reason_codes"]


def test_fallback_chain_is_observation_driven_and_ai_requires_certification() -> None:
    decision = evaluate_fallback_chain(
        (
            FallbackObservation(
                "DB",
                "DB",
                attempted=True,
                valid=False,
                reason_code="DB_RECORD_EXPIRED",
                attempts=1,
            ),
            FallbackObservation(
                "PRIMARY",
                "PROVIDER",
                attempted=True,
                valid=False,
                reason_code="HTTP_500",
                attempts=2,
            ),
            FallbackObservation(
                "FALLBACK",
                "PROVIDER",
                attempted=True,
                valid=False,
                reason_code="SCHEMA_INVALID",
                attempts=1,
            ),
            FallbackObservation(
                "AI",
                "AI",
                attempted=True,
                valid=True,
                value=2.6,
                ai_certified=False,
                attempts=1,
            ),
        )
    )

    assert decision.selected_source is None
    assert decision.delivered_value is None
    assert decision.attempted_order == ("DB", "PRIMARY", "FALLBACK", "AI")
    assert decision.total_attempts == 5
    assert decision.accounting[3]["reason_code"] == "AI_CAPABILITY_NOT_CERTIFIED"


def test_fallback_chain_rejects_ai_before_deterministic_fallback() -> None:
    with pytest.raises(
        ValueError,
        match="AI_MUST_FOLLOW_ALL_DETERMINISTIC_FALLBACKS",
    ):
        evaluate_fallback_chain(
            (
                FallbackObservation(
                    "DB", "DB", attempted=True, valid=False, attempts=1
                ),
                FallbackObservation(
                    "PRIMARY",
                    "PROVIDER",
                    attempted=True,
                    valid=False,
                    attempts=1,
                ),
                FallbackObservation(
                    "AI",
                    "AI",
                    attempted=True,
                    valid=False,
                    attempts=1,
                ),
                FallbackObservation(
                    "FALLBACK",
                    "PROVIDER",
                    attempted=True,
                    valid=True,
                    value=2.5,
                    attempts=1,
                ),
            )
        )


def test_policy_fallback_chain_accounting_is_complete_and_fail_closed() -> None:
    policy = {
        "dataset_id": "macro",
        "canonical_repository": "DB",
        "primary_provider": "PRIMARY",
        "fallback_providers": ("FALLBACK",),
        "ai_fallback_providers": ("AI",),
    }
    results = [
        {
            "capability_id": "DB|macro|actual",
            "provider_id": "DB",
            "dataset_id": "macro",
            "health_status": "DOWN",
            "eligible_as_primary": False,
            "eligible_as_fallback": False,
        },
        {
            "capability_id": "PRIMARY|macro|actual",
            "provider_id": "PRIMARY",
            "dataset_id": "macro",
            "health_status": "DOWN",
            "eligible_as_primary": False,
            "eligible_as_fallback": False,
        },
        {
            "capability_id": "FALLBACK|macro|actual",
            "provider_id": "FALLBACK",
            "dataset_id": "macro",
            "health_status": "HEALTHY",
            "eligible_as_primary": False,
            "eligible_as_fallback": True,
        },
        {
            "capability_id": "AI|macro|actual",
            "provider_id": "AI",
            "dataset_id": "macro",
            "health_status": "NOT_CONFIGURED",
            "eligible_as_primary": False,
            "eligible_as_fallback": False,
        },
    ]

    complete = verify_policy_fallback_chains((policy,), results)

    assert complete["complete"] is True
    assert complete["coverage_pct"] == 100.0
    row = complete["rows"][0]
    assert row["ai_phase_present"] is True
    assert row["ai_after_deterministic_fallbacks"] is True
    assert row["expected_order"] == ["DB", "PRIMARY", "FALLBACK", "AI"]
    assert row["selected_eligible_source"] == "FALLBACK"
    assert row["selected_eligible_role"] == "FALLBACK"
    assert [
        item["selected_by_eligibility_simulation"]
        for item in row["accounting"]
    ] == [False, False, True, False]

    incomplete = verify_policy_fallback_chains((policy,), results[:-1])

    assert incomplete["complete"] is False
    assert incomplete["coverage_pct"] == 0.0
    assert incomplete["rows"][0]["accounting"][-1][
        "terminal_results_present"
    ] is False


def test_policy_without_ai_phase_does_not_claim_vacuous_ai_ordering() -> None:
    accounting = verify_policy_fallback_chains(
        (
            {
                "dataset_id": "macro",
                "canonical_repository": "DB",
                "primary_provider": "PRIMARY",
                "fallback_providers": (),
                "ai_fallback_providers": (),
            },
        ),
        (
            {
                "capability_id": "DB|macro|actual",
                "provider_id": "DB",
                "dataset_id": "macro",
                "health_status": "DOWN",
                "eligible_as_primary": False,
                "eligible_as_fallback": False,
            },
            {
                "capability_id": "PRIMARY|macro|actual",
                "provider_id": "PRIMARY",
                "dataset_id": "macro",
                "health_status": "DOWN",
                "eligible_as_primary": False,
                "eligible_as_fallback": False,
            },
        ),
    )

    row = accounting["rows"][0]
    assert row["ai_phase_present"] is False
    assert row["ai_after_deterministic_fallbacks"] is False


def test_policy_fallback_chain_rejects_wildcard_dataset_results() -> None:
    policy = {
        "dataset_id": "treasury_rates",
        "canonical_repository": "DB",
        "primary_provider": "PRIMARY",
        "fallback_providers": (),
        "ai_fallback_providers": (),
    }
    wildcard_results = [
        {
            "capability_id": "DB|*|canonical_market_fact",
            "provider_id": "DB",
            "dataset_id": "*",
            "health_status": "HEALTHY",
            "eligible_as_primary": True,
            "eligible_as_fallback": True,
        },
        {
            "capability_id": "PRIMARY|treasury_rates|DGS2",
            "provider_id": "PRIMARY",
            "dataset_id": "treasury_rates",
            "health_status": "HEALTHY",
            "eligible_as_primary": True,
            "eligible_as_fallback": True,
        },
    ]

    rejected = verify_policy_fallback_chains(
        (policy,),
        wildcard_results,
    )

    assert rejected["complete"] is False
    assert rejected["coverage_pct"] == 0.0
    db_accounting = rejected["rows"][0]["accounting"][0]
    assert db_accounting["source_id"] == "DB"
    assert db_accounting["terminal_results_present"] is False
    assert db_accounting["capability_ids"] == []

    accepted = verify_policy_fallback_chains(
        (policy,),
        [
            *wildcard_results,
            {
                "capability_id": (
                    "DB|treasury_rates|canonical_treasury_rates_record"
                ),
                "provider_id": "DB",
                "dataset_id": "treasury_rates",
                "health_status": "HEALTHY",
                "eligible_as_primary": True,
                "eligible_as_fallback": True,
            },
        ],
    )

    assert accepted["complete"] is True
    assert accepted["coverage_pct"] == 100.0
    assert accepted["rows"][0]["accounting"][0][
        "capability_ids"
    ] == ["DB|treasury_rates|canonical_treasury_rates_record"]


def test_policy_chain_never_cross_selects_disjoint_metrics() -> None:
    policy = {
        "dataset_id": "compound",
        "canonical_repository": "DB",
        "primary_provider": "PRIMARY",
        "fallback_providers": ("FALLBACK",),
        "ai_fallback_providers": (),
    }
    registry = [
        {
            "provider_id": provider_id,
            "capabilities": [
                {
                    "dataset_id": "compound",
                    "metric_id": metric_id,
                    "supported_fields": ("value",),
                }
            ],
        }
        for provider_id, metric_id in (
            ("DB", "canonical_record"),
            ("PRIMARY", "cash_session"),
            ("FALLBACK", "market_holiday"),
        )
    ]
    results = [
        {
            "capability_id": (
                f"{provider_id}|compound|{metric_id}"
            ),
            "provider_id": provider_id,
            "dataset_id": "compound",
            "metric_id": metric_id,
            "supported_fields": ["value"],
            "field_results": {
                "value": {"health_status": "HEALTHY"}
            },
            "health_status": "HEALTHY",
            "eligible_as_primary": True,
            "eligible_as_fallback": True,
        }
        for provider_id, metric_id in (
            ("DB", "canonical_record"),
            ("PRIMARY", "cash_session"),
            ("FALLBACK", "market_holiday"),
        )
    ]

    accounting = verify_policy_fallback_chains(
        (policy,),
        results,
        registry=registry,
    )

    row = accounting["rows"][0]
    assert accounting["complete"] is False
    assert row["capability_relationship_valid"] is False
    assert row["selected_eligible_source"] == "DB"
    assert row["selected_eligible_role"] == "DB"
    assert row["selected_eligible_sources"] == ["DB"]
    assert row["database_phase_selected"] is True
    assert all(
        item["source_kind"] != "DB"
        for chain in row["capability_chains"]
        for item in chain["accounting"]
    )
    assert {
        (chain["metric_id"], chain["field"])
        for chain in row["capability_chains"]
    } == {("cash_session", "value")}


def test_policy_chain_missing_one_registered_field_is_incomplete() -> None:
    policy = {
        "dataset_id": "macro",
        "canonical_repository": "DB",
        "primary_provider": "PRIMARY",
        "fallback_providers": ("FALLBACK",),
        "ai_fallback_providers": (),
    }
    registry = [
        {
            "provider_id": provider_id,
            "capabilities": [
                {
                    "dataset_id": "macro",
                    "metric_id": "actual",
                    "supported_fields": ("value", "lineage"),
                }
            ],
        }
        for provider_id in ("DB", "PRIMARY", "FALLBACK")
    ]
    results = [
        {
            "capability_id": f"{provider_id}|macro|actual",
            "provider_id": provider_id,
            "dataset_id": "macro",
            "metric_id": "actual",
            "supported_fields": ["value", "lineage"],
            "field_results": {
                "value": {"health_status": "HEALTHY"},
                **(
                    {}
                    if provider_id == "FALLBACK"
                    else {
                        "lineage": {
                            "health_status": "HEALTHY"
                        }
                    }
                ),
            },
            "health_status": "HEALTHY",
            "eligible_as_primary": True,
            "eligible_as_fallback": True,
        }
        for provider_id in ("DB", "PRIMARY", "FALLBACK")
    ]

    accounting = verify_policy_fallback_chains(
        (policy,),
        results,
        registry=registry,
    )

    assert accounting["complete"] is False
    fallback = accounting["rows"][0]["accounting"][2]
    assert fallback["terminal_results_present"] is False
    assert fallback["atomic_capabilities"][0][
        "terminal_fields_present"
    ] is False


def test_every_registered_policy_source_has_atomic_accounting_surface() -> None:
    terminal_rows = [
        {
            "capability_id": (
                f"{provider.provider_id}|"
                f"{capability.dataset_id}|{capability.metric_id}"
            ),
            "provider_id": provider.provider_id,
            "dataset_id": capability.dataset_id,
            "metric_id": capability.metric_id,
            "supported_fields": list(
                capability.supported_fields
            ),
            "field_results": {
                field_name: {
                    "health_status": "NOT_CONFIGURED"
                }
                for field_name in capability.supported_fields
            },
            "health_status": "NOT_CONFIGURED",
            "eligible_as_primary": False,
            "eligible_as_fallback": False,
        }
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
    ]

    accounting = verify_policy_fallback_chains(
        DATASET_SOURCE_POLICIES,
        terminal_rows,
        registry=PROVIDER_REGISTRY,
    )

    assert accounting["policies_expected"] == 25
    assert accounting["policies_accounted"] == 25
    assert accounting["complete_rows"] == 25
    assert accounting["coverage_pct"] == 100.0
    assert accounting["complete"] is True


def test_real_capability_none_delivery_id_never_becomes_string_metric() -> None:
    policy = next(
        item
        for item in DATASET_SOURCE_POLICIES
        if item.dataset_id == "market_schedule"
    )
    assert all(
        isinstance(capability, CapabilityRegistration)
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
    )

    accounting = verify_policy_fallback_chains(
        (policy,),
        _terminal_policy_results("market_schedule"),
        registry=PROVIDER_REGISTRY,
    )

    row = accounting["rows"][0]
    assert row["complete"] is True
    assert all(
        chain["metric_id"] not in {None, "", "None"}
        for chain in row["capability_chains"]
    )
    assert {chain["metric_id"] for chain in row["capability_chains"]} == {
        "market_holidays",
        "mnq_futures_session",
        "nasdaq_cash_session",
    }


def test_vix_fallback_selects_only_exact_delivery_metric_fields() -> None:
    policy = next(
        item for item in DATASET_SOURCE_POLICIES if item.dataset_id == "vix"
    )
    primary = verify_policy_fallback_chains(
        (policy,),
        _terminal_policy_results(
            "vix",
            health_by_provider={"MARKET_FACT_REPOSITORY": "DOWN"},
        ),
        registry=PROVIDER_REGISTRY,
    )["rows"][0]

    assert primary["capability_relationship_valid"] is True
    assert primary["database_phase_selected"] is False
    assert {
        (chain["metric_id"], chain["field"])
        for chain in primary["capability_chains"]
    } == {
        ("vix", "value"),
        ("vix", "data_as_of"),
        ("vix", "lineage"),
    }
    assert all(
        chain["selected_eligible_sources"] == ["FRED"]
        for chain in primary["capability_chains"]
    )

    fallback = verify_policy_fallback_chains(
        (policy,),
        _terminal_policy_results(
            "vix",
            health_by_provider={
                "MARKET_FACT_REPOSITORY": "DOWN",
                "FRED": "DOWN",
            },
        ),
        registry=PROVIDER_REGISTRY,
    )["rows"][0]
    assert all(
        chain["selected_eligible_sources"] == ["CBOE"]
        for chain in fallback["capability_chains"]
    )


def test_flash_pmi_maps_spglobal_value_to_investing_actual_exactly() -> None:
    policy = next(
        item
        for item in DATASET_SOURCE_POLICIES
        if item.dataset_id == "flash_services_pmi"
    )
    row = verify_policy_fallback_chains(
        (policy,),
        _terminal_policy_results(
            "flash_services_pmi",
            health_by_provider={
                "CANONICAL_EVENT_REPOSITORY": "DOWN",
                "SPGLOBAL": "DOWN",
            },
        ),
        registry=PROVIDER_REGISTRY,
    )["rows"][0]

    assert row["complete"] is True
    assert row["capability_relationship_valid"] is True
    assert {
        (chain["metric_id"], chain["field"])
        for chain in row["capability_chains"]
    } == {
        ("flash_services_pmi", "actual"),
        ("flash_services_pmi", "lineage"),
    }
    actual = next(
        chain
        for chain in row["capability_chains"]
        if chain["field"] == "actual"
    )
    assert actual["source_order"] == ["SPGLOBAL", "INVESTING_EVENT_1062"]
    assert [
        item["source_field"] for item in actual["accounting"]
    ] == ["value", "actual"]
    assert actual["selected_eligible_sources"] == ["INVESTING_EVENT_1062"]


def test_macro_calendar_keeps_duplicate_provider_id_phases_distinct() -> None:
    policy = next(
        item
        for item in DATASET_SOURCE_POLICIES
        if item.dataset_id == "macro_calendar"
    )
    row = verify_policy_fallback_chains(
        (policy,),
        _terminal_policy_results("macro_calendar"),
        registry=PROVIDER_REGISTRY,
    )["rows"][0]

    database, primary = row["accounting"][:2]
    assert database["source_id"] == primary["source_id"] == (
        "CANONICAL_EVENT_REPOSITORY"
    )
    assert database["source_kind"] == "DB"
    assert primary["source_kind"] == "PRIMARY"
    assert database["phase_id"] != primary["phase_id"]
    assert database["phase_index"] == 0
    assert primary["phase_index"] == 1
    assert row["selected_eligible_role"] == "DB"
    assert all(
        chain["source_order"].count("CANONICAL_EVENT_REPOSITORY") == 1
        for chain in row["capability_chains"]
    )


def test_current_news_fan_in_selects_all_delivery_eligible_contributors() -> None:
    policy = next(
        item
        for item in DATASET_SOURCE_POLICIES
        if item.dataset_id == "current_news"
    )
    row = verify_policy_fallback_chains(
        (policy,),
        _terminal_policy_results(
            "current_news",
            health_by_provider={"MARKET_NEWS_REPOSITORY": "DOWN"},
        ),
        registry=PROVIDER_REGISTRY,
    )["rows"][0]
    canonical_url = next(
        chain
        for chain in row["capability_chains"]
        if chain["field"] == "canonical_url"
    )

    assert row["provider_strategy"] == "FAN_IN"
    assert canonical_url["selection_mode"] == "ALL_ELIGIBLE_CONTRIBUTORS"
    assert canonical_url["selected_eligible_sources"] == (
        canonical_url["source_order"]
    )
    assert "GDELT_DOC_API" in canonical_url["selected_eligible_sources"]
    assert "AI_RESEARCHER" not in canonical_url["source_order"]
    assert "AI_RESEARCHER" not in canonical_url["selected_eligible_sources"]


def test_nasdaq_cascade_preserves_all_dependency_phases_in_order() -> None:
    policy = next(
        item
        for item in DATASET_SOURCE_POLICIES
        if item.dataset_id == "nasdaq_100"
    )
    row = verify_policy_fallback_chains(
        (policy,),
        _terminal_policy_results(
            "nasdaq_100",
            health_by_provider={"MARKET_FACT_REPOSITORY": "DOWN"},
        ),
        registry=PROVIDER_REGISTRY,
    )["rows"][0]

    assert row["provider_strategy"] == "CASCADE"
    assert row["selected_eligible_sources"] == [
        "INVESCO",
        "ALPHA_VANTAGE",
        "NASDAQ",
        "SEC",
    ]
    assert all(
        chain["selection_mode"] == "ORDERED_ELIGIBLE_DEPENDENCIES"
        for chain in row["capability_chains"]
    )
    assert all(
        chain["phase_order"]
        == sorted(chain["phase_order"])
        for chain in row["capability_chains"]
    )


def test_market_schedule_fan_in_keeps_independent_groups_and_holiday_fallbacks() -> None:
    policy = next(
        item
        for item in DATASET_SOURCE_POLICIES
        if item.dataset_id == "market_schedule"
    )
    row = verify_policy_fallback_chains(
        (policy,),
        _terminal_policy_results(
            "market_schedule",
            health_by_provider={"MARKET_FACT_REPOSITORY": "DOWN"},
        ),
        registry=PROVIDER_REGISTRY,
    )["rows"][0]
    by_metric = {
        chain["metric_id"] for chain in row["capability_chains"]
    }
    holiday = next(
        chain
        for chain in row["capability_chains"]
        if chain["metric_id"] == "market_holidays"
        and chain["field"] == "holiday_date"
    )

    assert by_metric == {
        "market_holidays",
        "mnq_futures_session",
        "nasdaq_cash_session",
    }
    assert holiday["source_order"] == ["INVESTING_HOLIDAYS", "MARKETBEAT"]
    assert holiday["selected_eligible_sources"] == holiday["source_order"]


@pytest.mark.asyncio
async def test_engine_gates_full_chain_but_not_filtered_reprobe(
    tmp_path: Path,
) -> None:
    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            normalized_response={"actual": 2.6},
            attempts=1,
            checks=ALL_TRUE,
        )

    policy = {
        "dataset_id": "macro",
        "canonical_repository": "TEST",
        "primary_provider": "TEST",
        "fallback_providers": ("MISSING_FALLBACK",),
        "ai_fallback_providers": (),
    }
    engine = ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
        source_policies=(policy,),
    )

    full = await engine.run(sandbox_root=tmp_path / "full")
    filtered = await engine.run(
        filters=AuditFilters.from_values(providers=("TEST",)),
        sandbox_root=tmp_path / "filtered",
    )

    assert full.audit_status == "FAILED"
    assert "FALLBACK_CHAIN_ACCOUNTING_INCOMPLETE" in full.report[
        "internal_errors"
    ]
    assert full.report["fallback_chain_accounting"]["scope_applicable"] is True
    assert full.report["fallback_chain_accounting"]["complete"] is False
    assert filtered.audit_status == "COMPLETED"
    assert filtered.report["fallback_chain_accounting"] == {
        "mode": "NOT_APPLICABLE_FILTERED_OR_AI_EXCLUDED_SCOPE",
        "policies_expected": 1,
        "policies_accounted": 0,
        "complete_rows": 0,
        "coverage_pct": None,
        "complete": None,
        "rows": [],
        "scope_applicable": False,
    }


@pytest.mark.parametrize(
    ("observations", "selected", "value", "order", "reason"),
    [
        (
            (
                FallbackObservation(
                    "DB",
                    "DB",
                    attempted=True,
                    valid=True,
                    value=4.1,
                    attempts=1,
                ),
                FallbackObservation(
                    "PRIMARY",
                    "PROVIDER",
                    attempted=False,
                    valid=False,
                ),
            ),
            "DB",
            4.1,
            ("DB",),
            "SOURCE_SELECTED",
        ),
        (
            (
                FallbackObservation(
                    "DB",
                    "DB",
                    attempted=True,
                    valid=False,
                    reason_code="DB_RECORD_EXPIRED",
                    attempts=1,
                ),
                FallbackObservation(
                    "PRIMARY",
                    "PROVIDER",
                    attempted=True,
                    valid=True,
                    value=4.2,
                    attempts=1,
                ),
            ),
            "PRIMARY",
            4.2,
            ("DB", "PRIMARY"),
            "SOURCE_SELECTED",
        ),
        (
            (
                FallbackObservation(
                    "DB",
                    "DB",
                    attempted=True,
                    valid=False,
                    attempts=1,
                ),
                FallbackObservation(
                    "PRIMARY",
                    "PROVIDER",
                    attempted=True,
                    valid=False,
                    reason_code="PROBE_TIMEOUT",
                    attempts=2,
                ),
                FallbackObservation(
                    "FALLBACK_1",
                    "PROVIDER",
                    attempted=True,
                    valid=True,
                    value=3.9,
                    attempts=1,
                    lineage=({"field": "actual", "source": "FALLBACK_1"},),
                ),
            ),
            "FALLBACK_1",
            3.9,
            ("DB", "PRIMARY", "FALLBACK_1"),
            "SOURCE_SELECTED",
        ),
        (
            (
                FallbackObservation(
                    "DB", "DB", attempted=True, valid=False, attempts=1
                ),
                FallbackObservation(
                    "PRIMARY",
                    "PROVIDER",
                    attempted=True,
                    valid=False,
                    attempts=1,
                ),
                FallbackObservation(
                    "AI",
                    "AI",
                    attempted=True,
                    valid=True,
                    value=3.8,
                    ai_certified=True,
                    attempts=1,
                    lineage=({"field": "actual", "source": "AI"},),
                ),
            ),
            "AI",
            3.8,
            ("DB", "PRIMARY", "AI"),
            "SOURCE_SELECTED",
        ),
        (
            (
                FallbackObservation(
                    "DB", "DB", attempted=True, valid=False, attempts=1
                ),
                FallbackObservation(
                    "PRIMARY",
                    "PROVIDER",
                    attempted=True,
                    valid=False,
                    attempts=1,
                ),
                FallbackObservation(
                    "FALLBACK",
                    "PROVIDER",
                    attempted=True,
                    valid=False,
                    attempts=1,
                ),
            ),
            None,
            None,
            ("DB", "PRIMARY", "FALLBACK"),
            "ALL_SOURCES_FAILED_OR_UNAVAILABLE",
        ),
    ],
    ids=[
        "db-valid",
        "db-expired-primary",
        "primary-timeout-fallback",
        "certified-ai",
        "all-failed-null",
    ],
)
def test_fallback_chain_records_order_attempts_selection_and_null(
    observations: tuple[FallbackObservation, ...],
    selected: str | None,
    value: Any,
    order: tuple[str, ...],
    reason: str,
) -> None:
    decision = evaluate_fallback_chain(observations)

    assert decision.selected_source == selected
    assert decision.delivered_value == value
    assert decision.attempted_order == order
    assert decision.reason_code == reason
    assert decision.total_attempts == sum(item.attempts for item in observations)
    if selected is None:
        assert decision.lineage == ()


def test_database_bundle_guard_copies_bytes_without_opening_source(
    tmp_path: Path,
) -> None:
    source = tmp_path / "operational.sqlite"
    wal = Path(f"{source}-wal")
    source.write_bytes(b"sqlite-main")
    wal.write_bytes(b"sqlite-wal")
    guard = DatabaseBundleGuard(source, tmp_path / "sandbox")

    snapshot = guard.create_snapshot()

    assert snapshot is not None
    assert snapshot.read_bytes() == b"sqlite-main"
    assert Path(f"{snapshot}-wal").read_bytes() == b"sqlite-wal"
    assert guard.source_unchanged() is True
    source.write_bytes(b"changed")
    assert guard.source_unchanged() is False


@pytest.mark.asyncio
async def test_artifacts_save_exact_bytes_and_prepare_unpublished_candidate_pointer(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "data" / "provider-capability-audit"
    writer = ProviderAuditArtifactWriter(output_root, "20260731T100000Z")

    async def probe(_: Any) -> ProbeOutcome:
        body = b'{"actual":2.6}\r\n'
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            headers={"content-type": "application/json"},
            raw_response=body,
            normalized_response={"actual": 2.6},
            attempts=1,
            checks=ALL_TRUE,
            evidence={
                "real_adapter_invoked": True,
                "probe_dispatch_status": "REAL_ADAPTER",
            },
            network_exchanges=(
                CapturedHttpExchange(
                    method="GET",
                    url="https://example.test/data",
                    request_headers={},
                    status_code=200,
                    response_headers={"content-type": "application/json"},
                    response_body=body,
                    latency_ms=1.2,
                    attempt=1,
                ),
            ),
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
    ).run(
        run_id="20260731T100000Z",
        sandbox_root=tmp_path / "sandbox",
        artifact_writer=writer,
    )

    assert execution.audit_status == "COMPLETED"
    assert execution.latest_pointer is None
    assert execution.candidate_pointer is not None
    assert not (
        tmp_path / "data" / "provider-capability-audit-latest.json"
    ).exists()
    pointer = json.loads(
        execution.candidate_pointer.read_text(encoding="utf-8")
    )
    assert pointer["run_id"] == "20260731T100000Z"
    run_directory = Path(pointer["run_directory"])
    required = {
        "audit-report.json",
        "audit-report.md",
        "capability-matrix.json",
        "checksums.json",
        "comparison-with-previous.json",
    }
    assert required.issubset({path.name for path in run_directory.iterdir()})
    assert set(pointer["artifacts"]) == required
    exchange_body = next(
        (run_directory / "provider-responses").glob("*.http-001.response.bin")
    )
    assert exchange_body.read_bytes() == b'{"actual":2.6}\r\n'
    report = json.loads(
        (run_directory / "audit-report.json").read_text(encoding="utf-8")
    )
    assert report["acquisition_artifact_bindings_complete"] is True
    acquisition = report["acquisitions"][0]
    assert acquisition["artifact_binding_complete"] is True
    bindings = acquisition["artifact_bindings"]
    assert {
        "acquisition_metadata",
        "http_exchange_body",
        "http_exchange_headers",
        "normalized_response",
        "response_body",
        "response_headers",
    } == {item["kind"] for item in bindings}
    for binding in bindings:
        artifact = run_directory / binding["path"]
        body = artifact.read_bytes()
        assert len(body) == binding["size_bytes"]
        assert hashlib.sha256(body).hexdigest() == binding["sha256"]
    metadata_binding = next(
        item
        for item in bindings
        if item["kind"] == "acquisition_metadata"
    )
    metadata = json.loads(
        (run_directory / metadata_binding["path"]).read_text(encoding="utf-8")
    )
    assert metadata["run_id"] == report["run_id"]
    assert metadata["acquisition_id"] == acquisition["acquisition_id"]
    assert metadata["request_key"] == acquisition["request_key"]
    assert metadata["target_ids"] == acquisition["target_ids"]
    checksums = json.loads(
        (run_directory / "checksums.json").read_text(encoding="utf-8")
    )
    checksum_paths = {item["path"] for item in checksums["files"]}
    expected_checksum_paths = {
        path.relative_to(run_directory).as_posix()
        for path in run_directory.rglob("*")
        if path.is_file() and path.name != "checksums.json"
    }
    assert checksum_paths == expected_checksum_paths
    assert {item["path"] for item in bindings} <= checksum_paths
    assert any(
        item["path"].endswith(".http-001.response.bin")
        for item in checksums["files"]
    )


@pytest.mark.asyncio
async def test_secret_response_is_redacted_and_original_hash_is_only_diagnostic(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "data" / "provider-capability-audit"
    writer = ProviderAuditArtifactWriter(
        output_root,
        "secret-run",
        secret_values=("super-secret-value",),
    )

    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            raw_response=b'{"api_key":"super-secret-value","actual":1}',
            normalized_response={"actual": 1},
            attempts=1,
            checks=ALL_TRUE,
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
    ).run(
        run_id="secret-run",
        sandbox_root=tmp_path / "sandbox",
        artifact_writer=writer,
    )

    response = next(
        (execution.artifact_directory / "provider-responses").glob("*.response.bin")
    )
    assert b"super-secret-value" not in response.read_bytes()
    metadata = json.loads(
        next(
            (execution.artifact_directory / "provider-responses").glob("*.metadata.json")
        ).read_text(encoding="utf-8")
    )
    assert metadata["raw_response"]["exact_bytes_saved"] is False
    assert metadata["raw_response"]["redaction_applied"] is True


@pytest.mark.asyncio
async def test_dynamic_json_tokens_and_duplicate_sensitive_headers_are_redacted(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "data" / "provider-capability-audit"
    writer = ProviderAuditArtifactWriter(
        output_root,
        "dynamic-secret-run",
        secret_values=("FAKE_CONFIG_SECRET",),
    )
    body = b'{"access_token":"FAKE_DYNAMIC_TOKEN_123456789","actual":1}'
    response_headers = (
        ("content-type", "application/json"),
        ("set-cookie", "session=FAKE_SESSION_A"),
        ("set-cookie", "csrf=FAKE_SESSION_B"),
    )

    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            headers=response_headers,
            raw_response=body,
            normalized_response={"actual": 1},
            attempts=1,
            checks=ALL_TRUE,
            network_exchanges=(
                CapturedHttpExchange(
                    method="GET",
                    url="https://example.test/data",
                    request_headers=(
                        ("proxy-authorization", "Bearer FAKE_PROXY_TOKEN"),
                        ("x-custom-auth", "FAKE_CONFIG_SECRET"),
                    ),
                    status_code=200,
                    response_headers=response_headers,
                    response_body=body,
                    latency_ms=1.0,
                    attempt=1,
                ),
            ),
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
    ).run(
        run_id="dynamic-secret-run",
        sandbox_root=tmp_path / "sandbox",
        artifact_writer=writer,
    )

    artifacts = execution.artifact_directory / "provider-responses"
    saved_body = next(artifacts.glob("*.http-001.response.bin")).read_bytes()
    assert b"FAKE_DYNAMIC_TOKEN" not in saved_body
    assert b"<redacted>" in saved_body
    exchange_headers = json.loads(
        next(artifacts.glob("*.http-001.headers.json")).read_text(
            encoding="utf-8"
        )
    )
    request_items = exchange_headers["request"]["headers"]["items"]
    response_items = exchange_headers["response"]["headers"]["items"]
    assert [
        item["name"] for item in response_items if item["name"] == "set-cookie"
    ] == ["set-cookie", "set-cookie"]
    assert all(
        item["value"] == "<redacted>"
        for item in response_items
        if item["name"] == "set-cookie"
    )
    assert all(item["value"] == "<redacted>" for item in request_items)


@pytest.mark.asyncio
async def test_failed_audit_does_not_publish_latest_pointer(tmp_path: Path) -> None:
    output_root = tmp_path / "data" / "provider-capability-audit"
    writer = ProviderAuditArtifactWriter(output_root, "failed-run")

    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status=HealthStatus.DOWN.value,
            reason_codes=("TIMEOUT",),
        )

    execution = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
    ).run(
        run_id="failed-run",
        sandbox_root=tmp_path / "sandbox",
        artifact_writer=writer,
        registry_validation={"valid": False, "errors": ["BROKEN_REGISTRY"]},
    )

    assert execution.audit_status == "FAILED"
    assert execution.artifact_directory.is_dir()
    assert execution.candidate_pointer is None
    assert execution.latest_pointer is None
    assert not (tmp_path / "data" / "provider-capability-audit-latest.json").exists()


@pytest.mark.asyncio
async def test_filtered_completed_audit_cannot_replace_full_latest_pointer(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "data" / "provider-capability-audit"

    async def probe(_: Any) -> ProbeOutcome:
        return ProbeOutcome(
            transport_status="OK",
            http_status=200,
            attempts=1,
            checks=ALL_TRUE,
            evidence={
                "real_adapter_invoked": True,
                "probe_dispatch_status": "REAL_ADAPTER",
            },
        )

    full = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
    ).run(
        run_id="full-run",
        sandbox_root=tmp_path / "full-sandbox",
        artifact_writer=ProviderAuditArtifactWriter(output_root, "full-run"),
    )
    assert full.candidate_pointer is not None
    assert full.latest_pointer is None
    latest_pointer = output_root.parent / "provider-capability-audit-latest.json"
    latest_pointer.write_text(
        '{"run_id":"previous-full-run"}\n',
        encoding="utf-8",
        newline="\n",
    )
    original_pointer = latest_pointer.read_bytes()

    filtered = await ProviderCapabilityAuditEngine(
        (_provider(),),
        probe,
        settings={},
    ).run(
        filters=AuditFilters.from_values(metrics=("headline_pce_yoy",)),
        run_id="filtered-run",
        sandbox_root=tmp_path / "filtered-sandbox",
        artifact_writer=ProviderAuditArtifactWriter(output_root, "filtered-run"),
    )

    assert filtered.audit_status == "COMPLETED"
    assert filtered.report["full_audit_scope"] is False
    assert filtered.artifact_directory.is_dir()
    assert filtered.candidate_pointer is None
    assert filtered.latest_pointer is None
    assert latest_pointer.read_bytes() == original_pointer
    pointer = json.loads(latest_pointer.read_text(encoding="utf-8"))
    assert pointer["run_id"] == "previous-full-run"
