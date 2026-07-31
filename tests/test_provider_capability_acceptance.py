from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import sys
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

import scripts.provider_capability_audit as audit_script
import scripts.provider_capability_live_baseline as baseline_import
import scripts.generate_provider_capability_matrix as matrix_generator
import scripts.validate_senior_analyst_payload as acceptance
from app.services.provider_audit_artifacts import (
    ProviderAuditArtifactWriter,
    ProviderAuditPublicationRejected,
    publish_verified_audit_candidate,
)
from app.services.provider_capability_audit import (
    AuditFilters,
    CapturedHttpExchange,
    ProbeOutcome,
    ProviderCapabilityAuditEngine,
)
from app.services.provider_capability_registry import (
    DATASET_SOURCE_POLICIES,
    PROVIDER_REGISTRY,
    provider_by_id,
    registry_summary,
    validate_registry,
)
from scripts.provider_capability_live_baseline import (
    ProviderCapabilityBaselineImportError,
    _compact_field_result,
    baseline_file_bytes,
    baseline_file_sha256,
    import_verified_live_audit,
)


RUN_ID = "20260731T120000Z"
OFFLINE_SUBPROCESS_SOURCE_URL = (
    "https://evidence.example.test/offline-fixture"
)
OFFLINE_SUBPROCESS_SOURCE_BODY = b"offline-server-captured-source"
OFFLINE_SUBPROCESS_SOURCE_SHA256 = hashlib.sha256(
    OFFLINE_SUBPROCESS_SOURCE_BODY
).hexdigest()
ALL_TRUE = {
    "transport_valid": True,
    "schema_valid": True,
    "completeness_valid": True,
    "freshness_valid": True,
    "semantic_mapping_valid": True,
    "occurrence_match_valid": True,
    "lineage_valid": True,
}
TOP_LEVEL_ARTIFACTS = {
    "audit-report.json",
    "audit-report.md",
    "capability-matrix.json",
    "comparison-with-previous.json",
    "checksums.json",
}


def _offline_observation(
    target: Any,
    request: Any,
    *,
    capture_mode: str,
) -> dict[str, Any]:
    observed_at = datetime.now(UTC)
    reference_period = observed_at.date().isoformat()
    correlation = request.correlation_for(target)
    occurrence_id = str(
        correlation.get("expected_occurrence_id")
        or f"offline:{target.provider_id}:{target.metric_id}:{reference_period}"
    )
    field_values = {
        field_name: (
            [{"observed": True}]
            if field_name == "lineage"
            else occurrence_id
            if field_name == "occurrence_id"
            else reference_period
            if field_name == "reference_period"
            else observed_at.isoformat()
            if field_name
            in {"data_as_of", "released_at", "published_at"}
            else (observed_at + timedelta(days=30)).isoformat()
            if field_name
            in {"content_valid_until", "refresh_due_at"}
            else target.metric_id
            if field_name == "metric_id"
            else target.frequency
            if field_name == "frequency"
            else target.transformation
            if field_name == "transformation"
            else target.provider_id
            if field_name
            in {"source", "provider_id", "acquisition_provider"}
            else {"fixture": 1}
            if field_name == "raw_payload"
            else True
            if field_name == "available"
            else 1
        )
        for field_name in target.fields
    }
    source_url = (
        "https://offline-audit-fixture.invalid/probe"
        if capture_mode == "HTTPX"
        else OFFLINE_SUBPROCESS_SOURCE_URL
        if capture_mode == "SUBPROCESS"
        else None
    )
    field_lineage = {
        field_name: {
            "field": field_name,
            "value_sha256": acceptance.stable_sha256(field_values[field_name]),
            "publisher": "OFFLINE_FIXTURE_PUBLISHER",
            "distributor": "OFFLINE_FIXTURE_DISTRIBUTOR",
            "acquisition_provider": target.provider_id,
            "source_url": source_url,
            "source_url_reachable": capture_mode == "SUBPROCESS",
            "source_content_sha256": (
                OFFLINE_SUBPROCESS_SOURCE_SHA256
                if capture_mode == "SUBPROCESS"
                else None
            ),
            "verification_origin": (
                "AUDIT_TRANSPORT"
                if capture_mode == "SUBPROCESS"
                else None
            ),
            "target_id": target.target_id,
            "metric_id": target.metric_id,
        }
        for field_name in target.fields
    }
    return {
        "target_id": target.target_id,
        "dataset_id": target.dataset_id,
        "metric_id": target.metric_id,
        "name": target.metric_id,
        "frequency": (
            target.frequency
            if str(target.frequency or "").casefold()
            in {
                "daily",
                "event",
                "intraday",
                "monthly",
                "quarterly",
                "weekly",
                "yearly",
            }
            else None
        ),
        "transformation": target.transformation,
        "occurrence_id": occurrence_id,
        "reference_period": reference_period,
        "expected_occurrence_id": occurrence_id,
        "expected_reference_period": reference_period,
        "request_key": request.request_key,
        "provider_id": target.provider_id,
        "latest_release_verified": True,
        "data_as_of": observed_at.isoformat(),
        "content_valid_until": (
            observed_at + timedelta(days=30)
        ).isoformat(),
        "refresh_due_at": (
            observed_at + timedelta(days=30)
        ).isoformat(),
        "lifecycle": "CURRENT_LATEST_OFFICIAL_RELEASE",
        "lineage": list(field_lineage.values()),
        "field_lineage": field_lineage,
        "fields": field_values,
        **field_values,
    }


class OfflineRealDispatchExecutor:
    """Controlled real-dispatch evidence with no transport or network call."""

    requires_real_dispatch = True

    def __init__(self) -> None:
        self.dispatches: list[str] = []
        self.network_requests = 0

    async def __call__(self, request: Any) -> ProbeOutcome:
        self.dispatches.append(request.acquisition_id)
        capture_mode = str(request.registration.capture_mode)
        normalized = {
            "fixture": "OFFLINE_REAL_DISPATCH",
            "provider_id": request.provider_id,
            "request_key": request.request_key,
            "target_ids": [
                target.target_id for target in request.targets
            ],
            "observations": [
                _offline_observation(
                    target,
                    request,
                    capture_mode=capture_mode,
                )
                for target in request.targets
            ],
        }
        raw = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        provider_type = request.targets[0].provider_type.upper()
        if provider_type in {
            "MANUAL_FILE",
            "RECONCILIATION",
            "REPOSITORY",
            "TRANSFORMATION",
        }:
            capture_evidence = {
                "capture_mode": "LOCAL_SANDBOX",
                "capture_mode_expected": "LOCAL_SANDBOX",
                "capture_verified": True,
                "capture_attestation": {
                    "database_snapshot_isolated": False,
                    "sandbox_root": str(request.sandbox_root),
                },
            }
            exchanges: tuple[CapturedHttpExchange, ...] = ()
        elif request.provider_id == "AI_RESEARCHER":
            normalized["process_result"] = {
                "exit_code": 0,
                "failure_reason": None,
                "status": "OFFLINE_CONTROLLED_FIXTURE",
                "duration_ms": 0,
            }
            raw = json.dumps(
                normalized,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            capture_evidence = {
                "capture_mode": "SUBPROCESS",
                "capture_mode_expected": "SUBPROCESS",
                "capture_verified": True,
                "capture_attestation": {
                    "exit_code": 0,
                    "failure_reason": None,
                    "status": "OFFLINE_CONTROLLED_FIXTURE",
                    "duration_ms": 0,
                    "bounded_timeout_observed": False,
                    "subprocess_output_sha256": hashlib.sha256(
                        raw
                    ).hexdigest(),
                    "subprocess_output_size_bytes": len(raw),
                    "source_exchange_count": 1,
                },
            }
            exchanges = (
                CapturedHttpExchange(
                    method="GET",
                    url=OFFLINE_SUBPROCESS_SOURCE_URL,
                    request_headers=(),
                    status_code=200,
                    response_headers=(
                        ("content-type", "application/octet-stream"),
                    ),
                    response_body=OFFLINE_SUBPROCESS_SOURCE_BODY,
                    latency_ms=0.0,
                    attempt=1,
                ),
            )
        else:
            def handler(_: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    content=raw,
                    headers={"content-type": "application/json"},
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                response = await client.get(
                    "https://offline-audit-fixture.invalid/probe"
                )
                response_body = await response.aread()
            exchanges = (
                CapturedHttpExchange(
                    method="GET",
                    url=str(response.request.url),
                    request_headers=dict(response.request.headers),
                    status_code=response.status_code,
                    response_headers=dict(response.headers),
                    response_body=response_body,
                    latency_ms=0.0,
                    attempt=1,
                ),
            )
            capture_evidence = {
                "capture_mode": "HTTPX",
                "capture_mode_expected": "HTTPX",
                "capture_verified": True,
                "capture_attestation": {
                    "exchange_count": 1,
                    "first_attempt": 1,
                    "last_attempt": 1,
                },
            }
        checks, field_checks = audit_script._evidence_checks(
            request,
            normalized,
            exchanges,
        )
        fields = audit_script._extract_field_evidence(
            request,
            normalized,
            exchanges,
        )
        return ProbeOutcome(
            configured=True,
            transport_status="OK",
            http_status=200,
            raw_response=raw,
            normalized_response=normalized,
            latency_ms=0.1,
            attempts=1,
            checks=checks,
            field_checks=field_checks,
            evidence={
                "real_adapter_invoked": True,
                "probe_dispatch_status": "REAL_ADAPTER",
                "adapter_path": request.adapter_path,
                "probe_id": request.targets[0].probe_id,
                "network_call_count": len(exchanges),
                **capture_evidence,
                "fields": fields,
            },
            network_exchanges=exchanges,
        )


@dataclass(frozen=True)
class OfflineAuditRun:
    pointer_path: Path | None
    run_directory: Path
    report_path: Path
    matrix_path: Path
    checksums_path: Path
    executor: OfflineRealDispatchExecutor
    report: dict[str, Any]


def _run_offline_audit(
    root: Path,
    *,
    filters: AuditFilters | None = None,
    run_id: str = RUN_ID,
) -> OfflineAuditRun:
    assert validate_registry(raise_on_error=False) == ()
    data_root = root / "data"
    output_root = data_root / "provider-capability-audit"
    executor = OfflineRealDispatchExecutor()
    writer = ProviderAuditArtifactWriter(output_root, run_id)
    execution = asyncio.run(
        ProviderCapabilityAuditEngine(
            PROVIDER_REGISTRY,
            executor,
            settings={},
            source_policies=DATASET_SOURCE_POLICIES,
        ).run(
            filters=filters,
            run_id=run_id,
            sandbox_root=root / "sandbox",
            artifact_writer=writer,
            registry_validation={"valid": True, "errors": []},
        )
    )
    pointer_path = None
    if execution.candidate_pointer is not None:
        pointer_path = writer.publish_candidate(
            execution.candidate_pointer,
            lambda candidate, canonical: acceptance._verified_capability_audit(
                candidate,
                canonical_pointer_path=canonical,
            ),
        )
    run_directory = output_root / run_id
    report_path = run_directory / "audit-report.json"
    report = _read_json(report_path)
    return OfflineAuditRun(
        pointer_path=pointer_path,
        run_directory=run_directory,
        report_path=report_path,
        matrix_path=run_directory / "capability-matrix.json",
        checksums_path=run_directory / "checksums.json",
        executor=executor,
        report=report,
    )


@pytest.fixture
def full_audit(tmp_path: Path) -> OfflineAuditRun:
    run = _run_offline_audit(tmp_path)
    assert run.pointer_path == (
        tmp_path / "data" / "provider-capability-audit-latest.json"
    )
    assert run.executor.network_requests == 0
    return run


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _identity(path: Path) -> dict[str, Any]:
    body = path.read_bytes()
    return {
        "path": str(path.resolve()),
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def _refresh_genuine_artifact_integrity(run: OfflineAuditRun) -> None:
    """Re-sign a deliberately mutated genuine run for semantic tamper tests."""

    checksums = _read_json(run.checksums_path)
    checksums["files"] = [
        {
            "path": path.relative_to(run.run_directory).as_posix(),
            **{
                key: value
                for key, value in _identity(path).items()
                if key != "path"
            },
        }
        for path in sorted(run.run_directory.rglob("*"))
        if path.is_file() and path.name != "checksums.json"
    ]
    _write_json(run.checksums_path, checksums)
    assert run.pointer_path is not None
    pointer = _read_json(run.pointer_path)
    pointer["artifacts"] = {
        name: _identity(run.run_directory / name)
        for name in sorted(TOP_LEVEL_ARTIFACTS)
    }
    _write_json(run.pointer_path, pointer)


def _mutate_results(
    run: OfflineAuditRun,
    mutation: Any,
) -> None:
    report = _read_json(run.report_path)
    rows = [dict(item) for item in report["results"]]
    report["results"] = mutation(rows)
    _write_json(run.report_path, report)
    matrix = _read_json(run.matrix_path)
    matrix["capabilities"] = report["results"]
    _write_json(run.matrix_path, matrix)
    _refresh_genuine_artifact_integrity(run)


def test_full_audit_pointer_is_accepted_only_in_expected_run_tree(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    pointer, report, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert errors == []
    assert pointer is not None
    assert report is not None
    assert set(pointer["artifacts"]) == TOP_LEVEL_ARTIFACTS
    assert report["capabilities_tested"] == registry_summary()[
        "capabilities_registered"
    ]
    assert report["acquisition_artifact_bindings_complete"] is True
    assert report["fallback_chain_accounting"]["complete"] is True
    assert report["fallback_chain_accounting"]["coverage_pct"] == 100.0
    assert report["runtime_adapter_coverage"]["complete"] is True
    assert report["runtime_adapter_coverage"]["coverage_pct"] == 100.0
    assert report["runtime_adapter_coverage"]["adapter_bindings_expected"] == (
        sum(
            1
            for provider in PROVIDER_REGISTRY
            if provider.runtime_adapter
            for _ in (
                provider.adapter_path,
                *provider.additional_adapter_paths,
            )
        )
    )
    assert report["runtime_adapter_coverage"][
        "unique_adapter_paths_expected"
    ] == (
        registry_summary()["runtime_adapters_registered"]
    )
    assert report["real_adapter_probes"] == len(
        full_audit.executor.dispatches
    )
    assert all(
        acquisition["artifact_binding_complete"] is True
        and acquisition["artifact_bindings"]
        for acquisition in report["acquisitions"]
    )
    assert all(
        acquisition["artifact_binding_complete"] is True
        and acquisition["artifact_bindings"]
        for acquisition in report["runtime_adapter_acquisitions"]
    )

    # A forged candidate must not replace the already-published accepted run.
    previous_latest = full_audit.pointer_path.read_bytes()
    rejected_candidate = (
        full_audit.pointer_path.parent / ".rejected-audit-candidate.json"
    )
    forged_pointer = _read_json(full_audit.pointer_path)
    forged_pointer["providers_tested"] += 1
    _write_json(rejected_candidate, forged_pointer)
    with pytest.raises(ProviderAuditPublicationRejected) as rejection:
        publish_verified_audit_candidate(
            rejected_candidate,
            full_audit.pointer_path,
            lambda candidate, canonical: acceptance._verified_capability_audit(
                candidate,
                canonical_pointer_path=canonical,
            ),
        )
    assert rejection.value.errors
    assert full_audit.pointer_path.read_bytes() == previous_latest
    assert not rejected_candidate.exists()

    # The exact same run is published only when the complete acceptance passes.
    accepted_candidate = (
        full_audit.pointer_path.parent / ".accepted-audit-candidate.json"
    )
    accepted_candidate.write_bytes(previous_latest)
    full_audit.pointer_path.write_text(
        '{"run_id":"previous-accepted-run"}\n',
        encoding="utf-8",
        newline="\n",
    )
    published = publish_verified_audit_candidate(
        accepted_candidate,
        full_audit.pointer_path,
        lambda candidate, canonical: acceptance._verified_capability_audit(
            candidate,
            canonical_pointer_path=canonical,
        ),
    )
    assert published == full_audit.pointer_path
    assert published.read_bytes() == previous_latest
    assert not accepted_candidate.exists()


def test_filtered_audit_is_not_published_for_live_acceptance(
    tmp_path: Path,
) -> None:
    filtered = _run_offline_audit(
        tmp_path,
        filters=AuditFilters.from_values(providers=("FRED",)),
        run_id="filtered-audit",
    )

    assert filtered.pointer_path is None
    assert filtered.report["audit_status"] == "COMPLETED"
    assert filtered.report["full_audit_scope"] is False
    assert filtered.report["fallback_chain_accounting"][
        "scope_applicable"
    ] is False
    pointer_path = (
        tmp_path / "data" / "provider-capability-audit-latest.json"
    )
    _, _, errors = acceptance._verified_capability_audit(pointer_path)
    assert errors == ["CAPABILITY_AUDIT_POINTER_UNREADABLE"]


def test_audit_run_directory_outside_expected_root_is_rejected(
    full_audit: OfflineAuditRun,
    tmp_path: Path,
) -> None:
    assert full_audit.pointer_path is not None
    outside = tmp_path / "outside" / RUN_ID
    outside.mkdir(parents=True)
    pointer = _read_json(full_audit.pointer_path)
    pointer["run_directory"] = str(outside.resolve())
    _write_json(full_audit.pointer_path, pointer)

    _, report, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert report is None
    assert (
        "CAPABILITY_AUDIT_RUN_DIRECTORY_OUTSIDE_EXPECTED_ROOT"
        in errors
    )


def test_recomputed_artifacts_with_duplicate_capability_are_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    _mutate_results(
        full_audit,
        lambda rows: [*rows[:-1], dict(rows[0])],
    )

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_REPORT_INCOMPLETE" in errors


def test_forged_healthy_result_derivations_are_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None

    def forge(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        row = rows[0]
        field_name = row["supported_fields"][0]
        field = dict(row["field_results"][field_name])
        field["checks"] = {}
        field["score_components"] = {}
        field["quality_score"] = 100
        field["health_status"] = "HEALTHY"
        row["field_results"] = {
            **row["field_results"],
            field_name: field,
        }
        row["checks"] = {}
        row["quality_score"] = 100
        row["health_status"] = "HEALTHY"
        row["eligible_as_primary"] = True
        row["eligible_as_fallback"] = True
        return rows

    _mutate_results(full_audit, forge)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_REPORT_INCOMPLETE" in errors


def test_result_acquisition_mismatch_is_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None

    def mismatch(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows[0]["request_key"] = "0" * 64
        return rows

    _mutate_results(full_audit, mismatch)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_REPORT_INCOMPLETE" in errors
    assert "CAPABILITY_AUDIT_RESULT_ACQUISITION_MISMATCH" in errors


def test_unregistered_probe_adapter_path_is_rejected_even_if_resigned(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    report = _read_json(full_audit.report_path)
    row = next(item for item in report["results"] if item["configured"])
    acquisition = next(
        item
        for item in report["acquisitions"]
        if item["acquisition_id"] == row["acquisition_id"]
    )
    fake_adapter = "attacker.module:FakeAdapter"
    acquisition["probe_adapter_paths"] = [fake_adapter]
    for affected in report["results"]:
        if affected["acquisition_id"] != acquisition["acquisition_id"]:
            continue
        affected["probe_adapter_path"] = fake_adapter
        for field_result in affected["field_results"].values():
            field_result["evidence"]["registered_adapter_path"] = fake_adapter
            field_result["evidence"]["observed_adapter_path"] = fake_adapter
    _write_json(full_audit.report_path, report)
    matrix = _read_json(full_audit.matrix_path)
    matrix["capabilities"] = report["results"]
    _write_json(full_audit.matrix_path, matrix)
    _refresh_genuine_artifact_integrity(full_audit)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_REPORT_INCOMPLETE" in errors
    assert (
        "CAPABILITY_AUDIT_ACQUISITION_REGISTRY_IDENTITY_MISMATCH"
        in errors
    )


def test_missing_normalized_field_cannot_be_forged_healthy(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    report = _read_json(full_audit.report_path)
    row = next(
        item
        for item in report["results"]
        if item["configured"]
        and item["supported_fields"]
    )
    field_name = row["supported_fields"][0]
    acquisition = next(
        item
        for item in report["acquisitions"]
        if item["acquisition_id"] == row["acquisition_id"]
    )
    normalized_binding = next(
        item
        for item in acquisition["artifact_bindings"]
        if item["kind"] == "normalized_response"
    )
    normalized_path = (
        full_audit.run_directory / normalized_binding["path"]
    )
    normalized = _read_json(normalized_path)
    observation = next(
        item
        for item in normalized["observations"]
        if item["target_id"] == row["capability_id"]
    )
    del observation["fields"][field_name]
    del observation[field_name]
    _write_json(normalized_path, normalized)
    normalized_binding.update(_identity(normalized_path))
    normalized_binding["path"] = normalized_path.relative_to(
        full_audit.run_directory
    ).as_posix()
    normalized_sha256 = acceptance.stable_sha256(normalized)
    acquisition["normalized_response_sha256"] = normalized_sha256
    for affected in report["results"]:
        if affected["acquisition_id"] != acquisition["acquisition_id"]:
            continue
        for field_result in affected["field_results"].values():
            field_result["evidence"][
                "normalized_response_sha256"
            ] = normalized_sha256
    forged = row["field_results"][field_name]
    forged["checks"].update(ALL_TRUE)
    forged["quality_score"] = 100
    forged["health_status"] = "HEALTHY"
    forged["evidence"]["observed_checks"].update(ALL_TRUE)
    forged["evidence"].update(ALL_TRUE)
    _write_json(full_audit.report_path, report)
    matrix = _read_json(full_audit.matrix_path)
    matrix["capabilities"] = report["results"]
    _write_json(full_audit.matrix_path, matrix)
    _refresh_genuine_artifact_integrity(full_audit)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_REPORT_INCOMPLETE" in errors
    assert "CAPABILITY_AUDIT_FIELD_EVIDENCE_INVALID" in errors


def test_acquisition_artifact_binding_tamper_is_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    report = _read_json(full_audit.report_path)
    binding = report["acquisitions"][0]["artifact_bindings"][0]
    bound_path = full_audit.run_directory / binding["path"]
    bound_path.write_bytes(bound_path.read_bytes() + b"\n")
    _refresh_genuine_artifact_integrity(full_audit)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_REPORT_INCOMPLETE" in errors
    assert "CAPABILITY_AUDIT_ACQUISITION_ARTIFACT_INVALID" in errors


def test_top_level_artifact_byte_tamper_is_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    full_audit.report_path.write_bytes(
        full_audit.report_path.read_bytes() + b" "
    )

    _, report, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert report is None
    assert any(
        error.startswith(
            "CAPABILITY_AUDIT_ARTIFACT_INVALID:audit-report.json"
        )
        for error in errors
    )


def test_resigned_comparison_semantic_tamper_is_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    path = full_audit.run_directory / "comparison-with-previous.json"
    comparison = _read_json(path)
    comparison["changes"].append(
        {
            "capability_id": "FORGED|dataset|metric",
            "change": "ADDED",
        }
    )
    _write_json(path, comparison)
    _refresh_genuine_artifact_integrity(full_audit)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_COMPARISON_INVALID" in errors


def test_resigned_markdown_semantic_tamper_is_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    path = full_audit.run_directory / "audit-report.md"
    path.write_bytes(path.read_bytes() + b"\nforged summary\n")
    _refresh_genuine_artifact_integrity(full_audit)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_MARKDOWN_MISMATCH" in errors


def test_resigned_audit_log_sequence_tamper_is_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    path = full_audit.run_directory / "logs" / "audit.jsonl"
    events = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    events[1]["sequence"] = 999
    path.write_text(
        "".join(
            json.dumps(
                event,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for event in events
        ),
        encoding="utf-8",
        newline="\n",
    )
    _refresh_genuine_artifact_integrity(full_audit)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_LOG_INVALID" in errors


def test_resigned_ai_evidence_semantic_tamper_is_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    report = _read_json(full_audit.report_path)
    acquisition = next(
        item
        for item in (
            *report["acquisitions"],
            *report["runtime_adapter_acquisitions"],
        )
        if any(
            binding.get("kind") == "ai_evidence"
            for binding in item["artifact_bindings"]
        )
    )
    ai_binding = next(
        item
        for item in acquisition["artifact_bindings"]
        if item["kind"] == "ai_evidence"
    )
    ai_path = full_audit.run_directory / ai_binding["path"]
    ai_payload = _read_json(ai_path)
    ai_payload["evidence"].setdefault("fields", {})[
        "FORGED|dataset|metric|actual"
    ] = {"field": "actual", "value": 999}
    ai_payload["evidence_sha256"] = acceptance.stable_sha256(
        ai_payload["evidence"]
    )
    _write_json(ai_path, ai_payload)
    ai_binding.update(_identity(ai_path))
    ai_binding["path"] = ai_path.relative_to(
        full_audit.run_directory
    ).as_posix()
    ai_binding["evidence_sha256"] = ai_payload[
        "evidence_sha256"
    ]

    metadata_binding = next(
        item
        for item in acquisition["artifact_bindings"]
        if item["kind"] == "acquisition_metadata"
    )
    metadata_path = (
        full_audit.run_directory / metadata_binding["path"]
    )
    metadata = _read_json(metadata_path)
    metadata["ai_evidence"]["evidence_sha256"] = ai_payload[
        "evidence_sha256"
    ]
    _write_json(metadata_path, metadata)
    metadata_binding.update(_identity(metadata_path))
    metadata_binding["path"] = metadata_path.relative_to(
        full_audit.run_directory
    ).as_posix()
    _write_json(full_audit.report_path, report)
    _refresh_genuine_artifact_integrity(full_audit)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_AI_EVIDENCE_INVALID" in errors


def test_incoherent_pointer_report_matrix_provenance_is_rejected(
    full_audit: OfflineAuditRun,
) -> None:
    assert full_audit.pointer_path is not None
    report = _read_json(full_audit.report_path)
    matrix = _read_json(full_audit.matrix_path)
    report["source_provenance"]["audited_runtime_sha256"] = "1" * 64
    matrix["source_provenance"]["audited_runtime_sha256"] = "2" * 64
    _write_json(full_audit.report_path, report)
    _write_json(full_audit.matrix_path, matrix)
    _refresh_genuine_artifact_integrity(full_audit)
    pointer = _read_json(full_audit.pointer_path)
    pointer["source_provenance"]["audited_runtime_sha256"] = "3" * 64
    _write_json(full_audit.pointer_path, pointer)

    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )

    assert "CAPABILITY_AUDIT_SOURCE_PROVENANCE_MISMATCH" in errors


def test_expected_code_revision_mismatch_rejects_publication(
    full_audit: OfflineAuditRun,
    monkeypatch: Any,
) -> None:
    assert full_audit.pointer_path is not None
    observed = deepcopy(full_audit.report["source_provenance"])
    expected = {
        **observed,
        "audited_runtime_sha256": "f" * 64,
        "audited_file_count": int(observed["audited_file_count"]) + 1,
    }

    class MismatchedProvenance:
        def as_dict(self) -> dict[str, Any]:
            return expected

    monkeypatch.setattr(
        acceptance,
        "provider_audit_source_provenance",
        lambda: MismatchedProvenance(),
    )
    _, _, errors = acceptance._verified_capability_audit(
        full_audit.pointer_path
    )
    assert "CAPABILITY_AUDIT_CODE_REVISION_MISMATCH" in errors

    latest_before = full_audit.pointer_path.read_bytes()
    candidate = (
        full_audit.pointer_path.parent
        / ".code-revision-mismatch.candidate.json"
    )
    candidate.write_bytes(latest_before)
    with pytest.raises(ProviderAuditPublicationRejected) as rejection:
        publish_verified_audit_candidate(
            candidate,
            full_audit.pointer_path,
            lambda proposed, canonical: (
                acceptance._verified_capability_audit(
                    proposed,
                    canonical_pointer_path=canonical,
                )
            ),
        )

    assert "CAPABILITY_AUDIT_CODE_REVISION_MISMATCH" in (
        rejection.value.errors
    )
    assert full_audit.pointer_path.read_bytes() == latest_before
    assert not candidate.exists()


def test_registry_gate_uses_summary_counters(
    monkeypatch: Any,
) -> None:
    summary = registry_summary()
    summary.update(
        {
            "provider_registry_coverage": 0,
            "unregistered_runtime_providers": 3,
            "capabilities_without_probe": 4,
        }
    )
    monkeypatch.setattr(
        acceptance,
        "registry_summary",
        lambda: summary,
    )

    gate = acceptance._capability_audit_gate(
        {},
        pointer_path=None,
    )

    assert gate["checks"]["provider_registry_coverage"] == 0
    assert gate["checks"]["unregistered_runtime_providers"] == 3
    assert gate["checks"]["capabilities_without_probe"] == 4


def _ai_pce_delivery() -> dict[str, Any]:
    provider_type = "AI_RESEARCHER_CODEX_CLI"
    return {
        "provider_accounting": [
            {
                "dataset_id": "macro_calendar",
                "acquisition_selected_source": "AI_RESEARCHER",
                "delivered_value": [
                    {
                        "metric_id": "headline_pce_yoy",
                        "consensus": 2.7,
                        "previous": 2.6,
                        "field_lineage": {
                            "consensus": {
                                "provider_type": provider_type,
                            },
                            "previous": {
                                "provider_type": provider_type,
                            },
                        },
                    }
                ],
            }
        ]
    }


def _certified_ai_row(row: dict[str, Any]) -> dict[str, Any]:
    certified = deepcopy(row)
    certified.update(
        {
            "configured": True,
            "real_adapter_invoked": True,
            "probe_dispatch_status": "REAL_ADAPTER",
        }
    )
    for field_result in certified["field_results"].values():
        field_result["health_status"] = "HEALTHY"
        field_result["checks"].update(ALL_TRUE)
    return certified


def test_ai_researcher_audit_only_cannot_certify_delivery(
    full_audit: OfflineAuditRun,
) -> None:
    pce_row = next(
        row
        for row in full_audit.report["results"]
        if row["provider_id"] == "AI_RESEARCHER"
        and row["dataset_id"] == "macro_calendar"
        and row["metric_id"] == "headline_pce_yoy"
    )
    report = {"results": [_certified_ai_row(pce_row)]}
    payload = _ai_pce_delivery()

    assert acceptance._certified_ai_fields(report) == set()
    assert (
        acceptance._ai_used_without_certification(
            payload,
            report=report,
        )
        == 2
    )


def test_ai_certification_for_one_metric_does_not_authorize_another(
    full_audit: OfflineAuditRun,
) -> None:
    pce_row = next(
        row
        for row in full_audit.report["results"]
        if row["provider_id"] == "AI_RESEARCHER"
        and row["dataset_id"] == "macro_calendar"
        and row["metric_id"] == "headline_pce_yoy"
    )
    payload = _ai_pce_delivery()
    payload["provider_accounting"][0]["delivered_value"][0][
        "metric_id"
    ] = "headline_cpi_yoy"

    assert acceptance._ai_used_without_certification(
        payload,
        report={"results": [_certified_ai_row(pce_row)]},
    ) > 0


def test_mixed_delivery_detects_ai_source_in_field_lineage() -> None:
    payload = {
        "provider_accounting": [
            {
                "dataset_id": "macro_calendar",
                "acquisition_selected_source": "MIXED",
                "delivered_value": [
                    {
                        "metric_id": "headline_pce_yoy",
                        "consensus": 2.7,
                        "field_lineage": {
                            "consensus": {
                                "source": "AI_RESEARCHER",
                            }
                        },
                    },
                    {
                        "metric_id": "headline_pce_yoy",
                        "previous": 2.6,
                        "field_lineage": {
                            "previous": {
                                "source": "AI_UNATTRIBUTED_SYNTHESIS",
                            }
                        },
                    },
                ],
            }
        ]
    }

    usages = acceptance._delivered_ai_field_usages(payload)

    assert (
        "AI_RESEARCHER",
        "macro_calendar",
        "headline_pce_yoy",
        "consensus",
    ) in usages
    assert (
        "__UNATTRIBUTED_AI__",
        "macro_calendar",
        "headline_pce_yoy",
        "previous",
    ) in usages
    assert acceptance._ai_used_without_certification(
        payload,
        report={"results": []},
    ) == 2


@pytest.mark.parametrize(
    "lineage",
    [
        {"source": "XTB", "provider_type": "AI_RESEARCHER"},
        {"source": "XTB"},
    ],
)
def test_contradictory_deterministic_and_ai_identity_is_unattributed(
    lineage: dict[str, str],
) -> None:
    payload = {
        "provider_accounting": [
            {
                "dataset_id": "macro_calendar",
                "acquisition_selected_source": "AI_RESEARCHER",
                "delivered_value": [
                    {
                        "metric_id": "headline_pce_yoy",
                        "consensus": 2.7,
                        "field_lineage": {"consensus": lineage},
                    }
                ],
            }
        ]
    }

    usages = acceptance._delivered_ai_field_usages(payload)

    assert (
        "__UNATTRIBUTED_AI__",
        "macro_calendar",
        "headline_pce_yoy",
        "consensus",
    ) in usages
    assert (
        "AI_RESEARCHER",
        "macro_calendar",
        "headline_pce_yoy",
        "consensus",
    ) not in usages
    assert len(usages) == 1
    assert acceptance._ai_used_without_certification(
        payload,
        report={"results": []},
    ) == 1


def test_canonical_repository_role_is_primary_in_live_baseline() -> None:
    compact = _compact_field_result(
        {
            "health_status": "HEALTHY",
            "quality_score": 100,
            "checks": ALL_TRUE,
            "reason_codes": [],
        },
        roles={"CANONICAL_REPOSITORY"},
        checked_at="2026-07-31T12:00:00+00:00",
    )

    assert compact["eligible_as_primary"] is True
    assert compact["eligible_as_fallback"] is False
    assert compact["recommended_role"] == "KEEP_PRIMARY"
    assert compact["recommendations"] == ["KEEP_PRIMARY"]


def test_httpx_failed_attempt_attestation_is_terminal_not_eligible() -> None:
    attestation = {
        "attempted_send_count": 1,
        "response_exchange_count": 0,
        "failure_reason_code": "PROVIDER_TRANSPORT_FAILED",
        "bounded_timeout_observed": False,
        "local_invocation_observed": False,
    }
    acquisition = {
        "capture_mode": "HTTPX",
        "capture_mode_expected": "HTTPX",
        "capture_verified": True,
        "capture_attestation": attestation,
        "capture_attestation_sha256": acceptance.stable_sha256(
            attestation
        ),
        "network_exchange_count": 0,
        "transport_status": "DOWN",
    }

    assert acceptance._capture_attestation_valid(
        acquisition,
        provider=provider_by_id("FRED"),
    ) is True
    acquisition["transport_status"] = "OK"
    assert acceptance._capture_attestation_valid(
        acquisition,
        provider=provider_by_id("FRED"),
    ) is False


def test_fallback_validator_does_not_reuse_wildcard_repository_result(
    full_audit: OfflineAuditRun,
) -> None:
    policy = DATASET_SOURCE_POLICIES[0]
    results = deepcopy(full_audit.report["results"])
    repository_rows = [
        row
        for row in results
        if row["provider_id"] == policy.canonical_repository
        and row["dataset_id"] == policy.dataset_id
    ]
    assert repository_rows
    for row in repository_rows:
        row["dataset_id"] = "*"

    assert acceptance._fallback_chain_accounting_valid(
        full_audit.report["fallback_chain_accounting"],
        results=results,
    ) is False


def test_fallback_validator_recomputes_strategy_aware_phase_accounting(
    full_audit: OfflineAuditRun,
) -> None:
    observed = deepcopy(full_audit.report["fallback_chain_accounting"])
    row = observed["rows"][0]
    row["accounting"][0]["phase_id"] = "forged:database-phase"

    assert acceptance._fallback_chain_accounting_valid(
        observed,
        results=full_audit.report["results"],
    ) is False


def test_cli_imports_verified_full_audit_to_compact_baseline(
    full_audit: OfflineAuditRun,
    tmp_path: Path,
) -> None:
    assert full_audit.pointer_path is not None
    destination = tmp_path / "provider-capability-last-live.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/generate_provider_capability_matrix.py",
            "--import-live-pointer",
            str(full_audit.pointer_path),
            "--last-live-output",
            str(destination),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    baseline_bytes = destination.read_bytes()
    baseline_sha256 = hashlib.sha256(baseline_bytes).hexdigest()
    assert f"exact baseline file SHA-256: {baseline_sha256}" in completed.stdout
    assert baseline_bytes.endswith(b"\n")
    assert b"\r\n" not in baseline_bytes
    baseline = _read_json(destination)
    assert baseline_bytes == baseline_file_bytes(baseline)
    assert baseline_sha256 == baseline_file_sha256(baseline)
    assert baseline["contract"] == (
        "ProviderCapabilityLastLiveBaseline"
    )
    assert baseline["schema_version"] == "1.2"
    assert baseline["run_id"] == RUN_ID
    assert baseline["audit_status"] == "COMPLETED"
    assert baseline["system_health"] == full_audit.report["system_health"]
    assert baseline["registry_sha256"]
    assert len(baseline["source"]["audit_report_sha256"]) == 64
    assert len(
        baseline["source"]["extracted_capabilities_sha256"]
    ) == 64
    assert baseline["source"]["pointer_file"] == (
        "provider-capability-audit-latest.json"
    )
    assert baseline["attestation"]["algorithm"] == "SHA-256"
    assert len(baseline["attestation"]["content_sha256"]) == 64
    assert baseline["attestation"]["capability_rows"] == len(
        baseline["capabilities"]
    )
    assert baseline["attestation"]["field_rows"] == sum(
        len(row["field_results"]) for row in baseline["capabilities"]
    )
    assert len(baseline["capabilities"]) == registry_summary()[
        "capabilities_registered"
    ]
    assert [
        row["capability_id"] for row in baseline["capabilities"]
    ] == sorted(
        row["capability_id"] for row in baseline["capabilities"]
    )
    assert set(baseline["capabilities"][0]) == {
        "capability_id",
        "provider_id",
        "dataset_id",
        "metric_id",
        "health_status",
        "eligible_as_primary",
        "eligible_as_fallback",
        "recommendations",
        "reason_codes",
        "field_results",
    }
    assert set(baseline["capabilities"][0]["field_results"]) == set(
        full_audit.report["results"][0]["supported_fields"]
    )
    assert all(
        {
            "health_status",
            "quality_score",
            "eligible_as_primary",
            "eligible_as_fallback",
            "recommended_role",
            "recommendations",
            "reason_codes",
            "checked_at",
        }
        == set(field_result)
        for field_result in baseline["capabilities"][0][
            "field_results"
        ].values()
    )
    assert not list(
        destination.parent.glob(f".{destination.name}.*.tmp")
    )


def test_live_baseline_import_detects_source_toctou_and_restores_previous(
    full_audit: OfflineAuditRun,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert full_audit.pointer_path is not None
    destination = tmp_path / "existing-live-baseline.json"
    previous_bytes = b'{"previous":"baseline"}\n'
    destination.write_bytes(previous_bytes)
    original_report_bytes = full_audit.report_path.read_bytes()
    verified_result = (
        _read_json(full_audit.pointer_path),
        deepcopy(full_audit.report),
        [],
    )

    def mutate_after_verification(pointer_path: Path):
        del pointer_path
        full_audit.report_path.write_bytes(original_report_bytes + b" ")
        return verified_result

    monkeypatch.setattr(
        baseline_import,
        "_verified_capability_audit",
        mutate_after_verification,
    )
    with pytest.raises(
        ProviderCapabilityBaselineImportError,
        match="source changed during baseline import:verification",
    ):
        baseline_import.import_verified_live_audit(
            full_audit.pointer_path,
            destination=destination,
        )
    assert destination.read_bytes() == previous_bytes
    full_audit.report_path.write_bytes(original_report_bytes)
    monkeypatch.setattr(
        baseline_import,
        "_verified_capability_audit",
        lambda _pointer_path: verified_result,
    )

    original_snapshot = baseline_import._audit_source_snapshot
    snapshot_calls = 0

    def mutate_after_destination_replace(pointer_path: Path):
        nonlocal snapshot_calls
        snapshot_calls += 1
        if snapshot_calls == 4:
            full_audit.report_path.write_bytes(original_report_bytes + b" ")
        return original_snapshot(pointer_path)

    monkeypatch.setattr(
        baseline_import,
        "_audit_source_snapshot",
        mutate_after_destination_replace,
    )
    with pytest.raises(
        ProviderCapabilityBaselineImportError,
        match="source changed during baseline import:committed_write",
    ):
        baseline_import.import_verified_live_audit(
            full_audit.pointer_path,
            destination=destination,
        )
    assert snapshot_calls == 4
    assert destination.read_bytes() == previous_bytes
    assert not list(destination.parent.glob(f".{destination.name}.*.tmp"))
    full_audit.report_path.write_bytes(original_report_bytes)


def test_matrix_cli_prints_hash_from_derived_bytes_without_destination_reread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = {"run_id": "derived-hash-only"}
    destination = tmp_path / "replaceable-baseline.json"
    replacement_bytes = b'{"replacement":true}\n'

    def fake_import(
        pointer_path: Path,
        *,
        destination: Path,
    ) -> dict[str, Any]:
        del pointer_path
        destination.write_bytes(replacement_bytes)
        return baseline

    monkeypatch.setattr(
        matrix_generator,
        "import_verified_live_audit",
        fake_import,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_provider_capability_matrix.py",
            "--import-live-pointer",
            str(tmp_path / "pointer.json"),
            "--last-live-output",
            str(destination),
        ],
    )

    assert matrix_generator.main() == 0
    output = capsys.readouterr().out
    derived_sha256 = baseline_file_sha256(baseline)
    replacement_sha256 = hashlib.sha256(replacement_bytes).hexdigest()
    assert f"exact baseline file SHA-256: {derived_sha256}" in output
    assert replacement_sha256 not in output


def test_live_baseline_import_rejects_tampered_report(
    full_audit: OfflineAuditRun,
    tmp_path: Path,
) -> None:
    assert full_audit.pointer_path is not None
    full_audit.report_path.write_bytes(
        full_audit.report_path.read_bytes() + b" "
    )
    destination = tmp_path / "must-not-exist.json"

    with pytest.raises(
        ProviderCapabilityBaselineImportError,
        match="ARTIFACT_INVALID",
    ):
        import_verified_live_audit(
            full_audit.pointer_path,
            destination=destination,
        )

    assert not destination.exists()


def test_filtered_audit_cannot_be_imported_as_live_baseline(
    tmp_path: Path,
) -> None:
    filtered = _run_offline_audit(
        tmp_path,
        filters=AuditFilters.from_values(providers=("FRED",)),
        run_id="filtered-baseline",
    )
    assert filtered.pointer_path is None
    pointer_path = (
        tmp_path / "data" / "provider-capability-audit-latest.json"
    )

    with pytest.raises(
        ProviderCapabilityBaselineImportError,
        match="POINTER_UNREADABLE",
    ):
        import_verified_live_audit(
            pointer_path,
            destination=tmp_path / "must-not-exist.json",
        )


def test_live_baseline_import_rejects_registry_capability_mismatch(
    full_audit: OfflineAuditRun,
    tmp_path: Path,
) -> None:
    assert full_audit.pointer_path is not None

    def mismatch(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows[0]["metric_id"] = "mismatched_metric"
        return rows

    _mutate_results(full_audit, mismatch)

    with pytest.raises(
        ProviderCapabilityBaselineImportError,
        match="REPORT_INCOMPLETE",
    ):
        import_verified_live_audit(
            full_audit.pointer_path,
            destination=tmp_path / "must-not-exist.json",
        )
