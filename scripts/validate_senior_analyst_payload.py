from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.data_freshness_service import parse_datetime  # noqa: E402
from app.core.senior_analyst_policy import (  # noqa: E402
    MAX_SENIOR_ANALYST_PAYLOAD_BYTES,
)
from app.services.provider_capability_audit import (  # noqa: E402
    AuditFilters,
    AuditStatus,
    CORRECTNESS_CHECKS,
    TERMINAL_HEALTH_STATUSES,
    _recomputed_acquisition_request_key,
    _runtime_adapter_coverage,
    _runtime_adapter_quality,
    determine_system_health,
    normalized_field_observation,
    quality_score_formula,
    select_capability_targets,
    stable_sha256,
    validate_capability_result_derivations,
    verify_policy_fallback_chains,
)
from app.services.provider_audit_provenance import (  # noqa: E402
    is_git_commit_sha,
    provider_audit_source_provenance,
)
from app.services.provider_audit_artifacts import (  # noqa: E402
    _canonical_json_bytes,
    _comparison_payload,
    _load_previous_matrix,
    _markdown_report,
)
from app.services.provider_capability_registry import (  # noqa: E402
    DATASET_SOURCE_POLICIES,
    PROVIDER_REGISTRY,
    provider_by_id,
    registry_summary,
)
from app.services.senior_analyst_projection_v1 import (  # noqa: E402
    _INVALID_DELIVERY_REFERENCE,
    _resolve_accounting_delivery,
    validate_senior_analyst_payload_v1,
)

_LOCAL_CAPTURE_PROVIDER_TYPES = frozenset(
    {"MANUAL_FILE", "RECONCILIATION", "REPOSITORY", "TRANSFORMATION"}
)
_INVALID_AI_DELIVERY_REFERENCE = object()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the exact HTTP bytes received by Senior Analyst."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--headers", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--capability-audit-pointer", type=Path)
    parser.add_argument("--request-started-at")
    parser.add_argument("--require-live", action="store_true")
    parser.add_argument("--process-cleanup-ok", action="store_true")
    args = parser.parse_args()

    body = args.input.read_bytes()
    payload = json.loads(body.decode("utf-8"))
    now = datetime.now(UTC)
    result = validate_senior_analyst_payload_v1(
        payload,
        now=now,
        require_recent_response=args.require_live,
        exact_body_size_bytes=len(body),
    )
    headers_payload = _read_headers(args.headers)
    content_length = _content_length(headers_payload)
    content_length_matches = _content_length_matches_exact_body(
        headers_payload,
        exact_body_size_bytes=len(body),
    )
    http_status_ok = bool(
        headers_payload
        and int(headers_payload.get("status_code") or 0) == 200
    )
    started = parse_datetime(args.request_started_at)
    generated = parse_datetime(payload.get("generated_at"))
    same_execution = bool(
        started
        and generated
        and generated >= started
        and generated <= now.replace(microsecond=999999)
    )
    if args.require_live:
        result["checks"]["response_generated_recently"] = bool(
            result["checks"]["response_generated_recently"] and same_execution
        )
    result["checks"]["process_cleanup_ok"] = args.process_cleanup_ok
    result["checks"]["content_length_matches_body"] = (
        content_length_matches
        if args.require_live
        else content_length_matches
        if args.headers
        else True
    )
    result["checks"]["http_status_ok"] = (
        http_status_ok
        if args.require_live
        else http_status_ok
        if args.headers
        else True
    )
    audit_pointer = args.capability_audit_pointer
    if args.require_live and audit_pointer is None:
        audit_pointer = (
            ROOT / "data" / "provider-capability-audit-latest.json"
        )
    audit = _capability_audit_gate(
        payload,
        pointer_path=audit_pointer,
    )
    result["checks"].update(audit["checks"])
    required_zero = (
        "expired_values_delivered",
        "available_without_substantive_value",
        "selected_value_presence_mismatches",
        "readiness_section_classification_mismatches",
        "stale_values_presented_as_current",
        "invalid_temporal_mappings",
        "semantic_mapping_errors",
        "calendar_exact_duplicates",
        "past_due_awaiting_actual",
        "post_release_pre_fomc_probabilities",
        "contradictory_nasdaq_drivers",
        "expired_current_news",
        "unexplained_omissions",
        "required_dataset_omissions_without_reason",
        "duplicate_large_collections",
        "unregistered_runtime_providers",
        "capabilities_without_probe",
        "AI_used_without_certification",
    )
    capability_audit_required = bool(
        args.require_live or args.capability_audit_pointer is not None
    )
    passed = (
        result["checks"]["response_generated_recently"]
        and all(result["checks"][key] == 0 for key in required_zero)
        and result["checks"]["provider_accounting_valid"] is True
        and result["checks"]["provider_accounting_rows"] == 25
        and result["checks"]["process_cleanup_ok"] is True
        and result["checks"]["payload_size_within_budget"] is True
        and result["checks"]["content_length_matches_body"] is True
        and result["checks"]["http_status_ok"] is True
        and result["checks"]["provider_registry_coverage"] == 100
        and result["checks"]["capability_audit_accounting"] == 100
        and result["checks"]["capability_audit_pointer_valid"] is True
    )
    result["status"] = (
        "PASS"
        if args.require_live and passed
        else "PASS_OFFLINE"
        if not args.require_live
        and all(result["checks"][key] == 0 for key in required_zero)
        and result["checks"]["payload_size_within_budget"] is True
        and result["checks"]["content_length_matches_body"] is True
        and result["checks"]["http_status_ok"] is True
        and result["checks"]["provider_registry_coverage"] == 100
        and (
            not capability_audit_required
            or (
                result["checks"]["capability_audit_accounting"] == 100
                and result["checks"][
                    "capability_audit_pointer_valid"
                ]
                is True
            )
        )
        else "FAIL"
    )
    result.update(
        {
            "validated_file": str(args.input.resolve()),
            "validated_exact_http_body": True,
            "body_size_bytes": len(body),
            "max_body_size_bytes": MAX_SENIOR_ANALYST_PAYLOAD_BYTES,
            "body_sha256": hashlib.sha256(body).hexdigest(),
            "content_length": content_length,
            "headers_file": (
                str(args.headers.resolve())
                if args.headers and args.headers.exists()
                else None
            ),
            "request_started_at": (
                started.isoformat() if started else args.request_started_at
            ),
            "response_generated_at": (
                generated.isoformat() if generated else payload.get("generated_at")
            ),
            "same_execution": same_execution,
            "live_acceptance_evaluated": args.require_live,
            "capability_audit_pointer": (
                str(audit_pointer.resolve())
                if audit_pointer is not None
                else None
            ),
            "capability_audit": audit["metadata"],
        }
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result["status"] in {"PASS", "PASS_OFFLINE"} else 1


def _read_headers(path: Path | None) -> dict[str, object]:
    if path is None or not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _content_length(headers: dict[str, object]) -> int | None:
    content_headers = headers.get("content_headers")
    if not isinstance(content_headers, dict):
        return None
    raw = next(
        (
            value
            for key, value in content_headers.items()
            if str(key).lower() == "content-length"
        ),
        None,
    )
    if isinstance(raw, list):
        raw = raw[0] if len(raw) == 1 else None
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _content_length_matches_exact_body(
    headers: dict[str, object],
    *,
    exact_body_size_bytes: int,
) -> bool:
    content_length = _content_length(headers)
    return bool(
        type(exact_body_size_bytes) is int
        and exact_body_size_bytes >= 0
        and content_length is not None
        and content_length == exact_body_size_bytes
    )


def _capability_audit_gate(
    payload: dict[str, Any],
    *,
    pointer_path: Path | None,
) -> dict[str, Any]:
    summary = registry_summary()
    unregistered_runtime_providers = _summary_count(
        summary,
        "unregistered_runtime_providers",
    )
    capabilities_without_probe = _summary_count(
        summary,
        "capabilities_without_probe",
    )
    report: dict[str, Any] | None = None
    pointer: dict[str, Any] | None = None
    errors: list[str] = []
    if pointer_path is not None:
        pointer, report, errors = _verified_capability_audit(
            pointer_path
        )
    pointer_valid = bool(pointer_path is not None and not errors)
    accounting = 0
    if report is not None and pointer_valid:
        registered = int(
            summary.get("capabilities_registered") or 0
        )
        tested = int(report.get("capabilities_tested") or 0)
        accounting = (
            round(100 * tested / registered)
            if registered > 0
            else 0
        )
        accounting = min(accounting, 100)
    ai_without_certification = _ai_used_without_certification(
        payload,
        report=report if pointer_valid else None,
    )
    return {
        "checks": {
            "provider_registry_coverage": int(
                summary.get("provider_registry_coverage") or 0
            ),
            "capability_audit_accounting": accounting,
            "unregistered_runtime_providers": (
                unregistered_runtime_providers
            ),
            "capabilities_without_probe": capabilities_without_probe,
            "AI_used_without_certification": ai_without_certification,
            "capability_audit_pointer_valid": pointer_valid,
        },
        "metadata": {
            "run_id": pointer.get("run_id") if pointer else None,
            "audit_status": (
                report.get("audit_status") if report else None
            ),
            "system_health": (
                report.get("system_health") if report else None
            ),
            "registry_sha256": (
                report.get("registry_sha256") if report else None
            ),
            "providers_tested": (
                report.get("providers_tested") if report else 0
            ),
            "capabilities_tested": (
                report.get("capabilities_tested") if report else 0
            ),
            "verification_errors": errors,
        },
    }


def _verified_capability_audit(
    pointer_path: Path,
    *,
    canonical_pointer_path: Path | None = None,
) -> tuple[
    dict[str, Any] | None,
    dict[str, Any] | None,
    list[str],
]:
    errors: list[str] = []
    pointer = _read_json_object(pointer_path)
    if pointer is None:
        return None, None, ["CAPABILITY_AUDIT_POINTER_UNREADABLE"]
    resolved_pointer_path = pointer_path.resolve()
    canonical_pointer = (
        canonical_pointer_path or pointer_path
    ).resolve()
    if (
        canonical_pointer.name != "provider-capability-audit-latest.json"
        or canonical_pointer.parent.name != "data"
        or resolved_pointer_path.parent != canonical_pointer.parent
    ):
        errors.append("CAPABILITY_AUDIT_POINTER_LOCATION_INVALID")
    if (
        pointer.get("schema_version")
        != "provider-capability-audit-latest-v1"
        or pointer.get("audit_status") != "COMPLETED"
        or not pointer.get("run_id")
    ):
        errors.append("CAPABILITY_AUDIT_POINTER_INVALID")
    run_id = str(pointer.get("run_id") or "")
    run_directory = _resolved_artifact_path(
        pointer.get("run_directory"),
        base=canonical_pointer.parent,
    )
    audit_root = (
        canonical_pointer.parent / "provider-capability-audit"
    ).resolve()
    expected_run_directory = (audit_root / run_id).resolve()
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id)
        or run_id in {".", ".."}
        or not expected_run_directory.is_relative_to(audit_root)
        or run_directory != expected_run_directory
    ):
        errors.append(
            "CAPABILITY_AUDIT_RUN_DIRECTORY_OUTSIDE_EXPECTED_ROOT"
        )
    if run_directory is None or not run_directory.is_dir():
        errors.append("CAPABILITY_AUDIT_RUN_DIRECTORY_INVALID")
        return pointer, None, errors
    if errors:
        return pointer, None, sorted(set(errors))
    artifacts = pointer.get("artifacts")
    if not isinstance(artifacts, dict):
        return pointer, None, [
            *errors,
            "CAPABILITY_AUDIT_ARTIFACTS_INVALID",
        ]
    required_artifacts = {
        "audit-report.json",
        "audit-report.md",
        "capability-matrix.json",
        "comparison-with-previous.json",
        "checksums.json",
    }
    if required_artifacts != set(artifacts):
        errors.append("CAPABILITY_AUDIT_REQUIRED_ARTIFACT_MISSING")
    resolved: dict[str, Path] = {}
    for name in required_artifacts:
        identity = artifacts.get(name)
        path = (
            _resolved_artifact_path(
                identity.get("path"),
                base=canonical_pointer.parent,
            )
            if isinstance(identity, dict)
            else None
        )
        if (
            path is None
            or not path.is_relative_to(run_directory)
            or not _identity_matches(path, identity)
        ):
            errors.append(
                f"CAPABILITY_AUDIT_ARTIFACT_INVALID:{name}"
            )
            continue
        resolved[name] = path
    if required_artifacts != set(resolved):
        return pointer, None, errors

    checksummed: set[str] = set()
    checksums = _read_json_object(resolved["checksums.json"])
    if (
        checksums is None
        or checksums.get("schema_version")
        != "provider-audit-checksums-v1"
        or checksums.get("run_id") != pointer.get("run_id")
        or checksums.get("algorithm") != "SHA-256"
        or checksums.get("self_excluded") is not True
        or not isinstance(checksums.get("files"), list)
    ):
        errors.append("CAPABILITY_AUDIT_CHECKSUM_MANIFEST_INVALID")
    else:
        for item in checksums["files"]:
            if not isinstance(item, dict):
                errors.append("CAPABILITY_AUDIT_CHECKSUM_ROW_INVALID")
                continue
            relative = str(item.get("path") or "")
            path = (run_directory / relative).resolve()
            if (
                not relative
                or not path.is_relative_to(run_directory)
                or not _identity_matches(path, item)
            ):
                errors.append(
                    "CAPABILITY_AUDIT_CHECKSUM_MISMATCH:"
                    f"{relative or '<missing>'}"
                )
                continue
            normalized_relative = relative.replace("\\", "/")
            if normalized_relative in checksummed:
                errors.append(
                    "CAPABILITY_AUDIT_CHECKSUM_DUPLICATE:"
                    f"{normalized_relative}"
                )
            checksummed.add(normalized_relative)
        if not {
            "audit-report.json",
            "audit-report.md",
            "capability-matrix.json",
            "comparison-with-previous.json",
        } <= checksummed:
            errors.append(
                "CAPABILITY_AUDIT_CORE_ARTIFACT_NOT_CHECKSUMMED"
            )
        actual_files = {
            path.relative_to(run_directory).as_posix()
            for path in run_directory.rglob("*")
            if path.is_file() and path.name != "checksums.json"
        }
        if checksummed != actual_files:
            errors.append(
                "CAPABILITY_AUDIT_CHECKSUM_TREE_COVERAGE_MISMATCH"
            )

    report = _read_json_object(resolved["audit-report.json"])
    matrix = _read_json_object(resolved["capability-matrix.json"])
    if report is None or matrix is None:
        errors.append("CAPABILITY_AUDIT_REPORT_UNREADABLE")
        return pointer, report, errors
    registry_digest = stable_sha256(
        [asdict(provider) for provider in PROVIDER_REGISTRY]
    )
    observed_provenance = report.get("source_provenance")
    if (
        not isinstance(observed_provenance, dict)
        or pointer.get("source_provenance") != observed_provenance
        or matrix.get("source_provenance") != observed_provenance
    ):
        errors.append("CAPABILITY_AUDIT_SOURCE_PROVENANCE_MISMATCH")
    else:
        expected_provenance = provider_audit_source_provenance().as_dict()
        if (
            set(observed_provenance) != set(expected_provenance)
            or not is_git_commit_sha(
                observed_provenance.get("git_commit_sha")
            )
            or observed_provenance.get("audited_runtime_sha256")
            != expected_provenance["audited_runtime_sha256"]
            or observed_provenance.get("audited_file_count")
            != expected_provenance["audited_file_count"]
        ):
            errors.append("CAPABILITY_AUDIT_CODE_REVISION_MISMATCH")
    summary = registry_summary()
    results = report.get("results")
    capability_rows = (
        matrix.get("capabilities")
        if isinstance(matrix.get("capabilities"), list)
        else []
    )
    runtime_adapter_results = report.get("runtime_adapter_results")
    runtime_leaf_findings = report.get("runtime_leaf_findings")
    matrix_runtime_adapter_results = matrix.get(
        "runtime_adapter_capabilities"
    )
    matrix_runtime_leaf_findings = matrix.get(
        "runtime_leaf_findings"
    )
    comparison = _read_json_object(
        resolved["comparison-with-previous.json"]
    )
    semantic_artifact_errors = [
        *_markdown_artifact_errors(
            report,
            path=resolved["audit-report.md"],
        ),
        *_comparison_artifact_errors(
            comparison,
            path=resolved["comparison-with-previous.json"],
            matrix=matrix,
            matrix_path=resolved["capability-matrix.json"],
            run_id=run_id,
            audit_root=audit_root,
            pointer_path=resolved_pointer_path,
            canonical_pointer_path=canonical_pointer,
        ),
        *_audit_log_errors(
            report,
            run_directory=run_directory,
            checksummed=checksummed,
        ),
    ]
    errors.extend(semantic_artifact_errors)
    filters = report.get("filters")
    complete_filters = bool(
        isinstance(filters, dict)
        and filters.get("providers") == []
        and filters.get("datasets") == []
        and filters.get("metrics") == []
        and filters.get("include_ai") is True
    )
    capability_rows_complete = _capability_rows_match_registry(
        results
    )
    acquisition_errors = _capability_acquisitions_errors(
        report,
        run_directory=run_directory,
        checksummed=checksummed,
    )
    runtime_adapter_coverage_valid = _runtime_adapter_coverage_valid(
        report
    )
    fallback_accounting_valid = _fallback_chain_accounting_valid(
        report.get("fallback_chain_accounting"),
        results=results,
    )
    report_summary_valid = _capability_report_summary_valid(report)
    if not (
        report.get("schema_version")
        == "provider-capability-audit-v1"
        and report.get("run_id") == pointer.get("run_id")
        and report.get("audit_status") == "COMPLETED"
        and report.get("terminal_rows_complete") is True
        and report.get("internal_errors") == []
        and report.get("registry_validation_errors") == []
        and complete_filters
        and report.get("full_audit_scope") is True
        and pointer.get("full_audit_scope") is True
        and report.get("unsupported_isolated_probes") == 0
        and report.get("configured_dispatches_missing") == []
        and report.get("acquisition_artifact_bindings_complete")
        is True
        and not acquisition_errors
        and runtime_adapter_coverage_valid
        and fallback_accounting_valid
        and report_summary_valid
        and report.get("registry_sha256") == registry_digest
        and pointer.get("registry_sha256") == registry_digest
        and report.get("providers_registered")
        == summary["providers_registered"]
        and report.get("providers_tested")
        == summary["providers_registered"]
        and report.get("capabilities_registered")
        == summary["capabilities_registered"]
        and report.get("capabilities_tested")
        == summary["capabilities_registered"]
        and isinstance(results, list)
        and len(results) == summary["capabilities_registered"]
        and capability_rows_complete
        and _runtime_leaf_findings_valid(runtime_leaf_findings)
    ):
        errors.append("CAPABILITY_AUDIT_REPORT_INCOMPLETE")
    errors.extend(acquisition_errors)
    if not runtime_adapter_coverage_valid:
        errors.append("CAPABILITY_AUDIT_RUNTIME_ADAPTER_COVERAGE_INVALID")
    if not fallback_accounting_valid:
        errors.append(
            "CAPABILITY_AUDIT_FALLBACK_ACCOUNTING_INVALID"
        )
    if not report_summary_valid:
        errors.append("CAPABILITY_AUDIT_SUMMARY_INVALID")
    if not (
        matrix.get("schema_version")
        == "provider-capability-matrix-v1"
        and matrix.get("run_id") == pointer.get("run_id")
        and matrix.get("audit_status") == "COMPLETED"
        and matrix.get("registry_sha256") == registry_digest
        and capability_rows == results
        and matrix_runtime_adapter_results
        == runtime_adapter_results
        and matrix_runtime_leaf_findings == runtime_leaf_findings
    ):
        errors.append("CAPABILITY_AUDIT_MATRIX_MISMATCH")
    if (
        pointer.get("providers_tested")
        != report.get("providers_tested")
        or pointer.get("capabilities_tested")
        != report.get("capabilities_tested")
        or pointer.get("system_health")
        != report.get("system_health")
        or pointer.get("completed_at") != report.get("completed_at")
        or pointer.get("audit_status") != report.get("audit_status")
        or pointer.get("full_audit_scope") is not True
    ):
        errors.append("CAPABILITY_AUDIT_POINTER_REPORT_MISMATCH")
    return pointer, report, sorted(set(errors))


def _markdown_artifact_errors(
    report: dict[str, Any],
    *,
    path: Path,
) -> list[str]:
    try:
        observed = path.read_bytes()
    except OSError:
        return ["CAPABILITY_AUDIT_MARKDOWN_MISMATCH"]
    expected = _markdown_report(report).encode("utf-8")
    return (
        []
        if observed == expected
        else ["CAPABILITY_AUDIT_MARKDOWN_MISMATCH"]
    )


def _comparison_artifact_errors(
    comparison: dict[str, Any] | None,
    *,
    path: Path,
    matrix: dict[str, Any],
    matrix_path: Path,
    run_id: str,
    audit_root: Path,
    pointer_path: Path,
    canonical_pointer_path: Path,
) -> list[str]:
    if comparison is None:
        return ["CAPABILITY_AUDIT_COMPARISON_INVALID"]
    try:
        observed_bytes = path.read_bytes()
        matrix_bytes = matrix_path.read_bytes()
    except OSError:
        return ["CAPABILITY_AUDIT_COMPARISON_INVALID"]
    if observed_bytes != _canonical_json_bytes(comparison):
        return ["CAPABILITY_AUDIT_COMPARISON_INVALID"]

    previous: tuple[str, Mapping[str, Any], Mapping[str, Any]] | None
    if pointer_path != canonical_pointer_path:
        previous = _load_previous_matrix(
            canonical_pointer_path,
            audit_root,
        )
    elif comparison.get("comparison_status") == "COMPARED":
        previous_run_id = str(
            comparison.get("previous_run_id") or ""
        )
        previous_path = (
            audit_root / previous_run_id / "capability-matrix.json"
        ).resolve()
        if (
            not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]*",
                previous_run_id,
            )
            or previous_run_id in {".", "..", run_id}
            or not previous_path.is_relative_to(audit_root)
        ):
            return ["CAPABILITY_AUDIT_COMPARISON_INVALID"]
        previous_matrix = _read_json_object(previous_path)
        if (
            previous_matrix is None
            or previous_matrix.get("schema_version")
            != "provider-capability-matrix-v1"
            or previous_matrix.get("run_id") != previous_run_id
            or previous_matrix.get("audit_status") != "COMPLETED"
        ):
            return ["CAPABILITY_AUDIT_COMPARISON_INVALID"]
        previous_bytes = previous_path.read_bytes()
        previous = (
            previous_run_id,
            previous_matrix,
            {
                "size_bytes": len(previous_bytes),
                "sha256": hashlib.sha256(previous_bytes).hexdigest(),
            },
        )
    else:
        previous = None

    try:
        expected = (
            _comparison_payload(
                run_id=run_id,
                current_matrix=matrix,
            )
            if previous is None
            else _comparison_payload(
                run_id=run_id,
                current_matrix=matrix,
                previous_run_id=previous[0],
                previous_matrix=previous[1],
                previous_matrix_identity=previous[2],
            )
        )
    except (KeyError, TypeError, ValueError):
        return ["CAPABILITY_AUDIT_COMPARISON_INVALID"]
    current_identity_matches = bool(
        comparison.get("current_matrix_size_bytes")
        == len(matrix_bytes)
        and comparison.get("current_matrix_sha256")
        == hashlib.sha256(matrix_bytes).hexdigest()
    )
    return (
        []
        if comparison == expected and current_identity_matches
        else ["CAPABILITY_AUDIT_COMPARISON_INVALID"]
    )


def _audit_log_errors(
    report: dict[str, Any],
    *,
    run_directory: Path,
    checksummed: set[str],
) -> list[str]:
    relative = "logs/audit.jsonl"
    path = (run_directory / relative).resolve()
    if relative not in checksummed or not path.is_relative_to(
        run_directory
    ):
        return ["CAPABILITY_AUDIT_LOG_INVALID"]
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError):
        return ["CAPABILITY_AUDIT_LOG_INVALID"]
    if not raw.endswith(b"\n") or not text:
        return ["CAPABILITY_AUDIT_LOG_INVALID"]
    lines = text.splitlines()
    if not lines or any(not line for line in lines):
        return ["CAPABILITY_AUDIT_LOG_INVALID"]
    try:
        events = [json.loads(line) for line in lines]
    except json.JSONDecodeError:
        return ["CAPABILITY_AUDIT_LOG_INVALID"]
    if any(not isinstance(item, dict) for item in events):
        return ["CAPABILITY_AUDIT_LOG_INVALID"]
    canonical = b"".join(
        json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
        for item in events
    )
    if raw != canonical:
        return ["CAPABILITY_AUDIT_LOG_INVALID"]

    acquisitions = report.get("acquisitions")
    runtime_acquisitions = report.get("runtime_adapter_acquisitions")
    if not isinstance(acquisitions, list) or not isinstance(
        runtime_acquisitions,
        list,
    ):
        return ["CAPABILITY_AUDIT_LOG_INVALID"]
    ordered = [*acquisitions, *runtime_acquisitions]
    expected: list[dict[str, Any]] = [
        {
            "schema_version": "provider-audit-log-event-v1",
            "run_id": report.get("run_id"),
            "sequence": 1,
            "event": "AUDIT_ARTIFACT_STAGING_CREATED",
        }
    ]
    for sequence, acquisition in enumerate(ordered, start=2):
        if not isinstance(acquisition, dict):
            return ["CAPABILITY_AUDIT_LOG_INVALID"]
        raw_bindings = [
            item
            for item in acquisition.get("artifact_bindings") or []
            if isinstance(item, dict)
            and item.get("kind") == "response_body"
        ]
        raw_binding = raw_bindings[0] if len(raw_bindings) == 1 else {}
        expected.append(
            {
                "schema_version": "provider-audit-log-event-v1",
                "run_id": report.get("run_id"),
                "sequence": sequence,
                "event": "PROVIDER_ACQUISITION_RECORDED",
                "acquisition_kind": acquisition.get(
                    "acquisition_kind"
                ),
                "acquisition_id": acquisition.get("acquisition_id"),
                "request_key": acquisition.get("request_key"),
                "provider_id": acquisition.get("provider_id"),
                "target_ids": acquisition.get("target_ids"),
                "transport_status": acquisition.get(
                    "transport_status"
                ),
                "http_status": acquisition.get("http_status"),
                "exact_bytes_saved": raw_binding.get(
                    "exact_bytes_saved"
                ),
                "redaction_applied": raw_binding.get(
                    "redaction_applied",
                    False,
                ),
            }
        )
    expected.append(
        {
            "schema_version": "provider-audit-log-event-v1",
            "run_id": report.get("run_id"),
            "sequence": len(expected) + 1,
            "event": "AUDIT_ARTIFACT_FINALIZATION_STARTED",
            "audit_status": report.get("audit_status"),
        }
    )
    return (
        []
        if events == expected
        else ["CAPABILITY_AUDIT_LOG_INVALID"]
    )


def _ai_used_without_certification(
    payload: dict[str, Any],
    *,
    report: dict[str, Any] | None,
) -> int:
    certified = _certified_ai_fields(report)
    usages = _delivered_ai_field_usages(payload)
    return len(usages - certified)


def _certified_ai_fields(
    report: dict[str, Any] | None,
) -> set[tuple[str, str, str, str]]:
    ai_capabilities = {
        (
            provider.provider_id,
            capability.dataset_id,
            capability.metric_id,
        ): capability
        for provider in PROVIDER_REGISTRY
        if provider.provider_type == "AI"
        for capability in provider.capabilities
        if capability.ai_eligible
    }
    certified: set[tuple[str, str, str, str]] = set()
    for row in (report or {}).get("results") or []:
        if not isinstance(row, dict):
            continue
        identity = (
            str(row.get("provider_id") or ""),
            str(row.get("dataset_id") or ""),
            str(row.get("metric_id") or ""),
        )
        capability = ai_capabilities.get(identity)
        field_results = row.get("field_results")
        if (
            capability is None
            or not isinstance(field_results, dict)
            or row.get("configured") is not True
            or row.get("real_adapter_invoked") is not True
            or row.get("probe_dispatch_status") != "REAL_ADAPTER"
        ):
            continue
        for field in capability.supported_fields:
            field_result = field_results.get(field)
            checks = (
                field_result.get("checks")
                if isinstance(field_result, dict)
                else None
            )
            if (
                isinstance(field_result, dict)
                and field_result.get("health_status")
                in {"HEALTHY", "DEGRADED"}
                and isinstance(checks, dict)
                and all(
                    checks.get(check) is True
                    for check in CORRECTNESS_CHECKS
                )
            ):
                certified.add((*identity, field))
    return certified


def _delivered_ai_field_usages(
    payload: dict[str, Any],
) -> set[tuple[str, str, str, str]]:
    ai_provider_ids = {
        provider.provider_id
        for provider in PROVIDER_REGISTRY
        if provider.provider_type == "AI"
    }
    usages: set[tuple[str, str, str, str]] = set()
    for row in payload.get("provider_accounting") or []:
        if not isinstance(row, dict):
            continue
        dataset_id = str(row.get("dataset_id") or "")
        selected = (
            row.get("acquisition_selected_source")
            or row.get("selected_source")
        )
        default_provider = _ai_provider_from_value(
            selected,
            ai_provider_ids=ai_provider_ids,
        )
        delivered = _ai_delivery_value(
            row.get("delivered_value"),
            payload=payload,
            dataset_id=dataset_id,
        )
        if delivered is _INVALID_AI_DELIVERY_REFERENCE:
            if default_provider is not None:
                usages.add(
                    (
                        default_provider,
                        dataset_id,
                        "__INVALID_DELIVERY_REFERENCE__",
                        "__INVALID_DELIVERY_REFERENCE__",
                    )
                )
            continue
        _collect_ai_field_usages(
            delivered,
            dataset_id=dataset_id,
            default_provider=default_provider,
            metric_hint=None,
            ai_provider_ids=ai_provider_ids,
            usages=usages,
        )
    return usages


def _ai_delivery_value(
    value: Any,
    *,
    payload: dict[str, Any],
    dataset_id: str,
) -> Any:
    if not isinstance(value, dict) or "payload_path" not in value:
        return value
    resolved = _resolve_accounting_delivery(
        dataset_id,
        value,
        payload_root=payload,
    )
    if resolved is _INVALID_DELIVERY_REFERENCE:
        return _INVALID_AI_DELIVERY_REFERENCE
    return resolved


def _collect_ai_field_usages(
    value: Any,
    *,
    dataset_id: str,
    default_provider: str | None,
    metric_hint: str | None,
    ai_provider_ids: set[str],
    usages: set[tuple[str, str, str, str]],
) -> None:
    if isinstance(value, list):
        for item in value:
            _collect_ai_field_usages(
                item,
                dataset_id=dataset_id,
                default_provider=default_provider,
                metric_hint=metric_hint,
                ai_provider_ids=ai_provider_ids,
                usages=usages,
            )
        return
    if not isinstance(value, dict):
        return
    current_metric = next(
        (
            str(value.get(key))
            for key in (
                "metric_id",
                "event_metric_id",
                "canonical_metric_id",
            )
            if value.get(key) not in (None, "")
        ),
        metric_hint,
    )
    candidate_fields = _ai_candidate_fields(dataset_id)
    for observed_field, delivered_value in value.items():
        if observed_field == "field_lineage" and isinstance(
            delivered_value,
            (dict, list),
        ):
            continue
        field = _canonical_ai_field(dataset_id, observed_field)
        if (
            field not in candidate_fields
            or delivered_value in (None, "", [], {})
        ):
            continue
        provider_id = _ai_provider_for_field(
            value,
            observed_field=observed_field,
            canonical_field=field,
            default_provider=default_provider,
            ai_provider_ids=ai_provider_ids,
        )
        if provider_id is None:
            continue
        metric_id = current_metric or _unique_ai_metric(
            provider_id,
            dataset_id,
            field,
        )
        usages.add(
            (
                provider_id,
                dataset_id,
                metric_id or "__UNRESOLVED_METRIC__",
                field,
            )
        )
    ignored_nested = {
        "field_lineage",
        "lineage",
        "validation",
        "evidence",
        "source",
        "source_url",
        "publisher",
        "canonical_url",
    }
    nested_default = _nested_ai_provider(
        value,
        default_provider=default_provider,
        ai_provider_ids=ai_provider_ids,
    )
    for key, nested in value.items():
        if key in ignored_nested or not isinstance(nested, (dict, list)):
            continue
        _collect_ai_field_usages(
            nested,
            dataset_id=dataset_id,
            default_provider=nested_default,
            metric_hint=current_metric,
            ai_provider_ids=ai_provider_ids,
            usages=usages,
        )


def _ai_candidate_fields(dataset_id: str) -> set[str]:
    fields = {
        field
        for provider in PROVIDER_REGISTRY
        if provider.provider_type == "AI"
        for capability in provider.capabilities
        if capability.dataset_id == dataset_id
        for field in capability.supported_fields
    }
    if dataset_id in {"macro_calendar", "flash_services_pmi"}:
        fields.update(
            {
                "actual",
                "consensus",
                "previous",
                "previous_revised",
            }
        )
    return fields


def _canonical_ai_field(dataset_id: str, field: str) -> str:
    aliases = {
        "field_lineage": "lineage",
        "forecast": "consensus",
        "previous_revision": "previous_revised",
        "revision": "previous_revised",
    }
    if dataset_id == "flash_services_pmi" and field == "value":
        return "actual"
    return aliases.get(field, field)


def _ai_provider_for_field(
    value: dict[str, Any],
    *,
    observed_field: str,
    canonical_field: str,
    default_provider: str | None,
    ai_provider_ids: set[str],
) -> str | None:
    evidence: list[Any] = []
    for lineage in _field_lineage_entries(
        value,
        observed_field,
        canonical_field,
    ):
        evidence.extend(_provider_identity_values(lineage))
    if observed_field:
        evidence.extend(
            value.get(key)
            for key in (
                f"{observed_field}_acquisition_provider",
                f"{observed_field}_provider_id",
                f"{observed_field}_provider",
                f"{observed_field}_provider_type",
            )
        )
    evidence.extend(_provider_identity_values(value))
    return _resolve_ai_provider_identity(
        evidence,
        default_provider=default_provider,
        ai_provider_ids=ai_provider_ids,
    )


def _field_lineage_entries(
    value: dict[str, Any],
    observed_field: str,
    canonical_field: str,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    field_names = {observed_field, canonical_field} - {""}
    containers = [value.get("field_lineage")]
    lineage = value.get("lineage")
    if isinstance(lineage, dict):
        containers.extend((lineage, lineage.get("field_lineage")))
    elif isinstance(lineage, list):
        containers.append(lineage)
    for container in containers:
        if isinstance(container, dict):
            for field in field_names:
                entry = container.get(field)
                if isinstance(entry, dict):
                    entries.append(entry)
            if (
                str(container.get("field") or "") in field_names
                and container not in entries
            ):
                entries.append(container)
        elif isinstance(container, list):
            entries.extend(
                item
                for item in container
                if isinstance(item, dict)
                and str(item.get("field") or "") in field_names
            )
    return entries


def _provider_identity_values(value: dict[str, Any]) -> list[Any]:
    return [
        value.get(key)
        for key in (
            "acquisition_provider",
            "provider_id",
            "provider_type",
            "source",
        )
        if value.get(key) not in (None, "")
    ]


def _nested_ai_provider(
    value: dict[str, Any],
    *,
    default_provider: str | None,
    ai_provider_ids: set[str],
) -> str | None:
    evidence = _provider_identity_values(value)
    return _resolve_ai_provider_identity(
        evidence,
        default_provider=default_provider,
        ai_provider_ids=ai_provider_ids,
    )


def _resolve_ai_provider_identity(
    evidence: list[Any],
    *,
    default_provider: str | None,
    ai_provider_ids: set[str],
) -> str | None:
    observed = [item for item in evidence if item not in (None, "")]
    registered = {
        provider_id
        for item in observed
        if (provider_id := _registered_provider_id(item)) is not None
    }
    registered_ai = registered & ai_provider_ids
    registered_non_ai = registered - ai_provider_ids
    matched_ai = {
        provider_id
        for item in observed
        if (
            provider_id := _ai_provider_from_value(
                item,
                ai_provider_ids=ai_provider_ids,
            )
        )
        is not None
    }
    generic_ai_signal = any(
        "AI" in str(item or "").upper() for item in observed
    )
    default_ai = (
        default_provider
        if default_provider in ai_provider_ids
        else None
    )
    default_unattributed = default_provider == "__UNATTRIBUTED_AI__"
    ai_identities = registered_ai | matched_ai
    if default_ai is not None:
        ai_identities.add(default_ai)
    if (
        registered_non_ai
        and (
            ai_identities
            or generic_ai_signal
            or default_unattributed
        )
    ):
        return "__UNATTRIBUTED_AI__"
    if default_unattributed or len(ai_identities) > 1:
        return "__UNATTRIBUTED_AI__"
    if ai_identities:
        return next(iter(ai_identities))
    if generic_ai_signal:
        return "__UNATTRIBUTED_AI__"
    if registered_non_ai:
        return None
    return default_provider


def _registered_provider_id(value: Any) -> str | None:
    normalized = _provider_token(value)
    if not normalized:
        return None
    return next(
        (
            provider.provider_id
            for provider in PROVIDER_REGISTRY
            if _provider_token(provider.provider_id) == normalized
        ),
        None,
    )


def _ai_provider_from_value(
    value: Any,
    *,
    ai_provider_ids: set[str],
) -> str | None:
    normalized = _provider_token(value)
    if not normalized:
        return None
    matches = [
        provider_id
        for provider_id in ai_provider_ids
        if _provider_token(provider_id) in normalized
    ]
    return (
        max(matches, key=lambda item: len(_provider_token(item)))
        if matches
        else None
    )


def _provider_token(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _unique_ai_metric(
    provider_id: str,
    dataset_id: str,
    field: str,
) -> str | None:
    metrics = {
        capability.metric_id
        for provider in PROVIDER_REGISTRY
        if provider.provider_id == provider_id
        for capability in provider.capabilities
        if capability.ai_eligible
        and capability.dataset_id == dataset_id
        and field in capability.supported_fields
    }
    return next(iter(metrics)) if len(metrics) == 1 else None


def _read_json_object(path: Path) -> dict[str, Any] | None:
    value = _read_json_value(path)
    return value if isinstance(value, dict) else None


def _read_json_value(path: Path) -> Any:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value


def _summary_count(
    summary: dict[str, Any],
    key: str,
) -> int:
    value = summary.get(key)
    return value if type(value) is int and value >= 0 else 0


def _capability_report_summary_valid(
    report: dict[str, Any],
) -> bool:
    results = report.get("results")
    acquisitions = report.get("acquisitions")
    runtime_adapter_acquisitions = report.get(
        "runtime_adapter_acquisitions"
    )
    runtime_adapter_results = report.get("runtime_adapter_results")
    runtime_leaf_findings = report.get("runtime_leaf_findings")
    started_at = parse_datetime(report.get("started_at"))
    completed_at = parse_datetime(report.get("completed_at"))
    if (
        not isinstance(results, list)
        or not isinstance(acquisitions, list)
        or not isinstance(runtime_adapter_acquisitions, list)
        or not isinstance(runtime_adapter_results, list)
        or not isinstance(runtime_leaf_findings, list)
        or started_at is None
        or completed_at is None
        or completed_at < started_at
    ):
        return False
    all_evaluations = (
        *results,
        *runtime_adapter_results,
        *runtime_leaf_findings,
    )
    counts = Counter(
        str(item.get("health_status") or "").lower()
        for item in all_evaluations
        if isinstance(item, dict)
    )
    expected_counts = {
        status: counts[status]
        for status in (
            "healthy",
            "degraded",
            "unusable",
            "down",
            "auth_failed",
            "rate_limited",
            "not_configured",
            "unknown",
        )
    }
    duration_ms = max(
        0,
        round((completed_at - started_at).total_seconds() * 1000),
    )
    try:
        system_health = determine_system_health(
            all_evaluations,
            audit_status=AuditStatus.COMPLETED,
        ).value
    except (KeyError, TypeError, ValueError):
        return False
    return bool(
        all(report.get(key) == value for key, value in expected_counts.items())
        and report.get("source_gaps")
        == sum(
            "SOURCE_GAP" in (item.get("recommendations") or [])
            for item in all_evaluations
            if isinstance(item, dict)
        )
        and report.get("deduplicated_capability_count")
        == max(0, len(results) - len(acquisitions))
        and report.get("capability_acquisitions_executed")
        == len(acquisitions)
        and report.get("runtime_adapter_acquisitions_executed")
        == len(runtime_adapter_acquisitions)
        and report.get("runtime_adapter_capabilities_tested")
        == len(runtime_adapter_results)
        and report.get("uncertified_runtime_leaves")
        == len(runtime_leaf_findings)
        and report.get("evaluations_tested_total")
        == len(all_evaluations)
        and report.get("runtime_adapter_terminal_rows_complete")
        is True
        and report.get("runtime_leaf_findings_complete") is True
        and report.get("acquisitions_executed")
        == len(acquisitions) + len(runtime_adapter_acquisitions)
        and report.get("duration_ms") == duration_ms
        and report.get("system_health") == system_health
        and report.get("quality_score_formula")
        == quality_score_formula()
    )


def _capability_rows_match_registry(rows: Any) -> bool:
    if not isinstance(rows, list):
        return False
    expected = {
        (
            f"{provider.provider_id}|"
            f"{capability.dataset_id}|{capability.metric_id}"
        ): (
            {
                "provider_id": provider.provider_id,
                "provider_type": provider.provider_type,
                "dataset_id": capability.dataset_id,
                "metric_id": capability.metric_id,
                "supported_fields": list(
                    capability.supported_fields
                ),
                "audit_only_fields": list(
                    capability.audit_only_fields
                ),
                "audited_fields": list(
                    dict.fromkeys(
                        (
                            *capability.supported_fields,
                            *capability.audit_only_fields,
                        )
                    )
                ),
                "frequency": capability.frequency,
                "transformation": capability.transformation,
                "probe_id": capability.probe_id,
                "field_validator_id": (
                    capability.field_validator_id
                ),
                "probe_adapter_path": (
                    capability.probe_adapter_path
                    or provider.adapter_path
                ),
                "request_group": capability.request_group,
            },
            provider,
            capability,
        )
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
    }
    observed: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            return False
        capability_id = str(row.get("capability_id") or "")
        if not capability_id or capability_id in observed:
            return False
        observed[capability_id] = row
    if set(observed) != set(expected):
        return False
    return all(
        all(row.get(key) == value for key, value in identity.items())
        and _capability_result_contract_valid(
            row,
            supported_fields=identity["audited_fields"],
        )
        and not validate_capability_result_derivations(
            row,
            capability,
            provider,
        )
        for capability_id, (
            identity,
            provider,
            capability,
        ) in expected.items()
        for row in (observed[capability_id],)
    )


def _runtime_leaf_findings_valid(rows: Any) -> bool:
    if not isinstance(rows, list):
        return False
    expected = {
        (
            f"{provider.provider_id}|runtime_leaf|"
            f"{stable_sha256(leaf)[:16]}"
        ): (provider.provider_id, leaf)
        for provider in PROVIDER_REGISTRY
        for leaf in provider.uncertified_runtime_leaves
    }
    observed = {
        str(item.get("finding_id") or ""): item
        for item in rows
        if isinstance(item, dict)
    }
    if len(observed) != len(rows) or set(observed) != set(expected):
        return False
    return all(
        row.get("evaluation_kind") == "UNCERTIFIED_RUNTIME_LEAF"
        and row.get("provider_id") == provider_id
        and row.get("runtime_leaf") == leaf
        and row.get("health_status") == "UNUSABLE"
        and row.get("quality_score") == 0
        and row.get("eligible_as_primary") is False
        and row.get("eligible_as_fallback") is False
        and row.get("reason_codes")
        == ["RUNTIME_LEAF_NOT_CAPABILITY_CERTIFIED"]
        and "SOURCE_GAP" in (row.get("recommendations") or [])
        and row.get("terminal") is True
        for finding_id, (provider_id, leaf) in expected.items()
        for row in (observed[finding_id],)
    )


def _capability_result_contract_valid(
    row: dict[str, Any],
    *,
    supported_fields: list[str],
) -> bool:
    field_results = row.get("field_results")
    if (
        not isinstance(field_results, dict)
        or set(field_results) != set(supported_fields)
    ):
        return False
    for field in supported_fields:
        result = field_results.get(field)
        if (
            not isinstance(result, dict)
            or result.get("health_status")
            not in TERMINAL_HEALTH_STATUSES
            or type(result.get("quality_score")) is not int
            or not 0 <= result["quality_score"] <= 100
            or not isinstance(result.get("checks"), dict)
            or not isinstance(
                result.get("score_components"),
                dict,
            )
            or not isinstance(result.get("reason_codes"), list)
            or (
                result.get("evidence") is not None
                and not isinstance(result.get("evidence"), dict)
            )
        ):
            return False
    boolean_checks = (
        "schema_valid",
        "freshness_valid",
        "semantic_mapping_valid",
        "occurrence_match_valid",
        "lineage_valid",
    )
    allowed_recommendations = {
        "KEEP_PRIMARY",
        "KEEP_FALLBACK",
        "DEMOTE_TO_FALLBACK",
        "DISABLE_FOR_METRIC",
        "REMOVE_PROVIDER_CANDIDATE",
        "REQUIRES_FIX",
        "SOURCE_GAP",
    }
    return bool(
        row.get("acquisition_id")
        and row.get("request_key")
        and row.get("transport_status")
        and type(row.get("configured")) is bool
        and type(row.get("attempts")) is int
        and row["attempts"] >= 0
        and isinstance(row.get("checks"), dict)
        and isinstance(row.get("observed_reason_codes"), list)
        and all(
            row.get(check) is None
            or type(row.get(check)) is bool
            for check in boolean_checks
        )
        and type(row.get("quality_score")) is int
        and 0 <= row["quality_score"] <= 100
        and row.get("health_status") in TERMINAL_HEALTH_STATUSES
        and type(row.get("eligible_as_primary")) is bool
        and type(row.get("eligible_as_fallback")) is bool
        and isinstance(row.get("reason_codes"), list)
        and isinstance(row.get("recommendations"), list)
        and set(row["recommendations"]) <= allowed_recommendations
        and parse_datetime(row.get("checked_at")) is not None
    )


def _capability_acquisitions_errors(
    report: dict[str, Any],
    *,
    run_directory: Path,
    checksummed: set[str],
) -> list[str]:
    errors: list[str] = []
    acquisitions = report.get("acquisitions")
    runtime_adapter_acquisitions = report.get(
        "runtime_adapter_acquisitions"
    )
    results = report.get("results")
    runtime_adapter_results = report.get("runtime_adapter_results")
    if (
        not isinstance(acquisitions, list)
        or not isinstance(runtime_adapter_acquisitions, list)
        or not isinstance(results, list)
        or not isinstance(runtime_adapter_results, list)
    ):
        return ["CAPABILITY_AUDIT_ACQUISITIONS_MISSING"]
    if report.get("acquisitions_executed") != (
        len(acquisitions) + len(runtime_adapter_acquisitions)
    ):
        errors.append("CAPABILITY_AUDIT_ACQUISITION_COUNT_MISMATCH")

    by_id: dict[str, dict[str, Any]] = {}
    bound_paths: set[str] = set()
    started_at = parse_datetime(report.get("started_at"))
    completed_at = parse_datetime(report.get("completed_at"))
    run_id = str(report.get("run_id") or "")
    real_dispatches = 0
    registered_targets = {
        (
            f"{provider.provider_id}|"
            f"{capability.dataset_id}|{capability.metric_id}"
        ): (provider, capability)
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
    }
    normalized_by_acquisition: dict[str, Any] = {}
    for acquisition in acquisitions:
        if not isinstance(acquisition, dict):
            errors.append("CAPABILITY_AUDIT_ACQUISITION_INVALID")
            continue
        acquisition_id = str(
            acquisition.get("acquisition_id") or ""
        )
        request_key = str(acquisition.get("request_key") or "")
        checked_at = parse_datetime(acquisition.get("checked_at"))
        configured = acquisition.get("configured")
        if (
            not acquisition_id
            or acquisition_id in by_id
            or acquisition.get("acquisition_kind") != "CAPABILITY"
            or acquisition.get("run_id") != run_id
            or not re.fullmatch(r"[0-9a-f]{64}", request_key)
            or type(configured) is not bool
            or type(acquisition.get("attempts")) is not int
            or acquisition["attempts"] < 0
            or not isinstance(acquisition.get("target_ids"), list)
            or not acquisition["target_ids"]
            or len(acquisition["target_ids"])
            != len(set(acquisition["target_ids"]))
            or not isinstance(acquisition.get("probe_ids"), list)
            or not isinstance(
                acquisition.get("probe_adapter_paths"),
                list,
            )
            or not checked_at
            or not started_at
            or not completed_at
            or checked_at < started_at
            or checked_at > completed_at
        ):
            errors.append(
                "CAPABILITY_AUDIT_ACQUISITION_CONTRACT_INVALID"
            )
            continue
        registered_rows = [
            registered_targets.get(str(target_id))
            for target_id in acquisition["target_ids"]
        ]
        terminal_reasons = {
            str(getattr(provider, "terminal_audit_reason", None) or "").strip()
            for item in registered_rows
            if item is not None
            for provider, _ in (item,)
        }
        terminal_reasons.discard("")
        terminal_audit_reason = (
            next(iter(terminal_reasons))
            if len(terminal_reasons) == 1
            and len(registered_rows) > 0
            and all(item is not None for item in registered_rows)
            else None
        )
        terminal_audit_valid = bool(
            terminal_audit_reason
            and configured is True
            and acquisition.get("transport_status") == "UNUSABLE"
            and acquisition.get("attempts") == 0
            and acquisition.get("network_exchange_count") == 0
            and acquisition.get("real_adapter_invoked") is False
            and acquisition.get("probe_dispatch_status")
            == "TERMINAL_UNSUPPORTED"
            and acquisition.get("reason_codes")
            == [terminal_audit_reason]
            and acquisition.get("raw_response_sha256") is None
        )
        if configured:
            if terminal_audit_valid:
                pass
            elif (
                acquisition.get("real_adapter_invoked") is not True
                or acquisition.get("probe_dispatch_status")
                != "REAL_ADAPTER"
                or (
                    acquisition["attempts"] < 1
                    and not _terminal_runtime_failure_without_transport(
                        acquisition
                    )
                )
            ):
                errors.append(
                    "CAPABILITY_AUDIT_CONFIGURED_DISPATCH_UNPROVED"
                )
            else:
                real_dispatches += 1
        elif (
            acquisition.get("transport_status") != "NOT_CONFIGURED"
            or acquisition.get("real_adapter_invoked") is True
            or acquisition.get("probe_dispatch_status")
            not in {None, "NOT_CONFIGURED"}
        ):
            errors.append(
                "CAPABILITY_AUDIT_NOT_CONFIGURED_DISPATCH_INVALID"
            )
        if (
            any(item is None for item in registered_rows)
            or any(
                provider.provider_id != acquisition.get("provider_id")
                for item in registered_rows
                if item is not None
                for provider, _ in (item,)
            )
        ):
            errors.append(
                "CAPABILITY_AUDIT_ACQUISITION_REGISTRY_SCOPE_INVALID"
            )
        else:
            expected_probe_ids = sorted(
                {
                    capability.probe_id or provider.probe_id
                    for item in registered_rows
                    if item is not None
                    for provider, capability in (item,)
                    if capability.probe_id or provider.probe_id
                }
            )
            expected_adapter_paths = sorted(
                {
                    capability.probe_adapter_path
                    or provider.adapter_path
                    for item in registered_rows
                    if item is not None
                    for provider, capability in (item,)
                }
            )
            if (
                acquisition.get("probe_ids") != expected_probe_ids
                or acquisition.get("probe_adapter_paths")
                != expected_adapter_paths
            ):
                errors.append(
                    "CAPABILITY_AUDIT_ACQUISITION_REGISTRY_IDENTITY_MISMATCH"
                )
            providers = {
                provider.provider_id: provider
                for item in registered_rows
                if item is not None
                for provider, _ in (item,)
            }
            if len(providers) != 1 or not _capture_attestation_valid(
                acquisition,
                provider=next(iter(providers.values())),
            ):
                errors.append(
                    "CAPABILITY_AUDIT_CAPTURE_ATTESTATION_INVALID"
                )
        artifact_errors, paths = _acquisition_artifact_errors(
            acquisition,
            run_directory=run_directory,
            checksummed=checksummed,
            result_rows=[
                row
                for row in results
                if isinstance(row, dict)
                and row.get("acquisition_id") == acquisition_id
            ],
        )
        errors.extend(artifact_errors)
        if bound_paths & paths:
            errors.append(
                "CAPABILITY_AUDIT_ARTIFACT_BOUND_TO_MULTIPLE_ACQUISITIONS"
            )
        bound_paths.update(paths)
        by_id[acquisition_id] = acquisition
        normalized_by_acquisition[acquisition_id] = (
            _acquisition_normalized_payload(
                acquisition,
                run_directory=run_directory,
            )
        )

    runtime_real_dispatches = 0
    runtime_ids: set[str] = set()
    runtime_by_id: dict[str, dict[str, Any]] = {}
    for acquisition in runtime_adapter_acquisitions:
        if not isinstance(acquisition, dict):
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_ACQUISITION_INVALID"
            )
            continue
        acquisition_id = str(acquisition.get("acquisition_id") or "")
        request_key = str(acquisition.get("request_key") or "")
        checked_at = parse_datetime(acquisition.get("checked_at"))
        target_ids = acquisition.get("target_ids")
        adapter_paths = acquisition.get("probe_adapter_paths")
        configured = acquisition.get("configured")
        if (
            acquisition.get("acquisition_kind")
            != "RUNTIME_ADAPTER_COVERAGE"
            or not acquisition_id
            or acquisition_id in by_id
            or acquisition_id in runtime_ids
            or acquisition.get("run_id") != run_id
            or not re.fullmatch(r"[0-9a-f]{64}", request_key)
            or type(configured) is not bool
            or type(acquisition.get("attempts")) is not int
            or acquisition["attempts"] < 0
            or not isinstance(target_ids, list)
            or len(target_ids) != 1
            or not isinstance(adapter_paths, list)
            or len(adapter_paths) != 1
            or not checked_at
            or not started_at
            or not completed_at
            or checked_at < started_at
            or checked_at > completed_at
        ):
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_ACQUISITION_CONTRACT_INVALID"
            )
            continue
        registered = registered_targets.get(str(target_ids[0]))
        adapter_path = str(adapter_paths[0] or "").strip()
        if registered is None:
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_REGISTRY_SCOPE_INVALID"
            )
        else:
            provider, capability = registered
            registered_runtime_paths = {
                provider.adapter_path,
                *provider.additional_adapter_paths,
            }
            expected_probe_ids = sorted(
                {
                    capability.probe_id or provider.probe_id
                }
            )
            if (
                provider.runtime_adapter is not True
                or acquisition.get("provider_id") != provider.provider_id
                or adapter_path not in registered_runtime_paths
                or acquisition.get("probe_ids") != expected_probe_ids
                or _recomputed_acquisition_request_key(
                    acquisition,
                    provider,
                )
                != request_key
            ):
                errors.append(
                    "CAPABILITY_AUDIT_RUNTIME_ADAPTER_REGISTRY_IDENTITY_MISMATCH"
                )
        if configured:
            if (
                acquisition.get("real_adapter_invoked") is not True
                or acquisition.get("probe_dispatch_status")
                != "REAL_ADAPTER"
                or acquisition.get("observed_adapter_path") != adapter_path
                or (
                    acquisition["attempts"] < 1
                    and not _terminal_runtime_failure_without_transport(
                        acquisition
                    )
                )
            ):
                errors.append(
                    "CAPABILITY_AUDIT_RUNTIME_ADAPTER_DISPATCH_UNPROVED"
                )
            else:
                runtime_real_dispatches += 1
        elif (
            acquisition.get("transport_status") != "NOT_CONFIGURED"
            or acquisition.get("real_adapter_invoked") is True
            or acquisition.get("probe_dispatch_status")
            not in {None, "NOT_CONFIGURED"}
        ):
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_NOT_CONFIGURED_INVALID"
            )
        artifact_errors, paths = _acquisition_artifact_errors(
            acquisition,
            run_directory=run_directory,
            checksummed=checksummed,
            result_rows=[
                row
                for row in runtime_adapter_results
                if isinstance(row, dict)
                and row.get("acquisition_id") == acquisition_id
            ],
        )
        errors.extend(artifact_errors)
        if bound_paths & paths:
            errors.append(
                "CAPABILITY_AUDIT_ARTIFACT_BOUND_TO_MULTIPLE_ACQUISITIONS"
            )
        bound_paths.update(paths)
        runtime_ids.add(acquisition_id)
        runtime_by_id[acquisition_id] = acquisition
        normalized_by_acquisition[acquisition_id] = (
            _acquisition_normalized_payload(
                acquisition,
                run_directory=run_directory,
            )
        )

    observed_target_ids: set[str] = set()
    for row in results:
        if not isinstance(row, dict):
            errors.append("CAPABILITY_AUDIT_RESULT_INVALID")
            continue
        capability_id = str(row.get("capability_id") or "")
        acquisition = by_id.get(
            str(row.get("acquisition_id") or "")
        )
        if acquisition is None:
            errors.append(
                "CAPABILITY_AUDIT_RESULT_ACQUISITION_MISSING"
            )
            continue
        registered = registered_targets.get(capability_id)
        if registered is None:
            errors.append(
                "CAPABILITY_AUDIT_RESULT_REGISTRY_IDENTITY_MISSING"
            )
        else:
            provider, capability = registered
            evidence_errors = validate_capability_result_derivations(
                row,
                capability,
                provider,
                require_bound_evidence=True,
                normalized_response=normalized_by_acquisition.get(
                    str(acquisition.get("acquisition_id") or "")
                ),
                acquisition=acquisition,
                artifact_root=run_directory,
            )
            evidence_errors = (
                *evidence_errors,
                *_normalized_check_derivation_errors(
                    row,
                    capability=capability,
                    provider=provider,
                    normalized_response=normalized_by_acquisition.get(
                        str(acquisition.get("acquisition_id") or "")
                    ),
                    checked_at=acquisition.get("checked_at"),
                ),
            )
            if evidence_errors:
                errors.append(
                    "CAPABILITY_AUDIT_FIELD_EVIDENCE_INVALID"
                )
        if (
            row.get("health_status") in {"HEALTHY", "DEGRADED"}
            or row.get("eligible_as_primary") is True
            or row.get("eligible_as_fallback") is True
        ) and acquisition.get("capture_verified") is not True:
            errors.append(
                "CAPABILITY_AUDIT_ELIGIBLE_RESULT_CAPTURE_UNVERIFIED"
            )
        if capability_id in observed_target_ids:
            errors.append(
                "CAPABILITY_AUDIT_TARGET_ASSIGNED_MORE_THAN_ONCE"
            )
        observed_target_ids.add(capability_id)
        expected_adapter = str(
            row.get("probe_adapter_path") or ""
        )
        if (
            capability_id not in acquisition["target_ids"]
            or row.get("request_key")
            != acquisition.get("request_key")
            or row.get("provider_id")
            != acquisition.get("provider_id")
            or row.get("probe_id")
            not in acquisition.get("probe_ids", [])
            or expected_adapter
            not in acquisition.get("probe_adapter_paths", [])
            or row.get("configured")
            != acquisition.get("configured")
            or row.get("attempts")
            != acquisition.get("attempts")
            or row.get("transport_status")
            != acquisition.get("transport_status")
            or row.get("http_status")
            != acquisition.get("http_status")
            or row.get("checked_at")
            != acquisition.get("checked_at")
            or row.get("real_adapter_invoked")
            != acquisition.get("real_adapter_invoked")
            or row.get("probe_dispatch_status")
            != acquisition.get("probe_dispatch_status")
        ):
            errors.append(
                "CAPABILITY_AUDIT_RESULT_ACQUISITION_MISMATCH"
            )
    assigned_by_acquisition = {
        target_id
        for acquisition in by_id.values()
        for target_id in acquisition["target_ids"]
    }
    if observed_target_ids != assigned_by_acquisition:
        errors.append("CAPABILITY_AUDIT_TARGET_PARTITION_MISMATCH")
    errors.extend(
        _runtime_adapter_result_errors(
            runtime_adapter_results,
            runtime_by_id=runtime_by_id,
            registered_targets=registered_targets,
            normalized_by_acquisition=normalized_by_acquisition,
            run_directory=run_directory,
        )
    )
    if report.get("real_adapter_probes") != (
        real_dispatches + runtime_real_dispatches
    ):
        errors.append("CAPABILITY_AUDIT_REAL_DISPATCH_COUNT_MISMATCH")
    acquisition_artifacts = {
        relative
        for relative in checksummed
        if relative.startswith("provider-responses/")
        or relative.startswith("ai-evidence/")
    }
    if bound_paths != acquisition_artifacts:
        errors.append(
            "CAPABILITY_AUDIT_ACQUISITION_ARTIFACT_COVERAGE_MISMATCH"
        )
    return sorted(set(errors))


def _runtime_adapter_coverage_valid(report: dict[str, Any]) -> bool:
    acquisitions = report.get("acquisitions")
    runtime_adapter_acquisitions = report.get(
        "runtime_adapter_acquisitions"
    )
    if not isinstance(acquisitions, list) or not isinstance(
        runtime_adapter_acquisitions,
        list,
    ):
        return False
    results = report.get("results")
    runtime_adapter_results = report.get("runtime_adapter_results")
    if not isinstance(results, list) or not isinstance(
        runtime_adapter_results,
        list,
    ):
        return False
    targets = select_capability_targets(
        PROVIDER_REGISTRY,
        AuditFilters(),
    )
    expected = _runtime_adapter_coverage(
        targets,
        (*acquisitions, *runtime_adapter_acquisitions),
    )
    expected_quality = _runtime_adapter_quality(
        expected,
        (*results, *runtime_adapter_results),
    )
    return bool(
        report.get("runtime_adapter_coverage") == expected
        and report.get("runtime_adapter_quality")
        == expected_quality
        and expected.get("complete") is True
        and expected.get("coverage_pct") == 100.0
        and expected_quality.get("complete") is True
        and expected_quality.get("evaluation_coverage_pct")
        == 100.0
    )


def _runtime_adapter_result_errors(
    rows: list[Any],
    *,
    runtime_by_id: dict[str, dict[str, Any]],
    registered_targets: dict[str, tuple[Any, Any]],
    normalized_by_acquisition: dict[str, Any],
    run_directory: Path,
) -> list[str]:
    errors: list[str] = []
    if len(rows) != len(runtime_by_id):
        errors.append(
            "CAPABILITY_AUDIT_RUNTIME_ADAPTER_RESULT_COUNT_MISMATCH"
        )
    observed_acquisitions: set[str] = set()
    observed_ids: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_RESULT_INVALID"
            )
            continue
        acquisition_id = str(row.get("acquisition_id") or "")
        acquisition = runtime_by_id.get(acquisition_id)
        base_id = str(row.get("base_capability_id") or "")
        registered = registered_targets.get(base_id)
        adapter_path = str(row.get("runtime_adapter_path") or "")
        evaluation_id = str(row.get("capability_id") or "")
        expected_id = (
            f"{base_id}|runtime_adapter|"
            f"{stable_sha256(adapter_path)[:16]}"
        )
        if (
            acquisition is None
            or registered is None
            or acquisition_id in observed_acquisitions
            or evaluation_id in observed_ids
            or evaluation_id != expected_id
            or row.get("evaluation_kind")
            != "RUNTIME_ADAPTER_CAPABILITY"
        ):
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_RESULT_IDENTITY_INVALID"
            )
            continue
        provider, capability = registered
        target = replace(
            capability,
            probe_adapter_path=adapter_path,
        )
        if (
            acquisition.get("target_ids") != [base_id]
            or acquisition.get("probe_adapter_paths") != [adapter_path]
            or row.get("provider_id") != provider.provider_id
            or row.get("probe_adapter_path") != adapter_path
            or row.get("request_key")
            != acquisition.get("request_key")
            or row.get("configured")
            != acquisition.get("configured")
            or row.get("attempts") != acquisition.get("attempts")
            or row.get("transport_status")
            != acquisition.get("transport_status")
            or row.get("http_status") != acquisition.get("http_status")
            or row.get("checked_at") != acquisition.get("checked_at")
            or row.get("real_adapter_invoked")
            != acquisition.get("real_adapter_invoked")
            or row.get("probe_dispatch_status")
            != acquisition.get("probe_dispatch_status")
            or not _capability_result_contract_valid(
                row,
                supported_fields=list(capability.supported_fields),
            )
        ):
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_RESULT_ACQUISITION_MISMATCH"
            )
        derivation_errors = validate_capability_result_derivations(
            row,
            target,
            provider,
            require_bound_evidence=True,
            normalized_response=normalized_by_acquisition.get(
                acquisition_id
            ),
            acquisition=acquisition,
            artifact_root=run_directory,
        )
        normalized_errors = _normalized_check_derivation_errors(
            row,
            capability=target,
            provider=provider,
            normalized_response=normalized_by_acquisition.get(
                acquisition_id
            ),
            checked_at=acquisition.get("checked_at"),
        )
        if derivation_errors or normalized_errors:
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_FIELD_EVIDENCE_INVALID"
            )
        if (
            row.get("health_status") in {"HEALTHY", "DEGRADED"}
            or row.get("eligible_as_primary") is True
            or row.get("eligible_as_fallback") is True
        ) and acquisition.get("capture_verified") is not True:
            errors.append(
                "CAPABILITY_AUDIT_RUNTIME_ADAPTER_RESULT_CAPTURE_UNVERIFIED"
            )
        observed_acquisitions.add(acquisition_id)
        observed_ids.add(evaluation_id)
    if observed_acquisitions != set(runtime_by_id):
        errors.append(
            "CAPABILITY_AUDIT_RUNTIME_ADAPTER_RESULT_PARTITION_MISMATCH"
        )
    return errors


def _acquisition_normalized_payload(
    acquisition: dict[str, Any],
    *,
    run_directory: Path,
) -> Any:
    bindings = acquisition.get("artifact_bindings")
    if not isinstance(bindings, list):
        return None
    matches = [
        binding
        for binding in bindings
        if isinstance(binding, dict)
        and binding.get("kind") == "normalized_response"
    ]
    if len(matches) != 1:
        return None
    relative = str(matches[0].get("path") or "")
    path = (run_directory / relative).resolve()
    if not path.is_relative_to(run_directory):
        return None
    return _read_json_value(path)


def _normalized_check_derivation_errors(
    row: dict[str, Any],
    *,
    capability: Any,
    provider: Any,
    normalized_response: Any,
    checked_at: Any,
) -> tuple[str, ...]:
    # The live probe and the accepting validator deliberately share the pure
    # field rules but not their reported booleans. Re-running them here over
    # the checksummed normalized artifact prevents self-declared ALL_TRUE rows.
    from scripts.provider_capability_audit import (
        _field_schema_check,
        _freshness_check,
        _lineage_check,
        _occurrence_check,
        _semantic_check,
    )

    target_id = (
        f"{provider.provider_id}|"
        f"{capability.dataset_id}|{capability.metric_id}"
    )
    check_target = SimpleNamespace(
        provider_id=provider.provider_id,
        provider_type=provider.provider_type,
        dataset_id=capability.dataset_id,
        metric_id=capability.metric_id,
        frequency=capability.frequency,
        transformation=capability.transformation,
        field_validator_id=capability.field_validator_id,
        capability=capability,
        registration=provider,
    )
    observed_at = parse_datetime(checked_at)
    field_results = row.get("field_results")
    if not isinstance(field_results, dict):
        return ("NORMALIZED_CHECK_FIELD_RESULTS_MISSING",)
    errors: list[str] = []
    for field_name in (
        *capability.supported_fields,
        *capability.audit_only_fields,
    ):
        field_result = field_results.get(field_name)
        checks = (
            field_result.get("checks")
            if isinstance(field_result, dict)
            else None
        )
        if not isinstance(checks, dict):
            errors.append(f"{field_name}:NORMALIZED_CHECKS_MISSING")
            continue
        if checks.get("transport_valid") is not True:
            continue
        field_observed, value, owner = normalized_field_observation(
            normalized_response,
            target_id=target_id,
            metric_id=capability.metric_id,
            field_name=field_name,
        )
        owner_value = owner if owner is not None else normalized_response
        expected: dict[str, bool | None] = {
            "schema_valid": _field_schema_check(
                check_target,
                field_name,
                normalized_response,
                owner if field_observed else None,
                value,
            ),
            "completeness_valid": field_observed and value is not None,
        }
        if provider.provider_type != "AI":
            expected.update(
                {
                    "freshness_valid": _freshness_check(
                        owner_value,
                        target=check_target,
                        field_name=field_name,
                        field_value=value,
                        now=observed_at,
                    ),
                    "semantic_mapping_valid": _semantic_check(
                        check_target,
                        owner if field_observed else None,
                        normalized_response,
                        field_name=field_name,
                        value=value,
                    ),
                    "occurrence_match_valid": _occurrence_check(
                        check_target,
                        owner if field_observed else None,
                        normalized_response,
                    ),
                    "lineage_valid": _lineage_check(
                        field_name,
                        owner if field_observed else None,
                        normalized_response,
                    ),
                }
            )
        if any(checks.get(name) != expected_value for name, expected_value in expected.items()):
            errors.append(
                f"{field_name}:CHECKS_NOT_DERIVED_FROM_NORMALIZED_RESPONSE"
            )
    return tuple(errors)


def _capture_attestation_valid(
    acquisition: dict[str, Any],
    *,
    provider: Any,
) -> bool:
    capture_mode = acquisition.get("capture_mode")
    capture_mode_expected = acquisition.get("capture_mode_expected")
    capture_verified = acquisition.get("capture_verified")
    attestation = acquisition.get("capture_attestation")
    attestation_sha256 = acquisition.get("capture_attestation_sha256")
    capture_configuration = acquisition.get(
        "capture_mode_configuration"
    )
    configuration_setting = str(
        getattr(provider, "configuration_setting", None)
        or (
            "ai_researcher_mode"
            if provider.provider_id == "AI_RESEARCHER"
            else ""
        )
    ).strip()
    if configuration_setting:
        if not isinstance(capture_configuration, dict) or (
            capture_configuration.get("setting")
            != configuration_setting
        ):
            return False
        configured_value = str(
            capture_configuration.get("value") or ""
        ).strip().lower()
        if configuration_setting == "ai_researcher_mode":
            registered_expected = {
                "codex_cli": "SUBPROCESS",
                "openai_api": "HTTPX",
            }.get(configured_value)
        else:
            expected_value = str(
                getattr(provider, "configuration_value", None) or ""
            ).strip().lower()
            variant_not_selected = bool(
                acquisition.get("configured") is False
                and configured_value != expected_value
                and (
                    "PROVIDER_CONFIGURATION_VARIANT_NOT_SELECTED:"
                    f"{configuration_setting}={expected_value}"
                )
                in set(acquisition.get("reason_codes") or ())
            )
            if configured_value != expected_value and not variant_not_selected:
                return False
            registered_expected = str(
                getattr(provider, "capture_mode", None) or ""
            ).strip().upper()
        if registered_expected is None:
            return False
    else:
        if capture_configuration is not None:
            return False
        declared_capture_mode = str(
            getattr(provider, "capture_mode", "") or ""
        ).strip().upper()
        if declared_capture_mode:
            registered_expected = declared_capture_mode
        elif provider.provider_type in _LOCAL_CAPTURE_PROVIDER_TYPES:
            registered_expected = "LOCAL_SANDBOX"
        else:
            registered_expected = "HTTPX"
    if capture_verified is not True:
        return bool(
            (
                acquisition.get("configured") is False
                or acquisition.get("probe_dispatch_status")
                == "TERMINAL_UNSUPPORTED"
            )
            and
            (capture_verified is None or capture_verified is False)
            and capture_mode in {None, "NONE"}
            and attestation is None
            and attestation_sha256 is None
        )
    if (
        capture_mode != registered_expected
        or capture_mode_expected != registered_expected
        or not isinstance(attestation, dict)
        or attestation_sha256 != stable_sha256(attestation)
    ):
        return False
    network_count = acquisition.get("network_exchange_count")
    if capture_mode == "HTTPX":
        captured_response = bool(
            type(network_count) is int
            and network_count > 0
            and attestation.get("exchange_count") == network_count
            and type(attestation.get("first_attempt")) is int
            and type(attestation.get("last_attempt")) is int
        )
        attempted_failure = bool(
            type(network_count) is int
            and network_count >= 0
            and type(attestation.get("attempted_send_count")) is int
            and attestation["attempted_send_count"] > 0
            and attestation.get("response_exchange_count")
            == network_count
            and isinstance(
                attestation.get("failure_reason_code"),
                str,
            )
            and bool(attestation["failure_reason_code"])
            and acquisition.get("transport_status")
            not in {"OK", "SUCCESS", "HEALTHY"}
        )
        return captured_response or attempted_failure
    if capture_mode == "LOCAL_SANDBOX":
        completed_local_probe = bool(
            network_count == 0
            and isinstance(attestation.get("sandbox_root"), str)
            and bool(attestation["sandbox_root"])
            and type(attestation.get("database_snapshot_isolated"))
            is bool
        )
        failed_local_probe = bool(
            network_count == 0
            and attestation.get("local_invocation_observed") is True
            and isinstance(
                attestation.get("failure_reason_code"),
                str,
            )
            and bool(attestation["failure_reason_code"])
            and acquisition.get("transport_status")
            not in {"OK", "SUCCESS", "HEALTHY"}
        )
        return completed_local_probe or failed_local_probe
    if capture_mode != "SUBPROCESS":
        return False
    digest_pattern = r"[0-9a-f]{64}"
    process_observed = bool(
        attestation.get("process_observed") is True
        and type(attestation.get("process_id")) is int
        and attestation["process_id"] > 0
        and attestation.get("process_terminated") is True
        and all(
            re.fullmatch(digest_pattern, str(attestation.get(name) or ""))
            for name in (
                "stdout_sha256",
                "stderr_sha256",
                "command_sha256",
                "declared_output_sha256",
            )
        )
    )
    bounded_timeout = bool(
        attestation.get("bounded_timeout_observed") is True
        and attestation.get("process_terminated") is True
        and attestation.get("failure_reason")
        in {"ai_research_timeout", "codex_cli_timeout"}
        and "PROBE_TIMEOUT" in set(acquisition.get("reason_codes") or ())
        and acquisition.get("transport_status") == "DOWN"
    )
    process_completed = type(attestation.get("exit_code")) is int
    if (
        not process_observed
        or not (process_completed or bounded_timeout)
        or type(network_count) is not int
        or network_count < 0
        or attestation.get("source_exchange_count") != network_count
    ):
        return False
    if bounded_timeout:
        return True
    if attestation.get("exit_code") == 0:
        return bool(
            process_observed
            and re.fullmatch(
                digest_pattern,
                str(attestation.get("subprocess_output_sha256") or ""),
            )
            and re.fullmatch(
                digest_pattern,
                str(attestation.get("declared_output_sha256") or ""),
            )
            and attestation.get("subprocess_output_sha256")
            == acquisition.get("raw_response_sha256")
            and attestation.get("declared_output_sha256")
            == attestation.get("subprocess_output_sha256")
            and type(attestation.get("subprocess_output_size_bytes")) is int
            and attestation["subprocess_output_size_bytes"] >= 0
        )
    return process_completed


def _terminal_runtime_failure_without_transport(
    acquisition: dict[str, Any],
) -> bool:
    return bool(
        acquisition.get("attempts") == 0
        and acquisition.get("network_exchange_count") == 0
        and acquisition.get("transport_status")
        not in {"OK", "SUCCESS", "HEALTHY"}
        and (
            acquisition.get("capture_mode_expected") != "SUBPROCESS"
            or acquisition.get("capture_verified") is True
        )
        and set(acquisition.get("reason_codes") or ())
        <= {
            "ADAPTER_RUNTIME_FAILED",
            "ADAPTER_RESPONSE_PARSE_FAILED",
            "PROVIDER_TRANSPORT_FAILED",
            "PROBE_TIMEOUT",
        }
        and bool(acquisition.get("reason_codes"))
    )


def _acquisition_artifact_errors(
    acquisition: dict[str, Any],
    *,
    run_directory: Path,
    checksummed: set[str],
    result_rows: list[dict[str, Any]],
) -> tuple[list[str], set[str]]:
    errors: list[str] = []
    bindings = acquisition.get("artifact_bindings")
    if (
        acquisition.get("artifact_binding_complete") is not True
        or not isinstance(bindings, list)
    ):
        return (
            ["CAPABILITY_AUDIT_ACQUISITION_ARTIFACTS_MISSING"],
            set(),
        )
    paths: set[str] = set()
    kinds: list[str] = []
    identities: dict[str, dict[str, Any]] = {}
    for binding in bindings:
        if not isinstance(binding, dict):
            errors.append(
                "CAPABILITY_AUDIT_ACQUISITION_ARTIFACT_INVALID"
            )
            continue
        relative = str(binding.get("path") or "").replace("\\", "/")
        path = (run_directory / relative).resolve()
        if (
            not relative
            or relative in paths
            or relative not in checksummed
            or not path.is_relative_to(run_directory)
            or not _identity_matches(path, binding)
            or not str(binding.get("kind") or "")
        ):
            errors.append(
                "CAPABILITY_AUDIT_ACQUISITION_ARTIFACT_INVALID"
            )
            continue
        paths.add(relative)
        kinds.append(str(binding["kind"]))
        identities[str(binding["kind"])] = binding
    required = {
        "acquisition_metadata",
        "normalized_response",
        "response_headers",
    }
    if acquisition.get("raw_response_sha256") is not None:
        required.add("response_body")
    try:
        provider = provider_by_id(
            str(acquisition.get("provider_id") or "")
        )
    except KeyError:
        errors.append("CAPABILITY_AUDIT_ACQUISITION_PROVIDER_UNKNOWN")
        return sorted(set(errors)), paths
    if provider.provider_type == "AI":
        required.add("ai_evidence")
    ai_metadata_identity: dict[str, Any] | None = None
    network_count = acquisition.get("network_exchange_count")
    allowed = {
        *required,
        "http_exchange_body",
        "http_exchange_headers",
    }
    if (
        not required <= set(kinds)
        or not set(kinds) <= allowed
        or any(kinds.count(kind) != 1 for kind in required)
        or type(network_count) is not int
        or network_count < 0
        or kinds.count("http_exchange_body") != network_count
        or kinds.count("http_exchange_headers") != network_count
    ):
        errors.append(
            "CAPABILITY_AUDIT_ACQUISITION_ARTIFACT_SET_INVALID"
        )
    raw = identities.get("response_body")
    if (
        raw is not None
        and raw.get("original_sha256")
        != acquisition.get("raw_response_sha256")
    ):
        errors.append(
            "CAPABILITY_AUDIT_RAW_RESPONSE_BINDING_MISMATCH"
        )
    normalized = identities.get("normalized_response")
    if normalized is not None:
        normalized_path = (
            run_directory / str(normalized["path"])
        ).resolve()
        normalized_payload = _read_json_value(normalized_path)
        expected_hash = acquisition.get(
            "normalized_response_sha256"
        )
        if (
            expected_hash is None
            and normalized_payload is not None
            or expected_hash is not None
            and stable_sha256(normalized_payload) != expected_hash
        ):
            errors.append(
                "CAPABILITY_AUDIT_NORMALIZED_RESPONSE_BINDING_MISMATCH"
            )
    ai_evidence = identities.get("ai_evidence")
    if provider.provider_type == "AI" and ai_evidence is not None:
        ai_errors, ai_metadata_identity = _ai_evidence_artifact_errors(
            acquisition,
            binding=ai_evidence,
            run_directory=run_directory,
            result_rows=result_rows,
        )
        errors.extend(ai_errors)
    metadata = identities.get("acquisition_metadata")
    if metadata is not None:
        metadata_payload = _read_json_object(
            (run_directory / str(metadata["path"])).resolve()
        )
        expected = {
            "run_id": acquisition.get("run_id"),
            "acquisition_id": acquisition.get("acquisition_id"),
            "acquisition_kind": acquisition.get("acquisition_kind"),
            "request_key": acquisition.get("request_key"),
            "provider_id": acquisition.get("provider_id"),
            "target_ids": acquisition.get("target_ids"),
            "probe_adapter_paths": acquisition.get(
                "probe_adapter_paths"
            ),
            "observed_adapter_path": acquisition.get(
                "observed_adapter_path"
            ),
            "attempts": acquisition.get("attempts"),
            "transport_status": acquisition.get("transport_status"),
            "http_status": acquisition.get("http_status"),
            "latency_ms": acquisition.get("latency_ms"),
            "checked_at": acquisition.get("checked_at"),
            "capture_mode": acquisition.get("capture_mode"),
            "capture_mode_expected": acquisition.get(
                "capture_mode_expected"
            ),
            "capture_verified": acquisition.get("capture_verified"),
            "capture_attestation": acquisition.get(
                "capture_attestation"
            ),
            "capture_attestation_sha256": acquisition.get(
                "capture_attestation_sha256"
            ),
        }
        if (
            metadata_payload is None
            or any(
                metadata_payload.get(key) != value
                for key, value in expected.items()
            )
            or (
                provider.provider_type == "AI"
                and metadata_payload.get("ai_evidence")
                != ai_metadata_identity
            )
            or (
                provider.provider_type != "AI"
                and "ai_evidence" in metadata_payload
            )
        ):
            errors.append(
                "CAPABILITY_AUDIT_ACQUISITION_METADATA_MISMATCH"
            )
    return sorted(set(errors)), paths


def _ai_evidence_artifact_errors(
    acquisition: dict[str, Any],
    *,
    binding: dict[str, Any],
    run_directory: Path,
    result_rows: list[dict[str, Any]],
) -> tuple[list[str], dict[str, Any] | None]:
    relative = str(binding.get("path") or "").replace("\\", "/")
    path = (run_directory / relative).resolve()
    payload = _read_json_object(path)
    if payload is None:
        return ["CAPABILITY_AUDIT_AI_EVIDENCE_INVALID"], None
    try:
        exact_bytes = path.read_bytes()
    except OSError:
        return ["CAPABILITY_AUDIT_AI_EVIDENCE_INVALID"], None
    evidence = payload.get("evidence")
    evidence_sha256 = payload.get("evidence_sha256")
    identity = {
        "run_id": acquisition.get("run_id"),
        "acquisition_id": acquisition.get("acquisition_id"),
        "acquisition_kind": acquisition.get("acquisition_kind"),
        "request_key": acquisition.get("request_key"),
        "provider_id": acquisition.get("provider_id"),
        "target_ids": acquisition.get("target_ids"),
        "evidence_sha256": evidence_sha256,
    }
    expected_keys = {
        "schema_version",
        *identity,
        "evidence",
    }
    binding_identity = {
        key: binding.get(key) for key in identity
    }
    if (
        set(payload) != expected_keys
        or payload.get("schema_version")
        != "provider-audit-ai-evidence-v1"
        or not isinstance(evidence, dict)
        or evidence_sha256 != stable_sha256(evidence)
        or any(payload.get(key) != value for key, value in identity.items())
        or binding_identity != identity
        or exact_bytes != _canonical_json_bytes(payload)
    ):
        return ["CAPABILITY_AUDIT_AI_EVIDENCE_INVALID"], None

    execution_projection = {
        "adapter_path": acquisition.get("observed_adapter_path"),
        "real_adapter_invoked": acquisition.get("real_adapter_invoked"),
        "probe_dispatch_status": acquisition.get(
            "probe_dispatch_status"
        ),
        "capture_mode": acquisition.get("capture_mode"),
        "capture_mode_expected": acquisition.get(
            "capture_mode_expected"
        ),
        "capture_verified": acquisition.get("capture_verified"),
        "capture_attestation": acquisition.get("capture_attestation"),
    }
    if any(
        evidence.get(key) != value
        for key, value in execution_projection.items()
    ):
        return ["CAPABILITY_AUDIT_AI_EVIDENCE_INVALID"], None

    expected_fields: dict[str, Any] = {}
    for row in result_rows:
        target_id = str(
            row.get("base_capability_id")
            or row.get("capability_id")
            or ""
        )
        field_results = row.get("field_results")
        if not target_id or not isinstance(field_results, dict):
            return ["CAPABILITY_AUDIT_AI_EVIDENCE_INVALID"], None
        for field_name, field_result in field_results.items():
            field_evidence = (
                field_result.get("evidence")
                if isinstance(field_result, dict)
                else None
            )
            if not isinstance(field_evidence, dict):
                return ["CAPABILITY_AUDIT_AI_EVIDENCE_INVALID"], None
            if field_evidence.get("source_evidence_present") is True:
                source_evidence = field_evidence.get("source_evidence")
                if not isinstance(source_evidence, dict):
                    return ["CAPABILITY_AUDIT_AI_EVIDENCE_INVALID"], None
                expected_fields[f"{target_id}|{field_name}"] = (
                    source_evidence
                )
    observed_fields = evidence.get("fields")
    if observed_fields is None:
        observed_fields = {}
    if observed_fields != expected_fields:
        return ["CAPABILITY_AUDIT_AI_EVIDENCE_INVALID"], None
    return [], {"path": relative, **identity}


def _legacy_fallback_chain_accounting_valid(
    value: Any,
    *,
    results: Any,
) -> bool:
    if not isinstance(value, dict) or not isinstance(results, list):
        return False
    policies = {
        policy.dataset_id: policy
        for policy in DATASET_SOURCE_POLICIES
    }
    rows = value.get("rows")
    if (
        value.get("mode")
        != "POLICY_CHAIN_ACCOUNTING_NO_LIVE_FAULT_INJECTION"
        or value.get("scope_applicable") is not True
        or value.get("policies_expected") != len(policies)
        or value.get("policies_accounted") != len(policies)
        or value.get("complete_rows") != len(policies)
        or value.get("coverage_pct") != 100.0
        or value.get("complete") is not True
        or not isinstance(rows, list)
        or len(rows) != len(policies)
    ):
        return False
    by_dataset = {
        str(row.get("dataset_id") or ""): row
        for row in rows
        if isinstance(row, dict)
    }
    if set(by_dataset) != set(policies):
        return False
    for dataset_id, policy in policies.items():
        row = by_dataset[dataset_id]
        expected = [
            (policy.canonical_repository, "DB"),
            (policy.primary_provider, "PRIMARY"),
            *(
                (provider_id, "FALLBACK")
                for provider_id in policy.fallback_providers
            ),
            *(
                (provider_id, "AI")
                for provider_id in policy.ai_fallback_providers
            ),
        ]
        accounting = row.get("accounting")
        if (
            row.get("verification_mode")
            != "ATOMIC_CAPABILITY_ELIGIBILITY"
            or row.get("expected_order")
            != [item[0] for item in expected]
            or row.get("database_first") is not True
            or row.get("ai_after_deterministic_fallbacks") is not True
            or row.get("complete") is not True
            or not isinstance(accounting, list)
            or len(accounting) != len(expected)
        ):
            return False
        selected: str | None = None
        selected_role: str | None = None
        for observed, (source_id, source_kind) in zip(
            accounting,
            expected,
            strict=True,
        ):
            matches = [
                item
                for item in results
                if isinstance(item, dict)
                and item.get("provider_id") == source_id
                and item.get("dataset_id") == dataset_id
            ]
            capability_ids = sorted(
                str(item.get("capability_id")) for item in matches
            )
            eligible = any(
                (
                    item.get("health_status")
                    in {"HEALTHY", "DEGRADED"}
                    if source_kind == "DB"
                    else item.get("eligible_as_primary") is True
                    if source_kind == "PRIMARY"
                    else item.get("eligible_as_fallback") is True
                )
                for item in matches
            )
            selected_here = selected is None and eligible
            if selected_here:
                selected = source_id
                selected_role = source_kind
            if (
                not isinstance(observed, dict)
                or observed.get("source_id") != source_id
                or observed.get("source_kind") != source_kind
                or observed.get("capability_ids") != capability_ids
                or observed.get("terminal_results_present")
                != bool(matches)
                or observed.get("eligible_for_capability") != eligible
                or observed.get(
                    "selected_by_eligibility_simulation"
                )
                != selected_here
            ):
                return False
        if (
            row.get("selected_eligible_source") != selected
            or row.get("selected_eligible_role") != selected_role
            or row.get("delivery_value") is not None
            or row.get("delivery_reason_code")
            != (
                "ELIGIBLE_SOURCE_IDENTIFIED"
                if selected
                else "ALL_SOURCES_FAILED_OR_UNAVAILABLE"
            )
        ):
            return False
    return True


def _fallback_chain_accounting_valid(
    value: Any,
    *,
    results: Any,
) -> bool:
    if (
        not isinstance(value, dict)
        or not isinstance(results, list)
        or value.get("scope_applicable") is not True
    ):
        return False
    expected = verify_policy_fallback_chains(
        DATASET_SOURCE_POLICIES,
        results,
        registry=PROVIDER_REGISTRY,
    )
    observed = dict(value)
    observed.pop("scope_applicable", None)
    return observed == expected


def _resolved_artifact_path(
    value: Any,
    *,
    base: Path,
) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text)
    return (
        path.resolve()
        if path.is_absolute()
        else (base / path).resolve()
    )


def _identity_matches(
    path: Path,
    identity: Any,
) -> bool:
    if not path.is_file() or not isinstance(identity, dict):
        return False
    size = identity.get("size_bytes")
    digest = identity.get("sha256")
    if (
        type(size) is not int
        or size < 0
        or not isinstance(digest, str)
        or not re.fullmatch(r"[0-9a-f]{64}", digest)
    ):
        return False
    return bool(
        path.stat().st_size == size
        and hashlib.sha256(path.read_bytes()).hexdigest()
        == digest
    )


if __name__ == "__main__":
    raise SystemExit(main())
