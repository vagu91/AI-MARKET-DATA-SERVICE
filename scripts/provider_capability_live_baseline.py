from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.services.provider_audit_artifacts import (
    _file_identity as _audit_file_identity,
    _referenced_audit_tree_identity as _audit_tree_identity,
)
from app.services.provider_capability_audit import (
    CORRECTNESS_CHECKS,
    FALLBACK_ROLES,
    HealthStatus,
    PRIMARY_ROLES,
    recommendations_for_result,
    stable_sha256,
)
from app.services.provider_capability_registry import provider_by_id
from scripts.validate_senior_analyst_payload import (
    _verified_capability_audit,
)


BASELINE_CONTRACT = "ProviderCapabilityLastLiveBaseline"
BASELINE_SCHEMA_VERSION = "1.2"


class ProviderCapabilityBaselineImportError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _AuditSourceSnapshot:
    pointer_identity: dict[str, Any]
    artifact_tree_identity: dict[str, dict[str, Any]]


@dataclass(frozen=True, slots=True)
class _DestinationState:
    exists: bool
    payload: bytes | None


def import_verified_live_audit(
    pointer_path: Path,
    *,
    destination: Path,
) -> dict[str, Any]:
    resolved_pointer = pointer_path.resolve()
    resolved_destination = destination.resolve()
    source_snapshot = _audit_source_snapshot(resolved_pointer)
    previous_destination = _destination_state(resolved_destination)
    pointer, report, errors = _verified_capability_audit(
        resolved_pointer
    )
    if errors or pointer is None or report is None:
        detail = ",".join(errors) if errors else "AUDIT_NOT_AVAILABLE"
        raise ProviderCapabilityBaselineImportError(
            f"provider capability LIVE audit rejected:{detail}"
        )
    if (
        pointer.get("audit_status") != "COMPLETED"
        or report.get("audit_status") != "COMPLETED"
    ):
        raise ProviderCapabilityBaselineImportError(
            "provider capability LIVE audit is not COMPLETED"
        )
    report_identity = (
        pointer.get("artifacts", {}).get("audit-report.json")
        if isinstance(pointer.get("artifacts"), dict)
        else None
    )
    if not isinstance(report_identity, dict):
        raise ProviderCapabilityBaselineImportError(
            "verified audit report identity is unavailable"
        )
    _require_unchanged_audit_source(
        resolved_pointer,
        source_snapshot,
        phase="verification",
    )

    capability_rows = sorted(
        (
            _compact_capability_row(row)
            for row in report.get("results") or []
        ),
        key=lambda row: row["capability_id"],
    )
    baseline = {
        "contract": BASELINE_CONTRACT,
        "schema_version": BASELINE_SCHEMA_VERSION,
        "source": {
            "pointer_file": resolved_pointer.name,
            "audit_report_sha256": report_identity["sha256"],
            "audit_report_size_bytes": report_identity["size_bytes"],
            "extracted_capabilities_sha256": stable_sha256(
                capability_rows
            ),
        },
        "run_id": report["run_id"],
        "audit_status": report["audit_status"],
        "system_health": report["system_health"],
        "registry_sha256": report["registry_sha256"],
        "providers_tested": report["providers_tested"],
        "capabilities_tested": report["capabilities_tested"],
        "capabilities": capability_rows,
    }
    baseline["attestation"] = {
        "algorithm": "SHA-256",
        "content_sha256": stable_sha256(baseline),
        "capability_rows": len(capability_rows),
        "field_rows": sum(
            len(row["field_results"]) for row in capability_rows
        ),
    }
    payload = baseline_file_bytes(baseline)
    temporary = _stage_bytes(resolved_destination, payload)
    destination_replaced = False
    try:
        _require_unchanged_audit_source(
            resolved_pointer,
            source_snapshot,
            phase="staged_write",
        )
        if _destination_state(resolved_destination) != previous_destination:
            raise ProviderCapabilityBaselineImportError(
                "provider capability baseline destination changed during import"
            )
        os.replace(temporary, resolved_destination)
        destination_replaced = True
        if _destination_state(resolved_destination) != _DestinationState(
            exists=True,
            payload=payload,
        ):
            raise ProviderCapabilityBaselineImportError(
                "provider capability baseline destination bytes mismatch"
            )
        _require_unchanged_audit_source(
            resolved_pointer,
            source_snapshot,
            phase="committed_write",
        )
    except Exception as exc:
        if destination_replaced:
            try:
                _restore_destination(
                    resolved_destination,
                    previous_destination,
                )
            except OSError as rollback_exc:
                raise ProviderCapabilityBaselineImportError(
                    "provider capability baseline rollback failed"
                ) from rollback_exc
        if isinstance(exc, ProviderCapabilityBaselineImportError):
            raise
        raise ProviderCapabilityBaselineImportError(
            "provider capability baseline atomic import failed"
        ) from exc
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            # A staged-file cleanup failure must not turn a verified atomic
            # replacement into an apparent failed import.
            pass
    return baseline


def baseline_file_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def baseline_file_sha256(value: Any) -> str:
    return hashlib.sha256(baseline_file_bytes(value)).hexdigest()


def _compact_capability_row(row: Any) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ProviderCapabilityBaselineImportError(
            "verified audit contains a non-object capability row"
        )
    provider_id = str(row["provider_id"])
    field_results = row.get("field_results")
    supported_fields = row.get("supported_fields")
    audit_only_fields = row.get("audit_only_fields")
    audited_fields = row.get("audited_fields")
    if not isinstance(field_results, dict) or not isinstance(
        supported_fields,
        list,
    ) or not isinstance(
        audit_only_fields,
        list,
    ) or not isinstance(
        audited_fields,
        list,
    ):
        raise ProviderCapabilityBaselineImportError(
            "verified audit capability field results are unavailable"
        )
    provider = provider_by_id(provider_id)
    expected_audited_fields = list(
        dict.fromkeys((*supported_fields, *audit_only_fields))
    )
    if (
        audited_fields != expected_audited_fields
        or set(field_results) != set(expected_audited_fields)
    ):
        raise ProviderCapabilityBaselineImportError(
            "verified audit capability field scope is inconsistent"
        )
    roles = {role.upper() for role in provider.allowed_roles}
    compact_fields = {
        str(field_name): _compact_field_result(
            field_results.get(field_name),
            roles=roles,
            checked_at=row.get("checked_at"),
        )
        for field_name in audited_fields
    }
    compact = {
        "capability_id": str(row["capability_id"]),
        "provider_id": provider_id,
        "dataset_id": str(row["dataset_id"]),
        "metric_id": str(row["metric_id"]),
        "health_status": str(row["health_status"]),
        "eligible_as_primary": row["eligible_as_primary"],
        "eligible_as_fallback": row["eligible_as_fallback"],
        "recommendations": [
            str(item) for item in row.get("recommendations") or []
        ],
        "reason_codes": sorted(
            {
                str(item)
                for item in row.get("reason_codes") or []
                if str(item)
            }
        ),
        "field_results": compact_fields,
    }
    terminal_reason = str(provider.terminal_audit_reason or "").strip()
    if terminal_reason and row.get("configured") is True:
        terminal_attestation = {
            "attempts": row.get("attempts"),
            "real_adapter_invoked": row.get("real_adapter_invoked"),
            "probe_dispatch_status": row.get("probe_dispatch_status"),
            "reason_code": terminal_reason,
        }
        if terminal_attestation != {
            "attempts": 0,
            "real_adapter_invoked": False,
            "probe_dispatch_status": "TERMINAL_UNSUPPORTED",
            "reason_code": terminal_reason,
        }:
            raise ProviderCapabilityBaselineImportError(
                "verified terminal audit declaration is inconsistent"
            )
        compact["terminal_attestation"] = terminal_attestation
    elif terminal_reason and not (
        row.get("configured") is False
        and row.get("transport_status") == "NOT_CONFIGURED"
        and row.get("attempts") == 0
        and row.get("real_adapter_invoked") is not True
        and row.get("probe_dispatch_status")
        in {None, "NOT_CONFIGURED"}
        and row.get("health_status") == "NOT_CONFIGURED"
        and row.get("eligible_as_primary") is False
        and row.get("eligible_as_fallback") is False
    ):
        raise ProviderCapabilityBaselineImportError(
            "terminal provider is neither configured-terminal nor not-configured"
        )
    return compact


def _compact_field_result(
    value: Any,
    *,
    roles: set[str],
    checked_at: Any,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProviderCapabilityBaselineImportError(
            "verified audit contains a missing field result"
        )
    try:
        health = HealthStatus(str(value["health_status"]))
    except (KeyError, ValueError) as exc:
        raise ProviderCapabilityBaselineImportError(
            "verified audit contains an invalid field health status"
        ) from exc
    checks = value.get("checks")
    correctness_valid = isinstance(checks, dict) and all(
        checks.get(check) is True
        for check in CORRECTNESS_CHECKS
    )
    primary_allowed = not roles or bool(roles & PRIMARY_ROLES)
    fallback_allowed = not roles or bool(roles & FALLBACK_ROLES)
    eligible_as_primary = (
        health is HealthStatus.HEALTHY
        and primary_allowed
        and correctness_valid
    )
    eligible_as_fallback = (
        health in {HealthStatus.HEALTHY, HealthStatus.DEGRADED}
        and fallback_allowed
        and correctness_valid
    )
    recommendations = recommendations_for_result(
        health,
        roles=roles,
        eligible_as_primary=eligible_as_primary,
        eligible_as_fallback=eligible_as_fallback,
    )
    quality_score = value.get("quality_score")
    if type(quality_score) is not int:
        raise ProviderCapabilityBaselineImportError(
            "verified audit contains an invalid field quality score"
        )
    return {
        "health_status": health.value,
        "quality_score": quality_score,
        "eligible_as_primary": eligible_as_primary,
        "eligible_as_fallback": eligible_as_fallback,
        "recommended_role": recommendations[0],
        "recommendations": recommendations,
        "reason_codes": sorted(
            {
                str(item)
                for item in value.get("reason_codes") or []
                if str(item)
            }
        ),
        "checked_at": str(checked_at) if checked_at else None,
    }


def _audit_source_snapshot(pointer_path: Path) -> _AuditSourceSnapshot:
    try:
        pointer_before = dict(_audit_file_identity(pointer_path))
        tree = _audit_tree_identity(pointer_path, pointer_path)
        pointer_after = dict(_audit_file_identity(pointer_path))
    except FileNotFoundError as exc:
        raise ProviderCapabilityBaselineImportError(
            "provider capability LIVE audit rejected:"
            "CAPABILITY_AUDIT_POINTER_UNREADABLE"
        ) from exc
    except OSError as exc:
        raise ProviderCapabilityBaselineImportError(
            "provider capability audit source snapshot failed"
        ) from exc
    if pointer_before != pointer_after or tree is None:
        raise ProviderCapabilityBaselineImportError(
            "provider capability audit source changed during snapshot"
        )
    return _AuditSourceSnapshot(
        pointer_identity=pointer_before,
        artifact_tree_identity={
            str(relative): dict(identity)
            for relative, identity in tree.items()
        },
    )


def _require_unchanged_audit_source(
    pointer_path: Path,
    expected: _AuditSourceSnapshot,
    *,
    phase: str,
) -> None:
    if _audit_source_snapshot(pointer_path) != expected:
        raise ProviderCapabilityBaselineImportError(
            "provider capability audit source changed during "
            f"baseline import:{phase}"
        )


def _destination_state(path: Path) -> _DestinationState:
    try:
        return _DestinationState(exists=True, payload=path.read_bytes())
    except FileNotFoundError:
        return _DestinationState(exists=False, payload=None)
    except OSError as exc:
        raise ProviderCapabilityBaselineImportError(
            "provider capability baseline destination is unreadable"
        ) from exc


def _stage_bytes(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        return temporary
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise


def _restore_destination(path: Path, state: _DestinationState) -> None:
    if not state.exists:
        path.unlink(missing_ok=True)
        return
    if state.payload is None:
        raise OSError("previous provider baseline bytes are unavailable")
    temporary = _stage_bytes(path, state.payload)
    try:
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
