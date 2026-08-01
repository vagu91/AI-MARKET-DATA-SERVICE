from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from app.core.redaction import redact_payload, redact_sensitive
from app.services.provider_capability_audit import ProbeOutcome, ProbeRequest


_SAFE_FILE_PART = re.compile(r"[^A-Za-z0-9_.-]+")


class ProviderAuditPublicationRejected(RuntimeError):
    """Raised when a completed run fails the strong publication acceptance."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = tuple(sorted({str(item) for item in errors if str(item)}))
        message = ", ".join(self.errors) or "CAPABILITY_AUDIT_ACCEPTANCE_REJECTED"
        super().__init__(message)


PublicationVerifier = Callable[
    [Path, Path],
    tuple[Mapping[str, Any] | None, Mapping[str, Any] | None, Sequence[str]],
]


def publish_verified_audit_candidate(
    candidate_pointer: Path,
    latest_pointer: Path,
    verifier: PublicationVerifier,
) -> Path:
    """Atomically publish a candidate only after whole-run strong acceptance.

    The candidate lives beside, but is never visible as, the canonical latest
    pointer.  Any validator failure leaves an existing latest pointer byte-for-
    byte unchanged.
    """

    candidate = candidate_pointer.resolve()
    latest = latest_pointer.resolve()
    if candidate == latest or candidate.parent != latest.parent:
        raise ValueError("audit candidate and latest pointer locations are invalid")
    if latest.name != "provider-capability-audit-latest.json":
        raise ValueError("canonical audit pointer name is invalid")
    if not candidate.is_file():
        raise FileNotFoundError(f"audit candidate pointer is absent: {candidate}")

    candidate_identity = _file_identity(candidate)
    artifact_tree_identity = _referenced_audit_tree_identity(
        candidate,
        latest,
    )
    latest_identity = _file_identity(latest) if latest.is_file() else None
    latest_tree_identity = (
        _referenced_audit_tree_identity(latest, latest)
        if latest.is_file()
        else None
    )
    try:
        accepted_pointer, accepted_report, errors = verifier(candidate, latest)
    except Exception as exc:
        _discard_candidate(candidate)
        raise ProviderAuditPublicationRejected(
            (f"CAPABILITY_AUDIT_ACCEPTANCE_ERROR:{type(exc).__name__}",)
        ) from exc

    persisted_pointer = _read_json_mapping(candidate)
    rejection_errors = list(errors)
    if persisted_pointer is None or accepted_pointer != persisted_pointer:
        rejection_errors.append("CAPABILITY_AUDIT_ACCEPTED_POINTER_MISMATCH")
    if not isinstance(accepted_report, Mapping):
        rejection_errors.append("CAPABILITY_AUDIT_ACCEPTED_REPORT_MISSING")
    elif (
        persisted_pointer is None
        or accepted_report.get("run_id") != persisted_pointer.get("run_id")
        or accepted_report.get("audit_status") != "COMPLETED"
        or persisted_pointer.get("audit_status") != "COMPLETED"
        or persisted_pointer.get("full_audit_scope") is not True
    ):
        rejection_errors.append("CAPABILITY_AUDIT_ACCEPTED_RUN_MISMATCH")
    if _file_identity(candidate) != candidate_identity:
        rejection_errors.append("CAPABILITY_AUDIT_CANDIDATE_CHANGED_DURING_ACCEPTANCE")
    if (
        artifact_tree_identity is None
        or _referenced_audit_tree_identity(candidate, latest)
        != artifact_tree_identity
    ):
        rejection_errors.append(
            "CAPABILITY_AUDIT_ARTIFACT_TREE_CHANGED_DURING_ACCEPTANCE"
        )
    current_latest_identity = (
        _file_identity(latest) if latest.is_file() else None
    )
    current_latest_tree_identity = (
        _referenced_audit_tree_identity(latest, latest)
        if latest.is_file()
        else None
    )
    if (
        current_latest_identity != latest_identity
        or current_latest_tree_identity != latest_tree_identity
    ):
        rejection_errors.append(
            "CAPABILITY_AUDIT_PREVIOUS_RUN_CHANGED_DURING_ACCEPTANCE"
        )
    if rejection_errors:
        _discard_candidate(candidate)
        raise ProviderAuditPublicationRejected(rejection_errors)

    latest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(candidate, latest)
    return latest


class ProviderAuditArtifactWriter:
    """Writes a complete audit tree before atomically publishing its run and pointer."""

    def __init__(
        self,
        output_root: Path,
        run_id: str,
        *,
        secret_values: Iterable[str] = (),
    ) -> None:
        self.output_root = output_root.resolve()
        self.run_id = _safe_name(run_id)
        self.final_directory = self.output_root / self.run_id
        self.latest_pointer = self.output_root.parent / "provider-capability-audit-latest.json"
        self.candidate_pointer = self.output_root.parent / (
            f".provider-capability-audit-latest.{self.run_id}."
            f"{uuid.uuid4().hex}.candidate.json"
        )
        self.staging_directory = (
            self.output_root / f".inprogress-{self.run_id}-{uuid.uuid4().hex}"
        )
        self.secret_values = tuple(
            sorted(
                {str(value) for value in secret_values if value and len(str(value)) >= 4},
                key=len,
                reverse=True,
            )
        )
        self._log_sequence = 0
        if self.final_directory.exists():
            raise FileExistsError(f"audit run already exists: {self.final_directory}")
        self.staging_directory.mkdir(parents=True, exist_ok=False)
        for relative in ("provider-responses", "ai-evidence", "logs"):
            (self.staging_directory / relative).mkdir()
        self._log(
            {
                "event": "AUDIT_ARTIFACT_STAGING_CREATED",
                "run_id": self.run_id,
            }
        )

    def record_acquisition(
        self,
        request: ProbeRequest,
        outcome: ProbeOutcome,
    ) -> Mapping[str, Any]:
        acquisition = _safe_name(request.acquisition_id)
        provider = _safe_name(request.provider_id)
        prefix = self.staging_directory / "provider-responses" / f"{provider}--{acquisition}"
        headers = _redacted_header_items(
            outcome.headers,
            self.secret_values,
        )
        normalized = redact_payload(outcome.normalized_response)
        evidence = redact_payload(dict(outcome.evidence))
        capture_attestation = evidence.get("capture_attestation")
        raw = outcome.raw_response
        raw_original_sha256 = _sha256_bytes(raw) if raw is not None else None
        raw_saved_sha256: str | None = None
        exact_bytes_saved: bool | None = None
        redaction_applied = False
        body_path: Path | None = None
        artifact_bindings: list[Mapping[str, Any]] = []
        if raw is not None:
            saved, redaction_applied = _redact_bytes(raw, self.secret_values)
            exact_bytes_saved = not redaction_applied
            raw_saved_sha256 = _sha256_bytes(saved)
            body_path = _append_suffix(prefix, ".response.bin")
            _atomic_write_bytes(body_path, saved)
            artifact_bindings.append(
                {
                    "kind": "response_body",
                    "path": _relative(body_path, self.staging_directory),
                    **_file_identity(body_path),
                    "original_sha256": raw_original_sha256,
                    "exact_bytes_saved": exact_bytes_saved,
                    "redaction_applied": redaction_applied,
                }
            )
        headers_path = _append_suffix(prefix, ".headers.json")
        normalized_path = _append_suffix(prefix, ".normalized.json")
        metadata_path = _append_suffix(prefix, ".metadata.json")
        _atomic_write_json(headers_path, headers)
        _atomic_write_json(normalized_path, normalized)
        artifact_bindings.extend(
            (
                {
                    "kind": "response_headers",
                    "path": _relative(headers_path, self.staging_directory),
                    **_file_identity(headers_path),
                },
                {
                    "kind": "normalized_response",
                    "path": _relative(normalized_path, self.staging_directory),
                    **_file_identity(normalized_path),
                },
            )
        )
        metadata = {
            "schema_version": "provider-audit-acquisition-v1",
            "run_id": request.run_id,
            "acquisition_id": request.acquisition_id,
            "acquisition_kind": request.acquisition_kind,
            "request_key": request.request_key,
            "provider_id": request.provider_id,
            "target_ids": [target.target_id for target in request.targets],
            "probe_adapter_paths": [request.adapter_path],
            "observed_adapter_path": evidence.get("adapter_path"),
            "attempts": outcome.attempts,
            "transport_status": outcome.transport_status,
            "http_status": outcome.http_status,
            "latency_ms": outcome.latency_ms,
            "checked_at": outcome.checked_at,
            "reason_codes": list(outcome.reason_codes),
            "capture_mode": evidence.get("capture_mode"),
            "capture_mode_expected": evidence.get(
                "capture_mode_expected"
            ),
            "capture_verified": evidence.get("capture_verified"),
            "capture_attestation": capture_attestation,
            "capture_attestation_sha256": (
                _stable_sha256(capture_attestation)
                if capture_attestation is not None
                else None
            ),
            "raw_response": {
                "path": _relative(body_path, self.staging_directory),
                "original_sha256": raw_original_sha256,
                "saved_sha256": raw_saved_sha256,
                "size_bytes": body_path.stat().st_size if body_path else None,
                "exact_bytes_saved": exact_bytes_saved,
                "redaction_applied": redaction_applied,
            },
            "headers": {
                "path": _relative(headers_path, self.staging_directory),
                **_file_identity(headers_path),
            },
            "normalized_response": {
                "path": _relative(normalized_path, self.staging_directory),
                **_file_identity(normalized_path),
            },
            "http_exchanges": [],
        }
        for exchange_index, exchange in enumerate(outcome.network_exchanges, start=1):
            exchange_prefix = prefix.with_name(
                f"{prefix.name}.http-{exchange_index:03d}"
            )
            exchange_body = _append_suffix(exchange_prefix, ".response.bin")
            exchange_headers = _append_suffix(exchange_prefix, ".headers.json")
            saved_exchange_body, exchange_redacted = _redact_bytes(
                exchange.response_body,
                self.secret_values,
            )
            _atomic_write_bytes(exchange_body, saved_exchange_body)
            _atomic_write_json(
                exchange_headers,
                {
                    "request": {
                        "method": exchange.method,
                        "url": redact_sensitive(exchange.url),
                        "headers": _redacted_header_items(
                            exchange.request_headers,
                            self.secret_values,
                        ),
                    },
                    "response": {
                        "status_code": exchange.status_code,
                        "headers": _redacted_header_items(
                            exchange.response_headers,
                            self.secret_values,
                        ),
                    },
                },
            )
            metadata["http_exchanges"].append(
                {
                    "sequence": exchange_index,
                    "method": exchange.method,
                    "url": redact_sensitive(exchange.url),
                    "status_code": exchange.status_code,
                    "attempt": exchange.attempt,
                    "latency_ms": exchange.latency_ms,
                    "body": {
                        "path": _relative(exchange_body, self.staging_directory),
                        "original_sha256": _sha256_bytes(exchange.response_body),
                        "saved_sha256": _sha256_bytes(saved_exchange_body),
                        "size_bytes": len(saved_exchange_body),
                        "exact_bytes_saved": not exchange_redacted,
                        "redaction_applied": exchange_redacted,
                    },
                    "headers": {
                        "path": _relative(exchange_headers, self.staging_directory),
                        **_file_identity(exchange_headers),
                    },
                }
            )
            artifact_bindings.extend(
                (
                    {
                        "kind": "http_exchange_body",
                        "sequence": exchange_index,
                        "attempt": exchange.attempt,
                        "path": _relative(
                            exchange_body,
                            self.staging_directory,
                        ),
                        **_file_identity(exchange_body),
                        "original_sha256": _sha256_bytes(
                            exchange.response_body
                        ),
                        "exact_bytes_saved": not exchange_redacted,
                        "redaction_applied": exchange_redacted,
                    },
                    {
                        "kind": "http_exchange_headers",
                        "sequence": exchange_index,
                        "attempt": exchange.attempt,
                        "path": _relative(
                            exchange_headers,
                            self.staging_directory,
                        ),
                        **_file_identity(exchange_headers),
                    },
                )
            )
        ai_path: Path | None = None
        if "AI" in request.targets[0].provider_type.upper():
            ai_path = (
                self.staging_directory
                / "ai-evidence"
                / f"{provider}--{acquisition}.json"
            )
            evidence_sha256 = _stable_sha256(evidence)
            ai_evidence_identity = {
                "run_id": request.run_id,
                "acquisition_id": request.acquisition_id,
                "acquisition_kind": request.acquisition_kind,
                "request_key": request.request_key,
                "provider_id": request.provider_id,
                "target_ids": [target.target_id for target in request.targets],
                "evidence_sha256": evidence_sha256,
            }
            _atomic_write_json(
                ai_path,
                {
                    "schema_version": "provider-audit-ai-evidence-v1",
                    **ai_evidence_identity,
                    "evidence": evidence,
                },
            )
            metadata["ai_evidence"] = {
                "path": _relative(ai_path, self.staging_directory),
                **ai_evidence_identity,
            }
            artifact_bindings.append(
                {
                    "kind": "ai_evidence",
                    "path": _relative(ai_path, self.staging_directory),
                    **_file_identity(ai_path),
                    **ai_evidence_identity,
                }
            )
        _atomic_write_json(metadata_path, metadata)
        artifact_bindings.append(
            {
                "kind": "acquisition_metadata",
                "path": _relative(metadata_path, self.staging_directory),
                **_file_identity(metadata_path),
            }
        )
        self._log(
            {
                "event": "PROVIDER_ACQUISITION_RECORDED",
                "acquisition_kind": request.acquisition_kind,
                "acquisition_id": request.acquisition_id,
                "request_key": request.request_key,
                "provider_id": request.provider_id,
                "target_ids": [
                    target.target_id for target in request.targets
                ],
                "transport_status": outcome.transport_status,
                "http_status": outcome.http_status,
                "exact_bytes_saved": exact_bytes_saved,
                "redaction_applied": redaction_applied,
            }
        )
        return {
            "metadata": metadata,
            "artifact_bindings": sorted(
                artifact_bindings,
                key=lambda item: str(item["path"]),
            ),
        }

    def finalize(
        self,
        report: Mapping[str, Any],
        capability_matrix: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if not self.staging_directory.is_dir():
            raise RuntimeError("audit staging directory is unavailable")
        safe_report = redact_payload(dict(report))
        safe_matrix = redact_payload(dict(capability_matrix))
        comparison = self._comparison_with_previous(safe_matrix)
        report_path = self.staging_directory / "audit-report.json"
        markdown_path = self.staging_directory / "audit-report.md"
        matrix_path = self.staging_directory / "capability-matrix.json"
        comparison_path = self.staging_directory / "comparison-with-previous.json"
        _atomic_write_json(report_path, safe_report)
        _atomic_write_bytes(markdown_path, _markdown_report(safe_report).encode("utf-8"))
        _atomic_write_json(matrix_path, safe_matrix)
        _atomic_write_json(comparison_path, comparison)
        self._log(
            {
                "event": "AUDIT_ARTIFACT_FINALIZATION_STARTED",
                "audit_status": safe_report.get("audit_status"),
            }
        )
        checksums_path = self.staging_directory / "checksums.json"
        checksums = self._build_checksums()
        _atomic_write_json(checksums_path, checksums)
        self._verify_checksums(checksums)
        self.output_root.mkdir(parents=True, exist_ok=True)
        os.replace(self.staging_directory, self.final_directory)

        candidate_path: Path | None = None
        if (
            safe_report.get("audit_status") == "COMPLETED"
            and safe_report.get("terminal_rows_complete") is True
            and safe_report.get("full_audit_scope") is True
            and int(safe_report.get("providers_tested", -1))
            == int(safe_report.get("providers_registered", -2))
            and int(safe_report.get("capabilities_tested", -1))
            == int(safe_report.get("capabilities_registered", -2))
            and int(safe_report.get("capabilities_tested", -1))
            == len(safe_matrix.get("capabilities", []))
        ):
            pointer = self._latest_payload(safe_report)
            _atomic_write_json(self.candidate_pointer, pointer)
            candidate_path = self.candidate_pointer
        return {
            "run_directory": str(self.final_directory),
            "candidate_pointer": (
                str(candidate_path) if candidate_path is not None else None
            ),
            "latest_pointer": None,
            "checksums": str(self.final_directory / "checksums.json"),
        }

    def publish_candidate(
        self,
        candidate_pointer: Path,
        verifier: PublicationVerifier,
    ) -> Path:
        candidate = candidate_pointer.resolve()
        if candidate != self.candidate_pointer.resolve():
            raise ValueError("candidate pointer does not belong to this audit writer")
        return publish_verified_audit_candidate(
            candidate,
            self.latest_pointer,
            verifier,
        )

    def abandon(self) -> None:
        if self.staging_directory.is_dir():
            shutil.rmtree(self.staging_directory)
        _discard_candidate(self.candidate_pointer)

    def _build_checksums(self) -> Mapping[str, Any]:
        files: list[Mapping[str, Any]] = []
        for path in sorted(self.staging_directory.rglob("*")):
            if not path.is_file() or path.name == "checksums.json":
                continue
            files.append(
                {
                    "path": path.relative_to(self.staging_directory).as_posix(),
                    **_file_identity(path),
                }
            )
        return {
            "schema_version": "provider-audit-checksums-v1",
            "run_id": self.run_id,
            "algorithm": "SHA-256",
            "self_excluded": True,
            "files": files,
        }

    def _verify_checksums(self, manifest: Mapping[str, Any]) -> None:
        for item in manifest.get("files", []):
            path = _contained_path(self.staging_directory, str(item["path"]))
            identity = _file_identity(path)
            if identity != {
                "size_bytes": item["size_bytes"],
                "sha256": item["sha256"],
            }:
                raise RuntimeError(f"artifact checksum mismatch: {item['path']}")

    def _latest_payload(self, report: Mapping[str, Any]) -> Mapping[str, Any]:
        files = {
            name: self.final_directory / name
            for name in (
                "audit-report.json",
                "audit-report.md",
                "capability-matrix.json",
                "comparison-with-previous.json",
                "checksums.json",
            )
        }
        for path in files.values():
            if not path.is_file():
                raise RuntimeError(f"latest pointer source is absent: {path.name}")
        return {
            "schema_version": "provider-capability-audit-latest-v1",
            "run_id": self.run_id,
            "audit_status": report["audit_status"],
            "system_health": report["system_health"],
            "completed_at": report["completed_at"],
            "registry_sha256": report["registry_sha256"],
            "source_provenance": report["source_provenance"],
            "providers_tested": report["providers_tested"],
            "capabilities_tested": report["capabilities_tested"],
            "full_audit_scope": True,
            "run_directory": str(self.final_directory),
            "artifacts": {
                name: {
                    "path": str(path),
                    **_file_identity(path),
                }
                for name, path in files.items()
            },
        }

    def _comparison_with_previous(
        self,
        current_matrix: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        previous = _load_previous_matrix(self.latest_pointer, self.output_root)
        if previous is None:
            return _comparison_payload(
                run_id=self.run_id,
                current_matrix=current_matrix,
            )
        previous_run_id, previous_matrix, previous_identity = previous
        return _comparison_payload(
            run_id=self.run_id,
            current_matrix=current_matrix,
            previous_run_id=previous_run_id,
            previous_matrix=previous_matrix,
            previous_matrix_identity=previous_identity,
        )

    def _log(self, item: Mapping[str, Any]) -> None:
        path = self.staging_directory / "logs" / "audit.jsonl"
        self._log_sequence += 1
        event = redact_payload(dict(item))
        event.update(
            {
                "schema_version": "provider-audit-log-event-v1",
                "run_id": self.run_id,
                "sequence": self._log_sequence,
            }
        )
        payload = json.dumps(
            event,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
            stream.write("\n")


def _load_previous_matrix(
    pointer_path: Path,
    output_root: Path,
) -> tuple[
    str,
    Mapping[str, Any],
    Mapping[str, Any],
] | None:
    if not pointer_path.is_file():
        return None
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
        run_id = str(pointer.get("run_id") or "")
        if (
            pointer.get("schema_version")
            != "provider-capability-audit-latest-v1"
            or pointer.get("audit_status") != "COMPLETED"
            or pointer.get("full_audit_scope") is not True
            or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]*",
                run_id,
            )
            or run_id in {".", ".."}
        ):
            return None
        expected_run_directory = (output_root / run_id).resolve()
        if Path(str(pointer["run_directory"])).resolve() != expected_run_directory:
            return None
        matrix_identity = pointer["artifacts"]["capability-matrix.json"]
        matrix_path = Path(str(matrix_identity["path"])).resolve()
        if matrix_path != expected_run_directory / "capability-matrix.json":
            return None
        if _file_identity(matrix_path) != {
            "size_bytes": matrix_identity["size_bytes"],
            "sha256": matrix_identity["sha256"],
        }:
            return None
        matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
        if (
            matrix.get("schema_version")
            != "provider-capability-matrix-v1"
            or matrix.get("audit_status") != "COMPLETED"
            or matrix.get("run_id") != run_id
        ):
            return None
        return (
            run_id,
            matrix,
            _file_identity(matrix_path),
        )
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _comparison_payload(
    *,
    run_id: str,
    current_matrix: Mapping[str, Any],
    previous_run_id: str | None = None,
    previous_matrix: Mapping[str, Any] | None = None,
    previous_matrix_identity: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    current_identity = _bytes_identity(
        _canonical_json_bytes(current_matrix)
    )
    base: dict[str, Any] = {
        "schema_version": "provider-audit-comparison-v1",
        "run_id": run_id,
        "current_matrix_sha256": current_identity["sha256"],
        "current_matrix_size_bytes": current_identity["size_bytes"],
    }
    if previous_matrix is None:
        return {
            **base,
            "comparison_status": "NO_PREVIOUS_COMPLETED_AUDIT",
            "changes": [],
        }
    if not previous_run_id or previous_matrix_identity is None:
        raise ValueError("previous audit identity is incomplete")
    old = _capability_index(previous_matrix)
    new = _capability_index(current_matrix)
    changes: list[Mapping[str, Any]] = []
    for capability_id in sorted(set(old) | set(new)):
        before = old.get(capability_id)
        after = new.get(capability_id)
        if before is None:
            changes.append(
                {"capability_id": capability_id, "change": "ADDED"}
            )
            continue
        if after is None:
            changes.append(
                {"capability_id": capability_id, "change": "REMOVED"}
            )
            continue
        fields = (
            "health_status",
            "quality_score",
            "eligible_as_primary",
            "eligible_as_fallback",
            "recommendations",
        )
        changed = {
            name: {"before": before.get(name), "after": after.get(name)}
            for name in fields
            if before.get(name) != after.get(name)
        }
        if changed:
            changes.append(
                {
                    "capability_id": capability_id,
                    "change": "MODIFIED",
                    "fields": changed,
                }
            )
    return {
        **base,
        "comparison_status": "COMPARED",
        "previous_run_id": previous_run_id,
        "previous_matrix_sha256": previous_matrix_identity.get("sha256"),
        "previous_matrix_size_bytes": previous_matrix_identity.get(
            "size_bytes"
        ),
        "changes": changes,
    }


def _capability_index(
    matrix: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    rows = matrix.get("capabilities")
    if not isinstance(rows, Sequence) or isinstance(
        rows,
        (str, bytes, bytearray),
    ):
        raise ValueError("capability matrix rows are invalid")
    indexed: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("capability matrix row is invalid")
        capability_id = str(row.get("capability_id") or "")
        if not capability_id or capability_id in indexed:
            raise ValueError("capability matrix identity is ambiguous")
        indexed[capability_id] = row
    return indexed


def _markdown_report(report: Mapping[str, Any]) -> str:
    runtime_coverage = report.get("runtime_adapter_coverage")
    runtime_coverage = (
        runtime_coverage
        if isinstance(runtime_coverage, Mapping)
        else {}
    )
    rows = [
        "# Provider Capability Audit",
        "",
        f"- Run ID: `{report.get('run_id')}`",
        f"- Audit status: `{report.get('audit_status')}`",
        f"- System health: `{report.get('system_health')}`",
        f"- Registry SHA-256: `{report.get('registry_sha256')}`",
        (
            "- Source commit: `"
            f"{(report.get('source_provenance') or {}).get('git_commit_sha')}`"
        ),
        (
            "- Audited runtime SHA-256: `"
            f"{(report.get('source_provenance') or {}).get('audited_runtime_sha256')}`"
        ),
        f"- Providers tested: {report.get('providers_tested')}",
        f"- Capabilities tested: {report.get('capabilities_tested')}",
        (
            "- Runtime adapter coverage: "
            f"{runtime_coverage.get('adapters_accounted')}/"
            f"{runtime_coverage.get('adapter_bindings_expected')} "
            f"({runtime_coverage.get('coverage_pct')}%)"
        ),
        "",
        "| Provider | Dataset | Metric | Status | Score | Recommendation |",
        "|---|---|---|---:|---:|---|",
    ]
    for item in report.get("results", []):
        rows.append(
            "| {provider} | {dataset} | {metric} | {status} | {score} | {recommendation} |".format(
                provider=_markdown_cell(item.get("provider_id")),
                dataset=_markdown_cell(item.get("dataset_id")),
                metric=_markdown_cell(item.get("metric_id")),
                status=_markdown_cell(item.get("health_status")),
                score=item.get("quality_score"),
                recommendation=_markdown_cell(
                    ", ".join(item.get("recommendations", []))
                ),
            )
        )
    rows.extend(
        [
            "",
            "## Quality score",
            "",
            "The score totals 100 points. Each component earns its full documented weight "
            "only when the corresponding observed check is true; missing evidence earns zero "
            "and remains UNKNOWN rather than being inferred.",
            "",
        ]
    )
    return "\n".join(rows)


def _redact_bytes(raw: bytes, secret_values: tuple[str, ...]) -> tuple[bytes, bool]:
    result = raw
    changed = False
    for secret in secret_values:
        encoded = secret.encode("utf-8")
        if encoded in result:
            result = result.replace(encoded, b"<redacted>")
            changed = True
    try:
        text = result.decode("utf-8")
    except UnicodeDecodeError:
        return result, changed
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = None
    if parsed is not None:
        redacted_payload = redact_payload(parsed)
        if redacted_payload != parsed:
            result = json.dumps(
                redacted_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            text = result.decode("utf-8")
            changed = True
    redacted = redact_sensitive(text)
    if redacted != text:
        result = redacted.encode("utf-8")
        changed = True
    return result, changed


def _redacted_header_items(
    value: Any,
    secret_values: tuple[str, ...],
) -> Mapping[str, Any]:
    items: Iterable[Any]
    if isinstance(value, Mapping):
        items = value.items()
    elif isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        items = value
    else:
        items = ()
    safe_items: list[Mapping[str, str]] = []
    for item in items:
        if not isinstance(item, Sequence) or isinstance(
            item,
            (str, bytes, bytearray),
        ):
            continue
        if len(item) != 2:
            continue
        name = str(item[0])
        raw_value = str(item[1])
        safe_value = raw_value
        for secret in secret_values:
            safe_value = safe_value.replace(secret, "<redacted>")
        redacted_mapping = redact_payload({name: safe_value})
        safe_items.append(
            {
                "name": name,
                "value": str(redacted_mapping.get(name, "<redacted>")),
            }
        )
    return {
        "schema_version": "provider-audit-http-headers-v1",
        "items": safe_items,
    }


def _atomic_write_json(path: Path, value: Any) -> None:
    _atomic_write_bytes(path, _canonical_json_bytes(value))


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            redact_payload(value),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _bytes_identity(payload: bytes) -> Mapping[str, Any]:
    return {
        "size_bytes": len(payload),
        "sha256": _sha256_bytes(payload),
    }


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_json_mapping(path: Path) -> Mapping[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, Mapping) else None


def _referenced_audit_tree_identity(
    candidate_pointer: Path,
    latest_pointer: Path,
) -> Mapping[str, Mapping[str, Any]] | None:
    pointer = _read_json_mapping(candidate_pointer)
    if pointer is None:
        return None
    run_id = str(pointer.get("run_id") or "")
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id)
        or run_id in {".", ".."}
    ):
        return None
    audit_root = (
        latest_pointer.parent / "provider-capability-audit"
    ).resolve()
    expected = (audit_root / run_id).resolve()
    try:
        declared = Path(str(pointer["run_directory"])).resolve()
    except (KeyError, OSError, TypeError, ValueError):
        return None
    if (
        declared != expected
        or not declared.is_relative_to(audit_root)
        or not declared.is_dir()
    ):
        return None
    snapshot: dict[str, Mapping[str, Any]] = {}
    try:
        for path in sorted(
            declared.rglob("*"),
            key=lambda item: item.as_posix(),
        ):
            if not path.is_file():
                continue
            resolved = path.resolve()
            if not resolved.is_relative_to(declared):
                return None
            relative = path.relative_to(declared).as_posix()
            snapshot[relative] = _file_identity(resolved)
    except OSError:
        return None
    return snapshot


def _discard_candidate(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        # Publication is already fail-closed.  Cleanup must not hide the
        # validator rejection that prevented replacement of latest.
        pass


def _file_identity(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return {"size_bytes": size, "sha256": digest.hexdigest()}


def _stable_sha256(value: Any) -> str:
    return _sha256_bytes(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_name(value: str) -> str:
    safe = _SAFE_FILE_PART.sub("-", str(value)).strip(".-")
    if not safe:
        raise ValueError("artifact identifier is empty after sanitization")
    return safe[:160]


def _relative(path: Path | None, root: Path) -> str | None:
    if path is None:
        return None
    return path.relative_to(root).as_posix()


def _append_suffix(path: Path, suffix: str) -> Path:
    return path.parent / f"{path.name}{suffix}"


def _contained_path(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError("artifact path escapes the audit directory")
    return candidate


def _markdown_cell(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")


__all__ = [
    "ProviderAuditArtifactWriter",
    "ProviderAuditPublicationRejected",
    "PublicationVerifier",
    "publish_verified_audit_candidate",
]
