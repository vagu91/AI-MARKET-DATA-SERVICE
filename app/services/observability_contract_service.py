from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from app.core.config import Settings
from app.core.redaction import redact_payload, redact_sensitive
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.data_freshness_service import parse_datetime
from app.services.source_policy_service import SourcePolicyService


TELEMETRY_EVENTS = frozenset(
    {
        "enqueue",
        "dequeue",
        "lease",
        "heartbeat",
        "provider_call",
        "ai_invocation_attempted",
        "ai_invocation_completed",
        "ai_invocation_aborted",
        "search",
        "source_discovery",
        "fetch",
        "redirect",
        "verification",
        "extraction",
        "claim_acceptance",
        "claim_rejection",
        "persistence_transaction",
        "read_back",
        "materialization",
        "material_diff",
        "outbox_emission",
        "retry_backoff",
        "loop_emergency_ceiling",
    }
)
IDENTIFIER_FIELDS = (
    "trace_id",
    "span_id",
    "parent_span_id",
    "correlation_id",
    "parent_run_id",
    "child_job_id",
    "child_run_id",
    "invocation_id",
    "tool_call_id",
    "source_id",
    "verification_id",
    "claim_id",
    "snapshot_id",
    "outbox_event_id",
)


class TelemetryRepository:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        migrate_database(settings.database_path)

    def emit(
        self,
        event_name: str,
        *,
        identifiers: dict[str, Any] | None = None,
        decision_summary: str | None = None,
        evidence_ids: list[str] | None = None,
        confidence: float | None = None,
        rejection_reasons: list[str] | None = None,
        stop_reason: str | None = None,
        input_schema_version: str | None = None,
        output_schema_version: str | None = None,
        error: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if event_name not in TELEMETRY_EVENTS:
            raise ValueError("unsupported_telemetry_event")
        identifiers = dict(identifiers or {})
        redacted = redact_payload(payload or {})
        if not self.settings.telemetry_trace_detail_enabled:
            redacted = {
                key: redacted.get(key)
                for key in (
                    "status",
                    "reason",
                    "duration_ms",
                    "input_tokens",
                    "output_tokens",
                    "cached_tokens",
                    "cost_status",
                )
                if key in redacted
            }
        redacted = _bounded_payload(redacted)
        contract = {
            "event_name": event_name,
            "occurred_at": _iso(self.clock()),
            **{
                field: _identifier(identifiers.get(field))
                for field in IDENTIFIER_FIELDS
            },
            "decision_summary": _safe_text(decision_summary, 1000),
            "evidence_ids": [
                _identifier(item) for item in (evidence_ids or [])[:100]
            ],
            "confidence": (
                min(max(float(confidence), 0), 1)
                if confidence is not None
                else None
            ),
            "rejection_reasons": [
                _safe_text(item, 300)
                for item in (rejection_reasons or [])[:100]
            ],
            "stop_reason": _safe_text(stop_reason, 300),
            "input_schema_version": _safe_text(input_schema_version, 80),
            "output_schema_version": _safe_text(output_schema_version, 80),
            "redacted_error": _safe_text(
                redact_sensitive(str(error)) if error else None,
                1000,
            ),
        }
        payload_hash = hashlib.sha256(
            _json(redacted).encode("utf-8")
        ).hexdigest()
        contract["payload_hash"] = payload_hash
        telemetry_id = f"telemetry-{uuid.uuid4()}"
        expires_at = _iso(
            self.clock()
            + timedelta(days=int(self.settings.telemetry_retention_days))
        )
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO service_telemetry_events(
                  telemetry_id,event_name,occurred_at,trace_id,span_id,
                  parent_span_id,correlation_id,parent_run_id,child_job_id,
                  child_run_id,invocation_id,tool_call_id,source_id,
                  verification_id,claim_id,snapshot_id,outbox_event_id,
                  decision_summary,confidence,stop_reason,input_schema_version,
                  output_schema_version,payload_hash,payload_json,expires_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    telemetry_id,
                    event_name,
                    contract["occurred_at"],
                    *[contract[field] for field in IDENTIFIER_FIELDS],
                    contract["decision_summary"],
                    contract["confidence"],
                    contract["stop_reason"],
                    contract["input_schema_version"],
                    contract["output_schema_version"],
                    payload_hash,
                    _json({**contract, "payload": redacted}),
                    expires_at,
                ),
            )
            conn.commit()
        return {"telemetry_id": telemetry_id, **contract, "payload": redacted}

    def purge_expired(self, *, now: datetime | None = None) -> int:
        reference = _iso(now or self.clock())
        with connect_sqlite(self.settings.database_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM service_telemetry_events
                WHERE expires_at IS NOT NULL AND expires_at<=?
                """,
                (reference,),
            )
            conn.commit()
        return int(cursor.rowcount or 0)


class ModelPricingService:
    def __init__(self, path: Path) -> None:
        self.path = path

    def estimate(
        self,
        *,
        backend: str,
        model: str | None,
        input_tokens: int,
        cached_tokens: int,
        output_tokens: int,
    ) -> dict[str, Any]:
        if backend == "codex_cli":
            return {
                "cost": None,
                "cost_status": "unavailable",
                "billing_basis": "tokens_observed_codex_cli_pricing_unavailable",
            }
        pricing = self._pricing(backend, model)
        if pricing is None:
            return {
                "cost": None,
                "cost_status": "pricing_unavailable",
                "billing_basis": "versioned_pricing_table_no_matching_model",
            }
        uncached = max(int(input_tokens) - int(cached_tokens), 0)
        cost = (
            uncached * float(pricing["input_per_million"])
            + int(cached_tokens)
            * float(
                pricing.get("cached_input_per_million")
                or pricing["input_per_million"]
            )
            + int(output_tokens) * float(pricing["output_per_million"])
        ) / 1_000_000
        return {
            "cost": round(cost, 10),
            "cost_status": "estimated",
            "billing_basis": {
                "pricing_version": pricing.get("source_version"),
                "backend": backend,
                "model": model,
                "currency": pricing.get("currency") or "USD",
            },
        }

    def _pricing(
        self,
        backend: str,
        model: str | None,
    ) -> dict[str, Any] | None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        matches = [
            item
            for item in payload.get("models") or []
            if str(item.get("backend")) == backend
            and str(item.get("model")) == str(model)
        ]
        if not matches:
            return None
        selected = matches[-1]
        return {
            **selected,
            "source_version": payload.get("version"),
            "currency": payload.get("currency"),
        }


class DeterministicAnomalyDetector:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.policy = SourcePolicyService(settings.source_policy_path)
        migrate_database(settings.database_path)

    def detect(self, signals: dict[str, Any]) -> list[dict[str, Any]]:
        now = self.clock()
        findings: list[tuple[str, str, dict[str, Any]]] = []
        lease = parse_datetime(signals.get("lease_expires_at"))
        if signals.get("job_status") == "RUNNING" and lease and lease < now:
            findings.append(("HIGH", "lease_expired", {"lease_expires_at": lease}))
        if signals.get("job_status") == "RUNNING" and signals.get("stalled"):
            findings.append(("HIGH", "job_stuck", {"job_id": signals.get("job_id")}))
        if signals.get("loop_detected"):
            findings.append(("HIGH", "loop", {"count": signals.get("loop_count")}))
        retry = parse_datetime(signals.get("next_retry_at"))
        if signals.get("no_data_attempted") and retry and retry > now:
            findings.append(
                ("MEDIUM", "no_data_before_next_retry", {"next_retry_at": retry})
            )
        if int(signals.get("total_tokens") or 0) > int(
            signals.get("token_threshold") or 500_000
        ):
            findings.append(
                ("MEDIUM", "token_anomaly", {"total_tokens": signals.get("total_tokens")})
            )
        if int(signals.get("duration_ms") or 0) > int(
            signals.get("duration_threshold_ms") or 300_000
        ):
            findings.append(
                ("MEDIUM", "duration_anomaly", {"duration_ms": signals.get("duration_ms")})
            )
        if int(signals.get("rejected_claims") or 0) != int(
            signals.get("rejection_reason_count") or 0
        ):
            findings.append(
                (
                    "HIGH",
                    "rejection_accounting_mismatch",
                    {
                        "rejected_claims": signals.get("rejected_claims"),
                        "rejection_reason_count": signals.get(
                            "rejection_reason_count"
                        ),
                    },
                )
            )
        if signals.get("warning_accounting_mismatch"):
            findings.append(("MEDIUM", "warning_accounting_mismatch", {}))
        if signals.get("source_unavailable"):
            findings.append(("MEDIUM", "source_unavailable", {}))
        if signals.get("pending_actual_overdue"):
            findings.append(("HIGH", "pending_actual_overdue", {}))
        if int(signals.get("accepted_claims") or 0) > int(
            signals.get("projected_claims") or 0
        ):
            findings.append(("HIGH", "accepted_claim_not_projected", {}))
        if signals.get("consumer_readiness_regression"):
            findings.append(("HIGH", "consumer_readiness_regression", {}))
        for field, value in (signals.get("timestamps") or {}).items():
            parsed = parse_datetime(value)
            if parsed and parsed > now + timedelta(days=2):
                findings.append(
                    ("HIGH", "future_timestamp", {"field": field, "value": value})
                )
        for url in signals.get("urls") or []:
            validation = self.policy.validate_url(str(url))
            if validation.reason_code in {
                "test_reserved_domain",
                "loopback_host",
                "localhost_host",
                "SOURCE_HOST_RESERVED",
                "SOURCE_HOST_LOCALHOST",
                "SOURCE_HOST_NON_PUBLIC_IP",
            }:
                findings.append(
                    ("HIGH", "test_or_localhost_domain", {"url": validation.url})
                )
        if int(signals.get("read_only_write_count") or 0) > 0:
            findings.append(("CRITICAL", "read_only_endpoint_write", {}))
        if int(signals.get("outbox_lag_seconds") or 0) > int(
            signals.get("outbox_lag_threshold_seconds") or 300
        ):
            findings.append(("MEDIUM", "outbox_lag", {}))
        if signals.get("backend_contract_divergence"):
            findings.append(("HIGH", "cli_api_divergence", {}))
        return [
            self._persist(
                severity,
                category,
                evidence,
                signals=signals,
                now=now,
            )
            for severity, category, evidence in findings
        ]

    def _persist(
        self,
        severity: str,
        category: str,
        evidence: dict[str, Any],
        *,
        signals: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        entity_ids = sorted(
            {
                str(signals[key])
                for key in (
                    "job_id",
                    "parent_run_id",
                    "child_run_id",
                    "snapshot_id",
                    "outbox_event_id",
                )
                if signals.get(key)
            }
        )
        fingerprint = hashlib.sha256(
            _json(
                {
                    "category": category,
                    "entity_ids": entity_ids,
                    "evidence": redact_payload(evidence),
                }
            ).encode("utf-8")
        ).hexdigest()
        incident_id = f"incident-{uuid.uuid5(uuid.NAMESPACE_URL, fingerprint)}"
        timestamp = _iso(now)
        trace_ids = sorted(
            {str(item) for item in signals.get("trace_ids") or [] if item}
        )
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO anomaly_incidents(
                  incident_id,fingerprint,severity,category,first_seen_at,
                  last_seen_at,occurrence_count,trace_ids_json,entity_ids_json,
                  evidence_json,status,resolution_json
                ) VALUES (?,?,?,?,?,?,1,?,?,?,'OPEN',NULL)
                ON CONFLICT(fingerprint) DO UPDATE SET
                  last_seen_at=excluded.last_seen_at,
                  occurrence_count=anomaly_incidents.occurrence_count+1,
                  trace_ids_json=excluded.trace_ids_json,
                  entity_ids_json=excluded.entity_ids_json,
                  evidence_json=excluded.evidence_json
                """,
                (
                    incident_id,
                    fingerprint,
                    severity,
                    category,
                    timestamp,
                    timestamp,
                    _json(trace_ids),
                    _json(entity_ids),
                    _json(redact_payload(evidence)),
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM anomaly_incidents WHERE fingerprint=?",
                (fingerprint,),
            ).fetchone()
        output = dict(row)
        output["trace_ids"] = json.loads(output.pop("trace_ids_json") or "[]")
        output["entity_ids"] = json.loads(output.pop("entity_ids_json") or "[]")
        output["evidence"] = json.loads(output.pop("evidence_json") or "{}")
        output["resolution"] = (
            json.loads(output.pop("resolution_json"))
            if output.get("resolution_json")
            else None
        )
        return output


def _bounded_payload(value: dict[str, Any]) -> dict[str, Any]:
    encoded = _json(value)
    if len(encoded.encode("utf-8")) <= 32_000:
        return value
    return {
        "truncated": True,
        "payload_hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
    }


def _identifier(value: Any) -> str | None:
    return _safe_text(value, 160)


def _safe_text(value: Any, limit: int) -> str | None:
    if value in (None, ""):
        return None
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", str(value))
    return text.replace("\r", " ").replace("\n", " ")[:limit]


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _iso(value: datetime) -> str:
    return (
        value.astimezone(UTC)
        if value.tzinfo
        else value.replace(tzinfo=UTC)
    ).replace(microsecond=0).isoformat()
