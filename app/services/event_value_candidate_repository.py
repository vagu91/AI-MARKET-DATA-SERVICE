from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.data_freshness_service import parse_datetime
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_domain_service import canonical_event_key


class EventValueCandidateRepository:
    """Append-only-ish lineage store for every cross-provider numerical candidate."""

    def __init__(self, settings: Settings, *, policy: SourcePolicyService | None = None) -> None:
        self.settings = settings
        self.policy = policy or SourcePolicyService(settings.source_policy_path)
        migrate_database(settings.database_path)

    def persist_provider_payload(self, payload: dict[str, Any]) -> int:
        source = str(payload.get("source") or payload.get("provider") or "calendar_provider")
        default_url = str(payload.get("source_url") or "")
        count = 0
        for row in payload.get("items") or payload.get("events") or []:
            if not isinstance(row, dict):
                continue
            event_key = canonical_event_key(row)
            release = parse_datetime(row.get("release_at") or row.get("time_utc"))
            for field in ("forecast", "consensus", "previous", "actual"):
                value = row.get(field)
                if value in (None, ""):
                    continue
                source_url = str(row.get(f"{field}_source_url") or row.get("source_url") or default_url)
                candidate = {
                    **row,
                    "field": field,
                    "field_semantics": field,
                    "value": value,
                    "source": source,
                    "source_url": source_url,
                    "canonical_url": row.get("canonical_url") or source_url,
                    "retrieved_at": row.get("retrieved_at") or datetime.now(UTC).replace(microsecond=0).isoformat(),
                }
                decision = self.policy.validate(candidate, field_semantics=field, numerical=True)
                reasons = list(decision.reasons)
                if field == "actual" and release and datetime.now(UTC) < release:
                    reasons.append("future_actual_rejected")
                validation = "accepted" if decision.accepted and not reasons else "rejected"
                self._upsert(event_key, candidate, decision, validation, reasons)
                count += 1
        return count

    def persist_candidate(
        self,
        *,
        event_key: str,
        candidate: dict[str, Any],
        release_at: Any = None,
        expected_metric_id: str | None = None,
        expected_period: str | None = None,
        expected_unit: str | None = None,
    ) -> dict[str, Any]:
        item = dict(candidate)
        field = str(item.get("field") or item.get("field_semantics") or "")
        item["field"] = field
        item["field_semantics"] = field
        item["retrieved_at"] = item.get("retrieved_at") or datetime.now(UTC).replace(microsecond=0).isoformat()
        decision = self.policy.validate(item, field_semantics=field, numerical=field in {"actual", "forecast", "consensus", "previous"})
        reasons = list(decision.reasons)
        release = parse_datetime(release_at)
        if field == "actual" and release and datetime.now(UTC) < release:
            reasons.append("future_actual_rejected")
        if expected_metric_id and str(item.get("metric_id") or "").upper() != str(expected_metric_id).upper():
            reasons.append("metric_id_mismatch")
        if expected_period and _normalized_period(item.get("period")) != _normalized_period(expected_period):
            reasons.append("period_mismatch")
        if expected_unit and _normalized(item.get("unit")) != _normalized(expected_unit):
            reasons.append("unit_mismatch")
        validation = "accepted" if decision.accepted and not reasons else "rejected"
        self._upsert(event_key, item, decision, validation, reasons)
        restored = self.candidate(
            event_key=event_key,
            field_name=field,
            source_url=str(item.get("source_url") or ""),
            value=str(item.get("value")),
            period=item.get("period"),
        )
        if restored is None:
            raise RuntimeError("event value candidate read-back failed")
        return restored

    def candidate(
        self,
        *,
        event_key: str,
        field_name: str,
        source_url: str,
        value: str,
        period: Any,
    ) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM event_value_candidates
                WHERE canonical_event_key=? AND field_name=? AND source_url=? AND value=?
                  AND COALESCE(period,'')=COALESCE(?,'')
                LIMIT 1
                """,
                (event_key, field_name, source_url or "unknown://missing", value, period),
            ).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["payload"] = json.loads(data.pop("payload_json"))
        data["warnings"] = json.loads(data.pop("warnings_json") or "[]")
        data["calculation_lineage"] = json.loads(data.pop("calculation_lineage_json") or "{}")
        return data

    def accepted_official_actual(self, event_key: str) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM event_value_candidates
                WHERE canonical_event_key=? AND field_name='actual'
                  AND validation_status='accepted' AND source_tier=1
                ORDER BY retrieved_at DESC LIMIT 1
                """,
                (event_key,),
            ).fetchone()
        if row is None:
            return None
        result = json.loads(row["payload_json"])
        result.update({
            "field": "actual",
            "source_domain": row["source_domain"],
            "source_tier": row["source_tier"],
            "source_classification": row["source_classification"],
            "validation_status": row["validation_status"],
            "policy_version": row["policy_version"],
        })
        return result

    def _upsert(self, event_key: str, candidate: dict[str, Any], decision, validation: str, reasons: list[str]) -> None:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO event_value_candidates(
                  canonical_event_key,field_name,value,metric_id,period,frequency,unit,source,
                  source_url,source_domain,source_tier,source_classification,evidence_text,
                  reliability,confidence,validation_status,warnings_json,policy_version,retrieved_at,payload_json,
                  event_metric_id,source_series_id,transformation,seasonal_adjustment,reference_period,
                  release_timestamp,release_vintage,calculation_lineage_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(canonical_event_key,field_name,source_url,value,period) DO UPDATE SET
                  validation_status=excluded.validation_status,warnings_json=excluded.warnings_json,
                  retrieved_at=excluded.retrieved_at,payload_json=excluded.payload_json,
                  event_metric_id=excluded.event_metric_id,source_series_id=excluded.source_series_id,
                  transformation=excluded.transformation,seasonal_adjustment=excluded.seasonal_adjustment,
                  reference_period=excluded.reference_period,release_timestamp=excluded.release_timestamp,
                  release_vintage=excluded.release_vintage,calculation_lineage_json=excluded.calculation_lineage_json
                """,
                (
                    event_key, candidate["field"], str(candidate.get("value")), candidate.get("metric_id"),
                    candidate.get("period") or candidate.get("reference_period"), candidate.get("frequency"),
                    candidate.get("unit"), candidate.get("source"), candidate.get("source_url") or "unknown://missing",
                    decision.domain, decision.tier, decision.classification, candidate.get("evidence_text"),
                    float(candidate.get("reliability") or 0), float(candidate.get("confidence") or 0), validation,
                    json.dumps(reasons, sort_keys=True), decision.policy_version,
                    candidate.get("retrieved_at"), json.dumps(candidate, sort_keys=True, default=str),
                    candidate.get("event_metric_id") or candidate.get("metric_id"),
                    candidate.get("source_series_id"), candidate.get("transformation"),
                    candidate.get("seasonal_adjustment"), candidate.get("reference_period") or candidate.get("period"),
                    candidate.get("release_timestamp"), candidate.get("release_vintage"),
                    json.dumps(candidate.get("calculation_lineage") or {}, sort_keys=True, default=str),
                ),
            )
            conn.commit()


def _normalized(value: Any) -> str:
    return " ".join(str(value or "").lower().split())


def _normalized_period(value: Any) -> str:
    text = _normalized(value)
    if len(text) >= 7 and text[:4].isdigit() and text[4] in {"-", "/"}:
        return text[:7].replace("/", "-")
    return text
