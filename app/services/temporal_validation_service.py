from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.data_freshness_service import parse_datetime


QUARANTINED_STATUS = "QUARANTINED"
EVENT_HORIZON_REASON = "EVENT_BEYOND_CONFIGURED_HORIZON"


@dataclass(frozen=True)
class TemporalDecision:
    accepted: bool
    reason_code: str | None = None
    timestamp_field: str | None = None
    timestamp_value: str | None = None


class TemporalPolicy:
    """One domain-aware policy shared by ingestion, persistence, reads and research."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        clock_skew_seconds: int = 300,
        economic_event_max_future_days: int = 550,
        earnings_max_future_days: int = 400,
    ) -> None:
        self.clock = clock or (lambda: datetime.now(UTC))
        self.clock_skew_seconds = int(clock_skew_seconds)
        self.economic_event_max_future_days = int(economic_event_max_future_days)
        self.earnings_max_future_days = int(earnings_max_future_days)

    def evaluate(self, item: dict[str, Any], *, domain: str) -> TemporalDecision:
        existing_reason = item.get("temporal_invalid_reason")
        existing_status = str(
            item.get("temporal_audit_status") or item.get("temporal_status") or ""
        ).upper()
        if existing_status in {QUARANTINED_STATUS, "TEMPORALLY_INVALID"}:
            field, value = self._event_timestamp(item)
            return TemporalDecision(
                False,
                str(existing_reason or EVENT_HORIZON_REASON),
                field,
                value,
            )
        now = _aware(self.clock())
        skew = timedelta(seconds=self.clock_skew_seconds)
        for field in ("born_at", "retrieved_at", "published_at", "created_at"):
            value = parse_datetime(item.get(field))
            if value is not None and _aware(value) > now + skew:
                return TemporalDecision(
                    False,
                    "TIMESTAMP_FUTURE_CLOCK_SKEW",
                    field,
                    _aware(value).isoformat(),
                )
        event_field, event_value = self._event_timestamp(item)
        event_at = parse_datetime(event_value)
        if event_field and event_at is not None:
            horizon_days = (
                self.earnings_max_future_days
                if domain == "earnings"
                else self.economic_event_max_future_days
            )
            if _aware(event_at) > now + timedelta(days=horizon_days):
                return TemporalDecision(
                    False,
                    EVENT_HORIZON_REASON,
                    event_field,
                    _aware(event_at).isoformat(),
                )
        return TemporalDecision(True)

    @staticmethod
    def _event_timestamp(item: dict[str, Any]) -> tuple[str | None, str | None]:
        for field in (
            "release_at",
            "decision_at",
            "event_start_at",
            "event_at",
            "time_utc",
            "date",
        ):
            value = item.get(field)
            if parse_datetime(value) is not None:
                return field, str(value)
        return None, None


class TemporalValidationService:
    """Service-owned temporal audit. Invalid rows remain stored but are not selectable."""

    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.policy = TemporalPolicy(
            clock=self.clock,
            clock_skew_seconds=settings.research_clock_skew_seconds,
            economic_event_max_future_days=settings.economic_event_max_future_days,
            earnings_max_future_days=settings.research_earnings_horizon_days,
        )

    def validate(
        self,
        item: dict[str, Any],
        *,
        domain: str,
    ) -> tuple[bool, str | None, str | None, str | None]:
        decision = self.policy.evaluate(item, domain=domain)
        return (
            decision.accepted,
            decision.reason_code,
            decision.timestamp_field,
            decision.timestamp_value,
        )

    def is_active(self, item: dict[str, Any], *, domain: str = "macro_calendar") -> bool:
        return self.policy.evaluate(item, domain=domain).accepted

    def quarantine_if_invalid(
        self,
        item: dict[str, Any],
        *,
        entity_table: str,
        domain: str = "macro_calendar",
    ) -> bool:
        decision = self.policy.evaluate(item, domain=domain)
        if decision.accepted:
            return False
        self._persist_payload_quarantine(
            item,
            entity_table=entity_table,
            domain=domain,
            decision=decision,
        )
        return True

    def record_quarantine(
        self,
        conn: Any,
        record: dict[str, Any],
        *,
        entity_table: str,
        entity_key: str,
        domain: str,
        decision: TemporalDecision | None = None,
    ) -> dict[str, Any] | None:
        decision = decision or self.policy.evaluate(record, domain=domain)
        if decision.accepted:
            return None
        reason = str(decision.reason_code or "TEMPORALLY_INVALID")
        field = str(decision.timestamp_field or "unknown")
        value = str(decision.timestamp_value or record.get(field) or "")
        detected_at = _aware(self.clock()).replace(microsecond=0).isoformat()
        quarantine_id = quarantine_identity(
            entity_table=entity_table,
            entity_key=entity_key,
            timestamp_field=field,
            reason_code=reason,
        )
        details = quarantine_details(record)
        conn.execute(
            """
            INSERT INTO temporal_quarantine(
              quarantine_id,entity_table,entity_key,domain,timestamp_field,
              timestamp_value,reason_code,detected_at,details_json
            ) VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(entity_table,entity_key,timestamp_field,reason_code)
            DO NOTHING
            """,
            (
                quarantine_id,
                entity_table,
                entity_key,
                domain,
                field,
                value,
                reason,
                detected_at,
                json.dumps(
                    details,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
            ),
        )
        return {
            "quarantine_id": quarantine_id,
            "entity_table": entity_table,
            "entity_key": entity_key,
            "domain": domain,
            "timestamp_field": field,
            "timestamp_value": value,
            "reason_code": reason,
            "detected_at": detected_at,
        }

    def audit_economic_events(
        self,
        records: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        valid: list[dict[str, Any]] = []
        quarantined: list[dict[str, Any]] = []
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            for record in records:
                decision = self.policy.evaluate(record, domain="macro_calendar")
                if decision.accepted:
                    valid.append(record)
                    continue
                from app.services.temporal_domain_service import canonical_event_key

                entity_key = str(
                    record.get("event_key")
                    or record.get("canonical_event_key")
                    or canonical_event_key(record)
                )
                item = self.record_quarantine(
                    conn,
                    record,
                    entity_table="economic_events_history",
                    entity_key=entity_key,
                    domain="macro_calendar",
                    decision=decision,
                )
                conn.execute(
                    """
                    UPDATE economic_events_history
                    SET temporal_audit_status='QUARANTINED',
                        temporal_status='QUARANTINED',
                        status='QUARANTINED',
                        temporal_invalid_reason=?
                    WHERE event_key=?
                    """,
                    (decision.reason_code, entity_key),
                )
                if item:
                    quarantined.append(item)
            conn.commit()
        return valid, quarantined

    def sanitize_payload(
        self,
        payload: Any,
        *,
        entity_table: str = "market_context_input",
        path: tuple[str, ...] = (),
        persist: bool = True,
    ) -> Any:
        """Remove invalid event-shaped records from operational trees, retaining audit rows."""
        if isinstance(payload, list):
            output: list[Any] = []
            for item in payload:
                if isinstance(item, dict) and _event_shaped(item):
                    domain = "earnings" if "earnings" in path else "macro_calendar"
                    decision = self.policy.evaluate(item, domain=domain)
                    if not decision.accepted:
                        if persist:
                            self._persist_payload_quarantine(
                                item,
                                entity_table=entity_table,
                                domain=domain,
                                decision=decision,
                            )
                        continue
                output.append(
                    self.sanitize_payload(
                        item,
                        entity_table=entity_table,
                        path=path,
                        persist=persist,
                    )
                )
            return output
        if isinstance(payload, dict):
            if any(part in {"audit", "quarantine", "temporal_quarantine"} for part in path):
                return dict(payload)
            if path and _event_shaped(payload):
                domain = "earnings" if "earnings" in path else "macro_calendar"
                decision = self.policy.evaluate(payload, domain=domain)
                if not decision.accepted:
                    if persist:
                        self._persist_payload_quarantine(
                            payload,
                            entity_table=entity_table,
                            domain=domain,
                            decision=decision,
                        )
                    return None
            return {
                key: self.sanitize_payload(
                    value,
                    entity_table=entity_table,
                    path=(*path, str(key).lower()),
                    persist=persist,
                )
                for key, value in payload.items()
            }
        return payload

    def _persist_payload_quarantine(
        self,
        item: dict[str, Any],
        *,
        entity_table: str,
        domain: str,
        decision: TemporalDecision,
    ) -> None:
        from app.services.temporal_domain_service import canonical_event_key

        entity_key = str(
            item.get("event_key")
            or item.get("canonical_event_key")
            or item.get("event_id")
            or canonical_event_key(item)
        )
        with connect_sqlite(self.settings.database_path) as conn:
            self.record_quarantine(
                conn,
                item,
                entity_table=entity_table,
                entity_key=entity_key,
                domain=domain,
                decision=decision,
            )
            conn.commit()

    def quarantine_summary(self) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            total = int(
                conn.execute("SELECT COUNT(*) FROM temporal_quarantine").fetchone()[0]
            )
            rows = conn.execute(
                """
                SELECT domain,reason_code,COUNT(*) AS count
                FROM temporal_quarantine
                GROUP BY domain,reason_code
                ORDER BY domain,reason_code
                """
            ).fetchall()
            latest = conn.execute(
                "SELECT MAX(detected_at) FROM temporal_quarantine"
            ).fetchone()[0]
            reconciliation = conn.execute(
                """
                SELECT errors_json FROM temporal_reconciliation_runs
                ORDER BY completed_at DESC LIMIT 1
                """
            ).fetchone()
        return {
            "total": total,
            "by_domain_reason": [
                {
                    "domain": row["domain"],
                    "reason_code": row["reason_code"],
                    "count": int(row["count"]),
                }
                for row in rows
            ],
            "last_detected_at": latest,
            "reconciliation_errors": (
                _decoded(reconciliation["errors_json"]) if reconciliation else []
            ),
        }

    def quarantine_read_model(self, *, limit: int = 100) -> dict[str, Any]:
        summary = self.quarantine_summary()
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT quarantine_id,entity_table,entity_key,domain,timestamp_field,
                       timestamp_value,reason_code,detected_at
                FROM temporal_quarantine
                ORDER BY detected_at DESC,quarantine_id
                LIMIT ?
                """,
                (max(int(limit), 1),),
            ).fetchall()
        return {
            **summary,
            "items": [dict(row) for row in rows],
        }


def normalize_event_semantics(item: dict[str, Any]) -> dict[str, Any]:
    output = dict(item)
    category = str(output.get("category") or output.get("event_type") or "").upper()
    kind = str(output.get("event_kind") or "").lower()
    if "FOMC" in category:
        event_type = "FOMC_MEETING"
    elif "BOARD" in category or kind == "closed_board_meeting":
        event_type = "FED_BOARD_MEETING"
    elif "SPEECH" in category or kind == "scheduled_speech":
        event_type = "FED_SPEECH"
    elif "EARNING" in category:
        event_type = "EARNINGS_EVENT"
    elif "ISSUER" in category:
        event_type = "ISSUER_ANNOUNCEMENT"
    else:
        event_type = "ECONOMIC_RELEASE"
    output["event_type"] = event_type
    output["event_start_at"] = (
        output.get("event_start_at")
        or output.get("release_at")
        or output.get("time_utc")
    )
    output["event_end_at"] = output.get("event_end_at") or output["event_start_at"]
    if event_type == "FOMC_MEETING":
        output["decision_at"] = output.get("decision_at") or output.get("release_at")
    if event_type == "FED_BOARD_MEETING":
        output["post_event_semantics"] = "outcome"
    return output


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _decoded(value: Any) -> Any:
    if not isinstance(value, str):
        return value or {}
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return {}


def _audit_payload(record: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "event_id",
        "event_key",
        "canonical_event_key",
        "country",
        "category",
        "name",
        "event_name",
        "date",
        "release_at",
        "time_utc",
        "status",
        "temporal_status",
        "source",
        "source_url",
        "reference_period",
        "period",
    }
    return {key: record.get(key) for key in allowed if key in record}


def _event_shaped(item: dict[str, Any]) -> bool:
    has_identity = any(
        item.get(key)
        for key in ("event_id", "event_key", "canonical_event_key", "event_name", "name")
    )
    has_timestamp = any(
        item.get(key)
        for key in ("release_at", "time_utc", "event_at", "event_start_at", "decision_at", "date")
    )
    return has_identity and has_timestamp


def quarantine_identity(
    *,
    entity_table: str,
    entity_key: str,
    timestamp_field: str,
    reason_code: str,
) -> str:
    return "tq-" + hashlib.sha256(
        f"{entity_table}|{entity_key}|{timestamp_field}|{reason_code}".encode("utf-8")
    ).hexdigest()[:24]


def quarantine_details(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "original_state": {
            key: record.get(key)
            for key in (
                "event_id",
                "event_key",
                "canonical_event_key",
                "date",
                "release_at",
                "time_utc",
                "status",
                "temporal_status",
                "temporal_audit_status",
                "temporal_invalid_reason",
            )
        },
        "source": record.get("source"),
        "source_url": record.get("source_url"),
        "lineage": _decoded(
            record.get("field_lineage") or record.get("field_lineage_json")
        ),
        "raw_payload": _audit_payload(record),
    }
