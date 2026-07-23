from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.data_freshness_service import parse_datetime
from app.services.temporal_domain_service import canonical_event_key


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

    def validate(
        self,
        item: dict[str, Any],
        *,
        domain: str,
    ) -> tuple[bool, str | None, str | None, str | None]:
        now = _aware(self.clock())
        skew = timedelta(seconds=self.settings.research_clock_skew_seconds)
        for field in ("born_at", "retrieved_at", "published_at", "created_at"):
            value = parse_datetime(item.get(field))
            if value is not None and _aware(value) > now + skew:
                return False, "TIMESTAMP_FUTURE_CLOCK_SKEW", field, value.isoformat()
        event_field = next(
            (
                field
                for field in (
                    "release_at",
                    "decision_at",
                    "event_start_at",
                    "event_at",
                    "time_utc",
                )
                if parse_datetime(item.get(field)) is not None
            ),
            None,
        )
        if event_field:
            event_at = _aware(parse_datetime(item.get(event_field)))  # type: ignore[arg-type]
            horizon_days = (
                self.settings.research_earnings_horizon_days
                if domain == "earnings"
                else self.settings.research_macro_horizon_days
            )
            if event_at > now + timedelta(days=horizon_days):
                return (
                    False,
                    "EVENT_BEYOND_CONFIGURED_HORIZON",
                    event_field,
                    event_at.isoformat(),
                )
        return True, None, None, None

    def audit_economic_events(
        self,
        records: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        valid: list[dict[str, Any]] = []
        quarantined: list[dict[str, Any]] = []
        detected_at = _aware(self.clock()).replace(microsecond=0).isoformat()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            for record in records:
                accepted, reason, field, value = self.validate(record, domain="macro_calendar")
                if accepted:
                    valid.append(record)
                    continue
                entity_key = str(
                    record.get("canonical_event_key")
                    or record.get("event_key")
                    or canonical_event_key(record)
                )
                quarantine_id = "tq-" + hashlib.sha256(
                    f"economic_events_history|{entity_key}|{field}|{reason}".encode("utf-8")
                ).hexdigest()[:24]
                conn.execute(
                    """
                    INSERT OR IGNORE INTO temporal_quarantine(
                      quarantine_id,entity_table,entity_key,domain,timestamp_field,
                      timestamp_value,reason_code,detected_at,details_json
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        quarantine_id,
                        "economic_events_history",
                        entity_key,
                        "macro_calendar",
                        field,
                        value,
                        reason,
                        detected_at,
                        json.dumps(
                            {
                                "event_id": record.get("event_id"),
                                "category": record.get("category"),
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
                conn.execute(
                    """
                    UPDATE economic_events_history
                    SET temporal_audit_status='QUARANTINED',
                        temporal_invalid_reason=?
                    WHERE canonical_event_key=? OR event_key=?
                    """,
                    (reason, entity_key, entity_key),
                )
                quarantined.append(
                    {
                        "entity_key": entity_key,
                        "reason_code": reason,
                        "timestamp_field": field,
                        "timestamp_value": value,
                    }
                )
            conn.commit()
        return valid, quarantined


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
