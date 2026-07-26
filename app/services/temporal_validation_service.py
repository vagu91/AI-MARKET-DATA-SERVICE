from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.data_freshness_service import parse_datetime


QUARANTINED_STATUS = "QUARANTINED"
EVENT_HORIZON_REASON = "EVENT_BEYOND_CONFIGURED_HORIZON"
RELEASE_ON_IMPLAUSIBLE_WEEKEND = "RELEASE_ON_IMPLAUSIBLE_WEEKEND"
REFERENCE_PERIOD_AFTER_RELEASE_DATE = "REFERENCE_PERIOD_AFTER_RELEASE_DATE"
PERIOD_RELEASE_DATE_INCONSISTENT = "PERIOD_RELEASE_DATE_INCONSISTENT"
SCHEDULE_DATE_UNVERIFIED = "SCHEDULE_DATE_UNVERIFIED"
SOURCE_OCCURRENCE_AMBIGUOUS = "SOURCE_OCCURRENCE_AMBIGUOUS"

_OFFICIAL_MACRO_SOURCES = ("BLS", "BEA", "CENSUS")
_MONTHS = {
    name.lower(): index
    for index, name in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        start=1,
    )
}


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
            semantic = self._semantic_event_decision(
                item,
                event_field=event_field,
                event_at=_aware(event_at),
                domain=domain,
            )
            if semantic is not None:
                return semantic
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

    def _semantic_event_decision(
        self,
        item: dict[str, Any],
        *,
        event_field: str,
        event_at: datetime,
        domain: str,
    ) -> TemporalDecision | None:
        if domain != "macro_calendar":
            return None
        event_value = event_at.isoformat()
        timezone_name = item.get("source_timezone") or item.get("timezone")
        if timezone_name:
            try:
                ZoneInfo(str(timezone_name))
            except ZoneInfoNotFoundError:
                return TemporalDecision(
                    False,
                    SCHEDULE_DATE_UNVERIFIED,
                    event_field,
                    event_value,
                )
        precision = str(
            item.get("scheduled_time_precision")
            or item.get("time_precision")
            or ""
        ).upper()
        if precision in {"UNKNOWN", "INVALID"}:
            return TemporalDecision(
                False,
                SCHEDULE_DATE_UNVERIFIED,
                event_field,
                event_value,
            )
        validation = item.get("schedule_validation") or item.get(
            "temporal_validation"
        ) or item.get("validation")
        validation_status = str(
            validation.get("status")
            if isinstance(validation, dict)
            else item.get("schedule_validation_status")
            or item.get("temporal_validation_status")
            or ""
        ).upper()
        if (
            validation_status in {"REJECTED", "INVALID", "QUARANTINED"}
            and _is_schedule_validation(item, validation)
        ):
            return TemporalDecision(
                False,
                SCHEDULE_DATE_UNVERIFIED,
                event_field,
                event_value,
            )
        verification = str(
            item.get("schedule_verification_status")
            or item.get("calendar_verification_status")
            or ""
        ).upper()
        if verification in {"UNVERIFIED", "UNKNOWN", "FAILED", "TIMEOUT"}:
            return TemporalDecision(
                False,
                SCHEDULE_DATE_UNVERIFIED,
                event_field,
                event_value,
            )
        if item.get("source_occurrence_ambiguous") is True:
            return TemporalDecision(
                False,
                SOURCE_OCCURRENCE_AMBIGUOUS,
                event_field,
                event_value,
            )

        source = str(
            item.get("provider")
            or item.get("source")
            or ""
        ).upper()
        official_macro = any(token in source for token in _OFFICIAL_MACRO_SOURCES)
        source_event_at = _source_local_time(item, event_at)
        validation_checks = (
            validation.get("checks")
            if isinstance(validation, dict)
            else []
        ) or []
        weekday_is_authoritative = (
            "CALENDAR" in source
            or "SCHEDULE" in source
            or "weekday" in validation_checks
            or item.get("weekday_verified") is True
        )
        if (
            official_macro
            and source_event_at.weekday() >= 5
            and weekday_is_authoritative
            and not _weekend_release_expected(item)
        ):
            return TemporalDecision(
                False,
                RELEASE_ON_IMPLAUSIBLE_WEEKEND,
                event_field,
                event_value,
            )

        period = _reference_period(item)
        if period is None:
            return None
        period_start, _period_end, frequency = period
        release_date = source_event_at.date()
        if (
            frequency in {"monthly", "quarterly"}
            and period_start > release_date
        ):
            return TemporalDecision(
                False,
                REFERENCE_PERIOD_AFTER_RELEASE_DATE,
                event_field,
                event_value,
            )
        maximum_lag_days = {
            "monthly": 550,
            "quarterly": 1100,
            "annual": 2200,
        }.get(frequency)
        if (
            maximum_lag_days is not None
            and release_date - period_start > timedelta(days=maximum_lag_days)
        ):
            return TemporalDecision(
                False,
                PERIOD_RELEASE_DATE_INCONSISTENT,
                event_field,
                event_value,
            )
        return None

    @staticmethod
    def _event_timestamp(item: dict[str, Any]) -> tuple[str | None, str | None]:
        for field in (
            "release_at",
            "scheduled_at",
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


def _weekend_release_expected(item: dict[str, Any]) -> bool:
    if item.get("weekend_release_expected") is True:
        return True
    semantics = str(
        item.get("schedule_semantics")
        or item.get("event_kind")
        or item.get("event_type")
        or item.get("category")
        or ""
    ).upper()
    return any(
        token in semantics
        for token in (
            "WEEKEND_EXPECTED",
            "GEOPOLITICAL",
            "ELECTION",
            "SCHEDULED_SPEECH",
        )
    )


def _is_schedule_validation(
    item: dict[str, Any],
    validation: Any,
) -> bool:
    if item.get("schedule_validation") or item.get("temporal_validation"):
        return True
    if item.get("schedule_validation_status") or item.get(
        "temporal_validation_status"
    ):
        return True
    if not isinstance(validation, dict):
        return False
    validation_domain = str(
        validation.get("domain")
        or validation.get("validation_domain")
        or ""
    ).upper()
    if validation_domain in {
        "CALENDAR",
        "MACRO_CALENDAR",
        "SCHEDULE",
        "TEMPORAL",
    }:
        return True
    reason_codes = {
        str(value).upper()
        for value in (
            validation.get("reason_code"),
            *(validation.get("reasons") or []),
        )
        if value
    }
    return bool(
        reason_codes
        & {
            RELEASE_ON_IMPLAUSIBLE_WEEKEND,
            REFERENCE_PERIOD_AFTER_RELEASE_DATE,
            PERIOD_RELEASE_DATE_INCONSISTENT,
            SCHEDULE_DATE_UNVERIFIED,
            SOURCE_OCCURRENCE_AMBIGUOUS,
        }
    )


def _source_local_time(
    item: dict[str, Any],
    event_at: datetime,
) -> datetime:
    timezone_name = str(
        item.get("source_timezone")
        or item.get("timezone")
        or "America/New_York"
    )
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        timezone = ZoneInfo("America/New_York")
    return event_at.astimezone(timezone)


def _reference_period(
    item: dict[str, Any],
) -> tuple[date, date, str] | None:
    raw = str(
        item.get("reference_period")
        or item.get("period")
        or _period_from_title(item)
        or ""
    ).strip()
    if not raw:
        return None
    month = re.search(
        r"\b(" + "|".join(_MONTHS) + r")\s+(\d{4})\b",
        raw,
        re.IGNORECASE,
    )
    if month:
        year = int(month.group(2))
        month_number = _MONTHS[month.group(1).lower()]
        start = date(year, month_number, 1)
        next_month = (
            date(year + 1, 1, 1)
            if month_number == 12
            else date(year, month_number + 1, 1)
        )
        return start, next_month - timedelta(days=1), "monthly"
    iso_month = re.fullmatch(r"(\d{4})-(\d{2})", raw)
    if iso_month:
        year = int(iso_month.group(1))
        month_number = int(iso_month.group(2))
        if not 1 <= month_number <= 12:
            return None
        start = date(year, month_number, 1)
        next_month = (
            date(year + 1, 1, 1)
            if month_number == 12
            else date(year, month_number + 1, 1)
        )
        return start, next_month - timedelta(days=1), "monthly"
    quarter = re.search(
        r"\b(?:Q([1-4])|(?:First|Second|Third|Fourth)\s+Quarter)\s+(\d{4})\b",
        raw,
        re.IGNORECASE,
    )
    if quarter:
        quarter_number = (
            int(quarter.group(1))
            if quarter.group(1)
            else {
                "first": 1,
                "second": 2,
                "third": 3,
                "fourth": 4,
            }[quarter.group(0).split()[0].lower()]
        )
        year = int(quarter.group(2))
        start_month = 1 + (quarter_number - 1) * 3
        start = date(year, start_month, 1)
        next_quarter = (
            date(year + 1, 1, 1)
            if quarter_number == 4
            else date(year, start_month + 3, 1)
        )
        return start, next_quarter - timedelta(days=1), "quarterly"
    annual = re.search(r"\bAnnual\s+(\d{4})\b", raw, re.IGNORECASE)
    if annual:
        year = int(annual.group(1))
        return date(year, 1, 1), date(year, 12, 31), "annual"
    return None


def _period_from_title(item: dict[str, Any]) -> str | None:
    title = str(
        item.get("name")
        or item.get("event_name")
        or item.get("title")
        or ""
    )
    match = re.search(r"\(([^()]*(?:\d{4}|Annual)[^()]*)\)\s*$", title)
    return match.group(1) if match else None


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
