from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Literal

from app.core.config import Settings


CanonicalDataReferenceMode = Literal[
    "official_release",
    "point_in_time",
    "event_occurrence",
]


@dataclass(frozen=True)
class FreshnessResult:
    usable: bool
    cache_status: str
    warnings: list[str]
    stale: bool = False


@dataclass(frozen=True)
class CanonicalFreshnessResult:
    found: bool
    usable: bool
    expired: bool
    complete: bool
    evaluation: str
    reason_code: str
    data_as_of: Any = None
    content_valid_until: str | None = None
    refresh_due_at: str | None = None
    lifecycle: str | None = None
    evaluated_at: str | None = None


@dataclass(frozen=True)
class CanonicalFreshnessPolicy:
    max_age: timedelta
    data_reference_mode: CanonicalDataReferenceMode = "official_release"
    data_as_of_fields: tuple[str, ...] | None = None


_REFERENCE_FIELDS: dict[CanonicalDataReferenceMode, tuple[str, ...]] = {
    "official_release": (
        "database_data_as_of",
        "data_as_of",
        "reference_period",
        "report_date",
        "survey_date",
        "release_at",
        "published_at",
    ),
    "point_in_time": (
        "database_data_as_of",
        "data_as_of",
        "observed_at",
        "weight_as_of",
        "as_of",
        "retrieved_at",
        "last_successful_refresh_at",
    ),
    "event_occurrence": (
        "database_data_as_of",
        "event_at",
        "release_at",
        "time_utc",
        "scheduled_at_utc",
        "date",
        "data_as_of",
    ),
}
_CONTENT_DEADLINE_FIELDS = (
    "database_content_valid_until",
    "content_valid_until",
    "valid_until",
)
_REFRESH_DEADLINE_FIELDS = (
    "database_refresh_due_at",
    "refresh_due_at",
    "next_refresh_at",
    "next_refresh",
)
_LIFECYCLE_STATE_FIELDS = (
    "database_lifecycle_status",
    "freshness_state",
    "lifecycle_status",
    "temporal_status",
    "status",
)
_INVALID_LIFECYCLE_STATES = {
    "DUE",
    "OVERDUE",
    "EXPIRED",
    "STALE",
    "VERY_STALE",
    "HISTORICAL",
    "INVALID",
    "REJECTED",
    "REJECTED_FUTURE",
    "SUPERSEDED",
    "NO_DATA_BACKOFF",
    "NO_DATA",
    "NOT_FOUND",
    "NOT_CONFIGURED",
    "DISABLED",
    "RESTRICTED",
    "PROVIDER_FAILED",
    "NOT_CALLED",
}
_ENGLISH_MONTHS = {
    "JAN": 1,
    "JANUARY": 1,
    "FEB": 2,
    "FEBRUARY": 2,
    "MAR": 3,
    "MARCH": 3,
    "APR": 4,
    "APRIL": 4,
    "MAY": 5,
    "JUN": 6,
    "JUNE": 6,
    "JUL": 7,
    "JULY": 7,
    "AUG": 8,
    "AUGUST": 8,
    "SEP": 9,
    "SEPT": 9,
    "SEPTEMBER": 9,
    "OCT": 10,
    "OCTOBER": 10,
    "NOV": 11,
    "NOVEMBER": 11,
    "DEC": 12,
    "DECEMBER": 12,
}
_ENGLISH_DATA_REFERENCE = re.compile(
    r"^(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s*"
    r"(?P<year>\d{4})"
    r"(?:\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2}))?\s*(?P<ampm>AM|PM))?$",
    re.IGNORECASE,
)


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class DataFreshnessService:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))

    def evaluate(self, row: dict[str, Any], *, allow_stale: bool | None = None) -> FreshnessResult:
        allow_stale = self.settings.allow_stale_facts if allow_stale is None else allow_stale
        warnings: list[str] = []
        valid_until = parse_datetime(row.get("valid_until"))
        if valid_until is None:
            retrieved_at = parse_datetime(row.get("retrieved_at"))
            if retrieved_at is None:
                return FreshnessResult(False, "miss", ["missing_valid_until_and_retrieved_at"], stale=True)
            valid_until = retrieved_at + timedelta(hours=self.settings.default_fact_ttl_hours)
            warnings.append("valid_until_missing_default_ttl_used")
        if self.clock() < valid_until:
            return FreshnessResult(True, "hit", warnings)
        warnings.append("stale_fact")
        if allow_stale:
            return FreshnessResult(True, "expired", warnings, stale=True)
        return FreshnessResult(False, "expired", warnings, stale=True)

    def evaluate_canonical(
        self,
        row: dict[str, Any] | None,
        *,
        max_age: timedelta,
        data_reference_mode: CanonicalDataReferenceMode = (
            "official_release"
        ),
        data_as_of_fields: tuple[str, ...] | None = None,
        observed_at: datetime | str | None = None,
    ) -> CanonicalFreshnessResult:
        """Evaluate one canonical lookup with an explicit dataset policy."""

        return evaluate_canonical_freshness(
            row,
            policy=CanonicalFreshnessPolicy(
                max_age=max_age,
                data_reference_mode=data_reference_mode,
                data_as_of_fields=data_as_of_fields,
            ),
            observed_at=observed_at or self.clock(),
        )

    def macro_valid_until(self, event: Any) -> str | None:
        time_utc = getattr(event, "time_utc", None) if not isinstance(event, dict) else event.get("time_utc")
        if time_utc:
            parsed = parse_datetime(time_utc)
            return parsed.isoformat() if parsed else str(time_utc)
        date_value = getattr(event, "date", None) if not isinstance(event, dict) else event.get("date")
        if date_value:
            return f"{date_value}T23:59:59+00:00"
        return None

    def news_valid_until(
        self,
        *,
        published_at: str | None,
        retrieved_at: str,
        topics: list[str] | None = None,
    ) -> str:
        fast_topics = {"fed", "fomc", "cpi", "ppi", "nfp", "pce", "gdp", "risk_event", "inflation", "macro", "yields"}
        medium_topics = {"mega-cap", "semiconductors", "earnings"}
        normalized_topics = {topic.lower() for topic in topics or []}
        ttl_hours = 12 if fast_topics.intersection(normalized_topics) else 18 if medium_topics.intersection(normalized_topics) else self.settings.default_news_ttl_hours
        base = parse_datetime(published_at) or parse_datetime(retrieved_at) or self.clock()
        return (base + timedelta(hours=ttl_hours)).replace(microsecond=0).isoformat()

    def next_refresh_at(self, valid_until: str | None) -> str | None:
        parsed = parse_datetime(valid_until)
        return parsed.replace(microsecond=0).isoformat() if parsed else None


def evaluate_canonical_freshness(
    row: dict[str, Any] | None,
    *,
    policy: CanonicalFreshnessPolicy,
    observed_at: datetime | str,
) -> CanonicalFreshnessResult:
    """Pure canonical-record decision at the actual lookup observation time."""

    decision_at = parse_datetime(observed_at)
    if decision_at is None:
        raise ValueError("canonical_freshness_observed_at_invalid")
    if policy.max_age < timedelta(0):
        raise ValueError("canonical_freshness_max_age_invalid")
    if policy.data_reference_mode not in _REFERENCE_FIELDS:
        raise ValueError("canonical_freshness_reference_mode_invalid")
    if not row:
        return CanonicalFreshnessResult(
            found=False,
            usable=False,
            expired=False,
            complete=True,
            evaluation="NOT_FOUND",
            reason_code="CANONICAL_RECORD_NOT_FOUND",
            evaluated_at=decision_at.isoformat(),
        )

    layers = _evidence_layers(row)
    reference_fields = (
        policy.data_as_of_fields
        or _REFERENCE_FIELDS[policy.data_reference_mode]
    )
    data_as_of = _first_evidence_value(layers, reference_fields)
    data_reference = _parse_data_reference(data_as_of)
    content_deadline = _earliest_explicit_datetime(
        layers,
        _CONTENT_DEADLINE_FIELDS,
    )
    refresh_deadline = _earliest_explicit_datetime(
        layers,
        _REFRESH_DEADLINE_FIELDS,
    )
    lifecycle_states = _lifecycle_states(layers)
    lifecycle = _selected_lifecycle_state(lifecycle_states)
    content_iso = _deadline_iso(content_deadline)
    refresh_iso = _deadline_iso(refresh_deadline)

    if data_as_of in (None, ""):
        return _canonical_result(
            complete=False,
            evaluation="MISSING_DATA_AS_OF",
            reason_code="CANONICAL_RECORD_TIME_NOT_PROVED",
            data_as_of=data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=refresh_iso,
            lifecycle=lifecycle,
            decision_at=decision_at,
        )
    if data_reference is None:
        return _canonical_result(
            complete=False,
            evaluation="INVALID_DATA_AS_OF",
            reason_code="CANONICAL_RECORD_TIME_INVALID",
            data_as_of=data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=refresh_iso,
            lifecycle=lifecycle,
            decision_at=decision_at,
        )
    canonical_data_as_of = data_reference.isoformat()
    if content_deadline.missing:
        return _canonical_result(
            complete=False,
            evaluation="MISSING_CONTENT_VALID_UNTIL",
            reason_code="CANONICAL_CONTENT_VALID_UNTIL_NOT_PROVED",
            data_as_of=canonical_data_as_of,
            content_valid_until=None,
            refresh_due_at=refresh_iso,
            lifecycle=lifecycle,
            decision_at=decision_at,
        )
    if content_deadline.invalid:
        return _canonical_result(
            complete=False,
            evaluation="INVALID_CONTENT_VALID_UNTIL",
            reason_code="CANONICAL_CONTENT_VALID_UNTIL_INVALID",
            data_as_of=canonical_data_as_of,
            content_valid_until=None,
            refresh_due_at=refresh_iso,
            lifecycle=lifecycle,
            decision_at=decision_at,
        )
    if refresh_deadline.missing:
        return _canonical_result(
            complete=False,
            evaluation="MISSING_REFRESH_DUE_AT",
            reason_code="CANONICAL_REFRESH_DUE_AT_NOT_PROVED",
            data_as_of=canonical_data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=None,
            lifecycle=lifecycle,
            decision_at=decision_at,
        )
    if refresh_deadline.invalid:
        return _canonical_result(
            complete=False,
            evaluation="INVALID_REFRESH_DUE_AT",
            reason_code="CANONICAL_REFRESH_DUE_AT_INVALID",
            data_as_of=canonical_data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=None,
            lifecycle=lifecycle,
            decision_at=decision_at,
        )
    invalid_lifecycle = next(
        (
            state
            for state in lifecycle_states
            if state in _INVALID_LIFECYCLE_STATES
        ),
        None,
    )
    if (
        _any_layer_false(layers, "currently_valid")
        or _any_truthy_layer_value(layers, "superseded_by")
        or invalid_lifecycle
    ):
        reason = (
            f"CANONICAL_LIFECYCLE_{invalid_lifecycle}"
            if invalid_lifecycle
            else "CANONICAL_LIFECYCLE_SUPERSEDED"
            if _any_truthy_layer_value(layers, "superseded_by")
            else "CANONICAL_LIFECYCLE_NOT_CURRENTLY_VALID"
        )
        return _canonical_result(
            complete=True,
            evaluation="INVALID_LIFECYCLE",
            reason_code=reason,
            data_as_of=canonical_data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=refresh_iso,
            lifecycle=invalid_lifecycle or lifecycle,
            decision_at=decision_at,
            expired=True,
        )
    allow_future = policy.data_reference_mode == "event_occurrence"
    if (
        not allow_future
        and data_reference > decision_at + timedelta(minutes=5)
    ):
        return _canonical_result(
            complete=True,
            evaluation="FUTURE_DATA_AS_OF",
            reason_code="CANONICAL_RECORD_FROM_FUTURE",
            data_as_of=canonical_data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=refresh_iso,
            lifecycle=lifecycle,
            decision_at=decision_at,
        )
    if decision_at >= content_deadline.value:
        return _canonical_result(
            complete=True,
            evaluation="EXPIRED_CONTENT_VALID_UNTIL",
            reason_code="CANONICAL_CONTENT_VALID_UNTIL_EXPIRED",
            data_as_of=canonical_data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=refresh_iso,
            lifecycle=lifecycle,
            decision_at=decision_at,
            expired=True,
        )
    if decision_at >= refresh_deadline.value:
        return _canonical_result(
            complete=True,
            evaluation="REFRESH_DUE",
            reason_code="CANONICAL_REFRESH_DUE_REACHED",
            data_as_of=canonical_data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=refresh_iso,
            lifecycle=lifecycle,
            decision_at=decision_at,
            expired=True,
        )
    if decision_at - data_reference > policy.max_age:
        return _canonical_result(
            complete=True,
            evaluation="SLA_EXPIRED",
            reason_code="CANONICAL_RECORD_OUTSIDE_DATASET_SLA",
            data_as_of=canonical_data_as_of,
            content_valid_until=content_iso,
            refresh_due_at=refresh_iso,
            lifecycle=lifecycle,
            decision_at=decision_at,
            expired=True,
        )
    return _canonical_result(
        complete=True,
        evaluation="VALID",
        reason_code="CANONICAL_RECORD_WITHIN_SLA",
        data_as_of=canonical_data_as_of,
        content_valid_until=content_iso,
        refresh_due_at=refresh_iso,
        lifecycle=lifecycle,
        decision_at=decision_at,
        usable=True,
    )


@dataclass(frozen=True)
class _ExplicitDeadline:
    value: datetime | None
    missing: bool
    invalid: bool


def _evidence_layers(row: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw = (
        row.get("raw_payload")
        if isinstance(row.get("raw_payload"), dict)
        else row.get("raw_payload_json")
        if isinstance(row.get("raw_payload_json"), dict)
        else {}
    )
    lifecycle_layers = [
        value
        for value in (row.get("lifecycle"), raw.get("lifecycle"))
        if isinstance(value, dict)
    ]
    return (row, raw, *lifecycle_layers)


def _first_evidence_value(
    layers: tuple[dict[str, Any], ...],
    keys: tuple[str, ...],
) -> Any:
    for key in keys:
        for layer in layers:
            value = layer.get(key)
            if value not in (None, ""):
                return value
    return None


def _earliest_explicit_datetime(
    layers: tuple[dict[str, Any], ...],
    keys: tuple[str, ...],
) -> _ExplicitDeadline:
    raw_values = [
        value
        for key in keys
        for layer in layers
        if (value := layer.get(key)) not in (None, "")
    ]
    if not raw_values:
        return _ExplicitDeadline(None, missing=True, invalid=False)
    parsed = [parse_datetime(value) for value in raw_values]
    if any(value is None for value in parsed):
        return _ExplicitDeadline(None, missing=False, invalid=True)
    return _ExplicitDeadline(
        min(value for value in parsed if value is not None),
        missing=False,
        invalid=False,
    )


def _lifecycle_states(
    layers: tuple[dict[str, Any], ...],
) -> list[str]:
    return [
        str(value).strip().upper()
        for key in _LIFECYCLE_STATE_FIELDS
        for layer in layers
        if (value := layer.get(key)) not in (None, "")
    ]


def _selected_lifecycle_state(states: list[str]) -> str | None:
    return next(
        (
            state
            for state in states
            if state in _INVALID_LIFECYCLE_STATES
        ),
        states[0] if states else None,
    )


def _any_layer_false(
    layers: tuple[dict[str, Any], ...],
    key: str,
) -> bool:
    return any(layer.get(key) is False for layer in layers if key in layer)


def _any_truthy_layer_value(
    layers: tuple[dict[str, Any], ...],
    key: str,
) -> bool:
    return any(bool(layer.get(key)) for layer in layers)


def _deadline_iso(deadline: _ExplicitDeadline) -> str | None:
    return deadline.value.isoformat() if deadline.value else None


def _canonical_result(
    *,
    complete: bool,
    evaluation: str,
    reason_code: str,
    data_as_of: Any,
    content_valid_until: str | None,
    refresh_due_at: str | None,
    lifecycle: str | None,
    decision_at: datetime,
    usable: bool = False,
    expired: bool = False,
) -> CanonicalFreshnessResult:
    return CanonicalFreshnessResult(
        found=True,
        usable=usable,
        expired=expired,
        complete=complete,
        evaluation=evaluation,
        reason_code=reason_code,
        data_as_of=data_as_of,
        content_valid_until=content_valid_until,
        refresh_due_at=refresh_due_at,
        lifecycle=lifecycle,
        evaluated_at=decision_at.isoformat(),
    )


def _parse_data_reference(value: Any) -> datetime | None:
    parsed = parse_datetime(value)
    if parsed is not None:
        return parsed
    raw_text = str(value or "").strip()
    text = raw_text.upper()
    try:
        bea_month = re.fullmatch(
            r"(?P<year>\d{4})M(?P<month>0?[1-9]|1[0-2])",
            text,
        )
        if bea_month:
            year = int(bea_month.group("year"))
            month = int(bea_month.group("month"))
            next_month = (
                datetime(year + 1, 1, 1, tzinfo=UTC)
                if month == 12
                else datetime(year, month + 1, 1, tzinfo=UTC)
            )
            return next_month - timedelta(microseconds=1)
        english_reference = _ENGLISH_DATA_REFERENCE.fullmatch(
            raw_text
        )
        if english_reference:
            month = _ENGLISH_MONTHS.get(
                english_reference.group("month").upper()
            )
            if month is None:
                return None
            hour_text = english_reference.group("hour")
            hour = int(hour_text) if hour_text is not None else 0
            minute = int(english_reference.group("minute") or 0)
            second = int(english_reference.group("second") or 0)
            ampm = english_reference.group("ampm")
            if ampm:
                if not 1 <= hour <= 12:
                    return None
                hour = hour % 12 + (
                    12 if ampm.upper() == "PM" else 0
                )
            return datetime(
                int(english_reference.group("year")),
                month,
                int(english_reference.group("day")),
                hour,
                minute,
                second,
                tzinfo=UTC,
            )
        if len(text) == 7 and text[4] == "-":
            year = int(text[:4])
            month = int(text[5:])
            next_month = (
                datetime(year + 1, 1, 1, tzinfo=UTC)
                if month == 12
                else datetime(year, month + 1, 1, tzinfo=UTC)
            )
            return next_month - timedelta(microseconds=1)
        if (
            len(text) in {6, 7}
            and text[4:5] in {"Q", "-"}
            and text[-2:-1] == "Q"
        ):
            year = int(text[:4])
            quarter = int(text[-1:])
            month = quarter * 3
            next_month = (
                datetime(year + 1, 1, 1, tzinfo=UTC)
                if month == 12
                else datetime(year, month + 1, 1, tzinfo=UTC)
            )
            return next_month - timedelta(microseconds=1)
        if (
            len(text) == 8
            and text[4:6] == "-W"
        ):
            year = int(text[:4])
            week = int(text[6:])
            week_end = datetime.fromisocalendar(year, week, 7)
            return week_end.replace(tzinfo=UTC) + timedelta(
                days=1,
            ) - timedelta(microseconds=1)
    except (TypeError, ValueError):
        return None
    return None
