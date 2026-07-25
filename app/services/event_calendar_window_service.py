from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.event_occurrence_lifecycle_service import (
    classify_occurrence_lifecycle,
)
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_domain_service import canonical_event_key


logger = logging.getLogger(__name__)

DEFAULT_TIMEZONE = "America/New_York"
WEEK_BUCKETS = ("PREVIOUS_WEEK", "CURRENT_WEEK", "NEXT_WEEK")
RELEASE_STATUSES = frozenset(
    {
        "SCHEDULED",
        "AWAITING_RELEASE",
        "AWAITING_ACTUAL",
        "PUBLISHED",
        "REVISED",
        "POSTPONED",
        "CANCELLED",
        "UNAVAILABLE",
    }
)
_IMPACT_ORDER = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0}
_CONSUMER_EVENT_BYTES_LIMIT = 17_000
_NULL_MARKERS = frozenset({"", "-", "--", "N/A", "NA", "NONE", "NULL"})
_SCHEDULED_CALENDAR_SECTIONS = (
    "critical_macro_events",
    "fed_communications",
    "other_economic_events",
    "scheduled_regulatory_events",
    "scheduled_geopolitical_events",
)


def build_event_calendar_window(
    full: dict[str, Any],
    *,
    settings: Settings,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build the canonical previous/current/next Monday-Sunday projection.

    The projection is pure and provider-free. Date-only occurrences remain
    date-only; invalid or ambiguous local timestamps fail closed.
    """

    timezone_name = str(settings.event_calendar_timezone or DEFAULT_TIMEZONE)
    timezone = _timezone(timezone_name)
    now_utc = _aware(now or datetime.now(UTC))
    now_local = now_utc.astimezone(timezone)
    current_start_date = now_local.date() - timedelta(days=now_local.weekday())
    start_dates = {
        "PREVIOUS_WEEK": current_start_date - timedelta(days=7),
        "CURRENT_WEEK": current_start_date,
        "NEXT_WEEK": current_start_date + timedelta(days=7),
    }
    bucket_bounds = {
        key: _week_bounds(day, timezone)
        for key, day in start_dates.items()
    }
    window_start = bucket_bounds["PREVIOUS_WEEK"][0]
    window_end = bucket_bounds["NEXT_WEEK"][1]

    temporal_anomalies: list[dict[str, Any]] = []
    duplicate_occurrences: list[str] = []
    selected: dict[str, dict[str, Any]] = {}
    outside_window_count = 0
    for raw, event_type_hint in _scheduled_rows(full):
        occurrence, anomaly = _canonical_occurrence(
            raw,
            event_type_hint=event_type_hint,
            now_utc=now_utc,
            now_local=now_local,
            timezone=timezone,
            bucket_bounds=bucket_bounds,
        )
        if anomaly is not None:
            temporal_anomalies.append(anomaly)
            continue
        if occurrence is None:
            outside_window_count += 1
            continue
        occurrence_id = str(occurrence["occurrence_id"])
        current = selected.get(occurrence_id)
        if current is None:
            selected[occurrence_id] = occurrence
            continue
        duplicate_occurrences.append(occurrence_id)
        selected[occurrence_id] = _merge_occurrences(current, occurrence)

    impact_floor = str(settings.event_calendar_consumer_min_impact or "LOW").upper()
    minimum_impact = _IMPACT_ORDER.get(impact_floor, 1)
    candidates = [
        item
        for item in selected.values()
        if _IMPACT_ORDER.get(str(item.get("impact") or "UNKNOWN"), 0)
        >= minimum_impact
    ]
    candidates.sort(key=_event_sort_key)
    configured_limit = max(int(settings.event_calendar_consumer_max_events), 1)
    retained = _retain_bucket_aware(candidates, limit=configured_limit)
    nonempty_candidate_buckets = {
        str(item["week_bucket"]) for item in candidates
    }
    minimum_bucket_coverage_possible = True
    while (
        len(retained) > len(nonempty_candidate_buckets)
        and _compact_events_size(retained) > _CONSUMER_EVENT_BYTES_LIMIT
    ):
        retained = _retain_bucket_aware(
            candidates,
            limit=len(retained) - 1,
        )
    while retained and _compact_events_size(retained) > _CONSUMER_EVENT_BYTES_LIMIT:
        minimum_bucket_coverage_possible = False
        retained = _drop_lowest_retention_priority(retained)
    retained_event_bytes = _compact_events_size(retained)
    retained_ids = {str(item["occurrence_id"]) for item in retained}
    overflow_count = max(len(candidates) - len(retained), 0)

    buckets: dict[str, dict[str, Any]] = {}
    status_counts = {status: 0 for status in sorted(RELEASE_STATUSES)}
    for bucket_name in WEEK_BUCKETS:
        start, end = bucket_bounds[bucket_name]
        bucket_candidates = [
            item for item in candidates if item["week_bucket"] == bucket_name
        ]
        events = [
            item
            for item in retained
            if item["week_bucket"] == bucket_name
            and str(item["occurrence_id"]) in retained_ids
        ]
        events.sort(key=_event_sort_key)
        for item in events:
            status_counts[str(item["release_status"])] += 1
        buckets[bucket_name] = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "event_count": len(events),
            "candidate_count": len(bucket_candidates),
            "retained_count": len(events),
            "omitted_count": max(len(bucket_candidates) - len(events), 0),
            "events": events,
        }

    bucket_counts = {
        bucket_name: buckets[bucket_name]["event_count"]
        for bucket_name in WEEK_BUCKETS
    }
    missing_actual_count = sum(
        item["release_status"] in {"AWAITING_ACTUAL", "UNAVAILABLE"}
        and item["is_past"]
        for item in retained
    )
    revised_count = sum(
        item["release_status"] == "REVISED" for item in retained
    )
    coverage_status = (
        "COMPLETE"
        if overflow_count == 0
        else "TRUNCATED"
        if minimum_bucket_coverage_possible
        else "DEGRADED"
    )
    bucket_coverage = {
        bucket_name: {
            "candidate_count": buckets[bucket_name]["candidate_count"],
            "retained_count": buckets[bucket_name]["retained_count"],
            "omitted_count": buckets[bucket_name]["omitted_count"],
        }
        for bucket_name in WEEK_BUCKETS
    }
    result = {
        "timezone": timezone_name,
        "generated_at": now_utc.replace(microsecond=0).isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "previous_week": buckets["PREVIOUS_WEEK"],
        "current_week": buckets["CURRENT_WEEK"],
        "next_week": buckets["NEXT_WEEK"],
        "counts": {
            "by_bucket": bucket_counts,
            "by_status": status_counts,
            "total": len(retained),
        },
        "coverage": {
            "status": coverage_status,
            "candidate_count": len(candidates),
            "retained_count": len(retained),
            "omitted_count": overflow_count,
            "overflow_count": overflow_count,
            "by_bucket": bucket_coverage,
            "minimum_per_nonempty_bucket_preserved": all(
                not details["candidate_count"] or details["retained_count"] >= 1
                for details in bucket_coverage.values()
            ),
            "truncation_reason": (
                None
                if overflow_count == 0
                else "configured_event_limit_or_byte_budget"
                if minimum_bucket_coverage_possible
                else "byte_budget_insufficient_for_nonempty_bucket_minimum"
            ),
            "outside_window_count": outside_window_count,
            "minimum_impact": impact_floor,
            "deterministic_limit": configured_limit,
            "consumer_event_bytes": retained_event_bytes,
            "consumer_event_bytes_limit": _CONSUMER_EVENT_BYTES_LIMIT,
        },
        "telemetry": {
            "events_by_bucket": bucket_counts,
            "events_by_status": status_counts,
            "missing_actuals": missing_actual_count,
            "revisions": revised_count,
            "temporal_anomaly_count": len(temporal_anomalies),
            "duplicate_occurrence_count": len(set(duplicate_occurrences)),
            "overflow_count": overflow_count,
        },
        "audit": {
            "temporal_anomalies": temporal_anomalies,
            "duplicate_occurrence_ids": sorted(set(duplicate_occurrences)),
            "ordering": "scheduled_at,impact_desc,occurrence_id",
            "date_only_policy": "preserve_date_without_inventing_release_time",
            "unscheduled_news_policy": "excluded_from_scheduled_calendar_window",
            "removal_confirmations": _removal_confirmations(
                full,
                settings=settings,
            ),
        },
    }
    existing_comparison = (
        (
            (
                full.get("event_calendar_window")
                if isinstance(full.get("event_calendar_window"), dict)
                else {}
            ).get("audit")
            or {}
        ).get("comparison")
        or {}
    )
    if existing_comparison:
        result["audit"]["comparison"] = existing_comparison
    result["removals"] = [
        {
            "occurrence_id": item.get("occurrence_id"),
            "status": item.get("status"),
            "trigger_class": item.get("trigger_class"),
            "causes": list(item.get("causes") or []),
        }
        for item in existing_comparison.get("removals") or []
        if isinstance(item, dict)
    ]
    logger.info(
        "event_calendar_window_materialized",
        extra={
            "event_count": len(retained),
            "previous_week_count": bucket_counts["PREVIOUS_WEEK"],
            "current_week_count": bucket_counts["CURRENT_WEEK"],
            "next_week_count": bucket_counts["NEXT_WEEK"],
            "missing_actual_count": missing_actual_count,
            "revision_count": revised_count,
            "temporal_anomaly_count": len(temporal_anomalies),
            "duplicate_occurrence_count": len(set(duplicate_occurrences)),
            "overflow_count": overflow_count,
        },
    )
    return result


def compact_event_calendar_window(window: dict[str, Any]) -> dict[str, Any]:
    """Remove debug-only lineage while preserving semantic nulls and coverage."""

    output = {
        key: window.get(key)
        for key in (
            "timezone",
            "generated_at",
            "window_start",
            "window_end",
            "counts",
            "coverage",
            "telemetry",
            "removals",
        )
    }
    for key in ("previous_week", "current_week", "next_week"):
        raw_bucket = window.get(key) if isinstance(window.get(key), dict) else {}
        output[key] = {
            "start": raw_bucket.get("start"),
            "end": raw_bucket.get("end"),
            "event_count": int(raw_bucket.get("event_count") or 0),
            "candidate_count": int(raw_bucket.get("candidate_count") or 0),
            "retained_count": int(raw_bucket.get("retained_count") or 0),
            "omitted_count": int(raw_bucket.get("omitted_count") or 0),
            "events": [
                _compact_occurrence(item)
                for item in raw_bucket.get("events") or []
                if isinstance(item, dict)
            ],
        }
    return output


def classify_event_change(
    previous: dict[str, Any] | None,
    current: dict[str, Any] | None,
    *,
    consensus_trigger_enabled: bool = False,
) -> dict[str, Any]:
    """Classify semantic event changes independently from volatile refreshes."""

    previous = dict(previous or {})
    if current is None:
        previous = dict(previous or {})
        return {
            "trigger_class": "NON_TRIGGERING",
            "changed_event_id": (
                previous.get("occurrence_id") or previous.get("event_id")
            ),
            "causes": [],
            "removal_status": "UNCONFIRMED_REMOVAL",
            "comparison_lineage": {
                "previous_source": previous.get("source"),
                "previous_source_domain": previous.get("source_domain"),
                "confirmation_source": None,
            },
        }
    current = dict(current)
    causes: list[str] = []
    old_actual = _nullable(previous.get("actual"))
    new_actual = _nullable(current.get("actual"))
    if old_actual is None and new_actual is not None:
        causes.append("ACTUAL_FIRST_PUBLICATION")
    elif old_actual is not None and new_actual is not None and old_actual != new_actual:
        causes.append("ACTUAL_MATERIAL_REVISION")

    old_status = str(previous.get("release_status") or "").upper()
    new_status = str(current.get("release_status") or "").upper()
    if new_status == "CANCELLED" and old_status != "CANCELLED":
        causes.append("EVENT_CANCELLED")
    if new_status == "POSTPONED" and old_status != "POSTPONED":
        causes.append("EVENT_POSTPONED")
    if previous and _scheduled_identity(previous) != _scheduled_identity(current):
        causes.append("MATERIAL_TIME_CHANGE")
    if (
        not previous
        and str(current.get("impact") or "").upper() == "HIGH"
        and bool(current.get("is_future"))
    ):
        causes.append("NEW_HIGH_IMPACT_FUTURE_EVENT")
    if (
        consensus_trigger_enabled
        and previous
        and _nullable(previous.get("forecast"))
        != _nullable(current.get("forecast"))
    ):
        causes.append("MATERIAL_CONSENSUS_CHANGE")
    causes = sorted(set(causes))
    return {
        "trigger_class": "TRIGGERING" if causes else "NON_TRIGGERING",
        "changed_event_id": current.get("occurrence_id") or current.get("event_id"),
        "causes": causes,
        "removal_status": current.get("removal_status"),
        "comparison_lineage": current.get("comparison_lineage"),
    }


def coalesce_event_changes(changes: Iterable[dict[str, Any]]) -> dict[str, Any]:
    triggering = [
        change
        for change in changes
        if str(change.get("trigger_class") or "") == "TRIGGERING"
    ]
    changed_event_ids = sorted(
        {
            str(change.get("changed_event_id"))
            for change in triggering
            if change.get("changed_event_id")
        }
    )
    causes = sorted(
        {
            str(cause)
            for change in triggering
            for cause in change.get("causes") or []
            if cause
        }
    )
    fingerprint = hashlib.sha256(
        json.dumps(
            {"changed_event_ids": changed_event_ids, "causes": causes},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "trigger_class": "TRIGGERING" if triggering else "NON_TRIGGERING",
        "changed_event_ids": changed_event_ids,
        "causes": causes,
        "trigger_count": len(triggering),
        "coalesced": len(triggering) > 1,
        "idempotency_fingerprint": fingerprint,
    }


def _scheduled_rows(
    full: dict[str, Any],
) -> Iterable[tuple[dict[str, Any], str | None]]:
    calendar = (
        full.get("event_calendar")
        if isinstance(full.get("event_calendar"), dict)
        else {}
    )
    section_hints = {
        "fed_communications": "FOMC_COMMUNICATION",
        "scheduled_regulatory_events": "REGULATORY",
        "scheduled_geopolitical_events": "GEOPOLITICAL",
    }
    for section in _SCHEDULED_CALENDAR_SECTIONS:
        for raw in calendar.get(section) or []:
            if isinstance(raw, dict) and _is_scheduled_occurrence(raw):
                yield raw, section_hints.get(section)

    earnings = (
        (full.get("nasdaq_context") or {}).get("earnings")
        if isinstance(full.get("nasdaq_context"), dict)
        else {}
    ) or {}
    seen: set[str] = set()
    for key in (
        "upcoming",
        "events",
        "relevant_upcoming",
        "upcoming_mega_cap_earnings_14d",
        "released_earnings",
    ):
        for raw in earnings.get(key) or []:
            if not isinstance(raw, dict):
                continue
            fingerprint = json.dumps(raw, sort_keys=True, default=str)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            yield raw, "EARNINGS"


def _is_scheduled_occurrence(item: dict[str, Any]) -> bool:
    schedule_status = str(
        item.get("schedule_status")
        or item.get("event_kind")
        or item.get("event_type")
        or ""
    ).lower()
    if "unscheduled" in schedule_status or bool(item.get("is_unscheduled")):
        return False
    return bool(
        item.get("date")
        or item.get("scheduled_at")
        or item.get("release_at")
        or item.get("time_utc")
    )


def _canonical_occurrence(
    raw: dict[str, Any],
    *,
    event_type_hint: str | None,
    now_utc: datetime,
    now_local: datetime,
    timezone: ZoneInfo,
    bucket_bounds: dict[str, tuple[datetime, datetime]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    item = dict(raw)
    scheduled, scheduled_date, precision, invalid_reason = _scheduled_time(
        item,
        semantic_timezone=timezone,
    )
    occurrence_identity = str(
        item.get("occurrence_id")
        or item.get("canonical_event_key")
        or item.get("issuer_event_id")
        or ""
    )
    if (
        not occurrence_identity
        and item.get("event_id")
        and str(item.get("event_id"))
        == str(item.get("provider_event_id") or "")
    ):
        occurrence_identity = str(item["event_id"])
    if invalid_reason:
        return None, {
            "occurrence_id": occurrence_identity or None,
            "reason": invalid_reason,
            "raw_scheduled_at": _raw_scheduled_value(item),
        }
    if scheduled_date is None:
        return None, {
            "occurrence_id": occurrence_identity or None,
            "reason": "scheduled_date_missing",
            "raw_scheduled_at": None,
        }
    week_bucket = _bucket_for_date(scheduled_date, bucket_bounds)
    if week_bucket is None:
        return None, None

    event_type = str(
        event_type_hint
        or item.get("event_type")
        or item.get("event_kind")
        or item.get("category")
        or "SCHEDULED_EVENT"
    ).upper()
    if not occurrence_identity:
        occurrence_identity = canonical_event_key(
            {
                **item,
                "date": scheduled_date.isoformat(),
                "release_at": scheduled.isoformat() if scheduled else None,
                "category": event_type,
            }
        )
    event_id = str(
        item.get("event_id")
        or item.get("canonical_event_key")
        or item.get("issuer_event_id")
        or occurrence_identity
    )
    actual = _first_value(
        item.get("actual"),
        item.get("eps_actual"),
        item.get("revenue_actual"),
        (item.get("enrichment") or {}).get("actual")
        if isinstance(item.get("enrichment"), dict)
        else None,
    )
    forecast = _first_value(
        item.get("forecast"),
        item.get("consensus"),
        item.get("eps_estimate"),
        item.get("revenue_estimate"),
        (item.get("enrichment") or {}).get("forecast")
        if isinstance(item.get("enrichment"), dict)
        else None,
        (item.get("enrichment") or {}).get("consensus")
        if isinstance(item.get("enrichment"), dict)
        else None,
    )
    previous = _first_value(
        item.get("previous"),
        (item.get("enrichment") or {}).get("previous")
        if isinstance(item.get("enrichment"), dict)
        else None,
    )
    is_today = scheduled_date == now_local.date()
    is_past, is_future, temporal_state = _temporal_flags(
        scheduled=scheduled,
        scheduled_date=scheduled_date,
        precision=precision,
        now_utc=now_utc,
        now_local=now_local,
    )
    release_status = _release_status(
        item,
        actual=actual,
        is_past=is_past,
        is_future=is_future,
        is_today=is_today,
        precision=precision,
    )
    source_url = _first_value(
        item.get("source_url"),
        item.get("canonical_url"),
        (item.get("enrichment") or {}).get("source_url")
        if isinstance(item.get("enrichment"), dict)
        else None,
    )
    source = _first_value(
        item.get("source"),
        item.get("provider"),
        (item.get("enrichment") or {}).get("source")
        if isinstance(item.get("enrichment"), dict)
        else None,
    )
    retrieved_at = _iso_or_none(
        _first_value(
            item.get("retrieved_at"),
            item.get("retrieved_at_utc"),
            (item.get("enrichment") or {}).get("retrieved_at")
            if isinstance(item.get("enrichment"), dict)
            else None,
        )
    )
    valid_until = _iso_or_none(
        _first_value(
            item.get("valid_until"),
            (item.get("enrichment") or {}).get("valid_until")
            if isinstance(item.get("enrichment"), dict)
            else None,
        )
    )
    next_refresh_at = _iso_or_none(
        _first_value(
            item.get("next_refresh_at"),
            (item.get("enrichment") or {}).get("next_refresh_at")
            if isinstance(item.get("enrichment"), dict)
            else None,
        )
    )
    revision = _nullable(item.get("revision"))
    if revision is None and isinstance(item.get("enrichment"), dict):
        revision = _nullable(item["enrichment"].get("revision"))
    occurrence = {
        "event_id": event_id,
        "occurrence_id": occurrence_identity,
        "event_type": event_type,
        "title": _first_value(
            item.get("title"),
            item.get("name"),
            item.get("event_name"),
            item.get("issuer_name"),
            item.get("symbol"),
        ),
        "country": _nullable(item.get("country") or item.get("country_code")),
        "currency": _nullable(item.get("currency")),
        "impact": _impact(item),
        "scheduled_at": (
            scheduled.astimezone(timezone).replace(microsecond=0).isoformat()
            if scheduled is not None
            else scheduled_date.isoformat()
        ),
        "scheduled_at_utc": (
            scheduled.astimezone(UTC).replace(microsecond=0).isoformat()
            if scheduled is not None
            else None
        ),
        "scheduled_time_precision": precision,
        "timezone": str(timezone.key),
        "source_timezone": _nullable(
            item.get("source_timezone") or item.get("timezone")
        ),
        "week_bucket": week_bucket,
        "is_today": is_today,
        "is_past": is_past,
        "is_future": is_future,
        "temporal_state": temporal_state,
        "release_status": release_status,
        "actual": actual,
        "forecast": forecast,
        "previous": previous,
        "surprise": _surprise(item.get("surprise"), actual, forecast),
        "revision": revision,
        "source": source,
        "source_domain": _source_domain(source_url),
        "source_url": source_url,
        "retrieved_at": retrieved_at,
        "freshness_state": _nullable(
            item.get("freshness_state")
            or item.get("freshness")
            or (
                (item.get("lifecycle") or {}).get("freshness_state")
                if isinstance(item.get("lifecycle"), dict)
                else None
            )
        ),
        "valid_until": valid_until,
        "next_refresh_at": next_refresh_at,
        "trigger_class": _occurrence_trigger_class(item, release_status),
        "lineage": _full_lineage(item),
    }
    lifecycle_classification = classify_occurrence_lifecycle(
        {
            **item,
            **occurrence,
            "classification_hint": event_type_hint,
        }
    )
    occurrence["lifecycle"] = lifecycle_classification.as_dict()
    occurrence["lifecycle_entity_type"] = lifecycle_classification.entity_type
    occurrence["outcome_contract"] = lifecycle_classification.outcome_contract
    return occurrence, None


def _scheduled_time(
    item: dict[str, Any],
    *,
    semantic_timezone: ZoneInfo,
) -> tuple[datetime | None, date | None, str, str | None]:
    raw = _raw_scheduled_value(item)
    if raw is None:
        return None, None, "UNKNOWN", None
    if isinstance(raw, datetime):
        parsed = raw
    else:
        text = str(raw).strip()
        if len(text) == 10:
            try:
                return None, date.fromisoformat(text), "DATE", None
            except ValueError:
                return None, None, "INVALID", "scheduled_date_invalid"
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None, None, "INVALID", "scheduled_timestamp_invalid"

    if parsed.tzinfo is None:
        source_timezone_name = str(
            item.get("source_timezone")
            or item.get("timezone")
            or semantic_timezone.key
        )
        try:
            source_timezone = ZoneInfo(source_timezone_name)
        except ZoneInfoNotFoundError:
            return None, None, "INVALID", "source_timezone_invalid"
        localized, reason = _strict_localize(parsed, source_timezone)
        if reason:
            return None, None, "INVALID", reason
        parsed = localized
    local = parsed.astimezone(semantic_timezone)
    return parsed.astimezone(UTC), local.date(), "DATETIME", None


def _strict_localize(
    naive: datetime,
    timezone: ZoneInfo,
) -> tuple[datetime, str | None]:
    candidates: list[datetime] = []
    for fold in (0, 1):
        candidate = naive.replace(tzinfo=timezone, fold=fold)
        round_trip = candidate.astimezone(UTC).astimezone(timezone)
        if round_trip.replace(tzinfo=None) == naive:
            candidates.append(candidate)
    unique_offsets = {candidate.utcoffset() for candidate in candidates}
    if not candidates:
        return naive.replace(tzinfo=timezone), "scheduled_timestamp_nonexistent"
    if len(unique_offsets) > 1:
        return candidates[0], "scheduled_timestamp_ambiguous"
    return candidates[0], None


def _raw_scheduled_value(item: dict[str, Any]) -> Any:
    for key in (
        "scheduled_at",
        "event_at",
        "release_at",
        "release_at_utc",
        "time_utc",
        "earnings_date",
        "scheduled_date",
        "date",
    ):
        if item.get(key) not in (None, ""):
            return item[key]
    return None


def _bucket_for_date(
    scheduled_date: date,
    bucket_bounds: dict[str, tuple[datetime, datetime]],
) -> str | None:
    for name, (start, end) in bucket_bounds.items():
        if start.date() <= scheduled_date <= end.date():
            return name
    return None


def _week_bounds(
    monday: date,
    timezone: ZoneInfo,
) -> tuple[datetime, datetime]:
    start = datetime.combine(monday, time.min, timezone)
    end = datetime.combine(
        monday + timedelta(days=6),
        time(23, 59, 59),
        timezone,
    )
    return start, end


def _temporal_flags(
    *,
    scheduled: datetime | None,
    scheduled_date: date,
    precision: str,
    now_utc: datetime,
    now_local: datetime,
) -> tuple[bool, bool, str]:
    if scheduled is not None:
        if scheduled < now_utc:
            return True, False, "PAST"
        if scheduled > now_utc:
            return False, True, "FUTURE"
        return False, False, "NOW"
    if scheduled_date < now_local.date():
        return True, False, "PAST_DATE"
    if scheduled_date > now_local.date():
        return False, True, "FUTURE_DATE"
    return False, False, (
        "TODAY_TIME_UNKNOWN" if precision == "DATE" else "TODAY"
    )


def _release_status(
    item: dict[str, Any],
    *,
    actual: Any,
    is_past: bool,
    is_future: bool,
    is_today: bool,
    precision: str,
) -> str:
    explicit = str(
        item.get("release_status")
        or item.get("actual_status")
        or item.get("status")
        or item.get("temporal_status")
        or ""
    ).upper()
    aliases = {
        "PRE_RELEASE": "SCHEDULED",
        "RELEASED": "PUBLISHED",
        "COMPLETED": "PUBLISHED",
        "ACTUAL_UNAVAILABLE": "UNAVAILABLE",
        "AWAITING_OUTCOME": "AWAITING_ACTUAL",
    }
    explicit = aliases.get(explicit, explicit)
    if explicit in {"CANCELLED", "POSTPONED", "REVISED", "UNAVAILABLE"}:
        return explicit
    if actual is not None:
        return "PUBLISHED"
    if is_future:
        return "SCHEDULED"
    if is_past:
        return "AWAITING_ACTUAL"
    if is_today and precision == "DATE":
        return "AWAITING_RELEASE"
    return "AWAITING_RELEASE"


def _occurrence_trigger_class(
    item: dict[str, Any],
    release_status: str,
) -> str:
    explicit = str(item.get("trigger_class") or "").upper()
    if explicit in {"TRIGGER", "TRIGGERING"}:
        return "TRIGGERING"
    if release_status in {"PUBLISHED", "REVISED", "CANCELLED", "POSTPONED"}:
        return "TRIGGERING"
    return "NON_TRIGGERING"


def _impact(item: dict[str, Any]) -> str:
    raw = str(
        item.get("impact")
        or item.get("importance")
        or item.get("event_risk_level")
        or "UNKNOWN"
    ).upper()
    if raw in _IMPACT_ORDER:
        return raw
    try:
        numeric = int(raw)
    except ValueError:
        return "UNKNOWN"
    return "HIGH" if numeric >= 3 else "MEDIUM" if numeric == 2 else "LOW"


def _surprise(raw: Any, actual: Any, forecast: Any) -> Any:
    raw = _nullable(raw)
    if raw is not None:
        return raw
    if actual is None or forecast is None:
        return None
    try:
        difference = Decimal(str(actual)) - Decimal(str(forecast))
    except (InvalidOperation, ValueError):
        return None
    value: int | float
    if difference == difference.to_integral():
        value = int(difference)
    else:
        value = float(difference)
    return {
        "value": value,
        "direction": (
            "ABOVE"
            if difference > 0
            else "BELOW"
            if difference < 0
            else "IN_LINE"
        ),
        "method": "actual_minus_forecast",
    }


def _full_lineage(item: dict[str, Any]) -> dict[str, Any]:
    enrichment = (
        item.get("enrichment")
        if isinstance(item.get("enrichment"), dict)
        else {}
    )
    return {
        "provider": item.get("provider"),
        "provider_event_id": item.get("provider_event_id"),
        "source_event_id": item.get("source_event_id"),
        "issuer_event_id": item.get("issuer_event_id"),
        "source_url": item.get("source_url") or enrichment.get("source_url"),
        "field_lineage": (
            item.get("field_lineage")
            or enrichment.get("field_lineage")
            or {}
        ),
        "validation": item.get("validation") or enrichment.get("validation"),
        "retrieved_at": item.get("retrieved_at")
        or enrichment.get("retrieved_at"),
    }


def _compact_occurrence(item: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item.get(key)
        for key in (
            "event_id",
            "occurrence_id",
            "event_type",
            "title",
            "country",
            "currency",
            "impact",
            "scheduled_at",
            "scheduled_at_utc",
            "scheduled_time_precision",
            "timezone",
            "week_bucket",
            "is_today",
            "is_past",
            "is_future",
            "temporal_state",
            "release_status",
            "actual",
            "forecast",
            "previous",
            "surprise",
            "revision",
            "source",
            "source_domain",
            "retrieved_at",
            "freshness_state",
            "valid_until",
            "next_refresh_at",
            "trigger_class",
            "lifecycle_entity_type",
            "outcome_contract",
        )
    }


def _event_sort_key(item: dict[str, Any]) -> tuple[str, int, str]:
    scheduled = str(item.get("scheduled_at") or "9999-12-31")
    impact = -_IMPACT_ORDER.get(str(item.get("impact") or "UNKNOWN"), 0)
    return scheduled, impact, str(item.get("occurrence_id") or "")


def _compact_events_size(items: list[dict[str, Any]]) -> int:
    return len(
        json.dumps(
            [_compact_occurrence(item) for item in items],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    )


def _retain_bucket_aware(
    items: list[dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if len(items) <= limit:
        return list(items)
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    for bucket_name in WEEK_BUCKETS:
        bucket = [
            item for item in items if item.get("week_bucket") == bucket_name
        ]
        if not bucket or len(selected) >= limit:
            continue
        winner = min(bucket, key=_bucket_quota_priority)
        selected.append(winner)
        selected_ids.add(str(winner.get("occurrence_id") or ""))
    for item in sorted(items, key=_retention_priority):
        identity = str(item.get("occurrence_id") or "")
        if identity in selected_ids:
            continue
        if len(selected) >= limit:
            break
        selected.append(item)
        selected_ids.add(identity)
    return sorted(selected, key=_event_sort_key)


def _retention_priority(item: dict[str, Any]) -> tuple[int, int, int, str, str]:
    bucket = str(item.get("week_bucket") or "")
    status = str(item.get("release_status") or "")
    actual_priority = (
        0
        if bucket == "PREVIOUS_WEEK"
        and (
            item.get("actual") not in (None, "")
            or status in {"PUBLISHED", "REVISED"}
        )
        else 1
    )
    current_priority = (
        0
        if bucket == "CURRENT_WEEK"
        and (bool(item.get("is_today")) or status == "AWAITING_ACTUAL")
        else 1
    )
    next_priority = (
        0
        if bucket == "NEXT_WEEK"
        and str(item.get("impact") or "").upper() == "HIGH"
        else 1
    )
    semantic_priority = min(actual_priority, current_priority, next_priority)
    impact = -_IMPACT_ORDER.get(str(item.get("impact") or "UNKNOWN"), 0)
    return (
        impact,
        semantic_priority,
        WEEK_BUCKETS.index(bucket) if bucket in WEEK_BUCKETS else 99,
        str(item.get("scheduled_at") or "9999-12-31"),
        str(item.get("occurrence_id") or ""),
    )


def _bucket_quota_priority(
    item: dict[str, Any],
) -> tuple[int, int, str, str]:
    bucket = str(item.get("week_bucket") or "")
    status = str(item.get("release_status") or "")
    if bucket == "PREVIOUS_WEEK":
        semantic = int(
            not (
                item.get("actual") not in (None, "")
                or status in {"PUBLISHED", "REVISED"}
            )
        )
    elif bucket == "CURRENT_WEEK":
        semantic = int(
            not (
                bool(item.get("is_today"))
                or status == "AWAITING_ACTUAL"
            )
        )
    elif bucket == "NEXT_WEEK":
        semantic = int(
            str(item.get("impact") or "").upper() != "HIGH"
        )
    else:
        semantic = 1
    return (
        semantic,
        -_IMPACT_ORDER.get(str(item.get("impact") or "UNKNOWN"), 0),
        str(item.get("scheduled_at") or "9999-12-31"),
        str(item.get("occurrence_id") or ""),
    )


def _drop_lowest_retention_priority(
    retained: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not retained:
        return []
    worst = max(retained, key=_retention_priority)
    removed = False
    output: list[dict[str, Any]] = []
    for item in retained:
        if not removed and item is worst:
            removed = True
            continue
        output.append(item)
    return output


def _removal_confirmations(
    full: dict[str, Any],
    *,
    settings: Settings,
) -> list[dict[str, Any]]:
    calendar = (
        full.get("event_calendar")
        if isinstance(full.get("event_calendar"), dict)
        else {}
    )
    rows = (
        calendar.get("removal_confirmations")
        or calendar.get("removed_events")
        or []
    )
    confirmations: list[dict[str, Any]] = []
    source_policy = SourcePolicyService(settings.source_policy_path)
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        occurrence_id = str(
            raw.get("occurrence_id")
            or raw.get("event_id")
            or raw.get("canonical_event_key")
            or ""
        )
        status = str(
            raw.get("release_status") or raw.get("status") or ""
        ).upper()
        source = raw.get("source")
        source_url = raw.get("source_url")
        admitted = source_policy.validate(
            raw,
            field_semantics="event",
        ).accepted
        if (
            occurrence_id
            and status in {"CANCELLED", "POSTPONED"}
            and (source or source_url)
            and admitted
        ):
            confirmations.append(
                {
                    "occurrence_id": occurrence_id,
                    "release_status": status,
                    "source": source,
                    "source_url": source_url,
                    "retrieved_at": _iso_or_none(raw.get("retrieved_at")),
                }
            )
    return sorted(confirmations, key=lambda item: item["occurrence_id"])


def _merge_occurrences(
    primary: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    output = dict(primary)
    for key, value in candidate.items():
        if output.get(key) is None and value is not None:
            output[key] = value
    if output.get("actual") is None and candidate.get("actual") is not None:
        output["actual"] = candidate["actual"]
        output["release_status"] = candidate["release_status"]
    return output


def _first_value(*values: Any) -> Any:
    for value in values:
        normalized = _nullable(value)
        if normalized is not None:
            return normalized
    return None


def _nullable(value: Any) -> Any:
    if isinstance(value, str) and value.strip().upper() in _NULL_MARKERS:
        return None
    return value


def _source_domain(value: Any) -> str | None:
    if not value:
        return None
    parsed = urlparse(str(value))
    return parsed.hostname.lower() if parsed.hostname else None


def _iso_or_none(value: Any) -> str | None:
    parsed = parse_datetime(value)
    return parsed.replace(microsecond=0).isoformat() if parsed else None


def _scheduled_identity(item: dict[str, Any]) -> tuple[Any, Any]:
    return item.get("scheduled_at"), item.get("scheduled_at_utc")


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"event_calendar_timezone_invalid:{name}") from exc
