from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.data_freshness_service import parse_datetime


NEW_YORK = ZoneInfo("America/New_York")
TRIGGER_CLASSES = frozenset(
    {"TRIGGER", "REFRESH_ON_TRIGGER", "CACHE_UNTIL_DUE", "NON_TRIGGERING"}
)
FRESHNESS_STATES = frozenset(
    {
        "FRESH",
        "DUE",
        "STALE_LAST_KNOWN_GOOD",
        "EXPIRED",
        "AWAITING_ACTUAL",
        "NO_DATA_BACKOFF",
        "QUARANTINED",
    }
)

TRIGGER_CLASS_BY_ENTITY = {
    "macro_actual": "TRIGGER",
    "macro_actual_published": "TRIGGER",
    "macro_actual_revised": "TRIGGER",
    "fomc_decision": "TRIGGER",
    "fomc_communication": "TRIGGER",
    "earnings_actual": "TRIGGER",
    "earnings_guidance": "TRIGGER",
    "market_schedule_change": "TRIGGER",
    "event_cancelled": "TRIGGER",
    "event_postponed": "TRIGGER",
    "event_time_changed": "TRIGGER",
    "high_impact_event_added": "TRIGGER",
    "consensus_material_change": "TRIGGER",
    "cot_publication": "TRIGGER",
    "breaking_news": "TRIGGER",
    "official_correction": "TRIGGER",
    "vix": "REFRESH_ON_TRIGGER",
    "vvix": "REFRESH_ON_TRIGGER",
    "vix_futures": "REFRESH_ON_TRIGGER",
    "put_call": "REFRESH_ON_TRIGGER",
    "skew": "REFRESH_ON_TRIGGER",
    "options_positioning": "REFRESH_ON_TRIGGER",
    "market_internals": "REFRESH_ON_TRIGGER",
    "cross_asset_context": "REFRESH_ON_TRIGGER",
    "prices": "REFRESH_ON_TRIGGER",
    "breadth": "REFRESH_ON_TRIGGER",
    "cot": "CACHE_UNTIL_DUE",
    "earnings_schedule": "CACHE_UNTIL_DUE",
    "earnings_intelligence": "CACHE_UNTIL_DUE",
    "macro_schedule": "CACHE_UNTIL_DUE",
    "market_schedule": "CACHE_UNTIL_DUE",
    "macro_snapshot": "CACHE_UNTIL_DUE",
    "structural_fact": "CACHE_UNTIL_DUE",
}

VOLATILE_FINGERPRINT_KEYS = frozenset(
    {
        "generated_at",
        "generated_at_utc",
        "observed_at",
        "retrieved_at",
        "retrieved_at_utc",
        "created_at",
        "updated_at",
        "checked_at",
        "checked_at_utc",
        "searched_at",
        "duration_ms",
        "trace_id",
        "span_id",
        "parent_span_id",
        "attempt_count",
        "heartbeat_at",
        "lease_owner",
        "lease_expires_at",
    }
)


@dataclass(frozen=True)
class DatumLifecycle:
    entity_type: str
    entity_key: str
    trigger_class: str
    freshness_state: str
    observed_at: str | None
    data_as_of: str | None
    published_at: str | None
    event_at: str | None
    valid_from: str | None
    valid_until: str | None
    next_refresh_at: str | None
    next_retry_at: str | None
    superseded_by: str | None
    refresh_reason: str | None
    materiality_fingerprint: str
    source_lineage: tuple[dict[str, Any], ...]
    acquisition_method: str | None
    retry_class: str | None
    retry_policy: dict[str, Any]
    negative_cache_key: str | None
    negative_cache_expires_at: str | None
    session_state: str | None
    triggering_event: str | None
    fields_attempted: tuple[str, ...]
    attempt_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            **self.__dict__,
            "source_lineage": list(self.source_lineage),
            "fields_attempted": list(self.fields_attempted),
        }


def compute_datum_lifecycle(
    entity_type: str,
    entity_key: str,
    datum: dict[str, Any] | None,
    *,
    settings: Settings,
    now: datetime,
    attempt_count: int = 0,
    no_data: bool = False,
    fields_attempted: list[str] | tuple[str, ...] = (),
    session_state: str | None = None,
    triggering_event: str | None = None,
    retry_class: str | None = None,
    refresh_reason: str | None = None,
) -> DatumLifecycle:
    """The single service-owned lifecycle calculation for every datum."""
    now = _aware(now)
    value = dict(datum or {})
    entity_type = str(entity_type or "unknown").lower()
    trigger_class = TRIGGER_CLASS_BY_ENTITY.get(entity_type, "NON_TRIGGERING")
    observed_at = _first_time(value, "observed_at", "retrieved_at", "created_at")
    data_as_of = _first_time(
        value,
        "data_as_of",
        "report_date",
        "period",
        "published_at",
        "event_at",
        "release_at",
    )
    published_at = _first_time(value, "published_at")
    event_at = _event_time(value, settings=settings)
    valid_from = _first_time(value, "valid_from") or data_as_of or observed_at
    valid_until = _first_time(value, "valid_until", "fresh_until")
    next_refresh_at = _first_time(value, "next_refresh_at", "next_refresh")
    next_retry_at = _first_time(value, "next_retry_at")
    retry_policy = _retry_policy(settings, retry_class or "NO_DATA")
    actual_missing = _scheduled_actual_missing(entity_type, value)

    if entity_type in {"macro_actual", "fomc_decision"}:
        if event_at is not None and actual_missing:
            valid_until = valid_until or event_at
            if event_at <= now:
                next_retry_at = next_retry_at or (
                    now
                    if attempt_count <= 0
                    else _bounded_retry_at(
                        now,
                        attempt_count=attempt_count,
                        delays=_retry_delays(settings),
                    )
                )
                next_refresh_at = next_retry_at
            else:
                next_retry_at = None
                next_refresh_at = event_at
    elif entity_type in {"cot", "cot_positioning", "cot_publication"}:
        report_date = _date_value(value.get("report_date") or value.get("data_as_of"))
        if report_date is not None:
            cftc_due = next_cftc_publication(report_date, settings=settings, now=now)
            valid_until = cftc_due
            next_refresh_at = cftc_due
    elif entity_type in {
        "earnings",
        "earnings_schedule",
        "earnings_actual",
        "earnings_intelligence",
    }:
        if event_at is not None and actual_missing:
            valid_until = valid_until or event_at
            if event_at <= now:
                next_retry_at = next_retry_at or _bounded_retry_at(
                    now,
                    attempt_count=attempt_count,
                    delays=_retry_delays(settings),
                )
                next_refresh_at = next_retry_at
            else:
                next_retry_at = None
                next_refresh_at = max(
                    event_at,
                    next_refresh_at or event_at,
                )
        elif not actual_missing:
            next_retry_at = None
            valid_until = valid_until or (
                published_at or observed_at or now
            ) + _default_ttl(entity_type, settings)
            next_refresh_at = next_refresh_at or valid_until

    if valid_until is None:
        anchor = observed_at or data_as_of or event_at or now
        valid_until = anchor + _default_ttl(entity_type, settings)
    if next_refresh_at is None:
        next_refresh_at = valid_until

    negative_cache_key = None
    negative_cache_expires_at = None
    if no_data:
        retry_class = retry_class or "NO_DATA"
        next_retry_at = next_retry_at or _bounded_retry_at(
            now,
            attempt_count=attempt_count,
            delays=_retry_delays(settings),
        )
        negative_cache_key = negative_cache_fingerprint(
            entity_type,
            entity_key,
            fields_attempted,
            session_state=session_state,
        )
        negative_cache_expires_at = next_retry_at

    quarantined = str(
        value.get("audit_status")
        or value.get("source_audit_status")
        or value.get("verification_status")
        or ""
    ).upper() in {"QUARANTINED", "REJECTED"}
    if quarantined:
        freshness_state = "QUARANTINED"
    elif no_data and next_retry_at and next_retry_at > now:
        freshness_state = "NO_DATA_BACKOFF"
    elif (
        entity_type
        in {
            "macro_actual",
            "fomc_decision",
            "earnings",
            "earnings_schedule",
            "earnings_actual",
            "earnings_intelligence",
        }
        and event_at is not None
        and event_at <= now
        and actual_missing
    ):
        freshness_state = "AWAITING_ACTUAL"
    elif next_refresh_at is not None and next_refresh_at <= now:
        freshness_state = "DUE"
    elif valid_until is not None and valid_until <= now:
        freshness_state = (
            "STALE_LAST_KNOWN_GOOD"
            if _carry_forward_allowed(entity_type)
            else "EXPIRED"
        )
    elif not _has_material_data(value):
        freshness_state = "DUE"
    else:
        freshness_state = "FRESH"

    lineage = value.get("source_lineage") or value.get("lineage") or []
    if isinstance(lineage, dict):
        lineage = [lineage]
    if (
        not lineage
        and _has_material_data(value)
        and (value.get("source") or value.get("source_url"))
    ):
        lineage = [
            {
                "source": value.get("source") or value.get("provider"),
                "source_url": value.get("source_url"),
                "provider_type": value.get("provider_type"),
                "retrieved_at": value.get("retrieved_at"),
                "data_as_of": value.get("data_as_of")
                or value.get("report_date"),
            }
        ]
    lineage = tuple(item for item in lineage if isinstance(item, dict))
    datum_fingerprint = materiality_fingerprint(
        {
            "entity_type": entity_type,
            "entity_key": entity_key,
            "value": value,
            "freshness_state": freshness_state,
        }
    )
    return DatumLifecycle(
        entity_type=entity_type,
        entity_key=entity_key,
        trigger_class=trigger_class,
        freshness_state=freshness_state,
        observed_at=_iso(observed_at),
        data_as_of=_iso(data_as_of),
        published_at=_iso(published_at),
        event_at=_iso(event_at),
        valid_from=_iso(valid_from),
        valid_until=_iso(valid_until),
        next_refresh_at=_iso(next_refresh_at),
        next_retry_at=_iso(next_retry_at),
        superseded_by=value.get("superseded_by"),
        refresh_reason=refresh_reason or value.get("refresh_reason"),
        materiality_fingerprint=datum_fingerprint,
        source_lineage=lineage,
        acquisition_method=(
            value.get("acquisition_method")
            or ("api_provider" if lineage else None)
        ),
        retry_class=retry_class,
        retry_policy=retry_policy,
        negative_cache_key=negative_cache_key,
        negative_cache_expires_at=_iso(negative_cache_expires_at),
        session_state=session_state,
        triggering_event=triggering_event,
        fields_attempted=tuple(sorted({str(item) for item in fields_attempted if item})),
        attempt_count=max(int(attempt_count), 0),
    )


def next_cftc_publication(
    report_date: date,
    *,
    settings: Settings,
    now: datetime,
) -> datetime:
    """Return the next useful CFTC publication after the supplied report date."""
    weekday = int(settings.cftc_release_weekday)
    days_until_first = (weekday - report_date.weekday()) % 7
    first_release_date = report_date + timedelta(days=days_until_first)
    if first_release_date <= report_date:
        first_release_date += timedelta(days=7)
    candidate_date = first_release_date + timedelta(days=7)
    holidays = {
        item.strip()
        for item in str(settings.cftc_release_holidays or "").split(",")
        if item.strip()
    }
    while candidate_date.isoformat() in holidays:
        candidate_date += timedelta(days=1)
    candidate_date += timedelta(days=int(settings.cftc_release_delay_days))
    try:
        hour, minute = (
            int(item)
            for item in str(settings.cftc_release_time_new_york).split(":", 1)
        )
    except (TypeError, ValueError):
        hour, minute = 15, 30
    candidate = datetime.combine(
        candidate_date,
        time(hour=hour, minute=minute),
        NEW_YORK,
    ).astimezone(UTC)
    while candidate <= _aware(now):
        candidate += timedelta(days=7)
        while candidate.astimezone(NEW_YORK).date().isoformat() in holidays:
            candidate += timedelta(days=1)
    return candidate


def negative_cache_fingerprint(
    entity_type: str,
    entity_key: str,
    fields: list[str] | tuple[str, ...],
    *,
    session_state: str | None,
) -> str:
    seed = _canonical(
        {
            "entity_type": entity_type,
            "entity_key": entity_key,
            "fields": sorted({str(item) for item in fields if item}),
            "session_state": session_state,
        }
    )
    return f"negative:{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:40]}"


def materiality_fingerprint(value: Any) -> str:
    normalized = _strip_volatile(value)
    return hashlib.sha256(_canonical(normalized).encode("utf-8")).hexdigest()


def material_changes(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> tuple[list[str], list[dict[str, Any]]]:
    previous = dict(previous or {})
    changed_sections: list[str] = []
    changes: list[dict[str, Any]] = []
    for key in sorted(set(previous) | set(current)):
        if key in {
            "snapshot_id",
            "snapshot_revision",
            "audit",
            "metadata",
            "generated_at",
            "generated_at_utc",
        }:
            continue
        old_fingerprint = materiality_fingerprint(previous.get(key))
        new_fingerprint = materiality_fingerprint(current.get(key))
        if old_fingerprint == new_fingerprint:
            continue
        changed_sections.append(key)
        changes.append(
            {
                "section": key,
                "previous_fingerprint": old_fingerprint,
                "current_fingerprint": new_fingerprint,
            }
        )
    return changed_sections, changes


_LIFECYCLE_PERSISTED_FIELDS = (
    "trigger_class",
    "freshness_state",
    "observed_at",
    "data_as_of",
    "published_at",
    "event_at",
    "valid_from",
    "valid_until",
    "next_refresh_at",
    "next_retry_at",
    "superseded_by",
    "refresh_reason",
    "materiality_fingerprint",
    "source_lineage_json",
    "acquisition_method",
    "retry_class",
    "retry_policy_json",
    "negative_cache_key",
    "negative_cache_expires_at",
    "session_state",
    "triggering_event",
    "fields_attempted_json",
    "attempt_count",
    "work_status",
)


def _lifecycle_persistence_values(
    lifecycle: DatumLifecycle,
    *,
    work_status: str,
) -> dict[str, Any]:
    return {
        "trigger_class": lifecycle.trigger_class,
        "freshness_state": lifecycle.freshness_state,
        "observed_at": lifecycle.observed_at,
        "data_as_of": lifecycle.data_as_of,
        "published_at": lifecycle.published_at,
        "event_at": lifecycle.event_at,
        "valid_from": lifecycle.valid_from,
        "valid_until": lifecycle.valid_until,
        "next_refresh_at": lifecycle.next_refresh_at,
        "next_retry_at": lifecycle.next_retry_at,
        "superseded_by": lifecycle.superseded_by,
        "refresh_reason": lifecycle.refresh_reason,
        "materiality_fingerprint": lifecycle.materiality_fingerprint,
        "source_lineage_json": _canonical(list(lifecycle.source_lineage)),
        "acquisition_method": lifecycle.acquisition_method,
        "retry_class": lifecycle.retry_class,
        "retry_policy_json": _canonical(lifecycle.retry_policy),
        "negative_cache_key": lifecycle.negative_cache_key,
        "negative_cache_expires_at": lifecycle.negative_cache_expires_at,
        "session_state": lifecycle.session_state,
        "triggering_event": lifecycle.triggering_event,
        "fields_attempted_json": _canonical(list(lifecycle.fields_attempted)),
        "attempt_count": lifecycle.attempt_count,
        "work_status": work_status,
    }


def _lifecycle_persistence_is_unchanged(
    row: Any,
    *,
    lifecycle: DatumLifecycle,
    payload: dict[str, Any],
    work_status: str,
) -> bool:
    if row is None:
        return False
    expected = _lifecycle_persistence_values(
        lifecycle,
        work_status=work_status,
    )
    if any(row[field] != expected[field] for field in _LIFECYCLE_PERSISTED_FIELDS):
        return False
    try:
        existing_payload = json.loads(row["payload_json"] or "{}")
    except (TypeError, ValueError):
        return False
    if materiality_fingerprint(existing_payload) != materiality_fingerprint(
        payload
    ):
        return False
    return not (
        work_status != "LEASED"
        and any(
            row[field] is not None
            for field in ("lease_owner", "lease_expires_at", "heartbeat_at")
        )
    )


class LifecycleRepository:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        migrate_database(settings.database_path)

    def upsert(
        self,
        lifecycle: DatumLifecycle,
        *,
        payload: dict[str, Any] | None = None,
        work_status: str | None = None,
    ) -> dict[str, Any]:
        now = _iso(self.clock())
        item_id = f"datum-{uuid.uuid5(uuid.NAMESPACE_URL, lifecycle.entity_type + ':' + lifecycle.entity_key)}"
        status = work_status or (
            "BACKOFF"
            if lifecycle.freshness_state == "NO_DATA_BACKOFF"
            else "READY"
            if lifecycle.freshness_state
            in {"DUE", "AWAITING_ACTUAL", "STALE_LAST_KNOWN_GOOD"}
            else "COMPLETED"
        )
        persisted_payload = dict(payload or {})
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if lifecycle.entity_type in {
                "macro_actual",
                "fomc_decision",
            }:
                aliases = conn.execute(
                    """
                    SELECT item_id,entity_key,payload_json
                    FROM datum_lifecycle_items
                    WHERE entity_type=? AND entity_key<>?
                    """,
                    (lifecycle.entity_type, lifecycle.entity_key),
                ).fetchall()
                for alias in aliases:
                    try:
                        alias_payload = json.loads(
                            alias["payload_json"] or "{}"
                        )
                    except (TypeError, ValueError):
                        continue
                    if not _same_lifecycle_occurrence(
                        alias_payload,
                        persisted_payload,
                    ):
                        continue
                    conn.execute(
                        """
                        UPDATE datum_lifecycle_items
                        SET work_status='SUPERSEDED',superseded_by=?,
                            next_refresh_at=NULL,next_retry_at=NULL,
                            lease_owner=NULL,lease_expires_at=NULL,
                            heartbeat_at=NULL,updated_at=?
                        WHERE item_id=? AND work_status<>'LEASED'
                        """,
                        (item_id, now, alias["item_id"]),
                    )
            existing = conn.execute(
                """
                SELECT * FROM datum_lifecycle_items
                WHERE entity_type=? AND entity_key=?
                """,
                (lifecycle.entity_type, lifecycle.entity_key),
            ).fetchone()
            if _lifecycle_persistence_is_unchanged(
                existing,
                lifecycle=lifecycle,
                payload=persisted_payload,
                work_status=status,
            ):
                conn.commit()
                return _restore_item(existing)
            conn.execute(
                """
                INSERT INTO datum_lifecycle_items(
                  item_id,entity_type,entity_key,trigger_class,freshness_state,
                  observed_at,data_as_of,published_at,event_at,valid_from,valid_until,
                  next_refresh_at,next_retry_at,superseded_by,refresh_reason,
                  materiality_fingerprint,source_lineage_json,acquisition_method,
                  retry_class,retry_policy_json,negative_cache_key,
                  negative_cache_expires_at,session_state,triggering_event,
                  fields_attempted_json,attempt_count,work_status,payload_json,
                  created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(entity_type,entity_key) DO UPDATE SET
                  trigger_class=excluded.trigger_class,
                  freshness_state=excluded.freshness_state,
                  observed_at=excluded.observed_at,
                  data_as_of=excluded.data_as_of,
                  published_at=excluded.published_at,
                  event_at=excluded.event_at,
                  valid_from=excluded.valid_from,
                  valid_until=excluded.valid_until,
                  next_refresh_at=excluded.next_refresh_at,
                  next_retry_at=excluded.next_retry_at,
                  superseded_by=excluded.superseded_by,
                  refresh_reason=excluded.refresh_reason,
                  materiality_fingerprint=excluded.materiality_fingerprint,
                  source_lineage_json=excluded.source_lineage_json,
                  acquisition_method=excluded.acquisition_method,
                  retry_class=excluded.retry_class,
                  retry_policy_json=excluded.retry_policy_json,
                  negative_cache_key=excluded.negative_cache_key,
                  negative_cache_expires_at=excluded.negative_cache_expires_at,
                  session_state=excluded.session_state,
                  triggering_event=excluded.triggering_event,
                  fields_attempted_json=excluded.fields_attempted_json,
                  attempt_count=excluded.attempt_count,
                  work_status=excluded.work_status,
                  payload_json=excluded.payload_json,
                  lease_owner=CASE
                    WHEN excluded.work_status='LEASED'
                      THEN datum_lifecycle_items.lease_owner
                    ELSE NULL
                  END,
                  lease_expires_at=CASE
                    WHEN excluded.work_status='LEASED'
                      THEN datum_lifecycle_items.lease_expires_at
                    ELSE NULL
                  END,
                  heartbeat_at=CASE
                    WHEN excluded.work_status='LEASED'
                      THEN datum_lifecycle_items.heartbeat_at
                    ELSE NULL
                  END,
                  updated_at=excluded.updated_at
                """,
                (
                    item_id,
                    lifecycle.entity_type,
                    lifecycle.entity_key,
                    lifecycle.trigger_class,
                    lifecycle.freshness_state,
                    lifecycle.observed_at,
                    lifecycle.data_as_of,
                    lifecycle.published_at,
                    lifecycle.event_at,
                    lifecycle.valid_from,
                    lifecycle.valid_until,
                    lifecycle.next_refresh_at,
                    lifecycle.next_retry_at,
                    lifecycle.superseded_by,
                    lifecycle.refresh_reason,
                    lifecycle.materiality_fingerprint,
                    _canonical(list(lifecycle.source_lineage)),
                    lifecycle.acquisition_method,
                    lifecycle.retry_class,
                    _canonical(lifecycle.retry_policy),
                    lifecycle.negative_cache_key,
                    lifecycle.negative_cache_expires_at,
                    lifecycle.session_state,
                    lifecycle.triggering_event,
                    _canonical(list(lifecycle.fields_attempted)),
                    lifecycle.attempt_count,
                    status,
                    _canonical(persisted_payload),
                    now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM datum_lifecycle_items WHERE entity_type=? AND entity_key=?",
                (lifecycle.entity_type, lifecycle.entity_key),
            ).fetchone()
        return _restore_item(row)

    def record_no_data(
        self,
        entity_type: str,
        entity_key: str,
        *,
        fields_attempted: list[str],
        sources_attempted: list[dict[str, Any]],
        reason: str,
        session_state: str | None,
        triggering_event: str | None,
        attempt_count: int = 0,
    ) -> dict[str, Any]:
        lifecycle = compute_datum_lifecycle(
            entity_type,
            entity_key,
            {"refresh_reason": reason},
            settings=self.settings,
            now=self.clock(),
            attempt_count=attempt_count,
            no_data=True,
            fields_attempted=fields_attempted,
            session_state=session_state,
            triggering_event=triggering_event,
            retry_class="NO_DATA",
            refresh_reason=reason,
        )
        return self.upsert(
            lifecycle,
            payload={
                "reason": reason,
                "searched_at": _iso(self.clock()),
                "sources_attempted": sources_attempted[:20],
                "fields_attempted": fields_attempted,
            },
            work_status="BACKOFF",
        )

    def negative_cache(
        self,
        entity_type: str,
        entity_key: str,
        fields: list[str] | tuple[str, ...],
        *,
        session_state: str | None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        cache_key = negative_cache_fingerprint(
            entity_type,
            entity_key,
            fields,
            session_state=session_state,
        )
        reference = _iso(now or self.clock())
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM datum_lifecycle_items
                WHERE negative_cache_key=?
                  AND negative_cache_expires_at>?
                  AND freshness_state='NO_DATA_BACKOFF'
                """,
                (cache_key, reference),
            ).fetchone()
        return _restore_item(row) if row else None

    def claim_due(
        self,
        *,
        owner: str,
        limit: int | None = None,
        now: datetime | None = None,
        due_since: datetime | None = None,
        entity_types: set[str] | frozenset[str] | None = None,
        priority_since: datetime | None = None,
    ) -> list[dict[str, Any]]:
        reference = _aware(now or self.clock())
        timestamp = _iso(reference)
        due_since_value = _iso(due_since) if due_since is not None else None
        lease_until = _iso(
            reference + timedelta(seconds=int(self.settings.lifecycle_due_lease_seconds))
        )
        limit = min(
            int(limit or self.settings.lifecycle_due_max_concurrency),
            max(
                int(self.settings.lifecycle_due_max_concurrency),
                int(self.settings.event_calendar_catchup_max_per_tick),
            ),
        )
        normalized_types = sorted(
            {
                str(entity_type).lower()
                for entity_type in entity_types or set()
                if entity_type
            }
        )
        type_clause = (
            "AND entity_type IN ("
            + ",".join("?" for _ in normalized_types)
            + ")"
            if normalized_types
            else ""
        )
        priority_value = (
            _iso(priority_since) if priority_since is not None else None
        )
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                f"""
                SELECT * FROM datum_lifecycle_items
                WHERE (
                  work_status='READY'
                  OR (
                    work_status IN ('COMPLETED','IDLE')
                    AND COALESCE(next_retry_at,next_refresh_at)<=?
                  )
                  OR (work_status='BACKOFF' AND next_retry_at<=?)
                  OR (work_status='LEASED' AND lease_expires_at<=?)
                )
                AND COALESCE(next_retry_at,next_refresh_at,updated_at)<=?
                AND (
                  ? IS NULL
                  OR COALESCE(event_at,next_retry_at,next_refresh_at,updated_at)>=?
                )
                {type_clause}
                ORDER BY
                  CASE
                    WHEN ? IS NOT NULL AND COALESCE(event_at,updated_at)>=?
                      THEN 0
                    ELSE 1
                  END,
                  CASE UPPER(COALESCE(json_extract(payload_json,'$.impact'),''))
                    WHEN 'HIGH' THEN 0
                    WHEN 'MEDIUM' THEN 1
                    WHEN 'LOW' THEN 2
                    ELSE 3
                  END,
                  COALESCE(next_retry_at,next_refresh_at,updated_at),item_id
                LIMIT ?
                """,
                (
                    timestamp,
                    timestamp,
                    timestamp,
                    timestamp,
                    due_since_value,
                    due_since_value,
                    *normalized_types,
                    priority_value,
                    priority_value,
                    limit,
                ),
            ).fetchall()
            ids = [str(row["item_id"]) for row in rows]
            for item_id in ids:
                conn.execute(
                    """
                    UPDATE datum_lifecycle_items
                    SET work_status='LEASED',lease_owner=?,lease_expires_at=?,
                        heartbeat_at=?,updated_at=?
                    WHERE item_id=?
                    """,
                    (owner, lease_until, timestamp, timestamp, item_id),
                )
            conn.commit()
            claimed = [
                conn.execute(
                    "SELECT * FROM datum_lifecycle_items WHERE item_id=?",
                    (item_id,),
                ).fetchone()
                for item_id in ids
            ]
        return [_restore_item(row) for row in claimed if row is not None]

    def count_due(
        self,
        *,
        now: datetime | None = None,
        due_since: datetime | None = None,
        entity_types: set[str] | frozenset[str] | None = None,
    ) -> int:
        reference = _iso(now or self.clock())
        due_since_value = _iso(due_since) if due_since is not None else None
        normalized_types = sorted(
            {
                str(entity_type).lower()
                for entity_type in entity_types or set()
                if entity_type
            }
        )
        type_clause = (
            "AND entity_type IN ("
            + ",".join("?" for _ in normalized_types)
            + ")"
            if normalized_types
            else ""
        )
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS due_count
                FROM datum_lifecycle_items
                WHERE (
                  work_status='READY'
                  OR (
                    work_status IN ('COMPLETED','IDLE')
                    AND COALESCE(next_retry_at,next_refresh_at)<=?
                  )
                  OR (work_status='BACKOFF' AND next_retry_at<=?)
                  OR (work_status='LEASED' AND lease_expires_at<=?)
                )
                AND COALESCE(next_retry_at,next_refresh_at,updated_at)<=?
                AND (
                  ? IS NULL
                  OR COALESCE(event_at,next_retry_at,next_refresh_at,updated_at)>=?
                )
                {type_clause}
                """,
                (
                    reference,
                    reference,
                    reference,
                    reference,
                    due_since_value,
                    due_since_value,
                    *normalized_types,
                ),
            ).fetchone()
        return int(row["due_count"] if row else 0)

    def heartbeat(
        self,
        item_id: str,
        *,
        owner: str,
        now: datetime | None = None,
    ) -> bool:
        reference = _aware(now or self.clock())
        with connect_sqlite(self.settings.database_path) as conn:
            cursor = conn.execute(
                """
                UPDATE datum_lifecycle_items
                SET heartbeat_at=?,lease_expires_at=?,updated_at=?
                WHERE item_id=? AND work_status='LEASED' AND lease_owner=?
                """,
                (
                    _iso(reference),
                    _iso(
                        reference
                        + timedelta(
                            seconds=int(self.settings.lifecycle_due_lease_seconds)
                        )
                    ),
                    _iso(reference),
                    item_id,
                    owner,
                ),
            )
            conn.commit()
        return int(cursor.rowcount or 0) == 1

    def complete(
        self,
        item_id: str,
        *,
        owner: str,
        next_refresh_at: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        timestamp = _iso(now or self.clock())
        with connect_sqlite(self.settings.database_path) as conn:
            cursor = conn.execute(
                """
                UPDATE datum_lifecycle_items
                SET work_status='COMPLETED',freshness_state='FRESH',
                    next_refresh_at=COALESCE(?,next_refresh_at),
                    lease_owner=NULL,lease_expires_at=NULL,heartbeat_at=NULL,
                    updated_at=?
                WHERE item_id=? AND work_status='LEASED' AND lease_owner=?
                """,
                (next_refresh_at, timestamp, item_id, owner),
            )
            conn.commit()
        return int(cursor.rowcount or 0) == 1

    def transition(
        self,
        item_id: str,
        *,
        owner: str,
        work_status: str,
        refresh_reason: str,
        next_refresh_at: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        timestamp = _iso(now or self.clock())
        with connect_sqlite(self.settings.database_path) as conn:
            cursor = conn.execute(
                """
                UPDATE datum_lifecycle_items
                SET work_status=?,refresh_reason=?,
                    next_refresh_at=COALESCE(?,next_refresh_at),
                    lease_owner=NULL,lease_expires_at=NULL,heartbeat_at=NULL,
                    updated_at=?
                WHERE item_id=? AND work_status='LEASED' AND lease_owner=?
                """,
                (
                    work_status,
                    refresh_reason,
                    next_refresh_at,
                    timestamp,
                    item_id,
                    owner,
                ),
            )
            conn.commit()
        return int(cursor.rowcount or 0) == 1

    def transition_finalized(
        self,
        item_id: str,
        *,
        expected_work_status: str,
        work_status: str,
        refresh_reason: str,
        next_refresh_at: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Advance an already finalized item without reacquiring its lease."""
        timestamp = _iso(now or self.clock())
        with connect_sqlite(self.settings.database_path) as conn:
            cursor = conn.execute(
                """
                UPDATE datum_lifecycle_items
                SET work_status=?,refresh_reason=?,
                    next_refresh_at=COALESCE(?,next_refresh_at),
                    lease_owner=NULL,lease_expires_at=NULL,heartbeat_at=NULL,
                    updated_at=?
                WHERE item_id=? AND work_status=?
                  AND lease_owner IS NULL
                  AND lease_expires_at IS NULL
                  AND heartbeat_at IS NULL
                """,
                (
                    work_status,
                    refresh_reason,
                    next_refresh_at,
                    timestamp,
                    item_id,
                    expected_work_status,
                ),
            )
            conn.commit()
        return int(cursor.rowcount or 0) == 1

    def list_items(self, *, status: str | None = None) -> list[dict[str, Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM datum_lifecycle_items
                WHERE (? IS NULL OR work_status=?)
                ORDER BY COALESCE(next_retry_at,next_refresh_at,updated_at),item_id
                """,
                (status, status),
            ).fetchall()
        return [_restore_item(row) for row in rows]


def persist_lifecycle_in_transaction(
    conn: Any,
    lifecycle: DatumLifecycle,
    *,
    payload: dict[str, Any],
    work_status: str,
    timestamp: str,
) -> str:
    """Persist a lifecycle atomically with the caller's domain transaction."""
    item_id = (
        "datum-"
        + str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                lifecycle.entity_type + ":" + lifecycle.entity_key,
            )
        )
    )
    existing = conn.execute(
        """
        SELECT * FROM datum_lifecycle_items
        WHERE entity_type=? AND entity_key=?
        """,
        (lifecycle.entity_type, lifecycle.entity_key),
    ).fetchone()
    if _lifecycle_persistence_is_unchanged(
        existing,
        lifecycle=lifecycle,
        payload=payload,
        work_status=work_status,
    ):
        return item_id
    conn.execute(
        """
        INSERT INTO datum_lifecycle_items(
          item_id,entity_type,entity_key,trigger_class,freshness_state,
          observed_at,data_as_of,published_at,event_at,valid_from,valid_until,
          next_refresh_at,next_retry_at,superseded_by,refresh_reason,
          materiality_fingerprint,source_lineage_json,acquisition_method,
          retry_class,retry_policy_json,negative_cache_key,
          negative_cache_expires_at,session_state,triggering_event,
          fields_attempted_json,attempt_count,work_status,payload_json,
          created_at,updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(entity_type,entity_key) DO UPDATE SET
          trigger_class=excluded.trigger_class,
          freshness_state=excluded.freshness_state,
          observed_at=excluded.observed_at,
          data_as_of=excluded.data_as_of,
          published_at=excluded.published_at,
          event_at=excluded.event_at,
          valid_from=excluded.valid_from,
          valid_until=excluded.valid_until,
          next_refresh_at=excluded.next_refresh_at,
          next_retry_at=excluded.next_retry_at,
          superseded_by=excluded.superseded_by,
          refresh_reason=excluded.refresh_reason,
          materiality_fingerprint=excluded.materiality_fingerprint,
          source_lineage_json=excluded.source_lineage_json,
          acquisition_method=excluded.acquisition_method,
          retry_class=excluded.retry_class,
          retry_policy_json=excluded.retry_policy_json,
          negative_cache_key=excluded.negative_cache_key,
          negative_cache_expires_at=excluded.negative_cache_expires_at,
          session_state=excluded.session_state,
          triggering_event=excluded.triggering_event,
          fields_attempted_json=excluded.fields_attempted_json,
          attempt_count=excluded.attempt_count,
          work_status=excluded.work_status,
          payload_json=excluded.payload_json,
          lease_owner=CASE
            WHEN excluded.work_status='LEASED'
              THEN datum_lifecycle_items.lease_owner
            ELSE NULL
          END,
          lease_expires_at=CASE
            WHEN excluded.work_status='LEASED'
              THEN datum_lifecycle_items.lease_expires_at
            ELSE NULL
          END,
          heartbeat_at=CASE
            WHEN excluded.work_status='LEASED'
              THEN datum_lifecycle_items.heartbeat_at
            ELSE NULL
          END,
          updated_at=excluded.updated_at
        """,
        (
            item_id,
            lifecycle.entity_type,
            lifecycle.entity_key,
            lifecycle.trigger_class,
            lifecycle.freshness_state,
            lifecycle.observed_at,
            lifecycle.data_as_of,
            lifecycle.published_at,
            lifecycle.event_at,
            lifecycle.valid_from,
            lifecycle.valid_until,
            lifecycle.next_refresh_at,
            lifecycle.next_retry_at,
            lifecycle.superseded_by,
            lifecycle.refresh_reason,
            lifecycle.materiality_fingerprint,
            _canonical(list(lifecycle.source_lineage)),
            lifecycle.acquisition_method,
            lifecycle.retry_class,
            _canonical(lifecycle.retry_policy),
            lifecycle.negative_cache_key,
            lifecycle.negative_cache_expires_at,
            lifecycle.session_state,
            lifecycle.triggering_event,
            _canonical(list(lifecycle.fields_attempted)),
            lifecycle.attempt_count,
            work_status,
            _canonical(payload),
            timestamp,
            timestamp,
        ),
    )
    return item_id


def _restore_item(row: Any) -> dict[str, Any]:
    output = dict(row)
    for column in (
        "source_lineage_json",
        "retry_policy_json",
        "fields_attempted_json",
        "payload_json",
    ):
        try:
            output[column.removesuffix("_json")] = json.loads(output.pop(column) or "{}")
        except (TypeError, ValueError):
            output[column.removesuffix("_json")] = (
                [] if column in {"source_lineage_json", "fields_attempted_json"} else {}
            )
    return output


def _retry_policy(settings: Settings, retry_class: str) -> dict[str, Any]:
    delays = _retry_delays(settings)
    return {
        "retry_class": retry_class,
        "delays_seconds": delays,
        "max_attempts": len(delays),
        "bounded": True,
    }


def _retry_delays(settings: Settings) -> list[int]:
    values: list[int] = []
    for raw in str(settings.lifecycle_no_data_retry_seconds).split(","):
        try:
            value = int(raw.strip())
        except ValueError:
            continue
        if value > 0:
            values.append(value)
    return values or [900, 3600, 21600, 86400]


def _bounded_retry_at(
    now: datetime,
    *,
    attempt_count: int,
    delays: list[int],
) -> datetime:
    index = min(max(int(attempt_count), 0), len(delays) - 1)
    return _aware(now) + timedelta(seconds=delays[index])


def _same_lifecycle_occurrence(
    left: dict[str, Any],
    right: dict[str, Any],
) -> bool:
    left_lineage = (
        left.get("lineage")
        if isinstance(left.get("lineage"), dict)
        else {}
    )
    right_lineage = (
        right.get("lineage")
        if isinstance(right.get("lineage"), dict)
        else {}
    )
    left_id = str(
        left.get("provider_event_id")
        or left_lineage.get("provider_event_id")
        or left.get("event_id")
        or ""
    )
    right_id = str(
        right.get("provider_event_id")
        or right_lineage.get("provider_event_id")
        or right.get("event_id")
        or ""
    )
    if not left_id or left_id != right_id:
        return False
    left_release = _first_time(
        left,
        "event_at",
        "release_at",
        "time_utc",
        "scheduled_at_utc",
    )
    right_release = _first_time(
        right,
        "event_at",
        "release_at",
        "time_utc",
        "scheduled_at_utc",
    )
    return bool(
        left_release is not None
        and right_release is not None
        and left_release.replace(second=0, microsecond=0)
        == right_release.replace(second=0, microsecond=0)
    )


def _scheduled_actual_missing(
    entity_type: str,
    value: dict[str, Any],
) -> bool:
    if entity_type in {"macro_actual", "fomc_decision"}:
        return value.get("actual") in (None, "")
    if entity_type not in {
        "earnings",
        "earnings_schedule",
        "earnings_actual",
        "earnings_intelligence",
    }:
        return False
    return all(
        value.get(key) in (None, "")
        for key in ("actual_eps", "eps_actual", "actual_revenue", "revenue_actual")
    )


def _event_time(value: dict[str, Any], *, settings: Settings) -> datetime | None:
    exact = _first_time(
        value,
        "event_at",
        "release_at",
        "time_utc",
        "scheduled_at_utc",
    )
    if exact is not None:
        return exact
    raw_date = _date_value(
        value.get("earnings_date")
        or value.get("date")
        or value.get("event_date")
        or value.get("scheduled_at")
    )
    if raw_date is None:
        return None
    return datetime.combine(raw_date, time.min, NEW_YORK).astimezone(UTC) + timedelta(
        hours=int(settings.earnings_unknown_time_window_hours)
    )


def _date_value(value: Any) -> date | None:
    parsed = parse_datetime(value)
    if parsed is not None:
        return parsed.date()
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _first_time(value: dict[str, Any], *keys: str) -> datetime | None:
    for key in keys:
        parsed = parse_datetime(value.get(key))
        if parsed is not None:
            return _aware(parsed)
    return None


def _carry_forward_allowed(entity_type: str) -> bool:
    return entity_type in {
        "cot",
        "cot_positioning",
        "macro_snapshot",
        "market_schedule",
        "structural_fact",
    }


def _default_ttl(entity_type: str, settings: Settings) -> timedelta:
    if entity_type == "news":
        return timedelta(hours=int(settings.default_news_ttl_hours))
    if entity_type in {"vix", "vvix", "vix_futures", "put_call", "skew"}:
        return timedelta(minutes=int(settings.risk_context_ttl_minutes))
    if entity_type in {"options_positioning", "market_internals", "cross_asset_context"}:
        return timedelta(minutes=60)
    if entity_type in {"earnings", "earnings_schedule", "earnings_intelligence"}:
        return timedelta(hours=int(settings.earnings_ttl_hours))
    if entity_type in {"cot", "cot_positioning"}:
        return timedelta(days=7)
    if entity_type in {"market_schedule", "macro_schedule"}:
        return timedelta(hours=24)
    return timedelta(hours=int(settings.default_fact_ttl_hours))


def _has_material_data(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            key not in VOLATILE_FINGERPRINT_KEYS
            and key not in {"warnings", "errors", "lifecycle"}
            and _has_material_data(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_has_material_data(item) for item in value)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, bool):
        return value
    return True


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _strip_volatile(item)
            for key, item in sorted(value.items())
            if str(key) not in VOLATILE_FINGERPRINT_KEYS
            and str(key) not in {"audit", "telemetry", "diagnostics"}
        }
    if isinstance(value, list):
        return [_strip_volatile(item) for item in value]
    return value


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _aware(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _iso(value: datetime | None) -> str | None:
    return (
        _aware(value).replace(microsecond=0).isoformat()
        if value is not None
        else None
    )
