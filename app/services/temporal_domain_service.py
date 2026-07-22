from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import UTC, datetime, time
from typing import Any, Iterable

from app.models.common import Impact, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.services.data_freshness_service import parse_datetime


SPEECH_TOKENS = ("speech", "testimony", "testifies", "conference", "remarks", "press conference")

FAMILY_ALIASES: dict[str, tuple[str, ...]] = {
    "CPI": ("cpi", "ipc", "consumer price index", "indice prezzi al consumo", "inflazione al consumo"),
    "PPI": ("ppi", "ipp", "producer price index", "indice prezzi alla produzione", "prezzi produzione"),
    "PCE": ("pce", "personal consumption expenditures", "spese per consumi personali"),
    "GDP": ("gdp", "pil", "gross domestic product", "prodotto interno lordo"),
    "NFP": ("nfp", "nonfarm payroll", "non farm payroll", "job report", "rapporto occupazione", "buste paga non agricole"),
    "INITIAL_JOBLESS_CLAIMS": ("initial jobless claims", "jobless claims", "richieste sussidi disoccupazione", "sussidi disoccupazione"),
    "FOMC": ("fomc", "federal open market committee", "decisione tassi fed"),
    "FED_SPEECH": ("fed speech", "federal reserve speech", "discorso fed", "testimonianza fed"),
}


def canonical_event_key(event: dict[str, Any] | EconomicEvent) -> str:
    item = event.model_dump(mode="json") if hasattr(event, "model_dump") else dict(event)
    country = str(item.get("country") or item.get("country_code") or "US").upper()
    family = _family(item)
    release = parse_datetime(item.get("release_at") or item.get("time_utc"))
    release_key = release.replace(second=0, microsecond=0).isoformat() if release else str(item.get("date") or "")
    period = _normalized(item.get("reference_period") or item.get("period") or "unspecified")
    frequency = _normalized(item.get("frequency") or "unspecified")
    classified = family in FAMILY_ALIASES
    name = "" if classified else _normalized(item.get("name") or item.get("event_name") or item.get("original_title") or family)
    stable = f"{country}|{family}|{release_key}|{period}|{frequency}|{name}"
    return f"event:{hashlib.sha256(stable.encode('utf-8')).hexdigest()[:24]}"


def temporal_event_state(event: dict[str, Any] | EconomicEvent, *, now: datetime | None = None) -> dict[str, Any]:
    item = event.model_dump(mode="json") if hasattr(event, "model_dump") else dict(event)
    now = _aware(now or datetime.now(UTC))
    release = parse_datetime(item.get("release_at") or item.get("time_utc"))
    event_kind = "scheduled_speech" if _is_speech(item) else "scheduled_event"
    enrichment = item.get("enrichment") if isinstance(item.get("enrichment"), dict) else {}
    actual = item.get("actual") if item.get("actual") not in (None, "") else enrichment.get("actual")
    outcome = item.get("outcome") or enrichment.get("outcome") or (enrichment.get("summary") or {}).get("outcome")
    if release is None or now < release:
        status = "PRE_RELEASE"
        actual = None
    elif event_kind == "scheduled_speech":
        status = "COMPLETED" if outcome else "AWAITING_OUTCOME"
        actual = None
    else:
        status = "RELEASED" if actual not in (None, "") else "AWAITING_ACTUAL"
    return {
        "canonical_event_key": canonical_event_key(item),
        "event_kind": event_kind,
        "temporal_status": status,
        "release_at": release.isoformat() if release else None,
        "actual": actual,
        "outcome": outcome,
    }


def annotate_event(event: EconomicEvent, *, now: datetime | None = None) -> EconomicEvent:
    updated = event.model_copy(deep=True)
    state = temporal_event_state(updated, now=now)
    if state["temporal_status"] == "PRE_RELEASE":
        updated.actual = None
        updated.enrichment.actual = None
    updated.enrichment.summary = {
        **updated.enrichment.summary,
        "temporal_domain": state,
    }
    return updated


def reconcile_calendar_events(
    events: Iterable[EconomicEvent],
    provider_payloads: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> list[EconomicEvent]:
    """Merge compatible calendar rows by canonical identity; never promote aggregator actuals."""
    now = _aware(now or datetime.now(UTC))
    selected: dict[str, EconomicEvent] = {}
    for event in events:
        annotated = annotate_event(event, now=now)
        selected[canonical_event_key(annotated)] = annotated
    for payload in provider_payloads:
        source = str(payload.get("source") or payload.get("provider") or "calendar_provider")
        source_url = str(payload.get("source_url") or "")
        for row in payload.get("items") or payload.get("events") or []:
            if not isinstance(row, dict) or str(row.get("country") or row.get("country_code") or "US").upper() != "US":
                continue
            normalized = _provider_event(row, source=source, source_url=source_url, now=now)
            key = canonical_event_key(normalized)
            current = selected.get(key)
            if current is None:
                compatible_key = next(
                    (existing_key for existing_key, existing in selected.items() if _same_occurrence(existing, normalized)),
                    None,
                )
                if compatible_key is not None:
                    key = compatible_key
                    current = selected[compatible_key]
            if current is None:
                selected[key] = normalized
                continue
            selected[key] = _merge_candidate(current, row, source=source, source_url=source_url, now=now)
    return sorted(selected.values(), key=lambda event: (event.time_utc or datetime.combine(datetime.fromisoformat(event.date).date(), time.max, UTC), event.name))


def _provider_event(row: dict[str, Any], *, source: str, source_url: str, now: datetime) -> EconomicEvent:
    release = parse_datetime(row.get("release_at") or row.get("time_utc"))
    date_value = str(row.get("date") or (release.date().isoformat() if release else now.date().isoformat()))
    impact_text = str(row.get("impact") or "MEDIUM").upper()
    impact = Impact(impact_text) if impact_text in {item.value for item in Impact} else Impact.MEDIUM
    name = str(row.get("event_name") or row.get("name") or row.get("original_title") or "Economic event")
    category = _family(row)
    consensus = row.get("consensus")
    previous = row.get("previous")
    field_lineage = {
        field: {
            "source": source,
            "source_url": source_url or row.get(f"{field}_source_url"),
            "provider_type": "API",
            "value": row.get(field),
            "retrieved_at": row.get("retrieved_at"),
            "validation": {"status": "candidate_preserved", "official_actual": False},
        }
        for field in ("consensus", "forecast", "previous")
        if row.get(field) not in (None, "")
    }
    event = EconomicEvent(
        event_id=str(row.get("occurrence_id") or row.get("source_event_id") or canonical_event_key(row)),
        name=name,
        country="US",
        category=category,
        metric_id=row.get("metric_id") or row.get("series_id"),
        normalized_event_family=category,
        reference_period=row.get("reference_period") or row.get("period"),
        frequency=row.get("frequency"),
        date=date_value,
        time_utc=release,
        impact=impact,
        actual=None,
        forecast=None,
        previous=previous,
        source=source,
        source_url=source_url or str(row.get("source_url") or "calendar://unknown"),
        reliability=float(row.get("reliability") or 0.0),
        incomplete_time=release is None,
        event_risk_level=impact,
        enrichment=EventEnrichment(
            forecast=row.get("forecast"),
            consensus=consensus,
            previous=previous,
            actual=None,
            source=source,
            source_url=source_url or row.get("source_url"),
            provider_type=ProviderType.API,
            retrieved_at=row.get("retrieved_at"),
            reliability=float(row.get("reliability") or 0.0),
            confidence=float(row.get("confidence") or row.get("reliability") or 0.0),
            field_lineage=field_lineage,
        ),
    )
    return annotate_event(event, now=now)


def _merge_candidate(
    event: EconomicEvent,
    row: dict[str, Any],
    *,
    source: str,
    source_url: str,
    now: datetime,
) -> EconomicEvent:
    updated = event.model_copy(deep=True)
    lineage = dict(updated.enrichment.field_lineage)
    conflicts: list[dict[str, Any]] = list(updated.enrichment.summary.get("discordant_candidates") or [])
    for field in ("forecast", "consensus", "previous"):
        candidate = row.get(field)
        if candidate in (None, ""):
            continue
        current = getattr(updated.enrichment, field)
        candidate_lineage = {
            "source": source,
            "source_url": source_url or row.get("source_url"),
            "provider_type": "API",
            "value": candidate,
            "retrieved_at": row.get("retrieved_at"),
            "validation": {"status": "candidate_preserved"},
        }
        if current in (None, ""):
            setattr(updated.enrichment, field, candidate)
            lineage[field] = candidate_lineage
        elif str(current) != str(candidate):
            conflicts.append({"field": field, "existing_value": current, "candidate_value": candidate, **candidate_lineage})
    updated.enrichment.field_lineage = lineage
    updated.enrichment.summary = {**updated.enrichment.summary, "discordant_candidates": conflicts}
    return annotate_event(updated, now=now)


def _family(item: dict[str, Any]) -> str:
    fields = " ".join(
        str(item.get(key) or "")
        for key in (
            "normalized_event_family", "metric_id", "series_id", "normalized_event_type",
            "category", "event_type", "name", "event_name", "original_title",
        )
    )
    text = _normalized(fields)
    compact = text.replace(" ", "")
    for family, aliases in FAMILY_ALIASES.items():
        if any(_normalized(alias) in text or _normalized(alias).replace(" ", "") in compact for alias in aliases):
            return family
    fallback = _normalized(item.get("normalized_event_family") or item.get("metric_id") or item.get("category") or item.get("name") or "OTHER")
    return fallback.upper().replace(" ", "_")[:80] or "OTHER"


def _is_speech(item: dict[str, Any]) -> bool:
    text = _normalized(" ".join(str(item.get(key) or "") for key in ("name", "event_name", "category", "normalized_event_type")))
    return _family(item) == "FED_SPEECH" or "fed communication" in text or any(token in text for token in SPEECH_TOKENS)


def _same_occurrence(left: EconomicEvent, right: EconomicEvent) -> bool:
    left_payload = left.model_dump(mode="json")
    right_payload = right.model_dump(mode="json")
    left_release = parse_datetime(left_payload.get("time_utc"))
    right_release = parse_datetime(right_payload.get("time_utc"))
    if not left_release or not right_release:
        return False
    if left.country.upper() != right.country.upper() or _family(left_payload) != _family(right_payload):
        return False
    if left_release.replace(second=0, microsecond=0) != right_release.replace(second=0, microsecond=0):
        return False
    left_period = _normalized(left.reference_period or "")
    right_period = _normalized(right.reference_period or "")
    return not left_period or not right_period or left_period == right_period


def _normalized(value: Any) -> str:
    ascii_value = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", ascii_value.lower()).strip()


def _aware(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
