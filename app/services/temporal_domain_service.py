from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import UTC, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from app.models.common import Impact, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.services.data_freshness_service import parse_datetime
from app.services.official_actual_semantics import normalize_reference_period
from app.services.temporal_validation_service import (
    QUARANTINED_STATUS,
    TemporalPolicy,
    TemporalValidationService,
)


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
    provider = _normalized(item.get("provider") or item.get("source") or "")
    provider_event_id = _normalized(
        item.get("provider_event_id")
        or item.get("source_event_id")
        or item.get("occurrence_id")
        or ""
    )
    if provider and provider_event_id:
        normalized_type = _normalized(
            item.get("normalized_event_type")
            or item.get("provider_event_type")
            or "provider_event"
        )
        stable = (
            f"{provider}|{provider_event_id}|{country}|{release_key}|"
            f"{normalized_type}"
        )
        return f"event:{hashlib.sha256(stable.encode('utf-8')).hexdigest()[:24]}"
    period = _normalized(item.get("reference_period") or item.get("period") or "unspecified")
    frequency = _normalized(item.get("frequency") or "unspecified")
    classified = family in FAMILY_ALIASES
    name = "" if classified else _normalized(item.get("name") or item.get("event_name") or item.get("original_title") or family)
    stable = f"{country}|{family}|{release_key}|{period}|{frequency}|{name}"
    return f"event:{hashlib.sha256(stable.encode('utf-8')).hexdigest()[:24]}"


def exact_occurrence_key(event: dict[str, Any] | EconomicEvent) -> str:
    """Return the provider occurrence identity before semantic fallbacks.

    An occurrence id is an immutable release identity.  The semantic hash is
    intentionally only a fallback because title, period and provider metadata
    can become richer between calendar reads without creating a new release.
    """

    item = event.model_dump(mode="json") if hasattr(event, "model_dump") else dict(event)
    occurrence_id = str(item.get("occurrence_id") or "").strip()
    if occurrence_id:
        return occurrence_id
    event_id = str(item.get("event_id") or "").strip()
    if ":" in event_id:
        return event_id
    persisted = str(item.get("canonical_event_key") or "").strip()
    if persisted:
        return persisted
    if event_id:
        return event_id
    return canonical_event_key(item)


def temporal_event_state(
    event: dict[str, Any] | EconomicEvent,
    *,
    now: datetime | None = None,
    policy: TemporalPolicy | None = None,
) -> dict[str, Any]:
    item = event.model_dump(mode="json") if hasattr(event, "model_dump") else dict(event)
    now = _aware(now or datetime.now(UTC))
    policy = policy or TemporalPolicy(clock=lambda: now)
    decision = policy.evaluate(item, domain="macro_calendar")
    release = parse_datetime(item.get("release_at") or item.get("time_utc"))
    event_kind = "scheduled_speech" if _is_speech(item) else "scheduled_event"
    enrichment = item.get("enrichment") if isinstance(item.get("enrichment"), dict) else {}
    actual = item.get("actual") if item.get("actual") not in (None, "") else enrichment.get("actual")
    outcome = item.get("outcome") or enrichment.get("outcome") or (enrichment.get("summary") or {}).get("outcome")
    explicitly_quarantined = str(
        item.get("audit_status")
        or item.get("source_audit_status")
        or item.get("verification_status")
        or ""
    ).upper() in {"QUARANTINED", "REJECTED"}
    if explicitly_quarantined or not decision.accepted:
        status = QUARANTINED_STATUS
    elif release is None or now < release:
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
        "temporal_invalid_reason": (
            "persisted_quarantine"
            if explicitly_quarantined
            else decision.reason_code
        ),
    }


def annotate_event(
    event: EconomicEvent,
    *,
    now: datetime | None = None,
    policy: TemporalPolicy | None = None,
) -> EconomicEvent:
    updated = event.model_copy(deep=True)
    state = temporal_event_state(updated, now=now, policy=policy)
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
    temporal_validation: TemporalValidationService | None = None,
) -> list[EconomicEvent]:
    """Merge calendar rows and atomically promote verified provider records.

    Provider-reported actuals are not relabelled as official observations.  A
    complete post-release record can replace an incomplete occurrence only
    when its identity, retrieval time, numerical field semantics and reference
    period are deterministic.  Discordant complete records fail closed.
    """
    now = _aware(now or datetime.now(UTC))
    policy = (
        temporal_validation.policy
        if temporal_validation is not None
        else TemporalPolicy(clock=lambda: now)
    )
    selected: dict[str, EconomicEvent] = {}
    provider_actual_candidates: dict[str, list[dict[str, Any]]] = {}
    rejected_provider_actuals: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        raw_event = event.model_dump(mode="json")
        if not policy.evaluate(raw_event, domain="macro_calendar").accepted:
            if temporal_validation is not None:
                temporal_validation.quarantine_if_invalid(
                    raw_event,
                    entity_table="provider_ingestion",
                )
            continue
        annotated = annotate_event(event, now=now, policy=policy)
        selected[canonical_event_key(annotated)] = annotated
    for payload in provider_payloads:
        source = str(payload.get("source") or payload.get("provider") or "calendar_provider")
        source_url = str(payload.get("source_url") or "")
        for row in payload.get("items") or payload.get("events") or []:
            if not isinstance(row, dict) or str(row.get("country") or row.get("country_code") or "US").upper() != "US":
                continue
            if not policy.evaluate(row, domain="macro_calendar").accepted:
                if temporal_validation is not None:
                    temporal_validation.quarantine_if_invalid(
                        row,
                        entity_table="provider_ingestion",
                    )
                continue
            normalized = _provider_event(
                row,
                source=source,
                source_url=source_url,
                now=now,
                policy=policy,
            )
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
            else:
                selected[key] = _merge_candidate(
                    current,
                    row,
                    source=source,
                    source_url=source_url,
                    now=now,
                )
            candidate, rejection = _complete_provider_actual_candidate(
                row,
                source=source,
                source_url=source_url,
                now=now,
            )
            if candidate is not None:
                provider_actual_candidates.setdefault(key, []).append(candidate)
            elif rejection is not None:
                rejected_provider_actuals.setdefault(key, []).append(rejection)
    for key, candidates in provider_actual_candidates.items():
        current = selected.get(key)
        if current is None:
            continue
        distinct = {
            str(candidate["fingerprint"]): candidate
            for candidate in candidates
        }
        if len(distinct) != 1:
            selected[key] = _audit_provider_actual_reconciliation(
                current,
                status="CONFLICT",
                reason="discordant_complete_provider_records",
                candidates=list(distinct.values()),
                rejected=rejected_provider_actuals.get(key, []),
                now=now,
            )
            continue
        selected[key] = _promote_complete_provider_record(
            current,
            next(iter(distinct.values())),
            duplicate_observation_count=len(candidates),
            rejected=rejected_provider_actuals.get(key, []),
            now=now,
        )
    for key, rejected in rejected_provider_actuals.items():
        if key in provider_actual_candidates or key not in selected:
            continue
        selected[key] = _audit_provider_actual_reconciliation(
            selected[key],
            status="REJECTED",
            reason="provider_record_not_deterministically_complete",
            candidates=[],
            rejected=rejected,
            now=now,
        )
    return sorted(selected.values(), key=lambda event: (event.time_utc or datetime.combine(datetime.fromisoformat(event.date).date(), time.max, UTC), event.name))


def _provider_event(
    row: dict[str, Any],
    *,
    source: str,
    source_url: str,
    now: datetime,
    policy: TemporalPolicy,
) -> EconomicEvent:
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
        provider=source,
        provider_event_id=(
            str(row.get("provider_event_id") or row.get("event_id"))
            if row.get("provider_event_id") or row.get("event_id")
            else None
        ),
        source_event_id=(
            str(row.get("source_event_id"))
            if row.get("source_event_id")
            else None
        ),
        occurrence_id=(
            str(row.get("occurrence_id"))
            if row.get("occurrence_id")
            else None
        ),
        name=name,
        country="US",
        category=category,
        metric_id=row.get("metric_id") or row.get("series_id"),
        normalized_event_family=category,
        reference_period=row.get("reference_period") or row.get("period"),
        frequency=row.get("frequency"),
        date=date_value,
        time_utc=release,
        release_at=release,
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
    return annotate_event(event, now=now, policy=policy)


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


def _complete_provider_actual_candidate(
    row: dict[str, Any],
    *,
    source: str,
    source_url: str,
    now: datetime,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if row.get("actual") in (None, ""):
        return None, None
    reasons: list[str] = []
    if row.get("actual_is_official") is not True:
        reasons.append("actual_requires_official_source")
    occurrence_id = str(row.get("occurrence_id") or "").strip()
    provider_event_id = str(
        row.get("source_event_id")
        or row.get("provider_event_id")
        or ""
    ).strip()
    if not occurrence_id or not provider_event_id:
        reasons.append("stable_provider_occurrence_identity_missing")
    release = parse_datetime(row.get("release_at") or row.get("time_utc"))
    retrieved = parse_datetime(row.get("retrieved_at"))
    if release is None:
        reasons.append("release_timestamp_missing")
    elif release > now:
        reasons.append("future_actual_rejected")
    if retrieved is None:
        reasons.append("retrieved_at_missing")
    elif release is not None and retrieved < release:
        reasons.append("pre_release_provider_record_rejected")
    resolved_source_url = str(source_url or row.get("source_url") or "").strip()
    if not source.strip() or not resolved_source_url.startswith(("https://", "http://")):
        reasons.append("provider_provenance_incomplete")
    if row.get("consensus_verified") is not True:
        reasons.append("consensus_not_verified")

    forecast = (
        row.get("forecast")
        if row.get("forecast") not in (None, "")
        else row.get("consensus")
    )
    consensus = row.get("consensus")
    previous = row.get("previous")
    values = {
        "actual": row.get("actual"),
        "forecast": forecast,
        "previous": previous,
    }
    normalized_values: dict[str, str] = {}
    for field, value in values.items():
        normalized = _normalized_number(value)
        if normalized is None:
            reasons.append(f"{field}_not_numerical")
        else:
            normalized_values[field] = normalized
    if (
        forecast not in (None, "")
        and consensus not in (None, "")
        and _normalized_number(forecast) != _normalized_number(consensus)
    ):
        reasons.append("forecast_consensus_semantic_conflict")

    lineage = row.get("lineage")
    lineage = lineage if isinstance(lineage, dict) else {}
    required_lineage = {
        "actual": {"actual", "current", "observed", "value"},
        "previous": {"previous", "prior"},
    }
    for field, allowed_source_fields in required_lineage.items():
        item = lineage.get(field)
        source_field = (
            str(item.get("source_field") or "").strip().lower()
            if isinstance(item, dict)
            else ""
        )
        if source_field not in allowed_source_fields:
            reasons.append(f"{field}_lineage_missing")
    forecast_lineage = lineage.get("forecast") or lineage.get("consensus")
    forecast_source_field = (
        str(forecast_lineage.get("source_field") or "").strip().lower()
        if isinstance(forecast_lineage, dict)
        else ""
    )
    if forecast_source_field not in {"forecast", "consensus", "estimate"}:
        reasons.append("forecast_lineage_missing")

    frequency = str(row.get("frequency") or "monthly").strip().lower()
    if frequency not in {"monthly", "quarterly"}:
        reasons.append("unsupported_reference_period_frequency")
    period = normalize_reference_period(
        row.get("reference_period") or row.get("period"),
        frequency=frequency,
        release_date=release,
    )
    expected_period_pattern = (
        r"20\d{2}-\d{2}"
        if frequency == "monthly"
        else r"20\d{2}-Q[1-4]"
    )
    if period is None or re.fullmatch(expected_period_pattern, period) is None:
        reasons.append("canonical_reference_period_missing")

    audit = {
        "source": source,
        "source_url": resolved_source_url,
        "occurrence_id": occurrence_id or None,
        "provider_event_id": provider_event_id or None,
        "retrieved_at": retrieved.isoformat() if retrieved else None,
        "release_at": release.isoformat() if release else None,
        "reasons": sorted(set(reasons)),
    }
    if reasons:
        return None, audit
    fingerprint_input = "|".join(
        (
            occurrence_id,
            normalized_values["actual"],
            normalized_values["forecast"],
            normalized_values["previous"],
            str(period),
        )
    )
    return {
        **audit,
        "actual": row.get("actual"),
        "forecast": forecast,
        "previous": previous,
        "reference_period": period,
        "frequency": frequency,
        "fingerprint": hashlib.sha256(
            fingerprint_input.encode("utf-8")
        ).hexdigest(),
    }, None


def _promote_complete_provider_record(
    event: EconomicEvent,
    candidate: dict[str, Any],
    *,
    duplicate_observation_count: int,
    rejected: list[dict[str, Any]],
    now: datetime,
) -> EconomicEvent:
    updated = event.model_copy(deep=True)
    updated.occurrence_id = str(candidate["occurrence_id"])
    updated.reference_period = str(candidate["reference_period"])
    updated.frequency = str(candidate["frequency"])
    updated.actual = candidate["actual"]
    updated.forecast = candidate["forecast"]
    updated.previous = candidate["previous"]
    updated.enrichment.actual = candidate["actual"]
    updated.enrichment.forecast = candidate["forecast"]
    updated.enrichment.consensus = candidate["forecast"]
    updated.enrichment.previous = candidate["previous"]
    updated.enrichment.consensus_verified = True
    updated.enrichment.source = str(candidate["source"])
    updated.enrichment.source_url = str(candidate["source_url"])
    retrieved = parse_datetime(candidate.get("retrieved_at"))
    if retrieved is not None:
        updated.enrichment.retrieved_at = retrieved
    lineage = dict(updated.enrichment.field_lineage)
    for field in ("actual", "forecast", "consensus", "previous"):
        value_field = "forecast" if field == "consensus" else field
        lineage[field] = {
            "source": candidate["source"],
            "source_url": candidate["source_url"],
            "provider_type": "API",
            "value": candidate[value_field],
            "retrieved_at": candidate["retrieved_at"],
            "validation": {
                "status": "provider_complete_record_promoted",
                "official_actual": False,
                "atomic_record": True,
            },
        }
    updated.enrichment.field_lineage = lineage
    updated.validation = {
        **updated.validation,
        "provider_actual_reconciliation": {
            "status": "PROMOTED",
            "official_actual": False,
            "atomic_record": True,
        },
    }
    updated.enrichment.summary = {
        **updated.enrichment.summary,
        "provider_actual_reconciliation": {
            "status": "PROMOTED",
            "reason": "single_deterministic_complete_provider_record",
            "occurrence_id": candidate["occurrence_id"],
            "reference_period": candidate["reference_period"],
            "source": candidate["source"],
            "source_url": candidate["source_url"],
            "retrieved_at": candidate["retrieved_at"],
            "fingerprint": candidate["fingerprint"],
            "duplicate_observation_count": duplicate_observation_count,
            "rejected_candidate_count": len(rejected),
            "rejected_candidates": rejected,
            "official_actual": False,
            "atomic_record": True,
        },
    }
    return annotate_event(updated, now=now)


def _audit_provider_actual_reconciliation(
    event: EconomicEvent,
    *,
    status: str,
    reason: str,
    candidates: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    now: datetime,
) -> EconomicEvent:
    updated = event.model_copy(deep=True)
    updated.enrichment.summary = {
        **updated.enrichment.summary,
        "provider_actual_reconciliation": {
            "status": status,
            "reason": reason,
            "candidate_count": len(candidates),
            "candidate_fingerprints": sorted(
                str(candidate["fingerprint"])
                for candidate in candidates
            ),
            "candidate_sources": sorted(
                {
                    str(candidate["source"])
                    for candidate in candidates
                }
            ),
            "rejected_candidate_count": len(rejected),
            "rejected_candidates": rejected,
            "official_actual": False,
            "atomic_record": True,
        },
    }
    warning = f"provider_actual_reconciliation_{status.lower()}"
    if warning not in updated.enrichment.warnings:
        updated.enrichment.warnings.append(warning)
    return annotate_event(updated, now=now)


def _normalized_number(value: Any) -> str | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None
    if not number.is_finite():
        return None
    return format(number.normalize(), "f")


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
    left_occurrence = str(
        left_payload.get("occurrence_id") or ""
    ).strip()
    right_occurrence = str(
        right_payload.get("occurrence_id") or ""
    ).strip()
    if left_occurrence and left_occurrence == right_occurrence:
        return (
            left.country.upper() == right.country.upper()
            and left_release.replace(second=0, microsecond=0)
            == right_release.replace(second=0, microsecond=0)
        )
    left_provider = _normalized(
        left_payload.get("provider") or left_payload.get("source")
    )
    right_provider = _normalized(
        right_payload.get("provider") or right_payload.get("source")
    )
    left_provider_id = _normalized(
        left_payload.get("provider_event_id")
        or left_payload.get("source_event_id")
        or left_payload.get("occurrence_id")
    )
    right_provider_id = _normalized(
        right_payload.get("provider_event_id")
        or right_payload.get("source_event_id")
        or right_payload.get("occurrence_id")
    )
    if (
        left_provider
        and left_provider == right_provider
        and left_provider_id
        and left_provider_id == right_provider_id
    ):
        return (
            left.country.upper() == right.country.upper()
            and left_release.replace(second=0, microsecond=0)
            == right_release.replace(second=0, microsecond=0)
        )
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
