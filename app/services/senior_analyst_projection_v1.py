from __future__ import annotations

import hashlib
import json
import math
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Iterable
from urllib.parse import urlparse

from app.core.senior_analyst_policy import (
    MAX_SENIOR_ANALYST_PAYLOAD_BYTES,
    MNQ_EARNINGS_SELECTION_POLICY,
    MNQ_PRIMARY_SYMBOLS,
    PROVIDER_ACCOUNTING_COLLECTION_PATHS,
)
from app.services.data_integrity_service import (
    news_content_status,
    substantive_news_text,
)
from app.services.data_freshness_service import (
    _INVALID_LIFECYCLE_STATES,
    parse_datetime,
)
from app.services.market_context_sync_service import extract_sync_sections
from app.services.official_actual_semantics import (
    OFFICIAL_METRICS,
    metric_change_basis_from_text,
    metric_semantics_mismatch_reason,
    normalize_reference_period,
)
from app.services.provider_capability_registry import (
    DATASET_SOURCE_POLICIES,
    MARKET_FACT_REPOSITORY_DATASET_QUERIES,
    dataset_policy_by_id,
    provider_by_id,
)
from app.services.request_provider_accounting import (
    _canonical_database_evidence_valid,
    _provider_flow_valid,
    _shared_acquisition_links_valid,
)


CONTRACT_NAME = "SeniorAnalystPayloadV1"
SCHEMA_VERSION = "1.0"
FLASH_PMI_ACQUISITION_PREFIX = (
    "flash_services_pmi_actual_resolution:"
)
INVALID_ANALYTIC_STATES = {
    "EXPIRED",
    "REJECTED_FUTURE",
    "STALE",
    "VERY_STALE",
}
VALID_FRESHNESS_STATES = {
    "CURRENT",
    "CURRENT_LATEST_OFFICIAL_RELEASE",
    "LAST_AVAILABLE_OFFICIAL_CLOSE",
}
_INVALID_DELIVERY_REFERENCE = object()
REQUIRED_MEGA_CAPS = MNQ_PRIMARY_SYMBOLS


def _registered_repository_series(dataset_id: str) -> frozenset[str]:
    query = MARKET_FACT_REPOSITORY_DATASET_QUERIES.get(dataset_id)
    if query is None or not query.series_ids:
        raise RuntimeError(
            f"SENIOR_ANALYST_REPOSITORY_SERIES_MISSING:{dataset_id}"
        )
    return frozenset(query.series_ids)


def _registered_repository_dataset_id(
    series_id: str,
) -> str | None:
    normalized = str(series_id or "").strip().upper()
    matches = tuple(
        dataset_id
        for dataset_id, query in (
            MARKET_FACT_REPOSITORY_DATASET_QUERIES.items()
        )
        if normalized
        and normalized
        in {
            str(candidate).strip().upper()
            for candidate in query.series_ids
        }
    )
    return matches[0] if len(matches) == 1 else None


TREASURY_RATE_SERIES = _registered_repository_series(
    "treasury_rates"
)
FED_FUNDS_RATE_SERIES = _registered_repository_series("fed_funds")
TARGET_RANGE_SERIES = _registered_repository_series("target_range")
MACRO_DATASET_SERIES = {
    dataset_id: _registered_repository_series(dataset_id)
    for dataset_id in (
        "cpi",
        "ppi",
        "pce",
        "gdp",
        "employment",
        "wages",
        "nfp",
        "jobless_claims",
    )
}
GENERIC_REQUIRED_OMISSION_REASON_CODES = frozenset(
    {
        "",
        "FINAL_PAYLOAD_VALUE_NOT_AVAILABLE",
        "NO_VALID_VALUES_AVAILABLE",
        "ONE_OR_MORE_VALUES_UNAVAILABLE",
        "UNSPECIFIED_OMISSION",
        "VALUE_NOT_AVAILABLE",
    }
)
SECTION_NAMES = (
    "macro",
    "calendar",
    "fomc",
    "nasdaq",
    "market_internals",
    "news",
    "vix",
    "risk",
    "rates",
    "positioning",
    "earnings",
    "options_positioning",
    "market_schedule",
)


@dataclass(frozen=True)
class DatasetPolicy:
    dataset_id: str
    section: str
    frequency: str
    max_age: timedelta
    primary_provider: str
    fallback_providers: tuple[str, ...] = ()
    required_for_analysis: bool = True
    canonical_repository_required: bool = True
    provider_strategy: str = "FALLBACK"


DATASET_POLICIES: tuple[DatasetPolicy, ...] = tuple(
    DatasetPolicy(
        dataset_id=policy.dataset_id,
        section=policy.section,
        frequency=policy.frequency,
        max_age=timedelta(seconds=policy.sla_seconds),
        primary_provider=policy.primary_provider,
        fallback_providers=policy.fallback_providers,
        required_for_analysis=policy.required_for_analysis,
        canonical_repository_required=bool(
            policy.canonical_repository
        ),
        provider_strategy=policy.provider_strategy,
    )
    for policy in DATASET_SOURCE_POLICIES
)


MACRO_SEMANTICS: dict[str, dict[str, str]] = {
    "ICSA": {
        "metric_id": "initial_jobless_claims",
        "name": "Initial Jobless Claims",
        "unit": "claims",
        "transformation": "level",
    },
    "CUSR0000SA0": {
        "metric_id": "headline_cpi_index",
        "name": "Headline Consumer Price Index",
        "unit": "index_points",
        "transformation": "level",
    },
    "CUSR0000SA0L1E": {
        "metric_id": "core_cpi_index",
        "name": "Core Consumer Price Index",
        "unit": "index_points",
        "transformation": "level",
    },
    "BEA:GDP": {
        "metric_id": "real_gdp_annualized_qoq",
        "name": "Real GDP annualized quarterly growth",
        "unit": "percent",
        "transformation": "official_annualized_qoq_rate",
    },
    "BEA:PCE": {
        "metric_id": "personal_consumption_expenditures_nominal_level",
        "name": "Personal Consumption Expenditures",
        "unit": "millions_usd_saar",
        "transformation": "level",
    },
    "BEA:PCE_PRICE_INDEX": {
        "metric_id": "headline_pce_price_index",
        "name": "PCE Price Index",
        "unit": "index_points",
        "transformation": "level",
    },
    "CES0000000001": {
        "metric_id": "total_nonfarm_payroll_level",
        "name": "Total Nonfarm Payrolls",
        "unit": "thousands_jobs",
        "transformation": "level",
    },
}


def build_senior_analyst_payload_v1(
    source_payload: dict[str, Any],
    *,
    now: datetime | None = None,
    request_id: str | None = None,
    request_refresh_mode: str | None = None,
) -> dict[str, Any]:
    """Build a fail-closed analytical projection from one immutable source payload."""

    source_generated = parse_datetime(
        source_payload.get("generated_at_utc")
        or source_payload.get("generated_at")
        or source_payload.get("data_as_of")
    )
    clock = _utc(now or source_generated or datetime.now(UTC))
    generated_at = clock.isoformat()
    sections = _source_sections(source_payload)
    missing: list[dict[str, Any]] = []

    analytics = {
        "macro": _project_macro(sections.get("macro") or {}, clock, missing),
        "calendar": _project_calendar(
            sections.get("event_calendar") or {},
            clock,
            missing,
            macro_actuals=sections.get("macro_actuals") or {},
        ),
        "fomc": _project_fomc(sections.get("fed") or {}, clock, missing),
        "nasdaq": _project_nasdaq(sections.get("nasdaq") or {}, clock, missing),
        "market_internals": _project_market_internals(
            sections.get("market_internals") or {},
            clock,
            missing,
        ),
        "news": _project_news(sections.get("news") or {}, clock, missing),
        "vix": _project_vix(sections.get("vix") or {}, clock, missing),
        "risk": _project_risk(sections.get("risk") or {}, clock, missing),
        "rates": _project_rates(
            sections.get("rates") or {},
            sections.get("macro") or {},
            clock,
            missing,
        ),
        "positioning": _project_generic_section(
            "positioning",
            sections.get("positioning") or {},
            clock,
            missing,
        ),
        "earnings": _project_earnings(
            sections.get("earnings") or {},
            clock,
            missing,
        ),
        "options_positioning": _project_generic_section(
            "options_positioning",
            sections.get("options_positioning") or {},
            clock,
            missing,
        ),
        "market_schedule": _project_schedule(
            sections.get("market_schedule") or {},
            clock,
            missing,
        ),
    }
    missing = _ensure_required_dataset_missing_data(
        analytics,
        missing_data=missing,
        source_payload=source_payload,
    )
    missing = _deduplicate_missing(missing)
    readiness = _readiness(analytics)
    accounting = _provider_accounting(
        analytics,
        missing_data=missing,
        source_payload=source_payload,
        request_id=request_id,
        refresh_mode=request_refresh_mode,
    )
    provider_accounting = accounting["rows"]
    output = {
        "contract": CONTRACT_NAME,
        "schema_version": SCHEMA_VERSION,
        "symbol": source_payload.get("symbol") or "MNQ",
        "snapshot_id": source_payload.get("snapshot_id"),
        "snapshot_revision": source_payload.get("snapshot_revision"),
        "generated_at": generated_at,
        "source_snapshot_generated_at": (
            source_generated.isoformat() if source_generated else None
        ),
        "data_as_of": _latest_datetime_string(analytics) or generated_at,
        "request": {
            "request_id": request_id,
            "refresh_mode": request_refresh_mode,
            "accounting_correlation_id": accounting.get("correlation_id"),
            "accounting_request_started_at": accounting.get(
                "request_started_at"
            ),
            "accounting_request_completed_at": accounting.get(
                "request_completed_at"
            ),
            "accounting_evidence_origin": accounting.get("evidence_origin"),
            "same_request_provider_accounting": accounting[
                "same_request_complete"
            ],
        },
        "readiness": readiness,
        "analytics": analytics,
        "missing_data": missing,
        "provider_accounting": provider_accounting,
        "quality_gate": {
            "status": "PASS_OFFLINE_STRUCTURE_ONLY",
            "live_acceptance": "PENDING",
            "calculated_from_delivered_payload": True,
            "invalid_analytic_state_count": _invalid_state_count(analytics),
            "unexplained_omissions": 0,
            "required_dataset_omissions_without_reason": 0,
            "readiness_section_classification_mismatches": 0,
            "projection_is_not_live_acceptance": True,
        },
    }
    output["checksum_scope"] = "PAYLOAD_WITHOUT_CHECKSUM_AND_SIZE"
    canonical = _canonical_json(output)
    output["payload_size_bytes"] = len(canonical)
    output["checksum_sha256"] = hashlib.sha256(canonical).hexdigest()
    output["payload_size_bytes"] = len(_canonical_json(output))
    return output


def validate_senior_analyst_payload_v1(
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
    require_recent_response: bool = False,
    exact_body_size_bytes: int | None = None,
) -> dict[str, Any]:
    clock = _utc(now or datetime.now(UTC))
    generated = parse_datetime(payload.get("generated_at"))
    analytics = payload.get("analytics") if isinstance(payload.get("analytics"), dict) else {}
    calendar = analytics.get("calendar") if isinstance(analytics.get("calendar"), dict) else {}
    response_recent = bool(
        generated
        and (
            not require_recent_response
            or abs((clock - _utc(generated)).total_seconds()) <= 15 * 60
        )
    )
    calendar_lists = (
        calendar.get("active_event_windows") or [],
        calendar.get("next_24h_events") or [],
        calendar.get("next_7d_high_impact_events") or [],
    )
    exact_duplicates = sum(_duplicate_count(items) for items in calendar_lists)
    past_awaiting = sum(
        1
        for items in calendar_lists
        for item in items
        if _event_release(item)
        and _event_release(item) <= clock
        and str(item.get("release_status") or item.get("status")).upper()
        == "AWAITING_ACTUAL"
    )
    post_release_probabilities = _post_release_probability_count(
        analytics.get("fomc") or {},
        clock,
    )
    contradictory_drivers = sum(
        1
        for item in (analytics.get("nasdaq") or {}).get("drivers") or []
        if item.get("canonical_quote_match") is not True
    )
    expired_news = sum(
        1
        for item in (analytics.get("news") or {}).get("current_news") or []
        if str(item.get("freshness") or "").upper() in INVALID_ANALYTIC_STATES
    )
    semantic_errors = (
        _semantic_error_count(analytics.get("macro") or {})
        + _calendar_semantic_error_count(calendar, now=clock)
        + _earnings_policy_error_count(
            analytics.get("earnings") or {},
            now=clock,
        )
    )
    invalid_mappings = sum(
        1
        for items in calendar_lists
        for item in items
        if item.get("invalid_period_mapping") is True
    ) + _earnings_temporal_error_count(
        analytics.get("earnings") or {}
    )
    invalid_states = _invalid_state_count(analytics)
    available_without_value = _available_without_substantive_value_count(
        analytics
    )
    selected_value_presence_mismatches = (
        _selected_value_presence_mismatch_count(
            payload.get("provider_accounting") or [],
            payload_root=payload,
        )
    )
    readiness_section_classification_mismatches = (
        _readiness_section_classification_mismatch_count(
            payload.get("readiness"),
            analytics,
        )
    )
    expired_values = _expired_or_future_delivered_value_count(
        analytics,
        now=clock,
    )
    required_omissions_without_reason = (
        _required_dataset_omissions_without_reason(
            analytics,
            payload.get("missing_data") or [],
        )
    )
    unexplained = len(required_omissions_without_reason) + sum(
        1
        for item in payload.get("missing_data") or []
        if not item.get("reason_code")
        and not any(
            _missing_matches_dataset(
                dataset_id,
                str(item.get("field") or item.get("path") or ""),
            )
            for dataset_id in required_omissions_without_reason
        )
    )
    request = (
        payload.get("request")
        if isinstance(payload.get("request"), dict)
        else {}
    )
    accounting_valid = _provider_accounting_valid(
        payload.get("provider_accounting") or [],
        request=request,
        require_request_id=require_recent_response,
        analytics=analytics,
        missing_data=payload.get("missing_data") or [],
    )
    if require_recent_response:
        accounting_valid = bool(
            accounting_valid
            and _nested_value(
                payload,
                "request",
                "same_request_provider_accounting",
            )
            is True
            and _nested_value(payload, "request", "refresh_mode") == "force"
        )
    measured_payload_size = (
        exact_body_size_bytes
        if type(exact_body_size_bytes) is int
        and exact_body_size_bytes >= 0
        else len(_canonical_json(payload))
    )
    duplicate_large_collections = (
        _duplicate_large_collection_count(
            payload.get("provider_accounting") or []
        )
    )
    checks = {
        "response_generated_recently": response_recent,
        "expired_values_delivered": invalid_states + expired_values,
        "available_without_substantive_value": available_without_value,
        "selected_value_presence_mismatches": (
            selected_value_presence_mismatches
        ),
        "readiness_section_classification_mismatches": (
            readiness_section_classification_mismatches
        ),
        "stale_values_presented_as_current": _stale_presented_current(analytics),
        "invalid_temporal_mappings": invalid_mappings,
        "semantic_mapping_errors": semantic_errors,
        "calendar_exact_duplicates": exact_duplicates,
        "past_due_awaiting_actual": past_awaiting,
        "post_release_pre_fomc_probabilities": post_release_probabilities,
        "contradictory_nasdaq_drivers": contradictory_drivers,
        "expired_current_news": expired_news,
        "unexplained_omissions": unexplained,
        "required_dataset_omissions_without_reason": len(
            required_omissions_without_reason
        ),
        "provider_accounting_valid": accounting_valid,
        "provider_accounting_rows": len(
            payload.get("provider_accounting") or []
        ),
        "duplicate_large_collections": duplicate_large_collections,
        "payload_size_within_budget": (
            measured_payload_size
            <= MAX_SENIOR_ANALYST_PAYLOAD_BYTES
        ),
    }
    content_passed = (
        checks["response_generated_recently"]
        and all(
            checks[key] == 0
            for key in (
                "expired_values_delivered",
                "available_without_substantive_value",
                "selected_value_presence_mismatches",
                "readiness_section_classification_mismatches",
                "stale_values_presented_as_current",
                "invalid_temporal_mappings",
                "semantic_mapping_errors",
                "calendar_exact_duplicates",
                "past_due_awaiting_actual",
                "post_release_pre_fomc_probabilities",
                "contradictory_nasdaq_drivers",
                "expired_current_news",
                "unexplained_omissions",
                "required_dataset_omissions_without_reason",
                "duplicate_large_collections",
            )
        )
        and checks["payload_size_within_budget"] is True
    )
    passed = content_passed and (
        accounting_valid if require_recent_response else True
    )
    return {
        "status": (
            "PASS"
            if passed and require_recent_response
            else "PASS_OFFLINE"
            if passed
            else "FAIL"
        ),
        "live_acceptance_evaluated": require_recent_response,
        "checks": checks,
        "measured_payload_size_bytes": measured_payload_size,
        "max_payload_size_bytes": MAX_SENIOR_ANALYST_PAYLOAD_BYTES,
    }


def _source_sections(source: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if isinstance(source.get("sections"), dict):
        return {
            str(name): _section_payload(value)
            for name, value in source["sections"].items()
            if isinstance(value, dict)
        }
    return extract_sync_sections(source)


def _section_payload(value: dict[str, Any]) -> dict[str, Any]:
    payload = value.get("payload")
    return dict(payload) if isinstance(payload, dict) else dict(value)


def _project_macro(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    snapshot = section.get("snapshot") if isinstance(section.get("snapshot"), dict) else {}
    candidates: list[tuple[str, dict[str, Any]]] = []
    for group, values in snapshot.items():
        if not isinstance(values, dict) or group in {"lifecycle", "warnings"}:
            continue
        for key, item in values.items():
            if isinstance(item, dict) and (
                item.get("series_id") or item.get("value") is not None
            ):
                candidates.append((str(key), item))

    metrics: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key, item in candidates:
        series_id = str(item.get("series_id") or key)
        semantics = MACRO_SEMANTICS.get(series_id) or MACRO_SEMANTICS.get(key) or {
            "metric_id": str(item.get("metric") or series_id).lower(),
            "name": str(item.get("name") or series_id),
            "unit": str(item.get("unit") or "unknown"),
            "transformation": "level",
        }
        canonical_id = series_id
        if key == "CUSR0000SA0" and series_id != "CUSR0000SA0":
            canonical_id = series_id
        if canonical_id in seen:
            continue
        seen.add(canonical_id)
        assessment = _assess_datum(
            item,
            now,
            dataset_id=_registered_repository_dataset_id(
                canonical_id
            ),
        )
        metric = {
            "series_id": canonical_id,
            "metric_id": semantics["metric_id"],
            "name": semantics["name"],
            "value": item.get("value") if assessment["usable"] else None,
            "unit": semantics["unit"],
            "transformation": semantics["transformation"],
            "reference_period": item.get("reference_period")
            or item.get("latest_released_period")
            or item.get("data_as_of"),
            **_metadata(item, assessment),
        }
        metrics.append(metric)
        if not assessment["usable"]:
            _missing(
                missing,
                f"macro.metrics.{semantics['metric_id']}.value",
                assessment["status"],
                assessment["reason_code"],
                assessment["refresh_due_at"],
                ["macro_analysis"],
            )

    required_placeholders = {
        "CUSR0000SA0": "headline_cpi_index",
        "BEA:PCE_PRICE_INDEX": "headline_pce_price_index",
    }
    present_metric_ids = {str(item.get("metric_id")) for item in metrics}
    for series_id, metric_id in required_placeholders.items():
        if metric_id in present_metric_ids:
            continue
        semantics = MACRO_SEMANTICS[series_id]
        metrics.append(
            {
                "series_id": series_id,
                "metric_id": metric_id,
                "name": semantics["name"],
                "value": None,
                "unit": semantics["unit"],
                "transformation": semantics["transformation"],
                "reference_period": None,
                **_empty_metadata("UNAVAILABLE", "SOURCE_SERIES_NOT_AVAILABLE"),
            }
        )
        _missing(
            missing,
            f"macro.metrics.{metric_id}.value",
            "UNAVAILABLE",
            "SOURCE_SERIES_NOT_AVAILABLE",
            None,
            ["macro_analysis"],
        )

    if "total_nonfarm_payroll_level" in present_metric_ids:
        metrics.append(
            {
                "series_id": "CES0000000001",
                "metric_id": "nonfarm_payrolls_change",
                "name": "Monthly change in Total Nonfarm Payrolls",
                "value": None,
                "unit": "thousands_jobs",
                "transformation": "delta",
                "reference_period": None,
                **_empty_metadata(
                    "UNAVAILABLE",
                    "INSUFFICIENT_VALID_HISTORY",
                ),
            }
        )
        _missing(
            missing,
            "macro.metrics.nonfarm_payrolls_change.value",
            "UNAVAILABLE",
            "INSUFFICIENT_VALID_HISTORY",
            None,
            ["macro_analysis"],
        )
    metrics.sort(key=lambda item: (str(item["metric_id"]), str(item["series_id"])))
    valid_count = sum(item.get("value") is not None for item in metrics)
    section_status = _section_status(valid_count, len(metrics))
    return {
        **_section_metadata(
            section_status,
            _section_reason(section_status),
            _latest_value(metrics, "data_as_of"),
            _latest_value(metrics, "content_valid_until"),
            source="BLS/BEA/FRED",
        ),
        "metrics": metrics,
    }


def _project_calendar(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
    *,
    macro_actuals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    window = section.get("window") if isinstance(section.get("window"), dict) else {}
    current_week = (
        window.get("current_week")
        if isinstance(window.get("current_week"), dict)
        else {}
    )
    window_events = list(current_week.get("events") or [])
    next_24_candidates = list(section.get("next_24h_events") or [])
    next_7_candidates = list(
        section.get("upcoming_high_impact_events")
        or section.get("next_7d_critical_events")
        or []
    )
    released_candidates = [
        *window_events,
        *list(section.get("recently_released_events") or []),
        *list((macro_actuals or {}).get("items") or []),
    ]
    active = [
        event
        for event in [*window_events, *next_24_candidates]
        if isinstance(event, dict)
        and (
            event.get("is_window_active") is True
            or (
                _event_release(event)
                and _event_release(event) - timedelta(minutes=60)
                <= now
                <= _event_release(event) + timedelta(minutes=90)
            )
        )
    ]
    lists = {
        "active_event_windows": _canonical_events(active, now, mode="active"),
        "next_24h_events": _canonical_events(
            next_24_candidates,
            now,
            mode="next_24h",
        ),
        "next_7d_high_impact_events": _canonical_events(
            next_7_candidates,
            now,
            mode="next_7d",
        ),
        "latest_released_events": _canonical_events(
            released_candidates,
            now,
            mode="latest_release",
        ),
    }
    for name, values in lists.items():
        if not values:
            _missing(
                missing,
                f"calendar.{name}",
                "UNAVAILABLE",
                "NO_VALID_CANONICAL_EVENTS",
                None,
                ["event_risk_analysis"],
            )
        for event in values:
            field_reason_codes = event.pop("_field_reason_codes", {})
            identity = str(
                event.get("occurrence_id")
                or f"{event.get('metric_id')}@{event.get('release_at')}"
            )
            required_fields = [
                ("actual", "ACTUAL_NOT_AVAILABLE"),
                ("consensus", "CONSENSUS_NOT_AVAILABLE"),
                ("previous", "PREVIOUS_NOT_AVAILABLE"),
            ]
            if "previous_revised" in field_reason_codes:
                required_fields.append(
                    (
                        "previous_revised",
                        "PREVIOUS_REVISION_NOT_AVAILABLE",
                    )
                )
            for field, reason_code in required_fields:
                if event.get(field) is None:
                    _missing(
                        missing,
                        f"calendar.{name}.{identity}.{field}",
                        "UNAVAILABLE",
                        field_reason_codes.get(field, reason_code),
                        event.get("release_at"),
                        ["event_risk_analysis"],
                    )
    count = sum(len(values) for values in lists.values())
    status = "AVAILABLE" if count else "UNAVAILABLE"
    return {
        **_section_metadata(
            status,
            None if count else "NO_VALID_CANONICAL_EVENTS",
            now.isoformat(),
            (now + timedelta(hours=1)).isoformat(),
            source="CANONICAL_EVENT_REPOSITORY",
        ),
        **lists,
    }


def _canonical_events(
    values: Iterable[Any],
    now: datetime,
    *,
    mode: str,
) -> list[dict[str, Any]]:
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for raw in values:
        if not isinstance(raw, dict) or raw.get("invalid_period_mapping") is True:
            continue
        release = _event_release(raw)
        if release is None:
            continue
        if mode == "next_24h" and not (now < release <= now + timedelta(hours=24)):
            continue
        if mode == "next_7d" and not (now < release <= now + timedelta(days=7)):
            continue
        if mode == "active" and not (
            release - timedelta(minutes=60) <= now <= release + timedelta(minutes=90)
        ):
            continue
        if mode == "latest_release" and not (
            now - timedelta(days=45) <= release <= now
            and raw.get("actual") not in (None, "")
        ):
            continue
        release_status = str(
            raw.get("release_status")
            or raw.get("temporal_status")
            or raw.get("status")
            or "SCHEDULED"
        ).upper()
        if release <= now and release_status == "AWAITING_ACTUAL":
            continue
        metric_id = _event_metric_id(raw)
        reference_period = str(raw.get("reference_period") or raw.get("period") or "")
        if _is_fomc(raw):
            metric_id = "fomc_decision"
            reference_period = release.date().isoformat()
        key = (metric_id, release.isoformat(), reference_period)
        event = _project_event(
            raw,
            release,
            metric_id,
            release_status,
            now=now,
        )
        current = selected.get(key)
        if (
            current is None
            or _event_selection_rank(event, mode=mode)
            > _event_selection_rank(current, mode=mode)
        ):
            selected[key] = event
    return sorted(
        selected.values(),
        key=lambda item: (
            str(item.get("release_at") or ""),
            str(item.get("metric_id") or ""),
        ),
    )


def _project_event(
    raw: dict[str, Any],
    release: datetime,
    metric_id: str,
    release_status: str,
    *,
    now: datetime,
) -> dict[str, Any]:
    raw_metric_id = str(
        raw.get("metric_id")
        or raw.get("normalized_event_family")
        or metric_id
    ).strip().lower()
    semantic_reason = metric_semantics_mismatch_reason(
        raw_metric_id,
        name=raw.get("name") or raw.get("event_name"),
        frequency_hint=" ".join(
            str(item or "")
            for item in (
                raw.get("frequency"),
                raw.get("evaluation_method"),
            )
        ),
    )
    requires_official_evidence = _requires_official_event_evidence(
        raw,
        metric_id,
    )
    if (
        requires_official_evidence
        and metric_id not in OFFICIAL_METRICS
        and semantic_reason is None
    ):
        semantic_reason = "EVENT_METRIC_ID_NOT_PROVEN"
    expected_occurrence_ids = _event_expected_occurrence_ids(raw)
    selected_fields = {
        "actual": (
            "actual"
            if raw.get("actual") not in (None, "")
            else None
        ),
        "consensus": (
            "consensus"
            if raw.get("consensus") not in (None, "")
            else "forecast"
            if raw.get("forecast") not in (None, "")
            else None
        ),
        "previous": (
            "previous"
            if raw.get("previous") not in (None, "")
            else None
        ),
        "previous_revised": (
            "previous_revised"
            if raw.get("previous_revised") not in (None, "")
            else "revised_previous"
            if raw.get("revised_previous") not in (None, "")
            else None
        ),
    }
    lineage, field_reason_codes = _usable_event_field_lineage(
        raw,
        selected_fields=selected_fields,
        now=now,
        require_field_specific=requires_official_evidence,
        expected_occurrence_ids=expected_occurrence_ids,
        metric_id=metric_id,
        reference_period=(
            raw.get("reference_period")
            or raw.get("period")
        ),
        release=release,
    )
    if semantic_reason:
        lineage = []
        field_reason_codes.update(
            {
                output_field: semantic_reason
                for output_field, selected_field in selected_fields.items()
                if selected_field is not None
            }
        )
    if (
        requires_official_evidence
        and selected_fields["actual"] is not None
        and not _event_actual_source_status_proven(
            raw,
            metric_id=metric_id,
            lineage=lineage,
        )
    ):
        field_reason_codes.setdefault(
            "actual",
            "ACTUAL_OFFICIAL_STATUS_NOT_PROVEN",
        )
    record_reason = _event_lineage_rejection_reason(raw, now=now)
    if record_reason:
        lineage = []
        field_reason_codes.update(
            {
                output_field: record_reason
                for output_field, selected_field in selected_fields.items()
                if selected_field is not None
            }
        )
    rejected_lineage_fields = {
        alias
        for output_field in field_reason_codes
        for alias in {
            "actual": {"actual"},
            "consensus": {"consensus", "forecast"},
            "previous": {"previous"},
            "previous_revised": {
                "previous_revised",
                "revised_previous",
            },
        }.get(output_field, set())
    }
    if rejected_lineage_fields:
        lineage = [
            item
            for item in lineage
            if str(item.get("field") or "").strip().lower()
            not in rejected_lineage_fields
        ]
    actual = (
        None
        if "actual" in field_reason_codes
        else _number(raw.get("actual"))
    )
    consensus = (
        None
        if "consensus" in field_reason_codes
        else _number(
            raw.get("consensus")
            if selected_fields["consensus"] == "consensus"
            else raw.get("forecast")
        )
    )
    previous = (
        None
        if "previous" in field_reason_codes
        else _number(raw.get("previous"))
    )
    previous_revised = (
        None
        if "previous_revised" in field_reason_codes
        else _number(
            raw.get("previous_revised")
            if selected_fields["previous_revised"] == "previous_revised"
            else raw.get("revised_previous")
        )
    )
    occurrence_match = _occurrence_fields_reconciled(
        lineage,
        expected_occurrence_ids=expected_occurrence_ids,
    )
    reference_period_match = _event_reference_periods_reconciled(
        lineage,
        event_reference_period=(
            raw.get("reference_period")
            or raw.get("period")
        ),
        frequency=str(raw.get("frequency") or "monthly"),
        release=release,
    )
    surprise = (
        actual - consensus
        if (
            actual is not None
            and consensus is not None
            and occurrence_match
            and reference_period_match
        )
        else None
    )
    reason = field_reason_codes.get("actual")
    if reason is None and requires_official_evidence:
        reason = next(
            (
                field_reason_codes[field]
                for field in (
                    "consensus",
                    "previous",
                    "previous_revised",
                )
                if field in field_reason_codes
            ),
            None,
        )
    if "OCCURRENCE_FIELD_LINEAGE_MISMATCH" in set(
        field_reason_codes.values()
    ):
        reason = "OCCURRENCE_FIELD_LINEAGE_MISMATCH"
        lineage = []
        actual = consensus = previous = previous_revised = surprise = None
    elif not occurrence_match:
        reason = "OCCURRENCE_FIELD_LINEAGE_MISMATCH"
        actual = consensus = previous = previous_revised = surprise = None
    elif not reference_period_match:
        reference_reason = "REFERENCE_PERIOD_FIELD_LINEAGE_MISMATCH"
        reason = reference_reason
        field_reason_codes.update(
            {
                "actual": reference_reason,
                "consensus": reference_reason,
            }
        )
        lineage = [
            item
            for item in lineage
            if str(item.get("field") or "").strip().lower()
            not in {"actual", "consensus", "forecast"}
        ]
        actual = consensus = surprise = None
    return {
        "occurrence_id": raw.get("occurrence_id") or raw.get("event_id"),
        "provider_event_id": raw.get("provider_event_id"),
        "provider_occurrence_id": raw.get("provider_occurrence_id"),
        "metric_id": metric_id,
        "name": raw.get("name") or raw.get("event_name"),
        "release_at": release.isoformat(),
        "reference_period": raw.get("reference_period") or raw.get("period"),
        "release_status": release_status,
        "impact": raw.get("impact") or raw.get("event_risk_level"),
        "actual": actual,
        "consensus": consensus,
        "previous": previous,
        "previous_revised": previous_revised,
        "surprise_absolute": surprise,
        "surprise_percent": (
            round((surprise / abs(consensus)) * 100, 6)
            if surprise is not None and consensus not in {None, 0}
            else None
        ),
        "surprise_direction": (
            "ABOVE"
            if surprise is not None and surprise > 0
            else "BELOW"
            if surprise is not None and surprise < 0
            else "INLINE"
            if surprise == 0
            else None
        ),
        "actual_is_official": (
            raw.get("actual_is_official")
            if actual is not None
            else None
        ),
        "actual_source": (
            raw.get("actual_source")
            or raw.get("publisher")
            if actual is not None
            else None
        ),
        "freshness": (
            None
            if semantic_reason
            else (
                raw.get("freshness_state")
                or raw.get("freshness")
            )
        ),
        "content_valid_until": (
            raw.get("content_valid_until")
            or raw.get("valid_until")
        ),
        "refresh_due_at": (
            raw.get("refresh_due_at")
            or raw.get("next_refresh_at")
        ),
        "invalid_period_mapping": False,
        "source": _source(raw),
        "reason_code": reason,
        "lineage": lineage,
        "_field_reason_codes": field_reason_codes,
    }


def _project_fomc(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    context = (
        section.get("fomc_context")
        if isinstance(section.get("fomc_context"), dict)
        else {}
    )
    release = parse_datetime(
        context.get("decision_time_utc")
        or context.get("release_at")
        or context.get("meeting_date")
    )
    release = _utc(release) if release else None
    after_release = bool(release and now >= release)
    official_outcome = bool(
        context.get("official_outcome_available")
        or (
            context.get("current_target_range_lower") is not None
            and context.get("current_target_range_upper") is not None
            and context.get("expected_action") not in {None, "", "unknown"}
            and context.get("is_official_source") is True
        )
    )
    if after_release and not official_outcome:
        status = "UNAVAILABLE_AFTER_RELEASE"
        reason = "OFFICIAL_OUTCOME_NOT_AVAILABLE"
        probabilities: list[dict[str, Any]] = []
        action = lower = upper = change = None
        _missing(
            missing,
            "fomc.action",
            "UNAVAILABLE",
            reason,
            None,
            ["macro_analysis"],
        )
    else:
        status = "AVAILABLE" if context else "UNAVAILABLE"
        reason = None if context else "FOMC_CONTEXT_NOT_AVAILABLE"
        action = (
            context.get("expected_action")
            if str(context.get("expected_action") or "").lower()
            not in {"", "unknown", "unavailable"}
            else None
        )
        lower = context.get("current_target_range_lower")
        upper = context.get("current_target_range_upper")
        change = context.get("expected_change_bps")
        probabilities = _fomc_probabilities(context) if not after_release else []
        missing_fields = [
            field
            for field, value in (
                ("action", action),
                ("target_range_lower", lower),
                ("target_range_upper", upper),
                ("pre_meeting_probabilities", probabilities),
            )
            if value is None or value == () or value == []
        ]
        if context and missing_fields:
            status = "PARTIAL"
            reason = "PRE_RELEASE_FOMC_FIELDS_UNAVAILABLE"
            for field in missing_fields:
                _missing(
                    missing,
                    f"fomc.{field}",
                    "UNAVAILABLE",
                    "PRE_RELEASE_FOMC_FIELD_NOT_AVAILABLE",
                    release.isoformat() if release else None,
                    ["macro_analysis"],
                )
    return {
        **_section_metadata(
            status,
            reason,
            context.get("data_as_of"),
            context.get("valid_until"),
            source=_source(context),
        ),
        "release_at": release.isoformat() if release else None,
        "action": action,
        "change_bps": change,
        "target_range_lower": lower,
        "target_range_upper": upper,
        "pre_meeting_probabilities": probabilities,
        "probability_state": (
            "HISTORICAL"
            if after_release
            else "CURRENT"
            if probabilities
            else "UNAVAILABLE"
        ),
    }


def _project_nasdaq(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    holdings_block = (
        section.get("qqq_holdings")
        if isinstance(section.get("qqq_holdings"), dict)
        else {}
    )
    holdings = {
        str(item.get("symbol") or "").upper(): item
        for item in holdings_block.get("holdings") or []
        if isinstance(item, dict) and item.get("symbol")
    }
    components: list[dict[str, Any]] = []
    for symbol in REQUIRED_MEGA_CAPS:
        item = holdings.get(symbol)
        if item is None:
            _missing(
                missing,
                f"nasdaq.components.{symbol}",
                "UNAVAILABLE",
                "CANONICAL_QUOTE_NOT_AVAILABLE",
                None,
                ["trading_context"],
            )
            continue
        assessment = _assess_datum(
            item,
            now,
            dataset_id="mega_cap_quotes",
        )
        if not assessment["usable"]:
            _missing(
                missing,
                f"nasdaq.components.{symbol}",
                assessment["status"],
                assessment["reason_code"],
                assessment["refresh_due_at"],
                ["trading_context"],
            )
            continue
        components.append(
            {
                "symbol": symbol,
                "price": _number(item.get("price")),
                "change_pct": _number(item.get("change_pct")),
                "weight_pct": _number(item.get("weight_pct", item.get("weight"))),
                "quote_occurrence": item.get("as_of")
                or item.get("retrieved_at")
                or item.get("data_as_of"),
                **_metadata(item, assessment),
            }
        )

    drivers: list[dict[str, Any]] = []
    for raw in section.get("driver_context") or []:
        if not isinstance(raw, dict):
            continue
        symbol = str(raw.get("symbol") or "").upper()
        quote = holdings.get(symbol)
        coherent = bool(
            quote
            and _close(raw.get("change_pct"), quote.get("change_pct"))
            and _close(raw.get("qqq_weight"), quote.get("weight_pct", quote.get("weight")))
        )
        if not coherent:
            _missing(
                missing,
                f"nasdaq.drivers.{symbol or 'UNKNOWN'}",
                "UNAVAILABLE",
                "CONTRADICTORY_CANONICAL_QUOTE",
                None,
                ["trading_context"],
            )
            continue
        drivers.append(
            {
                "symbol": symbol,
                "change_pct": _number(raw.get("change_pct")),
                "weight_pct": _number(raw.get("qqq_weight")),
                "weighted_contribution": _number(raw.get("weighted_contribution")),
                "canonical_quote_match": True,
                "quote_occurrence": quote.get("as_of") or quote.get("retrieved_at"),
                "source": _source(quote),
                "reason_code": None,
            }
        )
    count = len(components) + len(drivers)
    status = _section_status(count, len(REQUIRED_MEGA_CAPS) * 2)
    return {
        **_section_metadata(
            status,
            _section_reason(status),
            _latest_value(components, "data_as_of"),
            _latest_value(components, "content_valid_until"),
            source=_source(holdings_block),
        ),
        "components": components,
        "drivers": drivers,
        "required_symbols": list(REQUIRED_MEGA_CAPS),
    }


def _project_market_internals(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    assessment = _assess_datum(
        section,
        now,
        dataset_id="market_internals",
    )
    stale_quotes = int(section.get("stale_quote_count") or 0)
    if stale_quotes:
        assessment = {
            **assessment,
            "usable": False,
            "status": "UNAVAILABLE",
            "freshness": "UNAVAILABLE",
            "reason_code": "STALE_CONSTITUENT_QUOTES",
        }
    fields = (
        "advance_decline_ratio",
        "advancers",
        "decliners",
        "percent_advancers",
        "weighted_breadth",
    )
    if assessment["usable"] and not any(
        _finite_scalar_number_present(section.get(field))
        for field in fields
    ):
        assessment = {
            **assessment,
            "usable": False,
            "status": "UNAVAILABLE",
            "freshness": "UNAVAILABLE",
            "reason_code": "MARKET_INTERNALS_VALUE_NOT_AVAILABLE",
        }
    values = {
        field: section.get(field) if assessment["usable"] else None
        for field in fields
    }
    if not assessment["usable"]:
        _missing(
            missing,
            "market_internals",
            assessment["status"],
            assessment["reason_code"],
            assessment["refresh_due_at"],
            ["trading_context"],
        )
    return {
        **_metadata(section, assessment),
        **values,
    }


def _project_news(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    context = section.get("context") if isinstance(section.get("context"), dict) else {}
    company = (
        section.get("current_company_news")
        if isinstance(section.get("current_company_news"), dict)
        else {}
    )
    candidates = [
        *list(section.get("latest") or []),
        *list(context.get("latest") or []),
        *list(context.get("articles") or []),
        *list(company.get("items") or []),
    ]
    selected: dict[str, dict[str, Any]] = {}
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        headline = raw.get("headline") or raw.get("title")
        summary = (
            raw.get("summary")
            or raw.get("content_snippet")
            or raw.get("description")
            or raw.get("content")
        )
        headline = (
            headline
            if substantive_news_text(headline)
            else None
        )
        summary = (
            summary
            if substantive_news_text(summary)
            else None
        )
        if headline is None and summary is None:
            continue
        normalized_news = {
            **raw,
            "title": headline,
            "summary": summary,
        }
        if news_content_status(normalized_news) == "invalid_content":
            continue
        audit_status = str(
            raw.get("source_audit_status") or ""
        ).upper()
        disposition = str(raw.get("disposition") or "").upper()
        if (
            (audit_status and audit_status != "ACTIVE")
            or disposition
            in {
                "QUARANTINED",
                "WITHHELD",
                "TECHNICALLY_INVALID",
                "TECHNICALLY_REJECTED",
            }
            or raw.get("accepted") is False
        ):
            continue
        published = parse_datetime(
            raw.get("published_at")
            or raw.get("published_at_utc")
            or raw.get("datetime")
        )
        freshness = str(raw.get("freshness") or "").upper()
        status = str(raw.get("status") or "").upper()
        if (
            freshness in INVALID_ANALYTIC_STATES
            or status in INVALID_ANALYTIC_STATES
            or not published
            or not timedelta(0) <= now - _utc(published) <= timedelta(hours=24)
        ):
            continue
        identity = str(
            raw.get("canonical_url")
            or raw.get("url")
            or raw.get("article_id")
            or _hash_identity(raw)
        )
        selected[identity] = {
            "article_id": raw.get("article_id"),
            "headline": headline,
            "summary": summary,
            "published_at": _utc(published).isoformat(),
            "symbols": sorted(set(raw.get("symbols") or [])),
            "source": _source(raw),
            "freshness": "CURRENT",
            "status": "AVAILABLE",
            "reason_code": None,
        }
    current = sorted(
        selected.values(),
        key=lambda item: (str(item["published_at"]), str(item["headline"])),
        reverse=True,
    )
    status = "AVAILABLE" if current else "NO_DATA"
    reason = None if current else "NO_CURRENT_NEWS"
    if not current:
        _missing(
            missing,
            "news.current_news",
            "UNAVAILABLE",
            reason,
            None,
            ["news_analysis"],
        )
    return {
        **_section_metadata(
            status,
            reason,
            _latest_value(current, "published_at"),
            (now + timedelta(hours=1)).isoformat(),
            source="SOURCE_GATEWAY",
        ),
        "current_news": current,
    }


def _project_vix(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key in ("vix", "vvix"):
        raw = section.get(key) if isinstance(section.get(key), dict) else {}
        assessment = _assess_datum(
            raw,
            now,
            dataset_id=key,
            section_sync=section.get("sync"),
        )
        if raw.get("value") is None:
            assessment = {
                **assessment,
                "usable": False,
                "status": "UNAVAILABLE",
                "freshness": "UNAVAILABLE",
                "reason_code": (
                    f"{key.upper()}_VALUE_NOT_AVAILABLE"
                    if assessment.get("reason_code")
                    in {None, "VALUE_NOT_AVAILABLE"}
                    else assessment.get("reason_code")
                ),
            }
        output[key.upper()] = {
            "symbol": key.upper(),
            "value": raw.get("value") if assessment["usable"] else None,
            **_metadata(raw, assessment),
        }
        if not assessment["usable"]:
            _missing(
                missing,
                f"vix.{key}.value",
                assessment["status"],
                assessment["reason_code"],
                assessment["refresh_due_at"],
                ["trading_context"],
            )
    count = sum(item.get("value") is not None for item in output.values())
    status = _section_status(count, 2)
    return {
        **_section_metadata(
            status,
            _section_reason(status),
            _latest_value(list(output.values()), "data_as_of"),
            _latest_value(list(output.values()), "content_valid_until"),
            source="CBOE/FRED",
        ),
        **output,
    }


def _project_risk(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    raw = section.get("risk_context") if isinstance(section.get("risk_context"), dict) else {}
    assessment = _assess_datum(
        raw,
        now,
        dataset_id="risk",
    )
    derived = raw.get("derived_context") if isinstance(raw.get("derived_context"), dict) else {}
    risk_sentiment = (
        derived.get("risk_regime")
        or derived.get("sentiment")
    )
    risk_score = derived.get("risk_score")
    if (
        assessment["usable"]
        and not _risk_delivery_present(
            {
                "risk_sentiment": risk_sentiment,
                "risk_score": risk_score,
            }
        )
    ):
        assessment = {
            **assessment,
            "usable": False,
            "status": "UNAVAILABLE",
            "freshness": "UNAVAILABLE",
            "reason_code": "RISK_VALUE_NOT_AVAILABLE",
        }
    if not assessment["usable"]:
        _missing(
            missing,
            "risk.risk_sentiment",
            assessment["status"],
            assessment["reason_code"],
            assessment["refresh_due_at"],
            ["trading_context"],
        )
    return {
        **_metadata(raw, assessment),
        "risk_sentiment": (
            risk_sentiment
            if assessment["usable"]
            else None
        ),
        "risk_score": risk_score if assessment["usable"] else None,
        "excluded_inputs_used": False,
    }


def _project_rates(
    section: dict[str, Any],
    macro_section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    snapshot = (
        macro_section.get("snapshot")
        if isinstance(macro_section.get("snapshot"), dict)
        else {}
    )
    rate_snapshot = (
        snapshot.get("rates_and_yields")
        if isinstance(snapshot.get("rates_and_yields"), dict)
        else {}
    )
    items: list[dict[str, Any]] = []
    for key, raw in rate_snapshot.items():
        if not isinstance(raw, dict) or not raw.get("series_id"):
            continue
        assessment = _assess_datum(
            raw,
            now,
            dataset_id=_registered_repository_dataset_id(
                str(raw.get("series_id") or key)
            ),
        )
        item = {
            "series_id": str(raw.get("series_id") or key),
            "metric_id": str(raw.get("series_id") or key).lower(),
            "value": raw.get("value") if assessment["usable"] else None,
            "unit": "percent",
            "transformation": "level",
            "reference_period": raw.get("data_as_of"),
            **_metadata(raw, assessment),
        }
        items.append(item)
        if not assessment["usable"]:
            _missing(
                missing,
                f"rates.metrics.{item['metric_id']}.value",
                assessment["status"],
                assessment["reason_code"],
                assessment["refresh_due_at"],
                ["macro_analysis"],
            )
    treasury_rates = [
        deepcopy(item)
        for item in items
        if str(item.get("series_id") or "").upper()
        in TREASURY_RATE_SERIES
        and _finite_scalar_number_present(item.get("value"))
    ]
    fed_funds = [
        deepcopy(item)
        for item in items
        if str(item.get("series_id") or "").upper()
        in FED_FUNDS_RATE_SERIES
        and _finite_scalar_number_present(item.get("value"))
    ]
    target_range = _project_target_range(items, missing)
    complete_dataset_count = sum(
        (
            bool(treasury_rates),
            bool(fed_funds),
            target_range["status"] == "AVAILABLE",
        )
    )
    status = _section_status(complete_dataset_count, 3)
    if not complete_dataset_count and target_range["status"] == "PARTIAL":
        status = "PARTIAL"
    reason = (
        None
        if status == "AVAILABLE"
        else "ONE_OR_MORE_RATE_DATASETS_UNAVAILABLE"
        if status == "PARTIAL"
        else "NO_SUBSTANTIVE_RATE_DATASET_AVAILABLE"
    )
    substantive_items = [
        *treasury_rates,
        *fed_funds,
        *(
            [target_range]
            if target_range["status"] == "AVAILABLE"
            else []
        ),
    ]
    return {
        **_section_metadata(
            status,
            reason,
            _latest_value(substantive_items, "data_as_of"),
            _earliest_datetime_value(
                substantive_items,
                "content_valid_until",
            ),
            source="FRED",
        ),
        "metrics": sorted(items, key=lambda item: item["series_id"]),
        "treasury_rates": treasury_rates,
        "fed_funds": fed_funds,
        "target_range": target_range,
    }


def _project_target_range(
    rate_metrics: list[dict[str, Any]],
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    by_series = {
        str(item.get("series_id") or "").upper(): item
        for item in rate_metrics
        if str(item.get("series_id") or "").upper()
        in TARGET_RANGE_SERIES
    }
    lower_item = by_series.get("DFEDTARL")
    upper_item = by_series.get("DFEDTARU")
    lower = (
        _number(lower_item.get("value"))
        if lower_item
        and _finite_scalar_number_present(lower_item.get("value"))
        else None
    )
    upper = (
        _number(upper_item.get("value"))
        if upper_item
        and _finite_scalar_number_present(upper_item.get("value"))
        else None
    )
    missing_bounds: list[tuple[str, str]] = []
    if lower is None:
        missing_bounds.append(
            (
                "target_range_lower",
                "TARGET_RANGE_LOWER_BOUND_NOT_AVAILABLE",
            )
        )
    if upper is None:
        missing_bounds.append(
            (
                "target_range_upper",
                "TARGET_RANGE_UPPER_BOUND_NOT_AVAILABLE",
            )
        )
    if lower is not None and upper is not None and lower > upper:
        lower = upper = None
        missing_bounds = [
            (
                "target_range",
                "TARGET_RANGE_BOUNDS_INVALID_ORDER",
            )
        ]
    for field, reason_code in missing_bounds:
        _missing(
            missing,
            f"rates.target_range.{field}",
            "PARTIAL" if len(missing_bounds) == 1 else "UNAVAILABLE",
            reason_code,
            _earliest_datetime_value(
                [item for item in (lower_item, upper_item) if item],
                "refresh_due_at",
            ),
            ["macro_analysis"],
        )

    bounds = [
        item
        for item in (lower_item, upper_item)
        if isinstance(item, dict)
    ]
    complete = lower is not None and upper is not None
    partial = (lower is None) != (upper is None)
    status = "AVAILABLE" if complete else "PARTIAL" if partial else "UNAVAILABLE"
    reason = (
        None
        if complete
        else missing_bounds[0][1]
        if len(missing_bounds) == 1
        else "TARGET_RANGE_BOUNDS_NOT_AVAILABLE"
        if missing_bounds
        else "TARGET_RANGE_NOT_AVAILABLE"
    )
    source = None
    sources = [
        item.get("source")
        for item in bounds
        if item.get("source") is not None
    ]
    if sources:
        common_source_fields = {
            key: sources[0].get(key)
            for key in (
                "publisher",
                "distributor",
                "acquisition_provider",
            )
            if isinstance(sources[0], dict)
            and all(
                isinstance(item, dict)
                and item.get(key) == sources[0].get(key)
                for item in sources
            )
        }
        source = (
            deepcopy(sources[0])
            if all(item == sources[0] for item in sources)
            else {
                **common_source_fields,
                "source_url": None,
                "bound_sources": {
                    "lower": deepcopy(
                        lower_item.get("source")
                        if lower_item
                        else None
                    ),
                    "upper": deepcopy(
                        upper_item.get("source")
                        if upper_item
                        else None
                    ),
                },
            }
        )
    lineage = [
        {
            "field": field,
            "series_id": series_id,
            "source": deepcopy(item.get("source")),
            "data_as_of": item.get("data_as_of"),
            "content_valid_until": item.get("content_valid_until"),
            "upstream_lineage": deepcopy(item.get("lineage") or []),
        }
        for field, series_id, item in (
            ("target_range_lower", "DFEDTARL", lower_item),
            ("target_range_upper", "DFEDTARU", upper_item),
        )
        if isinstance(item, dict)
    ]
    freshness_values = {
        str(item.get("freshness") or "")
        for item in bounds
        if item.get("freshness")
    }
    return {
        "status": status,
        "freshness": (
            freshness_values.pop()
            if complete and len(freshness_values) == 1
            else "CURRENT"
            if complete
            else "UNAVAILABLE"
        ),
        "data_as_of": _latest_value(bounds, "data_as_of"),
        "content_valid_until": _earliest_datetime_value(
            bounds,
            "content_valid_until",
        ),
        "snapshot_transport_valid_until": _earliest_datetime_value(
            bounds,
            "snapshot_transport_valid_until",
        ),
        "refresh_due_at": _earliest_datetime_value(
            bounds,
            "refresh_due_at",
        ),
        "source": source,
        "reason_code": reason,
        "lineage": lineage,
        "target_range_lower": lower,
        "target_range_upper": upper,
    }


def _project_generic_section(
    name: str,
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    assessment = _assess_datum(
        section,
        now,
        dataset_id=name,
    )
    if name == "positioning":
        projected_values = {
            "cot": _project_positioning_cot(section),
        }
        substantive = _positioning_delivery_present(
            projected_values["cot"]
        )
        missing_reason = "POSITIONING_VALUE_NOT_AVAILABLE"
    else:
        projected_values = {
            "iv_atm": section.get("iv_atm"),
            "open_interest": deepcopy(
                section.get("open_interest") or {}
            ),
            "volume": deepcopy(section.get("volume") or {}),
            "skew": deepcopy(section.get("skew") or {}),
        }
        substantive = _options_positioning_delivery_present(
            projected_values
        )
        missing_reason = "OPTIONS_POSITIONING_VALUE_NOT_AVAILABLE"
    if (
        assessment["usable"]
        and not substantive
    ):
        assessment = {
            **assessment,
            "usable": False,
            "status": "UNAVAILABLE",
            "freshness": "UNAVAILABLE",
            "reason_code": missing_reason,
        }
    if not assessment["usable"]:
        _missing(
            missing,
            name,
            assessment["status"],
            assessment["reason_code"],
            assessment["refresh_due_at"],
            ["trading_context"],
        )
    values = (
        projected_values
        if assessment["usable"]
        else (
            {"cot": {}}
            if name == "positioning"
            else {
                "iv_atm": None,
                "open_interest": {},
                "volume": {},
                "skew": {},
            }
        )
    )
    return {
        **_metadata(section, assessment),
        **values,
    }


def _project_earnings(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    policy = MNQ_EARNINGS_SELECTION_POLICY
    nasdaq = (
        section.get("nasdaq_earnings")
        if isinstance(section.get("nasdaq_earnings"), dict)
        else {}
    )
    corporate = (
        section.get("corporate_events")
        if isinstance(section.get("corporate_events"), dict)
        else {}
    )
    corporate_earnings = corporate.get("earnings")
    if isinstance(corporate_earnings, dict):
        corporate_candidates = [
            *list(
                corporate_earnings.get("relevant_upcoming")
                or corporate_earnings.get("events")
                or []
            ),
            *list(corporate_earnings.get("mega_cap") or []),
        ]
    elif isinstance(corporate_earnings, list):
        corporate_candidates = list(corporate_earnings)
    else:
        corporate_candidates = []
    observed_selection_counts = _observed_earnings_selection_counts(
        corporate_earnings,
        nasdaq,
    )
    candidates = [
        *list(nasdaq.get("upcoming") or []),
        *corporate_candidates,
    ]
    window_start = now.date() - timedelta(days=policy.lookback_days)
    window_end = now.date() + timedelta(days=policy.lookahead_days)
    identified: dict[tuple[str, str], dict[str, Any]] = {}
    candidate_diagnostics = {
        "invalid_identity_or_date": 0,
        "duplicate_occurrence": 0,
        "expired_or_invalid_freshness": 0,
        "freshness_rejection_reasons": {},
    }
    exclusions = {
        "outside_window": 0,
        "outside_universe": 0,
        "bounded_limit": 0,
        "provider_prefiltered_or_invalid": 0,
    }
    for raw in candidates:
        if not isinstance(raw, dict):
            candidate_diagnostics["invalid_identity_or_date"] += 1
            continue
        symbol = str(raw.get("symbol") or raw.get("ticker") or "").upper()
        temporal = _earnings_temporal_fields(raw)
        event_date = temporal.get("event_date")
        if not symbol or not event_date:
            candidate_diagnostics["invalid_identity_or_date"] += 1
            continue
        parsed_date = _date_value(event_date)
        if parsed_date is None:
            candidate_diagnostics["invalid_identity_or_date"] += 1
            continue
        freshness = _earnings_freshness(raw, now=now)
        if freshness["deliverable"] is not True:
            candidate_diagnostics[
                "expired_or_invalid_freshness"
            ] += 1
            freshness_reason = str(
                freshness.get("reason_code")
                or "EARNINGS_FRESHNESS_EVIDENCE_INVALID"
            )
            rejection_reasons = candidate_diagnostics[
                "freshness_rejection_reasons"
            ]
            rejection_reasons[freshness_reason] = (
                int(rejection_reasons.get(freshness_reason) or 0)
                + 1
            )
            continue
        source = _source(raw)
        eps_estimate = _number(
            raw.get("eps_estimate")
            if raw.get("eps_estimate") is not None
            else raw.get("eps_consensus")
        )
        candidate = {
            "symbol": symbol,
            "event_date": event_date,
            "event_at": temporal.get("event_at"),
            "temporal_precision": temporal["temporal_precision"],
            "timing": temporal["timing"],
            "eps_estimate": eps_estimate,
            "revenue_estimate": _number(raw.get("revenue_estimate")),
            "freshness": freshness["freshness"],
            "data_as_of": freshness["data_as_of"],
            "content_valid_until": freshness["content_valid_until"],
            "refresh_due_at": freshness["refresh_due_at"],
            "source": source,
            "lineage": _earnings_field_lineage(raw),
            "reason_code": (
                "EARNINGS_RELEASE_TIME_NOT_AVAILABLE"
                if temporal["temporal_precision"]
                == policy.date_only_temporal_precision
                else None
            ),
        }
        key = (symbol, event_date)
        prior = identified.get(key)
        if prior is not None:
            candidate_diagnostics["duplicate_occurrence"] += 1
        if (
            prior is None
            or _earnings_candidate_rank(candidate)
            > _earnings_candidate_rank(prior)
        ):
            identified[key] = candidate

    in_window = [
        item
        for item in identified.values()
        if (
            (event_date := _date_value(item.get("event_date")))
            is not None
            and window_start <= event_date <= window_end
        )
    ]
    exclusions["outside_window"] = len(identified) - len(in_window)
    relevant = [
        item
        for item in in_window
        if item.get("symbol") in policy.primary_symbols
    ]
    exclusions["outside_universe"] = len(in_window) - len(relevant)
    relevant.sort(
        key=lambda item: (
            item["event_date"],
            item["symbol"],
        )
    )
    events = relevant[: policy.max_events]
    observed_bounded_count = (
        max(
            observed_selection_counts["relevant_count"]
            - observed_selection_counts["delivered_count"],
            0,
        )
        if observed_selection_counts is not None
        else 0
    )
    exclusions["bounded_limit"] = max(
        len(relevant) - len(events),
        observed_bounded_count,
        0,
    )
    total_available = max(
        len(identified),
        (
            observed_selection_counts["total_available"]
            if observed_selection_counts is not None
            else 0
        ),
        len(events)
        + exclusions["outside_window"]
        + exclusions["outside_universe"]
        + exclusions["bounded_limit"],
    )
    relevant_count = max(
        len(relevant),
        (
            observed_selection_counts["relevant_count"]
            if observed_selection_counts is not None
            else 0
        ),
    )
    exclusions["provider_prefiltered_or_invalid"] = max(
        total_available
        - len(events)
        - exclusions["outside_window"]
        - exclusions["outside_universe"]
        - exclusions["bounded_limit"],
        0,
    )
    coverage = _earnings_coverage(events)
    status, reason_code = _earnings_section_status(
        coverage,
        bounded_count=exclusions["bounded_limit"],
    )
    if not events:
        _missing(
            missing,
            "earnings.events",
            "UNAVAILABLE",
            "NO_CURRENT_EARNINGS_EVENTS",
            None,
            ["trading_context"],
        )
    metadata = _section_metadata(
        status,
        reason_code,
        _latest_value(events, "data_as_of"),
        _earliest_datetime_value(events, "content_valid_until"),
        source=_first_delivery_field(events, "source"),
    )
    if events and coverage["freshness"]["ratio"] < 1.0:
        metadata["freshness"] = "UNKNOWN"
    metadata["refresh_due_at"] = metadata["content_valid_until"]
    return {
        **metadata,
        "selection_policy": {
            "policy_id": policy.policy_id,
            "universe": "MNQ_PRIMARY_SYMBOLS",
            "primary_symbols": list(policy.primary_symbols),
            "include_nasdaq_100_components": (
                policy.include_nasdaq_100_components
            ),
            "window": {
                "lookback_days": policy.lookback_days,
                "lookahead_days": policy.lookahead_days,
                "max_observation_age_hours": (
                    policy.max_observation_age_hours
                ),
                "start_date": window_start.isoformat(),
                "end_date": window_end.isoformat(),
            },
            "sort_fields": list(policy.sort_fields),
            "max_events": policy.max_events,
            "minimum_fields": list(policy.minimum_fields),
            "date_only": {
                "event_at": None,
                "temporal_precision": (
                    policy.date_only_temporal_precision
                ),
                "timing": policy.date_only_timing,
            },
        },
        "total_available": total_available,
        "relevant_count": relevant_count,
        "delivered_count": len(events),
        "excluded_count": total_available - len(events),
        "exclusion_counts": exclusions,
        "candidate_diagnostics": candidate_diagnostics,
        "coverage": coverage,
        "events": events,
    }


def _observed_earnings_selection_counts(
    *payloads: Any,
) -> dict[str, int] | None:
    """Read compact provider/runtime counts without reconstructing raw rows."""

    keys = (
        "total_available",
        "relevant_count",
        "delivered_count",
        "excluded_count",
    )
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        nested = payload.get("selection_counts")
        candidates = (
            payload,
            nested if isinstance(nested, dict) else {},
        )
        for candidate in candidates:
            if not all(key in candidate for key in keys):
                continue
            counts = {key: candidate.get(key) for key in keys}
            if not all(
                type(value) is int and value >= 0
                for value in counts.values()
            ):
                continue
            if not (
                counts["total_available"]
                >= counts["relevant_count"]
                >= counts["delivered_count"]
                and counts["excluded_count"]
                == counts["total_available"]
                - counts["delivered_count"]
            ):
                continue
            return counts
    return None


def _earnings_temporal_fields(raw: dict[str, Any]) -> dict[str, Any]:
    policy = MNQ_EARNINGS_SELECTION_POLICY
    declared_precision = str(
        raw.get("temporal_precision") or ""
    ).strip().upper()
    exact = next(
        (
            parsed
            for key in (
                "event_at",
                "release_at",
                "scheduled_at_utc",
                "scheduled_at",
            )
            if (parsed := _time_bearing_datetime(raw.get(key))) is not None
        ),
        None,
    )
    if (
        exact is not None
        and (
            declared_precision == policy.exact_temporal_precision
            or (
                not declared_precision
                and any(
                    (
                        exact.hour,
                        exact.minute,
                        exact.second,
                        exact.microsecond,
                    )
                )
            )
        )
    ):
        return {
            "event_date": _utc(exact).date().isoformat(),
            "event_at": _utc(exact).isoformat(),
            "temporal_precision": policy.exact_temporal_precision,
            "timing": _earnings_timing(raw),
        }
    event_date = next(
        (
            parsed
            for key in (
                "event_date",
                "earnings_date",
                "scheduled_date",
                "date",
            )
            if (parsed := _date_value(raw.get(key))) is not None
        ),
        None,
    )
    if event_date is None and exact is not None:
        event_date = _utc(exact).date()
    return {
        "event_date": event_date.isoformat() if event_date else None,
        "event_at": None,
        "temporal_precision": policy.date_only_temporal_precision,
        "timing": policy.date_only_timing,
    }


def _time_bearing_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value) if value.tzinfo is not None else None
    text = str(value or "").strip()
    if not text or re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return None
    if "T" not in text and ":" not in text:
        return None
    if not (
        text.upper().endswith("Z")
        or re.search(r"[+-]\d{2}:?\d{2}$", text)
    ):
        return None
    parsed = parse_datetime(text)
    return (
        _utc(parsed)
        if parsed is not None and parsed.tzinfo is not None
        else None
    )


def _date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return _utc(value).date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _earnings_timing(raw: dict[str, Any]) -> str:
    value = str(
        raw.get("timing")
        or raw.get("release_session")
        or raw.get("session")
        or raw.get("time")
        or ""
    ).upper()
    if any(token in value for token in ("BEFORE", "PRE", "BMO")):
        return "BEFORE_MARKET"
    if any(token in value for token in ("AFTER", "AMC")):
        return "AFTER_CLOSE"
    return "UNKNOWN"


def _earnings_freshness(
    raw: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    lifecycle = (
        raw.get("lifecycle")
        if isinstance(raw.get("lifecycle"), dict)
        else {}
    )
    raw_statuses = {
        str(container.get(key) or "").upper()
        for container in (raw, lifecycle)
        for key in (
            "status",
            "freshness",
            "freshness_state",
            "lifecycle_status",
        )
    }
    data_as_of = parse_datetime(
        raw.get("data_as_of") or lifecycle.get("data_as_of")
    )
    raw_content_deadlines = [
        value
        for value in (
            raw.get("content_valid_until"),
            raw.get("valid_until"),
            lifecycle.get("content_valid_until"),
            lifecycle.get("valid_until"),
        )
        if value not in (None, "")
    ]
    parsed_content_deadlines = [
        parse_datetime(value)
        for value in raw_content_deadlines
    ]
    content_until = (
        min(
            _utc(value)
            for value in parsed_content_deadlines
            if value is not None
        )
        if parsed_content_deadlines
        and all(value is not None for value in parsed_content_deadlines)
        else None
    )
    raw_refresh_deadlines = [
        value
        for value in (
            raw.get("refresh_due_at"),
            raw.get("next_refresh_at"),
            lifecycle.get("refresh_due_at"),
            lifecycle.get("next_refresh_at"),
            lifecycle.get("next_refresh"),
        )
        if value not in (None, "")
    ]
    parsed_refresh_deadlines = [
        parse_datetime(value)
        for value in raw_refresh_deadlines
    ]
    refresh_due_at = (
        min(
            _utc(value)
            for value in parsed_refresh_deadlines
            if value is not None
        )
        if parsed_refresh_deadlines
        and all(value is not None for value in parsed_refresh_deadlines)
        else None
    )
    invalid_status = bool(
        raw_statuses
        & (
            INVALID_ANALYTIC_STATES
            | _INVALID_LIFECYCLE_STATES
        )
        or raw.get("currently_valid") is False
        or lifecycle.get("currently_valid") is False
        or raw.get("superseded_by")
        or lifecycle.get("superseded_by")
    )
    missing_observation = data_as_of is None
    invalid_validity_deadline = bool(
        raw_content_deadlines
        and any(value is None for value in parsed_content_deadlines)
    )
    invalid_refresh_deadline = bool(
        raw_refresh_deadlines
        and any(value is None for value in parsed_refresh_deadlines)
    )
    missing_validity = not raw_content_deadlines
    missing_refresh_due = not raw_refresh_deadlines
    future_observation = bool(
        data_as_of and _utc(data_as_of) > now + timedelta(minutes=5)
    )
    old_observation = bool(
        data_as_of
        and now - _utc(data_as_of)
        > timedelta(
            hours=MNQ_EARNINGS_SELECTION_POLICY.max_observation_age_hours
        )
    )
    expired = bool(content_until and _utc(content_until) <= now)
    refresh_due = bool(
        refresh_due_at and _utc(refresh_due_at) <= now
    )
    invalid_lifecycle_order = bool(
        data_as_of
        and content_until
        and _utc(content_until) < _utc(data_as_of)
    )
    refresh_after_expiry = bool(
        refresh_due_at
        and content_until
        and _utc(refresh_due_at) > _utc(content_until)
    )
    rejection_reason = (
        "EARNINGS_FRESHNESS_STATE_INVALID"
        if invalid_status
        else "EARNINGS_OBSERVATION_TIME_NOT_AVAILABLE"
        if missing_observation
        else "EARNINGS_VALIDITY_DEADLINE_INVALID"
        if invalid_validity_deadline
        else "EARNINGS_VALIDITY_DEADLINE_NOT_AVAILABLE"
        if missing_validity
        else "EARNINGS_REFRESH_DUE_INVALID"
        if invalid_refresh_deadline
        else "EARNINGS_REFRESH_DUE_NOT_AVAILABLE"
        if missing_refresh_due
        else "EARNINGS_OBSERVATION_TIME_IN_FUTURE"
        if future_observation
        else "EARNINGS_OBSERVATION_TOO_OLD"
        if old_observation
        else "EARNINGS_CONTENT_EXPIRED"
        if expired
        else "EARNINGS_REFRESH_DUE"
        if refresh_due
        else "EARNINGS_LIFECYCLE_ORDER_INVALID"
        if invalid_lifecycle_order
        else "EARNINGS_REFRESH_AFTER_EXPIRY"
        if refresh_after_expiry
        else None
    )
    deliverable = rejection_reason is None
    return {
        "deliverable": deliverable,
        "freshness": "CURRENT" if deliverable else "UNAVAILABLE",
        "data_as_of": (
            _utc(data_as_of).isoformat() if data_as_of else None
        ),
        "content_valid_until": (
            _utc(content_until).isoformat() if content_until else None
        ),
        "refresh_due_at": (
            _utc(refresh_due_at).isoformat() if refresh_due_at else None
        ),
        "reason_code": rejection_reason,
    }


def _earnings_candidate_rank(item: dict[str, Any]) -> tuple[Any, ...]:
    source = item.get("source") if isinstance(item.get("source"), dict) else {}
    return (
        _earnings_source_quality_present(item),
        item.get("temporal_precision") == "EXACT",
        item.get("timing") != "UNKNOWN",
        item.get("eps_estimate") is not None,
        item.get("revenue_estimate") is not None,
        item.get("freshness") == "CURRENT",
        bool(source.get("publisher")),
        bool(source.get("source_url")),
        str(item.get("data_as_of") or ""),
        hashlib.sha256(_canonical_json(item)).hexdigest(),
    )


def _earnings_coverage(events: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(events)

    def coverage(predicate: Any) -> dict[str, Any]:
        count = sum(1 for item in events if predicate(item))
        return {
            "count": count,
            "total": total,
            "ratio": round(count / total, 4) if total else 0.0,
        }

    return {
        "relevant_symbols": coverage(
            lambda item: item.get("symbol")
            in MNQ_EARNINGS_SELECTION_POLICY.primary_symbols
        ),
        "event_date": coverage(
            lambda item: _date_value(item.get("event_date")) is not None
        ),
        "timing": coverage(
            lambda item: (
                item.get("temporal_precision") == "EXACT"
                or item.get("timing") != "UNKNOWN"
            )
        ),
        "eps_estimate": coverage(
            lambda item: _finite_scalar_number_present(
                item.get("eps_estimate")
            )
        ),
        "revenue_estimate": coverage(
            lambda item: _finite_scalar_number_present(
                item.get("revenue_estimate")
            )
        ),
        "freshness": coverage(
            lambda item: item.get("freshness") == "CURRENT"
        ),
        "source_quality": coverage(_earnings_source_quality_present),
    }


def _earnings_source_quality_present(item: dict[str, Any]) -> bool:
    source = item.get("source")
    if not isinstance(source, dict):
        return False
    acquisition_provider = str(
        source.get("acquisition_provider") or ""
    ).strip().upper()
    policy = dataset_policy_by_id("earnings")
    allowed_providers = {
        policy.primary_provider,
        *policy.fallback_providers,
        *policy.ai_fallback_providers,
    }
    if (
        acquisition_provider not in allowed_providers
        or not source.get("publisher")
        or not source.get("distributor")
        or not _registered_provider_source_url(
            acquisition_provider,
            source.get("source_url"),
        )
    ):
        return False
    lineage = item.get("lineage")
    if not isinstance(lineage, list):
        return False
    required_fields = {"event_date"}
    if (
        item.get("temporal_precision") == "EXACT"
        or item.get("timing") != "UNKNOWN"
    ):
        required_fields.add("timing")
    for field in ("eps_estimate", "revenue_estimate"):
        if item.get(field) is not None:
            required_fields.add(field)
    return all(
        any(
            isinstance(evidence, dict)
            and str(evidence.get("field") or "").strip().lower()
            == field
            and str(
                evidence.get("acquisition_provider") or ""
            ).strip().upper()
            == acquisition_provider
            and bool(evidence.get("publisher"))
            and bool(evidence.get("distributor"))
            and _registered_provider_source_url(
                acquisition_provider,
                evidence.get("source_url"),
            )
            for evidence in lineage
        )
        for field in required_fields
    )


def _earnings_field_lineage(value: dict[str, Any]) -> list[dict[str, Any]]:
    lineage = value.get("lineage")
    if isinstance(lineage, list):
        observed = [
            deepcopy(item)
            for item in lineage
            if isinstance(item, dict)
        ]
        if observed:
            return observed
    return _field_lineage(value)


def _registered_provider_source_url(
    provider_id: str,
    value: Any,
) -> bool:
    parsed = urlparse(str(value or "").strip())
    hostname = str(parsed.hostname or "").casefold()
    allowed_domains = _provider_source_domains(provider_id)
    return bool(
        parsed.scheme.casefold() == "https"
        and hostname
        and allowed_domains
        and any(
            hostname == domain or hostname.endswith(f".{domain}")
            for domain in allowed_domains
        )
        and hostname not in {"localhost", "127.0.0.1", "::1"}
        and not hostname.endswith(
            (".test", ".invalid", ".localhost")
        )
        and parsed.username is None
        and parsed.password is None
    )


def _registered_provider_source_domain(
    provider_id: str,
    *,
    source_url: Any,
    source_domain: Any,
) -> bool:
    parsed = urlparse(str(source_url or "").strip())
    hostname = str(parsed.hostname or "").casefold().rstrip(".")
    declared = str(source_domain or "").strip().casefold().lstrip(".").rstrip(".")
    allowed_domains = _provider_source_domains(provider_id)
    return bool(
        _registered_provider_source_url(provider_id, source_url)
        and declared
        and (
            hostname == declared
            or hostname.endswith(f".{declared}")
        )
        and any(
            declared == allowed
            or declared.endswith(f".{allowed}")
            for allowed in allowed_domains
        )
    )


def _provider_source_domains(provider_id: str) -> set[str]:
    try:
        registration = provider_by_id(str(provider_id).strip().upper())
    except KeyError:
        return set()
    return {
        str(item).strip().casefold().lstrip(".").rstrip(".")
        for item in registration.source_domains
        if str(item).strip()
    }


def _earnings_section_status(
    coverage: dict[str, Any],
    *,
    bounded_count: int,
) -> tuple[str, str | None]:
    total = int((coverage.get("event_date") or {}).get("total") or 0)
    if total <= 0:
        return "NO_DATA", "NO_CURRENT_EARNINGS_EVENTS"
    if bounded_count > 0:
        return "DEGRADED", "EARNINGS_SELECTION_BOUNDED"
    if any(
        float((coverage.get(field) or {}).get("ratio") or 0.0) < 1.0
        for field in (
            "relevant_symbols",
            "event_date",
            "timing",
            "eps_estimate",
            "revenue_estimate",
            "freshness",
            "source_quality",
        )
    ):
        return "DEGRADED", "EARNINGS_COVERAGE_INCOMPLETE"
    return "AVAILABLE", None


def _project_schedule(
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    verified = bool(
        section.get("nasdaq_cash_session_verified")
        or section.get("session_state_verified")
        or (
            isinstance(section.get("nasdaq_cash_session"), dict)
            and section["nasdaq_cash_session"].get("validation", {}).get("status")
            == "accepted"
        )
    )
    nasdaq_session = _select_schedule_session(
        section.get("nasdaq_cash_session")
    )
    mnq_session = _select_schedule_session(
        section.get("mnq_futures_session")
        or section.get("mnq_session")
    )
    schedule_values = {
        "market_session_status": section.get(
            "market_session_status"
        ),
        "nasdaq_cash_session": nasdaq_session,
        "mnq_futures_session": mnq_session,
    }
    substantive = _market_schedule_delivery_present(
        schedule_values
    )
    status = (
        "AVAILABLE"
        if verified and substantive
        else "PARTIAL"
        if substantive
        else "UNAVAILABLE"
    )
    reason = (
        None
        if status == "AVAILABLE"
        else "SESSION_VERIFICATION_PARTIAL"
        if substantive
        else "MARKET_SCHEDULE_VALUE_NOT_AVAILABLE"
    )
    if status != "AVAILABLE":
        _missing(
            missing,
            "market_schedule.session_state",
            status,
            reason,
            section.get("next_retry_at"),
            ["trading_context"],
        )
    return {
        **_section_metadata(
            status,
            reason,
            section.get("context_date"),
            _nested_value(section, "lifecycle", "valid_until"),
            source=_source(section),
        ),
        "context_date": section.get("context_date"),
        "market_session_status": section.get("market_session_status"),
        "nasdaq_cash_session": nasdaq_session,
        "mnq_futures_session": mnq_session,
    }


def _assess_datum(
    value: dict[str, Any],
    now: datetime,
    *,
    dataset_id: str | None,
    section_sync: Any = None,
) -> dict[str, Any]:
    try:
        policy = dataset_policy_by_id(str(dataset_id or ""))
    except KeyError:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "DATASET_POLICY_NOT_REGISTERED",
            value,
            None,
            None,
        )
    frequency = policy.frequency
    policy_age = timedelta(seconds=policy.sla_seconds)
    raw_status = str(value.get("status") or "").upper()
    raw_freshness = str(value.get("freshness") or "").upper()
    lifecycle = value.get("lifecycle") if isinstance(value.get("lifecycle"), dict) else {}
    sync = section_sync if isinstance(section_sync, dict) else {}
    lifecycle_states = {
        str(item).upper()
        for item in (
            lifecycle.get("status"),
            lifecycle.get("lifecycle_status"),
            lifecycle.get("freshness"),
            lifecycle.get("freshness_state"),
        )
        if item not in (None, "")
    }
    lifecycle_freshness = str(
        lifecycle.get("freshness")
        or lifecycle.get("freshness_state")
        or lifecycle.get("lifecycle_status")
        or lifecycle.get("status")
        or sync.get("freshness")
        or ""
    ).upper()
    raw_data_as_of = (
        value.get("data_as_of")
        or value.get("observed_at")
        or value.get("as_of")
    )
    raw_content_deadlines = [
        item
        for item in (
            value.get("content_valid_until"),
            value.get("valid_until"),
            lifecycle.get("content_valid_until"),
            lifecycle.get("valid_until"),
        )
        if item not in (None, "")
    ]
    parsed_content_deadlines = [
        parse_datetime(item)
        for item in raw_content_deadlines
    ]
    raw_refresh_deadlines = [
        item
        for item in (
            value.get("refresh_due_at"),
            value.get("next_refresh_at"),
            lifecycle.get("refresh_due_at"),
            lifecycle.get("next_refresh_at"),
            lifecycle.get("next_refresh"),
        )
        if item not in (None, "")
    ]
    parsed_refresh_deadlines = [
        parse_datetime(item)
        for item in raw_refresh_deadlines
    ]
    data_as_of = parse_datetime(raw_data_as_of)
    retrieved = parse_datetime(value.get("retrieved_at") or value.get("last_successful_refresh_at"))
    explicit_until = (
        min(
            _utc(item)
            for item in parsed_content_deadlines
            if item is not None
        )
        if parsed_content_deadlines
        and all(item is not None for item in parsed_content_deadlines)
        else None
    )
    refresh_due = (
        min(
            _utc(item)
            for item in parsed_refresh_deadlines
            if item is not None
        )
        if parsed_refresh_deadlines
        and all(item is not None for item in parsed_refresh_deadlines)
        else None
    )
    reference = _utc(data_as_of) if data_as_of else None
    recent_official_read = bool(
        retrieved
        and abs((now - _utc(retrieved)).total_seconds()) <= 24 * 60 * 60
        and (
            value.get("is_official_source") is True
            or value.get("data_origin_is_official") is True
        )
    )
    latest_official_release_verified = _latest_official_release_verified(
        value,
        now=now,
        frequency=frequency,
    )
    if raw_data_as_of not in (None, "") and data_as_of is None:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "DATA_AS_OF_INVALID",
            value,
            explicit_until,
            refresh_due,
        )
    if raw_content_deadlines and explicit_until is None:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "CONTENT_VALID_UNTIL_INVALID",
            value,
            None,
            refresh_due,
        )
    if raw_refresh_deadlines and refresh_due is None:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "REFRESH_DUE_AT_INVALID",
            value,
            explicit_until,
            None,
        )
    if data_as_of is None:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "DATA_AS_OF_NOT_AVAILABLE",
            value,
            explicit_until,
            refresh_due,
        )
    if data_as_of and _utc(data_as_of) > now + timedelta(minutes=5):
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "FUTURE_DATA_AS_OF",
            value,
            explicit_until,
            refresh_due,
        )
    if explicit_until and _utc(explicit_until) <= now:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "CONTENT_VALIDITY_EXPIRED",
            value,
            explicit_until,
            refresh_due,
        )
    if refresh_due and _utc(refresh_due) <= now:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "REFRESH_DUE",
            value,
            explicit_until,
            refresh_due,
        )
    lifecycle_invalid = lifecycle_states & (
        INVALID_ANALYTIC_STATES
        | _INVALID_LIFECYCLE_STATES
    )
    if (
        lifecycle_invalid
        or lifecycle.get("currently_valid") is False
        or lifecycle.get("superseded_by")
    ):
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            f"{_worst_state(lifecycle_invalid or {'INVALID'})}_VALUE_EXCLUDED",
            value,
            explicit_until,
            refresh_due,
        )
    invalid = (
        {raw_status, raw_freshness, lifecycle_freshness}
        & INVALID_ANALYTIC_STATES
    )
    if invalid and not (
        latest_official_release_verified
        and frequency.lower() in {"monthly", "quarterly", "weekly"}
    ):
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            (
                "LATEST_OFFICIAL_RELEASE_NOT_PROVEN"
                if recent_official_read
                else f"{_worst_state(invalid)}_VALUE_EXCLUDED"
            ),
            value,
            explicit_until,
            refresh_due,
        )
    if (
        reference
        and now - reference > policy_age
        and not latest_official_release_verified
    ):
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            (
                "LATEST_OFFICIAL_RELEASE_NOT_PROVEN"
                if recent_official_read
                else "DATASET_SLA_EXCEEDED"
            ),
            value,
            explicit_until,
            refresh_due,
        )
    value_present = _datum_has_value(value)
    if not value_present:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "VALUE_NOT_AVAILABLE",
            value,
            explicit_until,
            refresh_due,
        )
    if (
        latest_official_release_verified
        and frequency.lower() in {"monthly", "quarterly", "weekly"}
    ):
        freshness = "CURRENT_LATEST_OFFICIAL_RELEASE"
        evidence_until = parse_datetime(
            (value.get("official_release_evidence") or {}).get(
                "next_expected_release_at"
            )
        )
        content_until = (
            min(_utc(explicit_until), _utc(evidence_until))
            if explicit_until and evidence_until
            else _utc(explicit_until)
            if explicit_until
            else _utc(evidence_until)
            if evidence_until
            else None
        )
    elif frequency.lower() == "daily":
        freshness = "LAST_AVAILABLE_OFFICIAL_CLOSE"
        content_until = (
            _utc(reference) + policy_age if reference else now + timedelta(hours=12)
        )
    else:
        freshness = "CURRENT"
        content_until = (
            min(_utc(explicit_until), _utc(reference) + policy_age)
            if explicit_until
            else _utc(reference) + policy_age
        )
    return _assessment(
        True,
        "AVAILABLE",
        freshness,
        None,
        value,
        content_until,
        refresh_due,
    )


def _latest_official_release_verified(
    value: dict[str, Any],
    *,
    now: datetime,
    frequency: str,
) -> bool:
    evidence = value.get("official_release_evidence")
    if not isinstance(evidence, dict):
        return False
    cadence = frequency.lower()
    if (
        cadence not in {"weekly", "monthly", "quarterly"}
        or str(evidence.get("status") or "").upper() != "VERIFIED"
        or evidence.get("is_latest_expected_release") is not True
        or str(evidence.get("frequency") or "").lower() != cadence
        or str(evidence.get("source_series_id") or "")
        != str(value.get("series_id") or "")
    ):
        return False
    occurrence_id = str(value.get("occurrence_id") or "")
    if (
        not occurrence_id
        or str(evidence.get("occurrence_id") or "") != occurrence_id
    ):
        return False
    release_at = parse_datetime(
        value.get("release_at")
        or value.get("latest_release_at")
        or value.get("released_at")
    )
    expected_release_at = parse_datetime(evidence.get("expected_release_at"))
    next_expected_release_at = parse_datetime(
        evidence.get("next_expected_release_at")
    )
    validated_at = parse_datetime(evidence.get("validated_at"))
    if (
        not release_at
        or not expected_release_at
        or _utc(release_at) != _utc(expected_release_at)
        or not next_expected_release_at
        or _utc(next_expected_release_at) <= now
        or not validated_at
        or _utc(validated_at) > now + timedelta(minutes=5)
        or now - _utc(validated_at) > timedelta(hours=24)
    ):
        return False
    observed_period = normalize_reference_period(
        value.get("reference_period")
        or value.get("latest_released_period")
        or value.get("data_as_of"),
        frequency=cadence,
        release_date=_utc(release_at),
    )
    expected_period = normalize_reference_period(
        evidence.get("expected_reference_period"),
        frequency=cadence,
        release_date=_utc(expected_release_at),
    )
    return bool(observed_period and observed_period == expected_period)


def _assessment(
    usable: bool,
    status: str,
    freshness: str,
    reason_code: str | None,
    value: dict[str, Any],
    content_until: datetime | None,
    refresh_due: datetime | None,
) -> dict[str, Any]:
    return {
        "usable": usable,
        "status": status,
        "freshness": freshness,
        "reason_code": reason_code,
        "data_as_of": _datetime_value(
            value.get("data_as_of")
            or value.get("observed_at")
            or value.get("as_of")
        ),
        "content_valid_until": (
            _utc(content_until).isoformat() if content_until else None
        ),
        "refresh_due_at": _utc(refresh_due).isoformat() if refresh_due else None,
    }


def _metadata(
    value: dict[str, Any],
    assessment: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": assessment["status"],
        "freshness": assessment["freshness"],
        "data_as_of": assessment["data_as_of"],
        "content_valid_until": assessment["content_valid_until"],
        "snapshot_transport_valid_until": _datetime_value(
            value.get("snapshot_transport_valid_until")
            or _nested_value(value, "sync", "valid_until")
        ),
        "refresh_due_at": assessment["refresh_due_at"],
        "source": _source(value),
        "reason_code": assessment["reason_code"],
        "lineage": _field_lineage(value),
    }


def _empty_metadata(status: str, reason: str) -> dict[str, Any]:
    return {
        "status": status,
        "freshness": "UNAVAILABLE",
        "data_as_of": None,
        "content_valid_until": None,
        "snapshot_transport_valid_until": None,
        "refresh_due_at": None,
        "source": None,
        "reason_code": reason,
        "lineage": [],
    }


def _section_metadata(
    status: str,
    reason: str | None,
    data_as_of: Any,
    content_valid_until: Any,
    *,
    source: Any,
) -> dict[str, Any]:
    return {
        "status": status,
        "freshness": (
            "CURRENT"
            if status == "AVAILABLE"
            else "UNAVAILABLE"
            if status in {"UNAVAILABLE", "NO_DATA", "UNAVAILABLE_AFTER_RELEASE"}
            else "CURRENT"
        ),
        "data_as_of": _datetime_value(data_as_of),
        "content_valid_until": _datetime_value(content_valid_until),
        "snapshot_transport_valid_until": None,
        "refresh_due_at": None,
        "source": source,
        "reason_code": reason,
        "lineage": [],
    }


def _provider_accounting(
    analytics: dict[str, Any],
    *,
    missing_data: list[dict[str, Any]],
    source_payload: dict[str, Any],
    request_id: str | None,
    refresh_mode: str | None,
) -> dict[str, Any]:
    # Acquisition is accepted only from the explicit request-scoped manifest.
    # Delivery is computed here from the final analytical projection.
    envelope = source_payload.get("request_scoped_provider_accounting")
    manifest = envelope if isinstance(envelope, dict) else {}
    correlation_id = manifest.get("correlation_id")
    request_started_at = manifest.get("request_started_at")
    request_completed_at = manifest.get("request_completed_at")
    parsed_request_started_at = parse_datetime(
        request_started_at
    )
    evidence_origin = manifest.get("evidence_origin")
    raw_rows = manifest.get("datasets")
    raw_rows = raw_rows if isinstance(raw_rows, list) else []
    by_dataset: dict[str, dict[str, Any]] = {}
    duplicates: set[str] = set()
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        dataset_id = str(item.get("dataset_id") or "")
        if dataset_id in by_dataset:
            duplicates.add(dataset_id)
            continue
        by_dataset[dataset_id] = item

    correlated_manifest = bool(
        request_id
        and manifest.get("request_id") == request_id
        and correlation_id == request_id
        and evidence_origin == "NORMAL_APPLICATION_REQUEST"
        and manifest.get("evidence_status")
        in {"ACQUISITION_COMPLETE", "INCOMPLETE"}
        and parsed_request_started_at
        and parse_datetime(request_completed_at)
    )
    rows: list[dict[str, Any]] = []
    for policy in DATASET_POLICIES:
        raw = by_dataset.get(policy.dataset_id)
        if (
            raw is None
            or policy.dataset_id in duplicates
            or not correlated_manifest
        ):
            rows.append(
                _incomplete_provider_accounting_row(
                    policy,
                    request_id=request_id,
                    correlation_id=correlation_id,
                    refresh_mode=refresh_mode,
                    reason_code=(
                        "DUPLICATE_REQUEST_SCOPED_EVIDENCE"
                        if policy.dataset_id in duplicates
                        else "UNCORRELATED_REQUEST_SCOPED_EVIDENCE"
                        if raw is not None
                        else "REQUEST_SCOPED_EVIDENCE_NOT_AVAILABLE"
                    ),
                )
            )
            continue
        delivery = _delivery_evidence(
            policy.dataset_id,
            analytics=analytics,
            missing_data=missing_data,
        )
        candidate = {
                "dataset_id": policy.dataset_id,
                "request_id": raw.get("request_id"),
                "correlation_id": raw.get("correlation_id"),
                "evidence_origin": raw.get("evidence_origin"),
                "evidence_status": "COMPLETE",
                "observed_at": raw.get("observed_at"),
                "acquisition_id": raw.get("acquisition_id"),
                "shared_acquisition_dataset_ids": deepcopy(
                    raw.get("shared_acquisition_dataset_ids")
                ),
                "database_lookup_performed": raw.get(
                    "database_lookup_performed"
                ),
                "database_lookup_reason": raw.get(
                    "database_lookup_reason"
                ),
                "database_record_found": raw.get("database_record_found"),
                "database_data_as_of": raw.get("database_data_as_of"),
                "database_content_valid_until": raw.get(
                    "database_content_valid_until"
                ),
                "database_refresh_due_at": raw.get(
                    "database_refresh_due_at"
                ),
                "database_lifecycle_status": raw.get(
                    "database_lifecycle_status"
                ),
                "database_record_expired": raw.get("database_record_expired"),
                "database_freshness_evaluation": raw.get(
                    "database_freshness_evaluation"
                ),
                **(
                    {
                        "database_lookup_summary": deepcopy(
                            raw["database_lookup_summary"]
                        )
                    }
                    if isinstance(
                        raw.get("database_lookup_summary"),
                        dict,
                    )
                    else {}
                ),
                "capability_acquisitions": deepcopy(
                    raw.get("capability_acquisitions") or []
                ),
                "primary_provider": deepcopy(raw.get("primary_provider")),
                "fallbacks": deepcopy(raw.get("fallbacks")),
                "acquisition_selected_source": raw.get(
                    "acquisition_selected_source"
                ),
                "acquisition_reason_code": raw.get(
                    "acquisition_reason_code"
                ),
                **delivery,
                "refresh_mode": refresh_mode,
                "source_snapshot_revision": source_payload.get(
                    "snapshot_revision"
                ),
            }
        if (
            not _acquisition_accounting_row_complete(
                raw,
                policy=policy,
                request_id=request_id,
                correlation_id=correlation_id,
                request_started_at=request_started_at,
                request_completed_at=request_completed_at,
            )
            or not _acquisition_delivery_observation_link_complete(
                candidate,
                payload_root={"analytics": analytics},
            )
        ):
            candidate["evidence_status"] = "INCOMPLETE"
            candidate["reason_code"] = (
                "REQUEST_ACQUISITION_EVIDENCE_INCOMPLETE"
            )
            observed_at = parse_datetime(raw.get("observed_at"))
            if (
                raw.get("database_lookup_performed") is True
                and not raw.get("capability_acquisitions")
                and observed_at is not None
                and parsed_request_started_at is not None
                and not _canonical_database_evidence_valid(
                    raw,
                    policy=policy,
                    observed_at=observed_at,
                    request_started_at=parsed_request_started_at,
                )
            ):
                candidate["database_record_expired"] = None
                candidate["database_freshness_evaluation"] = None
            rows.append(candidate)
            continue
        rows.append(
            candidate
        )
    same_request_complete = bool(
        correlated_manifest
        and not duplicates
        and len(by_dataset) == len(DATASET_POLICIES)
        and all(
            _request_accounting_row_complete(
                row,
                request_id=request_id,
                correlation_id=correlation_id,
                request_started_at=request_started_at,
                request_completed_at=request_completed_at,
                payload_root={"analytics": analytics},
            )
            for row in rows
        )
    )
    return {
        "rows": rows,
        "correlation_id": correlation_id,
        "request_started_at": request_started_at,
        "request_completed_at": request_completed_at,
        "evidence_origin": evidence_origin,
        "same_request_complete": same_request_complete,
    }


def _incomplete_provider_accounting_row(
    policy: DatasetPolicy,
    *,
    request_id: str | None,
    correlation_id: Any,
    refresh_mode: str | None,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "dataset_id": policy.dataset_id,
        "request_id": request_id,
        "correlation_id": correlation_id,
        "evidence_origin": None,
        "evidence_status": "INCOMPLETE",
        "observed_at": None,
        "acquisition_id": None,
        "shared_acquisition_dataset_ids": [],
        "database_lookup_performed": None,
        "database_lookup_reason": None,
        "database_record_found": None,
        "database_data_as_of": None,
        "database_content_valid_until": None,
        "database_refresh_due_at": None,
        "database_lifecycle_status": None,
        "database_record_expired": None,
        "database_freshness_evaluation": None,
        "capability_acquisitions": [],
        "primary_provider": {
            "provider": policy.primary_provider,
            "called": None,
            "attempts": None,
            "result": "EVIDENCE_NOT_AVAILABLE",
        },
        "fallbacks": [
            {
                "provider": provider,
                "called": None,
                "attempts": None,
                "result": "EVIDENCE_NOT_AVAILABLE",
            }
            for provider in policy.fallback_providers
        ],
        "acquisition_selected_source": None,
        "acquisition_reason_code": reason_code,
        "selected_source": None,
        "selected_value_present": None,
        "delivered_value": None,
        "payload_freshness": None,
        "reason_code": reason_code,
        "refresh_mode": refresh_mode,
        "source_snapshot_revision": None,
    }


def _delivery_evidence(
    dataset_id: str,
    *,
    analytics: dict[str, Any],
    missing_data: list[dict[str, Any]],
) -> dict[str, Any]:
    section_name, value = _dataset_delivery_value(
        dataset_id,
        analytics,
    )
    resolved_delivery = (
        deepcopy(value)
        if _substantive_delivery_present(dataset_id, value)
        else None
    )
    present = resolved_delivery is not None
    collection_path = PROVIDER_ACCOUNTING_COLLECTION_PATHS.get(
        dataset_id
    )
    delivered = (
        _delivery_collection_reference(
            collection_path,
            payload_root={"analytics": analytics},
        )
        if present
        and collection_path
        and isinstance(resolved_delivery, list)
        else resolved_delivery
    )
    section = (
        analytics.get(section_name)
        if isinstance(analytics.get(section_name), dict)
        else {}
    )
    source = _first_delivery_field(resolved_delivery, "source")
    if source is None:
        source = section.get("source")
    freshness = _first_delivery_field(resolved_delivery, "freshness")
    if freshness is None:
        freshness = (
            section.get("freshness") or "UNAVAILABLE"
            if present
            else "UNAVAILABLE"
        )
    missing_reasons = sorted(
        {
            str(item.get("reason_code"))
            for item in missing_data
            if isinstance(item, dict)
            and item.get("reason_code")
            and _missing_matches_dataset(
                dataset_id,
                str(item.get("field") or item.get("path") or ""),
            )
        }
    )
    return {
        "selected_source": source if present else None,
        "selected_value_present": present,
        "delivered_value": delivered,
        "payload_freshness": str(freshness),
        "delivery_missing_reason_codes": missing_reasons,
        "reason_code": (
            "FINAL_PAYLOAD_VALUE_DELIVERED"
            if present
            else missing_reasons[0]
            if missing_reasons
            else "FINAL_PAYLOAD_VALUE_NOT_AVAILABLE"
        ),
    }


def _delivery_collection_reference(
    payload_path: str | tuple[str, ...],
    *,
    payload_root: dict[str, Any],
) -> dict[str, Any]:
    paths = _collection_reference_paths(payload_path)
    values: list[Any] = []
    for path in paths:
        resolved = _resolve_payload_path(payload_root, path)
        if not isinstance(resolved, list):
            raise RuntimeError(
                "PROVIDER_ACCOUNTING_COLLECTION_PATH_NOT_A_LIST:"
                f"{path}"
            )
        values.extend(resolved)
    return {
        "payload_path": paths[0] if len(paths) == 1 else list(paths),
        "item_count": len(values),
        "content_sha256": hashlib.sha256(
            _canonical_json(values)
        ).hexdigest(),
    }


def _collection_reference_paths(
    value: str | tuple[str, ...],
) -> tuple[str, ...]:
    return (value,) if isinstance(value, str) else value


def _resolve_payload_path(
    payload: Any,
    payload_path: str,
) -> Any:
    if (
        not isinstance(payload, dict)
        or not payload_path
        or payload_path.startswith(".")
        or payload_path.endswith(".")
    ):
        return _INVALID_DELIVERY_REFERENCE
    current: Any = payload
    for part in payload_path.split("."):
        if (
            not part
            or not isinstance(current, dict)
            or part not in current
        ):
            return _INVALID_DELIVERY_REFERENCE
        current = current[part]
    return current


def _is_delivery_collection_reference(value: Any) -> bool:
    return isinstance(value, dict) and "payload_path" in value


def _resolve_accounting_delivery(
    dataset_id: str,
    value: Any,
    *,
    payload_root: dict[str, Any] | None,
) -> Any:
    expected_path = PROVIDER_ACCOUNTING_COLLECTION_PATHS.get(dataset_id)
    if expected_path is not None and value is None:
        return None
    if not _is_delivery_collection_reference(value):
        if expected_path is not None or isinstance(value, list):
            return _INVALID_DELIVERY_REFERENCE
        return value
    if expected_path is None:
        return _INVALID_DELIVERY_REFERENCE
    expected_paths = _collection_reference_paths(expected_path)
    supplied_path = value.get("payload_path")
    supplied_paths = (
        (supplied_path,)
        if isinstance(supplied_path, str)
        else tuple(supplied_path)
        if isinstance(supplied_path, list)
        and all(isinstance(path, str) for path in supplied_path)
        else ()
    )
    if (
        payload_root is None
        or set(value) != {
            "payload_path",
            "item_count",
            "content_sha256",
        }
        or supplied_paths != expected_paths
        or (
            len(expected_paths) == 1
            and not isinstance(supplied_path, str)
        )
        or (
            len(expected_paths) > 1
            and not isinstance(supplied_path, list)
        )
        or type(value.get("item_count")) is not int
        or value["item_count"] < 0
        or not re.fullmatch(
            r"[0-9a-f]{64}",
            str(value.get("content_sha256") or ""),
        )
    ):
        return _INVALID_DELIVERY_REFERENCE
    resolved: list[Any] = []
    for path in expected_paths:
        collection = _resolve_payload_path(payload_root, path)
        if not isinstance(collection, list):
            return _INVALID_DELIVERY_REFERENCE
        resolved.extend(collection)
    if (
        len(resolved) != value["item_count"]
        or hashlib.sha256(_canonical_json(resolved)).hexdigest()
        != value["content_sha256"]
    ):
        return _INVALID_DELIVERY_REFERENCE
    analytics = payload_root.get("analytics")
    if not isinstance(analytics, dict):
        return _INVALID_DELIVERY_REFERENCE
    # The reference attests the shared consumer node(s); dataset_id then
    # selects the registered semantic subset instead of treating every item
    # in a shared collection as delivered for every dataset.
    _, dataset_delivery = _dataset_delivery_value(
        dataset_id,
        analytics,
    )
    if not isinstance(dataset_delivery, list):
        return _INVALID_DELIVERY_REFERENCE
    return dataset_delivery


def _ensure_required_dataset_missing_data(
    analytics: dict[str, Any],
    *,
    missing_data: list[dict[str, Any]],
    source_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    output = list(missing_data)
    envelope = source_payload.get("request_scoped_provider_accounting")
    manifest = envelope if isinstance(envelope, dict) else {}
    rows = manifest.get("datasets")
    rows = rows if isinstance(rows, list) else []
    acquisition_by_dataset = {
        str(item.get("dataset_id") or ""): item
        for item in rows
        if isinstance(item, dict) and item.get("dataset_id")
    }
    for policy in DATASET_POLICIES:
        if not policy.required_for_analysis:
            continue
        _, value = _dataset_delivery_value(
            policy.dataset_id,
            analytics,
        )
        if _substantive_delivery_present(policy.dataset_id, value):
            continue
        exact_field = f"datasets.{policy.dataset_id}"
        if any(
            isinstance(item, dict)
            and str(item.get("field") or item.get("path") or "")
            == exact_field
            and _specific_required_omission_reason(
                item.get("reason_code")
            )
            for item in output
        ):
            continue
        acquisition = acquisition_by_dataset.get(policy.dataset_id)
        reason_code = _required_dataset_omission_reason(
            policy.dataset_id,
            analytics=analytics,
            acquisition=acquisition,
            existing_missing=output,
        )
        _missing(
            output,
            exact_field,
            (
                "PARTIAL"
                if policy.dataset_id == "target_range"
                and str(
                    ((analytics.get("rates") or {}).get("target_range") or {}).get(
                        "status"
                    )
                    or ""
                ).upper()
                == "PARTIAL"
                else "UNAVAILABLE"
            ),
            reason_code,
            (
                acquisition.get("database_refresh_due_at")
                if isinstance(acquisition, dict)
                else None
            ),
            ["senior_analyst_v1"],
        )
    return output


def _required_dataset_omission_reason(
    dataset_id: str,
    *,
    analytics: dict[str, Any],
    acquisition: dict[str, Any] | None,
    existing_missing: list[dict[str, Any]],
) -> str:
    if dataset_id == "target_range":
        target = (analytics.get("rates") or {}).get("target_range")
        target_reason = (
            target.get("reason_code")
            if isinstance(target, dict)
            else None
        )
        if _specific_required_omission_reason(target_reason):
            return str(target_reason)

    matching_reasons = [
        str(item.get("reason_code") or "")
        for item in existing_missing
        if isinstance(item, dict)
        and _missing_matches_dataset(
            dataset_id,
            str(item.get("field") or item.get("path") or ""),
        )
        and _specific_required_omission_reason(
            item.get("reason_code")
        )
    ]
    if "INSUFFICIENT_VALID_HISTORY" in matching_reasons:
        return "INSUFFICIENT_VALID_HISTORY"
    if not isinstance(acquisition, dict):
        return (
            sorted(set(matching_reasons))[0]
            if matching_reasons
            else "REQUEST_SCOPED_ACQUISITION_EVIDENCE_NOT_AVAILABLE"
        )

    provider_observations = [
        item
        for item in (
            acquisition.get("primary_provider"),
            *(acquisition.get("fallbacks") or []),
        )
        if isinstance(item, dict)
    ]
    called = [
        item
        for item in provider_observations
        if item.get("called") is True
    ]
    successful = [
        item
        for item in called
        if str(item.get("result") or "").upper()
        in {"FOUND", "OK", "SUCCESS", "VALUE_ACQUIRED"}
    ]
    not_called_reasons = {
        str(item.get("not_called_reason") or "").upper()
        for item in provider_observations
    }
    if not called and any(
        "NOT_CONFIGURED" in reason
        for reason in not_called_reasons
    ):
        return "REQUIRED_DATASET_PROVIDER_NOT_CONFIGURED"

    database_expired = acquisition.get("database_record_expired") is True
    lifecycle = str(
        acquisition.get("database_lifecycle_status") or ""
    ).upper()
    freshness = str(
        acquisition.get("database_freshness_evaluation") or ""
    ).upper()

    if (
        _delivery_mapping_missing(dataset_id, analytics)
        and (
            successful
            or acquisition.get("acquisition_selected_source")
        )
    ):
        return "DELIVERY_MAPPING_MISSING"
    if matching_reasons:
        return sorted(set(matching_reasons))[0]
    if successful:
        return (
            "PROVIDER_SUCCEEDED_REQUIRED_SERIES_ABSENT_AFTER_EXPIRED_DATABASE_RECORD"
            if database_expired
            else "PROVIDER_SUCCEEDED_REQUIRED_SERIES_ABSENT"
        )
    if lifecycle not in {"", "ACTIVE", "CURRENT", "VALID"} or (
        freshness == "INVALID_LIFECYCLE"
    ):
        return "INVALID_DATABASE_LIFECYCLE_AND_NO_VALID_REPLACEMENT"
    if database_expired:
        return (
            "DATABASE_RECORD_EXPIRED_AND_PROVIDER_CHAIN_FAILED"
            if called
            else "DATABASE_RECORD_EXPIRED_AND_PROVIDER_NOT_CALLED"
        )
    if called:
        return "PROVIDER_CHAIN_FAILED_NO_VALID_VALUE"
    if acquisition.get("database_record_found") is False:
        return "DATABASE_RECORD_ABSENT_AND_NO_VALID_PROVIDER_VALUE"
    if (
        acquisition.get("database_record_found") is True
        and freshness == "VALID"
    ):
        return "VALID_DATABASE_RECORD_MISSING_REQUIRED_SERIES"
    return "OBSERVED_ACQUISITION_DID_NOT_PRODUCE_REQUIRED_VALUE"


def _specific_required_omission_reason(value: Any) -> bool:
    return (
        str(value or "").strip().upper()
        not in GENERIC_REQUIRED_OMISSION_REASON_CODES
    )


def _delivery_mapping_missing(
    dataset_id: str,
    analytics: dict[str, Any],
) -> bool:
    rates = analytics.get("rates")
    if not isinstance(rates, dict):
        return False
    metrics = rates.get("metrics")
    if not isinstance(metrics, list):
        return False

    valid_series = {
        str(item.get("series_id") or "").upper()
        for item in metrics
        if isinstance(item, dict)
        and _finite_scalar_number_present(item.get("value"))
    }
    if dataset_id == "treasury_rates":
        return bool(
            isinstance(rates.get("treasury_rates"), list)
            and valid_series & TREASURY_RATE_SERIES
        )
    if dataset_id == "fed_funds":
        return bool(
            isinstance(rates.get("fed_funds"), list)
            and valid_series & FED_FUNDS_RATE_SERIES
        )
    if dataset_id == "target_range":
        return bool(
            not isinstance(rates.get("target_range"), dict)
            and TARGET_RANGE_SERIES <= valid_series
        )
    return False


def _dataset_delivery_value(
    dataset_id: str,
    analytics: dict[str, Any],
) -> tuple[str, Any]:
    if dataset_id in MACRO_DATASET_SERIES:
        metrics = (analytics.get("macro") or {}).get("metrics") or []
        return "macro", [
            item
            for item in metrics
            if isinstance(item, dict)
            and str(item.get("series_id") or "").upper()
            in MACRO_DATASET_SERIES[dataset_id]
            and item.get("value") is not None
        ]
    if dataset_id in {"treasury_rates", "fed_funds"}:
        expected = (
            FED_FUNDS_RATE_SERIES
            if dataset_id == "fed_funds"
            else TREASURY_RATE_SERIES
        )
        rates = analytics.get("rates") or {}
        dataset_metrics = rates.get(dataset_id)
        metrics = (
            dataset_metrics
            if isinstance(dataset_metrics, list)
            else rates.get("metrics") or []
        )
        return "rates", [
            item
            for item in metrics
            if isinstance(item, dict)
            and str(item.get("series_id") or "").upper() in expected
            and item.get("value") is not None
        ]
    if dataset_id in {"nasdaq_100", "mega_cap_quotes"}:
        components = (analytics.get("nasdaq") or {}).get("components") or []
        keys = (
            ("symbol", "weight_pct")
            if dataset_id == "nasdaq_100"
            else ("symbol", "price", "change_pct")
        )
        return "nasdaq", [
            {
                key: item.get(key)
                for key in (
                    *keys,
                    "source",
                    "freshness",
                    "data_as_of",
                    "content_valid_until",
                )
            }
            for item in components
            if isinstance(item, dict)
            and any(item.get(key) is not None for key in keys[1:])
        ]
    if dataset_id == "market_internals":
        return "market_internals", _delivery_fields(
            analytics.get("market_internals") or {},
            (
                "advance_decline_ratio",
                "advancers",
                "decliners",
                "percent_advancers",
                "weighted_breadth",
            ),
        )
    if dataset_id in {"vix", "vvix"}:
        value = (analytics.get("vix") or {}).get(dataset_id.upper())
        return (
            "vix",
            value
            if isinstance(value, dict) and value.get("value") is not None
            else None,
        )
    if dataset_id == "risk":
        return "risk", _delivery_fields(
            analytics.get("risk") or {},
            ("risk_sentiment", "risk_score"),
        )
    if dataset_id == "target_range":
        rates_target = (analytics.get("rates") or {}).get(
            "target_range"
        )
        return "rates", deepcopy(rates_target)
    if dataset_id == "fomc_expectations":
        return "fomc", _delivery_fields(
            analytics.get("fomc") or {},
            (
                "action",
                "change_bps",
                "pre_meeting_probabilities",
            ),
        )
    if dataset_id in {"macro_calendar", "flash_services_pmi"}:
        calendar = analytics.get("calendar") or {}
        delivery_keys = (
            (
                "latest_released_events",
                "active_event_windows",
                "next_24h_events",
                "next_7d_high_impact_events",
            )
            if dataset_id == "flash_services_pmi"
            else (
                "active_event_windows",
                "next_24h_events",
                "next_7d_high_impact_events",
            )
        )
        events = [
            item
            for key in delivery_keys
            for item in calendar.get(key) or []
            if isinstance(item, dict)
        ]
        if dataset_id == "flash_services_pmi":
            pmi_events = [
                item
                for item in events
                if (
                    "flash_services_pmi"
                    in str(item.get("metric_id") or "").lower()
                    or "flash services pmi"
                    in str(item.get("name") or "").lower()
                )
                and item.get("actual") not in (None, "")
                and (
                    item.get("occurrence_id")
                    or item.get("event_id")
                )
            ]
            latest = (
                max(
                    pmi_events,
                    key=lambda item: (
                        parse_datetime(
                            item.get("release_at")
                            or item.get("scheduled_at_utc")
                            or item.get("time_utc")
                        )
                        or datetime.min.replace(tzinfo=UTC),
                        str(
                            item.get("occurrence_id")
                            or item.get("event_id")
                            or ""
                        ),
                    ),
                )
                if pmi_events
                else None
            )
            events = [
                {
                    "occurrence_id": (
                        latest.get("occurrence_id")
                        or latest.get("event_id")
                    ),
                    "value": latest.get("actual"),
                    "source": (
                        latest.get("actual_source")
                        or latest.get("publisher")
                    ),
                    "freshness": (
                        latest.get("freshness_state")
                        or latest.get("freshness")
                    ),
                    "data_as_of": (
                        latest.get("reference_period")
                        or latest.get("release_at")
                    ),
                    "release_at": (
                        latest.get("release_at")
                        or latest.get("scheduled_at_utc")
                        or latest.get("time_utc")
                    ),
                    "content_valid_until": (
                        latest.get("content_valid_until")
                        or latest.get("valid_until")
                    ),
                }
            ] if latest is not None else []
        return "calendar", events
    if dataset_id == "earnings":
        return "earnings", (analytics.get("earnings") or {}).get(
            "events"
        )
    if dataset_id == "options_positioning":
        return "options_positioning", _delivery_fields(
            analytics.get("options_positioning") or {},
            ("iv_atm", "open_interest", "volume", "skew"),
        )
    if dataset_id == "positioning":
        return "positioning", (analytics.get("positioning") or {}).get(
            "cot"
        )
    if dataset_id == "current_news":
        return "news", (analytics.get("news") or {}).get(
            "current_news"
        )
    if dataset_id == "market_schedule":
        return "market_schedule", analytics.get("market_schedule")
    return dataset_id, None


def _delivery_fields(
    section: dict[str, Any],
    fields: tuple[str, ...],
) -> dict[str, Any] | None:
    values = {
        key: deepcopy(section.get(key))
        for key in fields
        if _meaningful_delivery(section.get(key))
    }
    if not values:
        return None
    return {
        **values,
        "source": section.get("source"),
        "freshness": section.get("freshness"),
        "data_as_of": section.get("data_as_of"),
        "content_valid_until": section.get("content_valid_until"),
    }


def _meaningful_delivery(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        ignored = {
            "status",
            "freshness",
            "source",
            "reason_code",
            "data_as_of",
            "content_valid_until",
            "snapshot_transport_valid_until",
            "refresh_due_at",
            "lineage",
        }
        return any(
            _meaningful_delivery(item)
            for key, item in value.items()
            if key not in ignored
        )
    if isinstance(value, (list, tuple)):
        return any(_meaningful_delivery(item) for item in value)
    return value != ""


def _substantive_delivery_present(dataset_id: str, value: Any) -> bool:
    if dataset_id == "target_range":
        return _target_range_delivery_present(value)
    if dataset_id == "positioning":
        return _positioning_delivery_present(value)
    if dataset_id == "options_positioning":
        return _options_positioning_delivery_present(value)
    if dataset_id == "current_news":
        return _current_news_delivery_present(value)
    if dataset_id == "risk":
        return _risk_delivery_present(value)
    if dataset_id == "market_schedule":
        return _market_schedule_delivery_present(value)
    if isinstance(value, dict) and "value" in value:
        return _meaningful_delivery(value.get("value"))
    return _meaningful_delivery(value)


def _target_range_delivery_present(value: Any) -> bool:
    return bool(
        isinstance(value, dict)
        and str(value.get("status") or "").upper() == "AVAILABLE"
        and _finite_scalar_number_present(
            value.get("target_range_lower")
        )
        and _finite_scalar_number_present(
            value.get("target_range_upper")
        )
    )


def _positioning_delivery_present(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if _finite_scalar_number_present(
        value.get("open_interest")
    ):
        return True
    return any(
        _numeric_measure_present(value.get(group))
        for group in (
            "asset_managers",
            "leveraged_funds",
            "dealers",
        )
    )


def _numeric_measure_present(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, dict):
        return any(
            _numeric_measure_present(item)
            for key, item in value.items()
            if key
            not in {
                "symbol",
                "underlying",
                "target_context",
                "expiration",
                "expiration_date",
                "date",
                "data_as_of",
                "as_of",
                "retrieved_at",
                "content_valid_until",
                "refresh_due_at",
                "source",
                "source_url",
                "provider",
                "provider_type",
                "status",
                "freshness",
                "reason_code",
                "contract_code",
                "cftc_contract_market_code",
            }
        )
    if isinstance(value, (list, tuple)):
        return any(_numeric_measure_present(item) for item in value)
    return _finite_scalar_number_present(value)


def _finite_scalar_number_present(value: Any) -> bool:
    if (
        isinstance(value, bool)
        or value is None
        or isinstance(value, (dict, list, tuple, set))
    ):
        return False
    number = _number(value)
    return bool(number is not None and math.isfinite(number))


def _options_positioning_delivery_present(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return bool(
        _finite_scalar_number_present(value.get("iv_atm"))
        or _option_metric_present(
            value.get("open_interest"),
            keys={
                "calls",
                "puts",
                "put_call_ratio",
                "total",
                "total_contracts",
                "total_open_interest",
                "call_open_interest",
                "put_open_interest",
            },
        )
        or _option_metric_present(
            value.get("volume"),
            keys={
                "calls",
                "puts",
                "put_call_ratio",
                "total",
                "total_contracts",
                "total_volume",
                "call_volume",
                "put_volume",
            },
        )
        or _option_metric_present(
            value.get("skew"),
            keys={
                "value",
                "skew",
                "iv_skew",
                "put_call_skew",
                "slope",
                "percentile",
            },
        )
    )


def _option_metric_present(
    value: Any,
    *,
    keys: set[str],
) -> bool:
    if not isinstance(value, dict):
        return _finite_scalar_number_present(value)
    return any(
        _finite_scalar_number_present(value.get(key))
        for key in keys
        if key in value
    )


def _current_news_delivery_present(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    return any(
        isinstance(item, dict)
        and any(
            substantive_news_text(item.get(field))
            for field in (
                "headline",
                "title",
                "summary",
                "content",
                "content_snippet",
                "description",
            )
        )
        for item in value
    )


def _risk_delivery_present(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return bool(
        _nonempty_text_present(value.get("risk_sentiment"))
        or _finite_scalar_number_present(value.get("risk_score"))
    )


def _market_schedule_delivery_present(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if _nonempty_text_present(
        value.get("market_session_status")
    ):
        return True
    for key in ("nasdaq_cash_session", "mnq_futures_session"):
        session = value.get(key)
        if not isinstance(session, dict):
            continue
        if (
            _nonempty_text_present(session.get("status"))
            or type(session.get("is_open")) is bool
            or _valid_timestamp_present(session.get("next_open"))
            or _valid_timestamp_present(session.get("next_close"))
        ):
            return True
    return False


def _nonempty_text_present(value: Any) -> bool:
    return bool(isinstance(value, str) and value.strip())


def _valid_timestamp_present(value: Any) -> bool:
    return bool(
        not isinstance(value, (dict, list, tuple, set, bool))
        and parse_datetime(value) is not None
    )


def _first_delivery_field(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        if value.get(field) not in (None, ""):
            return value[field]
        for item in value.values():
            found = _first_delivery_field(item, field)
            if found not in (None, ""):
                return found
    elif isinstance(value, list):
        for item in value:
            found = _first_delivery_field(item, field)
            if found not in (None, ""):
                return found
    return None


def _missing_matches_dataset(dataset_id: str, field: str) -> bool:
    lowered = field.lower()
    if lowered == f"datasets.{dataset_id.lower()}":
        return True
    tokens = {
        "nasdaq_100": ("nasdaq.components",),
        "mega_cap_quotes": ("nasdaq.components", "nasdaq.drivers"),
        "market_internals": ("market_internals",),
        "vix": ("vix.vix",),
        "vvix": ("vix.vvix",),
        "risk": ("risk.",),
        "treasury_rates": (
            "rates.treasury_rates",
            "rates.metrics.dgs2",
            "rates.metrics.dgs10",
            "rates.metrics.dgs30",
            "rates.metrics.t10y2y",
            "rates.metrics.t10y3m",
            "rates.metrics.nfci",
        ),
        "fed_funds": (
            "rates.fed_funds",
            "rates.metrics.dff",
            "rates.metrics.fedfunds",
            "rates.metrics.sofr",
        ),
        "target_range": (
            "rates.target_range",
            "rates.metrics.dfedtarl",
            "rates.metrics.dfedtaru",
            "fomc.target_range",
        ),
        "fomc_expectations": ("fomc.action", "fomc.pre_meeting"),
        "cpi": ("headline_cpi", "core_cpi",),
        "ppi": ("ppi", "wpufd4"),
        "pce": ("pce",),
        "gdp": ("gdp",),
        "employment": ("employment", "unrate"),
        "wages": ("wage",),
        "nfp": ("payroll", "nfp"),
        "jobless_claims": ("jobless", "icsa"),
        "macro_calendar": ("calendar.",),
        "flash_services_pmi": ("flash_services_pmi",),
        "earnings": ("earnings.",),
        "options_positioning": ("options_positioning",),
        "positioning": ("positioning",),
        "current_news": ("news.current_news",),
        "market_schedule": ("market_schedule",),
    }
    return any(token in lowered for token in tokens.get(dataset_id, ()))


def _required_dataset_omissions_without_reason(
    analytics: dict[str, Any],
    missing_data: Any,
) -> set[str]:
    missing_items = (
        missing_data
        if isinstance(missing_data, list)
        else []
    )
    omissions: set[str] = set()
    for policy in DATASET_POLICIES:
        if not policy.required_for_analysis:
            continue
        _, value = _dataset_delivery_value(
            policy.dataset_id,
            analytics,
        )
        if _substantive_delivery_present(policy.dataset_id, value):
            continue
        has_specific_reason = any(
            isinstance(item, dict)
            and _missing_matches_dataset(
                policy.dataset_id,
                str(item.get("field") or item.get("path") or ""),
            )
            and _specific_required_omission_reason(
                item.get("reason_code")
            )
            for item in missing_items
        )
        if not has_specific_reason:
            omissions.add(policy.dataset_id)
    return omissions


def _acquisition_accounting_row_complete(
    item: Any,
    *,
    policy: DatasetPolicy,
    request_id: Any,
    correlation_id: Any,
    request_started_at: Any,
    request_completed_at: Any,
) -> bool:
    if (
        not isinstance(item, dict)
        or item.get("dataset_id") != policy.dataset_id
        or item.get("request_id") != request_id
        or item.get("correlation_id") != correlation_id
        or item.get("evidence_origin") != "NORMAL_APPLICATION_REQUEST"
        or item.get("evidence_status") != "ACQUISITION_COMPLETE"
        or not item.get("acquisition_id")
        or policy.dataset_id
        not in (item.get("shared_acquisition_dataset_ids") or [])
        or type(item.get("database_lookup_performed")) is not bool
        or not item.get("database_lookup_reason")
        or not item.get("database_freshness_evaluation")
        or not item.get("acquisition_reason_code")
    ):
        return False
    if (
        policy.dataset_id == "flash_services_pmi"
        and _flash_pmi_acquisition_observation_id(item) is None
    ):
        return False
    observed = parse_datetime(item.get("observed_at"))
    started = parse_datetime(request_started_at)
    completed = parse_datetime(request_completed_at)
    if (
        not observed
        or not started
        or not completed
        or _utc(observed) < _utc(started)
        or _utc(observed) > _utc(completed)
    ):
        return False
    lookup = item["database_lookup_performed"]
    if policy.canonical_repository_required and not lookup:
        return False
    capability_scoped = (
        str(policy.provider_strategy).upper() == "FAN_IN"
        and bool(item.get("capability_acquisitions"))
    )
    if capability_scoped:
        if (
            lookup is not True
            or item.get("database_record_found") is not None
            or item.get("database_record_expired") is not None
            or item.get("database_data_as_of") is not None
            or item.get("database_content_valid_until") is not None
            or item.get("database_refresh_due_at") is not None
            or item.get("database_lifecycle_status") is not None
            or item.get("database_freshness_evaluation")
            != "CAPABILITY_SCOPED"
        ):
            return False
    elif lookup:
        if (
            type(item.get("database_record_found")) is not bool
            or type(item.get("database_record_expired")) is not bool
        ):
            return False
        if item["database_record_found"] and (
            not item.get("database_data_as_of")
            or not item.get("database_content_valid_until")
            or not item.get("database_refresh_due_at")
        ):
            return False
        if not _canonical_database_evidence_valid(
            item,
            policy=policy,
            observed_at=observed,
            request_started_at=started,
        ):
            return False
    elif any(
        item.get(key) is not None
        for key in (
            "database_record_found",
            "database_data_as_of",
            "database_content_valid_until",
            "database_refresh_due_at",
            "database_lifecycle_status",
            "database_record_expired",
        )
    ) or item.get("database_freshness_evaluation") != "NOT_LOOKED_UP":
        return False
    primary = item.get("primary_provider")
    fallbacks = item.get("fallbacks")
    if (
        not isinstance(primary, dict)
        or primary.get("provider") != policy.primary_provider
        or not isinstance(fallbacks, list)
        or [
            value.get("provider")
            for value in fallbacks
            if isinstance(value, dict)
        ]
        != list(policy.fallback_providers)
    ):
        return False
    return bool(
        all(
            _provider_attempt_complete(attempt)
            for attempt in [primary, *fallbacks]
        )
        and _provider_flow_valid(
            item,
            policy=policy,
            governed_dataset_ids={
                registered.dataset_id
                for registered in DATASET_POLICIES
            },
        )
    )


def _provider_attempt_complete(attempt: Any) -> bool:
    if not isinstance(attempt, dict):
        return False
    called = attempt.get("called")
    count = attempt.get("attempts")
    origin = attempt.get("execution_origin")
    if (
        type(called) is not bool
        or not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        or not attempt.get("provider")
        or not attempt.get("result")
    ):
        return False
    if called:
        return bool(
            count > 0
            and origin == "PROVIDER_CALL"
            and not attempt.get("not_called_reason")
        )
    return bool(
        count == 0
        and attempt.get("not_called_reason")
        and origin in {"OBSERVED_SKIP", "CACHE_DECISION"}
    )


def _flash_pmi_acquisition_observation_id(
    item: Any,
) -> str | None:
    if not isinstance(item, dict):
        return None
    acquisition_id = str(item.get("acquisition_id") or "")
    if not acquisition_id.startswith(FLASH_PMI_ACQUISITION_PREFIX):
        return None
    observation_id = acquisition_id.removeprefix(
        FLASH_PMI_ACQUISITION_PREFIX
    ).strip()
    if not observation_id or observation_id == "NO_OCCURRENCE":
        return None
    return observation_id


def _acquisition_delivery_observation_link_complete(
    item: Any,
    *,
    payload_root: dict[str, Any] | None = None,
) -> bool:
    if (
        not isinstance(item, dict)
        or item.get("dataset_id") != "flash_services_pmi"
    ):
        return True
    observation_id = _flash_pmi_acquisition_observation_id(item)
    if observation_id is None:
        return False
    if item.get("selected_value_present") is not True:
        return True
    delivered = _resolve_accounting_delivery(
        "flash_services_pmi",
        item.get("delivered_value"),
        payload_root=payload_root,
    )
    if not isinstance(delivered, list):
        return False
    return bool(
        len(delivered) == 1
        and isinstance(delivered[0], dict)
        and str(delivered[0].get("occurrence_id") or "")
        == observation_id
    )


def _readiness(analytics: dict[str, Any]) -> dict[str, Any]:
    value_counts = {
        name: _readiness_section_value_count(name, analytics)
        for name in SECTION_NAMES
    }
    statuses = {
        name: (
            _earnings_readiness_status(analytics.get(name) or {})
            if name == "earnings"
            else _readiness_section_status(
                (analytics.get(name) or {}).get("status"),
                value_count=value_counts[name],
            )
        )
        for name in SECTION_NAMES
    }
    available = [
        name
        for name in SECTION_NAMES
        if statuses[name] == "AVAILABLE"
    ]
    degraded = [
        name
        for name in SECTION_NAMES
        if statuses[name] in {"PARTIAL", "DEGRADED"}
    ]
    unavailable = [
        name
        for name in SECTION_NAMES
        if statuses[name] == "UNAVAILABLE"
    ]
    usable_count = len(available) + len(degraded)
    coverage = round(usable_count / len(SECTION_NAMES), 4)
    return {
        "status": (
            "READY"
            if len(available) == len(SECTION_NAMES)
            else "PARTIAL"
            if usable_count
            else "UNAVAILABLE"
        ),
        "calculated_from_delivered_payload": True,
        "available_section_count": len(available),
        "degraded_section_count": len(degraded),
        "unavailable_section_count": len(unavailable),
        "section_count": len(SECTION_NAMES),
        "coverage_ratio": coverage,
        "sections_available": available,
        "sections_degraded": degraded,
        "sections_unavailable": unavailable,
        "section_status": statuses,
        "delivered_value_counts": value_counts,
        "excluded_values_contribute": False,
    }


def _readiness_section_value_count(
    section_name: str,
    analytics: dict[str, Any],
) -> int:
    count = 0
    for policy in DATASET_POLICIES:
        if policy.section != section_name:
            continue
        _, delivered = _dataset_delivery_value(
            policy.dataset_id,
            analytics,
        )
        count += _substantive_dataset_delivery_count(
            policy.dataset_id,
            delivered,
        )
    return count


def _substantive_dataset_delivery_count(
    dataset_id: str,
    delivered: Any,
) -> int:
    if dataset_id == "target_range":
        return int(
            isinstance(delivered, dict)
            and any(
                _finite_scalar_number_present(delivered.get(field))
                for field in (
                    "target_range_lower",
                    "target_range_upper",
                )
            )
        )
    if dataset_id == "market_schedule":
        return int(_readiness_market_schedule_present(delivered))
    if dataset_id == "macro_calendar":
        return sum(
            1
            for item in delivered or []
            if _readiness_calendar_event_present(item)
        )
    if dataset_id == "earnings":
        return sum(
            1
            for item in delivered or []
            if _readiness_earnings_event_present(item)
        )
    if not _substantive_delivery_present(dataset_id, delivered):
        return 0
    if isinstance(delivered, list):
        return sum(
            1
            for item in delivered
            if _substantive_delivery_present(
                dataset_id,
                [item] if dataset_id == "current_news" else item,
            )
        )
    return 1


def _readiness_market_schedule_present(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if _nonempty_text_present(value.get("market_session_status")):
        return True
    return any(
        isinstance(session, dict)
        and (
            _nonempty_text_present(session.get("status"))
            or type(session.get("is_open")) is bool
        )
        for session in (
            value.get("nasdaq_cash_session"),
            value.get("mnq_futures_session"),
        )
    )


def _readiness_calendar_event_present(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if any(
        _nonempty_text_present(value.get(field))
        for field in ("name", "event_name", "title")
    ):
        return True
    return any(
        _finite_scalar_number_present(value.get(field))
        for field in (
            "actual",
            "consensus",
            "previous",
            "previous_revised",
            "surprise_absolute",
            "surprise_percent",
        )
    )


def _readiness_earnings_event_present(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if any(
        _finite_scalar_number_present(value.get(field))
        for field in (
            "eps_actual",
            "eps_estimate",
            "revenue_actual",
            "revenue_estimate",
        )
    ):
        return True
    issuer_present = any(
        _nonempty_text_present(value.get(field))
        for field in (
            "symbol",
            "issuer",
            "issuer_name",
            "company",
            "company_name",
        )
    )
    event_time_present = any(
        _valid_timestamp_present(value.get(field))
        for field in (
            "event_date",
            "event_at",
            "release_at",
            "earnings_date",
            "scheduled_date",
            "date",
        )
    )
    return issuer_present and event_time_present


def _earnings_readiness_status(section: dict[str, Any]) -> str:
    events = [
        item
        for item in section.get("events") or []
        if isinstance(item, dict)
        and item.get("symbol")
        in MNQ_EARNINGS_SELECTION_POLICY.primary_symbols
        and _date_value(item.get("event_date")) is not None
    ]
    bounded_count = max(
        int(section.get("relevant_count") or 0) - len(events),
        0,
    )
    status, _ = _earnings_section_status(
        _earnings_coverage(events),
        bounded_count=bounded_count,
    )
    return "UNAVAILABLE" if status == "NO_DATA" else status


def _readiness_section_status(
    producer_status: Any,
    *,
    value_count: int,
) -> str:
    if value_count <= 0:
        return "UNAVAILABLE"
    normalized = str(producer_status or "").strip().upper()
    if normalized in {"PARTIAL", "DEGRADED"}:
        return normalized
    return "AVAILABLE"


def _readiness_section_classification_mismatch_count(
    readiness: Any,
    analytics: dict[str, Any],
) -> int:
    if not isinstance(readiness, dict):
        return len(SECTION_NAMES)

    expected = _readiness(analytics)
    actual_lists = {
        key: value if isinstance(value, list) else []
        for key, value in (
            ("available", readiness.get("sections_available")),
            ("degraded", readiness.get("sections_degraded")),
            ("unavailable", readiness.get("sections_unavailable")),
        )
    }
    expected_lists = {
        "available": expected["sections_available"],
        "degraded": expected["sections_degraded"],
        "unavailable": expected["sections_unavailable"],
    }
    actual_statuses = (
        readiness.get("section_status")
        if isinstance(readiness.get("section_status"), dict)
        else {}
    )
    actual_counts = (
        readiness.get("delivered_value_counts")
        if isinstance(readiness.get("delivered_value_counts"), dict)
        else {}
    )

    mismatches = 0
    for classification, values in actual_lists.items():
        declared_count = readiness.get(
            f"{classification}_section_count"
        )
        if declared_count != len(values):
            mismatches += 1
        if (
            any(not isinstance(item, str) for item in values)
            or len(values) != len(set(values))
        ):
            mismatches += 1
    for name in SECTION_NAMES:
        if tuple(
            name in actual_lists[classification]
            for classification in ("available", "degraded", "unavailable")
        ) != tuple(
            name in expected_lists[classification]
            for classification in ("available", "degraded", "unavailable")
        ):
            mismatches += 1
        if actual_statuses.get(name) != expected["section_status"][name]:
            mismatches += 1
        if actual_counts.get(name) != expected["delivered_value_counts"][name]:
            mismatches += 1

    expected_names = set(SECTION_NAMES)
    mismatches += sum(
        len(
            {
                item
                for item in values
                if isinstance(item, str) and item not in expected_names
            }
        )
        for values in actual_lists.values()
    )
    mismatches += len(set(actual_statuses) - expected_names)
    mismatches += len(set(actual_counts) - expected_names)
    for key in (
        "status",
        "calculated_from_delivered_payload",
        "available_section_count",
        "degraded_section_count",
        "unavailable_section_count",
        "section_count",
        "coverage_ratio",
        "excluded_values_contribute",
    ):
        if readiness.get(key) != expected[key]:
            mismatches += 1
    return mismatches


def _missing(
    missing: list[dict[str, Any]],
    field: str,
    status: str,
    reason_code: str | None,
    expected_refresh_due_at: Any,
    blocking_for: list[str],
) -> None:
    missing.append(
        {
            "field": field,
            "status": status,
            "reason_code": reason_code or "UNSPECIFIED_OMISSION",
            "expected_refresh_due_at": _datetime_value(expected_refresh_due_at),
            "blocking_for": blocking_for,
        }
    )


def _deduplicate_missing(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for item in values:
        selected[(str(item["field"]), str(item["reason_code"]))] = item
    return [
        selected[key]
        for key in sorted(selected, key=lambda item: (item[0], item[1]))
    ]


def _event_metric_id(event: dict[str, Any]) -> str:
    explicit = event.get("metric_id") or event.get("normalized_event_family")
    if explicit:
        metric_id = str(explicit).strip().lower()
        mismatch = metric_semantics_mismatch_reason(
            metric_id,
            name=event.get("name") or event.get("event_name"),
            frequency_hint=" ".join(
                str(item or "")
                for item in (
                    event.get("frequency"),
                    event.get("evaluation_method"),
                )
            ),
        )
        if mismatch == "EVENT_METRIC_FREQUENCY_MISMATCH":
            basis = metric_change_basis_from_text(
                " ".join(
                    str(item or "")
                    for item in (
                        event.get("name"),
                        event.get("event_name"),
                        event.get("frequency"),
                        event.get("evaluation_method"),
                    )
                )
            )
            if basis:
                candidate = re.sub(
                    r"_(?:mom|yoy|qoq)$",
                    f"_{basis}",
                    metric_id,
                )
                if candidate in OFFICIAL_METRICS:
                    return candidate
        return metric_id
    implicit_official = _implicit_official_inflation_metric_id(event)
    if implicit_official is not None:
        return implicit_official
    name = str(event.get("name") or event.get("event_name") or "").lower()
    if "employment situation" in name:
        return "employment_situation"
    if "flash" in name and "pmi" in name and "serv" in name:
        return "flash_services_pmi"
    normalized = re.sub(r"[^a-z0-9]+", "_", name).strip("_")
    return normalized or "unknown_event"


def _implicit_official_inflation_metric_id(
    event: dict[str, Any],
) -> str | None:
    families = _event_inflation_families(event)
    if len(families) != 1:
        return None
    basis = metric_change_basis_from_text(
        " ".join(
            str(item or "")
            for item in (
                event.get("name"),
                event.get("event_name"),
                event.get("frequency"),
                event.get("evaluation_method"),
            )
        )
    )
    if basis not in {"mom", "yoy"}:
        return None
    normalized_name = " ".join(
        re.findall(
            r"[a-z0-9]+",
            str(
                event.get("name")
                or event.get("event_name")
                or ""
            ).casefold(),
        )
    )
    core_markers = (
        "core",
        "base",
        "di base",
        "di fondo",
        "excluding food and energy",
        "ex food and energy",
    )
    variant = (
        "core"
        if any(
            re.search(
                rf"\b{re.escape(marker)}\b",
                normalized_name,
            )
            for marker in core_markers
        )
        else "headline"
    )
    metric_id = f"{variant}_{next(iter(families))}_{basis}"
    return metric_id if metric_id in OFFICIAL_METRICS else None


def _requires_official_event_evidence(
    event: dict[str, Any],
    metric_id: str,
) -> bool:
    return (
        metric_id in OFFICIAL_METRICS
        or bool(_event_inflation_families(event))
    )


def _event_inflation_families(event: dict[str, Any]) -> set[str]:
    normalized = " ".join(
        re.findall(
            r"[a-z0-9]+",
            " ".join(
                str(event.get(key) or "")
                for key in (
                    "name",
                    "event_name",
                    "category",
                    "metric_id",
                    "normalized_event_family",
                )
            ).casefold(),
        )
    )
    markers = {
        "cpi": (
            "cpi",
            "consumer price index",
            "prezzi al consumo",
        ),
        "ppi": (
            "ppi",
            "producer price index",
            "prezzi alla produzione",
        ),
        "pce": (
            "pce",
            "personal consumption expenditure",
            "personal consumption expenditures",
        ),
    }
    return {
        family
        for family, family_markers in markers.items()
        if any(
            re.search(rf"\b{re.escape(marker)}\b", normalized)
            for marker in family_markers
        )
    }


def _event_expected_occurrence_ids(
    event: dict[str, Any],
) -> tuple[Any, ...]:
    occurrence_ids = tuple(
        event.get(key)
        for key in (
            "occurrence_id",
            "canonical_event_key",
            "provider_occurrence_id",
            "source_occurrence_id",
        )
        if event.get(key) not in (None, "")
    )
    if occurrence_ids:
        return occurrence_ids
    return tuple(
        event.get(key)
        for key in ("event_id", "source_event_id")
        if event.get(key) not in (None, "")
    )


def _event_release(event: dict[str, Any]) -> datetime | None:
    value = (
        event.get("release_at")
        or event.get("event_at")
        or event.get("time_utc")
        or event.get("date")
    )
    parsed = parse_datetime(value)
    return _utc(parsed) if parsed else None


def _is_fomc(event: dict[str, Any]) -> bool:
    text = " ".join(
        str(event.get(key) or "")
        for key in ("name", "event_name", "metric_id", "category")
    ).lower()
    return "fomc" in text or "federal open market" in text


def _occurrence_fields_reconciled(
    lineage: list[dict[str, Any]],
    *,
    expected_occurrence_ids: Iterable[Any],
) -> bool:
    occurrence_ids = {
        str(item["occurrence_id"])
        for item in lineage
        if item.get("occurrence_id")
    }
    expected = {
        str(item)
        for item in expected_occurrence_ids
        if item not in (None, "")
    }
    if not occurrence_ids:
        return True
    if not expected:
        return len(occurrence_ids) <= 1
    if not occurrence_ids <= expected:
        return False
    return len(expected) > 1 or len(occurrence_ids) <= 1


def _event_reference_periods_reconciled(
    lineage: list[dict[str, Any]],
    *,
    event_reference_period: Any,
    frequency: str,
    release: datetime,
) -> bool:
    expected = normalize_reference_period(
        event_reference_period,
        frequency=frequency,
        release_date=release,
    )
    observed: set[str] = set()
    for item in lineage:
        field = str(item.get("field") or "").strip().lower()
        if field not in {"actual", "consensus", "forecast"}:
            continue
        raw_period = item.get("reference_period") or item.get("period")
        if raw_period in (None, ""):
            continue
        normalized = normalize_reference_period(
            raw_period,
            frequency=frequency,
            release_date=release,
        )
        if normalized:
            observed.add(normalized)
    return bool(
        len(observed) <= 1
        and (
            not observed
            or expected is None
            or observed == {expected}
        )
    )


def _fomc_probabilities(context: dict[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for action, key in (
        ("CUT_25BPS", "probability_cut_25bps"),
        ("HOLD", "probability_hold"),
        ("HIKE_25BPS", "probability_hike_25bps"),
    ):
        value = _number(context.get(key))
        if value is not None:
            output.append({"action": action, "probability": value})
    return output


def _source(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    publisher = (
        value.get("publisher")
        or value.get("source_originator")
        or value.get("source")
        or value.get("provider")
    )
    distributor = (
        value.get("distributor")
        or value.get("distribution_source")
        or (
        value.get("source")
        if publisher and value.get("source") != publisher
        else None
        )
    )
    acquisition = (
        value.get("acquisition_provider")
        or value.get("provider_adapter")
        or value.get("provider")
    )
    if not any((publisher, distributor, acquisition)):
        return None
    return {
        "publisher": publisher,
        "distributor": distributor,
        "acquisition_provider": acquisition,
        "source_url": value.get("canonical_url") or value.get("source_url"),
    }


def _field_lineage(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, dict):
        return []
    raw = value.get("field_lineage") or value.get("lineage")
    if isinstance(raw, list):
        return [deepcopy(item) for item in raw if isinstance(item, dict)]
    if isinstance(raw, dict):
        return [
            {**deepcopy(item), "field": str(field)}
            for field, item in sorted(raw.items())
            if isinstance(item, dict)
            and (
                item.get("field") in (None, "")
                or str(item.get("field")).strip().lower()
                == str(field).strip().lower()
            )
        ]
    source = _source(value)
    return [{"field": "value", "source": source}] if source else []


def _usable_event_field_lineage(
    value: dict[str, Any],
    *,
    selected_fields: dict[str, str | None],
    now: datetime,
    require_field_specific: bool = False,
    expected_occurrence_ids: Iterable[Any] = (),
    metric_id: str | None = None,
    reference_period: Any = None,
    release: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    aliases = {
        "actual": ("actual",),
        "consensus": ("consensus", "forecast"),
        "forecast": ("forecast", "consensus"),
        "previous": ("previous",),
        "previous_revised": (
            "previous_revised",
            "revised_previous",
        ),
        "revised_previous": (
            "revised_previous",
            "previous_revised",
        ),
    }
    by_field: dict[str, list[dict[str, Any]]] = {}
    generic: list[dict[str, Any]] = []
    occurrence_envelope = _event_occurrence_lineage_envelope(value)
    for raw in _field_lineage(value):
        raw_field = str(raw.get("field") or "").strip().lower()
        source_envelope = (
            _event_actual_source_lineage_envelope(value)
            if raw_field == "actual"
            else {}
        )
        item = {
            **occurrence_envelope,
            **source_envelope,
            **deepcopy(raw),
        }
        field = str(item.get("field") or "").strip().lower()
        if field in {
            "actual",
            "consensus",
            "forecast",
            "previous",
            "previous_revised",
            "revised_previous",
        }:
            by_field.setdefault(field, []).append(item)
        else:
            generic.append(item)

    usable: list[dict[str, Any]] = []
    rejected_reasons: dict[str, str] = {}
    for output_field, selected_field in selected_fields.items():
        if selected_field is None:
            continue
        selected_value = value.get(selected_field)
        selected_lineage = list(by_field.get(selected_field) or [])
        if not selected_lineage:
            alias_lineage: list[dict[str, Any]] = []
            for alias in aliases[selected_field][1:]:
                candidates = list(by_field.get(alias) or [])
                if not candidates:
                    continue
                alias_value = value.get(alias)
                alias_matches = (
                    alias_value not in (None, "")
                    and _event_values_equal(alias_value, selected_value)
                )
                evidence_matches = any(
                    _event_lineage_value_matches(item, selected_value)
                    for item in candidates
                )
                if alias_matches or evidence_matches:
                    alias_lineage.extend(candidates)
                else:
                    rejected_reasons[output_field] = (
                        "FIELD_LINEAGE_VALUE_NOT_RECONCILED"
                    )
            selected_lineage = alias_lineage
        if not selected_lineage and output_field in rejected_reasons:
            continue
        if not selected_lineage and require_field_specific:
            rejected_reasons[output_field] = (
                "FIELD_SPECIFIC_LINEAGE_NOT_AVAILABLE"
            )
            continue
        candidates = selected_lineage or generic
        if not candidates:
            continue
        using_generic_lineage = not selected_lineage
        selected_usable, rejection_reason = (
            _assess_event_lineage_candidates(
                candidates,
                selected_value=selected_value,
                now=now,
                require_all_consistent=using_generic_lineage,
            )
        )
        if rejection_reason:
            rejected_reasons[output_field] = rejection_reason
        else:
            if require_field_specific:
                binding_reasons = [
                    _event_field_lineage_binding_reason(
                        item,
                        output_field=output_field,
                        selected_value=selected_value,
                        expected_occurrence_ids=expected_occurrence_ids,
                        metric_id=metric_id,
                        reference_period=reference_period,
                        release=release,
                    )
                    for item in selected_usable
                ]
                bound = [
                    item
                    for item, reason in zip(
                        selected_usable,
                        binding_reasons,
                        strict=True,
                    )
                    if reason is None
                ]
                if not bound:
                    rejected_reasons[output_field] = (
                        _worst_event_lineage_reason(
                            reason
                            for reason in binding_reasons
                            if reason is not None
                        )
                    )
                    continue
                usable.extend(bound)
            else:
                usable.extend(selected_usable)

    deduplicated: list[dict[str, Any]] = []
    fingerprints: set[str] = set()
    for item in usable:
        fingerprint = json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        if fingerprint not in fingerprints:
            fingerprints.add(fingerprint)
            deduplicated.append(item)
    return deduplicated, rejected_reasons


def _event_lifecycle_container(value: dict[str, Any]) -> dict[str, Any]:
    return (
        value.get("lifecycle")
        if isinstance(value.get("lifecycle"), dict)
        else {}
    )


def _event_lifecycle_states(value: dict[str, Any]) -> set[str]:
    lifecycle = _event_lifecycle_container(value)
    return {
        str(container.get(field) or "").strip().upper()
        for container in (value, lifecycle)
        for field in (
            "freshness",
            "freshness_state",
            "lifecycle_status",
            "status",
        )
        if container.get(field) not in (None, "")
    }


def _event_lineage_freshness_states(
    value: dict[str, Any],
) -> set[str]:
    lifecycle = _event_lifecycle_container(value)
    states = {
        str(item).strip().upper()
        for item in (
            value.get("freshness"),
            value.get("freshness_state"),
            lifecycle.get("freshness"),
            lifecycle.get("freshness_state"),
        )
        if item not in (None, "")
    }
    if states:
        return states
    return {
        str(item).strip().upper()
        for item in (
            lifecycle.get("lifecycle_status"),
            lifecycle.get("status"),
        )
        if item not in (None, "")
    }


def _event_lineage_has_timestamp(
    value: dict[str, Any],
    fields: tuple[str, ...],
) -> bool:
    lifecycle = _event_lifecycle_container(value)
    return any(
        container.get(field) not in (None, "")
        for container in (value, lifecycle)
        for field in fields
    )


def _event_occurrence_lineage_envelope(
    value: dict[str, Any],
) -> dict[str, Any]:
    lifecycle = _event_lifecycle_container(value)
    envelope: dict[str, Any] = {}
    freshness = next(
        (
            item
            for item in (
                value.get("freshness_state"),
                value.get("freshness"),
                lifecycle.get("freshness_state"),
                lifecycle.get("freshness"),
            )
            if item not in (None, "")
        ),
        None,
    )
    if freshness is not None:
        envelope["freshness"] = freshness
    for field, candidates in {
        "content_valid_until": (
            value.get("content_valid_until"),
            lifecycle.get("content_valid_until"),
        ),
        "valid_until": (
            value.get("valid_until"),
            lifecycle.get("valid_until"),
        ),
        "refresh_due_at": (
            value.get("refresh_due_at"),
            lifecycle.get("refresh_due_at"),
        ),
        "next_refresh_at": (
            value.get("next_refresh_at"),
            lifecycle.get("next_refresh_at"),
            lifecycle.get("next_refresh"),
        ),
    }.items():
        candidate = next(
            (
                item
                for item in candidates
                if item not in (None, "")
            ),
            None,
        )
        if candidate is not None:
            envelope[field] = candidate
    return envelope


def _event_actual_source_lineage_envelope(
    value: dict[str, Any],
) -> dict[str, Any]:
    envelope: dict[str, Any] = {}
    for field, candidates in {
        "source_url": (
            value.get("actual_source_url"),
            value.get("source_url"),
        ),
        "source_domain": (
            value.get("actual_source_domain"),
            value.get("source_domain"),
        ),
        "acquisition_provider": (
            value.get("actual_acquisition_provider"),
            value.get("acquisition_provider"),
        ),
    }.items():
        candidate = next(
            (
                item
                for item in candidates
                if item not in (None, "")
            ),
            None,
        )
        if candidate is not None:
            envelope[field] = candidate
    return envelope


def _event_field_lineage_binding_reason(
    lineage: dict[str, Any],
    *,
    output_field: str,
    selected_value: Any,
    expected_occurrence_ids: Iterable[Any],
    metric_id: str | None,
    reference_period: Any,
    release: datetime | None,
) -> str | None:
    expected_occurrences = {
        str(item)
        for item in expected_occurrence_ids
        if item not in (None, "")
    }
    if not expected_occurrences:
        return "EVENT_OCCURRENCE_NOT_PROVEN"
    observed_occurrences = {
        str(lineage[key])
        for key in (
            "occurrence_id",
            "canonical_event_key",
            "provider_occurrence_id",
            "source_occurrence_id",
        )
        if lineage.get(key) not in (None, "")
    }
    if not observed_occurrences:
        observed_occurrences = {
            str(lineage[key])
            for key in ("event_id", "source_event_id")
            if lineage.get(key) not in (None, "")
        }
    if not observed_occurrences:
        return "FIELD_LINEAGE_OCCURRENCE_NOT_PROVEN"
    if (
        expected_occurrences
        and not observed_occurrences <= expected_occurrences
    ):
        return "OCCURRENCE_FIELD_LINEAGE_MISMATCH"

    expected_metric_id = str(metric_id or "").strip().lower()
    observed_metric_id = str(
        lineage.get("metric_id")
        or lineage.get("event_metric_id")
        or ""
    ).strip().lower()
    if not observed_metric_id:
        return "FIELD_LINEAGE_METRIC_NOT_PROVEN"
    if (
        expected_metric_id
        and observed_metric_id != expected_metric_id
    ):
        return "FIELD_LINEAGE_METRIC_MISMATCH"

    evidence_value = _event_lineage_value(lineage)
    if evidence_value is _MISSING:
        return "FIELD_LINEAGE_VALUE_NOT_PROVEN"
    if not _event_values_equal(evidence_value, selected_value):
        return "FIELD_LINEAGE_VALUE_NOT_RECONCILED"

    source = (
        lineage.get("source")
        or lineage.get("publisher")
        or lineage.get("originator")
        or lineage.get("source_originator")
    )
    if not source:
        return "FIELD_LINEAGE_SOURCE_NOT_PROVEN"

    spec = OFFICIAL_METRICS.get(expected_metric_id)
    frequency = spec.frequency if spec is not None else "monthly"
    expected_period = normalize_reference_period(
        reference_period,
        frequency=frequency,
        release_date=release,
    )
    observed_period = normalize_reference_period(
        lineage.get("reference_period") or lineage.get("period"),
        frequency=frequency,
        release_date=release,
    )
    if not expected_period:
        return "EVENT_REFERENCE_PERIOD_NOT_PROVEN"
    if not observed_period:
        return "FIELD_LINEAGE_REFERENCE_PERIOD_NOT_PROVEN"
    if output_field in {"previous", "previous_revised"}:
        expected_previous_period = _previous_reference_period(
            expected_period,
            frequency=frequency,
        )
        if (
            expected_previous_period is None
            or observed_period != expected_previous_period
        ):
            return "REFERENCE_PERIOD_FIELD_LINEAGE_MISMATCH"
    elif observed_period != expected_period:
        return "REFERENCE_PERIOD_FIELD_LINEAGE_MISMATCH"

    expected_basis = metric_change_basis_from_text(expected_metric_id)
    observed_basis = metric_change_basis_from_text(
        " ".join(
            str(item or "")
            for item in (
                lineage.get("frequency"),
                lineage.get("transformation"),
            )
        )
    )
    if expected_basis:
        if observed_basis is None:
            return "FIELD_LINEAGE_FREQUENCY_NOT_PROVEN"
        if expected_basis != observed_basis:
            return "FIELD_LINEAGE_FREQUENCY_MISMATCH"
    if spec is not None:
        observed_frequency = str(
            lineage.get("frequency") or ""
        ).strip().lower()
        if not observed_frequency:
            return "FIELD_LINEAGE_FREQUENCY_NOT_PROVEN"
        if (
            expected_basis is None
            and observed_frequency != spec.frequency
        ):
            return "FIELD_LINEAGE_FREQUENCY_MISMATCH"
        asserted_transformation = str(
            lineage.get("transformation") or ""
        ).strip()
        if (
            asserted_transformation
            and asserted_transformation != spec.transformation
        ):
            return "FIELD_LINEAGE_TRANSFORMATION_MISMATCH"
    validation = (
        lineage.get("validation")
        if isinstance(lineage.get("validation"), dict)
        else {}
    )
    validation_statuses = {
        str(item).strip().upper()
        for item in (
            validation.get("status"),
            lineage.get("validation_status"),
            lineage.get("verification_status"),
        )
        if item not in (None, "")
    }
    accepted_validation_states = {
        "ACCEPTED",
        "APPROVED",
        "DETERMINISTIC_VERIFIED",
        "FIELD_LEVEL_VALIDATED",
        "PASSED",
        "VALID",
        "VERIFIED",
    }
    if not validation_statuses:
        return "FIELD_LINEAGE_VALIDATION_NOT_PROVEN"
    if not validation_statuses <= accepted_validation_states:
        return "FIELD_LINEAGE_VALIDATION_NOT_ACCEPTED"
    freshness_states = _event_lineage_freshness_states(lineage)
    if not freshness_states:
        return "FIELD_LINEAGE_FRESHNESS_NOT_PROVEN"
    if not freshness_states <= {
        "CURRENT",
        "CURRENT_LATEST_OFFICIAL_RELEASE",
        "CURRENT_RELEASE",
        "FRESH",
        "VALID",
    }:
        return "FIELD_LINEAGE_CONTENT_NOT_CURRENT"
    if not _event_lineage_has_timestamp(
        lineage,
        ("content_valid_until", "valid_until"),
    ):
        return "FIELD_LINEAGE_CONTENT_VALIDITY_NOT_PROVEN"
    if not _event_lineage_has_timestamp(
        lineage,
        ("refresh_due_at", "next_refresh_at", "next_refresh"),
    ):
        return "FIELD_LINEAGE_REFRESH_DUE_NOT_PROVEN"
    if output_field == "actual" and spec is not None:
        expected_provider = _source_identity(spec.provider_id)
        observed_provider = _source_identity(lineage.get("source"))
        if not observed_provider:
            return "FIELD_LINEAGE_SOURCE_NOT_PROVEN"
        if observed_provider != expected_provider:
            return "FIELD_LINEAGE_SOURCE_PROVIDER_MISMATCH"
        acquisition_provider = str(
            lineage.get("acquisition_provider")
            or spec.provider_id
        ).strip().upper()
        source_url = lineage.get("source_url")
        if source_url in (None, ""):
            return "FIELD_LINEAGE_SOURCE_URL_NOT_PROVEN"
        if not _registered_provider_source_url(
            acquisition_provider,
            source_url,
        ):
            return "FIELD_LINEAGE_SOURCE_URL_MISMATCH"
        if _source_identity(acquisition_provider) != expected_provider:
            originator_url = (
                lineage.get("canonical_url")
                or lineage.get("source_originator_url")
            )
            if originator_url in (None, ""):
                return "FIELD_LINEAGE_ORIGINATOR_URL_NOT_PROVEN"
            if not _registered_provider_source_url(
                spec.provider_id,
                originator_url,
            ):
                return "FIELD_LINEAGE_ORIGINATOR_URL_MISMATCH"
        source_domain = lineage.get("source_domain")
        if (
            source_domain not in (None, "")
            and not _registered_provider_source_domain(
                acquisition_provider,
                source_url=source_url,
                source_domain=source_domain,
            )
        ):
            return "FIELD_LINEAGE_SOURCE_DOMAIN_MISMATCH"
        source_series_id = str(
            lineage.get("source_series_id") or ""
        ).strip()
        if not source_series_id:
            return "FIELD_LINEAGE_SOURCE_SERIES_NOT_PROVEN"
        if source_series_id != spec.source_series_id:
            return "FIELD_LINEAGE_SOURCE_SERIES_MISMATCH"
        transformation = str(
            lineage.get("transformation") or ""
        ).strip()
        if not transformation:
            return "FIELD_LINEAGE_TRANSFORMATION_NOT_PROVEN"
        if transformation != spec.transformation:
            return "FIELD_LINEAGE_TRANSFORMATION_MISMATCH"
    return None


def _previous_reference_period(
    current_period: str,
    *,
    frequency: str,
) -> str | None:
    if frequency == "monthly":
        match = re.fullmatch(r"(20\d{2})-(0[1-9]|1[0-2])", current_period)
        if not match:
            return None
        year = int(match.group(1))
        month = int(match.group(2)) - 1
        if month == 0:
            year -= 1
            month = 12
        return f"{year:04d}-{month:02d}"
    if frequency == "quarterly":
        match = re.fullmatch(r"(20\d{2})-Q([1-4])", current_period)
        if not match:
            return None
        year = int(match.group(1))
        quarter = int(match.group(2)) - 1
        if quarter == 0:
            year -= 1
            quarter = 4
        return f"{year:04d}-Q{quarter}"
    if frequency in {"daily", "weekly"}:
        parsed = parse_datetime(current_period)
        if parsed is None:
            return None
        previous = _utc(parsed) - timedelta(
            days=7 if frequency == "weekly" else 1
        )
        return previous.date().isoformat()
    return None


def _assess_event_lineage_candidates(
    candidates: list[dict[str, Any]],
    *,
    selected_value: Any,
    now: datetime,
    require_all_consistent: bool,
) -> tuple[list[dict[str, Any]], str | None]:
    if require_all_consistent:
        rejected = [
            reason
            for item in candidates
            if (reason := _event_lineage_rejection_reason(item, now=now))
            is not None
        ]
        if rejected:
            return [], _worst_event_lineage_reason(rejected)
        explicit_values = [
            evidence_value
            for item in candidates
            if (evidence_value := _event_lineage_value(item))
            is not _MISSING
        ]
        if explicit_values and not all(
            _event_values_equal(item, selected_value)
            for item in explicit_values
        ):
            return [], "FIELD_LINEAGE_VALUE_NOT_RECONCILED"
        return candidates, None

    matching: list[dict[str, Any]] = []
    unspecified: list[dict[str, Any]] = []
    explicit_value_seen = False
    for item in candidates:
        evidence_value = _event_lineage_value(item)
        if evidence_value is _MISSING:
            unspecified.append(item)
            continue
        explicit_value_seen = True
        if _event_values_equal(evidence_value, selected_value):
            matching.append(item)
    if matching:
        relevant = [*matching, *unspecified]
    elif explicit_value_seen:
        return [], "FIELD_LINEAGE_VALUE_NOT_RECONCILED"
    else:
        relevant = unspecified
    rejected = [
        reason
        for item in relevant
        if (reason := _event_lineage_rejection_reason(item, now=now))
        is not None
    ]
    if rejected:
        return [], _worst_event_lineage_reason(rejected)
    return relevant, None


_MISSING = object()

_EVENT_LINEAGE_VALUE_FIELD_KEYS = frozenset(
    {
        "actual",
        "consensus",
        "forecast",
        "previous",
        "previous_revised",
        "revised_previous",
    }
)


def _event_lineage_value(value: dict[str, Any]) -> Any:
    if "value" in value and value.get("value") not in (None, ""):
        return value["value"]
    declared_field = str(value.get("field") or "").strip().lower()
    if (
        declared_field in _EVENT_LINEAGE_VALUE_FIELD_KEYS
        and value.get(declared_field) not in (None, "")
    ):
        return value[declared_field]
    return _MISSING


def _event_lineage_value_matches(
    value: dict[str, Any],
    expected: Any,
) -> bool:
    observed = _event_lineage_value(value)
    return (
        observed is not _MISSING
        and _event_values_equal(observed, expected)
    )


def _event_values_equal(left: Any, right: Any) -> bool:
    left_number = _number(left)
    right_number = _number(right)
    if left_number is not None and right_number is not None:
        return left_number == right_number
    return left == right


def _event_lineage_rejection_reason(
    value: dict[str, Any],
    *,
    now: datetime,
) -> str | None:
    reasons: list[str] = []
    states = _event_lifecycle_states(value)
    invalid_lifecycle_states = (
        _INVALID_LIFECYCLE_STATES
        | {"EXHAUSTED_NO_DATA"}
    )
    if "REJECTED_FUTURE" in states:
        reasons.append("FIELD_LINEAGE_REJECTED_FUTURE")
    if states & invalid_lifecycle_states:
        reasons.append("FIELD_LINEAGE_CONTENT_NOT_CURRENT")

    validation = (
        value.get("validation")
        if isinstance(value.get("validation"), dict)
        else {}
    )
    validation_statuses = {
        str(item).strip().upper()
        for item in (
            validation.get("status"),
            value.get("validation_status"),
            value.get("verification_status"),
        )
        if item not in (None, "")
    }
    accepted_validation_states = {
        "ACCEPTED",
        "APPROVED",
        "DETERMINISTIC_VERIFIED",
        "FIELD_LEVEL_VALIDATED",
        "PASSED",
        "VALID",
        "VERIFIED",
    }
    if (
        validation_statuses
        and not validation_statuses <= accepted_validation_states
    ):
        reasons.append("FIELD_LINEAGE_VALIDATION_NOT_ACCEPTED")

    lifecycle = _event_lifecycle_container(value)
    for container, field in (
        (value, "content_valid_until"),
        (value, "valid_until"),
        (lifecycle, "content_valid_until"),
        (lifecycle, "valid_until"),
    ):
        raw_timestamp = container.get(field)
        if raw_timestamp in (None, ""):
            continue
        content_valid_until = parse_datetime(raw_timestamp)
        if content_valid_until is None:
            reasons.append("FIELD_LINEAGE_TIMESTAMP_INVALID")
            continue
        if _utc(content_valid_until) <= now:
            reasons.append("FIELD_LINEAGE_CONTENT_VALIDITY_EXPIRED")

    for field in ("data_as_of", "observed_at"):
        raw_timestamp = value.get(field)
        if raw_timestamp in (None, ""):
            continue
        data_as_of = parse_datetime(raw_timestamp)
        if data_as_of is None:
            reasons.append("FIELD_LINEAGE_TIMESTAMP_INVALID")
            continue
        if (
            _utc(data_as_of) > now + timedelta(minutes=5)
        ):
            reasons.append("FIELD_LINEAGE_REJECTED_FUTURE")

    for container, field in (
        (value, "refresh_due_at"),
        (value, "next_refresh_at"),
        (lifecycle, "refresh_due_at"),
        (lifecycle, "next_refresh_at"),
        (lifecycle, "next_refresh"),
    ):
        raw_timestamp = container.get(field)
        if raw_timestamp in (None, ""):
            continue
        refresh_due_at = parse_datetime(raw_timestamp)
        if refresh_due_at is None:
            reasons.append("FIELD_LINEAGE_TIMESTAMP_INVALID")
            continue
        if _utc(refresh_due_at) <= now:
            reasons.append("FIELD_LINEAGE_REFRESH_DUE")
    return _worst_event_lineage_reason(reasons) if reasons else None


def _worst_event_lineage_reason(reasons: Iterable[str]) -> str:
    priority = {
        "FIELD_LINEAGE_REJECTED_FUTURE": 0,
        "FIELD_LINEAGE_VALIDATION_NOT_ACCEPTED": 1,
        "FIELD_LINEAGE_VALIDATION_NOT_PROVEN": 2,
        "FIELD_LINEAGE_FRESHNESS_NOT_PROVEN": 3,
        "FIELD_LINEAGE_CONTENT_VALIDITY_NOT_PROVEN": 4,
        "FIELD_LINEAGE_REFRESH_DUE_NOT_PROVEN": 5,
        "FIELD_LINEAGE_TIMESTAMP_INVALID": 6,
        "FIELD_LINEAGE_CONTENT_VALIDITY_EXPIRED": 7,
        "FIELD_LINEAGE_REFRESH_DUE": 8,
        "FIELD_LINEAGE_CONTENT_NOT_CURRENT": 9,
        "FIELD_LINEAGE_VALUE_NOT_RECONCILED": 10,
        "OCCURRENCE_FIELD_LINEAGE_MISMATCH": 11,
        "REFERENCE_PERIOD_FIELD_LINEAGE_MISMATCH": 12,
        "FIELD_LINEAGE_METRIC_MISMATCH": 13,
        "FIELD_LINEAGE_FREQUENCY_MISMATCH": 14,
        "FIELD_LINEAGE_OCCURRENCE_NOT_PROVEN": 15,
        "FIELD_LINEAGE_METRIC_NOT_PROVEN": 16,
        "EVENT_REFERENCE_PERIOD_NOT_PROVEN": 17,
        "FIELD_LINEAGE_REFERENCE_PERIOD_NOT_PROVEN": 18,
        "FIELD_LINEAGE_FREQUENCY_NOT_PROVEN": 19,
        "FIELD_LINEAGE_VALUE_NOT_PROVEN": 20,
        "FIELD_LINEAGE_SOURCE_NOT_PROVEN": 21,
        "FIELD_LINEAGE_SOURCE_URL_NOT_PROVEN": 22,
        "FIELD_LINEAGE_SOURCE_URL_MISMATCH": 23,
        "FIELD_LINEAGE_ORIGINATOR_URL_NOT_PROVEN": 24,
        "FIELD_LINEAGE_ORIGINATOR_URL_MISMATCH": 25,
        "FIELD_LINEAGE_SOURCE_DOMAIN_MISMATCH": 26,
        "FIELD_LINEAGE_SOURCE_SERIES_NOT_PROVEN": 27,
        "FIELD_LINEAGE_TRANSFORMATION_NOT_PROVEN": 28,
        "FIELD_SPECIFIC_LINEAGE_NOT_AVAILABLE": 29,
    }
    return min(
        (str(reason) for reason in reasons),
        key=lambda reason: (priority.get(reason, 99), reason),
    )


def _datum_has_value(value: dict[str, Any]) -> bool:
    scalar_keys = (
        "actual",
        "price",
        "market_session_status",
        "iv_atm",
    )
    if any(value.get(key) not in (None, "") for key in scalar_keys):
        return True
    if "value" in value:
        return value.get("value") not in (None, "")
    return any(
        isinstance(item, (list, dict)) and bool(item)
        for key, item in value.items()
        if key
        not in {
            "warnings",
            "errors",
            "validation",
            "sync",
            "lifecycle",
            "source",
            "lineage",
        }
    )


def _worst_state(values: set[str]) -> str:
    for item in ("REJECTED_FUTURE", "EXPIRED", "VERY_STALE", "STALE"):
        if item in values:
            return item
    return "UNAVAILABLE"


def _section_status(valid: int, total: int) -> str:
    if valid <= 0:
        return "UNAVAILABLE"
    if valid < max(total, 1):
        return "PARTIAL"
    return "AVAILABLE"


def _section_reason(status: str) -> str | None:
    return {
        "PARTIAL": "ONE_OR_MORE_VALUES_UNAVAILABLE",
        "UNAVAILABLE": "NO_VALID_VALUES_AVAILABLE",
    }.get(status)


def _analytic_section_value_count(value: Any) -> int:
    count = 0
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {
                "status",
                "freshness",
                "data_as_of",
                "content_valid_until",
                "snapshot_transport_valid_until",
                "refresh_due_at",
                "source",
                "reason_code",
                "lineage",
                "release_at",
                "reference_period",
                "request_id",
            }:
                continue
            if key in {
                "value",
                "price",
                "actual",
                "consensus",
                "market_session_status",
                "action",
                "target_range_lower",
                "target_range_upper",
                "risk_sentiment",
                "risk_score",
                "iv_atm",
            } and item is not None:
                count += 1
            elif isinstance(item, (dict, list)):
                count += _analytic_section_value_count(item)
            elif key in {"occurrence_id", "event_at"} and item not in {None, ""}:
                count += 1
            elif (
                isinstance(item, (int, float))
                and not isinstance(item, bool)
                and key
                not in {
                    "snapshot_revision",
                    "payload_size_bytes",
                    "available_section_count",
                    "degraded_section_count",
                    "unavailable_section_count",
                    "section_count",
                    "coverage_ratio",
                }
            ):
                count += 1
    elif isinstance(value, list):
        count += sum(_analytic_section_value_count(item) for item in value)
    return count


def _invalid_state_count(value: Any) -> int:
    count = 0
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"freshness", "status"} and str(item).upper() in INVALID_ANALYTIC_STATES:
                count += 1
            count += _invalid_state_count(item)
    elif isinstance(value, list):
        count += sum(_invalid_state_count(item) for item in value)
    return count


def _available_without_substantive_value_count(value: Any) -> int:
    if not isinstance(value, dict):
        return 0
    count = 0
    for section_name in SECTION_NAMES:
        section = value.get(section_name)
        if not isinstance(section, dict):
            continue
        explicit_nulls = _explicit_available_null_count(section)
        datasets = [
            policy.dataset_id
            for policy in DATASET_POLICIES
            if policy.section == section_name
        ]
        section_declared_available_without_value = bool(
            str(section.get("status") or "").upper()
            == "AVAILABLE"
            and datasets
            and not any(
                _substantive_delivery_present(
                    dataset_id,
                    _dataset_delivery_value(
                        dataset_id,
                        value,
                    )[1],
                )
                for dataset_id in datasets
            )
        )
        count += (
            explicit_nulls
            if explicit_nulls
            else int(section_declared_available_without_value)
        )
    return count


def _explicit_available_null_count(value: Any) -> int:
    if isinstance(value, dict):
        count = int(
            str(value.get("status") or "").upper() == "AVAILABLE"
            and "value" in value
            and value.get("value") is None
        )
        return count + sum(
            _explicit_available_null_count(item)
            for item in value.values()
        )
    if isinstance(value, list):
        return sum(
            _explicit_available_null_count(item)
            for item in value
        )
    return 0


def _selected_value_presence_mismatch_count(
    rows: Any,
    *,
    payload_root: dict[str, Any] | None = None,
) -> int:
    if not isinstance(rows, list):
        return 0
    mismatches = 0
    for item in rows:
        if (
            not isinstance(item, dict)
            or not isinstance(
                item.get("selected_value_present"),
                bool,
            )
        ):
            continue
        dataset_id = str(item.get("dataset_id") or "")
        resolved = _resolve_accounting_delivery(
            dataset_id,
            item.get("delivered_value"),
            payload_root=payload_root,
        )
        if resolved is _INVALID_DELIVERY_REFERENCE:
            mismatches += 1
            continue
        if item["selected_value_present"] != (
            _substantive_delivery_present(dataset_id, resolved)
        ):
            mismatches += 1
    return mismatches


def _duplicate_large_collection_count(rows: Any) -> int:
    if not isinstance(rows, list):
        return 0
    count = 0
    for item in rows:
        if not isinstance(item, dict):
            continue
        delivered = item.get("delivered_value")
        if (
            isinstance(delivered, list)
            and not _is_delivery_collection_reference(delivered)
        ):
            count += 1
    return count


def _expired_or_future_delivered_value_count(
    value: Any,
    *,
    now: datetime,
) -> int:
    count = 0
    if isinstance(value, dict):
        delivered = _analytic_section_value_count(value) > 0
        content_until = parse_datetime(value.get("content_valid_until"))
        data_as_of = parse_datetime(value.get("data_as_of"))
        if delivered and content_until and _utc(content_until) < now:
            count += 1
        if (
            delivered
            and data_as_of
            and _utc(data_as_of) > now + timedelta(minutes=5)
        ):
            count += 1
        count += sum(
            _expired_or_future_delivered_value_count(item, now=now)
            for item in value.values()
        )
    elif isinstance(value, list):
        count += sum(
            _expired_or_future_delivered_value_count(item, now=now)
            for item in value
        )
    return count


def _earnings_policy_error_count(
    section: Any,
    *,
    now: datetime,
) -> int:
    if not isinstance(section, dict):
        return 1
    policy = MNQ_EARNINGS_SELECTION_POLICY
    declared = (
        section.get("selection_policy")
        if isinstance(section.get("selection_policy"), dict)
        else {}
    )
    window = (
        declared.get("window")
        if isinstance(declared.get("window"), dict)
        else {}
    )
    expected_window_start = (
        now.date() - timedelta(days=policy.lookback_days)
    ).isoformat()
    expected_window_end = (
        now.date() + timedelta(days=policy.lookahead_days)
    ).isoformat()
    errors = int(
        declared.get("policy_id") != policy.policy_id
        or declared.get("primary_symbols")
        != list(policy.primary_symbols)
        or declared.get("include_nasdaq_100_components")
        is not policy.include_nasdaq_100_components
        or declared.get("sort_fields") != list(policy.sort_fields)
        or declared.get("max_events") != policy.max_events
        or declared.get("minimum_fields")
        != list(policy.minimum_fields)
        or window.get("lookback_days") != policy.lookback_days
        or window.get("lookahead_days") != policy.lookahead_days
        or window.get("max_observation_age_hours")
        != policy.max_observation_age_hours
        or window.get("start_date") != expected_window_start
        or window.get("end_date") != expected_window_end
    )
    events = section.get("events")
    if not isinstance(events, list) or any(
        not isinstance(item, dict) for item in events
    ):
        return errors + 1
    total = section.get("total_available")
    relevant = section.get("relevant_count")
    delivered = section.get("delivered_count")
    excluded = section.get("excluded_count")
    counts_valid = bool(
        all(
            type(value) is int and value >= 0
            for value in (
                total,
                relevant,
                delivered,
                excluded,
            )
        )
        and total >= relevant >= delivered
        and delivered == len(events)
        and delivered <= policy.max_events
        and excluded == total - delivered
    )
    if not counts_valid:
        errors += 1
    exclusions = (
        section.get("exclusion_counts")
        if isinstance(section.get("exclusion_counts"), dict)
        else {}
    )
    if (
        set(exclusions)
        != {
            "outside_window",
            "outside_universe",
            "bounded_limit",
            "provider_prefiltered_or_invalid",
        }
        or any(type(value) is not int or value < 0 for value in exclusions.values())
        or (
            type(excluded) is int
            and sum(exclusions.values()) != excluded
        )
    ):
        errors += 1
    identities: list[tuple[str, str]] = []
    for item in events:
        symbol = str(item.get("symbol") or "")
        event_date = _date_value(item.get("event_date"))
        if (
            symbol not in policy.primary_symbols
            or event_date is None
            or not (
                now.date() - timedelta(days=policy.lookback_days)
                <= event_date
                <= now.date() + timedelta(days=policy.lookahead_days)
            )
        ):
            errors += 1
        lifecycle = _earnings_freshness(item, now=now)
        if (
            lifecycle["deliverable"] is not True
            or item.get("freshness") != "CURRENT"
            or item.get("data_as_of") != lifecycle["data_as_of"]
            or item.get("content_valid_until")
            != lifecycle["content_valid_until"]
            or item.get("refresh_due_at")
            != lifecycle["refresh_due_at"]
        ):
            errors += 1
        identities.append(
            (
                event_date.isoformat() if event_date else "",
                symbol,
            )
        )
    if identities != sorted(identities) or len(identities) != len(
        set(identities)
    ):
        errors += 1
    expected_coverage = _earnings_coverage(events)
    if section.get("coverage") != expected_coverage:
        errors += 1
    bounded_count = (
        int(exclusions.get("bounded_limit") or 0)
        if exclusions
        else 0
    )
    expected_status, expected_reason = _earnings_section_status(
        expected_coverage,
        bounded_count=bounded_count,
    )
    if (
        section.get("status") != expected_status
        or section.get("reason_code") != expected_reason
    ):
        errors += 1
    return errors


def _earnings_temporal_error_count(section: Any) -> int:
    if not isinstance(section, dict):
        return 1
    errors = 0
    for item in section.get("events") or []:
        if not isinstance(item, dict):
            errors += 1
            continue
        precision = str(item.get("temporal_precision") or "")
        event_date = _date_value(item.get("event_date"))
        timing = str(item.get("timing") or "")
        if precision == "DATE_ONLY":
            if (
                event_date is None
                or item.get("event_at") is not None
                or timing != "UNKNOWN"
            ):
                errors += 1
            continue
        if precision == "EXACT":
            event_at = _time_bearing_datetime(item.get("event_at"))
            if (
                event_at is None
                or event_date is None
                or _utc(event_at).date() != event_date
                or timing
                not in {"UNKNOWN", "BEFORE_MARKET", "AFTER_CLOSE"}
            ):
                errors += 1
            continue
        errors += 1
    return errors


def _semantic_error_count(macro: dict[str, Any]) -> int:
    by_series = {
        str(item.get("series_id")): item
        for item in macro.get("metrics") or []
        if isinstance(item, dict)
    }
    errors = 0
    for series_id, expected in MACRO_SEMANTICS.items():
        item = by_series.get(series_id)
        if item is None:
            continue
        for key in ("metric_id", "unit", "transformation"):
            if item.get(key) != expected[key]:
                errors += 1
    return errors


def _calendar_semantic_error_count(
    calendar: dict[str, Any],
    *,
    now: datetime,
) -> int:
    invalid_occurrences: set[tuple[str, ...]] = set()
    for list_name in (
        "active_event_windows",
        "next_24h_events",
        "next_7d_high_impact_events",
        "latest_released_events",
    ):
        for event in calendar.get(list_name) or []:
            if not isinstance(event, dict):
                continue
            identity = _calendar_event_identity(event)
            if (
                _event_lineage_rejection_reason(event, now=now)
                is not None
                and any(
                    event.get(field) not in (None, "")
                    for field in (
                        "actual",
                        "consensus",
                        "previous",
                        "previous_revised",
                    )
                )
            ):
                invalid_occurrences.add(identity)
                continue
            metric_id = str(event.get("metric_id") or "").strip().lower()
            if metric_semantics_mismatch_reason(
                metric_id,
                name=event.get("name") or event.get("event_name"),
                frequency_hint=" ".join(
                    str(item or "")
                    for item in (
                        event.get("frequency"),
                        event.get("evaluation_method"),
                    )
                ),
            ):
                invalid_occurrences.add(identity)
                continue
            requires_official_evidence = (
                _requires_official_event_evidence(
                    event,
                    metric_id,
                )
            )
            if (
                requires_official_evidence
                and metric_id not in OFFICIAL_METRICS
            ):
                if any(
                    event.get(field) not in (None, "")
                    for field in (
                        "actual",
                        "consensus",
                        "previous",
                        "previous_revised",
                    )
                ):
                    invalid_occurrences.add(identity)
                continue
            if metric_id not in OFFICIAL_METRICS:
                continue
            release = _event_release(event)
            event_lineage = _field_lineage(event)
            for output_field, aliases in (
                ("actual", {"actual"}),
                ("consensus", {"consensus", "forecast"}),
                ("previous", {"previous"}),
                (
                    "previous_revised",
                    {"previous_revised", "revised_previous"},
                ),
            ):
                selected_value = event.get(output_field)
                if selected_value in (None, ""):
                    continue
                candidates = [
                    item
                    for item in event_lineage
                    if str(item.get("field") or "").strip().lower()
                    in aliases
                ]
                matched_candidates = [
                    item
                    for item in candidates
                    if (
                        _event_lineage_rejection_reason(item, now=now)
                        is None
                        and _event_field_lineage_binding_reason(
                            item,
                            output_field=output_field,
                            selected_value=selected_value,
                            expected_occurrence_ids=(
                                _event_expected_occurrence_ids(
                                    event
                                )
                            ),
                            metric_id=metric_id,
                            reference_period=event.get(
                                "reference_period"
                            ),
                            release=release,
                        )
                        is None
                    )
                ]
                if not matched_candidates:
                    invalid_occurrences.add(identity)
                    break
                if (
                    output_field == "actual"
                    and not _event_actual_source_status_proven(
                        event,
                        metric_id=metric_id,
                        lineage=matched_candidates,
                    )
                ):
                    invalid_occurrences.add(identity)
                    break
    return len(invalid_occurrences)


def _calendar_event_identity(event: dict[str, Any]) -> tuple[str, ...]:
    occurrence_id = event.get("occurrence_id")
    if occurrence_id not in (None, ""):
        return ("occurrence", str(occurrence_id))
    return (
        "event",
        str(event.get("metric_id") or ""),
        str(event.get("release_at") or ""),
        str(event.get("reference_period") or ""),
        str(event.get("name") or event.get("event_name") or ""),
    )


def _event_lineage_source_matches(
    lineage: dict[str, Any],
    expected_source: Any,
) -> bool:
    expected = _source_identity(expected_source)
    if not expected:
        return False
    observed = {
        _source_identity(lineage.get(field))
        for field in (
            "source",
            "publisher",
            "originator",
            "source_originator",
            "acquisition_provider",
            "distributor",
        )
        if lineage.get(field) not in (None, "")
    }
    return expected in observed


def _event_actual_source_status_proven(
    event: dict[str, Any],
    *,
    metric_id: str,
    lineage: Iterable[dict[str, Any]],
) -> bool:
    actual_source = (
        event.get("actual_source")
        or event.get("publisher")
    )
    if not actual_source:
        return False
    actual_lineage = [
        item
        for item in lineage
        if str(item.get("field") or "").strip().lower() == "actual"
    ]
    if event.get("actual_is_official") is True:
        spec = OFFICIAL_METRICS.get(metric_id)
        return bool(
            spec is not None
            and _source_identity(actual_source)
            == _source_identity(spec.provider_id)
            and any(
                _event_lineage_source_matches(item, actual_source)
                and _source_identity(
                    item.get("acquisition_provider")
                    or spec.provider_id
                )
                == _source_identity(spec.provider_id)
                for item in actual_lineage
            )
        )
    if (
        event.get("actual_is_official") is not False
        or metric_id != "flash_services_pmi"
    ):
        return False
    policy = dataset_policy_by_id("flash_services_pmi")
    observed_source = _source_identity(actual_source)
    allowed_fallbacks = {
        _source_identity(provider_id)
        for provider_id in policy.fallback_providers
    }
    return bool(
        observed_source in allowed_fallbacks
        and any(
            _source_identity(item.get("acquisition_provider"))
            == observed_source
            and _source_identity(
                item.get("source") or item.get("publisher")
            )
            == _source_identity(
                OFFICIAL_METRICS[metric_id].provider_id
            )
            for item in actual_lineage
        )
    )


def _source_identity(value: Any) -> str:
    return re.sub(
        r"[^a-z0-9]+",
        "",
        str(value or "").casefold(),
    )


def _post_release_probability_count(fomc: dict[str, Any], now: datetime) -> int:
    release = parse_datetime(fomc.get("release_at"))
    if not release or now < _utc(release):
        return 0
    return len(fomc.get("pre_meeting_probabilities") or [])


def _stale_presented_current(value: Any) -> int:
    count = 0
    if isinstance(value, dict):
        freshness = str(value.get("freshness") or "").upper()
        reason = str(value.get("reason_code") or "").upper()
        if freshness in VALID_FRESHNESS_STATES and any(
            token in reason for token in INVALID_ANALYTIC_STATES
        ):
            count += 1
        count += sum(_stale_presented_current(item) for item in value.values())
    elif isinstance(value, list):
        count += sum(_stale_presented_current(item) for item in value)
    return count


def _provider_accounting_valid(
    rows: Any,
    *,
    request: dict[str, Any],
    require_request_id: bool,
    analytics: dict[str, Any],
    missing_data: list[dict[str, Any]],
) -> bool:
    if not isinstance(rows, list):
        return False
    request_id = request.get("request_id")
    correlation_id = request.get("accounting_correlation_id")
    request_started_at = request.get("accounting_request_started_at")
    request_completed_at = request.get("accounting_request_completed_at")
    evidence_origin = request.get("accounting_evidence_origin")
    if (
        (require_request_id and not request_id)
        or not correlation_id
        or correlation_id != request_id
        or evidence_origin != "NORMAL_APPLICATION_REQUEST"
        or not parse_datetime(request_started_at)
        or not parse_datetime(request_completed_at)
    ):
        return False
    expected = {policy.dataset_id for policy in DATASET_POLICIES}
    observed = {
        str(item.get("dataset_id"))
        for item in rows
        if isinstance(item, dict)
    }
    if (
        expected != observed
        or len(rows) != len(expected)
        or not _shared_acquisition_links_valid(
            rows,
            governed_dataset_ids=expected,
        )
    ):
        return False
    for item in rows:
        if not _request_accounting_row_complete(
            item,
            request_id=request_id,
            correlation_id=correlation_id,
            request_started_at=request_started_at,
            request_completed_at=request_completed_at,
            payload_root={"analytics": analytics},
        ):
            return False
        expected_delivery = _delivery_evidence(
            str(item.get("dataset_id")),
            analytics=analytics,
            missing_data=missing_data,
        )
        if any(
            item.get(key) != expected_delivery.get(key)
            for key in (
                "selected_source",
                "selected_value_present",
                "delivered_value",
                "payload_freshness",
                "delivery_missing_reason_codes",
                "reason_code",
            )
        ):
            return False
    return True


def _request_accounting_row_complete(
    item: Any,
    *,
    request_id: Any,
    correlation_id: Any,
    request_started_at: Any,
    request_completed_at: Any,
    payload_root: dict[str, Any] | None = None,
) -> bool:
    required = {
        "dataset_id",
        "request_id",
        "correlation_id",
        "evidence_origin",
        "evidence_status",
        "observed_at",
        "acquisition_id",
        "shared_acquisition_dataset_ids",
        "database_lookup_performed",
        "database_lookup_reason",
        "database_record_found",
        "database_data_as_of",
        "database_content_valid_until",
        "database_refresh_due_at",
        "database_lifecycle_status",
        "database_record_expired",
        "database_freshness_evaluation",
        "primary_provider",
        "fallbacks",
        "acquisition_selected_source",
        "acquisition_reason_code",
        "selected_source",
        "selected_value_present",
        "delivered_value",
        "payload_freshness",
        "delivery_missing_reason_codes",
        "reason_code",
    }
    if (
        not isinstance(item, dict)
        or not required <= set(item)
        or not item.get("reason_code")
        or not item.get("acquisition_reason_code")
        or item.get("request_id") != request_id
        or item.get("correlation_id") != correlation_id
        or item.get("evidence_origin") != "NORMAL_APPLICATION_REQUEST"
        or item.get("evidence_status") != "COMPLETE"
        or type(item.get("database_lookup_performed")) is not bool
        or not isinstance(item.get("selected_value_present"), bool)
        or not isinstance(
            item.get("delivery_missing_reason_codes"),
            list,
        )
        or str(item.get("reason_code") or "").upper()
        in {"DB_VALID_REUSED", "SOURCE_SELECTED_FROM_SAME_REQUEST"}
    ):
        return False
    dataset_id = str(item.get("dataset_id") or "")
    resolved_delivery = _resolve_accounting_delivery(
        dataset_id,
        item.get("delivered_value"),
        payload_root=payload_root,
    )
    if (
        resolved_delivery is _INVALID_DELIVERY_REFERENCE
        or item.get("selected_value_present")
        != _substantive_delivery_present(
            dataset_id,
            resolved_delivery,
        )
    ):
        return False
    observed_at = parse_datetime(item.get("observed_at"))
    started_at = parse_datetime(request_started_at)
    completed_at = parse_datetime(request_completed_at)
    if (
        not observed_at
        or not started_at
        or not completed_at
        or _utc(started_at) > _utc(completed_at)
        or _utc(observed_at) < _utc(started_at)
        or _utc(observed_at) > _utc(completed_at)
    ):
        return False
    policy = next(
        (
            policy
            for policy in DATASET_POLICIES
            if policy.dataset_id == item.get("dataset_id")
        ),
        None,
    )
    if policy is None:
        return False
    if (
        not item.get("acquisition_id")
        or policy.dataset_id
        not in (item.get("shared_acquisition_dataset_ids") or [])
        or not item.get("database_lookup_reason")
    ):
        return False
    if (
        policy.canonical_repository_required
        and item.get("database_lookup_performed") is not True
    ):
        return False
    primary = item.get("primary_provider")
    fallbacks = item.get("fallbacks")
    attempts = [primary, *fallbacks] if isinstance(fallbacks, list) else []
    if not isinstance(primary, dict) or len(attempts) != len(fallbacks or []) + 1:
        return False
    if primary.get("provider") != policy.primary_provider:
        return False
    if [
        attempt.get("provider")
        for attempt in fallbacks
        if isinstance(attempt, dict)
    ] != list(policy.fallback_providers):
        return False
    for attempt in attempts:
        if not isinstance(attempt, dict):
            return False
        if not {
            "provider",
            "called",
            "attempts",
            "result",
            "execution_origin",
        } <= set(attempt):
            return False
        if not _provider_attempt_complete(attempt):
            return False
    if not item.get("database_freshness_evaluation"):
        return False
    capability_scoped = (
        str(policy.provider_strategy).upper() == "FAN_IN"
        and bool(item.get("capability_acquisitions"))
    )
    if capability_scoped:
        if (
            item["database_lookup_performed"] is not True
            or item.get("database_record_found") is not None
            or item.get("database_record_expired") is not None
            or item.get("database_data_as_of") is not None
            or item.get("database_content_valid_until") is not None
            or item.get("database_refresh_due_at") is not None
            or item.get("database_lifecycle_status") is not None
            or item.get("database_freshness_evaluation")
            != "CAPABILITY_SCOPED"
        ):
            return False
    elif item["database_lookup_performed"]:
        if (
            type(item.get("database_record_found")) is not bool
            or type(item.get("database_record_expired")) is not bool
        ):
            return False
        if item["database_record_found"] and (
            not item.get("database_data_as_of")
            or not item.get("database_content_valid_until")
            or not item.get("database_refresh_due_at")
        ):
            return False
        if not _canonical_database_evidence_valid(
            item,
            policy=policy,
            observed_at=observed_at,
            request_started_at=started_at,
        ):
            return False
    elif any(
        item.get(key) is not None
        for key in (
            "database_record_found",
            "database_data_as_of",
            "database_content_valid_until",
            "database_refresh_due_at",
            "database_lifecycle_status",
            "database_record_expired",
        )
    ) or item.get("database_freshness_evaluation") != "NOT_LOOKED_UP":
        return False
    return bool(
        _provider_flow_valid(
            item,
            policy=policy,
            governed_dataset_ids={
                registered.dataset_id
                for registered in DATASET_POLICIES
            },
        )
        and _acquisition_delivery_observation_link_complete(
            item,
            payload_root=payload_root,
        )
        and item.get("payload_freshness")
        and not (
            item["selected_value_present"]
            and not item.get("selected_source")
        )
    )


def _duplicate_count(items: list[Any]) -> int:
    keys: list[tuple[str, str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        keys.append(
            (
                str(item.get("metric_id") or ""),
                str(item.get("release_at") or ""),
                str(item.get("reference_period") or ""),
            )
        )
    return len(keys) - len(set(keys))


def _event_completeness(item: dict[str, Any]) -> int:
    return sum(
        item.get(key) is not None
        for key in ("actual", "consensus", "previous", "reference_period", "source")
    )


def _event_selection_rank(
    item: dict[str, Any],
    *,
    mode: str,
) -> tuple[int, int, int]:
    actual_lineage = any(
        str(lineage.get("field") or "").strip().lower() == "actual"
        for lineage in _field_lineage(item)
    )
    return (
        int(mode == "latest_release" and item.get("actual") is not None),
        int(item.get("actual") is not None and actual_lineage),
        _event_completeness(item),
    )


def _select_schedule_session(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {
            "status": None,
            "is_open": None,
            "next_open": None,
            "next_close": None,
            "reason_code": "SESSION_NOT_AVAILABLE",
        }
    return {
        "status": value.get("current_status")
        or value.get("session_state")
        or value.get("status"),
        "is_open": value.get("is_open"),
        "next_open": value.get("next_open") or value.get("next_open_at"),
        "next_close": value.get("next_close"),
        "reason_code": None,
    }


def _select_position_group(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        key: _number(value.get(key))
        for key in ("long", "short", "spreading", "net", "net_change_week")
        if value.get(key) is not None
    }


def _project_positioning_cot(
    section: dict[str, Any],
) -> dict[str, Any]:
    cot = (
        section.get("cot")
        if isinstance(section.get("cot"), dict)
        else {}
    )
    nasdaq = (
        cot.get("nasdaq_100")
        if isinstance(cot.get("nasdaq_100"), dict)
        else {}
    )
    return {
        "report_date": (
            nasdaq.get("report_date")
            or section.get("data_as_of")
        ),
        "publication_date": nasdaq.get("publication_date"),
        "contract_code": nasdaq.get(
            "cftc_contract_market_code"
        ),
        "open_interest": nasdaq.get("open_interest"),
        "asset_managers": _select_position_group(
            nasdaq.get("asset_managers")
        ),
        "leveraged_funds": _select_position_group(
            nasdaq.get("leveraged_funds")
        ),
        "dealers": _select_position_group(nasdaq.get("dealers")),
    }


def _selected_source(section: Any) -> Any:
    source = _find_first(section, "source")
    if isinstance(source, dict):
        return (
            source.get("acquisition_provider")
            or source.get("publisher")
            or source.get("distributor")
        )
    return source


def _find_first(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if _not_empty(value.get(key)):
            return value[key]
        for item in value.values():
            found = _find_first(item, key)
            if _not_empty(found):
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_first(item, key)
            if _not_empty(found):
                return found
    return None


def _not_empty(value: Any) -> bool:
    return value is not None and value != "" and value != [] and value != {}


def _contains_token(value: Any, token: str) -> bool:
    if isinstance(value, dict):
        return any(_contains_token(item, token) for item in value.values())
    if isinstance(value, list):
        return any(_contains_token(item, token) for item in value)
    return str(value).upper() == token.upper()


def _latest_value(values: Iterable[dict[str, Any]], key: str) -> Any:
    parsed: list[tuple[datetime, Any]] = []
    raw_values: list[Any] = []
    for item in values:
        value = item.get(key)
        if value in {None, ""}:
            continue
        raw_values.append(value)
        dt = parse_datetime(value)
        if dt:
            parsed.append((_utc(dt), value))
    if parsed:
        return max(parsed, key=lambda item: item[0])[1]
    return raw_values[-1] if raw_values else None


def _earliest_datetime_value(
    values: Iterable[dict[str, Any]],
    key: str,
) -> Any:
    parsed: list[tuple[datetime, Any]] = []
    for item in values:
        value = item.get(key)
        if value in {None, ""}:
            continue
        timestamp = parse_datetime(value)
        if timestamp:
            parsed.append((_utc(timestamp), value))
    return min(parsed, key=lambda item: item[0])[1] if parsed else None


def _latest_datetime_string(value: Any) -> str | None:
    values: list[datetime] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "data_as_of":
                parsed = parse_datetime(item)
                if parsed:
                    values.append(_utc(parsed))
            else:
                nested = _latest_datetime_string(item)
                parsed = parse_datetime(nested)
                if parsed:
                    values.append(_utc(parsed))
    elif isinstance(value, list):
        for item in value:
            nested = _latest_datetime_string(item)
            parsed = parse_datetime(nested)
            if parsed:
                values.append(_utc(parsed))
    return max(values).isoformat() if values else None


def _number(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(str(value).replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None


def _integer(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _close(left: Any, right: Any, tolerance: float = 0.02) -> bool:
    a, b = _number(left), _number(right)
    return bool(a is not None and b is not None and abs(a - b) <= tolerance)


def _datetime_value(value: Any) -> Any:
    if value in {None, ""}:
        return None
    parsed = parse_datetime(value)
    return _utc(parsed).isoformat() if parsed else value


def _nested_value(value: dict[str, Any], *keys: str) -> Any:
    current: Any = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _hash_identity(value: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
