from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from app.services.data_freshness_service import parse_datetime
from app.services.market_context_sync_service import extract_sync_sections
from app.services.official_actual_semantics import normalize_reference_period
from app.services.request_provider_accounting import (
    _canonical_database_evidence_valid,
    _provider_flow_valid,
)


CONTRACT_NAME = "SeniorAnalystPayloadV1"
SCHEMA_VERSION = "1.0"
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
REQUIRED_MEGA_CAPS = ("AAPL", "NVDA", "AMZN", "META", "TSLA", "AMD")
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


DATASET_POLICIES: tuple[DatasetPolicy, ...] = (
    DatasetPolicy(
        "nasdaq_100",
        "nasdaq",
        "intraday",
        timedelta(hours=12),
        "INVESCO",
        ("ALPHA_VANTAGE", "NASDAQ", "SEC"),
        provider_strategy="CASCADE",
    ),
    DatasetPolicy(
        "mega_cap_quotes",
        "nasdaq",
        "intraday",
        timedelta(hours=12),
        "YAHOO_FINANCE_CHART",
        (
            "STOOQ",
            "ALPHA_VANTAGE",
            "YAHOO_FINANCE_QUOTE",
        ),
    ),
    DatasetPolicy(
        "market_internals",
        "market_internals",
        "intraday",
        timedelta(hours=2),
        "TRADIER",
    ),
    DatasetPolicy("vix", "vix", "daily", timedelta(days=2), "FRED", ("CBOE",)),
    DatasetPolicy("vvix", "vix", "intraday", timedelta(hours=2), "CBOE"),
    DatasetPolicy("risk", "risk", "intraday", timedelta(hours=2), "CBOE"),
    DatasetPolicy("treasury_rates", "rates", "daily", timedelta(days=2), "FRED"),
    DatasetPolicy("fed_funds", "rates", "daily", timedelta(days=2), "FRED"),
    DatasetPolicy(
        "target_range",
        "fomc",
        "event",
        timedelta(days=45),
        "FEDERAL_RESERVE",
        ("FRED",),
    ),
    DatasetPolicy(
        "fomc_expectations",
        "fomc",
        "intraday",
        timedelta(hours=2),
        "INVESTING_FED_RATE_MONITOR",
    ),
    DatasetPolicy("cpi", "macro", "monthly", timedelta(days=45), "BLS"),
    DatasetPolicy("ppi", "macro", "monthly", timedelta(days=45), "BLS"),
    DatasetPolicy("pce", "macro", "monthly", timedelta(days=45), "BEA"),
    DatasetPolicy("gdp", "macro", "quarterly", timedelta(days=120), "BEA"),
    DatasetPolicy("employment", "macro", "monthly", timedelta(days=45), "BLS"),
    DatasetPolicy("wages", "macro", "monthly", timedelta(days=45), "BLS"),
    DatasetPolicy("nfp", "macro", "monthly", timedelta(days=45), "BLS"),
    DatasetPolicy("jobless_claims", "macro", "weekly", timedelta(days=14), "FRED"),
    DatasetPolicy(
        "macro_calendar",
        "calendar",
        "event",
        timedelta(days=7),
        "CANONICAL_EVENT_REPOSITORY",
        ("INVESTING_ECONOMIC_CALENDAR", "XTB"),
    ),
    DatasetPolicy(
        "flash_services_pmi",
        "calendar",
        "monthly",
        timedelta(days=45),
        "SPGLOBAL",
        ("INVESTING_EVENT_1062",),
    ),
    DatasetPolicy(
        "earnings",
        "earnings",
        "event",
        timedelta(days=14),
        "NASDAQ",
        ("FMP_EARNINGS_CALENDAR",),
    ),
    DatasetPolicy(
        "options_positioning",
        "options_positioning",
        "intraday",
        timedelta(hours=2),
        "TRADIER",
    ),
    DatasetPolicy("positioning", "positioning", "weekly", timedelta(days=10), "CFTC"),
    DatasetPolicy(
        "current_news",
        "news",
        "intraday",
        timedelta(hours=24),
        "ALPHA_VANTAGE_NEWS_SENTIMENT",
        (
            "GDELT_DOC_API",
            "FEDERAL_RESERVE_RSS",
            "BLS_RSS",
            "BEA_RSS",
            "YAHOO_FINANCE_RSS",
            "MARKETWATCH_RSS",
            "GOOGLE_NEWS_RSS",
        ),
        provider_strategy="FAN_IN",
    ),
    DatasetPolicy(
        "market_schedule",
        "market_schedule",
        "event",
        timedelta(days=370),
        "NASDAQ_MARKET_INFO",
        ("CME", "INVESTING_HOLIDAYS", "MARKETBEAT"),
    ),
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
    semantic_errors = _semantic_error_count(analytics.get("macro") or {})
    invalid_mappings = sum(
        1
        for items in calendar_lists
        for item in items
        if item.get("invalid_period_mapping") is True
    )
    invalid_states = _invalid_state_count(analytics)
    expired_values = _expired_or_future_delivered_value_count(
        analytics,
        now=clock,
    )
    unexplained = sum(
        1
        for item in payload.get("missing_data") or []
        if not item.get("reason_code")
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
    checks = {
        "response_generated_recently": response_recent,
        "expired_values_delivered": invalid_states + expired_values,
        "stale_values_presented_as_current": _stale_presented_current(analytics),
        "invalid_temporal_mappings": invalid_mappings,
        "semantic_mapping_errors": semantic_errors,
        "calendar_exact_duplicates": exact_duplicates,
        "past_due_awaiting_actual": past_awaiting,
        "post_release_pre_fomc_probabilities": post_release_probabilities,
        "contradictory_nasdaq_drivers": contradictory_drivers,
        "expired_current_news": expired_news,
        "unexplained_omissions": unexplained,
        "provider_accounting_valid": accounting_valid,
    }
    content_passed = (
        checks["response_generated_recently"]
        and all(
            checks[key] == 0
            for key in (
                "expired_values_delivered",
                "stale_values_presented_as_current",
                "invalid_temporal_mappings",
                "semantic_mapping_errors",
                "calendar_exact_duplicates",
                "past_due_awaiting_actual",
                "post_release_pre_fomc_probabilities",
                "contradictory_nasdaq_drivers",
                "expired_current_news",
                "unexplained_omissions",
            )
        )
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
        assessment = _assess_datum(item, now, frequency=str(item.get("frequency") or ""))
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
            identity = str(
                event.get("occurrence_id")
                or f"{event.get('metric_id')}@{event.get('release_at')}"
            )
            for field, reason_code in (
                ("actual", "ACTUAL_NOT_AVAILABLE"),
                ("consensus", "CONSENSUS_NOT_AVAILABLE"),
                ("previous", "PREVIOUS_NOT_AVAILABLE"),
            ):
                if event.get(field) is None:
                    _missing(
                        missing,
                        f"calendar.{name}.{identity}.{field}",
                        "UNAVAILABLE",
                        reason_code,
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
        event = _project_event(raw, release, metric_id, release_status)
        current = selected.get(key)
        if current is None or _event_completeness(event) > _event_completeness(current):
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
) -> dict[str, Any]:
    actual = _number(raw.get("actual"))
    consensus = _number(raw.get("consensus", raw.get("forecast")))
    previous = _number(raw.get("previous"))
    occurrence_match = _occurrence_fields_reconciled(raw)
    surprise = (
        actual - consensus
        if actual is not None and consensus is not None and occurrence_match
        else None
    )
    reason = None
    if not occurrence_match:
        reason = "OCCURRENCE_FIELD_LINEAGE_MISMATCH"
        actual = consensus = previous = surprise = None
    return {
        "occurrence_id": raw.get("occurrence_id") or raw.get("event_id"),
        "metric_id": metric_id,
        "name": raw.get("name") or raw.get("event_name"),
        "release_at": release.isoformat(),
        "reference_period": raw.get("reference_period") or raw.get("period"),
        "release_status": release_status,
        "impact": raw.get("impact") or raw.get("event_risk_level"),
        "actual": actual,
        "consensus": consensus,
        "previous": previous,
        "previous_revised": _number(
            raw.get("previous_revised", raw.get("revised_previous"))
        ),
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
            raw.get("freshness_state")
            or raw.get("freshness")
        ),
        "content_valid_until": (
            raw.get("content_valid_until")
            or raw.get("valid_until")
        ),
        "invalid_period_mapping": False,
        "source": _source(raw),
        "reason_code": reason,
        "lineage": _field_lineage(raw),
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
        assessment = _assess_datum(item, now, frequency="intraday")
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
    assessment = _assess_datum(section, now, frequency="intraday")
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
            "headline": raw.get("headline") or raw.get("title"),
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
            frequency="intraday" if key == "vvix" else "daily",
            section_sync=section.get("sync"),
        )
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
    assessment = _assess_datum(raw, now, frequency="intraday")
    if not assessment["usable"]:
        _missing(
            missing,
            "risk.risk_sentiment",
            assessment["status"],
            assessment["reason_code"],
            assessment["refresh_due_at"],
            ["trading_context"],
        )
    derived = raw.get("derived_context") if isinstance(raw.get("derived_context"), dict) else {}
    return {
        **_metadata(raw, assessment),
        "risk_sentiment": (
            derived.get("risk_regime")
            or derived.get("sentiment")
            if assessment["usable"]
            else None
        ),
        "risk_score": derived.get("risk_score") if assessment["usable"] else None,
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
        assessment = _assess_datum(raw, now, frequency=str(raw.get("frequency") or "daily"))
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
    valid = sum(item["value"] is not None for item in items)
    status = _section_status(valid, len(items) or 1)
    return {
        **_section_metadata(
            status,
            _section_reason(status),
            _latest_value(items, "data_as_of"),
            _latest_value(items, "content_valid_until"),
            source="FRED",
        ),
        "metrics": sorted(items, key=lambda item: item["series_id"]),
    }


def _project_generic_section(
    name: str,
    section: dict[str, Any],
    now: datetime,
    missing: list[dict[str, Any]],
) -> dict[str, Any]:
    frequency = "weekly" if name == "positioning" else "intraday"
    assessment = _assess_datum(section, now, frequency=frequency)
    if not assessment["usable"]:
        _missing(
            missing,
            name,
            assessment["status"],
            assessment["reason_code"],
            assessment["refresh_due_at"],
            ["trading_context"],
        )
    values: dict[str, Any] = {}
    if assessment["usable"]:
        if name == "positioning":
            cot = section.get("cot") if isinstance(section.get("cot"), dict) else {}
            nasdaq = (
                cot.get("nasdaq_100")
                if isinstance(cot.get("nasdaq_100"), dict)
                else {}
            )
            values["cot"] = {
                "report_date": nasdaq.get("report_date") or section.get("data_as_of"),
                "publication_date": nasdaq.get("publication_date"),
                "contract_code": nasdaq.get("cftc_contract_market_code"),
                "open_interest": nasdaq.get("open_interest"),
                "asset_managers": _select_position_group(
                    nasdaq.get("asset_managers")
                ),
                "leveraged_funds": _select_position_group(
                    nasdaq.get("leveraged_funds")
                ),
                "dealers": _select_position_group(nasdaq.get("dealers")),
            }
        else:
            values.update(
                {
                    "iv_atm": section.get("iv_atm"),
                    "open_interest": deepcopy(section.get("open_interest") or {}),
                    "volume": deepcopy(section.get("volume") or {}),
                    "skew": deepcopy(section.get("skew") or {}),
                }
            )
    else:
        values = (
            {"cot": {}}
            if name == "positioning"
            else {
                "iv_atm": None,
                "open_interest": {},
                "volume": {},
                "skew": {},
            }
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
    candidates = [
        *list(nasdaq.get("upcoming") or []),
        *corporate_candidates,
    ]
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        symbol = str(raw.get("symbol") or raw.get("ticker") or "").upper()
        event_time = parse_datetime(
            raw.get("event_at") or raw.get("earnings_date") or raw.get("date")
        )
        if not symbol or not event_time:
            continue
        event_time = _utc(event_time)
        if not (now - timedelta(days=1) <= event_time <= now + timedelta(days=14)):
            continue
        selected[(symbol, event_time.date().isoformat())] = {
            "symbol": symbol,
            "event_at": event_time.isoformat(),
            "timing": raw.get("timing") or raw.get("time"),
            "eps_estimate": _number(raw.get("eps_estimate")),
            "revenue_estimate": _number(raw.get("revenue_estimate")),
            "source": _source(raw),
            "reason_code": None,
        }
    events = sorted(selected.values(), key=lambda item: (item["event_at"], item["symbol"]))
    status = "AVAILABLE" if events else "NO_DATA"
    if not events:
        _missing(
            missing,
            "earnings.events",
            "UNAVAILABLE",
            "NO_CURRENT_EARNINGS_EVENTS",
            None,
            ["trading_context"],
        )
    return {
        **_section_metadata(
            status,
            None if events else "NO_CURRENT_EARNINGS_EVENTS",
            now.isoformat(),
            (now + timedelta(hours=6)).isoformat(),
            source="NASDAQ",
        ),
        "events": events,
    }


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
    status = "AVAILABLE" if verified else "PARTIAL" if section else "UNAVAILABLE"
    reason = None if verified else "SESSION_VERIFICATION_PARTIAL"
    if not verified:
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
        "nasdaq_cash_session": _select_schedule_session(
            section.get("nasdaq_cash_session")
        ),
        "mnq_futures_session": _select_schedule_session(
            section.get("mnq_futures_session") or section.get("mnq_session")
        ),
    }


def _assess_datum(
    value: dict[str, Any],
    now: datetime,
    *,
    frequency: str,
    section_sync: Any = None,
) -> dict[str, Any]:
    raw_status = str(value.get("status") or "").upper()
    raw_freshness = str(value.get("freshness") or "").upper()
    lifecycle = value.get("lifecycle") if isinstance(value.get("lifecycle"), dict) else {}
    sync = section_sync if isinstance(section_sync, dict) else {}
    lifecycle_freshness = str(
        lifecycle.get("freshness")
        or lifecycle.get("freshness_state")
        or sync.get("freshness")
        or ""
    ).upper()
    data_as_of = parse_datetime(
        value.get("data_as_of")
        or value.get("observed_at")
        or value.get("as_of")
        or value.get("retrieved_at")
    )
    retrieved = parse_datetime(value.get("retrieved_at") or value.get("last_successful_refresh_at"))
    explicit_until = parse_datetime(
        value.get("content_valid_until")
        or lifecycle.get("valid_until")
        or value.get("valid_until")
    )
    refresh_due = parse_datetime(
        value.get("refresh_due_at")
        or value.get("next_refresh_at")
        or lifecycle.get("next_refresh_at")
        or lifecycle.get("next_refresh")
    )
    policy_age = _policy_age(frequency)
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
    if explicit_until and _utc(explicit_until) < now:
        return _assessment(
            False,
            "UNAVAILABLE",
            "UNAVAILABLE",
            "CONTENT_VALIDITY_EXPIRED",
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
            min(_utc(explicit_until), now + policy_age)
            if explicit_until
            else now + policy_age
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
            or value.get("retrieved_at")
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
        and parse_datetime(request_started_at)
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
        if not _acquisition_accounting_row_complete(
            raw,
            policy=policy,
            request_id=request_id,
            correlation_id=correlation_id,
            request_started_at=request_started_at,
            request_completed_at=request_completed_at,
        ):
            candidate["evidence_status"] = "INCOMPLETE"
            candidate["reason_code"] = (
                "REQUEST_ACQUISITION_EVIDENCE_INCOMPLETE"
            )
            observed_at = parse_datetime(raw.get("observed_at"))
            if (
                raw.get("database_lookup_performed") is True
                and observed_at is not None
                and not _canonical_database_evidence_valid(
                    raw,
                    policy=policy,
                    observed_at=observed_at,
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
    delivered = deepcopy(value) if _meaningful_delivery(value) else None
    section = (
        analytics.get(section_name)
        if isinstance(analytics.get(section_name), dict)
        else {}
    )
    source = _first_delivery_field(delivered, "source")
    if source is None:
        source = section.get("source")
    freshness = _first_delivery_field(delivered, "freshness")
    if freshness is None:
        freshness = section.get("freshness") or "UNAVAILABLE"
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
    present = delivered is not None
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


def _dataset_delivery_value(
    dataset_id: str,
    analytics: dict[str, Any],
) -> tuple[str, Any]:
    macro_series = {
        "cpi": {"CUSR0000SA0", "CUSR0000SA0L1E"},
        "ppi": {"WPUFD4"},
        "pce": {
            "BEA:PCE",
            "BEA:PCE_PRICE_INDEX",
            "BEA:CORE_PCE",
        },
        "gdp": {"BEA:GDP", "GDP"},
        "employment": {"LNS14000000", "UNRATE"},
        "wages": {"CES0500000003"},
        "nfp": {"CES0000000001", "PAYEMS"},
        "jobless_claims": {"ICSA"},
    }
    if dataset_id in macro_series:
        metrics = (analytics.get("macro") or {}).get("metrics") or []
        return "macro", [
            item
            for item in metrics
            if isinstance(item, dict)
            and str(item.get("series_id") or "").upper()
            in macro_series[dataset_id]
            and item.get("value") is not None
        ]
    if dataset_id in {"treasury_rates", "fed_funds"}:
        expected = (
            {"DFF", "FEDFUNDS", "SOFR"}
            if dataset_id == "fed_funds"
            else {
                "DGS2",
                "DGS10",
                "DGS30",
                "T10Y2Y",
                "T10Y3M",
                "NFCI",
            }
        )
        metrics = (analytics.get("rates") or {}).get("metrics") or []
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
        return "vix", (analytics.get("vix") or {}).get(
            dataset_id.upper()
        )
    if dataset_id == "risk":
        return "risk", _delivery_fields(
            analytics.get("risk") or {},
            ("risk_sentiment", "risk_score"),
        )
    if dataset_id == "target_range":
        return "fomc", _delivery_fields(
            analytics.get("fomc") or {},
            ("target_range_lower", "target_range_upper"),
        )
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
            events = [
                {
                    "value": item.get("actual"),
                    "source": (
                        item.get("actual_source")
                        or item.get("publisher")
                    ),
                    "freshness": (
                        item.get("freshness_state")
                        or item.get("freshness")
                    ),
                    "data_as_of": (
                        item.get("reference_period")
                        or item.get("release_at")
                    ),
                    "content_valid_until": (
                        item.get("content_valid_until")
                        or item.get("valid_until")
                    ),
                }
                for item in events
                if (
                    "flash_services_pmi"
                    in str(item.get("metric_id") or "").lower()
                    or "flash services pmi"
                    in str(item.get("name") or "").lower()
                )
                and item.get("actual") not in (None, "")
            ]
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
    tokens = {
        "nasdaq_100": ("nasdaq.components",),
        "mega_cap_quotes": ("nasdaq.components", "nasdaq.drivers"),
        "market_internals": ("market_internals",),
        "vix": ("vix.vix",),
        "vvix": ("vix.vvix",),
        "risk": ("risk.",),
        "treasury_rates": ("rates.",),
        "fed_funds": ("rates.",),
        "target_range": ("fomc.target_range",),
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
    if lookup:
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
        and _provider_flow_valid(item, policy=policy)
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


def _readiness(analytics: dict[str, Any]) -> dict[str, Any]:
    statuses = {
        name: str((analytics.get(name) or {}).get("status") or "UNAVAILABLE")
        for name in SECTION_NAMES
    }
    value_counts = {
        name: _analytic_section_value_count(analytics.get(name))
        for name in SECTION_NAMES
    }
    usable = [
        name
        for name in SECTION_NAMES
        if value_counts[name] > 0
        and statuses[name] not in {"UNAVAILABLE", "NO_DATA", "UNAVAILABLE_AFTER_RELEASE"}
    ]
    degraded = [
        name
        for name in usable
        if statuses[name] in {"PARTIAL", "DEGRADED"}
    ]
    unavailable = [name for name in SECTION_NAMES if name not in usable]
    coverage = round(len(usable) / len(SECTION_NAMES), 4)
    return {
        "status": (
            "READY"
            if len(usable) == len(SECTION_NAMES) and not degraded
            else "PARTIAL"
            if usable
            else "UNAVAILABLE"
        ),
        "calculated_from_delivered_payload": True,
        "available_section_count": len(usable),
        "degraded_section_count": len(degraded),
        "unavailable_section_count": len(unavailable),
        "section_count": len(SECTION_NAMES),
        "coverage_ratio": coverage,
        "sections_available": usable,
        "sections_degraded": degraded,
        "sections_unavailable": unavailable,
        "section_status": statuses,
        "delivered_value_counts": value_counts,
        "excluded_values_contribute": False,
    }


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
        return str(explicit).strip().lower()
    name = str(event.get("name") or event.get("event_name") or "").lower()
    if "employment situation" in name:
        return "employment_situation"
    if "flash" in name and "pmi" in name and "serv" in name:
        return "flash_services_pmi"
    normalized = re.sub(r"[^a-z0-9]+", "_", name).strip("_")
    return normalized or "unknown_event"


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


def _occurrence_fields_reconciled(event: dict[str, Any]) -> bool:
    occurrence_ids: set[str] = set()
    for key in ("field_lineage", "lineage"):
        raw = event.get(key)
        if not isinstance(raw, dict):
            continue
        for field in ("actual", "consensus", "forecast", "previous"):
            item = raw.get(field)
            if isinstance(item, dict) and item.get("occurrence_id"):
                occurrence_ids.add(str(item["occurrence_id"]))
    return len(occurrence_ids) <= 1


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
    distributor = value.get("distribution_source") or (
        value.get("source")
        if publisher and value.get("source") != publisher
        else None
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
            {"field": str(field), **deepcopy(item)}
            for field, item in sorted(raw.items())
            if isinstance(item, dict)
        ]
    source = _source(value)
    return [{"field": "value", "source": source}] if source else []


def _datum_has_value(value: dict[str, Any]) -> bool:
    scalar_keys = (
        "value",
        "actual",
        "price",
        "status",
        "market_session_status",
        "iv_atm",
    )
    if any(value.get(key) not in {None, ""} for key in scalar_keys):
        return True
    return any(
        isinstance(item, (list, dict)) and bool(item)
        for key, item in value.items()
        if key not in {"warnings", "errors", "validation", "sync", "lifecycle"}
    )


def _policy_age(frequency: str) -> timedelta:
    return {
        "intraday": timedelta(hours=2),
        "daily": timedelta(days=2),
        "weekly": timedelta(days=14),
        "monthly": timedelta(days=45),
        "quarterly": timedelta(days=120),
        "event": timedelta(days=370),
    }.get(frequency.lower(), timedelta(hours=24))


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
    ):
        return False
    for item in rows:
        if not _request_accounting_row_complete(
            item,
            request_id=request_id,
            correlation_id=correlation_id,
            request_started_at=request_started_at,
            request_completed_at=request_completed_at,
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
        or (
            item.get("selected_value_present")
            != (item.get("delivered_value") is not None)
        )
        or str(item.get("reason_code") or "").upper()
        in {"DB_VALID_REUSED", "SOURCE_SELECTED_FROM_SAME_REQUEST"}
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
    if item["database_lookup_performed"]:
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
        _provider_flow_valid(item, policy=policy)
        and
        item.get("payload_freshness")
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
