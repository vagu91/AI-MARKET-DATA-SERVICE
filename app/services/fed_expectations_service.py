from __future__ import annotations

import calendar
import copy
import logging
import math
import re
from datetime import UTC, date, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.fed_expectations_repository import FedExpectationsRepository


logger = logging.getLogger(__name__)
CALCULATION_VERSION = "fed_expectations_v1"
SOURCE_RANK = {
    "not_found": 0,
    "last_known_good_vendor": 1,
    "last_known_good_official": 2,
    "secondary_monitor": 3,
    "verified_vendor_probability": 4,
    "official_futures_derived_partial": 5,
    "official_futures_derived_complete": 6,
}


class FedExpectationsService:
    def __init__(self, settings: Settings, repository: FedExpectationsRepository | None = None) -> None:
        self.settings = settings
        self.repository = repository or FedExpectationsRepository(settings)

    def snapshot(
        self,
        *,
        refresh: str,
        provider_payload: dict[str, Any] | None,
        macro_snapshot: dict[str, Any],
        event_calendar: dict[str, Any],
        legacy_block: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        logger.info("fed_expectations_lookup_started", extra={"refresh": refresh})
        latest = self.repository.latest()
        if refresh == "false" or (refresh == "auto" and latest and not _is_stale(latest)):
            return _runtime_view(latest, refresh=refresh) if latest else not_found_snapshot(
                refresh=refresh,
                legacy_block=legacy_block,
                warning="fed_expectations_not_in_db_refresh_false" if refresh == "false" else "fed_expectations_not_in_db",
            )

        provider_payload = provider_payload or {}
        candidate = canonicalize_investing_monitor(
            provider_payload,
            macro_snapshot=macro_snapshot,
            event_calendar=event_calendar,
            legacy_block=legacy_block,
            history=self.repository.history(),
        )
        expected_history_depth = self.repository.count() + (1 if candidate.get("status") == "available" else 0)
        candidate.setdefault("diagnostics", {})["history_snapshot_count"] = expected_history_depth
        candidate.setdefault("quality", {})["history_depth"] = expected_history_depth
        candidate_rank = SOURCE_RANK.get((candidate.get("source_summary") or {}).get("ranking_class"), 0)
        latest_rank = SOURCE_RANK.get((latest or {}).get("source_summary", {}).get("ranking_class"), 0)
        if latest and (candidate.get("status") != "available" or candidate_rank < latest_rank):
            fallback = _runtime_view(latest, refresh=refresh)
            fallback["status"] = "stale_acceptable" if _is_stale(latest) else "available"
            fallback["source_summary"]["last_known_good_used"] = True
            fallback["diagnostics"]["last_known_good_used"] = True
            fallback["diagnostics"]["source_failure_count"] = 1
            fallback["warnings"] = list(dict.fromkeys([*(fallback.get("warnings") or []), "new_source_did_not_replace_higher_quality_last_known_good"]))
            logger.warning("fed_expectations_fallback_selected", extra={"fallback_reason": "candidate_lower_quality"})
            return fallback
        if candidate.get("status") != "available":
            return candidate

        self.repository.append(candidate)
        read_back = self.repository.latest() or candidate
        read_back["diagnostics"]["history_snapshot_count"] = self.repository.count()
        read_back["quality"]["history_depth"] = self.repository.count()
        read_back["diagnostics"].update(
            {
                "persisted_count": 1,
                "read_back_count": 1,
                "materialized_count": len(read_back.get("meetings") or []),
            }
        )
        logger.info("fed_expectations_persisted", extra={"source": read_back.get("source_summary", {}).get("selected_source")})
        logger.info("fed_expectations_read_back", extra={"meeting_count": len(read_back.get("meetings") or [])})
        return _runtime_view(read_back, refresh=refresh, force_read_back=True)


def canonicalize_investing_monitor(
    payload: dict[str, Any],
    *,
    macro_snapshot: dict[str, Any],
    event_calendar: dict[str, Any],
    legacy_block: dict[str, Any] | None = None,
    history: list[dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    meetings_raw = payload.get("meetings") if isinstance(payload.get("meetings"), list) else []
    official_dates = official_fomc_dates(event_calendar, now=now)
    calendar_coverage_end = official_calendar_coverage_end(event_calendar)
    current_state = build_current_fed_state(macro_snapshot, official_dates=official_dates, now=now)
    current_midpoint = current_state.get("current_target_midpoint")
    meetings: list[dict[str, Any]] = []
    invalid = 0
    normalized_count = 0
    warnings = list(payload.get("warnings") or [])
    for raw in meetings_raw:
        canonical, validation = canonicalize_meeting(
            raw,
            current_midpoint=current_midpoint,
            official_dates=official_dates,
            official_calendar_coverage_end=calendar_coverage_end,
            source=str(payload.get("source") or "Investing.com Fed Rate Monitor"),
            source_url=payload.get("source_url"),
            retrieved_at=payload.get("retrieved_at"),
            valid_until=payload.get("valid_until"),
            now=now,
        )
        if canonical:
            meetings.append(canonical)
            normalized_count += int(validation["probabilities_normalized"])
        else:
            invalid += 1
            warnings.extend(validation["errors"])

    meetings.sort(key=lambda item: item["meeting_date"])
    repricing = calculate_repricing(meetings, history or [], now=now)
    available = bool(meetings)
    source = str(payload.get("source") or "Investing.com Fed Rate Monitor")
    retrieved_at = str(payload.get("retrieved_at") or _iso(now))
    valid_until = str(payload.get("valid_until") or _iso(now + timedelta(minutes=30)))
    stale = _date_or_none(valid_until) is not None and now >= _date_or_none(valid_until)
    mapped_count = sum(1 for item in meetings if item["validation"]["meeting_date_match"] is True)
    mapping_evaluated_count = sum(1 for item in meetings if item["validation"]["meeting_date_match"] is not None)
    current_fields = ("current_target_lower_bound", "current_target_upper_bound", "effective_fed_funds_rate", "sofr")
    current_complete = sum(current_state.get(key) is not None for key in current_fields) / len(current_fields) * 100
    source_quality = 0.72
    distribution_coverage = 100.0 if meetings else 0.0
    mapping_valid_pct = (mapped_count / mapping_evaluated_count * 100) if mapping_evaluated_count else 0.0
    quality_score = 0.0
    if available:
        quality_score = source_quality * 0.35 + distribution_coverage / 100 * 0.25 + current_complete / 100 * 0.2
        quality_score += (mapping_valid_pct / 100 * 0.1) + (0.05 if not stale else 0.0)
        quality_score = round(min(quality_score, 0.79), 3)
    next_meeting = meetings[0] if meetings else None
    diagnostics = {
        "source_attempt_count": int(payload.get("provider_calls") or (1 if payload else 0)),
        "source_success_count": 1 if available else 0,
        "source_failure_count": 0 if available else 1,
        "official_source_success": False,
        "vendor_source_success": available,
        "reconstruction_used": False,
        "last_known_good_used": False,
        "meeting_count": len(meetings),
        "contract_count": sum(1 for item in meetings if item.get("contract_symbols")),
        "valid_distribution_count": len(meetings),
        "invalid_distribution_count": invalid,
        "missing_contract_count": 0,
        "probability_normalization_count": normalized_count,
        "history_snapshot_count": len(history or []),
        "provider_calls": int(payload.get("provider_calls") or (1 if payload else 0)),
        "AI_called": False,
        "cache_used": bool(payload.get("cache_used")),
        "browser_calls": 0,
        "error_breakdown": {
            "access_restricted": 0,
            "rate_limited": 0,
            "schema_changed": int(bool(payload) and not meetings_raw),
            "missing_contract": 0,
            "invalid_price": 0,
            "invalid_probability_sum": invalid,
            "meeting_mapping_failed": max(mapping_evaluated_count - mapped_count, 0),
            "current_range_missing": int(current_midpoint is None),
            "calendar_mismatch": max(mapping_evaluated_count - mapped_count, 0),
            "stale_source": int(stale),
            "partial_response": int(bool(meetings) and invalid > 0),
            "history_insufficient": int(not repricing["history_available"]),
        },
    }
    result = {
        "status": "available" if available else "not_found",
        "current_fed_state": current_state,
        "next_meeting": _next_meeting_summary(next_meeting),
        "meetings": meetings,
        "repricing": repricing,
        "source_summary": {
            "selected_source": source if available else None,
            "original_provider": payload.get("provider") or source,
            "selected_source_type": "secondary_monitor" if available else "not_found",
            "ranking_class": "secondary_monitor" if available else "not_found",
            "source_url": payload.get("source_url"),
            "is_official_source": False,
            "is_reconstructed": False,
            "alternative_sources": [],
            "last_known_good_used": False,
        },
        "quality": {
            "meeting_coverage_pct": 100.0 if meetings else 0.0,
            "probability_distribution_coverage_pct": distribution_coverage,
            "official_source_coverage_pct": 0.0,
            "current_state_completeness_pct": round(current_complete, 2),
            "mapping_valid_pct": round(mapping_valid_pct, 2),
            "history_depth": len(history or []),
            "stale_snapshot_count": int(stale),
            "missing_contract_count": 0,
            "quality_score": quality_score,
        },
        "diagnostics": diagnostics,
        "data_as_of": payload.get("data_as_of") or (meetings[0].get("data_as_of") if meetings else None),
        "retrieved_at": retrieved_at,
        "valid_until": valid_until,
        "age_minutes": max(0.0, round((now - (_date_or_none(retrieved_at) or now)).total_seconds() / 60, 2)),
        "stale": stale,
        "last_successful_refresh_at": retrieved_at if available else None,
        "next_refresh_at": valid_until,
        "market_session_status": "weekend" if now.weekday() >= 5 else "open_or_intraday",
        "calculation_method": "vendor_probability_normalization",
        "calculation_version": CALCULATION_VERSION,
        "contract_symbols": [],
        "raw_prices": [item.get("future_price") for item in meetings if item.get("future_price") is not None],
        "confidence": quality_score,
        "warnings": list(dict.fromkeys(warnings)),
        "errors": [] if available else ["fed_expectations_not_available"],
        "fed_funds_futures": _legacy_fed_funds_block(payload, legacy_block),
        "service_role": "data provider only",
    }
    result["sanity_check"] = build_fed_sanity_check(result, macro_snapshot=macro_snapshot)
    return result


def build_fed_sanity_check(
    snapshot: dict[str, Any],
    *,
    macro_snapshot: dict[str, Any],
    tolerance: float = 0.015,
) -> dict[str, Any]:
    meetings = [item for item in snapshot.get("meetings") or [] if isinstance(item, dict)]
    current = snapshot.get("current_fed_state") or {}
    lower = _number(current.get("current_target_lower_bound"))
    upper = _number(current.get("current_target_upper_bound"))
    midpoint = _number(current.get("current_target_midpoint"))
    target_range_consistent = (
        True if lower is None or upper is None or midpoint is None else math.isclose((lower + upper) / 2, midpoint, abs_tol=tolerance)
    )
    expected_midpoint_consistent = True
    expected_change_consistent = True
    future_price_consistent = True
    probability_distribution_consistent = True
    for meeting in meetings:
        outcomes = [item for item in meeting.get("outcomes") or [] if isinstance(item, dict)]
        probability_sum = sum(_number(item.get("probability")) or 0.0 for item in outcomes)
        if outcomes and not math.isclose(probability_sum, 1.0, abs_tol=tolerance):
            probability_distribution_consistent = False
        calculated_midpoint = sum(
            (_number(item.get("target_midpoint")) or 0.0) * (_number(item.get("probability")) or 0.0)
            for item in outcomes
        )
        reported_midpoint = _number(meeting.get("expected_target_midpoint"))
        if outcomes and reported_midpoint is not None and not math.isclose(calculated_midpoint, reported_midpoint, abs_tol=tolerance):
            expected_midpoint_consistent = False
        reported_change = _number(meeting.get("expected_change_bps"))
        if reported_midpoint is not None and midpoint is not None and reported_change is not None:
            calculated_change = (reported_midpoint - midpoint) * 100
            if not math.isclose(calculated_change, reported_change, abs_tol=0.5):
                expected_change_consistent = False
        future_price = _number(meeting.get("future_price"))
        if future_price is not None and reported_midpoint is not None:
            implied_rate = 100.0 - future_price
            if abs(implied_rate - reported_midpoint) > 1.25:
                future_price_consistent = False
        meeting["probability_semantics"] = "probability_target_range_after_meeting_relative_to_current_range"
        meeting["is_single_meeting_action_probability"] = False

    meeting_dates = [str(item.get("meeting_date") or "") for item in meetings]
    meeting_sequence_consistent = meeting_dates == sorted(meeting_dates) and len(meeting_dates) == len(set(meeting_dates))
    calendar_mapping_consistent = all(
        (item.get("validation") or {}).get("meeting_date_match") is not False for item in meetings
    )
    ranking = str((snapshot.get("source_summary") or {}).get("ranking_class") or "")
    crosscheck_available = ranking.startswith("official_") or bool((snapshot.get("source_summary") or {}).get("crosscheck_source"))
    core = all(
        (
            target_range_consistent,
            expected_midpoint_consistent,
            expected_change_consistent,
            future_price_consistent,
            meeting_sequence_consistent,
            probability_distribution_consistent,
            calendar_mapping_consistent,
        )
    )
    warnings: list[str] = []
    if not crosscheck_available:
        warnings.append("official_source_crosscheck_unavailable")
    if not core:
        warnings.append("fed_expectations_internal_consistency_failed")
    status = "FAIL" if not core else "PASS" if crosscheck_available else "WARN"
    yields = _yields_context(macro_snapshot, meetings)
    result = {
        "status": status,
        "target_range_consistent": target_range_consistent,
        "expected_midpoint_consistent": expected_midpoint_consistent,
        "expected_change_consistent": expected_change_consistent,
        "future_price_consistent": future_price_consistent,
        "meeting_sequence_consistent": meeting_sequence_consistent,
        "probability_distribution_consistent": probability_distribution_consistent,
        "calendar_mapping_consistent": calendar_mapping_consistent,
        "source_crosscheck_available": crosscheck_available,
        "requires_crosscheck": not crosscheck_available,
        "probability_semantics": "probability_target_range_after_meeting_relative_to_current_range",
        "is_single_meeting_action_probability": False,
        "yields_context": yields,
        "warnings": warnings,
    }
    logger.info(
        "fed_expectations_sanity_checked",
        extra={"status": status, "meeting_count": len(meetings), "source_crosscheck_available": crosscheck_available},
    )
    return result


def _yields_context(macro_snapshot: dict[str, Any], meetings: list[dict[str, Any]]) -> dict[str, Any]:
    rates = macro_snapshot.get("rates_and_yields") or {}
    dgs2 = _number((rates.get("DGS2") or {}).get("value"))
    dgs10 = _number((rates.get("DGS10") or {}).get("value"))
    curve = round(dgs10 - dgs2, 4) if dgs2 is not None and dgs10 is not None else None
    expected_change = _number((meetings[0] if meetings else {}).get("expected_change_bps"))
    if curve is None or expected_change is None:
        direction = "UNKNOWN"
    elif abs(expected_change) < 5 or abs(curve) < 0.05:
        direction = "NEUTRAL"
    elif (expected_change < 0 and curve >= 0) or (expected_change > 0 and curve <= 0):
        direction = "SUPPORTIVE"
    else:
        direction = "DIVERGENT"
    return {"dgs2": dgs2, "dgs10": dgs10, "curve_2s10s": curve, "directional_consistency": direction}


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def canonicalize_meeting(
    raw: dict[str, Any],
    *,
    current_midpoint: float | None,
    official_dates: set[str],
    official_calendar_coverage_end: str | None = None,
    source: str,
    source_url: str | None,
    retrieved_at: str | None,
    valid_until: str | None,
    now: datetime,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    meeting_date = str(raw.get("meeting_date") or "")
    if not _valid_date(meeting_date):
        return None, {"errors": ["meeting_mapping_failed"], "probabilities_normalized": False}
    seen: set[tuple[float, float]] = set()
    outcomes: list[dict[str, Any]] = []
    errors: list[str] = []
    raw_items = raw.get("target_rate_probabilities") or raw.get("outcomes") or []
    prepared: list[tuple[tuple[float, float] | None, float | None]] = []
    for item in raw_items:
        bounds = parse_target_range(item.get("target_rate") or item.get("target_range"))
        raw_probability = item.get("current_probability_pct", item.get("probability"))
        try:
            numeric_probability = float(str(raw_probability).replace("%", "").strip())
        except (TypeError, ValueError):
            numeric_probability = None
        prepared.append((bounds, numeric_probability))
    raw_total = sum(value for _, value in prepared if value is not None)
    percent_distribution = 99.0 <= raw_total <= 101.0
    decimal_distribution = 0.99 <= raw_total <= 1.01
    for bounds, numeric_probability in prepared:
        probability = None
        if numeric_probability is not None:
            if percent_distribution:
                probability = round(numeric_probability / 100, 12)
            elif decimal_distribution:
                probability = numeric_probability
            else:
                probability = parse_probability(numeric_probability)
        if bounds is None or probability is None or not 0 <= probability <= 1:
            errors.append("invalid_probability")
            continue
        if bounds in seen:
            return None, {"errors": ["duplicate_target_range"], "probabilities_normalized": False}
        seen.add(bounds)
        midpoint = round(sum(bounds) / 2, 4)
        change = round((midpoint - current_midpoint) * 100, 2) if current_midpoint is not None else None
        outcomes.append(
            {
                "target_lower_bound": bounds[0],
                "target_upper_bound": bounds[1],
                "target_midpoint": midpoint,
                "change_bps": change,
                "probability": probability,
                "probability_pct": round(probability * 100, 6),
                "classification": classify_change(change),
            }
        )
    validation = validate_distribution(outcomes)
    if not validation["valid"]:
        return None, {"errors": [*errors, *validation["errors"]], "probabilities_normalized": False}
    if validation["normalized"]:
        total = sum(item["probability"] for item in outcomes)
        for item in outcomes:
            item["probability"] = item["probability"] / total
            item["probability_pct"] = round(item["probability"] * 100, 6)
    aggregates = aggregate_outcomes(outcomes, current_midpoint=current_midpoint)
    most_likely = max(outcomes, key=lambda item: item["probability"])
    meeting_at = raw.get("meeting_at")
    meeting_time_utc = None
    if parsed := _date_or_none(meeting_at):
        meeting_time_utc = _iso(parsed.astimezone(UTC))
    data_as_of = raw.get("updated_at") or retrieved_at
    freshness = "STALE" if (_date_or_none(valid_until) and now >= _date_or_none(valid_until)) else "RECENT"
    meeting_date_match: bool | None = None
    if meeting_date in official_dates:
        meeting_date_match = True
    elif official_calendar_coverage_end and meeting_date <= official_calendar_coverage_end:
        meeting_date_match = False
    result = {
        "meeting_id": str(raw.get("event_id") or f"fomc:{meeting_date}"),
        "meeting_date": meeting_date,
        "meeting_time_utc": meeting_time_utc,
        "contract_month": meeting_date[:7],
        "contract_symbols": [],
        "current_target_range": None if current_midpoint is None else {
            "midpoint": current_midpoint,
            "lower_bound": round(current_midpoint - 0.125, 3),
            "upper_bound": round(current_midpoint + 0.125, 3),
        },
        "outcomes": outcomes,
        **aggregates,
        "most_likely_target_range": f"{most_likely['target_lower_bound']:.2f}-{most_likely['target_upper_bound']:.2f}",
        "most_likely_probability": most_likely["probability"],
        "most_likely_probability_pct": most_likely["probability_pct"],
        "source": source,
        "source_url": source_url or raw.get("source_url"),
        "provider_type": "secondary_monitor",
        "data_as_of": data_as_of,
        "retrieved_at": retrieved_at,
        "valid_until": valid_until,
        "freshness": freshness,
        "reliability": 0.72,
        "confidence": 0.72 if current_midpoint is not None else 0.62,
        "cache_status": "provider",
        "future_price": raw.get("future_price"),
        "calculation_method": "vendor_probability_distribution",
        "calculation_version": CALCULATION_VERSION,
        "is_official_source": False,
        "is_reconstructed": False,
        "validation": {
            **validation,
            "meeting_date_match": meeting_date_match,
            "contract_mapping_valid": True,
            "current_range_present": current_midpoint is not None,
            "most_likely_outcome_present": True,
        },
    }
    logger.info("fed_probability_distribution_validated", extra={"meeting_date": meeting_date, "probability_sum": validation["probability_sum"]})
    return result, {"errors": errors, "probabilities_normalized": validation["normalized"]}


def build_current_fed_state(
    macro_snapshot: dict[str, Any], *, official_dates: set[str], now: datetime
) -> dict[str, Any]:
    rates = macro_snapshot.get("rates_and_yields") or {}
    lower = _series_value(rates, "DFEDTARL")
    upper = _series_value(rates, "DFEDTARU")
    midpoint = round((lower + upper) / 2, 4) if lower is not None and upper is not None else None
    next_date = min((value for value in official_dates if value >= now.date().isoformat()), default=None)
    return {
        "current_target_lower_bound": lower,
        "current_target_upper_bound": upper,
        "current_target_midpoint": midpoint,
        "effective_fed_funds_rate": _series_value(rates, "DFF", "FEDFUNDS"),
        "sofr": _series_value(rates, "SOFR"),
        "next_fomc_meeting_at": f"{next_date}T18:00:00Z" if next_date else None,
        "days_to_next_fomc": (date.fromisoformat(next_date) - now.date()).days if next_date else None,
        "source": "FRED and Federal Reserve Calendar",
        "source_url": "https://fred.stlouisfed.org/",
        "data_as_of": max(
            (str((rates.get(key) or {}).get("data_as_of") or "") for key in ("DFF", "DFEDTARL", "DFEDTARU", "SOFR")),
            default="",
        ) or None,
        "is_official_source": any(value is not None for value in (lower, upper)),
    }


def official_fomc_dates(event_calendar: dict[str, Any], *, now: datetime | None = None) -> set[str]:
    now = now or datetime.now(UTC)
    events = []
    for key in ("fed_communications", "critical_macro_events", "other_economic_events"):
        events.extend(event_calendar.get(key) or [])
    dates: set[str] = set()
    for item in events:
        category = str(item.get("category") or "").upper()
        name = str(item.get("name") or "").upper()
        if category != "FOMC" or "MINUTES" in name or "SPEECH" in name:
            continue
        if "PRESS CONFERENCE" in name and "MEETING" not in name:
            continue
        event_date = str(item.get("date") or "")
        if _valid_date(event_date) and event_date >= (now.date() - timedelta(days=1)).isoformat():
            dates.add(event_date)
    return dates


def official_calendar_coverage_end(event_calendar: dict[str, Any]) -> str | None:
    dates = []
    for key in ("fed_communications", "critical_macro_events", "other_economic_events"):
        for item in event_calendar.get(key) or []:
            event_date = str(item.get("date") or "")
            if _valid_date(event_date):
                dates.append(event_date)
    return max(dates, default=None)


def validate_distribution(outcomes: list[dict[str, Any]], *, tolerance: float = 0.01) -> dict[str, Any]:
    if not outcomes:
        return {"valid": False, "normalized": False, "probability_sum": 0.0, "errors": ["empty_distribution"]}
    total = sum(float(item.get("probability") or 0) for item in outcomes)
    negative = sum(float(item.get("probability") or 0) < 0 for item in outcomes)
    over = sum(float(item.get("probability") or 0) > 1 for item in outcomes)
    duplicate = len({(item.get("target_lower_bound"), item.get("target_upper_bound")) for item in outcomes}) != len(outcomes)
    errors = []
    if negative:
        errors.append("negative_probability")
    if over:
        errors.append("over_100_probability")
    if duplicate:
        errors.append("duplicate_target_range")
    if abs(total - 1.0) > tolerance:
        errors.append("invalid_probability_sum")
    valid = not errors
    return {
        "valid": valid,
        "normalized": valid and not math.isclose(total, 1.0, abs_tol=1e-9),
        "probability_sum": round(total, 8),
        "negative_probability_count": negative,
        "over_100_probability_count": over,
        "duplicate_target_range_count": int(duplicate),
        "outcome_count": len(outcomes),
        "errors": errors,
    }


def aggregate_outcomes(outcomes: list[dict[str, Any]], *, current_midpoint: float | None) -> dict[str, Any]:
    result = {name: 0.0 for name in (
        "cut_probability", "hold_probability", "hike_probability", "cut_25_probability",
        "cut_50_or_more_probability", "hike_25_probability", "hike_50_or_more_probability",
    )}
    expected = 0.0
    for item in outcomes:
        probability = float(item["probability"])
        expected += float(item["target_midpoint"]) * probability
        classification = item["classification"]
        result[f"{classification}_probability"] += probability
        change = item.get("change_bps")
        if change is not None:
            if -37.5 < change < -12.5:
                result["cut_25_probability"] += probability
            elif change <= -37.5:
                result["cut_50_or_more_probability"] += probability
            elif 12.5 < change < 37.5:
                result["hike_25_probability"] += probability
            elif change >= 37.5:
                result["hike_50_or_more_probability"] += probability
    expected = round(expected, 6)
    result = {key: round(value, 8) for key, value in result.items()}
    result["expected_target_midpoint"] = expected
    result["expected_change_bps"] = round((expected - current_midpoint) * 100, 4) if current_midpoint is not None else None
    return result


def reconstruct_monthly_futures_distribution(
    *, futures_price: float, meeting_date: str, current_effective_rate: float, current_target_midpoint: float,
    contract_month: str | None = None, as_of: str | None = None,
) -> dict[str, Any]:
    """Invert a monthly average Fed Funds futures rate around one meeting.

    The contract-implied monthly average is ``100 - price``. The known pre-meeting
    days are removed from that average, leaving an implied post-meeting rate. The
    result is linearly allocated only between adjacent 25 bp target midpoints.
    """
    try:
        meeting = date.fromisoformat(meeting_date)
    except ValueError:
        return {"status": "partial", "warnings": ["meeting_mapping_failed"], "outcomes": []}
    month = contract_month or meeting.strftime("%Y-%m")
    if month != meeting.strftime("%Y-%m"):
        return {"status": "partial", "warnings": ["meeting_contract_month_mismatch"], "outcomes": []}
    if as_of and meeting < date.fromisoformat(as_of[:10]):
        return {"status": "excluded", "warnings": ["contract_expired"], "outcomes": []}
    if not 0 < futures_price < 100 or current_effective_rate is None:
        return {"status": "partial", "warnings": ["invalid_price"], "outcomes": []}
    days = calendar.monthrange(meeting.year, meeting.month)[1]
    pre_days = meeting.day - 1
    post_days = days - pre_days
    if post_days <= 0:
        return {"status": "partial", "warnings": ["insufficient_post_meeting_days"], "outcomes": []}
    monthly_implied_rate = 100.0 - float(futures_price)
    post_rate = (monthly_implied_rate * days - current_effective_rate * pre_days) / post_days
    lower = math.floor(post_rate * 4) / 4
    upper = lower + 0.25
    upper_probability = min(1.0, max(0.0, (post_rate - lower) / 0.25))
    lower_probability = 1.0 - upper_probability
    outcomes = []
    for midpoint, probability in ((lower, lower_probability), (upper, upper_probability)):
        if probability <= 1e-12:
            continue
        change = round((midpoint - current_target_midpoint) * 100, 4)
        outcomes.append({
            "target_lower_bound": round(midpoint - 0.125, 3),
            "target_upper_bound": round(midpoint + 0.125, 3),
            "target_midpoint": midpoint,
            "change_bps": change,
            "probability": round(probability, 8),
            "classification": classify_change(change),
        })
    return {
        "status": "available",
        "monthly_implied_rate": round(monthly_implied_rate, 6),
        "implied_post_meeting_rate": round(post_rate, 6),
        "days_in_month": days,
        "pre_meeting_days": pre_days,
        "post_meeting_days": post_days,
        "outcomes": outcomes,
        "calculation_method": "monthly_average_fed_funds_futures_inversion",
        "calculation_version": CALCULATION_VERSION,
        "warnings": [],
    }


def calculate_repricing(
    meetings: list[dict[str, Any]], history: list[dict[str, Any]], *, now: datetime | None = None
) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    current = meetings[0] if meetings else None
    output: dict[str, Any] = {"history_available": False, "history_status": "history_insufficient", "compact_series": []}
    for suffix in ("1h", "24h", "7d"):
        output[f"probability_change_{suffix}"] = None
        output[f"expected_rate_change_{suffix}_bps"] = None
    if not current:
        return output
    matching = []
    for snapshot in history:
        previous = next((item for item in snapshot.get("meetings") or [] if item.get("meeting_date") == current.get("meeting_date")), None)
        observed = _date_or_none(snapshot.get("retrieved_at"))
        if previous and observed and observed < now:
            matching.append((observed, previous))
    for observed, previous in matching[:12]:
        output["compact_series"].append({
            "retrieved_at": _iso(observed),
            "cut_probability": previous.get("cut_probability"),
            "hold_probability": previous.get("hold_probability"),
            "hike_probability": previous.get("hike_probability"),
            "expected_target_midpoint": previous.get("expected_target_midpoint"),
        })
    for suffix, delta in (("1h", timedelta(hours=1)), ("24h", timedelta(hours=24)), ("7d", timedelta(days=7))):
        eligible = [(observed, item) for observed, item in matching if now - observed >= delta]
        if not eligible:
            continue
        observed, previous = min(eligible, key=lambda pair: abs((now - pair[0]) - delta))
        output[f"probability_change_{suffix}"] = {
            key: _delta(current.get(key), previous.get(key)) for key in ("cut_probability", "hold_probability", "hike_probability")
        }
        output[f"expected_rate_change_{suffix}_bps"] = _bps_delta(
            current.get("expected_target_midpoint"), previous.get("expected_target_midpoint")
        )
    output["history_available"] = any(output[f"probability_change_{suffix}"] is not None for suffix in ("1h", "24h", "7d"))
    output["history_status"] = "available" if output["history_available"] else "history_insufficient"
    return output


def select_source(candidates: list[dict[str, Any]], last_known_good: dict[str, Any] | None = None) -> dict[str, Any] | None:
    available = [item for item in candidates if item.get("status") in {"available", "partial"}]
    if last_known_good:
        available.append(last_known_good)
    return max(available, key=lambda item: SOURCE_RANK.get(item.get("ranking_class") or item.get("source_type"), 0), default=None)


def parse_target_range(value: Any) -> tuple[float, float] | None:
    text = str(value or "").replace("%", "").strip()
    if text.startswith("-"):
        return None
    numbers = re.findall(r"\d+(?:\.\d+)?", text)
    if len(numbers) < 2:
        return None
    lower, upper = float(numbers[0]), float(numbers[1])
    return (lower, upper) if 0 <= lower < upper <= 25 else None


def parse_probability(value: Any) -> float | None:
    try:
        number = float(str(value).replace("%", "").strip())
    except (TypeError, ValueError):
        return None
    return round(number / 100, 12) if number > 1 else number


def classify_change(change_bps: float | None) -> str:
    if change_bps is None or abs(change_bps) <= 12.5:
        return "hold"
    return "cut" if change_bps < 0 else "hike"


def not_found_snapshot(*, refresh: str, legacy_block: dict[str, Any] | None, warning: str) -> dict[str, Any]:
    now = _iso(datetime.now(UTC))
    return {
        "status": "not_found", "current_fed_state": {}, "next_meeting": None, "meetings": [],
        "repricing": {"history_available": False, "history_status": "history_insufficient"},
        "source_summary": {"selected_source": None, "selected_source_type": "not_found", "ranking_class": "not_found", "is_official_source": False, "is_reconstructed": False, "last_known_good_used": False},
        "quality": {"meeting_coverage_pct": 0.0, "probability_distribution_coverage_pct": 0.0, "official_source_coverage_pct": 0.0, "current_state_completeness_pct": 0.0, "mapping_valid_pct": 0.0, "history_depth": 0, "stale_snapshot_count": 0, "missing_contract_count": 0, "quality_score": 0.0},
        "diagnostics": {"source_attempt_count": 0, "source_success_count": 0, "source_failure_count": 0, "official_source_success": False, "vendor_source_success": False, "reconstruction_used": False, "last_known_good_used": False, "meeting_count": 0, "contract_count": 0, "valid_distribution_count": 0, "invalid_distribution_count": 0, "missing_contract_count": 0, "probability_normalization_count": 0, "history_snapshot_count": 0, "provider_calls": 0, "browser_calls": 0, "AI_called": False, "cache_used": refresh == "false"},
        "retrieved_at": now, "valid_until": None, "stale": False, "warnings": [warning], "errors": [],
        "fed_funds_futures": copy.deepcopy(legacy_block or {}), "service_role": "data provider only",
    }


def _runtime_view(payload: dict[str, Any], *, refresh: str, force_read_back: bool = False) -> dict[str, Any]:
    output = copy.deepcopy(payload)
    diagnostics = output.setdefault("diagnostics", {})
    diagnostics["provider_calls"] = 0 if refresh == "false" else diagnostics.get("provider_calls", 0)
    diagnostics["browser_calls"] = 0
    diagnostics["AI_called"] = False
    diagnostics["cache_used"] = refresh == "false" or force_read_back
    output["cache_status"] = "DB_READ_BACK" if force_read_back else "DB"
    output["stale"] = _is_stale(output)
    output["age_minutes"] = max(0.0, round((datetime.now(UTC) - (_date_or_none(output.get("retrieved_at")) or datetime.now(UTC))).total_seconds() / 60, 2))
    for meeting in output.get("meetings") or []:
        meeting["cache_status"] = output["cache_status"]
    return output


def _legacy_fed_funds_block(payload: dict[str, Any], legacy_block: dict[str, Any] | None) -> dict[str, Any]:
    block = copy.deepcopy(legacy_block or {})
    futures = dict(block.get("fed_funds_futures") or {})
    futures.setdefault("investing_fed_rate_monitor", payload)
    futures["primary_source"] = None
    futures["secondary_source"] = "Investing.com Fed Rate Monitor" if payload.get("meetings") else None
    futures["official_fed_source"] = False
    block["fed_funds_futures"] = futures
    return block["fed_funds_futures"]


def _next_meeting_summary(meeting: dict[str, Any] | None) -> dict[str, Any] | None:
    if not meeting:
        return None
    return copy.deepcopy(meeting)


def _series_value(rates: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        try:
            value = (rates.get(key) or {}).get("value")
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _valid_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _date_or_none(value: Any) -> datetime | None:
    parsed = parse_datetime(value)
    if parsed and parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _is_stale(payload: dict[str, Any]) -> bool:
    valid_until = _date_or_none(payload.get("valid_until"))
    return bool(valid_until and datetime.now(UTC) >= valid_until)


def _delta(current: Any, previous: Any) -> float | None:
    try:
        return round(float(current) - float(previous), 8)
    except (TypeError, ValueError):
        return None


def _bps_delta(current: Any, previous: Any) -> float | None:
    delta = _delta(current, previous)
    return round(delta * 100, 4) if delta is not None else None


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
