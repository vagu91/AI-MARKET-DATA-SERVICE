from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import Any

from app.core.text_normalization import normalize_payload_text
from app.models.common import Impact
from app.models.events import EconomicEvent
from app.models.macro import MacroLatestResponse
from app.services.data_freshness_service import parse_datetime
from app.services.data_integrity_service import (
    calculate_surprise,
    classify_holding_sector,
    classify_source,
    clean_text,
    freshness_label,
    news_content_status,
    sector_exposure,
    temporal_status,
)
from app.services.bls_required_series import normalize_bls_series_id
from app.services.context_extensions_service import apply_context_extensions
from app.services.news_intelligence_service import build_news_context as build_intelligence_news_context
from app.services.qqq_weight_intelligence_service import weight_quality_score

logger = logging.getLogger(__name__)


MACRO_BUCKETS = {
    "rates_and_yields": {"FEDFUNDS", "DFF", "DFEDTARL", "DFEDTARU", "SOFR", "DGS2", "DGS10", "T10Y2Y"},
    "financial_conditions": {"VIXCLS", "NFCI"},
    "growth": {"BEA:GDP", "BEA:REAL_GDP"},
    "inflation": {"CUSR0000SA0", "WPUFD4", "BEA:PCE", "BEA:CORE_PCE"},
    "labor": {"CES0000000001", "CES0500000003", "LNS14000000", "ICSA"},
    "consumer": {"BEA:PERSONAL_INCOME", "BEA:PERSONAL_SPENDING"},
}

NEWS_RELEVANCE_TERMS = {
    "nasdaq",
    "qqq",
    "ndx",
    "mnq",
    "nvidia",
    "nvda",
    "apple",
    "microsoft",
    "meta",
    "amazon",
    "google",
    "alphabet",
    "semiconductor",
    "chip",
    "fed",
    "fomc",
    "inflation",
    "rates",
    "yield",
    "payroll",
    "bls",
    "bea",
    "bureau of labor statistics",
    "bureau of economic analysis",
    "cpi",
    "ppi",
    "pce",
    "earnings",
}
NEWS_EXCLUSION_TERMS = {
    "mortgage",
    "retirement",
    "pension",
    "personal finance",
    "crypto",
    "bitcoin",
    "oil",
    "gold",
    "commodity",
}

SOURCE_URLS = {
    "FRED": "https://fred.stlouisfed.org/",
    "BLS": "https://www.bls.gov/",
    "BEA": "https://www.bea.gov/",
}

SERIES_META = {
    "FEDFUNDS": ("effective_fed_funds_rate", "percent", "monthly"),
    "DFF": ("effective_fed_funds_rate_daily", "percent", "daily"),
    "DFEDTARL": ("fed_funds_target_lower_bound", "percent", "daily"),
    "DFEDTARU": ("fed_funds_target_upper_bound", "percent", "daily"),
    "SOFR": ("secured_overnight_financing_rate", "percent", "daily"),
    "DGS2": ("treasury_2y_yield", "percent", "daily"),
    "DGS10": ("treasury_10y_yield", "percent", "daily"),
    "T10Y2Y": ("treasury_10y_minus_2y", "percent", "daily"),
    "VIXCLS": ("vix_index", "index", "daily"),
    "NFCI": ("national_financial_conditions_index", "index", "weekly"),
    "BEA:GDP": ("gdp_current_dollars", "BEA reported units", "quarterly"),
    "BEA:REAL_GDP": ("real_gdp", "BEA reported units", "quarterly"),
    "CUSR0000SA0": ("headline_cpi_index", "index", "monthly"),
    "WPUFD4": ("headline_ppi_final_demand_index", "index", "monthly"),
    "BEA:PCE": ("personal_consumption_expenditures", "BEA reported units", "monthly"),
    "BEA:CORE_PCE": ("core_pce_price_index", "BEA reported units", "monthly"),
    "CES0000000001": ("nonfarm_payrolls_level", "thousands of persons", "monthly"),
    "LNS14000000": ("unemployment_rate", "percent", "monthly"),
    "CES0500000003": ("average_hourly_earnings", "US dollars per hour", "monthly"),
    "ICSA": ("initial_jobless_claims", "thousands of claims", "weekly"),
    "BEA:PERSONAL_INCOME": ("personal_income", "BEA reported units", "monthly"),
    "BEA:PERSONAL_SPENDING": ("personal_spending", "BEA reported units", "monthly"),
}

CRITICAL_CATEGORIES = {
    "CPI",
    "PPI",
    "NFP",
    "NONFARM PAYROLLS",
    "GDP",
    "PCE",
    "FOMC",
    "RETAIL SALES",
    "ISM MANUFACTURING",
    "ISM SERVICES",
    "INITIAL JOBLESS CLAIMS",
    "JOLTS",
}

RESEARCH_PRIORITY = {
    "PCE": 1,
    "CPI": 2,
    "PPI": 3,
    "NFP": 4,
    "NONFARM PAYROLLS": 4,
    "GDP": 5,
    "FOMC": 6,
}

MONTHS = {
    "JANUARY": 1,
    "FEBRUARY": 2,
    "MARCH": 3,
    "APRIL": 4,
    "MAY": 5,
    "JUNE": 6,
    "JULY": 7,
    "AUGUST": 8,
    "SEPTEMBER": 9,
    "OCTOBER": 10,
    "NOVEMBER": 11,
    "DECEMBER": 12,
}


def build_market_context_contract(
    *,
    symbol: str,
    macro: MacroLatestResponse,
    events_today: list[EconomicEvent],
    upcoming_events: list[EconomicEvent],
    event_windows: Any,
    nasdaq_context: dict[str, Any] | None,
    news_items: list[dict[str, Any]],
    data_quality: dict[str, Any],
    db_summary: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    event_facts: list[dict[str, Any]] | None = None,
    positioning_context: dict[str, Any] | None = None,
    sentiment_context: dict[str, Any] | None = None,
    news_context_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    macro_snapshot = build_macro_snapshot(macro)
    if event_facts:
        augment_macro_snapshot_from_event_facts(macro_snapshot, event_facts)
    event_calendar = build_event_calendar(upcoming_events)
    news_context = news_context_override or build_news_context(news_items)
    section_quality = build_section_quality(
        macro_snapshot=macro_snapshot,
        event_calendar=event_calendar,
        nasdaq_context=nasdaq_context,
        news_context=news_context,
        existing_quality=data_quality,
    )
    overall = build_overall_quality(section_quality)
    generated_at = datetime.now(UTC).isoformat()
    legacy_high = [event.model_dump(mode="json") for event in event_calendar["critical_macro_events"]]
    contract = {
        "symbol": symbol.upper(),
        "generated_at_utc": generated_at,
        "service_role": "data provider only",
        "macro_snapshot": macro_snapshot,
        "event_calendar": {
            key: [event.model_dump(mode="json") for event in value]
            for key, value in event_calendar.items()
        },
        "nasdaq_context": nasdaq_context,
        "news_context": news_context,
        "positioning": positioning_context,
        "sentiment_context": sentiment_context,
        "event_windows": event_windows.model_dump(mode="json") if hasattr(event_windows, "model_dump") else event_windows,
        "data_quality": {
            **data_quality,
            "section_quality": section_quality,
            "overall_data_quality": overall,
        },
        "db_summary": db_summary,
        "metadata": {
            "decisions_delegated_to": "AI-TRADER",
            "trading_logic": "not implemented; data service only",
            **(metadata or {}),
        },
        "macro": macro.model_dump(mode="json"),
        "events_today": [event.model_dump(mode="json") for event in events_today],
        "upcoming_high_impact_events": legacy_high,
        "latest_news": {"articles": news_context["latest"], "deprecated": "Use news_context.latest."},
    }
    return normalize_payload_text(apply_context_extensions(contract))


def build_macro_snapshot(macro: MacroLatestResponse) -> dict[str, Any]:
    snapshot: dict[str, Any] = {bucket: {} for bucket in MACRO_BUCKETS}
    snapshot["provider_results"] = [item.model_dump(mode="json") for item in macro.provider_results]
    for series in macro.series:
        item = series.model_dump(mode="json")
        raw_series_id = str(item.get("series_id") or "").upper()
        series_id = normalize_bls_series_id(raw_series_id) or raw_series_id
        source = str(item.get("source") or "")
        metric, unit, frequency = SERIES_META.get(series_id, (_slug(item.get("name")), item.get("units") or item.get("unit"), None))
        valid_until = item.get("valid_until")
        normalized = {
            "series_id": item.get("series_id"),
            "name": item.get("name"),
            "value": item.get("value"),
            "latest_released_value": item.get("value"),
            "latest_released_period": item.get("data_as_of"),
            "latest_release_at": item.get("data_as_of"),
            "unit": item.get("unit") or item.get("units") or unit,
            "metric": item.get("metric") or metric,
            "frequency": item.get("frequency") or frequency,
            "data_as_of": item.get("data_as_of"),
            "source": source,
            "source_url": item.get("source_url") or SOURCE_URLS.get(source),
            "actual_is_official": classify_source(source, item.get("source_url") or SOURCE_URLS.get(source))["is_official_source"],
            "provider_type": (item.get("metadata") or {}).get("provider_type"),
            "reliability": (item.get("metadata") or {}).get("reliability"),
            "retrieved_at": (item.get("metadata") or {}).get("retrieved_at"),
            "valid_until": valid_until,
            "next_refresh_at": item.get("next_refresh_at") or valid_until,
            "freshness": freshness_label(valid_until=valid_until),
            "cache_status": item.get("cache_status") or "DB",
        }
        for bucket, keys in MACRO_BUCKETS.items():
            if series_id in keys:
                snapshot[bucket][series_id] = normalized
                break
    return snapshot


def augment_macro_snapshot_from_event_facts(snapshot: dict[str, Any], facts: list[dict[str, Any]]) -> None:
    mapping = {
        "headline_cpi_mom": ("inflation", "CUSR0000SA0", "Headline CPI MoM"),
        "headline_ppi_mom": ("inflation", "WPUFD4", "Headline PPI Final Demand MoM"),
        "headline_ppi_final_demand_mom": ("inflation", "WPUFD4", "PPI Final Demand MoM"),
        "final_demand_ppi_mom": ("inflation", "WPUFD4", "PPI Final Demand MoM"),
        "ppi_final_demand_mom": ("inflation", "WPUFD4", "PPI Final Demand MoM"),
        "nonfarm_payrolls_change": ("labor", "CES0000000001", "Nonfarm Payrolls Change"),
        "unemployment_rate": ("labor", "LNS14000000", "Unemployment Rate"),
        "average_hourly_earnings_mom": ("labor", "CES0500000003", "Average Hourly Earnings"),
        "average_hourly_earnings_yoy": ("labor", "CES0500000003", "Average Hourly Earnings"),
        "initial_jobless_claims": ("labor", "ICSA", "Initial Jobless Claims"),
    }
    provider_sources = {item.get("source") for item in snapshot.get("provider_results", [])}
    for fact in facts:
        raw = fact.get("raw_payload") if isinstance(fact.get("raw_payload"), dict) else {}
        for metric in raw.get("metrics") or []:
            if not isinstance(metric, dict):
                continue
            metric_id = str(metric.get("metric_id") or "")
            target = mapping.get(metric_id)
            if not target:
                continue
            bucket, series_id, name = target
            if snapshot.get(bucket, {}).get(series_id):
                continue
            value = metric.get("actual")
            value = metric.get("previous") if value in (None, "") else value
            if value in (None, ""):
                continue
            source = metric.get("source") or fact.get("source")
            source_url = metric.get("source_url") or fact.get("source_url")
            valid_until = metric.get("valid_until") or fact.get("valid_until")
            release_at = fact.get("release_at") or raw.get("time_utc")
            source_info = classify_source(source, source_url)
            snapshot.setdefault(bucket, {})[series_id] = {
                "series_id": series_id,
                "name": name,
                "value": value,
                "latest_released_value": value,
                "latest_released_period": raw.get("period") or fact.get("period"),
                "latest_release_at": release_at,
                "unit": metric.get("unit"),
                "metric": metric_id,
                "frequency": metric.get("frequency"),
                "data_as_of": raw.get("period") or fact.get("period"),
                "source": source,
                "source_url": source_url,
                "actual_is_official": source_info["is_official_source"],
                "provider_type": fact.get("provider_type"),
                "reliability": metric.get("reliability") or fact.get("reliability"),
                "retrieved_at": metric.get("retrieved_at") or fact.get("retrieved_at"),
                "valid_until": valid_until,
                "next_refresh_at": fact.get("next_refresh_at") or valid_until,
                "freshness": freshness_label(valid_until=valid_until, release_at=release_at, actual=metric.get("actual")),
                "cache_status": "DB",
            }
            source_upper = str(source).upper()
            canonical_source = "BLS" if ("BLS" in source_upper or "LABOR STATISTICS" in source_upper) else "BEA"
            if source and canonical_source not in provider_sources and (
                "BUREAU" in source_upper or "BLS" in source_upper or "BEA" in source_upper
            ):
                snapshot.setdefault("provider_results", []).append(
                    {
                        "source": canonical_source,
                        "provider_type": fact.get("provider_type"),
                        "retrieved_at": fact.get("retrieved_at"),
                        "freshness": "RECENT",
                        "reliability": fact.get("reliability"),
                        "is_fallback": False,
                        "errors": [],
                    }
                )
                provider_sources.add(canonical_source)


def build_event_calendar(events: list[EconomicEvent]) -> dict[str, list[EconomicEvent]]:
    critical: list[EconomicEvent] = []
    fed: list[EconomicEvent] = []
    other: list[EconomicEvent] = []
    seen: dict[str, str] = {}
    for event in events:
        enriched = _annotate_event(event, seen)
        if _flag(enriched, "invalid_period_mapping") or _flag(enriched, "is_duplicate"):
            other.append(enriched)
        elif _is_fed_communication(enriched):
            fed.append(enriched)
        elif _is_critical_macro(enriched):
            critical.append(enriched)
        else:
            other.append(enriched)
    return {
        "critical_macro_events": critical,
        "fed_communications": fed,
        "other_economic_events": other,
    }


def build_news_context(news_items: list[dict[str, Any]], limit: int = 12) -> dict[str, Any]:
    return build_intelligence_news_context(news_items, limit=limit)


def build_section_quality(
    *,
    macro_snapshot: dict[str, Any],
    event_calendar: dict[str, list[EconomicEvent]],
    nasdaq_context: dict[str, Any] | None,
    news_context: dict[str, Any],
    existing_quality: dict[str, Any],
) -> dict[str, Any]:
    macro_required = sum(len(keys) for keys in MACRO_BUCKETS.values())
    macro_present = sum(
        1 for bucket in MACRO_BUCKETS if isinstance(macro_snapshot.get(bucket), dict) for _ in macro_snapshot[bucket]
    )
    event_quality = _critical_event_quality(
        event_calendar.get("critical_macro_events", []),
        refresh_mode=str(existing_quality.get("refresh_mode") or "auto"),
    )
    nasdaq_missing = [] if nasdaq_context else ["nasdaq_context"]
    unknown_weight = (nasdaq_context.get("sector_exposure") or {}).get("unknown_weight_pct") if nasdaq_context else None
    if unknown_weight is not None and unknown_weight >= 10:
        nasdaq_missing.append("sector_exposure_unknown_weight_above_threshold")
    qqq = (nasdaq_context or {}).get("qqq_holdings") or {}
    qqq_quality = qqq.get("data_quality") or {}
    breadth = (nasdaq_context or {}).get("mega_cap_breadth") or {}
    breadth_quality = breadth.get("data_quality") or {}
    weight_coverage = float(qqq_quality.get("weight_coverage_pct") or 0.0)
    price_coverage = float(breadth_quality.get("price_coverage_pct") or 0.0)
    sector_coverage = max(0.0, 100.0 - float(unknown_weight or 0.0)) if unknown_weight is not None else 0.0
    nasdaq_score = weight_quality_score(
        method=qqq.get("weight_method"),
        weight_coverage_pct=weight_coverage,
        price_coverage_pct=price_coverage,
        sector_coverage_pct=sector_coverage,
        stale=bool(qqq_quality.get("stale")),
        issuer_semantics_quality_score=float(qqq_quality.get("issuer_semantics_quality_score", 1.0)),
    )
    if qqq.get("weight_method") == "equal_weight_proxy":
        nasdaq_missing.append("qqq_weights_equal_weight_proxy")
    if qqq_quality.get("missing_weight_count"):
        nasdaq_missing.append("qqq_weights_missing")
    news_quality = dict(news_context.get("quality") or {})
    news_missing = [] if news_context.get("latest") else ["latest"]
    if news_context.get("latest") and float(news_quality.get("news_quality_score") or 0) < 0.4:
        news_missing.append("news_quality_below_threshold")
    return {
        "macro_snapshot": {
            "completeness_score": _ratio(macro_present, macro_required),
            "freshness_score": 0.95 if macro_present else 0.0,
            "reliability_score": _average_reliability(macro_snapshot),
            "missing_fields": _missing_macro_fields(macro_snapshot),
        },
        "critical_macro_events": event_quality,
        "nasdaq_context": {
            "completeness_score": nasdaq_score["weight_quality_score"] if nasdaq_context else 0.0,
            "freshness_score": 0.65 if qqq_quality.get("stale") else nasdaq_score["weight_quality_score"],
            "reliability_score": float(qqq.get("weight_confidence") or qqq.get("reliability") or 0.0),
            **nasdaq_score,
            "weight_coverage_pct": weight_coverage,
            "official_weight_coverage_pct": float(qqq_quality.get("official_weight_coverage_pct") or 0.0),
            "price_coverage_pct": price_coverage,
            "weighted_contribution_coverage_pct": float(breadth.get("covered_weight_pct") or 0.0),
            "sector_weight_coverage_pct": sector_coverage,
            "stale_weight_count": int(qqq_quality.get("stale_weight_count") or 0),
            "missing_weight_count": int(qqq_quality.get("missing_weight_count") or 0),
            "missing_price_count": len(breadth.get("missing_price_symbols") or []),
            "missing_fields": nasdaq_missing,
        },
        "news_context": {
            "completeness_score": float(news_quality.get("completeness_score") or 0.0),
            "freshness_score": float(news_quality.get("published_at_coverage_pct") or 0.0) / 100,
            "reliability_score": round(
                sum(float(item.get("reliability") or 0) for item in news_context.get("latest") or [])
                / max(len(news_context.get("latest") or []), 1),
                3,
            ),
            **news_quality,
            "missing_fields": news_missing,
        },
    }


def build_overall_quality(section_quality: dict[str, Any]) -> dict[str, Any]:
    scores = [float(item.get("completeness_score", 0.0)) for item in section_quality.values()]
    completeness = sum(scores) / len(scores) if scores else 0.0
    missing = [
        field
        for item in section_quality.values()
        for field in item.get("missing_fields", [])
    ]
    blocking = []
    if section_quality["macro_snapshot"]["completeness_score"] < 0.7:
        blocking.append("macro_snapshot_incomplete")
    if section_quality["nasdaq_context"]["completeness_score"] < 0.5:
        blocking.append("nasdaq_context_insufficient")
    if section_quality["critical_macro_events"].get("missing_fields"):
        blocking.append("critical_event_enrichment_missing")
    if "sector_exposure_unknown_weight_above_threshold" in section_quality["nasdaq_context"].get("missing_fields", []):
        blocking.append("sector_exposure_insufficient")
    invalid_future_actual_count = int(section_quality.get("_integrity", {}).get("invalid_future_actual_count", 0))
    stale_as_recent_count = int(section_quality.get("_integrity", {}).get("stale_as_recent_count", 0))
    if invalid_future_actual_count:
        blocking.append("future_actual_detected")
    if stale_as_recent_count:
        blocking.append("stale_as_recent_detected")
    return {
        "completeness_score": round(completeness, 3),
        "freshness_score": round(sum(float(item.get("freshness_score", 0.9 if item.get("completeness_score") else 0.0)) for item in section_quality.values()) / len(section_quality), 3),
        "reliability_score": round(sum(float(item.get("reliability_score", item.get("completeness_score", 0.0))) for item in section_quality.values()) / len(section_quality), 3),
        "temporal_consistency_score": 0.0 if invalid_future_actual_count else 1.0,
        "source_integrity_score": 1.0,
        "critical_missing_count": len(missing),
        "missing_critical_fields": missing,
        "invalid_future_actual_count": invalid_future_actual_count,
        "stale_as_recent_count": stale_as_recent_count,
        "is_ready_for_market_analysis": not blocking,
        "blocking_reasons": blocking,
    }


def _critical_event_quality(events: list[EconomicEvent], *, refresh_mode: str) -> dict[str, Any]:
    if not events:
        return {
            "completeness_score": 0.0,
            "missing_fields": ["critical_macro_events"],
            "event_count": 0,
            "quantitative_event_count": 0,
            "complete_event_count": 0,
            "partial_event_count": 0,
            "missing_event_count": 0,
            "not_applicable_event_count": 0,
        }

    scores: list[float] = []
    missing: list[str] = []
    complete = 0
    partial = 0
    not_applicable = 0
    quantitative = 0
    for event in events:
        if not _quantitative_applicable(event):
            not_applicable += 1
            continue
        quantitative += 1
        applicable_fields = ["expectation", "previous"]
        release_at = parse_datetime(event.time_utc or event.date)
        if release_at is not None and datetime.now(UTC) >= release_at:
            applicable_fields.append("actual")
        has_expectation = (
            event.enrichment.forecast not in (None, "")
            or (event.enrichment.consensus_verified and event.enrichment.consensus not in (None, ""))
            or any(
                isinstance(metric, dict)
                and (
                    metric.get("forecast") not in (None, "")
                    or (
                        bool(metric.get("consensus_verified") or (metric.get("field_semantics") or {}).get("consensus_verified"))
                        and metric.get("consensus") not in (None, "")
                    )
                )
                for metric in event.enrichment.metrics
            )
        )
        present = {"expectation"} if has_expectation else set()
        for field in ("previous", "actual"):
            if field not in applicable_fields:
                continue
            if getattr(event.enrichment, field, None) not in (None, "") or any(
                isinstance(metric, dict) and metric.get(field) not in (None, "")
                for metric in event.enrichment.metrics
            ):
                present.add(field)
        score = len(present) / len(applicable_fields) if applicable_fields else 1.0
        scores.append(score)
        if not present:
            missing_key = f"event_enrichment:{event.event_id}"
            missing.append(missing_key)
            logger.warning(
                "event_quality_invariant_failed",
                extra={
                    "event_id": event.event_id,
                    "fact_key": missing_key,
                    "fact_type": "macro_event_enrichment",
                    "refresh_mode": refresh_mode,
                    "cache_status": event.enrichment.cache_status,
                    "provider_type": str(event.enrichment.provider_type or ""),
                    "valid_until": str(event.enrichment.valid_until or ""),
                },
            )
        elif score >= 1.0:
            complete += 1
        else:
            partial += 1

    return {
        "completeness_score": round(sum(scores) / len(scores), 3) if scores else 1.0,
        "missing_fields": missing,
        "event_count": len(events),
        "quantitative_event_count": quantitative,
        "complete_event_count": complete,
        "partial_event_count": partial,
        "missing_event_count": len(missing),
        "not_applicable_event_count": not_applicable,
    }


def materialize_nasdaq_context_from_facts(facts_by_type: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    holdings = _latest_raw(facts_by_type.get("qqq_holdings", []))
    snapshot = _latest_raw(facts_by_type.get("mega_cap_snapshot", []) or facts_by_type.get("nasdaq_context", []))
    breadth = _latest_raw(facts_by_type.get("mega_cap_breadth", []))
    earnings = _latest_raw(facts_by_type.get("earnings_event", []))
    if not any((holdings, snapshot, breadth, earnings)):
        return None
    return normalize_nasdaq_context(
        {
            "qqq_holdings": holdings,
            "qqq_holdings_summary": holdings,
            "mega_cap_snapshot": snapshot,
            "mega_cap_breadth": breadth,
            "upcoming_earnings": earnings,
            "latest_news": {"articles": []},
        }
    )


def normalize_nasdaq_context(context: Any) -> dict[str, Any] | None:
    if context is None:
        return None
    data = context.model_dump(mode="json") if hasattr(context, "model_dump") else dict(context)
    holdings = data.get("qqq_holdings") or data.get("qqq_holdings_summary") or {}
    summary = data.get("qqq_holdings_summary") or holdings or {}
    all_holdings = [classify_holding_sector(item) for item in (holdings.get("holdings") or summary.get("top_holdings") or [])]
    all_holdings.sort(key=lambda item: float(item.get("weight_pct") or item.get("weight") or -1), reverse=True)
    top_holdings = all_holdings[:15]
    snapshot = data.get("mega_cap_snapshot") or {}
    breadth = data.get("mega_cap_breadth") or {}
    earnings = data.get("upcoming_earnings") or data.get("earnings") or {}
    holdings_count = holdings.get("holdings_count") or len(holdings.get("holdings") or top_holdings or [])
    return {
        "qqq_holdings": {
            "status": holdings.get("status") or ("found" if holdings.get("holdings") or top_holdings else "not_found"),
            "as_of": holdings.get("as_of") or summary.get("as_of"),
            "holdings_count": holdings_count,
            "holdings": all_holdings,
            "top_holdings": top_holdings,
            "source": holdings.get("source") or summary.get("source"),
            "source_url": holdings.get("weight_source_url") or holdings.get("source_url"),
            "reliability": holdings.get("reliability") or summary.get("reliability"),
            "is_proxy": bool(holdings.get("is_proxy")),
            "proxy_for": holdings.get("proxy_for"),
            "weight_data_available": holdings.get("weight_data_available"),
            "official_etf_holdings": holdings.get("official_etf_holdings", True),
            "weight_method": holdings.get("weight_method"),
            "weight_source": holdings.get("weight_source") or holdings.get("source"),
            "weight_source_url": holdings.get("weight_source_url") or holdings.get("source_url"),
            "weight_as_of": holdings.get("weight_as_of") or holdings.get("as_of"),
            "weight_valid_until": holdings.get("weight_valid_until"),
            "weight_verified": bool(holdings.get("weight_verified")),
            "weight_is_official": bool(holdings.get("weight_is_official")),
            "weight_is_reconstructed": bool(holdings.get("weight_is_reconstructed")),
            "weight_calculation_validated": bool(holdings.get("weight_verified")),
            "official_weight_verified": bool(holdings.get("weight_verified") and holdings.get("weight_is_official")),
            "weight_method_classification": (
                "official_etf_weight"
                if holdings.get("weight_verified") and holdings.get("weight_is_official")
                else "reconstructed_market_cap_proxy"
                if "reconstruct" in str(holdings.get("weight_method") or "")
                else "equal_weight_proxy"
                if "equal" in str(holdings.get("weight_method") or "")
                else "unavailable"
            ),
            "weight_confidence": float(holdings.get("weight_confidence") or 0.0),
            "data_quality": holdings.get("data_quality") or {},
        },
        "mega_cap_snapshot": {
            "tracked_count": (snapshot.get("data_quality") or {}).get("tracked_count") or len(snapshot.get("stocks") or []),
            "resolved_count": (snapshot.get("data_quality") or {}).get("resolved_count") or len(snapshot.get("stocks") or []),
            "stocks": snapshot.get("stocks") or [],
            "retrieved_at": snapshot.get("retrieved_at"),
            "source": snapshot.get("source"),
            "reliability": snapshot.get("reliability"),
        },
        "mega_cap_breadth": breadth,
        "earnings": {"upcoming": earnings.get("events") or [], "data_quality": earnings.get("data_quality", {})},
        "sector_exposure": sector_exposure(
            all_holdings,
            total_holdings_count=holdings_count,
            coverage_scope="complete_portfolio" if holdings.get("weight_data_available") and holdings_count == len(all_holdings) else None,
        ),
        "data_quality": data.get("metadata") or {},
    }


def _annotate_event(event: EconomicEvent, seen: dict[str, dict[str, Any]]) -> EconomicEvent:
    payload = event.model_copy(deep=True)
    _reject_event_future_actual(payload)
    category = _category_key(payload)
    event_type = _event_type(payload)
    invalid = _invalid_period_mapping(payload)
    duplicate_key, duplicate_confidence, duplicate_reason = _dedup_signature(payload, event_type)
    duplicate_record = seen.get(duplicate_key)
    duplicate_of = duplicate_record.get("event_id") if duplicate_record else None
    if duplicate_record is None:
        seen[duplicate_key] = {"event_id": payload.event_id, "confidence": duplicate_confidence, "reason": duplicate_reason}
    if payload.enrichment.metrics:
        payload.enrichment.metrics = [_normalize_metric(metric, payload) for metric in payload.enrichment.metrics]
    payload.enrichment.summary = _enrichment_summary(payload.enrichment.model_dump(mode="json"))
    if not payload.enrichment.metrics:
        payload.enrichment.metrics = _legacy_metric(payload)
    payload.enrichment.summary.update(
        {
            "event_type": event_type,
            "quantitative_fields_applicable": _quantitative_applicable(payload),
            "research_priority": RESEARCH_PRIORITY.get(category, 99),
            "is_duplicate": duplicate_of is not None,
            "duplicate_of_event_id": duplicate_of,
            "invalid_period_mapping": invalid,
            "temporal_status": temporal_status(
                release_at=payload.time_utc,
                actual=payload.enrichment.actual or _first_metric_value(payload.enrichment.metrics, "actual"),
                valid_until=payload.enrichment.valid_until,
                invalid=invalid,
                duplicate=duplicate_of is not None,
            ),
            "deduplication_confidence": duplicate_confidence if duplicate_of is not None else 0.0,
            "duplicate_confidence_label": "exact" if duplicate_confidence >= 1.0 else "high" if duplicate_confidence >= 0.85 else "medium" if duplicate_confidence >= 0.65 else "low",
            "duplicate_reason": duplicate_reason if duplicate_of is not None else None,
            "possible_duplicate": False,
        }
    )
    if category == "NFP":
        named_period = _period_key(payload.name)
        month_name, _, year = named_period.partition(":")
        payload.enrichment.summary.update(
            {
                "release_period": f"{month_name.title()} {year}" if month_name and year else None,
                "release_month": month_name.title() if month_name else None,
                "release_date": payload.date,
                "period_date_consistent": not invalid,
                "calendar_verified": not invalid,
            }
        )
    if category == "FOMC":
        payload.enrichment.fomc_context = _fomc_context(payload)
    return payload


def _dedup_signature(event: EconomicEvent, event_type: str) -> tuple[str, float, str]:
    source_url = str(event.source_url or "").strip().lower()
    source_event_id = str(event.event_id or "").strip().lower()
    title = _dedup_text(event.name)
    speaker = _speaker_key(event.name)
    period = _period_key(event.name)
    release_time = event.time_utc or event.date
    if source_event_id and not source_url and not source_event_id.startswith(("fed-", "event-")):
        return f"id:{source_event_id}", 1.0, "same_source_event_id"
    return (
        f"event:{_category_key(event)}:{event_type}:{event.date}:{release_time}:{title}:{speaker}:{period}:{source_url}",
        0.9 if source_url else 0.75,
        "same_specific_title_time_speaker_period_source" if source_url else "same_specific_title_time_speaker_period",
    )


def _dedup_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")


def _speaker_key(value: Any) -> str:
    text = str(value or "")
    match = re.search(r"\b(?:Governor|Chair|President|Fed's|Fed)\s+([A-Z][A-Za-z.-]+)", text)
    return match.group(1).lower() if match else ""


def _period_key(value: Any) -> str:
    text = str(value or "").upper()
    month = next((name for name in MONTHS if name in text), "")
    year_match = re.search(r"\b(20\d{2})\b", text)
    return f"{month}:{year_match.group(1) if year_match else ''}"


def _reject_event_future_actual(event: EconomicEvent) -> None:
    release = parse_datetime(event.time_utc or event.date)
    if release is None or datetime.now(UTC) >= release:
        return
    rejected = False
    if event.enrichment.actual not in (None, ""):
        event.enrichment.actual = None
        rejected = True
    for metric in event.enrichment.metrics:
        if isinstance(metric, dict) and metric.get("actual") not in (None, ""):
            metric["actual"] = None
            semantics = dict(metric.get("field_semantics") or {})
            semantics["actual_is_official"] = False
            semantics["actual_release_verified"] = False
            metric["field_semantics"] = semantics
            rejected = True
    if rejected and "actual_before_release_rejected" not in event.enrichment.warnings:
        event.enrichment.warnings.append("actual_before_release_rejected")


def _legacy_metric(event: EconomicEvent) -> list[dict[str, Any]]:
    enrichment = event.enrichment
    if not enrichment.source_url or not any([enrichment.forecast, enrichment.previous, enrichment.consensus, enrichment.actual]):
        return []
    category = _category_key(event)
    metric_id, label, unit, frequency = _primary_metric(category)
    if not metric_id:
        return []
    return [
        {
            "metric_id": metric_id,
            "label": label,
            "value_type": "numeric_or_percent",
            "frequency": frequency,
            "forecast": enrichment.forecast,
            "consensus": enrichment.consensus,
            "previous": enrichment.previous,
            "actual": enrichment.actual,
            "unit": unit,
            "source": enrichment.source,
            "source_url": enrichment.source_url,
            "provider_type": enrichment.provider_type,
            "retrieved_at": enrichment.retrieved_at,
            "valid_until": enrichment.valid_until,
            "reliability": enrichment.reliability,
            "confidence": enrichment.confidence,
            "field_semantics": {
                "forecast_is_consensus": enrichment.forecast is not None and enrichment.forecast == enrichment.consensus,
                "forecast_origin": "source_forecast",
                "period_match": "period_mismatch" not in enrichment.warnings,
            },
            "warnings": enrichment.warnings,
        }
    ]


def _enrichment_summary(enrichment: dict[str, Any]) -> dict[str, Any]:
    metrics = [metric for metric in enrichment.get("metrics") or [] if isinstance(metric, dict)]
    fields = ["forecast", "consensus", "previous", "actual"]
    values = {
        field: enrichment.get(field) not in (None, "") or any(metric.get(field) not in (None, "") for metric in metrics)
        for field in fields
    }
    filled = sum(1 for field in fields if values[field])
    verified_consensus = bool(enrichment.get("consensus_verified") and enrichment.get("consensus") not in (None, "")) or any(
        metric.get("consensus") not in (None, "")
        and bool(metric.get("consensus_verified") or (metric.get("field_semantics") or {}).get("consensus_verified"))
        for metric in metrics
    )
    single_source_forecast = any(
        metric.get("forecast") not in (None, "")
        and str(metric.get("forecast_origin") or (metric.get("field_semantics") or {}).get("forecast_origin") or "").lower()
        in {"single_institution", "institution", "source_forecast"}
        for metric in metrics
    ) or (
        enrichment.get("forecast") not in (None, "")
        and str(enrichment.get("forecast_origin") or "").lower() in {"single_institution", "institution", "source_forecast"}
    )
    estimate_distribution = any(
        any(metric.get(field) not in (None, "") for field in ("estimate_count", "estimate_low", "estimate_high", "median_estimate", "average_estimate"))
        for metric in metrics
    ) or any(
        enrichment.get(field) not in (None, "")
        for field in ("estimate_count", "estimate_low", "estimate_high", "median_estimate", "average_estimate")
    )
    surprise_ready = any(
        metric.get("actual") not in (None, "")
        and bool((metric.get("field_semantics") or {}).get("actual_is_official"))
        and (
            (
                metric.get("consensus") not in (None, "")
                and bool(metric.get("consensus_verified") or (metric.get("field_semantics") or {}).get("consensus_verified"))
            )
            or metric.get("forecast") not in (None, "")
        )
        for metric in metrics
    )
    return {
        "has_forecast": values["forecast"],
        "has_consensus": values["consensus"],
        "has_previous": values["previous"],
        "has_actual": values["actual"],
        "has_verified_consensus": verified_consensus,
        "has_single_source_forecast": single_source_forecast,
        "has_estimate_distribution": estimate_distribution,
        "surprise_ready": surprise_ready,
        "completeness_score": round(filled / len(fields), 3),
    }


def _fomc_context(event: EconomicEvent) -> dict[str, Any]:
    return {
        "meeting_date": event.date,
        "decision_time_utc": event.time_utc.isoformat() if event.time_utc else None,
        "press_conference_time_utc": event.time_utc.isoformat() if "PRESS CONFERENCE" in event.name.upper() and event.time_utc else None,
        "current_target_range_lower": None,
        "current_target_range_upper": None,
        "expected_target_range_lower": None,
        "expected_target_range_upper": None,
        "expected_action": "unknown",
        "expected_change_bps": None,
        "probability_hold": None,
        "probability_cut": None,
        "probability_hike": None,
        "previous_action": None,
        "source": event.enrichment.source,
        "source_url": event.enrichment.source_url,
        "retrieved_at": event.enrichment.retrieved_at.isoformat() if event.enrichment.retrieved_at else None,
        "valid_until": event.enrichment.valid_until.isoformat() if event.enrichment.valid_until else None,
        "reliability": event.enrichment.reliability,
        "confidence": event.enrichment.confidence,
        "warnings": event.enrichment.warnings,
    }


def _primary_metric(category: str) -> tuple[str | None, str | None, str | None, str | None]:
    mapping = {
        "CPI": ("headline_cpi_mom", "Headline CPI MoM", "percent", "MoM"),
        "PPI": ("headline_ppi_mom", "Headline PPI MoM", "percent", "MoM"),
        "PCE": ("headline_pce_mom", "Headline PCE MoM", "percent", "MoM"),
        "GDP": ("real_gdp_annualized_qoq", "Real GDP Annualized QoQ", "percent", "QoQ annualized"),
        "NFP": ("nonfarm_payrolls_change", "Nonfarm Payrolls Change", "thousands of jobs", "monthly"),
        "NONFARM PAYROLLS": ("nonfarm_payrolls_change", "Nonfarm Payrolls Change", "thousands of jobs", "monthly"),
    }
    return mapping.get(category, (None, None, None, None))


def _is_critical_macro(event: EconomicEvent) -> bool:
    return event.impact == Impact.HIGH and _category_key(event) in CRITICAL_CATEGORIES and _quantitative_applicable(event)


def _is_fed_communication(event: EconomicEvent) -> bool:
    name = event.name.upper()
    return any(token in name for token in ("FED SPEECH", "SPEECH", "TESTIMONY", "PRESS CONFERENCE", "MINUTES")) and "DECISION" not in name


def _quantitative_applicable(event: EconomicEvent) -> bool:
    name = event.name.upper()
    if _is_fed_communication(event):
        return False
    if _category_key(event) == "FOMC" and "PRESS CONFERENCE" in name:
        return False
    return True


def _event_type(event: EconomicEvent) -> str:
    name = event.name.upper()
    if _is_fed_communication(event):
        return "fed_communication"
    if _category_key(event) == "FOMC":
        return "fomc_decision"
    if "EMPLOYMENT SITUATION" in name or "PAYROLL" in name:
        return "employment_situation"
    return _slug(event.category or event.name)


def _invalid_period_mapping(event: EconomicEvent) -> bool:
    category = _category_key(event)
    if category not in {"NFP", "NONFARM PAYROLLS"} and "EMPLOYMENT SITUATION" not in event.name.upper():
        return False
    named_month = next((number for name, number in MONTHS.items() if name in event.name.upper()), None)
    if not named_month:
        return False
    try:
        release = datetime.fromisoformat(event.date)
    except ValueError:
        return False
    expected = release.month - 1 or 12
    return named_month != expected


def _category_key(event: EconomicEvent) -> str:
    category = str(event.category or "").upper()
    if "NONFARM" in category or "PAYROLL" in category:
        return "NFP"
    return category


def _flag(event: EconomicEvent, name: str) -> bool:
    return bool((event.enrichment.summary or {}).get(name))


def _news_item(item: dict[str, Any]) -> dict[str, Any]:
    source_info = classify_source(item.get("source"), item.get("source_url") or item.get("url"))
    valid_until = item.get("valid_until")
    source_url = item.get("source_url") or item.get("url")
    aggregator_url = source_url if "news.google.com" in str(source_url or "") else None
    canonical_url = item.get("canonical_url") or (None if aggregator_url else source_url)
    source_text_available = bool(item.get("source_text_available") or item.get("summary") or item.get("content_snippet"))
    summary = clean_text(item.get("summary") or item.get("content_snippet")) if source_text_available else None
    warnings = ["summary_missing", "summary_source_unavailable"] if not summary else []
    if aggregator_url and not canonical_url:
        warnings.append("canonical_unresolved")
    if not item.get("published_at"):
        warnings.append("published_at_missing")
    return {
        "title": clean_text(item.get("title")),
        "summary": summary,
        "source": clean_text(item.get("source")),
        "source_url": source_url,
        "canonical_url": canonical_url,
        "aggregator_url": aggregator_url,
        "canonical_status": "canonical_resolved" if canonical_url else ("canonical_unresolved" if aggregator_url else "canonical_unavailable"),
        "redirect_chain": item.get("redirect_chain") or [],
        "summary_source_type": item.get("summary_source_type"),
        "summary_source_url": item.get("summary_source_url") or source_url,
        "source_text_available": source_text_available,
        "published_at": item.get("published_at"),
        "retrieved_at": item.get("retrieved_at"),
        "symbols": item.get("symbols") or [],
        "topics": item.get("topics") or [],
        "entities": item.get("entities") or item.get("symbols") or [],
        "event_type": item.get("event_type"),
        "relevance": item.get("relevance"),
        "relevance_score": {"HIGH": 1.0, "MEDIUM": 0.6, "LOW": 0.3}.get(str(item.get("relevance") or "").upper(), 0.5),
        "reliability": item.get("reliability"),
        "provider_type": item.get("provider_type"),
        "valid_until": valid_until,
        "freshness": freshness_label(valid_until=valid_until),
        "content_status": news_content_status(item),
        **source_info,
        "warnings": warnings,
    }


def _news_relevant_to_mnq(item: dict[str, Any]) -> bool:
    text = " ".join(
        str(value or "")
        for value in [
            item.get("title"),
            item.get("summary"),
            item.get("source"),
            " ".join(item.get("topics") or []),
            " ".join(item.get("symbols") or []),
        ]
    ).lower()
    if any(term in text for term in NEWS_EXCLUSION_TERMS) and not any(term in text for term in NEWS_RELEVANCE_TERMS):
        return False
    if str(item.get("relevance") or "").upper() == "LOW" and not any(term in text for term in NEWS_RELEVANCE_TERMS):
        return False
    if " ai " in f" {text} " and not any(term in text for term in NEWS_RELEVANCE_TERMS - {"fed"}):
        return False
    if item.get("symbols"):
        return True
    return any(term in text for term in NEWS_RELEVANCE_TERMS)


def _topic_key(topic: str) -> str:
    lowered = str(topic).lower().replace(" ", "_")
    if lowered in {"jobs", "payrolls"}:
        return "labor"
    if lowered == "fed":
        return "fed"
    if lowered == "mega-cap":
        return "mega_cap"
    return lowered


def _latest_raw(facts: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not facts:
        return None
    raw = facts[0].get("raw_payload")
    return raw if isinstance(raw, dict) else None


def _normalize_metric(metric: dict[str, Any], event: EconomicEvent) -> dict[str, Any]:
    metric = dict(metric)
    source_info = classify_source(metric.get("source") or event.enrichment.source, metric.get("source_url") or event.enrichment.source_url)
    semantics = dict(metric.get("field_semantics") or {})
    semantics.setdefault("forecast_is_consensus", False)
    semantics.setdefault("forecast_origin", "unknown")
    semantics.setdefault("consensus_verified", False)
    semantics.setdefault("actual_is_official", source_info["is_official_source"] and metric.get("actual") not in (None, ""))
    semantics.setdefault("actual_release_verified", metric.get("actual") not in (None, "") and temporal_status(release_at=event.time_utc, actual=metric.get("actual")) == "released")
    semantics.setdefault("period_match", True)
    semantics.setdefault("release_time_match", True)
    metric["field_semantics"] = semantics
    actual_verified = bool(semantics.get("actual_is_official") and semantics.get("actual_release_verified"))
    consensus_verified = bool(semantics.get("consensus_verified") or metric.get("consensus_verified"))
    surprise = calculate_surprise(metric) if actual_verified and (consensus_verified or metric.get("forecast") not in (None, "")) else None
    if surprise:
        baseline_name = "consensus" if consensus_verified and metric.get("consensus") not in (None, "") else "forecast"
        baseline = metric.get(baseline_name)
        actual = metric.get("actual")
        surprise_value = surprise["vs_consensus"] if baseline_name == "consensus" else surprise["vs_forecast"]
        try:
            surprise_pct = None if float(str(baseline).replace("%", "").replace(",", "")) == 0 else round(
                float(surprise_value) / abs(float(str(baseline).replace("%", "").replace(",", ""))) * 100,
                6,
            )
        except (TypeError, ValueError):
            surprise_pct = None
        surprise.update({
            "surprise_value": surprise_value,
            "surprise_pct": surprise_pct,
            "surprise_direction": surprise.get("direction"),
            "surprise_baseline": baseline_name,
            "actual": actual,
            "baseline_value": baseline,
        })
        metric["surprise"] = surprise
    return metric


def _first_metric_value(metrics: list[dict[str, Any]], field: str) -> Any:
    for metric in metrics:
        if isinstance(metric, dict) and metric.get(field) not in (None, ""):
            return metric.get(field)
    return None


def _average_reliability(snapshot: dict[str, Any]) -> float:
    values = [
        item.get("reliability")
        for bucket in MACRO_BUCKETS
        for item in snapshot.get(bucket, {}).values()
        if item.get("reliability") is not None
    ]
    return round(sum(float(value) for value in values) / len(values), 3) if values else 0.0


def _missing_macro_fields(snapshot: dict[str, Any]) -> list[str]:
    missing = []
    for bucket, keys in MACRO_BUCKETS.items():
        present = set(snapshot.get(bucket, {}))
        missing.extend(sorted(keys - present))
    return missing


def _ratio(value: int, total: int) -> float:
    return round(value / total, 3) if total else 0.0


def _slug(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace(":", "_").replace("/", "_")
