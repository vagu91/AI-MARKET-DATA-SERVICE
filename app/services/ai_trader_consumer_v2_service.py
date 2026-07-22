from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.market_context_hardening_service import harden_market_context
from app.services.market_session_service import NEW_YORK
from app.services.temporal_domain_service import temporal_event_state


logger = logging.getLogger(__name__)
CONTRACT_NAME = "ai_trader_market_context_consumer"
SCHEMA_VERSION = "2.1"
INCLUDED_SECTIONS = [
    "readiness",
    "snapshot_summary",
    "macro",
    "event_risk",
    "rates",
    "risk",
    "positioning",
    "nasdaq",
    "earnings",
    "news",
    "sentiment",
    "market_schedule",
    "quality",
]
EXCLUDED_DEBUG_SECTIONS = [
    "provider_diagnostics",
    "source_attempts",
    "fallback_chain",
    "raw_normalized_structures",
    "legacy_duplicate_fields",
    "enrichment_metadata",
    "detailed_history",
    "full_qqq_holdings",
    "multi_year_calendar",
    "raw_fed_monitor",
    "pipeline_counters",
]


def build_ai_trader_consumer_v2(
    full: dict[str, Any],
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    generated_at = parse_datetime(full.get("generated_at_utc") or full.get("generated_at"))
    hardened = harden_market_context(full, settings=settings, now=generated_at)
    readiness = dict(hardened.get("readiness") or {})
    ai_enrichment = _ai_enrichment(hardened.get("ai_enrichment") or {})
    research = _research(hardened.get("research") or {})
    temporal_pending = _has_temporal_status(
        hardened,
        {"AWAITING_ACTUAL", "AWAITING_OUTCOME", "ACTUAL_UNAVAILABLE"},
    )
    ai_pending = ai_enrichment["status"] in {"PENDING", "RUNNING", "PARTIAL", "FAILED"}
    research_pending = research["status"] in {"PENDING", "RUNNING", "PARTIAL", "FAILED"}
    research_blocking = bool(research["blocking_gaps"])
    semantic_incompatible = _has_semantic_actual_mismatch(hardened)
    if temporal_pending or ai_pending or research_pending or research_blocking or semantic_incompatible:
        readiness["ready_for_full_analysis"] = False
        readiness["full_analysis_confidence"] = min(
            float(readiness.get("full_analysis_confidence") or 0.0),
            0.5,
        )
        reasons = list(readiness.get("degrading_reasons") or [])
        if temporal_pending:
            reasons.append("temporal_data_pending")
        if ai_pending:
            reasons.append("ai_enrichment_pending")
        if research_pending:
            reasons.append("research_pending_or_incomplete")
        if research_blocking:
            reasons.append("research_blocking_gaps")
        if semantic_incompatible:
            reasons.append("critical_actual_semantic_mismatch")
        readiness["degrading_reasons"] = sorted(set(reasons))
    section_status = readiness.get("section_status") or {}
    sections_available = sorted(
        key for key, value in section_status.items()
        if value in {"AVAILABLE", "LAST_KNOWN_GOOD", "NO_DATA_EXPECTED", "NO_RELEVANT_DATA", "NO_RELEVANT_MARKETS"}
    )
    sections_missing = sorted(key for key in section_status if key not in sections_available)
    generated = hardened.get("generated_at_utc") or hardened.get("generated_at")
    consumer = {
        "contract": CONTRACT_NAME,
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": hardened.get("snapshot_id"),
        "snapshot_revision": hardened.get("snapshot_revision"),
        "symbol": hardened.get("symbol") or "MNQ",
        "generated_at": generated,
        "data_as_of": hardened.get("data_as_of") or generated,
        "context_date": (hardened.get("market_schedule") or {}).get("context_date"),
        "market_session_status": (hardened.get("market_schedule") or {}).get("market_session_status"),
        "readiness_status": readiness.get("status"),
        "ready_for_trading_context": readiness.get("ready_for_trading_context", False),
        "ready_for_full_analysis": readiness.get("ready_for_full_analysis", False),
        "available_data_confidence": readiness.get("available_data_confidence", 0.0),
        "full_analysis_confidence": readiness.get("full_analysis_confidence", 0.0),
        "sections_available": sections_available,
        "sections_missing": sections_missing,
        "readiness": readiness,
        "snapshot_summary": _snapshot_summary(hardened),
        "macro": _macro(hardened.get("macro_snapshot") or {}, now=generated_at),
        "event_risk": _event_risk(hardened),
        "rates": _rates(hardened.get("rates_expectations") or {}),
        "risk": _risk(hardened.get("risk_context") or {}),
        "positioning": _positioning(hardened.get("positioning") or {}),
        "nasdaq": _nasdaq(hardened.get("nasdaq_context") or {}),
        "earnings": _earnings(hardened),
        "news": _news(
            hardened.get("news_context") or {},
            hardened.get("news_digest") or {},
            hardened.get("market_schedule") or {},
        ),
        "sentiment": _sentiment(hardened.get("sentiment_context") or {}),
        "market_schedule": _schedule(hardened.get("market_schedule") or {}),
        "quality": hardened.get("quality") or {},
        "lifecycle": ((hardened.get("metadata") or {}).get("data_lifecycle") or hardened.get("lifecycle") or {}),
        "ai_enrichment": ai_enrichment,
        "research": research,
        "warnings": _warnings(hardened),
    }
    _enforce_payload_limit(consumer)
    size = _payload_size(consumer)
    logger.info("consumer_payload_materialized", extra={"payload_size_bytes": size})
    logger.info(
        "consumer_payload_size_validated",
        extra={"payload_size_bytes": size, "under_90kb": size < 90_000},
    )
    logger.info("consumer_contract_validated", extra={"contract": CONTRACT_NAME, "schema_version": SCHEMA_VERSION})
    return consumer


def _enforce_payload_limit(consumer: dict[str, Any], limit: int = 90_000) -> None:
    if _payload_size(consumer) < limit:
        return
    news = consumer.get("news") or {}
    event_risk = consumer.get("event_risk") or {}
    earnings = consumer.get("earnings") or {}
    for container, key, keep in (
        (news, "articles", 4),
        (news, "clusters", 4),
        (news, "current_drivers", 4),
        (news, "previous_session_drivers", 4),
        (event_risk, "critical_events", 4),
        (event_risk.get("xtb_us_macro_calendar") or {}, "events", 8),
        (earnings, "upcoming_mega_cap_earnings_14d", 10),
        (earnings, "released_earnings", 10),
    ):
        if isinstance(container.get(key), list):
            container[key] = container[key][:keep]
    warnings = list(consumer.get("warnings") or [])
    warnings.append({"code": "consumer_payload_truncated", "count": 1, "blocking": False})
    consumer["warnings"] = warnings
    if _payload_size(consumer) >= limit:
        # Quality remains present, but verbose debug-only provider traces are not part of the consumer contract.
        quality = consumer.get("quality") or {}
        consumer["quality"] = {
            key: value for key, value in quality.items()
            if key in {"section_quality", "overall_data_quality", "pipeline_integrity", "consumer_quality"}
        }


def _ai_enrichment(value: dict[str, Any]) -> dict[str, Any]:
    status = str(value.get("status") or "NOT_REQUIRED").upper()
    allowed = {"NOT_REQUIRED", "PENDING", "RUNNING", "PARTIAL", "SUCCEEDED", "NO_DATA", "FAILED"}
    normalized_status = status if status in allowed else "FAILED"
    return {
        "status": normalized_status,
        "job_ids": list(value.get("job_ids") or []),
        "requested_at": value.get("requested_at"),
        "completed_at": value.get("completed_at"),
        "pending_fields": list(value.get("pending_fields") or []),
        "accepted_fields": list(value.get("accepted_fields") or []),
        "rejected_fields": list(value.get("rejected_fields") or []),
        "policy_version": value.get("policy_version"),
        "prompt_version": value.get("prompt_version"),
        "last_error": value.get("last_error"),
    }


def _research(value: dict[str, Any]) -> dict[str, Any]:
    status = str(value.get("status") or "NOT_REQUIRED").upper()
    allowed = {"NOT_REQUIRED", "PENDING", "RUNNING", "PARTIAL", "SUCCEEDED", "NO_DATA", "FAILED"}
    normalized_status = status if status in allowed else "FAILED"
    return {
        "status": normalized_status,
        "run_id": value.get("run_id"), "snapshot_id": value.get("snapshot_id"),
        "started_at": value.get("started_at"), "completed_at": value.get("completed_at"),
        "data_as_of": value.get("data_as_of"), "fresh_until": value.get("fresh_until"),
        "coverage_score": float(value.get("coverage_score") or 0),
        "required_topics": list(value.get("required_topics") or []),
        "completed_topics": list(value.get("completed_topics") or []),
        "missing_topics": list(value.get("missing_topics") or []),
        "blocking_gaps": list(value.get("blocking_gaps") or []),
        "non_blocking_gaps": list(value.get("non_blocking_gaps") or []),
        "claim_count": int(value.get("claim_count") or 0),
        "evidence_count": int(value.get("evidence_count") or 0),
        "key_verified_drivers": list(value.get("key_verified_drivers") or [])[:8],
        "critical_evidence_references": list(value.get("critical_evidence_references") or [])[:8],
        "source_domains": list(value.get("source_domains") or [])[:12],
        "warnings": list(value.get("warnings") or [])[:12],
        "research_complete": normalized_status in {"NOT_REQUIRED", "SUCCEEDED"},
        "research_partial": normalized_status == "PARTIAL",
        "research_unavailable": normalized_status in {"NO_DATA", "FAILED"},
    }


def _has_temporal_status(value: Any, statuses: set[str]) -> bool:
    if isinstance(value, dict):
        if str(value.get("temporal_status") or value.get("release_status") or "").upper() in statuses:
            return True
        return any(_has_temporal_status(item, statuses) for item in value.values())
    if isinstance(value, list):
        return any(_has_temporal_status(item, statuses) for item in value)
    return False


def _has_semantic_actual_mismatch(value: Any) -> bool:
    if isinstance(value, dict):
        semantics = value.get("actual_semantics")
        if isinstance(semantics, dict) and semantics.get("semantic_compatible") is False:
            return True
        return any(_has_semantic_actual_mismatch(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_semantic_actual_mismatch(item) for item in value)
    return False


def _snapshot_summary(full: dict[str, Any]) -> dict[str, Any]:
    news = full.get("news_context") or {}
    event_context = full.get("events_today_context") or {}
    nasdaq = full.get("nasdaq_context") or {}
    earnings = (nasdaq.get("earnings") or {}).get("upcoming") or []
    runtime = (full.get("metadata") or {}).get("multi_source_runtime") or {}
    return {
        "generated_at": full.get("generated_at_utc") or full.get("generated_at"),
        "symbol": full.get("symbol"),
        "readiness_status": (full.get("readiness") or {}).get("status"),
        "ready": (full.get("readiness") or {}).get("ready_for_trading_context"),
        "critical_error_count": (full.get("readiness") or {}).get("critical_error_count"),
        "market_session_status": (full.get("market_schedule") or {}).get("market_session_status"),
        "events_today_status": event_context.get("status"),
        "events_today_count": event_context.get("event_count"),
        "news_status": news.get("status"),
        "news_article_count": news.get("accepted_article_count"),
        "next_earnings_count_14d": len(earnings),
        "risk_context_status": (full.get("risk_context") or {}).get("status"),
        "cache_used": bool(runtime.get("cache_used") or runtime.get("db_hits") or runtime.get("refresh_mode") == "false"),
    }


def _macro(snapshot: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    rates = snapshot.get("rates_and_yields") or {}
    financial = snapshot.get("financial_conditions") or {}
    growth = snapshot.get("growth") or {}
    inflation = snapshot.get("inflation") or {}
    labor = snapshot.get("labor") or {}
    rows = {
        "2Y": rates.get("DGS2"),
        "10Y": rates.get("DGS10"),
        "2s10s": rates.get("T10Y2Y"),
        "EFFR": rates.get("DFF") or rates.get("FEDFUNDS"),
        "SOFR": rates.get("SOFR"),
        "Fed target lower": rates.get("DFEDTARL"),
        "Fed target upper": rates.get("DFEDTARU"),
        "VIX": financial.get("VIXCLS"),
        "NFCI": financial.get("NFCI"),
        "GDP": growth.get("BEA:REAL_GDP") or growth.get("BEA:GDP"),
        "PCE": inflation.get("BEA:PCE"),
        "CPI": inflation.get("CUSR0000SA0"),
        "PPI": inflation.get("WPUFD4"),
        "NFP": labor.get("CES0000000001"),
        "Average Hourly Earnings": labor.get("CES0500000003"),
        "unemployment": labor.get("LNS14000000"),
        "Initial Jobless Claims": labor.get("ICSA"),
    }
    projected = {key: _metric(value or {}) for key, value in rows.items()}
    series_lifecycle = {
        key: _series_lifecycle(key, value or {}, now=now)
        for key, value in rows.items()
        if value
    }
    policy_refs = {item["policy_ref"] for item in series_lifecycle.values()}
    return {
        **projected,
        "series_lifecycle": series_lifecycle,
        "series_lifecycle_policies": {
            key: value
            for key, value in _macro_lifecycle_policies().items()
            if key in policy_refs
        },
        "lifecycle": snapshot.get("lifecycle") or {},
    }


def _event_risk(full: dict[str, Any]) -> dict[str, Any]:
    calendar = full.get("event_calendar") or {}
    windows = full.get("event_windows") or {}
    critical = list(calendar.get("critical_macro_events") or [])
    fed = list(calendar.get("fed_communications") or [])
    active = list(windows.get("active") or windows.get("active_event_windows") or [])
    upcoming = [item for item in (windows.get("upcoming") or windows.get("upcoming_event_windows") or []) if str(item.get("impact") or "").upper() == "HIGH" and _event_release_at(item)]
    unscheduled = [item for item in (windows.get("upcoming_unscheduled") or []) if str(item.get("impact") or "").upper() == "HIGH"]
    scheduled_critical = [item for item in critical if _event_release_at(item)]
    xtb = ((full.get("economic_calendar_enrichment") or {}).get("xtb") or {})
    return {
        "consensus_lifecycle": ((full.get("metadata") or {}).get("data_lifecycle") or {}).get("macro_consensus") or {},
        "actual_lifecycle": ((full.get("metadata") or {}).get("data_lifecycle") or {}).get("macro_actual") or {},
        "events_today": _events_today(full.get("events_today_context") or {}),
        "event_risk_window_status": windows.get("event_risk_window_status"),
        "active_windows": [_event(item) for item in active[:6]],
        "upcoming_high_impact_windows": [_event(item) for item in upcoming[:6]],
        "upcoming_high_impact_events_unscheduled": [_unscheduled_event(item) for item in unscheduled[:6]],
        "next_critical_event": _event(scheduled_critical[0]) if scheduled_critical else None,
        "next_fomc": _event(next((item for item in [*critical, *fed] if "FOMC" in _event_text(item)), {})) or None,
        "critical_events": [_event(item) for item in critical[:6]],
        "xtb_us_macro_calendar": {
            "status": xtb.get("status"),
            "provider_status": xtb.get("status"),
            "retrieved_at": xtb.get("retrieved_at"),
            "valid_until": xtb.get("valid_until"),
            "source": xtb.get("source"),
            "events": [_xtb_event(item) for item in (xtb.get("events") or xtb.get("items") or [])[:12]],
        },
        "warnings": windows.get("warnings") or [],
    }


def _rates(rates: dict[str, Any]) -> dict[str, Any]:
    meetings = [_meeting(item) for item in (rates.get("meetings") or [])[:4]]
    return {
        "lifecycle": rates.get("lifecycle") or {},
        "status": rates.get("status"),
        "current_fed_state": rates.get("current_fed_state") or {},
        "next_meeting": dict(meetings[0]) if meetings else None,
        "meetings": meetings,
        "sanity_check": rates.get("sanity_check") or {},
        "repricing_summary": _select(
            rates.get("repricing") or {},
            "history_available",
            "history_status",
            "probability_change_1h",
            "probability_change_24h",
            "probability_change_7d",
            "expected_rate_change_1h_bps",
            "expected_rate_change_24h_bps",
            "expected_rate_change_7d_bps",
        ),
        "source_classification": rates.get("source_summary") or {},
        "quality": rates.get("quality") or {},
        "freshness": rates.get("freshness"),
        "warnings": rates.get("warnings") or [],
    }


def _risk(risk: dict[str, Any]) -> dict[str, Any]:
    curve = risk.get("vix_term_structure") or {}
    contracts = curve.get("contracts") or curve.get("curve") or []
    ratios = (risk.get("put_call") or {}).get("by_id") or {}
    ratio_ids = [
        "total_volume_put_call",
        "equity_volume_put_call",
        "index_volume_put_call",
        "spx_volume_put_call",
        "qqq_volume_put_call",
        "total_open_interest_put_call",
        "qqq_open_interest_put_call",
    ]
    return {
        "lifecycle": risk.get("lifecycle") or {},
        "status": risk.get("status"),
        "VIX": _risk_metric(risk.get("vix") or {}),
        "VVIX": _risk_metric(risk.get("vvix") or {}),
        "SKEW": _risk_metric(risk.get("skew") or {}),
        "term_structure": {
            "lifecycle": curve.get("lifecycle") or {},
            "status": curve.get("status"),
            "structure": curve.get("structure"),
            "m1_m2_spread_points": curve.get("m1_m2_spread_points"),
            "m1_m2_spread_pct": curve.get("m1_m2_spread_pct"),
            "contracts": [_contract(item) for item in contracts[:3]],
            "source": curve.get("source"),
            "freshness": curve.get("freshness"),
        },
        "put_call": {key: _ratio(ratios[key]) for key in ratio_ids if key in ratios},
        "put_call_lifecycle": (risk.get("put_call") or {}).get("lifecycle") or {},
        "relative_regimes": risk.get("derived_context") or {},
        "quality": risk.get("quality") or {},
        "warnings": risk.get("warnings") or [],
    }


def _positioning(positioning: dict[str, Any]) -> dict[str, Any]:
    cot = (positioning.get("cot") or {}).get("nasdaq_100") or positioning
    return {
        "lifecycle": positioning.get("lifecycle") or cot.get("lifecycle") or {},
        "status": positioning.get("status") or cot.get("status"),
        "report_date": cot.get("report_date"),
        "asset_managers": cot.get("asset_managers") or {},
        "leveraged_funds": cot.get("leveraged_funds") or {},
        "dealers": cot.get("dealers") or {},
        "open_interest": cot.get("open_interest"),
        "source": cot.get("source") or positioning.get("source"),
        "freshness": cot.get("freshness") or positioning.get("freshness"),
        "warnings": positioning.get("warnings") or cot.get("warnings") or [],
    }


def _nasdaq(nasdaq: dict[str, Any]) -> dict[str, Any]:
    qqq = nasdaq.get("qqq_holdings") or {}
    holdings = qqq.get("holdings") or qqq.get("top_holdings") or []
    breadth = nasdaq.get("mega_cap_breadth") or {}
    return {
        "lifecycle": qqq.get("lifecycle") or {},
        "status": nasdaq.get("status"),
        "top_20_holdings": [_holding(item) for item in holdings[:20]],
        "holdings_count": qqq.get("holdings_count"),
        "concentration": nasdaq.get("concentration") or {},
        "sector_exposure": _compact_sector(nasdaq.get("sector_exposure") or {}),
        "mega_cap_contributors": {
            "top_positive": (breadth.get("top_positive_contributors") or [])[:8],
            "top_negative": (breadth.get("top_negative_contributors") or [])[:8],
            "net_contribution": breadth.get("weighted_net_contribution"),
        },
        "semiconductor_context": nasdaq.get("semiconductor_context") or {},
        "alphabet_aggregate": _alphabet_aggregate(holdings),
        "proxy_status": {"is_proxy": qqq.get("is_proxy"), "proxy_for": qqq.get("proxy_for")},
        "weight_method": {
            "method": qqq.get("weight_method"),
            "classification": qqq.get("weight_method_classification"),
            "calculation_validated": qqq.get("weight_calculation_validated"),
            "official_weight_verified": qqq.get("official_weight_verified"),
        },
        "quality": nasdaq.get("weight_quality") or qqq.get("data_quality") or {},
    }


def _earnings(full: dict[str, Any]) -> dict[str, Any]:
    earnings = (full.get("nasdaq_context") or {}).get("earnings") or {}
    events = earnings.get("upcoming") or earnings.get("events") or []
    today = datetime.now(NEW_YORK).date()
    projected = [_earnings_event(item, today=today) for item in events[:50]]
    upcoming = [item for item in projected if item.get("temporal_status") == "PRE_RELEASE"][:20]
    released = [item for item in projected if item.get("temporal_status") != "PRE_RELEASE"][:20]
    return {
        "lifecycle": earnings.get("lifecycle") or {},
        "status": earnings.get("status") or (
            "AVAILABLE"
            if events
            else "NO_DATA_EXPECTED"
            if str((full.get("market_schedule") or {}).get("market_session_status") or "") in {"weekend", "holiday", "market_closed"}
            and (earnings.get("data_quality") or {}).get("no_data_found")
            else "NO_RELEVANT_DATA_FOUND"
        ),
        "issuer_event_count": earnings.get("issuer_event_count", len(events)),
        "upcoming_mega_cap_earnings_14d": upcoming,
        "released_earnings": released,
        "quality": earnings.get("data_quality") or {},
    }


def _news(news: dict[str, Any], digest: dict[str, Any], schedule: dict[str, Any]) -> dict[str, Any]:
    current_drivers: list[dict[str, Any]] = []
    previous_session_drivers: list[dict[str, Any]] = []
    for raw in (digest.get("drivers") or [])[:12]:
        driver = _news_driver(
            raw,
            context_date=str(news.get("context_date") or schedule.get("context_date") or ""),
            previous_session_date=str(schedule.get("last_market_session_date") or ""),
        )
        if not driver:
            continue
        if driver["context_classification"] == "CURRENT_DAY":
            current_drivers.append(driver)
        elif driver["context_classification"] == "PREVIOUS_SESSION":
            previous_session_drivers.append(driver)
    return {
        "lifecycle": news.get("lifecycle") or {},
        "status": news.get("status"),
        "context_date": news.get("context_date"),
        "search_completed": news.get("search_completed"),
        "market_session_status": news.get("market_session_status"),
        "usable_for_analysis": news.get("usable_for_analysis"),
        "coverage_window_hours": news.get("coverage_window_hours"),
        "provider_attempt_count": news.get("provider_attempt_count"),
        "provider_success_count": news.get("provider_success_count"),
        "provider_failure_count": news.get("provider_failure_count"),
        "candidate_article_count": news.get("candidate_article_count"),
        "accepted_article_count": news.get("accepted_article_count"),
        "rejected_article_count": news.get("rejected_article_count"),
        "articles": [_article(item) for item in (news.get("articles") or news.get("latest") or [])[:8]],
        "clusters": [_cluster(item) for item in (news.get("clusters") or [])[:8]],
        "current_drivers": current_drivers[:8],
        "previous_session_drivers": previous_session_drivers[:8],
        "quality": news.get("quality") or {},
        "reason": news.get("reason"),
        "warnings": news.get("warnings") or digest.get("warnings") or [],
        "last_known_good_used": news.get("last_known_good_used", False),
        "last_known_good_age_hours": news.get("last_known_good_age_hours"),
    }


def _sentiment(sentiment: dict[str, Any]) -> dict[str, Any]:
    return {
        "AAII": _select(sentiment.get("aaii") or {}, "status", "survey_date", "bullish_pct", "neutral_pct", "bearish_pct", "bull_bear_spread", "source", "reliability", "warnings", "lifecycle"),
        "retail_QQQ": sentiment.get("retail_qqq") or {},
        "technology_discussion": _select(sentiment.get("technology_discussion") or {}, "status", "classification", "mention_count", "source_count", "social_market_sentiment", "source", "reliability", "warnings", "lifecycle"),
        "Fear_Greed": sentiment.get("fear_greed") or {},
        "prediction_markets": _compact_prediction(sentiment.get("prediction_markets") or {}),
        "quality": sentiment.get("sentiment_quality") or {},
    }


def _schedule(schedule: dict[str, Any]) -> dict[str, Any]:
    return {
        "lifecycle": schedule.get("lifecycle") or {},
        "holiday_calendar_lifecycle": schedule.get("holiday_calendar_lifecycle") or {},
        "status": schedule.get("status"),
        "context_date": schedule.get("context_date"),
        "market_session_status": schedule.get("market_session_status"),
        "last_market_session_date": schedule.get("last_market_session_date"),
        "mnq_session": _session(schedule.get("mnq_session") or {}),
        "nasdaq_cash_session": _session(schedule.get("nasdaq_cash_session") or {}),
        "cme_equity_futures_session": _session(schedule.get("cme_equity_futures_session") or {}),
        "next_open": (schedule.get("mnq_session") or {}).get("next_open"),
        "next_close": (schedule.get("mnq_session") or {}).get("next_close"),
        "maintenance_break": (schedule.get("mnq_session") or {}).get("maintenance_break"),
        "next_holiday": schedule.get("next_holiday"),
        "next_early_close": schedule.get("next_early_close"),
        "source": schedule.get("source"),
        "warnings": schedule.get("warnings") or [],
    }


def _warnings(full: dict[str, Any]) -> list[dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for source in (
        full.get("data_quality") or {},
        full.get("risk_context") or {},
        full.get("rates_expectations") or {},
        full.get("news_context") or {},
        full.get("sentiment_context") or {},
    ):
        for raw in source.get("warnings") or []:
            code = str(raw).split(":", 1)[0]
            if not code:
                continue
            entry = output.setdefault(code, {"code": code, "count": 0, "blocking": False})
            entry["count"] += 1
    return sorted(output.values(), key=lambda item: item["code"])


def _metric(item: dict[str, Any]) -> dict[str, Any]:
    return _select(
        item,
        "value",
        "unit",
        "frequency",
        "data_as_of",
        "source",
        "source_url",
        "provider_type",
        "freshness",
        "reliability",
        "confidence",
        "cache_status",
        "valid_until",
        "next_refresh_at",
    )


def _risk_metric(item: dict[str, Any]) -> dict[str, Any]:
    return _select(item, "status", "value", "previous_close", "change_pct", "data_as_of", "relative_regime", "tail_risk_regime", "percentile_1y", "z_score_1y", "source", "source_url", "provider_type", "freshness", "reliability", "confidence", "valid_until", "next_refresh_at", "lifecycle")


def _contract(item: dict[str, Any]) -> dict[str, Any]:
    return _select(item, "contract_symbol", "expiration_date", "last_price", "previous_close", "change_pct", "volume", "open_interest", "data_as_of", "source", "freshness")


def _ratio(item: dict[str, Any]) -> dict[str, Any]:
    return _select(item, "ratio_id", "scope", "basis", "put_value", "call_value", "ratio", "change_1d", "change_5d", "moving_average_5d", "moving_average_20d", "percentile_1y", "z_score_1y", "history_depth", "history_status", "relative_regime", "data_as_of", "source", "freshness")


def _event(item: dict[str, Any]) -> dict[str, Any]:
    if not item:
        return {}
    enrichment = item.get("enrichment") or {}
    lineage = _drop_empty(_select(
        enrichment,
        "source",
        "source_url",
        "provider_type",
        "confidence",
        "reliability",
        "evidence",
        "evidence_text",
        "validation",
        "field_lineage",
        "cache_status",
        "valid_until",
        "next_refresh_at",
    ))
    if lineage.get("field_lineage"):
        lineage["field_lineage"] = _compact_field_lineage(
            lineage["field_lineage"],
            shared_evidence=lineage.get("evidence"),
        )
    if lineage.get("evidence_text") == lineage.get("evidence"):
        lineage.pop("evidence_text", None)
    projected = {
        **_select(item, "event_id", "canonical_event_key", "name", "event_name", "category", "impact", "date", "time_utc", "event_type", "event_kind", "temporal_status", "release_status", "release_period", "period_date_consistent", "event_risk_window_status"),
        "release_at": _event_release_at(item),
        "consensus": enrichment.get("consensus"),
        "previous": enrichment.get("previous"),
        "actual": enrichment.get("actual"),
        "metrics": [
            _event_metric(metric)
            for metric in (enrichment.get("metrics") or [])[:6]
        ],
    }
    temporal = (enrichment.get("summary") or {}).get("temporal_domain") or temporal_event_state(item)
    projected["canonical_event_key"] = projected.get("canonical_event_key") or temporal.get("canonical_event_key")
    projected["event_kind"] = projected.get("event_kind") or temporal.get("event_kind")
    projected["temporal_status"] = str(
        projected.get("temporal_status") or temporal.get("temporal_status") or ""
    ).upper() or None
    if lineage and (lineage.get("provider_type") in {"AI_RESEARCHER_CODEX_CLI", "MIXED"} or lineage.get("field_lineage")):
        projected["lineage"] = lineage
    return projected


def _meeting(item: dict[str, Any]) -> dict[str, Any]:
    return {
        **_select(item, "meeting_id", "meeting_date", "meeting_time_utc", "expected_target_midpoint", "expected_change_bps", "cut_probability", "hold_probability", "hike_probability", "most_likely_target_range", "most_likely_probability", "probability_semantics", "is_single_meeting_action_probability", "source", "freshness"),
        "outcomes": [_select(row, "target_lower_bound", "target_upper_bound", "target_midpoint", "change_bps", "probability", "classification") for row in (item.get("outcomes") or [])[:8]],
    }


def _holding(item: dict[str, Any]) -> dict[str, Any]:
    return _select(item, "symbol", "name", "weight", "weight_pct", "sector", "share_class", "issuer_name", "issuer_aggregate_weight_pct", "source", "weight_method")


def _compact_sector(exposure: dict[str, Any]) -> dict[str, Any]:
    return {
        **_select(exposure, "status", "coverage_scope", "sector_weight_coverage_pct", "unknown_weight_pct", "source", "weight_method"),
        "sectors": (exposure.get("sectors") or [])[:12],
    }


def _earnings_event(item: dict[str, Any], *, today: Any | None = None) -> dict[str, Any]:
    output = _select(
        item,
        "issuer_event_id",
        "issuer_name",
        "symbols",
        "symbol",
        "earnings_date",
        "date",
        "time",
        "is_primary_event",
        "duplicate_security_event",
        "eps_estimate",
        "eps_actual",
        "revenue_estimate",
        "revenue_actual",
        "provider_last_updated",
        "retrieved_at_utc",
        "source",
        "source_url",
        "reliability",
        "lineage",
    )
    output["lineage"] = _compact_source_field_lineage(output.get("lineage"))
    event_date = parse_datetime(output.get("earnings_date") or output.get("date"))
    event_day = event_date.date() if event_date else None
    today = today or datetime.now(NEW_YORK).date()
    has_actual = output.get("eps_actual") not in (None, "") or output.get("revenue_actual") not in (None, "")
    output["temporal_status"] = (
        "RELEASED" if has_actual
        else "PRE_RELEASE" if event_day is None or event_day >= today
        else "AWAITING_ACTUAL"
    )
    output["eps_surprise"] = _difference(output.get("eps_actual"), output.get("eps_estimate"))
    output["revenue_surprise"] = _difference(output.get("revenue_actual"), output.get("revenue_estimate"))
    return output


def _difference(actual: Any, expected: Any) -> dict[str, Any] | None:
    if actual in (None, "") or expected in (None, ""):
        return None
    try:
        difference = float(actual) - float(expected)
    except (TypeError, ValueError):
        return None
    return {
        "value": difference,
        "direction": "above" if difference > 0 else "below" if difference < 0 else "in_line",
    }


def _xtb_event(item: dict[str, Any]) -> dict[str, Any]:
    output = _drop_empty(_select(
        item,
        "source_event_id",
        "indicator_id",
        "event_name",
        "normalized_event_type",
        "date",
        "time_local",
        "release_at",
        "all_day",
        "impact",
        "importance",
        "consensus",
        "consensus_verified",
        "forecast_display",
        "previous",
        "previous_display",
        "actual",
        "actual_display",
        "unit",
        "currency",
        "source",
        "retrieved_at",
        "lineage",
    ))
    output["lineage"] = _compact_source_field_lineage(output.get("lineage"))
    return output


def _compact_source_field_lineage(raw: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    output: dict[str, dict[str, Any]] = {}
    for field, details in raw.items():
        if not isinstance(details, dict):
            continue
        compact = _drop_empty(_select(details, "source_field", "source_fields"))
        if compact:
            output[str(field)] = compact
    return output


def _alphabet_aggregate(holdings: list[dict[str, Any]]) -> dict[str, Any]:
    classes = [item for item in holdings if str(item.get("symbol") or "").upper() in {"GOOG", "GOOGL"}]
    if not classes:
        return {}
    aggregate = next((item.get("issuer_aggregate_weight_pct") for item in classes if item.get("issuer_aggregate_weight_pct") is not None), None)
    if aggregate is None:
        weights = [item.get("weight_pct", item.get("weight")) for item in classes]
        aggregate = round(sum(float(value) for value in weights if value is not None), 6)
    return {
        "issuer_name": "Alphabet Inc.",
        "symbols": sorted(str(item.get("symbol")).upper() for item in classes),
        "aggregate_weight_pct": aggregate,
        "share_classes": [_holding(item) for item in classes],
    }


def _article(item: dict[str, Any]) -> dict[str, Any]:
    return _select(item, "article_id", "title", "summary", "source", "source_tier", "source_classification", "canonical_url", "published_at", "published_at_source", "published_at_verified", "timestamp_inferred", "timestamp_confidence", "symbols", "topics", "mnq_relevance_score", "market_impact_score", "source_quality_score", "recency_score", "final_acceptance_score", "reliability", "confidence", "cluster_id")


def _cluster(item: dict[str, Any]) -> dict[str, Any]:
    return _select(item, "cluster_id", "headline", "summary", "topics", "symbols", "article_count", "independent_source_count", "confidence", "reliability", "confirmed", "published_at_latest")


def _events_today(context: dict[str, Any]) -> dict[str, Any]:
    return {
        **_select(context, "status", "date", "market_session_status", "calendar_query_completed", "event_count", "blocking", "errors"),
        "events": [_event(item) for item in (context.get("events") or [])[:12]],
    }


def _session(session: dict[str, Any]) -> dict[str, Any]:
    return _select(
        session,
        "status",
        "market",
        "instrument",
        "venue",
        "timezone",
        "regular_trading_hours",
        "extended_trading_hours",
        "maintenance_break",
        "holiday_schedule",
        "early_close",
        "next_open",
        "next_close",
        "source",
        "source_classification",
        "calendar_crosscheck_status",
        "freshness",
    )


def _compact_prediction(prediction: dict[str, Any]) -> dict[str, Any]:
    output = _select(prediction, "status", "failure_type", "market_count", "source", "short_reason")
    warnings = [str(item) for item in prediction.get("warnings") or []]
    if prediction.get("status") == "SSL_ERROR" or any("SSL" in item.upper() for item in warnings):
        output["warnings"] = ["ssl_certificate_verification_failed"]
    else:
        output["warnings"] = [item[:180] for item in warnings[:5]]
    output["blocking"] = False
    return output


def _series_lifecycle(series_name: str, item: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    frequency = str(item.get("frequency") or "unknown").lower()
    if series_name in {"Fed target lower", "Fed target upper"}:
        policy_ref = "fed_target"
    elif frequency == "daily":
        policy_ref = "daily_market"
    elif frequency == "weekly":
        policy_ref = "weekly_release"
    elif frequency == "monthly":
        policy_ref = "monthly_release"
    elif frequency == "quarterly":
        policy_ref = "quarterly_release"
    else:
        policy_ref = "unknown_frequency"
    next_refresh = parse_datetime(item.get("next_refresh_at"))
    valid_until = parse_datetime(item.get("valid_until"))
    data_as_of = parse_datetime(item.get("data_as_of") or item.get("latest_release_at"))
    now = now or datetime.now(UTC)
    if next_refresh and next_refresh > now:
        lifecycle_status = "NEXT_RELEASE_SCHEDULED"
    elif valid_until and valid_until > now:
        lifecycle_status = "KNOWN_NEXT_RELEASE"
    elif frequency == "daily" and data_as_of and data_as_of.date() < now.date():
        lifecycle_status = "LAST_SESSION"
    elif data_as_of:
        lifecycle_status = "CURRENT_RELEASE"
    else:
        lifecycle_status = "UNKNOWN"
    lifecycle = {
        "frequency": frequency,
        "lifecycle_status": lifecycle_status,
        "policy_ref": policy_ref,
    }
    if item.get("valid_until"):
        lifecycle["valid_until"] = item["valid_until"]
    if item.get("next_refresh_at"):
        lifecycle["next_refresh_at"] = item["next_refresh_at"]
    return lifecycle


def _macro_lifecycle_policies() -> dict[str, dict[str, Any]]:
    return {
        "daily_market": {
            "refresh_policy": "refresh_next_market_session",
            "carry_forward_allowed": True,
            "stale_policy": "carry_only_with_explicit_last_session_or_stale_label",
        },
        "fed_target": {
            "refresh_policy": "verify_daily_and_refresh_on_fomc_decision",
            "carry_forward_allowed": True,
            "stale_policy": "valid_until_superseded_by_official_target_decision",
        },
        "weekly_release": {
            "refresh_policy": "refresh_on_next_official_weekly_release",
            "carry_forward_allowed": True,
            "stale_policy": "carry_only_with_current_release_or_stale_label",
        },
        "monthly_release": {
            "refresh_policy": "refresh_on_next_official_monthly_release",
            "carry_forward_allowed": True,
            "stale_policy": "carry_published_value_until_superseded_with_release_freshness",
        },
        "quarterly_release": {
            "refresh_policy": "refresh_on_next_official_quarterly_release",
            "carry_forward_allowed": True,
            "stale_policy": "carry_published_value_until_superseded_with_release_freshness",
        },
        "unknown_frequency": {
            "refresh_policy": "refresh_from_official_provider_when_due",
            "carry_forward_allowed": True,
            "stale_policy": "unknown_frequency_requires_explicit_freshness",
        },
    }


def _event_release_at(item: dict[str, Any]) -> str | None:
    direct = item.get("release_at") or item.get("release_at_utc")
    parsed = parse_datetime(direct)
    if parsed:
        return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    event_date = str(item.get("date") or "").strip()
    event_time = str(item.get("time_utc") or "").strip()
    parsed = parse_datetime(event_time)
    if parsed:
        return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if event_date and event_time:
        parsed = parse_datetime(f"{event_date}T{event_time.replace('Z', '')}Z")
        if parsed:
            return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return None


def _unscheduled_event(item: dict[str, Any]) -> dict[str, Any]:
    return _select(
        item,
        "event_id",
        "event_name",
        "event_type",
        "impact",
        "schedule_status",
        "reason",
        "source",
        "source_url",
    )


def _news_driver(
    raw: Any,
    *,
    context_date: str,
    previous_session_date: str,
) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    published = parse_datetime(raw.get("published_at_latest") or raw.get("published_at"))
    if published is None:
        return None
    source_context_date = published.astimezone(NEW_YORK).date().isoformat()
    if source_context_date == context_date:
        classification = "CURRENT_DAY"
    elif source_context_date == previous_session_date:
        classification = "PREVIOUS_SESSION"
    else:
        return None
    return {
        **raw,
        "context_classification": classification,
        "source_context_date": source_context_date,
        "is_current_context_date": classification == "CURRENT_DAY",
        "is_previous_session_context": classification == "PREVIOUS_SESSION",
        "usable_for_current_news_analysis": classification == "CURRENT_DAY",
    }


def _event_metric(metric: dict[str, Any]) -> dict[str, Any]:
    output = _select(
        metric,
        "metric_id",
        "name",
        "consensus",
        "forecast",
        "previous",
        "actual",
        "unit",
        "frequency",
        "source",
        "source_url",
        "provider_type",
        "confidence",
        "reliability",
        "evidence",
        "validation",
        "field_lineage",
    )
    for key in ("source", "source_url", "provider_type", "confidence", "reliability", "evidence", "validation", "field_lineage"):
        if output.get(key) in (None, "", {}, []):
            output.pop(key, None)
    if output.get("field_lineage"):
        output["field_lineage"] = _compact_field_lineage(
            output["field_lineage"],
            shared_evidence=output.get("evidence"),
        )
    return output


def _compact_field_lineage(raw: Any, *, shared_evidence: Any = None) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    output: dict[str, Any] = {}
    for field, value in raw.items():
        if not isinstance(value, dict):
            continue
        compact = _drop_empty(
            _select(
                value,
                "source",
                "source_url",
                "provider_type",
                "confidence",
                "reliability",
                "evidence",
                "validation",
            )
        )
        if compact.get("evidence") == shared_evidence:
            compact.pop("evidence", None)
        if compact:
            output[str(field)] = compact
    return output


def _drop_empty(item: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if value not in (None, "", {}, [])}


def _select(item: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: item.get(key) for key in keys}


def _event_text(item: dict[str, Any]) -> str:
    return " ".join(str(item.get(key) or "") for key in ("name", "event_name", "category")).upper()


def _payload_size(value: dict[str, Any]) -> int:
    return len(json.dumps(value, default=str, separators=(",", ":")).encode("utf-8"))
