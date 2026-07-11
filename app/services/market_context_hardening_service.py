from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime, timedelta
from statistics import fmean
from typing import Any

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.data_integrity_service import classify_source
from app.services.market_session_service import (
    build_session_aware_schedule,
    is_market_closed,
    last_market_session_date,
)


logger = logging.getLogger(__name__)
READINESS_VERSION = "session_aware_readiness_v2"
QUALITY_VERSION = "available_data_quality_v2"
NEWS_STATUSES = {
    "AVAILABLE",
    "PARTIAL",
    "NO_RELEVANT_NEWS",
    "MARKET_CLOSED_NO_FRESH_NEWS",
    "PROVIDER_UNAVAILABLE",
    "PIPELINE_ERROR",
    "NOT_CONFIGURED",
    "LAST_KNOWN_GOOD",
}
OPTIONAL_SECTION_CONFIG = {
    "news_context": "readiness_require_news",
    "rates_expectations": "readiness_require_rates",
    "positioning": "readiness_require_positioning",
    "sentiment": "readiness_require_sentiment",
    "prediction_markets": "readiness_require_prediction_markets",
}


def harden_market_context(
    full: dict[str, Any],
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    settings = settings or Settings(_env_file=None)
    now = _aware(now or datetime.now(UTC))
    output = dict(full)
    output["market_schedule"] = build_session_aware_schedule(output.get("market_schedule") or {}, now=now)
    session_status = output["market_schedule"]["market_session_status"]

    output["event_calendar"] = _annotate_event_calendar(output.get("event_calendar") or {})
    output["events_today"] = [_annotate_event(item) for item in output.get("events_today") or []]
    output["events_today_context"] = events_today_context(output, session_status=session_status, now=now)
    output["event_windows"] = _event_window_status(
        output.get("event_windows") or {},
        events_today=output["events_today_context"],
    )
    output["news_context"] = apply_news_semantics(
        output.get("news_context") or {},
        pipeline=(output.get("data_quality") or {}).get("news_pipeline") or {},
        market_schedule=output["market_schedule"],
        settings=settings,
        now=now,
    )
    output["news_digest"] = _news_digest_view(output["news_context"], output.get("news_digest") or {})
    output["nasdaq_context"] = _harden_nasdaq(output.get("nasdaq_context") or {})
    output["corporate_events"] = _harden_corporate_events(output.get("corporate_events") or {})
    output["rates_expectations"] = _harden_fed_expectations(
        output.get("rates_expectations") or {},
        macro_snapshot=output.get("macro_snapshot") or {},
    )
    output["sentiment_context"] = _harden_sentiment(
        output.get("sentiment_context") or {},
        social=output.get("social_sentiment") or {},
        market_closed=is_market_closed(session_status),
    )
    output["quality"] = build_consumer_quality(output, session_status=session_status)
    output["readiness"] = evaluate_readiness(output, settings=settings)
    output["data_quality"] = _update_data_quality(output.get("data_quality") or {}, output)
    output["metadata"] = _materialization_metadata(output.get("metadata") or {}, output)
    _walk_semantics(output, session_status=session_status, now=now)
    logger.info(
        "readiness_evaluated",
        extra={
            "status": output["readiness"]["status"],
            "ready": output["readiness"]["ready"],
            "market_session_status": session_status,
            "confidence": output["readiness"]["confidence"],
        },
    )
    if is_market_closed(session_status):
        logger.info("readiness_market_closed_adjustment", extra={"market_session_status": session_status})
    return output


def apply_news_semantics(
    context: dict[str, Any],
    *,
    pipeline: dict[str, Any] | None,
    market_schedule: dict[str, Any],
    settings: Settings,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = _aware(now or datetime.now(UTC))
    output = dict(context)
    pipeline = dict(pipeline or {})
    articles = list(output.get("latest") or output.get("articles") or [])
    diagnostics = dict(output.get("diagnostics") or {})
    candidate_count = int(
        output.get("candidate_article_count")
        or diagnostics.get("raw_article_count")
        or pipeline.get("fetched_count")
        or 0
    )
    accepted_count = len(articles)
    rejected_count = int(
        output.get("rejected_article_count")
        or diagnostics.get("excluded_count")
        or pipeline.get("excluded_count")
        or max(candidate_count - accepted_count, 0)
    )
    explicit_errors = list(output.get("errors") or [])
    provider_failure_count = int(output.get("provider_failure_count") or pipeline.get("provider_failure_count") or 0)
    provider_success_count = int(output.get("provider_success_count") or pipeline.get("provider_success_count") or 0)
    if provider_success_count == 0 and candidate_count > 0:
        provider_success_count = 1
    provider_attempt_count = int(
        output.get("provider_attempt_count")
        or pipeline.get("provider_attempt_count")
        or provider_success_count + provider_failure_count
    )
    session_status = str(market_schedule.get("market_session_status") or "unknown").lower()
    closed = is_market_closed(session_status)
    coverage = _news_lookback(settings, session_status)
    cutoff = now - timedelta(hours=coverage)
    in_window: list[dict[str, Any]] = []
    filtered_out = 0
    for article in articles:
        published = parse_datetime(article.get("published_at"))
        if published is not None and _aware(published) < cutoff:
            filtered_out += 1
            continue
        in_window.append(article)
    articles = in_window
    accepted_count = len(articles)
    rejected_count += filtered_out
    pipeline_error = bool(output.get("pipeline_error") or explicit_errors)
    configured = output.get("configured", True) is not False
    if articles:
        status = "LAST_KNOWN_GOOD" if output.get("last_known_good_used") else "AVAILABLE" if not explicit_errors else "PARTIAL"
        reason = None
    elif not configured:
        status = "NOT_CONFIGURED"
        reason = "news_pipeline_not_configured"
    elif pipeline_error:
        status = "PIPELINE_ERROR"
        reason = "news_pipeline_error"
    elif provider_success_count == 0 and provider_failure_count > 0:
        status = "PROVIDER_UNAVAILABLE"
        reason = "news_provider_unavailable"
    elif closed:
        status = "MARKET_CLOSED_NO_FRESH_NEWS"
        reason = "no_articles_passed_relevance_and_recency_filters"
    else:
        status = "NO_RELEVANT_NEWS"
        reason = "no_articles_passed_relevance_and_recency_filters"
    search_completed = status not in {"PROVIDER_UNAVAILABLE", "PIPELINE_ERROR", "NOT_CONFIGURED"}
    latest_published = max(
        (parse_datetime(item.get("published_at")) for item in articles if parse_datetime(item.get("published_at"))),
        default=None,
    )
    lkg_age = (
        round((now - _aware(latest_published)).total_seconds() / 3600, 2)
        if output.get("last_known_good_used") and latest_published
        else output.get("last_known_good_age_hours")
    )
    output.update(
        {
            "status": status,
            "legacy_status": "available" if articles else "no_data_available",
            "usable_for_analysis": bool(articles),
            "blocking": False,
            "market_session_status": session_status,
            "coverage_window_hours": coverage,
            "last_market_session_date": last_market_session_date(market_schedule, now=now),
            "search_completed": search_completed,
            "provider_attempt_count": provider_attempt_count,
            "provider_success_count": provider_success_count,
            "provider_failure_count": provider_failure_count,
            "candidate_article_count": candidate_count,
            "accepted_article_count": accepted_count,
            "rejected_article_count": rejected_count,
            "reason": reason,
            "articles": articles,
            "latest": articles,
            "confidence": float((output.get("digest") or {}).get("confidence") or output.get("confidence") or 0.0),
            "last_known_good_used": bool(output.get("last_known_good_used")),
            "last_known_good_age_hours": lkg_age,
            "last_known_good_original_published_at": (
                latest_published.isoformat() if output.get("last_known_good_used") and latest_published else output.get("last_known_good_original_published_at")
            ),
        }
    )
    accepted_ids = {item.get("article_id") for item in articles}
    if accepted_ids:
        output["clusters"] = [
            cluster
            for cluster in output.get("clusters") or []
            if accepted_ids.intersection(cluster.get("article_ids") or [])
        ]
    elif not articles:
        output["clusters"] = []
    if status == "MARKET_CLOSED_NO_FRESH_NEWS":
        logger.info("news_market_closed_no_fresh_news", extra={"candidate_article_count": candidate_count})
    elif status == "NO_RELEVANT_NEWS":
        logger.info("news_search_completed_no_results", extra={"candidate_article_count": candidate_count})
    elif status == "PROVIDER_UNAVAILABLE":
        logger.warning("news_provider_unavailable", extra={"provider_failure_count": provider_failure_count})
    elif status == "PIPELINE_ERROR":
        logger.error("news_pipeline_error", extra={"errors": explicit_errors[:3]})
    return output


def events_today_context(
    full: dict[str, Any],
    *,
    session_status: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = _aware(now or datetime.now(UTC))
    events = list(full.get("events_today") or [])
    quality = full.get("data_quality") or {}
    event_errors = list((quality.get("event_pipeline") or {}).get("errors") or [])
    if events:
        status = "AVAILABLE"
    elif event_errors:
        status = "PIPELINE_ERROR"
    else:
        status = "NO_EVENTS_SCHEDULED"
        logger.info("no_events_scheduled", extra={"date": now.date().isoformat(), "market_session_status": session_status})
    return {
        "status": status,
        "date": now.date().isoformat(),
        "market_session_status": str(session_status).lower(),
        "calendar_query_completed": status != "PIPELINE_ERROR",
        "event_count": len(events),
        "events": events,
        "blocking": False,
        "errors": event_errors,
    }


def evaluate_readiness(full: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
    session_status = str((full.get("market_schedule") or {}).get("market_session_status") or "unknown").lower()
    news = full.get("news_context") or {}
    events_today = full.get("events_today_context") or {}
    rates = full.get("rates_expectations") or {}
    sentiment = full.get("sentiment_context") or {}
    prediction = sentiment.get("prediction_markets") or (full.get("sentiment") or {}).get("prediction_markets") or {}
    section_status = {
        "macro_snapshot": _section_status(_macro_available(full.get("macro_snapshot") or {})),
        "event_risk": _event_section_status(events_today),
        "market_schedule": _section_status(bool(full.get("market_schedule"))),
        "risk_context": _section_status(_risk_available(full.get("risk_context") or {})),
        "nasdaq_context": _section_status(_nasdaq_available(full.get("nasdaq_context") or {})),
        "news_context": str(news.get("status") or "NOT_CONFIGURED"),
        "rates_expectations": _optional_status(rates),
        "positioning": _optional_status(full.get("positioning") or {}),
        "earnings": _optional_status((full.get("nasdaq_context") or {}).get("earnings") or {}),
        "sentiment": _optional_status(sentiment),
        "prediction_markets": _optional_status(prediction),
    }
    critical_errors: list[str] = []
    blocking: list[str] = []
    degrading: list[str] = []
    informational: list[str] = []
    missing_optional: list[str] = []

    for key in ("macro_snapshot", "event_risk", "market_schedule", "risk_context", "nasdaq_context"):
        if section_status[key] in {"NOT_AVAILABLE", "PIPELINE_ERROR"}:
            blocking.append(f"{key}_missing")
            if section_status[key] == "PIPELINE_ERROR":
                critical_errors.append(f"{key}_pipeline_error")

    if is_market_closed(session_status):
        informational.append(session_status)
        if news.get("status") == "MARKET_CLOSED_NO_FRESH_NEWS":
            informational.append("no_fresh_news_expected")
        if events_today.get("status") == "NO_EVENTS_SCHEDULED":
            informational.append("no_events_scheduled")
    elif news.get("status") == "NO_RELEVANT_NEWS":
        degrading.append("no_relevant_news_found")

    if news.get("status") == "PROVIDER_UNAVAILABLE":
        degrading.append("news_provider_unavailable")
    if news.get("status") == "PIPELINE_ERROR":
        degrading.append("news_pipeline_error")
    if section_status["rates_expectations"] not in {"AVAILABLE", "LAST_KNOWN_GOOD"}:
        degrading.append("rates_expectations_unavailable")

    for section, field in OPTIONAL_SECTION_CONFIG.items():
        status = section_status[section]
        if status not in {"AVAILABLE", "LAST_KNOWN_GOOD"}:
            missing_optional.append(section)
            if bool(getattr(settings, field)):
                blocking.append(f"{section}_required")

    trading_ready = not blocking
    core_available = sum(section_status[key] == "AVAILABLE" for key in ("macro_snapshot", "event_risk", "market_schedule", "risk_context", "nasdaq_context"))
    if not trading_ready:
        status = "NOT_READY" if core_available <= 2 or critical_errors else "PARTIAL"
    elif degrading:
        status = "DEGRADED"
    else:
        status = "READY"
    readiness = {
        "status": status,
        "ready": trading_ready,
        "ready_for_macro_analysis": section_status["macro_snapshot"] == "AVAILABLE",
        "ready_for_event_risk_analysis": section_status["event_risk"] == "AVAILABLE",
        "ready_for_rates_analysis": section_status["rates_expectations"] in {"AVAILABLE", "LAST_KNOWN_GOOD"},
        "ready_for_risk_context_analysis": section_status["risk_context"] == "AVAILABLE",
        "ready_for_nasdaq_context_analysis": section_status["nasdaq_context"] == "AVAILABLE",
        "ready_for_news_analysis": news.get("status") in {"AVAILABLE", "PARTIAL", "LAST_KNOWN_GOOD"},
        "ready_for_sentiment_analysis": section_status["sentiment"] in {"AVAILABLE", "LAST_KNOWN_GOOD"},
        "ready_for_trading_context": trading_ready,
        "ready_for_full_analysis": trading_ready and not critical_errors,
        "critical_errors": sorted(set(critical_errors)),
        "critical_error_count": len(set(critical_errors)),
        "blocking_reasons": sorted(set(blocking)),
        "degrading_reasons": sorted(set(degrading)),
        "informational_reasons": sorted(set(informational)),
        "missing_optional_sections": sorted(set(missing_optional)),
        "section_status": section_status,
        "confidence": _readiness_confidence(section_status, trading_ready),
        "market_session_status": session_status,
        "version": READINESS_VERSION,
    }
    return readiness


def build_consumer_quality(full: dict[str, Any], *, session_status: str) -> dict[str, Any]:
    section_quality = (full.get("data_quality") or {}).get("section_quality") or {}
    macro = _score((section_quality.get("macro_snapshot") or {}).get("completeness_score"), default=1.0 if _macro_available(full.get("macro_snapshot") or {}) else 0.0)
    event_context = full.get("events_today_context") or {}
    event = 1.0 if event_context.get("status") in {"AVAILABLE", "NO_EVENTS_SCHEDULED"} else 0.0
    rates = _score((full.get("rates_expectations") or {}).get("quality", {}).get("quality_score"))
    risk = _score((full.get("risk_context") or {}).get("quality", {}).get("quality_score"))
    nasdaq = _score((full.get("nasdaq_context") or {}).get("weight_quality", {}).get("weight_quality_score"), default=1.0 if _nasdaq_available(full.get("nasdaq_context") or {}) else 0.0)
    news_context = full.get("news_context") or {}
    news = _score((news_context.get("quality") or {}).get("news_quality_score"))
    sentiment_context = full.get("sentiment_context") or {}
    sentiment = _score((sentiment_context.get("sentiment_quality") or {}).get("quality_score"))
    schedule = 0.9 if (full.get("market_schedule") or {}).get("status") == "AVAILABLE" else 0.0
    closed_no_news = is_market_closed(session_status) and news_context.get("status") == "MARKET_CLOSED_NO_FRESH_NEWS"
    available_scores = [macro, event, risk, nasdaq, schedule]
    if _optional_status(full.get("rates_expectations") or {}) == "AVAILABLE":
        available_scores.append(rates)
    if news_context.get("status") in {"AVAILABLE", "PARTIAL", "LAST_KNOWN_GOOD"}:
        available_scores.append(news)
    if _optional_status(sentiment_context) == "AVAILABLE":
        available_scores.append(sentiment)
    full_scores = [macro, event, rates, risk, nasdaq, 1.0 if closed_no_news else news, sentiment, schedule]
    return {
        "macro_quality": macro,
        "event_quality": event,
        "rates_quality": rates,
        "risk_quality": risk,
        "nasdaq_quality": nasdaq,
        "news_quality": news,
        "sentiment_quality": sentiment,
        "schedule_quality": schedule,
        "overall_available_data_quality": round(fmean(available_scores), 3) if available_scores else 0.0,
        "full_analysis_quality": round(fmean(full_scores), 3),
        "market_closed_adjustment_applied": closed_no_news,
        "version": QUALITY_VERSION,
    }


def classify_freshness(
    *,
    data_as_of: Any,
    retrieved_at: Any = None,
    frequency: Any = None,
    session_status: str = "unknown",
    now: datetime | None = None,
) -> str:
    now = _aware(now or datetime.now(UTC))
    observed = parse_datetime(data_as_of) or parse_datetime(retrieved_at)
    if observed is None:
        return "UNKNOWN"
    age = now - _aware(observed)
    frequency_text = str(frequency or "").lower()
    if age < timedelta(minutes=20) and not is_market_closed(session_status):
        return "LIVE"
    if frequency_text in {"monthly", "month"}:
        return "CURRENT_RELEASE" if age <= timedelta(days=45) else "STALE" if age <= timedelta(days=90) else "VERY_STALE"
    if frequency_text in {"quarterly", "quarter"}:
        return "CURRENT_RELEASE" if age <= timedelta(days=120) else "STALE" if age <= timedelta(days=240) else "VERY_STALE"
    if frequency_text in {"weekly", "week"}:
        return "CURRENT_RELEASE" if age <= timedelta(days=14) else "STALE" if age <= timedelta(days=35) else "VERY_STALE"
    if is_market_closed(session_status) and age <= timedelta(days=4):
        return "LAST_SESSION"
    if age <= timedelta(hours=6):
        return "RECENT"
    if age <= timedelta(days=2):
        return "LAST_SESSION"
    return "STALE" if age <= timedelta(days=14) else "VERY_STALE"


def deduplicate_issuer_earnings(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for raw in events:
        item = dict(raw)
        symbol = str(item.get("symbol") or "").upper()
        issuer = "Alphabet Inc." if symbol in {"GOOG", "GOOGL"} else str(item.get("issuer_name") or item.get("company_name") or symbol)
        event_date = str(item.get("earnings_date") or item.get("date") or "")
        grouped.setdefault((issuer, event_date), []).append(item)
    output: list[dict[str, Any]] = []
    for (issuer, event_date), rows in grouped.items():
        symbols = sorted({str(row.get("symbol") or "").upper() for row in rows if row.get("symbol")})
        primary = next((row for row in rows if str(row.get("symbol") or "").upper() == "GOOGL"), rows[0])
        primary.update(
            {
                "issuer_event_id": _issuer_event_id(issuer, event_date),
                "symbols": symbols,
                "issuer_name": issuer,
                "issuer": issuer,
                "earnings_date": event_date or primary.get("earnings_date"),
                "is_primary_event": True,
                "duplicate_security_event": len(rows) > 1,
            }
        )
        output.append(primary)
        if len(rows) > 1:
            logger.info("earnings_issuer_deduplicated", extra={"issuer": issuer, "symbols": symbols, "earnings_date": event_date})
    return sorted(output, key=lambda item: str(item.get("earnings_date") or item.get("date") or ""))


def classify_semiconductor_contribution(context: dict[str, Any], *, tolerance: float = 0.0001) -> str:
    net = _float(context.get("semiconductor_net_contribution"))
    if net is None:
        return "UNKNOWN"
    positive = abs(_float(context.get("semiconductor_positive_contribution")) or 0.0)
    negative = abs(_float(context.get("semiconductor_negative_contribution")) or 0.0)
    if abs(net) <= tolerance:
        return "MIXED" if positive > tolerance and negative > tolerance else "FLAT"
    return "POSITIVE_CONTRIBUTION" if net > tolerance else "NEGATIVE_CONTRIBUTION"


def _harden_nasdaq(nasdaq: dict[str, Any]) -> dict[str, Any]:
    output = dict(nasdaq)
    qqq = dict(output.get("qqq_holdings") or {})
    method = str(qqq.get("weight_method") or "")
    calculated = bool(qqq.get("weight_verified"))
    official = bool(qqq.get("weight_is_official"))
    qqq.update(
        {
            "weight_calculation_validated": calculated,
            "official_weight_verified": official and calculated,
            "weight_method_classification": (
                "official_etf_weight"
                if official and calculated
                else "reconstructed_market_cap_proxy"
                if "reconstruct" in method
                else "equal_weight_proxy"
                if "equal" in method
                else "unavailable"
            ),
        }
    )
    output["qqq_holdings"] = qqq
    semi = dict(output.get("semiconductor_context") or {})
    previous = semi.get("classification")
    semi["classification"] = classify_semiconductor_contribution(semi)
    semi["classification_tolerance"] = 0.0001
    output["semiconductor_context"] = semi
    if previous != semi["classification"]:
        logger.info(
            "semiconductor_classification_corrected",
            extra={"previous": previous, "classification": semi["classification"], "net": semi.get("semiconductor_net_contribution")},
        )
    earnings = dict(output.get("earnings") or {})
    events = list(earnings.get("upcoming") or earnings.get("events") or [])
    if events:
        deduped = deduplicate_issuer_earnings(events)
        earnings["upcoming"] = deduped
        if "events" in earnings:
            earnings["events"] = deduped
        earnings["issuer_event_count"] = len(deduped)
        earnings["security_event_count"] = len(events)
    output["earnings"] = earnings
    return output


def _harden_corporate_events(corporate: dict[str, Any]) -> dict[str, Any]:
    output = dict(corporate)
    earnings = dict(output.get("earnings") or {})
    for key in ("relevant_upcoming", "mega_cap", "semiconductors"):
        if isinstance(earnings.get(key), list):
            earnings[key] = deduplicate_issuer_earnings(earnings[key])
    output["earnings"] = earnings
    return output


def _harden_fed_expectations(rates: dict[str, Any], *, macro_snapshot: dict[str, Any]) -> dict[str, Any]:
    if not rates:
        return rates
    output = dict(rates)
    try:
        from app.services.fed_expectations_service import build_fed_sanity_check

        output["sanity_check"] = build_fed_sanity_check(output, macro_snapshot=macro_snapshot)
    except Exception as exc:
        output["sanity_check"] = {
            "status": "FAIL",
            "warnings": [f"sanity_check_pipeline_error:{type(exc).__name__}"],
        }
    for meeting in output.get("meetings") or []:
        if isinstance(meeting, dict):
            meeting["probability_semantics"] = "probability_target_range_after_meeting_relative_to_current_range"
            meeting["is_single_meeting_action_probability"] = False
    return output


def _harden_sentiment(sentiment: dict[str, Any], *, social: dict[str, Any], market_closed: bool) -> dict[str, Any]:
    output = dict(sentiment)
    aaii = dict(output.get("aaii") or {})
    aaii["status"] = _sentiment_status(aaii.get("status"), enabled=True)
    output["aaii"] = aaii
    output["retail_qqq"] = output.get("retail_qqq") or {
        "status": "NOT_CONFIGURED",
        "source_classification": "OPTIONAL_SECONDARY",
        "blocking": False,
    }
    output["technology_discussion"] = {
        **social,
        "status": _sentiment_status(social.get("status"), enabled=True),
        "classification": "technology_discussion_context",
        "is_retail_trading_sentiment": False,
        "blocking": False,
    }
    output["fear_greed"] = output.get("fear_greed") or {
        "status": "NOT_CONFIGURED",
        "methodology_known": False,
        "blocking": False,
    }
    prediction = dict(output.get("prediction_markets") or {})
    prediction["status"] = _prediction_status(prediction.get("status"), prediction)
    prediction["blocking"] = False
    output["prediction_markets"] = prediction
    available = [
        aaii.get("status") == "AVAILABLE",
        output["retail_qqq"].get("status") == "AVAILABLE",
        output["technology_discussion"].get("status") == "AVAILABLE",
        output["fear_greed"].get("status") == "AVAILABLE",
        prediction.get("status") == "AVAILABLE",
    ]
    output["sentiment_quality"] = {
        "aaii_available": available[0],
        "retail_symbol_available": available[1],
        "technology_discussion_available": available[2],
        "fear_greed_available": available[3],
        "prediction_markets_available": available[4],
        "source_diversity": sum(available),
        "quality_score": round(sum(available) / len(available), 3),
        "market_closed_adjustment": market_closed,
    }
    output["status"] = "AVAILABLE" if any(available) else "NOT_CONFIGURED"
    output["blocking"] = False
    return output


def _news_digest_view(news: dict[str, Any], legacy: dict[str, Any]) -> dict[str, Any]:
    digest = dict(news.get("digest") or legacy)
    digest.update(
        {
            "status": news.get("status"),
            "search_completed": news.get("search_completed"),
            "market_session_status": news.get("market_session_status"),
            "coverage_window_hours": news.get("coverage_window_hours"),
            "reason": news.get("reason"),
            "blocking": news.get("blocking", False),
        }
    )
    return digest


def _event_window_status(windows: dict[str, Any], *, events_today: dict[str, Any]) -> dict[str, Any]:
    output = dict(windows)
    active = list(output.get("active") or output.get("active_event_windows") or [])
    upcoming = list(output.get("upcoming") or output.get("upcoming_event_windows") or [])
    high_active = any(str(item.get("impact") or "").upper() == "HIGH" for item in active if isinstance(item, dict))
    medium_active = any(str(item.get("impact") or "").upper() == "MEDIUM" for item in active if isinstance(item, dict))
    high_upcoming = any(str(item.get("impact") or "").upper() == "HIGH" for item in upcoming if isinstance(item, dict))
    status = (
        "ACTIVE_HIGH_IMPACT_WINDOW"
        if high_active
        else "ACTIVE_MEDIUM_IMPACT_WINDOW"
        if medium_active
        else "UPCOMING_HIGH_IMPACT_WINDOW"
        if high_upcoming
        else "NO_EVENTS_SCHEDULED"
        if events_today.get("status") == "NO_EVENTS_SCHEDULED"
        else "NO_ACTIVE_WINDOW"
    )
    output["event_risk_window_status"] = status
    return output


def _annotate_event_calendar(calendar: dict[str, Any]) -> dict[str, Any]:
    return {
        key: [_annotate_event(item) for item in values] if isinstance(values, list) else values
        for key, values in calendar.items()
    }


def _annotate_event(raw: Any) -> Any:
    if not isinstance(raw, dict):
        return raw
    item = dict(raw)
    text = " ".join(str(item.get(key) or "") for key in ("name", "event_name", "category"))
    if "EMPLOYMENT SITUATION" not in text.upper() and "NONFARM" not in text.upper() and "PAYROLL" not in text.upper():
        return item
    named = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b",
        text,
        re.IGNORECASE,
    )
    release_value = str(item.get("date") or item.get("release_at") or item.get("time_utc") or "")[:10]
    release_date = _date(release_value)
    release_month = f"{named.group(1).title()} {named.group(2)}" if named else None
    expected_period = None
    if release_date:
        first = release_date.replace(day=1)
        expected_period = (first - timedelta(days=1)).strftime("%B %Y")
    consistent = None if not release_month or not expected_period else release_month == expected_period
    summary = dict((item.get("enrichment") or {}).get("summary") or {})
    invalid = bool(summary.get("invalid_period_mapping")) if consistent is None else not consistent
    item.update(
        {
            "release_period": release_month,
            "release_month": release_month,
            "release_date": release_value or None,
            "period_date_consistent": consistent,
            "calendar_verified": not invalid,
            "invalid_period_mapping": invalid,
        }
    )
    return item


def _walk_semantics(value: Any, *, session_status: str, now: datetime) -> None:
    if isinstance(value, dict):
        source = value.get("source")
        source_url = value.get("source_url")
        if source or source_url:
            classification = classify_source(source, source_url)
            source_text = f"{source or ''} {source_url or ''}".lower()
            redistributor = "fred" in source_text
            official = bool(classification["is_official_source"]) or any(
                token in source_text
                for token in ("fred", "stlouisfed.org", "cboe", "nasdaq official", "cme group", "cmegroup.com")
            )
            value.setdefault("data_origin_is_official", official)
            value.setdefault("distribution_source_is_official", official)
            value.setdefault("source_is_primary_originator", official and not redistributor)
            value.setdefault("source_is_official_redistributor", official and redistributor)
            value.setdefault("is_official_source", official)
        if str(value.get("freshness") or "").upper() in {"", "UNKNOWN"} and (
            value.get("data_as_of") or value.get("retrieved_at")
        ):
            value["freshness"] = classify_freshness(
                data_as_of=value.get("data_as_of"),
                retrieved_at=value.get("retrieved_at"),
                frequency=value.get("frequency"),
                session_status=session_status,
                now=now,
            )
        for nested in value.values():
            _walk_semantics(nested, session_status=session_status, now=now)
    elif isinstance(value, list):
        for nested in value:
            _walk_semantics(nested, session_status=session_status, now=now)


def _update_data_quality(existing: dict[str, Any], full: dict[str, Any]) -> dict[str, Any]:
    output = dict(existing)
    overall = dict(output.get("overall_data_quality") or {})
    readiness = full["readiness"]
    overall["is_ready_for_market_analysis"] = readiness["ready_for_trading_context"]
    overall["blocking_reasons"] = readiness["blocking_reasons"]
    overall["critical_errors"] = readiness["critical_errors"]
    overall["session_aware"] = True
    output["overall_data_quality"] = overall
    output["consumer_quality"] = full["quality"]
    pipeline = dict(output.get("pipeline_integrity") or {})
    pipeline.update(_materialization_flags(full))
    output["pipeline_integrity"] = pipeline
    return output


def _materialization_metadata(metadata: dict[str, Any], full: dict[str, Any]) -> dict[str, Any]:
    output = dict(metadata)
    output["materialization"] = _materialization_flags(full)
    runtime = output.get("multi_source_runtime") or {}
    refresh_mode = str(runtime.get("refresh_mode") or "unknown")
    enrichment = output.get("event_enrichment") or {}
    output["runtime_io"] = {
        "refresh_mode": refresh_mode,
        "provider_calls": 0 if refresh_mode == "false" else int(runtime.get("provider_calls") or 0),
        "actual_network_calls": 0 if refresh_mode == "false" else int(runtime.get("provider_calls") or 0),
        "browser_calls": 0,
        "AI_called": False if refresh_mode == "false" else bool(enrichment.get("AI_called")),
        "cache_used": True if refresh_mode == "false" else bool(runtime.get("cache_used")),
        "persisted_diagnostics_are_historical": True,
    }
    return output


def _materialization_flags(full: dict[str, Any]) -> dict[str, bool]:
    required = all(key in full for key in ("macro_snapshot", "event_calendar", "nasdaq_context", "news_context"))
    try:
        json.dumps(full, default=str)
        serialized = True
    except (TypeError, ValueError, OverflowError):
        serialized = False
    return {
        "snapshot_built_from_db": bool((full.get("data_quality") or {}).get("pipeline_integrity", {}).get("snapshot_built_from_db", True)),
        "snapshot_materialization_completed": required,
        "snapshot_serialization_completed": serialized,
        "snapshot_contract_validation_completed": required and bool(full.get("symbol")),
        "consumer_materialization_completed": False,
    }


def _macro_available(snapshot: dict[str, Any]) -> bool:
    return any(
        isinstance(bucket, dict) and any(isinstance(item, dict) and item.get("value") is not None for item in bucket.values())
        for key, bucket in snapshot.items()
        if key != "provider_results"
    )


def _risk_available(risk: dict[str, Any]) -> bool:
    status = str(risk.get("status") or "").lower()
    if status in {"complete", "available", "found", "partial", "degraded", "stale_acceptable"}:
        return True
    return any(str((risk.get(key) or {}).get("status") or "").lower() in {"found", "available"} for key in ("vix", "vvix", "skew"))


def _nasdaq_available(nasdaq: dict[str, Any]) -> bool:
    qqq = nasdaq.get("qqq_holdings") or {}
    return bool(qqq.get("holdings_count") or qqq.get("holdings") or qqq.get("top_holdings")) or str(nasdaq.get("status") or "").lower() == "available"


def _event_section_status(context: dict[str, Any]) -> str:
    return "AVAILABLE" if context.get("status") in {"AVAILABLE", "NO_EVENTS_SCHEDULED"} else str(context.get("status") or "NOT_AVAILABLE")


def _section_status(available: bool) -> str:
    return "AVAILABLE" if available else "NOT_AVAILABLE"


def _optional_status(block: dict[str, Any]) -> str:
    status = str(block.get("status") or "").upper()
    if status in {"FOUND", "AVAILABLE", "COMPLETE", "PARTIAL"}:
        return "AVAILABLE"
    if status in {"STALE_ACCEPTABLE", "LAST_KNOWN_GOOD"}:
        return "LAST_KNOWN_GOOD"
    if status in {"DISABLED", "NOT_CONFIGURED"}:
        return "NOT_CONFIGURED"
    if status in {"PROVIDER_FAILED", "PROVIDER_UNAVAILABLE", "SSL_ERROR", "ACCESS_RESTRICTED"}:
        return "PROVIDER_UNAVAILABLE"
    if status == "PIPELINE_ERROR":
        return status
    if (block.get("data_quality") or {}).get("no_data_found"):
        return "NO_DATA_EXPECTED"
    return "AVAILABLE" if block and any(value not in (None, {}, [], "") for value in block.values()) else "NOT_AVAILABLE"


def _readiness_confidence(section_status: dict[str, str], ready: bool) -> float:
    weights = {
        "macro_snapshot": 0.22,
        "event_risk": 0.18,
        "market_schedule": 0.15,
        "risk_context": 0.22,
        "nasdaq_context": 0.23,
    }
    score = sum(weight for key, weight in weights.items() if section_status.get(key) == "AVAILABLE")
    return round(score if ready else score * 0.6, 3)


def _sentiment_status(value: Any, *, enabled: bool) -> str:
    status = str(value or "").lower()
    if not enabled or status in {"disabled", "not_configured"}:
        return "NOT_CONFIGURED"
    if status in {"found", "available", "partial"}:
        return "AVAILABLE"
    if status == "access_restricted":
        return "ACCESS_RESTRICTED"
    if status in {"not_found", "no_data_available"}:
        return "NO_NEW_SURVEY"
    return "PROVIDER_UNAVAILABLE"


def _prediction_status(value: Any, payload: dict[str, Any]) -> str:
    status = str(value or "").lower()
    if status in {"disabled", "not_configured", ""}:
        return "NOT_CONFIGURED"
    if status in {"found", "available", "partial"}:
        return "AVAILABLE"
    if status in {"not_found", "no_data_available"}:
        return "NO_RELEVANT_MARKETS"
    if status == "ssl_error" or payload.get("ssl_error") or payload.get("failure_type") == "ssl_error":
        return "SSL_ERROR"
    return "PROVIDER_UNAVAILABLE"


def _news_lookback(settings: Settings, status: str) -> int:
    if status == "weekend":
        return int(settings.news_weekend_lookback_hours)
    if status == "holiday":
        return int(settings.news_holiday_lookback_hours)
    return int(settings.news_market_open_lookback_hours)


def _issuer_event_id(issuer: str, event_date: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", issuer.lower()).strip("-")
    return f"earnings:{slug}:{event_date or 'unknown'}"


def _score(value: Any, *, default: float = 0.0) -> float:
    try:
        return round(max(0.0, min(1.0, float(value))), 3)
    except (TypeError, ValueError):
        return default


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value
