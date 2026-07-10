from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

from app.services.data_freshness_service import parse_datetime


SEMICONDUCTOR_SYMBOLS = {"NVDA", "AVGO", "AMD", "MU", "INTC", "AMAT", "QCOM", "ARM", "ASML", "LRCX", "KLAC", "MRVL"}


def apply_context_extensions(contract: dict[str, Any]) -> dict[str, Any]:
    output = dict(contract)
    now = datetime.now(UTC)
    output["event_calendar"] = _enrich_event_calendar(output.get("event_calendar") or {}, now=now)
    output["events_today"] = [_enrich_event(event, now=now) for event in output.get("events_today") or []]
    output["next_24h_events"] = _events_within(output["event_calendar"], now=now, hours=24)
    output["next_7d_critical_events"] = [
        event for event in output["event_calendar"].get("critical_macro_events", []) if _event_dt(event) and now <= _event_dt(event) <= now + timedelta(days=7)
    ]
    output["fed_communications_today"] = [
        event for event in output["event_calendar"].get("fed_communications", []) if _event_dt(event) and _event_dt(event).date() == now.date()
    ]
    output["recently_released_events"] = _recently_released(output["event_calendar"], now=now)
    output["event_windows"] = _normalize_event_windows(output.get("event_windows") or {}, output["event_calendar"], now=now)
    output["positioning"] = output.get("positioning") or build_positioning_context()
    output["sentiment_context"] = output.get("sentiment_context") or build_sentiment_context()
    output["risk_sentiment"] = build_risk_sentiment(output.get("macro_snapshot") or {})
    output["fomc_context"] = build_fomc_context(output["event_calendar"])
    output["nasdaq_context"] = enrich_nasdaq_context(output.get("nasdaq_context") or {}, output.get("news_context") or {})
    output["news_context"] = enrich_news_context(output.get("news_context") or {})
    output["news_digest"] = build_news_digest(output["news_context"])
    return output


def block_metadata(*, status: str, source: str | None = None, source_url: str | None = None, freshness: str = "UNKNOWN", reliability: float | None = None, confidence: float | None = None, warnings: list[str] | None = None, errors: list[str] | None = None) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "status": status,
        "data_as_of": None,
        "retrieved_at": now,
        "valid_until": None,
        "next_refresh_at": None,
        "source": source,
        "source_url": source_url,
        "provider_type": "DB_DERIVED" if status != "no_data_available" else None,
        "freshness": freshness,
        "reliability": reliability,
        "confidence": confidence,
        "warnings": warnings or [],
        "errors": errors or [],
    }


def build_positioning_context() -> dict[str, Any]:
    meta = block_metadata(
        status="not_found",
        source="CFTC",
        source_url="https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
        freshness="WEEKLY",
        reliability=0.95,
        confidence=0.0,
        warnings=["cot_provider_not_configured"],
    )
    return {
        **meta,
        "cot": {
            "nasdaq_100": {
                "report_date": None,
                "publication_date": None,
                "contract_scope": None,
                "asset_managers": {"long": None, "short": None, "spreading": None, "net": None, "net_change_week": None},
                "leveraged_funds": {"long": None, "short": None, "spreading": None, "net": None, "net_change_week": None},
                "dealers": {"long": None, "short": None, "net": None},
                "open_interest": None,
                "source": "CFTC",
                "source_url": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
                "freshness": "WEEKLY",
                "reliability": 0.95,
                "status": "not_found",
                "reason": "cot_not_loaded_runtime",
                "warnings": ["cot_not_loaded_runtime"],
            }
        },
    }


def build_sentiment_context() -> dict[str, Any]:
    return {
        **block_metadata(status="not_found", source="AAII", source_url="https://www.aaii.com/sentimentsurvey", freshness="WEEKLY", warnings=["aaii_not_loaded_runtime", "retail_social_optional_not_configured"]),
        "aaii": {
            "status": "not_found",
            "survey_date": None,
            "bullish_pct": None,
            "neutral_pct": None,
            "bearish_pct": None,
            "bull_bear_spread": None,
            "historical_average_bullish_pct": None,
            "source": "AAII",
            "source_url": "https://www.aaii.com/sentimentsurvey",
            "freshness": "WEEKLY",
            "reliability": None,
            "reason": "aaii_not_loaded_runtime",
            "warnings": ["aaii_not_loaded_runtime"],
        },
        "retail_social": {
            "QQQ": {
                "sentiment_score": None,
                "bullish_messages": None,
                "bearish_messages": None,
                "message_volume": None,
                "message_volume_change_pct": None,
                "source": None,
                "source_url": None,
                "freshness": None,
                "reliability": None,
                "warnings": ["social_sentiment_optional_not_configured"],
            }
        },
    }


def build_risk_sentiment(macro_snapshot: dict[str, Any]) -> dict[str, Any]:
    vix = (macro_snapshot.get("financial_conditions") or {}).get("VIXCLS") or {}
    return {
        **block_metadata(status="partial" if vix else "no_data_available", source="FRED", source_url="https://fred.stlouisfed.org/series/VIXCLS", freshness=vix.get("freshness") or "UNKNOWN", reliability=vix.get("reliability"), confidence=vix.get("reliability")),
        "vix": vix,
        "vix_term_structure": {
            "front_month": None,
            "second_month": None,
            "spread": None,
            "structure": "UNKNOWN",
            "source": None,
            "source_url": None,
            "warnings": ["vix_term_structure_provider_not_configured"],
        },
        "put_call_ratio": {
            "value": None,
            "data_as_of": None,
            "source": None,
            "source_url": None,
            "warnings": ["put_call_provider_not_configured"],
        },
        "fear_greed": {
            "value": None,
            "classification": None,
            "source": None,
            "source_url": None,
            "warnings": ["fear_greed_optional_not_configured"],
        },
    }


def build_fomc_context(event_calendar: dict[str, Any]) -> dict[str, Any]:
    events = event_calendar.get("critical_macro_events", []) + event_calendar.get("fed_communications", []) + event_calendar.get("other_economic_events", [])
    fomc = next((event for event in events if "FOMC" in str(event.get("category") or event.get("name") or "").upper()), None)
    if not fomc:
        return {
            **block_metadata(status="no_data_available", source="Federal Reserve", source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm", warnings=["fomc_event_not_in_current_window"]),
            "meeting_date": None,
            "decision_time_utc": None,
            "press_conference_time_utc": None,
            "current_target_range_lower": None,
            "current_target_range_upper": None,
            "expected_action": "unknown",
            "expected_change_bps": None,
            "probability_hold": None,
            "probability_cut_25bps": None,
            "probability_hike_25bps": None,
            "probability_source": None,
            "probability_source_url": None,
            "previous_action": None,
            "latest_statement_url": None,
            "latest_minutes_url": None,
        }
    enrichment_fomc = ((fomc.get("enrichment") or {}).get("fomc_context") or {})
    return {
        **block_metadata(status="available", source=fomc.get("source"), source_url=fomc.get("source_url"), freshness="FRESH", reliability=fomc.get("reliability"), confidence=fomc.get("reliability")),
        "meeting_date": enrichment_fomc.get("meeting_date") or fomc.get("date"),
        "decision_time_utc": enrichment_fomc.get("decision_time_utc") or fomc.get("time_utc"),
        "press_conference_time_utc": enrichment_fomc.get("press_conference_time_utc"),
        "current_target_range_lower": enrichment_fomc.get("current_target_range_lower"),
        "current_target_range_upper": enrichment_fomc.get("current_target_range_upper"),
        "expected_action": enrichment_fomc.get("expected_action") or "unknown",
        "expected_change_bps": enrichment_fomc.get("expected_change_bps"),
        "probability_hold": enrichment_fomc.get("probability_hold"),
        "probability_cut_25bps": enrichment_fomc.get("probability_cut_25bps") or enrichment_fomc.get("probability_cut"),
        "probability_hike_25bps": enrichment_fomc.get("probability_hike_25bps") or enrichment_fomc.get("probability_hike"),
        "probability_source": enrichment_fomc.get("probability_source"),
        "probability_source_url": enrichment_fomc.get("probability_source_url"),
        "previous_action": enrichment_fomc.get("previous_action"),
        "latest_statement_url": enrichment_fomc.get("latest_statement_url"),
        "latest_minutes_url": enrichment_fomc.get("latest_minutes_url"),
    }


def enrich_news_context(news_context: dict[str, Any]) -> dict[str, Any]:
    output = dict(news_context)
    latest = []
    for item in output.get("latest") or []:
        article = dict(item)
        source_url = article.get("source_url")
        article["aggregator_url"] = source_url if "news.google.com" in str(source_url or "") else None
        article["canonical_url"] = article.get("canonical_url") or (None if article["aggregator_url"] else source_url)
        article["canonical_status"] = "canonical_resolved" if article.get("canonical_url") else ("canonical_unresolved" if article["aggregator_url"] else "canonical_unavailable")
        article["redirect_chain"] = article.get("redirect_chain") or []
        article["entities"] = article.get("entities") or article.get("symbols") or []
        article["event_type"] = article.get("event_type") or _topic_event_type(article.get("topics") or [])
        article["relevance_score"] = _relevance_score(article.get("relevance"))
        article["language"] = article.get("language") or "en"
        article["duplicate_group_id"] = _duplicate_group_id(article)
        warnings = list(article.get("warnings") or [])
        if not article.get("summary") and "summary_source_unavailable" not in warnings:
            warnings.append("summary_source_unavailable")
        if article.get("canonical_status") == "canonical_unresolved" and "canonical_unresolved" not in warnings:
            warnings.append("canonical_unresolved")
        article["warnings"] = warnings
        latest.append(article)
    output["latest"] = latest
    output["status"] = "available" if latest else "no_data_available"
    output["summary_coverage_pct"] = _pct(sum(1 for item in latest if item.get("summary")), len(latest))
    output["canonical_url_coverage_pct"] = _pct(sum(1 for item in latest if item.get("canonical_url")), len(latest))
    output["duplicate_group_count"] = len({item.get("duplicate_group_id") for item in latest if item.get("duplicate_group_id")})
    return output


def build_news_digest(news_context: dict[str, Any], *, coverage_window_hours: int = 24) -> dict[str, Any]:
    latest = list(news_context.get("latest") or [])
    source_urls = [item.get("canonical_url") or item.get("source_url") for item in latest if item.get("canonical_url") or item.get("source_url")]
    topics: dict[str, list[dict[str, Any]]] = {}
    for item in latest:
        for topic in item.get("topics") or ["uncategorized"]:
            topics.setdefault(str(topic), []).append(item)
    topic_rows = [
        {
            "topic": topic,
            "article_count": len(items),
            "weighted_relevance": round(sum(_relevance_score(item.get("relevance")) for item in items) / len(items), 3),
            "representative_articles": [item.get("canonical_url") or item.get("source_url") for item in items[:3] if item.get("canonical_url") or item.get("source_url")],
        }
        for topic, items in sorted(topics.items())
    ]
    drivers = []
    for topic, items in sorted(topics.items()):
        urls = [item.get("canonical_url") or item.get("source_url") for item in items if item.get("canonical_url") or item.get("source_url")]
        if not urls:
            continue
        drivers.append(
            {
                "driver_id": _stable_id(topic, urls),
                "category": _driver_category(topic),
                "headline": _driver_headline(topic),
                "summary": _driver_summary(topic, items),
                "affected_symbols": sorted({symbol for item in items for symbol in item.get("symbols", [])}),
                "source_count": len(set(urls)),
                "source_urls": sorted(set(urls)),
                "confidence": 0.75 if len(set(urls)) > 1 else 0.45,
                "reliability": round(sum(float(item.get("reliability") or 0.0) for item in items) / len(items), 3),
                "is_confirmed_by_multiple_sources": len(set(urls)) > 1,
            }
        )
    return {
        **block_metadata(status="available" if latest else "no_data_available", source="news_context.latest", freshness="FRESH" if latest else "UNKNOWN", reliability=0.64 if latest else None, confidence=0.6 if latest else None),
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "coverage_window_hours": coverage_window_hours,
        "source_count": len({item.get("source") for item in latest if item.get("source")}),
        "high_reliability_source_count": sum(1 for item in latest if float(item.get("reliability") or 0) >= 0.8),
        "official_source_count": sum(1 for item in latest if item.get("is_official_source")),
        "article_count": len(latest),
        "topics": topic_rows,
        "drivers": drivers,
        "narrative_balance": {
            "positive_driver_count": 0,
            "negative_driver_count": 0,
            "mixed_driver_count": len(drivers),
            "classification": "MIXED" if drivers else "INSUFFICIENT_DATA",
        },
        "warnings": [] if drivers else ["news_digest_insufficient_sources"],
    }


def enrich_nasdaq_context(nasdaq_context: dict[str, Any], news_context: dict[str, Any]) -> dict[str, Any]:
    output = dict(nasdaq_context)
    holdings = list((output.get("qqq_holdings") or {}).get("top_holdings") or [])
    stocks = list((output.get("mega_cap_snapshot") or {}).get("stocks") or [])
    breadth = output.get("mega_cap_breadth") or {}
    output["breadth_summary"] = _breadth_summary(breadth)
    output["concentration"] = _concentration(holdings)
    output["semiconductor_context"] = _semiconductor_context(holdings, stocks)
    output["driver_context"] = _driver_context(holdings, stocks, output.get("earnings") or {}, news_context)
    output["status"] = "available" if output else "no_data_available"
    return output


def _enrich_event_calendar(calendar: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    return {key: [_enrich_event(event, now=now) for event in value] for key, value in calendar.items()}


def _enrich_event(event: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    item = dict(event)
    release = _event_dt(item)
    before = int(item.get("default_risk_window_before_minutes") or 0)
    after = int(item.get("default_risk_window_after_minutes") or 0)
    start = release - timedelta(minutes=before) if release else None
    end = release + timedelta(minutes=after) if release else None
    summary = (item.get("enrichment") or {}).get("summary") or {}
    item.update(
        {
            "minutes_until_event": int((release - now).total_seconds() // 60) if release and release >= now else None,
            "minutes_since_release": int((now - release).total_seconds() // 60) if release and release <= now else None,
            "temporal_status": summary.get("temporal_status") or _temporal_status(release, item),
            "event_type": summary.get("event_type") or str(item.get("category") or "").lower(),
            "release_at": release.isoformat().replace("+00:00", "Z") if release else None,
            "window_start_utc": start.isoformat().replace("+00:00", "Z") if start else None,
            "window_end_utc": end.isoformat().replace("+00:00", "Z") if end else None,
            "is_window_active": bool(start and end and start <= now <= end),
            "is_upcoming_window": bool(start and now < start),
            "is_high_impact_window_active": bool(str(item.get("impact")).upper() == "HIGH" and start and end and start <= now <= end),
            "temporal_proximity": _temporal_proximity(release, now),
            "quantitative_fields_applicable": summary.get("quantitative_fields_applicable"),
            "enrichment_completeness": summary.get("completeness_score"),
            "awaiting_actual": summary.get("temporal_status") == "awaiting_actual",
            "next_refresh_at": ((release.isoformat().replace("+00:00", "Z")) if release and now >= release and not summary.get("has_actual") else None),
        }
    )
    return item


def _events_within(calendar: dict[str, Any], *, now: datetime, hours: int) -> list[dict[str, Any]]:
    events = [event for values in calendar.values() for event in values]
    return [event for event in events if _event_dt(event) and now <= _event_dt(event) <= now + timedelta(hours=hours)]


def _recently_released(calendar: dict[str, Any], *, now: datetime) -> list[dict[str, Any]]:
    events = [event for values in calendar.values() for event in values]
    return [event for event in events if _event_dt(event) and now - timedelta(hours=24) <= _event_dt(event) <= now]


def _normalize_event_windows(raw: dict[str, Any], calendar: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    active = [_window_row(item, "ACTIVE", now=now) for item in raw.get("active_event_windows", [])]
    upcoming = [_window_row(item, "UPCOMING", now=now) for item in raw.get("upcoming_event_windows", [])]
    recently_closed = []
    for event in _recently_released(calendar, now=now):
        if event.get("window_end_utc") and parse_datetime(event["window_end_utc"]) and parse_datetime(event["window_end_utc"]) <= now:
            recently_closed.append(_window_from_event(event, "CLOSED", now=now))
    return {
        **block_metadata(status="available", source="official_event_calendar", freshness="FRESH", reliability=0.9, confidence=0.9),
        "active": [item for item in active if item],
        "upcoming": [item for item in upcoming if item],
        "recently_closed": recently_closed,
        "legacy": raw,
    }


def _window_row(item: dict[str, Any], status: str, *, now: datetime) -> dict[str, Any] | None:
    event = item.get("event") or {}
    enriched = _enrich_event(event, now=now)
    enriched["window_start_utc"] = item.get("window_start_utc") or enriched.get("window_start_utc")
    enriched["window_end_utc"] = item.get("window_end_utc") or enriched.get("window_end_utc")
    return _window_from_event(enriched, status, now=now)


def _window_from_event(event: dict[str, Any], status: str, *, now: datetime) -> dict[str, Any]:
    start = parse_datetime(event.get("window_start_utc"))
    release = parse_datetime(event.get("release_at") or event.get("time_utc"))
    end = parse_datetime(event.get("window_end_utc"))
    return {
        "event_id": event.get("event_id"),
        "event_name": event.get("name"),
        "event_type": event.get("event_type") or event.get("category"),
        "impact": event.get("impact"),
        "window_start_utc": event.get("window_start_utc"),
        "release_at_utc": event.get("release_at") or event.get("time_utc"),
        "window_end_utc": event.get("window_end_utc"),
        "status": status,
        "minutes_to_start": int((start - now).total_seconds() // 60) if start else None,
        "minutes_to_release": int((release - now).total_seconds() // 60) if release else None,
        "minutes_to_end": int((end - now).total_seconds() // 60) if end else None,
        "reason": "HIGH_IMPACT_MACRO" if str(event.get("impact")).upper() == "HIGH" else "SCHEDULED_EVENT",
        "source": event.get("source"),
        "source_url": event.get("source_url"),
    }


def _breadth_summary(breadth: dict[str, Any]) -> dict[str, Any]:
    avg = float(breadth.get("weighted_average_change_pct") or 0.0)
    classification = "POSITIVE" if avg > 0.05 else "NEGATIVE" if avg < -0.05 else "MIXED"
    return {
        "positive_count": int(breadth.get("positive_count") or 0),
        "negative_count": int(breadth.get("negative_count") or 0),
        "neutral_count": int(breadth.get("neutral_count") or 0),
        "weighted_positive_pct": breadth.get("weighted_positive_pct"),
        "weighted_negative_pct": breadth.get("weighted_negative_pct"),
        "weighted_average_change_pct": breadth.get("weighted_average_change_pct"),
        "classification": classification,
    }


def _concentration(holdings: list[dict[str, Any]]) -> dict[str, Any]:
    weights = [float(item.get("weight") or 0.0) for item in holdings]
    top5 = round(sum(weights[:5]), 4)
    top10 = round(sum(weights[:10]), 4)
    largest = holdings[0] if holdings else {}
    classification = "HIGH" if top10 >= 45 else "MEDIUM" if top10 >= 30 else "LOW"
    return {
        "top_5_weight_pct": top5,
        "top_10_weight_pct": top10,
        "largest_constituent_symbol": largest.get("symbol"),
        "largest_constituent_weight_pct": largest.get("weight"),
        "classification": classification,
    }


def _semiconductor_context(holdings: list[dict[str, Any]], stocks: list[dict[str, Any]]) -> dict[str, Any]:
    by_symbol = {str(item.get("symbol")).upper(): item for item in stocks}
    weight_by_symbol = {str(item.get("symbol")).upper(): float(item.get("weight") or 0.0) for item in holdings}
    holding_symbols = {str(item.get("symbol")).upper() for item in holdings}
    relevant = [symbol for symbol in SEMICONDUCTOR_SYMBOLS if symbol in holding_symbols or symbol in by_symbol]
    resolved = [symbol for symbol in relevant if symbol in by_symbol]
    unresolved = [symbol for symbol in relevant if symbol not in by_symbol]
    excluded = [symbol for symbol in SEMICONDUCTOR_SYMBOLS if symbol not in relevant]
    positive = sum(1 for symbol in resolved if float(by_symbol[symbol].get("change_pct") or 0.0) > 0)
    negative = sum(1 for symbol in resolved if float(by_symbol[symbol].get("change_pct") or 0.0) < 0)
    total_weight = sum(weight_by_symbol.get(symbol, 0.0) for symbol in resolved)
    weighted = None
    if total_weight:
        weighted = round(sum(weight_by_symbol.get(symbol, 0.0) * float(by_symbol[symbol].get("change_pct") or 0.0) for symbol in resolved) / total_weight, 4)
    classification = "INSUFFICIENT_DATA" if not resolved else "POSITIVE" if positive > negative else "NEGATIVE" if negative > positive else "MIXED"
    return {
        "symbols": sorted(SEMICONDUCTOR_SYMBOLS),
        "requested_count": len(SEMICONDUCTOR_SYMBOLS),
        "relevant_count": len(relevant),
        "resolved_count": len(resolved),
        "resolved_symbols": sorted(resolved),
        "unresolved_symbols": sorted(unresolved),
        "excluded_symbols": sorted(excluded),
        "exclusion_reason": {symbol: "not_present_in_current_qqq_holdings_or_snapshot" for symbol in excluded},
        "positive_count": positive,
        "negative_count": negative,
        "weighted_change_pct": weighted,
        "classification": classification,
        "recent_news_count": 0,
        "upcoming_earnings_count": 0,
        "data_quality": {
            "resolution_pct": _pct(len(resolved), len(SEMICONDUCTOR_SYMBOLS)),
            "unresolved_reason": {symbol: "missing_from_mega_cap_snapshot_provider_output" for symbol in unresolved},
        },
    }


def _driver_context(holdings: list[dict[str, Any]], stocks: list[dict[str, Any]], earnings: dict[str, Any], news_context: dict[str, Any]) -> list[dict[str, Any]]:
    stock_by_symbol = {str(item.get("symbol")).upper(): item for item in stocks}
    earnings_by_symbol = {str(item.get("symbol")).upper(): item for item in earnings.get("upcoming", []) if isinstance(item, dict)}
    latest_news = news_context.get("latest") or []
    output = []
    for holding in holdings[:15]:
        symbol = str(holding.get("symbol") or "").upper()
        stock = stock_by_symbol.get(symbol, {})
        related_news = [item for item in latest_news if symbol in [str(value).upper() for value in item.get("symbols", [])]]
        earnings_item = earnings_by_symbol.get(symbol)
        weight = float(holding.get("weight") or 0.0)
        change = stock.get("change_pct")
        output.append(
            {
                "symbol": symbol,
                "qqq_weight": weight,
                "change_pct": change,
                "weighted_contribution": round(weight * float(change) / 100, 6) if change is not None else None,
                "has_recent_news": bool(related_news),
                "recent_news_count": len(related_news),
                "upcoming_earnings": earnings_item is not None,
                "earnings_date": (earnings_item or {}).get("date"),
            }
        )
    return output


def _event_dt(event: dict[str, Any]) -> datetime | None:
    return parse_datetime(event.get("time_utc") or event.get("release_at") or event.get("date"))


def _temporal_status(release: datetime | None, event: dict[str, Any]) -> str:
    if release is None:
        return "scheduled"
    now = datetime.now(UTC)
    actual = event.get("actual") or ((event.get("enrichment") or {}).get("actual"))
    if now < release:
        return "pre_release"
    return "released" if actual not in (None, "") else "awaiting_actual"


def _temporal_proximity(release: datetime | None, now: datetime) -> str:
    if release is None:
        return "UNKNOWN"
    minutes = int((release - now).total_seconds() // 60)
    if minutes < -60:
        return "PAST"
    if minutes < 0:
        return "RECENTLY_RELEASED"
    if minutes <= 60:
        return "WITHIN_1H"
    if minutes <= 24 * 60:
        return "WITHIN_24H"
    return "FUTURE"


def _relevance_score(value: Any) -> float:
    text = str(value or "").upper()
    return {"HIGH": 1.0, "MEDIUM": 0.6, "LOW": 0.3}.get(text, 0.5)


def _topic_event_type(topics: list[Any]) -> str | None:
    values = {str(topic).lower() for topic in topics}
    if "semiconductors" in values:
        return "SEMICONDUCTORS"
    if "mega-cap" in values or "mega_cap" in values:
        return "MEGA_CAP"
    return None


def _driver_category(topic: str) -> str:
    text = topic.lower()
    if "semiconductor" in text:
        return "SEMICONDUCTORS"
    if "earning" in text:
        return "EARNINGS"
    if "fed" in text:
        return "FED"
    if "macro" in text:
        return "MACRO"
    return "MEGA_CAP"


def _driver_headline(topic: str) -> str:
    return f"{topic.replace('_', ' ').title()} news cluster"


def _driver_summary(topic: str, items: list[dict[str, Any]]) -> str:
    titles = [str(item.get("title")) for item in items[:2] if item.get("title")]
    return "; ".join(titles) if titles else f"{topic} coverage from available sources"


def _duplicate_group_id(article: dict[str, Any]) -> str:
    key = str(article.get("canonical_url") or article.get("source_url") or article.get("title") or "").lower()
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def _stable_id(topic: str, urls: list[str]) -> str:
    return hashlib.sha1(f"{topic}:{'|'.join(sorted(urls))}".encode("utf-8")).hexdigest()[:16]


def _pct(count: int, total: int) -> float:
    return round((count / total) * 100, 2) if total else 0.0
