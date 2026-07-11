from __future__ import annotations

import json
from typing import Any


CONTRACT_NAME = "ai_trader_market_context"
SCHEMA_VERSION = "1.0"


def build_ai_trader_market_context(full: dict[str, Any]) -> dict[str, Any]:
    nasdaq = full.get("nasdaq_context") or {}
    data_quality = full.get("data_quality") or {}
    consumer = {
        "contract": CONTRACT_NAME,
        "schema_version": SCHEMA_VERSION,
        "symbol": full.get("symbol"),
        "generated_at": full.get("generated_at_utc") or full.get("generated_at"),
        "snapshot_summary": _snapshot_summary(full),
        "service_role": "data provider only",
        "readiness": _readiness(data_quality),
        "data_quality": _compact_quality(data_quality),
        "macro_snapshot": full.get("macro_snapshot") or {},
        "event_calendar": _compact_events(full.get("event_calendar") or {}),
        "event_windows": full.get("event_windows") or {},
        "positioning": full.get("positioning") or {},
        "sentiment_context": _compact_sentiment_context(full.get("sentiment_context") or {}),
        "social_sentiment": full.get("social_sentiment") or _empty_social(),
        "risk_sentiment": full.get("risk_sentiment") or {},
        "risk_context": full.get("risk_context") or {},
        "rates_expectations": full.get("rates_expectations") or {},
        "market_schedule": _compact_market_schedule(full.get("market_schedule") or {}),
        "corporate_events": full.get("corporate_events") or {},
        "nasdaq_context": _compact_nasdaq(nasdaq),
        "news_digest": full.get("news_digest") or {},
        "news_context": {"latest": (full.get("news_context") or {}).get("latest") or []},
        "warnings": _consumer_warnings(full),
        "decisions_delegated_to": "AI-TRADER",
        "trading_logic": "not implemented; data service only",
        "debug_available": "/market-context/mnq/debug",
        "payload_size_bytes": 0,
        "events_today": full.get("events_today") or [],
        "metadata": {
            "event_enrichment": (full.get("metadata") or {}).get("event_enrichment") or {},
            "refresh_mode": ((full.get("metadata") or {}).get("multi_source_runtime") or {}).get("refresh_mode"),
        },
    }
    consumer["payload_size_bytes"] = len(json.dumps(consumer, default=str, separators=(",", ":")).encode("utf-8"))
    return consumer


def _readiness(data_quality: dict[str, Any]) -> dict[str, Any]:
    overall = data_quality.get("overall_data_quality") or {}
    explicit_errors = data_quality.get("critical_errors") or data_quality.get("errors") or []
    blocking = list(overall.get("blocking_reasons") or [])
    return {
        "ready": len(explicit_errors) == 0 and not blocking,
        "critical_errors": len(explicit_errors) + len(blocking),
        "blocking_reasons": blocking,
    }


def _compact_quality(data_quality: dict[str, Any]) -> dict[str, Any]:
    overall = data_quality.get("overall_data_quality") or {}
    return {
        "completeness_score": overall.get("completeness_score"),
        "freshness_score": overall.get("freshness_score"),
        "reliability_score": overall.get("reliability_score"),
        "critical_missing_count": overall.get("critical_missing_count"),
        "missing_critical_fields": data_quality.get("missing_critical_fields") or overall.get("missing_critical_fields") or [],
        "section_quality": data_quality.get("section_quality") or {},
        "pipeline_integrity": data_quality.get("pipeline_integrity") or {},
        "multi_source_pipeline": _compact_multi_source_pipeline(data_quality.get("multi_source_pipeline") or {}),
    }


def _compact_events(calendar: dict[str, Any]) -> dict[str, Any]:
    return {
        "critical_macro_events": [_compact_event_item(item) for item in (calendar.get("critical_macro_events") or [])[:12]],
        "fed_communications": [_compact_event_item(item) for item in (calendar.get("fed_communications") or [])[:8]],
        "other_economic_events": [_compact_event_item(item) for item in (calendar.get("other_economic_events") or [])[:12]],
    }


def _compact_nasdaq(nasdaq: dict[str, Any]) -> dict[str, Any]:
    output = {
        "qqq_holdings": nasdaq.get("qqq_holdings") or {},
        "sector_exposure": nasdaq.get("sector_exposure") or {},
        "mega_cap_snapshot": _compact_snapshot(nasdaq.get("mega_cap_snapshot") or {}),
        "mega_cap_breadth": _compact_breadth(nasdaq.get("mega_cap_breadth") or {}),
        "concentration": nasdaq.get("concentration") or {},
        "semiconductor_context": nasdaq.get("semiconductor_context") or {},
        "earnings": nasdaq.get("earnings") or {},
        "qqq_options": _compact_options(((nasdaq.get("nasdaq_context_additions") or {}).get("qqq_options") or nasdaq.get("qqq_options") or {})),
        "nasdaq_100_official_snapshot": _compact_nasdaq_100(nasdaq.get("nasdaq_100_official_snapshot") or {}),
    }
    if isinstance(output["qqq_holdings"], dict) and output["qqq_holdings"].get("top_holdings"):
        output["qqq_holdings"] = {
            **output["qqq_holdings"],
            "holdings": (output["qqq_holdings"].get("holdings") or [])[:15],
            "top_holdings": output["qqq_holdings"]["top_holdings"][:15],
        }
    return output


def _compact_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {**snapshot, "stocks": (snapshot.get("stocks") or [])[:20]}


def _compact_breadth(breadth: dict[str, Any]) -> dict[str, Any]:
    if not breadth:
        return {}
    missing_weights = ((breadth.get("data_quality") or {}).get("missing_weights") or [])
    output = dict(breadth)
    if missing_weights and not breadth.get("weight_method"):
        output["calculation_method"] = "equal_weight_proxy"
        output["is_proxy"] = True
        output["proxy_reason"] = "QQQ constituent weights unavailable; equal-weight mega-cap proxy only"
        output["weighted_average_change_pct"] = None
        output["weighted_positive_pct"] = None
        output["weighted_negative_pct"] = None
        output["top_positive_contributors"] = [_strip_proxy_weight(item) for item in output.get("top_positive_contributors") or []]
        output["top_negative_contributors"] = [_strip_proxy_weight(item) for item in output.get("top_negative_contributors") or []]
    return output


def _strip_proxy_weight(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": item.get("symbol"),
        "change_pct": item.get("change_pct"),
        "weight": None,
        "weighted_contribution": None,
        "calculation_method": "equal_weight_proxy",
    }


def _compact_multi_source_pipeline(pipeline: dict[str, Any]) -> dict[str, Any]:
    output = dict(pipeline)
    output["warnings"] = _compact_messages(output.get("warnings") or [])
    output["errors"] = _compact_messages(output.get("errors") or [])
    return output


def _compact_messages(messages: list[Any]) -> list[str]:
    compacted: list[str] = []
    for message in messages:
        text = str(message)
        if "CERTIFICATE_VERIFY_FAILED" in text or "SSL" in text.upper():
            text = "polymarket:ssl_error:ssl_certificate_verification_failed"
        elif len(text) > 180:
            text = text[:177] + "..."
        if text not in compacted:
            compacted.append(text)
    return compacted


def _compact_options(options: dict[str, Any]) -> dict[str, Any]:
    observed = options.get("observed_aggregates") or {}
    matrix = options.get("open_interest_matrix") or {}
    by_strike = matrix.get("by_strike") or []
    snapshot = options.get("snapshot") or {}
    spot = _float(snapshot.get("underlying_price") or snapshot.get("last_price"))
    concentrations = sorted(by_strike, key=lambda item: float(item.get("total_open_interest") or item.get("open_interest") or 0), reverse=True)[:10]
    return {
        "status": options.get("status"),
        "underlying": snapshot.get("underlying") or "QQQ",
        "source_timestamp": snapshot.get("retrieved_at") or options.get("retrieved_at"),
        "partial_snapshot": True,
        "partial": True,
        "incomplete": bool((options.get("warnings") or [])),
        "coverage_contract_pct": (options.get("diagnostics") or {}).get("coverage_contract_pct"),
        "covered_expirations": (options.get("diagnostics") or {}).get("covered_expirations"),
        "requested_expirations": (options.get("diagnostics") or {}).get("requested_expirations"),
        "provider_total_chain_records": (options.get("diagnostics") or {}).get("provider_total_chain_records"),
        "requested_scope_records": (options.get("diagnostics") or {}).get("requested_scope_records"),
        "observed_put_call_oi_ratio": observed.get("put_call_oi_ratio"),
        "observed_put_call_volume_ratio": observed.get("put_call_volume_ratio"),
        "observed_scope": "partial_provider_snapshot",
        "top_combined_oi_concentrations": [_option_concentration(item, spot) for item in concentrations],
        "warnings": options.get("warnings") or [],
    }


def _option_concentration(item: dict[str, Any], spot: float | None) -> dict[str, Any]:
    strike = _float(item.get("strike"))
    oi = item.get("total_open_interest") or item.get("open_interest") or item.get("call_open_interest") or item.get("put_open_interest")
    return {
        "strike": strike,
        "expiration_date": item.get("expiration_date"),
        "distance_from_spot_pct": round(((strike - spot) / spot) * 100, 4) if strike is not None and spot else None,
        "moneyness": "atm" if strike is not None and spot and abs((strike - spot) / spot) < 0.01 else ("otm_call_side" if strike and spot and strike > spot else "itm_call_side" if strike and spot else None),
        "open_interest": oi,
        "pct_observed_open_interest": item.get("pct_observed_open_interest"),
    }


def _compact_nasdaq_100(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": snapshot.get("status"),
        "retrieved_at": snapshot.get("retrieved_at"),
        "constituents_count": len(snapshot.get("constituents") or []),
        "diagnostics": snapshot.get("diagnostics") or {},
        "warnings": snapshot.get("warnings") or [],
    }


def _compact_sentiment_context(sentiment: dict[str, Any]) -> dict[str, Any]:
    output = dict(sentiment)
    prediction = output.get("prediction_markets")
    if isinstance(prediction, dict):
        errors = prediction.get("errors") or []
        warnings = prediction.get("warnings") or []
        combined = [str(item) for item in [*errors, *warnings] if item]
        failure_type = prediction.get("failure_type")
        if not failure_type and any("CERTIFICATE_VERIFY_FAILED" in item or "SSL" in item.upper() for item in combined):
            failure_type = "ssl_error"
        short_reason = _short_reason(combined, fallback=prediction.get("warning") or prediction.get("error"))
        output["prediction_markets"] = {
            "status": prediction.get("status"),
            "failure_type": failure_type,
            "attempt_count": int(prediction.get("attempt_count") or (prediction.get("diagnostics") or {}).get("attempt_count") or len((prediction.get("diagnostics") or {}).get("attempts") or []) or 0),
            "blocking": False,
            "retryable": prediction.get("retryable"),
            "next_retry_at": prediction.get("next_retry_at"),
            "short_reason": short_reason,
            "source": prediction.get("source"),
            "source_url": prediction.get("source_url"),
        }
    return output


def _short_reason(messages: list[str], *, fallback: Any = None) -> str | None:
    candidates = messages or ([str(fallback)] if fallback else [])
    if not candidates:
        return None
    unique = []
    for message in candidates:
        text = str(message)
        if "CERTIFICATE_VERIFY_FAILED" in text or "SSL" in text.upper():
            text = "ssl_certificate_verification_failed"
        elif len(text) > 160:
            text = text[:157] + "..."
        if text not in unique:
            unique.append(text)
    return "; ".join(unique[:3])


def _compact_market_schedule(schedule: dict[str, Any]) -> dict[str, Any]:
    return {
        "nasdaq_cash_session": schedule.get("nasdaq_cash_session") or {},
        "holidays": (schedule.get("holidays") or [])[:10],
        "holiday_source": schedule.get("holiday_source") or {},
    }


def _consumer_warnings(full: dict[str, Any]) -> list[dict[str, Any]]:
    warnings: list[str] = []
    for source in (full.get("data_quality") or {}, full.get("social_sentiment") or {}):
        warnings.extend(source.get("warnings") or [])
    warnings.extend(_event_optional_warnings(full))
    output: dict[str, dict[str, Any]] = {}
    for item in warnings:
        code = _warning_code(str(item))
        if not code:
            continue
        if code.startswith("optional_event_enrichment"):
            code = "optional_event_enrichment_partial"
        entry = output.setdefault(code, {"code": code, "count": 0, "blocking": False})
        entry["count"] += 1
    return sorted(output.values(), key=lambda item: item["code"])


def _compact_event_item(item: Any) -> Any:
    if not isinstance(item, dict):
        return item
    output = dict(item)
    enrichment = output.get("enrichment")
    if isinstance(enrichment, dict):
        warnings = [
            warning
            for warning in (enrichment.get("warnings") or [])
            if _warning_code(str(warning)) != "optional_event_enrichment_partial"
        ]
        output["enrichment"] = {**enrichment, "warnings": warnings}
    warnings = [
        warning
        for warning in (output.get("warnings") or [])
        if _warning_code(str(warning)) != "optional_event_enrichment_partial"
    ]
    if "warnings" in output:
        output["warnings"] = warnings
    return output


def _event_optional_warnings(full: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    calendar = full.get("event_calendar") or {}
    for section in ("critical_macro_events", "fed_communications", "other_economic_events"):
        for item in calendar.get(section) or []:
            if not isinstance(item, dict):
                continue
            warnings.extend(str(warning) for warning in item.get("warnings") or [])
            enrichment = item.get("enrichment") or {}
            if isinstance(enrichment, dict):
                warnings.extend(str(warning) for warning in enrichment.get("warnings") or [])
    return [warning for warning in warnings if _warning_code(warning) == "optional_event_enrichment_partial"]


def _warning_code(value: str) -> str:
    if not value:
        return ""
    if value == "optional_enrichment_timeout" or value.startswith("optional_event_enrichment_timeout_after_"):
        return "optional_event_enrichment_partial"
    return value.split(":", 1)[0]


def _snapshot_summary(full: dict[str, Any]) -> dict[str, Any]:
    data_quality = full.get("data_quality") or {}
    readiness = _readiness(data_quality)
    overall = data_quality.get("overall_data_quality") or {}
    event_calendar = full.get("event_calendar") or {}
    critical_events = event_calendar.get("critical_macro_events") or []
    all_events = critical_events + (event_calendar.get("fed_communications") or []) + (event_calendar.get("other_economic_events") or [])
    nasdaq = full.get("nasdaq_context") or {}
    earnings = nasdaq.get("earnings") or {}
    news_latest = (full.get("news_context") or {}).get("latest") or []
    statuses = _collect_statuses(full)
    return {
        "generated_at": full.get("generated_at_utc") or full.get("generated_at"),
        "symbol": full.get("symbol"),
        "ready": readiness["ready"],
        "critical_errors": readiness["critical_errors"],
        "provider_success_count": sum(1 for status in statuses if status == "found"),
        "provider_partial_count": sum(1 for status in statuses if status in {"partial", "stale_acceptable"}),
        "provider_failure_count": sum(1 for status in statuses if status in {"provider_failed", "ssl_error", "rate_limited", "access_restricted"}),
        "cache_used": _cache_used(full),
        "critical_event_count": len(critical_events),
        "high_impact_event_count_next_7d": sum(1 for event in all_events if str((event or {}).get("impact") or (event or {}).get("event_risk_level") or "").upper() == "HIGH"),
        "next_critical_event_at": _next_event_at(critical_events),
        "next_fomc_meeting_at": _next_fomc_at(all_events),
        "next_earnings_count_14d": len(earnings.get("events") or earnings.get("upcoming") or []),
        "news_article_count": len(news_latest),
        "social_sentiment_status": (full.get("social_sentiment") or {}).get("status"),
        "risk_context_status": _block_status(full.get("risk_context") or full.get("risk_sentiment") or {}),
        "market_status": (((full.get("market_schedule") or {}).get("nasdaq_cash_session") or {}).get("status")),
        "data_freshness_score": overall.get("freshness_score"),
        "data_reliability_score": overall.get("reliability_score"),
    }


def _collect_statuses(value: Any) -> list[str]:
    statuses: list[str] = []
    if isinstance(value, dict):
        status = value.get("status")
        if isinstance(status, str):
            statuses.append(status)
        for item in value.values():
            statuses.extend(_collect_statuses(item))
    elif isinstance(value, list):
        for item in value:
            statuses.extend(_collect_statuses(item))
    return statuses


def _cache_used(full: dict[str, Any]) -> bool:
    runtime = ((full.get("metadata") or {}).get("multi_source_runtime") or {})
    return bool(runtime.get("cache_used") or runtime.get("db_hits") or "false" == runtime.get("refresh_mode"))


def _next_event_at(events: list[Any]) -> str | None:
    candidates = []
    for event in events:
        if isinstance(event, dict):
            candidates.append(event.get("time_utc") or event.get("release_at") or event.get("date"))
    return sorted(str(item) for item in candidates if item)[:1][0] if any(candidates) else None


def _next_fomc_at(events: list[Any]) -> str | None:
    fomc = [
        event
        for event in events
        if isinstance(event, dict)
        and "FOMC" in " ".join(str(event.get(key) or "") for key in ("category", "name", "event_name")).upper()
    ]
    return _next_event_at(fomc)


def _block_status(block: dict[str, Any]) -> str | None:
    if block.get("status"):
        return block.get("status")
    statuses = [value.get("status") for value in block.values() if isinstance(value, dict) and value.get("status")]
    if any(status == "found" for status in statuses):
        return "found"
    return statuses[0] if statuses else None


def _empty_social() -> dict[str, Any]:
    return {"status": "not_found", "source_count": 0, "mention_count": 0, "warnings": ["social_sentiment_not_available"]}


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
