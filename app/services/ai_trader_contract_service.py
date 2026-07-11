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
        "service_role": "data provider only",
        "readiness": _readiness(data_quality),
        "data_quality": _compact_quality(data_quality),
        "macro_snapshot": full.get("macro_snapshot") or {},
        "event_calendar": _compact_events(full.get("event_calendar") or {}),
        "event_windows": full.get("event_windows") or {},
        "positioning": full.get("positioning") or {},
        "sentiment_context": full.get("sentiment_context") or {},
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
    return {
        "ready": len(explicit_errors) == 0,
        "critical_errors": len(explicit_errors),
        "blocking_reasons": overall.get("blocking_reasons") or [],
    }


def _compact_quality(data_quality: dict[str, Any]) -> dict[str, Any]:
    overall = data_quality.get("overall_data_quality") or {}
    return {
        "completeness_score": overall.get("completeness_score"),
        "freshness_score": overall.get("freshness_score"),
        "reliability_score": overall.get("reliability_score"),
        "critical_missing_count": overall.get("critical_missing_count"),
        "missing_critical_fields": data_quality.get("missing_critical_fields") or [],
        "section_quality": data_quality.get("section_quality") or {},
        "pipeline_integrity": data_quality.get("pipeline_integrity") or {},
        "multi_source_pipeline": data_quality.get("multi_source_pipeline") or {},
    }


def _compact_events(calendar: dict[str, Any]) -> dict[str, Any]:
    return {
        "critical_macro_events": (calendar.get("critical_macro_events") or [])[:12],
        "fed_communications": (calendar.get("fed_communications") or [])[:8],
        "other_economic_events": (calendar.get("other_economic_events") or [])[:12],
    }


def _compact_nasdaq(nasdaq: dict[str, Any]) -> dict[str, Any]:
    output = {
        "qqq_holdings": nasdaq.get("qqq_holdings") or {},
        "sector_exposure": nasdaq.get("sector_exposure") or {},
        "mega_cap_snapshot": _compact_snapshot(nasdaq.get("mega_cap_snapshot") or {}),
        "mega_cap_breadth": nasdaq.get("mega_cap_breadth") or {},
        "concentration": nasdaq.get("concentration") or {},
        "semiconductor_context": nasdaq.get("semiconductor_context") or {},
        "earnings": nasdaq.get("earnings") or {},
        "qqq_options": _compact_options(((nasdaq.get("nasdaq_context_additions") or {}).get("qqq_options") or nasdaq.get("qqq_options") or {})),
        "nasdaq_100_official_snapshot": _compact_nasdaq_100(nasdaq.get("nasdaq_100_official_snapshot") or {}),
    }
    if isinstance(output["qqq_holdings"], dict) and output["qqq_holdings"].get("top_holdings"):
        output["qqq_holdings"] = {**output["qqq_holdings"], "top_holdings": output["qqq_holdings"]["top_holdings"][:15]}
    return output


def _compact_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {**snapshot, "stocks": (snapshot.get("stocks") or [])[:20]}


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


def _compact_market_schedule(schedule: dict[str, Any]) -> dict[str, Any]:
    return {
        "nasdaq_cash_session": schedule.get("nasdaq_cash_session") or {},
        "holidays": (schedule.get("holidays") or [])[:10],
        "holiday_source": schedule.get("holiday_source") or {},
    }


def _consumer_warnings(full: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    for source in (full.get("data_quality") or {}, full.get("social_sentiment") or {}):
        warnings.extend(source.get("warnings") or [])
    ignored_prefixes = ("optional_event_enrichment_timeout_after_",)
    return sorted(
        set(
            str(item)
            for item in warnings
            if item and not any(str(item).startswith(prefix) for prefix in ignored_prefixes)
        )
    )


def _empty_social() -> dict[str, Any]:
    return {"status": "not_found", "source_count": 0, "mention_count": 0, "warnings": ["social_sentiment_not_available"]}


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
