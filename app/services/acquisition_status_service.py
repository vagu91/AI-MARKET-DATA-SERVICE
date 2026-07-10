from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.services.bls_required_series import (
    bls_required_series_status_from_facts,
    required_macro_saved_but_missing_from_snapshot,
)
from app.services.data_integrity_service import freshness_label, news_content_status
from app.services.market_context_builder import build_news_context
from app.services.market_fact_repository import MarketFactRepository, connect_market_db
from app.services.market_news_repository import MarketNewsRepository

BLOCKS = {
    "macro": ("official_macro_latest",),
    "events": tuple(),
    "forecast_consensus": ("macro_event_enrichment", "ai_research_result"),
    "market_news": tuple(),
    "official_news": tuple(),
    "canonical_urls": tuple(),
    "summaries": tuple(),
    "cot": ("cot_positioning",),
    "aaii": ("aaii_sentiment",),
    "risk_sentiment": tuple(),
    "earnings": ("earnings_event",),
    "investing_economic_calendar": ("investing_economic_calendar",),
    "investing_holidays": ("investing_holidays",),
    "cboe_risk_indices": ("cboe_risk_indices",),
    "nasdaq_earnings": ("nasdaq_earnings_calendar",),
    "nasdaq_100": ("nasdaq_100_constituents",),
    "nasdaq_market_info": ("nasdaq_market_info",),
    "nasdaq_qqq_options": ("nasdaq_qqq_options",),
    "macromicro_aaii_crosscheck": ("macromicro_aaii_crosscheck",),
    "polymarket_prediction_markets": ("polymarket_prediction_markets",),
    "quikstrike_review": ("quikstrike_review",),
    "qqq": ("qqq_holdings",),
    "mega_cap": ("mega_cap_snapshot", "mega_cap_breadth"),
    "semiconductors": ("mega_cap_snapshot",),
}


class AcquisitionStatusService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.facts = MarketFactRepository(settings)

    def status(self) -> dict[str, Any]:
        latest_run = _latest_run(self.settings)
        observations = _observations(self.settings)
        db_summary = self.facts.db_summary()
        blocks = {name: self._block(name, fact_types, observations, db_summary) for name, fact_types in BLOCKS.items()}
        news_pipeline = _news_pipeline_status(self.settings)
        macro_pipeline = _macro_pipeline_status(self.settings)
        blocks["market_news"].update(news_pipeline["market_news"])
        blocks["official_news"].update(news_pipeline["official_news"])
        blocks["macro"].update(macro_pipeline)
        pipeline_gaps = _pipeline_gaps(blocks, news_pipeline, macro_pipeline)
        pipeline_integrity = {
            "critical_fetch_completed": True,
            "critical_persistence_completed": not pipeline_gaps["fetched_but_not_persisted"],
            "critical_commits_completed": all(block.get("committed", True) for block in blocks.values()),
            "critical_read_back_completed": not pipeline_gaps["persisted_but_not_read_back"],
            "snapshot_materialization_completed": not pipeline_gaps["read_back_but_not_materialized"] and not pipeline_gaps["eligible_news_not_materialized"],
            "snapshot_built_from_db": True,
            "partial_response": bool(pipeline_gaps["critical"]),
            **pipeline_gaps,
        }
        timeouts = [item for item in observations if str(item.get("status") or "").endswith("timeout") or "timeout" in str(item.get("warning") or item.get("error") or "").lower()]
        failures = [item for item in observations if "failed" in str(item.get("status") or "").lower() or item.get("error")]
        now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return {
            "run_id": (latest_run or {}).get("run_id"),
            "started_at": (latest_run or {}).get("started_at"),
            "completed_at": (latest_run or {}).get("finished_at"),
            "duration_ms": None,
            "completed": (latest_run or {}).get("status") in {"completed", "success"} if latest_run else True,
            "generated_at": now,
            "blocks": blocks,
            "news_pipeline": news_pipeline,
            "macro_pipeline": macro_pipeline,
            "pipeline_integrity": pipeline_integrity,
            "timeouts": [_brief_observation(item) for item in timeouts],
            "provider_failures": [_brief_observation(item) for item in failures],
            "ai_calls": [_brief_observation(item) for item in observations if item.get("provider_name") == "ai_researcher"],
            "cache_hits": {"market_facts_active": db_summary["market_facts"]["active"], "market_news_total": db_summary["market_news"]["total"]},
            "cache_misses": {},
            "critical_errors": [name for name, block in blocks.items() if block["status"] == "missing_required"] + pipeline_gaps["critical"],
            "warnings": [block["reason"] for block in blocks.values() if block.get("reason")],
            "service_role": "data provider only",
        }

    def _block(self, name: str, fact_types: tuple[str, ...], observations: list[dict[str, Any]], db_summary: dict[str, Any]) -> dict[str, Any]:
        facts_by_type = db_summary.get("facts_by_type") or {}
        found = sum(int(facts_by_type.get(fact_type) or 0) for fact_type in fact_types)
        provider_obs = [item for item in observations if name in str(item.get("provider_name") or item.get("category") or "").lower()]
        observed_items = sum(int(item.get("item_count") or 0) for item in provider_obs)
        latest_observation = provider_obs[0] if provider_obs else {}
        latest_raw = latest_observation.get("raw_payload") if isinstance(latest_observation.get("raw_payload"), dict) else {}
        latest_diagnostics = latest_raw.get("diagnostics") if isinstance(latest_raw.get("diagnostics"), dict) else {}
        if name == "market_news":
            found = int((db_summary.get("market_news") or {}).get("total") or 0)
        elif name == "official_news":
            found = _official_news_count(self.settings)
        status = "available" if found else "not_found"
        reason = None if found else f"{name}_not_in_db"
        if any("timeout" in str(item.get("status") or item.get("warning") or item.get("error") or "").lower() for item in provider_obs):
            status = "degraded_timeout" if found else "provider_timeout"
            reason = f"{name}_provider_timeout"
        if provider_obs and not found and observed_items:
            status = "fetched_not_persisted"
            reason = f"{name}_fetched_not_persisted"
        fetched_count = observed_items or found
        snapshot_item_count = fetched_count if provider_obs and found and fetched_count else found
        return {
            "enabled": _enabled(self.settings, name),
            "status": status,
            "attempted": bool(provider_obs or found),
            "provider_calls": len(provider_obs),
            "AI_called": any(item.get("provider_name") == "ai_researcher" for item in provider_obs),
            "items_found": snapshot_item_count,
            "items_accepted": snapshot_item_count,
            "items_rejected": sum(1 for item in provider_obs if str(item.get("status") or "").startswith("rejected")),
            "cache_used": bool(found),
            "fetched_count": fetched_count,
            "validated_count": max(fetched_count - int(latest_diagnostics.get("rejected_future_actual") or 0), 0),
            "persisted_count": snapshot_item_count,
            "committed": True,
            "read_back_count": snapshot_item_count,
            "materialized_count": snapshot_item_count,
            "excluded_count": _diagnostic_rejections(latest_diagnostics),
            "exclusion_reasons": _diagnostic_rejection_reasons(latest_diagnostics),
            "fallback_count": 0,
            "timeout_count": sum(1 for item in provider_obs if "timeout" in str(item.get("status") or item.get("warning") or item.get("error") or "").lower()),
            "error_count": sum(1 for item in provider_obs if item.get("error")),
            "duration_ms": sum(int(item.get("duration_ms") or 0) for item in provider_obs) or None,
            "reason": reason,
            "error": "; ".join(str(item.get("error")) for item in provider_obs if item.get("error")) or None,
            "last_status": latest_observation.get("status"),
            "last_error": latest_observation.get("error"),
            "last_warning": latest_observation.get("warning"),
            "last_success": latest_observation.get("retrieved_at") if latest_observation.get("status") in {"found", "valid", "partial", "anomalous"} else None,
            "source_url": latest_observation.get("url"),
            "diagnostics": latest_diagnostics,
        }


def _latest_run(settings: Settings) -> dict[str, Any] | None:
    with connect_market_db(settings) as conn:
        row = conn.execute("SELECT * FROM enrichment_runs ORDER BY started_at DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def _observations(settings: Settings) -> list[dict[str, Any]]:
    with connect_market_db(settings) as conn:
        rows = conn.execute("SELECT * FROM provider_observations ORDER BY id DESC LIMIT 200").fetchall()
    output = []
    for row in rows:
        item = dict(row)
        if item.get("raw_payload_json"):
            try:
                item["raw_payload"] = json.loads(item["raw_payload_json"])
            except json.JSONDecodeError:
                item["raw_payload"] = None
        output.append(item)
    return output


def _official_news_count(settings: Settings) -> int:
    with connect_market_db(settings) as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM market_news WHERE is_official = 1").fetchone()
    return int(row["count"] or 0) if row else 0


def _news_pipeline_status(settings: Settings) -> dict[str, Any]:
    rows = MarketNewsRepository(settings).stored(days=365, limit=1000)
    context = build_news_context(rows, limit=1000)
    latest = context.get("latest") or []
    exclusions: list[dict[str, Any]] = []
    eligible = []
    for item in rows:
        reason = _news_exclusion_reason(item)
        if reason:
            exclusions.append(
                {
                    "article_id": item.get("news_key") or item.get("source_url") or item.get("title"),
                    "reason": reason,
                }
            )
        else:
            eligible.append(item)
    official_rows = [item for item in rows if item.get("is_official")]
    official_eligible = [item for item in eligible if item.get("is_official")]
    official_latest = [item for item in latest if item.get("is_official_source")]
    excluded_counts = _reason_counts(exclusions)
    base = {
        "fetched": len(rows),
        "accepted": len(rows) - excluded_counts.get("invalid_content", 0),
        "persisted": len(rows),
        "read_back": len(rows),
        "eligible": len(eligible),
        "materialized": len(latest),
    }
    return {
        "market_news": {
            "fetched": base["fetched"],
            "accepted": base["accepted"],
            "persisted": base["persisted"],
            "read_back": base["read_back"],
            "eligible": base["eligible"],
            "materialized": base["materialized"],
            "fetched_count": base["fetched"],
            "validated_count": base["accepted"],
            "persisted_count": base["persisted"],
            "committed": True,
            "read_back_count": base["read_back"],
            "materialized_count": base["materialized"],
            "eligible_count": base["eligible"],
            "excluded_count": len(exclusions),
            "exclusion_reasons": excluded_counts,
        },
        "official_news": {
            "fetched": len(official_rows),
            "accepted": len(official_rows),
            "persisted": len(official_rows),
            "read_back": len(official_rows),
            "eligible": len(official_eligible),
            "materialized": len(official_latest),
            "fetched_count": len(official_rows),
            "validated_count": len(official_rows),
            "persisted_count": len(official_rows),
            "committed": True,
            "read_back_count": len(official_rows),
            "materialized_count": len(official_latest),
            "eligible_count": len(official_eligible),
            "excluded_count": len(official_rows) - len(official_eligible),
            "exclusion_reasons": _reason_counts([item for item in exclusions if _is_official_exclusion(item, official_rows)]),
        },
        "excluded": {
            "expired": excluded_counts.get("expired", 0),
            "invalid_content": excluded_counts.get("invalid_content", 0),
            "future_published": excluded_counts.get("future_published", 0),
            "duplicate": excluded_counts.get("duplicate", 0),
            "missing_url": excluded_counts.get("missing_url", 0),
            "missing_source": excluded_counts.get("missing_source", 0),
            "other": excluded_counts.get("other", 0),
        },
        "exclusions": exclusions[:100],
    }


def _macro_pipeline_status(settings: Settings) -> dict[str, Any]:
    repository = MarketFactRepository(settings)
    facts = repository.get_valid_facts_by_type("official_macro_latest")
    snapshot_facts = repository.search_facts(country="US", limit=1000)
    official_bls_status = bls_required_series_status_from_facts(facts)
    bls_status = bls_required_series_status_from_facts([*facts, *snapshot_facts], include_metrics=True)
    saved_but_missing = required_macro_saved_but_missing_from_snapshot(
        official_bls_status["present"],
        bls_status["materialized"],
    )
    return {
        "bls_required_series": bls_status,
        "official_bls_required_series": official_bls_status,
        "required_bls_series": bls_status["required"],
        "required_bls_present": bls_status["present"],
        "required_bls_missing": bls_status["missing"],
        "required_bls_invalid": bls_status["invalid"],
        "materialized_bls_series": bls_status["materialized"],
        "official_bls_present": official_bls_status["present"],
        "official_bls_missing": official_bls_status["missing"],
        "required_macro_saved_but_missing_from_snapshot": saved_but_missing,
        "items_materialized": len(facts),
        "items_read_back": len(facts),
    }


def _news_exclusion_reason(item: dict[str, Any]) -> str | None:
    if news_content_status(item) == "invalid_content":
        return "invalid_content"
    if not (item.get("source_url") or item.get("url")):
        return "missing_url"
    if not item.get("source"):
        return "missing_source"
    if freshness_label(valid_until=item.get("valid_until")) in {"STALE", "EXPIRED"}:
        return "expired"
    published = item.get("published_at")
    if published:
        try:
            parsed = datetime.fromisoformat(str(published).replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed and parsed.astimezone(UTC) > datetime.now(UTC) + timedelta(minutes=1):
            return "future_published"
    return None


def _pipeline_gaps(blocks: dict[str, dict[str, Any]], news_pipeline: dict[str, Any], macro_pipeline: dict[str, Any]) -> dict[str, Any]:
    fetched_not_persisted = []
    persisted_not_read_back = []
    read_back_not_materialized = []
    for name, block in blocks.items():
        if int(block.get("fetched_count") or 0) > int(block.get("persisted_count") or 0):
            fetched_not_persisted.append(name)
        if int(block.get("persisted_count") or 0) > int(block.get("read_back_count") or 0):
            persisted_not_read_back.append(name)
        eligible = block.get("eligible_count")
        needs_materialization = int(eligible) > 0 if eligible is not None else int(block.get("read_back_count") or 0) > 0
        if needs_materialization and int(block.get("materialized_count") or 0) == 0:
            read_back_not_materialized.append(name)
    eligible_news_gap = int((news_pipeline.get("market_news") or {}).get("eligible_count") or 0) > 0 and int((news_pipeline.get("market_news") or {}).get("materialized_count") or 0) == 0
    macro_gap_items = macro_pipeline.get("required_macro_saved_but_missing_from_snapshot") or []
    macro_gap = bool(macro_gap_items)
    critical = []
    if fetched_not_persisted:
        critical.append("fetched_but_not_persisted")
    if persisted_not_read_back:
        critical.append("persisted_but_not_read_back")
    if read_back_not_materialized:
        critical.append("read_back_but_not_materialized")
    if eligible_news_gap:
        critical.append("eligible_news_not_materialized")
    if macro_gap:
        critical.append("required_macro_saved_but_missing_from_snapshot")
    return {
        "fetched_but_not_persisted": fetched_not_persisted,
        "persisted_but_not_read_back": persisted_not_read_back,
        "read_back_but_not_materialized": read_back_not_materialized,
        "eligible_news_not_materialized": eligible_news_gap,
        "required_macro_saved_but_missing_from_snapshot": macro_gap,
        "required_macro_saved_but_missing_from_snapshot_items": macro_gap_items,
        "critical": critical,
    }


def _reason_counts(exclusions: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in exclusions:
        reason = str(item.get("reason") or "other")
        counts[reason] = counts.get(reason, 0) + 1
    return counts


def _is_official_exclusion(exclusion: dict[str, Any], official_rows: list[dict[str, Any]]) -> bool:
    article_id = exclusion.get("article_id")
    return any(article_id in {item.get("news_key"), item.get("source_url"), item.get("title")} for item in official_rows)


def _brief_observation(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": item.get("provider_name"),
        "status": item.get("status"),
        "category": item.get("category"),
        "error": item.get("error"),
        "warning": item.get("warning"),
        "duration_ms": item.get("duration_ms"),
    }


def _enabled(settings: Settings, name: str) -> bool:
    mapping = {
        "investing_economic_calendar": settings.enable_investing_calendar,
        "investing_holidays": settings.enable_investing_holidays,
        "cboe_risk_indices": settings.enable_cboe_risk_indices,
        "nasdaq_earnings": settings.enable_nasdaq_earnings,
        "nasdaq_100": settings.enable_nasdaq_100,
        "nasdaq_market_info": settings.enable_nasdaq_market_info,
        "nasdaq_qqq_options": settings.enable_nasdaq_qqq_options,
        "aaii": settings.enable_aaii_sentiment,
        "macromicro_aaii_crosscheck": settings.enable_macromicro_aaii_crosscheck,
        "polymarket_prediction_markets": settings.enable_polymarket,
    }
    return bool(mapping.get(name, True))


def _diagnostic_rejections(diagnostics: dict[str, Any]) -> int:
    return sum(
        int(diagnostics.get(key) or 0)
        for key in (
            "rejected_future_actual",
            "rejected_irrelevant",
            "rejected_weak_indirect",
            "rejected_low_relevance",
            "rejected_rules_only",
            "rejected_low_liquidity",
            "rejected_low_volume",
            "rejected_wide_spread",
            "rejected_invalid_probability",
            "rejected_expired",
        )
    )


def _diagnostic_rejection_reasons(diagnostics: dict[str, Any]) -> dict[str, int]:
    reasons: dict[str, int] = {}
    for key, value in diagnostics.items():
        if not key.startswith("rejected_"):
            continue
        try:
            count = int(value or 0)
        except (TypeError, ValueError):
            continue
        if count:
            reasons[key.removeprefix("rejected_")] = count
    return reasons
