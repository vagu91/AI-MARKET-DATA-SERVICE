from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.services.bls_required_series import (
    BLS_CANONICAL_NAME_BY_ID,
    BLS_REQUIRED_SERIES,
    BLS_REQUIRED_SERIES_IDS,
    bls_required_series_status_from_macro_snapshot,
    normalize_bls_series_id,
)
from app.services.data_freshness_service import parse_datetime
from app.services.data_integrity_service import classify_source, news_content_status


REPORT_VERSION = "1.0"
SERVICE_ROLE = "data provider only"
DEFAULT_RETRY_SECONDS = [30, 120, 300, 900, 1800, 3600]
PLACEHOLDER_TOKENS = ("META_TITLE_QUOTE", "PLACEHOLDER", "TEMPLATE_TITLE", "TODO", "UNKNOWN_TITLE")
MOJIBAKE_TOKENS = ("Ã¢", "Ãƒ", "ï¿½", "â", "\ufffd")
MARKET_PUBLISHERS = ("MARKETBEAT", "SEEKING ALPHA", "YAHOO FINANCE", "BARRON", "MARKETWATCH", "REUTERS", "CNBC", "BLOOMBERG", "WSJ")
REQUIRED_FRED = ("VIXCLS", "DGS2", "DGS10", "FEDFUNDS", "NFCI", "SOFR", "T10Y2Y")
REQUIRED_BEA = ("BEA:GDP", "BEA:REAL_GDP", "BEA:CORE_PCE", "BEA:PCE", "BEA:PERSONAL_INCOME", "BEA:PERSONAL_SPENDING")
REQUIRED_BLS = tuple(BLS_REQUIRED_SERIES.keys())


class HealthReportService:
    def build_report(
        self,
        *,
        base_url: str,
        refresh_mode: str = "false",
        service_status: str = "ok",
        db_health: dict[str, Any] | None = None,
        market_context: dict[str, Any] | None = None,
        temporal_integrity: dict[str, Any] | None = None,
        release_refresh: dict[str, Any] | None = None,
        news_freshness: dict[str, Any] | None = None,
        source_classification: dict[str, Any] | None = None,
        db_summary: dict[str, Any] | None = None,
        ai_researcher_enabled: bool | None = None,
        ai_researcher_mode: str | None = None,
        fail_on_warning: bool = False,
    ) -> dict[str, Any]:
        market_context = market_context or {}
        temporal_integrity = temporal_integrity or {}
        release_refresh = release_refresh or {}
        news_freshness = news_freshness or {}
        source_classification = source_classification or {}
        db_health = db_health or {}
        db_summary = db_summary or market_context.get("db_summary") or {}

        quality = _overall_quality(market_context)
        data_quality = market_context.get("data_quality") or {}
        model_counts = _model_counts(market_context, db_summary)
        temporal = _temporal_summary(temporal_integrity)
        release = _release_summary(release_refresh)
        latest_news = _latest_news(market_context)
        news = _news_summary(news_freshness, latest_news)
        sources = _source_summary(market_context, source_classification)
        nasdaq = _nasdaq_summary(market_context)
        providers = _provider_summary(market_context)
        required = _required_series(market_context)
        event_enrichment_health = _event_enrichment_health(market_context)
        positioning_health = _positioning_health(market_context)
        news_enrichment_health = _news_enrichment_health(market_context, sources)
        nasdaq_enrichment_health = _nasdaq_enrichment_health(market_context)
        ai_research_health = _ai_research_health(market_context)
        no_action_logic = _report_has_no_action_logic(
            {
                "quality_scores": quality,
                "model_counts": model_counts,
                "temporal_integrity": temporal,
                "release_refresh": release,
                "news_health": news,
                "source_health": sources,
                "nasdaq_health": nasdaq,
                "provider_health": providers,
            }
        )

        checks = _build_checks(
            service_status=service_status,
            db_health=db_health,
            service_role=market_context.get("service_role"),
            quality=quality,
            data_quality=data_quality,
            model_counts=model_counts,
            temporal=temporal,
            release=release,
            news=news,
            sources=sources,
            nasdaq=nasdaq,
            providers=providers,
            required=required,
            event_enrichment_health=event_enrichment_health,
            positioning_health=positioning_health,
            news_enrichment_health=news_enrichment_health,
            nasdaq_enrichment_health=nasdaq_enrichment_health,
            latest_news=latest_news,
            market_context=market_context,
            refresh_mode=refresh_mode,
            no_action_logic=no_action_logic,
        )
        critical_errors = [check["message"] for check in checks if check["severity"] == "CRITICAL" and check["status"] == "FAIL"]
        warnings = [check["message"] for check in checks if check["severity"] == "WARNING" and check["status"] == "WARNING"]
        infos = [check["message"] for check in checks if check["severity"] == "INFO" and check["status"] == "INFO"]
        status = "FAIL" if critical_errors else ("WARNING" if warnings else "PASS")
        exit_code = 2 if status == "FAIL" or (status == "WARNING" and fail_on_warning) else (1 if status == "WARNING" else 0)

        return {
            "report_version": REPORT_VERSION,
            "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "service": {
                "base_url": base_url.rstrip("/"),
                "status": service_status,
                "service_role": SERVICE_ROLE,
                "ai_researcher_enabled": ai_researcher_enabled,
                "ai_researcher_mode": ai_researcher_mode,
            },
            "status": status,
            "exit_code": exit_code,
            "summary": {
                "critical_error_count": len(critical_errors),
                "warning_count": len(warnings),
                "info_count": len(infos),
                "is_ready_for_market_analysis": bool(quality.get("is_ready_for_market_analysis")),
            },
            "quality_scores": {
                "completeness_score": quality.get("completeness_score", 0.0),
                "freshness_score": quality.get("freshness_score", 0.0),
                "reliability_score": quality.get("reliability_score", 0.0),
                "temporal_consistency_score": quality.get("temporal_consistency_score", 0.0),
                "source_integrity_score": quality.get("source_integrity_score", 0.0),
            },
            "model_counts": model_counts,
            "temporal_integrity": temporal,
            "release_refresh": release,
            "news_health": news,
            "source_health": sources,
            "nasdaq_health": nasdaq,
            "provider_health": providers,
            "event_enrichment_health": event_enrichment_health,
            "positioning_health": positioning_health,
            "news_enrichment_health": news_enrichment_health,
            "nasdaq_enrichment_health": nasdaq_enrichment_health,
            "ai_research_health": ai_research_health,
            "macro_required_series": required,
            "checks": checks,
            "critical_errors": critical_errors,
            "warnings": warnings,
            "infos": infos,
            "files": {
                "current_report": None,
                "latest_report": None,
                "history_report": None,
            },
        }

    def write_report(
        self,
        report: dict[str, Any],
        *,
        output_directory: str | Path,
        no_history: bool = False,
        retention_days: int | None = None,
    ) -> dict[str, Any]:
        output = Path(output_directory)
        output.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        latest = output / "latest.json"
        history = None if no_history else output / f"{stamp}.json"
        report["files"] = {
            "current_report": str(history or latest),
            "latest_report": str(latest),
            "history_report": None if history is None else str(history),
        }
        if history is not None:
            _atomic_json_write(history, report)
        _atomic_json_write(latest, report)
        self.cleanup(output, retention_days=retention_days)
        return report

    def cleanup(self, output_directory: str | Path, *, retention_days: int | None = None) -> None:
        retention = retention_days
        if retention is None:
            try:
                retention = int(os.getenv("AI_MARKET_HEALTH_REPORT_RETENTION_DAYS", "30"))
            except ValueError:
                retention = 30
        if retention <= 0:
            return
        cutoff = datetime.now() - timedelta(days=retention)
        for path in Path(output_directory).glob("*.json"):
            if path.name == "latest.json":
                continue
            try:
                if datetime.fromtimestamp(path.stat().st_mtime) < cutoff:
                    path.unlink()
            except OSError:
                continue


def _build_checks(
    *,
    service_status: str,
    db_health: dict[str, Any],
    service_role: Any,
    quality: dict[str, Any],
    data_quality: dict[str, Any],
    model_counts: dict[str, int],
    temporal: dict[str, int],
    release: dict[str, Any],
    news: dict[str, int],
    sources: dict[str, Any],
    nasdaq: dict[str, Any],
    providers: dict[str, Any],
    required: dict[str, Any],
    event_enrichment_health: dict[str, Any],
    positioning_health: dict[str, Any],
    news_enrichment_health: dict[str, Any],
    nasdaq_enrichment_health: dict[str, Any],
    latest_news: list[dict[str, Any]],
    market_context: dict[str, Any],
    refresh_mode: str,
    no_action_logic: bool,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(check_id: str, severity: str, failed: bool, message: str, actual: Any = None, threshold: Any = None) -> None:
        checks.append(
            {
                "id": check_id,
                "severity": severity,
                "status": "FAIL" if severity == "CRITICAL" and failed else ("WARNING" if severity == "WARNING" and failed else ("INFO" if severity == "INFO" and failed else "PASS")),
                "message": message,
                "actual_value": actual,
                "threshold": threshold,
            }
        )

    db_ok = str(db_health.get("status") or "").lower() == "ok"
    ready = bool(quality.get("is_ready_for_market_analysis"))
    pipeline = data_quality.get("pipeline_integrity") or {}
    news_pipeline = data_quality.get("news_pipeline") or {}
    macro_pipeline = data_quality.get("macro_pipeline") or {}
    news_context = market_context.get("news_context") or {}
    news_status = str(news_context.get("status") or "").upper()
    no_fresh_news_expected = news_status == "MARKET_CLOSED_NO_FRESH_NEWS"
    news_claims_available = news_status in {"AVAILABLE", "PARTIAL", "LAST_KNOWN_GOOD"}
    runtime_io = (market_context.get("metadata") or {}).get("runtime_io") or {}
    bls_status = _bls_status_from_macro_pipeline(macro_pipeline)
    bls_actual = {"missing": bls_status["missing"], "invalid": bls_status["invalid"]}
    add("SERVICE_REACHABLE", "CRITICAL", service_status != "ok", "Service must be reachable.", service_status, "ok")
    add("DB_HEALTH_OK", "CRITICAL", not db_ok, "Database health must be ok.", db_health.get("status"), "ok")
    add("SERVICE_ROLE_DATA_ONLY", "CRITICAL", str(service_role or "") != SERVICE_ROLE, "Service role must remain data provider only.", service_role, SERVICE_ROLE)
    add("READY_FOR_MARKET_ANALYSIS", "CRITICAL", not ready, "Market context must be ready for analysis.", ready, True)

    add("QUALITY_COMPLETENESS", "WARNING", float(quality.get("completeness_score") or 0) < 0.95, "Completeness score below warning threshold.", quality.get("completeness_score"), 0.95)
    add("QUALITY_FRESHNESS", "WARNING", float(quality.get("freshness_score") or 0) < 0.85, "Freshness score below warning threshold.", quality.get("freshness_score"), 0.85)
    add("QUALITY_RELIABILITY", "WARNING", float(quality.get("reliability_score") or 0) < 0.85, "Reliability score below warning threshold.", quality.get("reliability_score"), 0.85)
    add("QUALITY_TEMPORAL_CONSISTENCY", "CRITICAL", float(quality.get("temporal_consistency_score") or 0) < 1.0, "Temporal consistency score must be 1.0.", quality.get("temporal_consistency_score"), 1.0)
    add("QUALITY_SOURCE_INTEGRITY", "CRITICAL", float(quality.get("source_integrity_score") or 0) < 1.0, "Source integrity score must be 1.0.", quality.get("source_integrity_score"), 1.0)

    add("TEMPORAL_FUTURE_ACTUAL", "CRITICAL", temporal["future_actual_count"] > 0 or int(quality.get("invalid_future_actual_count") or 0) > 0, "No future actual values are allowed.", temporal["future_actual_count"], 0)
    add("TEMPORAL_STALE_AS_RECENT", "CRITICAL", temporal["stale_as_recent_count"] > 0 or int(quality.get("stale_as_recent_count") or 0) > 0, "Stale data must not be labeled recent.", temporal["stale_as_recent_count"], 0)
    awaiting_overdue = any(int(item.get("attempt_count") or 0) >= int(release.get("max_attempts") or 0) for item in release.get("awaiting_actual_items", []))
    add("TEMPORAL_RELEASED_WITHOUT_ACTUAL", "CRITICAL", awaiting_overdue, "No post-release event may exceed retry policy without actual.", release["awaiting_actual_count"], 0)
    add("TEMPORAL_INVALID_PERIOD_MAPPING", "CRITICAL", temporal["invalid_period_mapping_count"] > 0, "Invalid period mappings must be zero.", temporal["invalid_period_mapping_count"], 0)
    add("TEMPORAL_DUPLICATES", "CRITICAL", temporal["duplicates_count"] > 0, "Duplicate event count must be zero.", temporal["duplicates_count"], 0)
    add("TEMPORAL_BLOCKING_ERRORS", "CRITICAL", temporal["blocking_errors_count"] > 0, "Temporal blocking errors must be zero.", temporal["blocking_errors_count"], 0)
    add("TEMPORAL_AWAITING_WITHIN_RETRY", "WARNING", release["awaiting_actual_count"] > 0 and not awaiting_overdue, "Awaiting actual values remain within retry policy.", release["awaiting_actual_count"], 0)

    add("RELEASE_QUEUE_FRED_EMPTY", "CRITICAL", release["fred_in_release_queue"] > 0, "FRED series must not be in release retry queue.", release["fred_in_release_queue"], 0)
    add("RELEASE_QUEUE_RETRY_CONFIGURED", "CRITICAL", not release["retry_seconds"], "Release retry schedule must be configured.", release["retry_seconds"], DEFAULT_RETRY_SECONDS)
    add("RELEASE_QUEUE_MAX_ATTEMPTS_VALID", "CRITICAL", int(release.get("max_attempts") or 0) <= 0, "Max release refresh attempts must be positive.", release.get("max_attempts"), ">0")

    add("NEWS_LATEST_NOT_EMPTY", "CRITICAL", news_claims_available and model_counts["latest_news"] == 0, "News status AVAILABLE/PARTIAL/LAST_KNOWN_GOOD requires current articles.", model_counts["latest_news"], ">0 when status claims availability")
    add("NEWS_MARKET_CLOSED_NO_FRESH_EXPECTED", "INFO", no_fresh_news_expected, "No current-date news is expected for the closed market session.", news_status, "MARKET_CLOSED_NO_FRESH_NEWS")
    add("NEWS_EXPIRED_NOT_IN_LATEST", "CRITICAL", news["expired_in_latest_count"] > 0, "Expired news must not appear in latest.", news["expired_in_latest_count"], 0)
    add("NEWS_PLACEHOLDER_EMPTY", "CRITICAL", news["placeholder_news_count"] > 0, "Placeholder news titles must not appear in latest.", news["placeholder_news_count"], 0)
    add("NEWS_INVALID_CONTENT_TRACKED", "INFO", news["invalid_content_count"] > 0, "Invalid news content was tracked in history.", news["invalid_content_count"], 0)
    add("NEWS_STALE_AS_RECENT_ZERO", "CRITICAL", news["stale_as_recent_count"] > 0, "News stale-as-recent count must be zero.", news["stale_as_recent_count"], 0)
    add("NEWS_MOJIBAKE_ZERO", "WARNING", news["mojibake_count"] > 0, "News latest contains mojibake.", news["mojibake_count"], 0)
    add("NEWS_SUMMARY_COVERAGE", "WARNING", _missing_summary_ratio(latest_news) > 0.5, "More than half of latest news have null summaries.", round(_missing_summary_ratio(latest_news), 3), 0.5)
    add("NEWS_OFFICIAL_PRESENT", "WARNING", sources["official_news_count"] == 0 and model_counts["latest_news"] > 0, "No official news source in current batch.", sources["official_news_count"], ">0")
    add("NEWS_COLD_START_COMPLETE", "CRITICAL", int(news_pipeline.get("eligible_count") or 0) > 0 and int(news_pipeline.get("read_back_count") or 0) > 0 and int(news_pipeline.get("materialized_count") or 0) == 0, "Current-date eligible news saved in DB must materialize into latest context.", news_pipeline.get("materialized_count"), ">0 when eligible_count>0")
    add("NEWS_ELIGIBLE_MATERIALIZED", "CRITICAL", int(news_pipeline.get("eligible_count") or 0) > 0 and int(news_pipeline.get("materialized_count") or 0) == 0, "Eligible news must be materialized.", news_pipeline.get("materialized_count"), ">0")

    add("SOURCES_OFFICIAL_CLASSIFICATION", "INFO", sources["official_news_count"] == 0 and sources["misclassified_source_count"] == 0 and model_counts["latest_news"] > 0, "No official news source in current batch; source classification is otherwise coherent.", sources["official_news_count"], "classification coherent")
    add("SOURCES_MARKET_CLASSIFICATION", "WARNING", sources["market_news_count"] == 0 and model_counts["latest_news"] > 0, "Market news classification count is zero.", sources["market_news_count"], ">0")
    add("SOURCES_NO_MISCLASSIFICATION", "CRITICAL", sources["misclassified_source_count"] > 0, "Source misclassification count must be zero.", sources["misclassified_source_count"], 0)
    add("SOURCES_MACRO_OFFICIAL_PRESENT", "CRITICAL", len(sources["official_macro_sources"]) == 0, "Official macro sources must be present.", sources["official_macro_sources"], "non-empty")

    add("NASDAQ_QQQ_HOLDINGS_PRESENT", "CRITICAL", nasdaq["qqq_holdings_count"] == 0, "QQQ holdings must be present.", nasdaq["qqq_holdings_count"], ">0")
    add("NASDAQ_MEGA_CAP_RESOLVED", "CRITICAL", nasdaq["mega_cap_resolved"] == 0, "Mega-cap snapshot must resolve symbols.", nasdaq["mega_cap_resolved"], ">0")
    add("NASDAQ_SECTOR_UNKNOWN_THRESHOLD", "CRITICAL", float(nasdaq["sector_unknown_weight_pct"] or 0) >= 10.0, "Unknown sector weight must stay below 10%.", nasdaq["sector_unknown_weight_pct"], "<10")
    add("NASDAQ_SECTOR_UNKNOWN_WARNING", "WARNING", 5.0 <= float(nasdaq["sector_unknown_weight_pct"] or 0) < 10.0, "Unknown sector weight is elevated.", nasdaq["sector_unknown_weight_pct"], "<5")
    add("NASDAQ_BREADTH_PRESENT", "CRITICAL", not nasdaq["breadth_present"], "Mega-cap breadth must be present.", nasdaq["breadth_present"], True)
    add("NASDAQ_EARNINGS_STATUS_VALID", "WARNING", not nasdaq["earnings_data_available"], "Earnings data is currently unavailable.", nasdaq["earnings_data_available"], True)

    add("MACRO_FRED_PRESENT", "CRITICAL", "FRED" not in sources["official_macro_sources"], "FRED macro source must be present.", sources["official_macro_sources"], "FRED")
    add("MACRO_BEA_PRESENT", "CRITICAL", not any(str(source).startswith("BEA") for source in sources["official_macro_sources"]), "BEA macro source must be present.", sources["official_macro_sources"], "BEA")
    add("MACRO_BLS_PRESENT", "CRITICAL", not any("BLS" in str(source) for source in sources["official_macro_sources"]), "BLS macro source must be present.", sources["official_macro_sources"], "BLS")
    add("MACRO_REQUIRED_SERIES_PRESENT", "CRITICAL", bool(required["required_series_missing"] or required["required_series_invalid"]), "Required macro series must be present and non-null.", required["required_series_missing"] + required["required_series_invalid"], [])
    add("MACRO_REQUIRED_SERIES_STALE", "WARNING", bool(required["required_series_stale"]), "Some required macro series are stale.", required["required_series_stale"], [])
    add("MACRO_BLS_COLD_START_COMPLETE", "CRITICAL", bool(bls_actual["missing"] or bls_actual["invalid"]), "Required BLS series must survive cold-start materialization.", bls_actual, {"missing": [], "invalid": []})
    add("PIPELINE_CRITICAL_FETCH_COMPLETE", "CRITICAL", pipeline.get("critical_fetch_completed") is False, "Critical fetch phase must complete.", pipeline.get("critical_fetch_completed"), True)
    add("PIPELINE_CRITICAL_PERSISTENCE_COMPLETE", "CRITICAL", pipeline.get("critical_persistence_completed") is False, "Critical persistence phase must complete.", pipeline.get("critical_persistence_completed"), True)
    add("PIPELINE_CRITICAL_COMMIT_COMPLETE", "CRITICAL", pipeline.get("critical_commits_completed") is False, "Critical commits must complete.", pipeline.get("critical_commits_completed"), True)
    add("PIPELINE_CRITICAL_READBACK_COMPLETE", "CRITICAL", pipeline.get("critical_read_back_completed") is False, "Critical read-back must complete.", pipeline.get("critical_read_back_completed"), True)
    add("PIPELINE_SNAPSHOT_FROM_DB", "CRITICAL", pipeline.get("snapshot_built_from_db") is False, "Snapshot must be built from committed DB state.", pipeline.get("snapshot_built_from_db"), True)
    add("PIPELINE_NO_FETCH_PERSISTENCE_GAP", "CRITICAL", bool(pipeline.get("fetched_but_not_persisted")), "Fetched records must be persisted.", pipeline.get("fetched_but_not_persisted"), [])
    add("PIPELINE_NO_PERSISTENCE_READBACK_GAP", "CRITICAL", bool(pipeline.get("persisted_but_not_read_back")), "Persisted records must be readable.", pipeline.get("persisted_but_not_read_back"), [])
    add("PIPELINE_NO_READBACK_MATERIALIZATION_GAP", "CRITICAL", bool(pipeline.get("read_back_but_not_materialized")), "Read-back records must materialize into the snapshot.", pipeline.get("read_back_but_not_materialized"), [])

    add("EVENTS_CRITICAL_ONLY_HIGH", "CRITICAL", _critical_events_not_high(market_context) > 0, "Critical macro events must have HIGH impact.", _critical_events_not_high(market_context), 0)
    add("EVENTS_FED_SEPARATED", "CRITICAL", _fed_events_in_critical(market_context) > 0, "Fed communications must be separated from critical macro events.", _fed_events_in_critical(market_context), 0)
    add("EVENTS_NO_INVALID_DUPLICATES", "CRITICAL", temporal["invalid_period_mapping_count"] > 0 or temporal["duplicates_count"] > 0, "Events must not contain invalid period mappings or duplicates.", temporal["invalid_period_mapping_count"] + temporal["duplicates_count"], 0)
    add("EVENTS_SUMMARY_FLAGS_COHERENT", "CRITICAL", _incoherent_event_summaries(market_context) > 0, "Event summary flags must be coherent with metrics.", _incoherent_event_summaries(market_context), 0)
    add("EVENTS_FORECAST_COVERAGE", "WARNING", event_enrichment_health["critical_events_total"] > 0 and event_enrichment_health["events_with_forecast"] < event_enrichment_health["critical_events_total"], "Some critical events are missing forecast values.", event_enrichment_health["events_with_forecast"], event_enrichment_health["critical_events_total"])
    add("EVENTS_CONSENSUS_COVERAGE", "WARNING", event_enrichment_health["critical_events_total"] > 0 and event_enrichment_health["events_with_consensus"] < event_enrichment_health["critical_events_total"], "Some critical events are missing verified consensus values.", event_enrichment_health["events_with_consensus"], event_enrichment_health["critical_events_total"])

    active_threshold = _int_env("AI_MARKET_HEALTH_MIN_ACTIVE_FACTS", 0)
    add("CACHE_MARKET_FACTS_ACTIVE", "WARNING", active_threshold > 0 and model_counts["market_facts_active"] < active_threshold, "Active market facts below configured threshold.", model_counts["market_facts_active"], active_threshold)
    add("CACHE_REFRESH_FALSE_NO_AI", "WARNING", refresh_mode == "false" and (providers["ai_research_called"] or providers["ai_research_requests"] > 0), "AI Researcher should not be called with refresh=false.", providers["ai_research_requests"], 0)
    add("CACHE_REFRESH_FALSE_LOW_DB_MISSES", "WARNING", refresh_mode == "false" and providers["db_misses"] > 0 and not bool(runtime_io.get("cache_used")), "DB misses are anomalous only when cache-only coverage is unavailable.", providers["db_misses"], "allowed with cache_used=true")

    add("PROVIDER_FAILURES", "CRITICAL", providers["blocking_provider_error"] > 0, _provider_warning_message("Blocking provider error reported.", providers), providers["blocking_provider_error"], 0)
    add("PROVIDER_WARNINGS", "WARNING", providers["degraded_provider_warning"] > 0, _provider_warning_message("Provider warnings reported.", providers), providers["degraded_provider_warning"], 0)
    add("PROVIDER_EXPECTED_WARNINGS", "INFO", providers["expected_provider_warning"] > 0 and providers["degraded_provider_warning"] == 0 and providers["blocking_provider_error"] == 0, _provider_warning_message("Expected provider warnings observed with fallback/cache coverage.", providers), providers["expected_provider_warning"], 0)
    add("POSITIONING_COT_AVAILABLE", "WARNING", not positioning_health["cot_available"], "COT positioning is not available; optional context omitted.", positioning_health["cot_available"], True)
    add("POSITIONING_AAII_AVAILABLE", "WARNING", not positioning_health["aaii_available"], "AAII sentiment is not available; optional context omitted.", positioning_health["aaii_available"], True)
    add("NEWS_CANONICAL_COVERAGE", "WARNING", news_enrichment_health["latest_count"] > 0 and news_enrichment_health["canonical_url_coverage_pct"] < 50, "Canonical URL coverage is low.", news_enrichment_health["canonical_url_coverage_pct"], ">=50")
    add("NEWS_DIGEST_DRIVER_SOURCES", "CRITICAL", _digest_drivers_without_sources(market_context) > 0, "News digest drivers must include source URLs.", _digest_drivers_without_sources(market_context), 0)
    add("NASDAQ_SEMICONDUCTOR_CONTEXT", "WARNING", not nasdaq_enrichment_health["semiconductor_context_available"], "Semiconductor context is incomplete.", nasdaq_enrichment_health["semiconductor_context_available"], True)
    add("NASDAQ_CONCENTRATION_CONTEXT", "WARNING", not nasdaq_enrichment_health["concentration_available"], "Nasdaq concentration context is unavailable.", nasdaq_enrichment_health["concentration_available"], True)
    add("NO_TRADING_LOGIC", "CRITICAL", not no_action_logic, "Health report must not contain operational market action fields.", no_action_logic, True)
    return checks


def _overall_quality(model: dict[str, Any]) -> dict[str, Any]:
    quality = (model.get("data_quality") or {}).get("overall_data_quality") or {}
    return {
        "completeness_score": float(quality.get("completeness_score") or 0.0),
        "freshness_score": float(quality.get("freshness_score") or 0.0),
        "reliability_score": float(quality.get("reliability_score") or 0.0),
        "temporal_consistency_score": float(quality.get("temporal_consistency_score") or 0.0),
        "source_integrity_score": float(quality.get("source_integrity_score") or 0.0),
        "critical_missing_count": int(quality.get("critical_missing_count") or 0),
        "invalid_future_actual_count": int(quality.get("invalid_future_actual_count") or 0),
        "stale_as_recent_count": int(quality.get("stale_as_recent_count") or 0),
        "is_ready_for_market_analysis": bool(quality.get("is_ready_for_market_analysis")),
        "blocking_reasons": list(quality.get("blocking_reasons") or []),
    }


def _model_counts(model: dict[str, Any], db_summary: dict[str, Any]) -> dict[str, int]:
    event_calendar = model.get("event_calendar") or {}
    nasdaq = model.get("nasdaq_context") or {}
    mega = nasdaq.get("mega_cap_snapshot") or {}
    db_facts = (db_summary or {}).get("market_facts") or {}
    db_news = (db_summary or {}).get("market_news") or {}
    return {
        "critical_macro_events": len(event_calendar.get("critical_macro_events") or []),
        "fed_communications": len(event_calendar.get("fed_communications") or []),
        "other_economic_events": len(event_calendar.get("other_economic_events") or []),
        "latest_news": len(_latest_news(model)),
        "qqq_holdings": _qqq_holdings_count(nasdaq),
        "mega_cap_resolved": int(mega.get("resolved_count") or len(mega.get("stocks") or [])),
        "market_facts_total": int(db_facts.get("total") or 0),
        "market_facts_active": int(db_facts.get("active") or 0),
        "market_news_total": int(db_news.get("total") or 0),
    }


def _temporal_summary(payload: dict[str, Any]) -> dict[str, Any]:
    blocking = payload.get("blocking_errors") or []
    return {
        "future_actual_count": int(payload.get("future_actual_count") or 0),
        "stale_as_recent_count": int(payload.get("stale_as_recent_count") or 0),
        "released_without_actual_count": int(payload.get("released_without_actual_count") or 0),
        "awaiting_actual_count": int(payload.get("awaiting_actual_count") or 0),
        "invalid_period_mapping_count": int(payload.get("invalid_period_mapping_count") or 0),
        "duplicates_count": int(payload.get("duplicates_count") or 0),
        "blocking_errors_count": len(blocking),
    }


def _release_summary(payload: dict[str, Any]) -> dict[str, Any]:
    awaiting = list(payload.get("awaiting_actual") or [])
    fred = [item for item in awaiting if "FRED:" in str(item.get("fact_key") if isinstance(item, dict) else item)]
    return {
        "awaiting_actual_count": len(awaiting),
        "fred_in_release_queue": len(fred),
        "retry_seconds": list(payload.get("retry_seconds") or []),
        "max_attempts": int(payload.get("max_attempts") or 0),
        "awaiting_actual_items": awaiting,
    }


def _news_summary(payload: dict[str, Any], latest: list[dict[str, Any]]) -> dict[str, int]:
    placeholder_count = sum(1 for item in latest if _has_placeholder(item.get("title")))
    mojibake_count = sum(1 for item in latest if _has_mojibake(item.get("title")) or _has_mojibake(item.get("summary")))
    expired_latest = sum(1 for item in latest if str(item.get("freshness") or "").upper() in {"EXPIRED", "STALE"})
    invalid_latest = sum(1 for item in latest if news_content_status(item) == "invalid_content")
    future_published = sum(1 for item in latest if _is_future_news(item.get("published_at")))
    empty_url = sum(1 for item in latest if not item.get("source_url"))
    empty_source = sum(1 for item in latest if not item.get("source"))
    duplicate_title_or_url = _duplicate_news_count(latest)
    return {
        "total_news": int(payload.get("total_news") or 0),
        "latest_eligible_count": int(payload.get("latest_eligible_count") or 0),
        "latest_returned_count": len(latest),
        "expired_count": int(payload.get("expired_count") or 0),
        "invalid_content_count": int(payload.get("invalid_content_count") or 0),
        "placeholder_news_count": placeholder_count + invalid_latest,
        "stale_as_recent_count": int(payload.get("stale_as_recent_count") or 0),
        "mojibake_count": mojibake_count,
        "expired_in_latest_count": expired_latest,
        "future_published_count": future_published,
        "empty_url_count": empty_url,
        "empty_source_count": empty_source,
        "duplicate_title_or_url_count": duplicate_title_or_url,
    }


def _source_summary(model: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    macro_sources = sorted({source for source in _macro_sources(model) if source})
    items = list(payload.get("items") or [])
    official_news = int(payload.get("official_count") or sum(1 for item in items if item.get("is_official_source")))
    market_news = int(payload.get("market_count") or sum(1 for item in items if item.get("source_classification") == "market_source"))
    other_news = sum(1 for item in items if item.get("source_classification") == "other_source")
    unknown = sum(1 for item in items if not item.get("source") or item.get("source_classification") in {None, "unknown"})
    misclassified = sum(1 for item in items if item.get("is_official_source") and any(token in str(item.get("source") or "").upper() for token in MARKET_PUBLISHERS))
    return {
        "official_macro_sources": macro_sources,
        "official_news_count": official_news,
        "market_news_count": market_news,
        "other_news_count": other_news,
        "unknown_source_count": unknown,
        "misclassified_source_count": misclassified,
    }


def _nasdaq_summary(model: dict[str, Any]) -> dict[str, Any]:
    nasdaq = model.get("nasdaq_context") or {}
    mega = nasdaq.get("mega_cap_snapshot") or {}
    breadth = nasdaq.get("mega_cap_breadth") or {}
    earnings = nasdaq.get("earnings") or {}
    sector = nasdaq.get("sector_exposure") or {}
    earnings_quality = earnings.get("data_quality") or {}
    return {
        "qqq_holdings_count": _qqq_holdings_count(nasdaq),
        "mega_cap_tracked": int(mega.get("tracked_count") or 0),
        "mega_cap_resolved": int(mega.get("resolved_count") or len(mega.get("stocks") or [])),
        "sector_classified_weight_pct": float(sector.get("classified_weight_pct") or 0.0),
        "sector_unknown_weight_pct": float(sector.get("unknown_weight_pct") or 0.0),
        "earnings_data_available": bool(earnings_quality.get("final_data_available", True)),
        "breadth_present": bool(breadth),
    }


def _provider_summary(model: dict[str, Any]) -> dict[str, Any]:
    quality = model.get("data_quality") or {}
    macro = quality.get("macro") or {}
    nasdaq = quality.get("nasdaq") or {}
    multi_source = quality.get("multi_source_pipeline") or {}
    provider_failures = (
        int(quality.get("provider_failures") or 0)
        + int(macro.get("provider_failures") or 0)
        + int(nasdaq.get("provider_failures") or 0)
        + len(multi_source.get("errors") or [])
    )
    warning_details = _deduplicate_provider_warning_details(
        _provider_warning_details(quality, macro, nasdaq, multi_source)
    )
    return {
        "provider_failures": provider_failures,
        "provider_warnings": len(warning_details),
        "warning_details": warning_details,
        "expected_provider_warning": sum(1 for item in warning_details if item["code"] == "expected_provider_warning"),
        "degraded_provider_warning": sum(1 for item in warning_details if item["code"] == "degraded_provider_warning"),
        "blocking_provider_error": sum(1 for item in warning_details if item["code"] == "blocking_provider_error"),
        "ai_research_called": bool(quality.get("ai_research_called")),
        "ai_research_requests": int(quality.get("ai_research_requests") or 0),
        "db_hits": int(quality.get("db_hits") or 0) + int(macro.get("db_hits") or 0) + int(nasdaq.get("db_hits") or 0),
        "db_misses": int(quality.get("db_misses") or 0) + int(macro.get("db_misses") or 0) + int(nasdaq.get("db_misses") or 0),
    }


def _event_enrichment_health(model: dict[str, Any]) -> dict[str, Any]:
    events = (model.get("event_calendar") or {}).get("critical_macro_events") or []
    def has_field(event: dict[str, Any], field: str) -> bool:
        enrichment = event.get("enrichment") or {}
        metrics = enrichment.get("metrics") or []
        return enrichment.get(field) not in (None, "") or any(item.get(field) not in (None, "") for item in metrics if isinstance(item, dict))

    total = len(events)
    with_forecast = sum(1 for event in events if has_field(event, "forecast"))
    with_consensus = sum(1 for event in events if has_field(event, "consensus"))
    return {
        "critical_events_total": total,
        "events_with_previous": sum(1 for event in events if has_field(event, "previous")),
        "events_with_forecast": with_forecast,
        "events_with_consensus": with_consensus,
        "forecast_coverage_pct": round((with_forecast / total) * 100, 2) if total else 0.0,
        "consensus_coverage_pct": round((with_consensus / total) * 100, 2) if total else 0.0,
        "events_awaiting_actual": sum(1 for event in events if ((event.get("enrichment") or {}).get("summary") or {}).get("temporal_status") == "awaiting_actual"),
        "events_released_with_actual": sum(1 for event in events if has_field(event, "actual")),
    }


def _positioning_health(model: dict[str, Any]) -> dict[str, Any]:
    positioning = model.get("positioning") or {}
    cot = ((positioning.get("cot") or {}).get("nasdaq_100") or {})
    sentiment = model.get("sentiment_context") or {}
    aaii = sentiment.get("aaii") or {}
    return {
        "cot_available": bool(cot.get("report_date")),
        "cot_fresh": str(positioning.get("freshness") or "").upper() in {"FRESH", "WEEKLY"},
        "cot_status": cot.get("status") or positioning.get("status"),
        "cot_last_report_date": cot.get("report_date"),
        "cot_age_days": _age_days(cot.get("report_date")),
        "aaii_available": bool(aaii.get("survey_date")),
        "aaii_fresh": str(aaii.get("freshness") or "").upper() in {"FRESH", "WEEKLY"} and bool(aaii.get("survey_date")),
        "aaii_status": aaii.get("status") or sentiment.get("status"),
        "aaii_last_survey_date": aaii.get("survey_date"),
    }


def _news_enrichment_health(model: dict[str, Any], sources: dict[str, Any]) -> dict[str, Any]:
    latest = _latest_news(model)
    digest = model.get("news_digest") or {}
    drivers = digest.get("drivers") or []
    duplicates = len({item.get("duplicate_group_id") for item in latest if item.get("duplicate_group_id")})
    summary_sources = [str(item.get("summary_source_type") or "") for item in latest]
    return {
        "latest_count": len(latest),
        "summary_coverage_pct": round((sum(1 for item in latest if item.get("summary")) / len(latest)) * 100, 2) if latest else 0.0,
        "canonical_url_coverage_pct": round((sum(1 for item in latest if item.get("canonical_url")) / len(latest)) * 100, 2) if latest else 0.0,
        "official_news_count": sources.get("official_news_count", 0),
        "official_news_last_seen_at": max([item.get("published_at") for item in latest if item.get("is_official_source") and item.get("published_at")] or [None]),
        "multi_source_driver_count": sum(1 for item in drivers if item.get("is_confirmed_by_multiple_sources")),
        "duplicate_group_count": duplicates,
        "rss_summary_count": sum(1 for source in summary_sources if source == "rss_description"),
        "metadata_summary_count": sum(1 for source in summary_sources if source in {"api", "opengraph", "meta_description"}),
        "extracted_text_summary_count": sum(1 for source in summary_sources if source in {"content_encoded", "article_text"}),
        "ai_summary_count": sum(1 for item in latest if item.get("generated_by_ai")),
        "missing_summary_count": sum(1 for item in latest if not item.get("summary")),
    }


def _nasdaq_enrichment_health(model: dict[str, Any]) -> dict[str, Any]:
    nasdaq = model.get("nasdaq_context") or {}
    earnings = (nasdaq.get("earnings") or {}).get("upcoming") or []
    semiconductor = nasdaq.get("semiconductor_context") or {}
    holdings_count = _qqq_holdings_count(nasdaq)
    today = datetime.now(UTC).date()
    earnings_30 = sum(1 for item in earnings if _date_within(item.get("date"), today, 30))
    earnings_90 = sum(1 for item in earnings if _date_within(item.get("date"), today, 90))
    return {
        "breadth_available": bool(nasdaq.get("mega_cap_breadth")),
        "semiconductor_context_available": bool((nasdaq.get("semiconductor_context") or {}).get("resolved_count")),
        "concentration_available": bool(nasdaq.get("concentration")),
        "earnings_coverage_pct": round((len(earnings) / holdings_count) * 100, 2) if holdings_count else 0.0,
        "semiconductor_resolution_pct": semiconductor.get("data_quality", {}).get("resolution_pct", 0.0),
        "earnings_symbols_covered_pct": round((len({item.get("symbol") for item in earnings}) / 13) * 100, 2),
        "earnings_30d_count": earnings_30,
        "earnings_90d_count": earnings_90,
    }


def _ai_research_health(model: dict[str, Any]) -> dict[str, Any]:
    quality = model.get("data_quality") or {}
    calls = {}
    requests = int(quality.get("ai_research_requests") or 0)
    if requests:
        calls["macro_events"] = requests
    rejected = int(quality.get("ai_results_rejected") or 0)
    accepted = int(quality.get("ai_results_valid") or quality.get("ai_results_used") or 0)
    return {
        "calls_by_type": calls,
        "accepted": accepted,
        "rejected": rejected,
        "not_found": int(quality.get("ai_not_found") or 0),
        "blocked": int(quality.get("ai_blocked") or 0),
        "access_restricted": int(quality.get("ai_access_restricted") or 0),
        "rejection_reasons": quality.get("ai_rejection_reasons") or {},
    }


def _provider_warning_details(quality: dict[str, Any], macro: dict[str, Any], nasdaq: dict[str, Any], multi_source: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for provider, payload in (("market_context", quality), ("macro", macro), ("nasdaq", nasdaq), ("multi_source", multi_source or {})):
        db_hits = int(payload.get("db_hits") or 0)
        provider_hits = int(payload.get("provider_hits") or 0)
        db_misses = int(payload.get("db_misses") or 0)
        failures = int(payload.get("provider_failures") or 0)
        for warning in payload.get("warnings") or []:
            details.append(_provider_warning_detail(provider, str(warning), now, db_hits=db_hits, provider_hits=provider_hits, db_misses=db_misses, failures=0))
        for error in payload.get("errors") or []:
            details.append(_provider_warning_detail(provider, str(error), now, db_hits=db_hits, provider_hits=provider_hits, db_misses=db_misses, failures=max(failures, 1)))
        if failures and not payload.get("errors"):
            details.append(_provider_warning_detail(provider, f"{provider} provider_failures={failures}", now, db_hits=db_hits, provider_hits=provider_hits, db_misses=db_misses, failures=failures))
    return details


def _provider_warning_detail(provider: str, message: str, occurred_at: str, *, db_hits: int, provider_hits: int, db_misses: int, failures: int) -> dict[str, Any]:
    lowered = message.lower()
    is_cached_failure = db_hits > 0 or "cache" in lowered or "db" in lowered or "fallback" in lowered
    if failures > 0 and db_hits == 0 and provider_hits == 0 and db_misses > 0:
        code = "blocking_provider_error"
        is_blocking = True
    elif is_cached_failure or "preview" in lowered or "fallback" in lowered or "disabled" in lowered or "excluded" in lowered:
        code = "expected_provider_warning"
        is_blocking = False
    else:
        code = "degraded_provider_warning"
        is_blocking = False
    return {
        "provider": provider,
        "code": code,
        "message": message,
        "occurred_at": occurred_at,
        "is_cached_failure": is_cached_failure,
        "is_blocking": is_blocking,
    }


def _deduplicate_provider_warning_details(details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in details:
        key = (str(item.get("provider")), str(item.get("code")), str(item.get("message")))
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _provider_warning_message(prefix: str, providers: dict[str, Any]) -> str:
    details = providers.get("warning_details") or []
    if not details:
        return prefix
    sample = "; ".join(f"{item['provider']}:{item['code']}:{item['message']}" for item in details[:3])
    suffix = "" if len(details) <= 3 else f"; +{len(details) - 3} more"
    return f"{prefix} {sample}{suffix}"


def _required_series(model: dict[str, Any]) -> dict[str, list[str]]:
    snapshot = model.get("macro_snapshot") or {}
    macro_items = _flatten_macro(snapshot)
    bls_status = bls_required_series_status_from_macro_snapshot(snapshot)
    present: set[str] = set()
    invalid: list[str] = []
    stale: list[str] = []
    for key, item in macro_items:
        identifiers = {key.upper(), str(item.get("series_id") or "").upper(), str(item.get("metric") or "").upper(), str(item.get("name") or "").upper()}
        present.update(identifier for identifier in identifiers if identifier)
        if _is_required_candidate(identifiers) and item.get("value") in (None, "") and item.get("latest_released_value") in (None, ""):
            invalid.append(key)
        if _is_required_candidate(identifiers) and str(item.get("freshness") or "").upper() in {"STALE", "EXPIRED"}:
            stale.append(key)
    required = [*REQUIRED_FRED, *REQUIRED_BEA, *REQUIRED_BLS]
    missing = []
    for series in REQUIRED_FRED:
        if series not in present:
            missing.append(series)
    for series in REQUIRED_BEA:
        if series not in present:
            missing.append(series)
    for series_id in BLS_REQUIRED_SERIES_IDS:
        label = BLS_CANONICAL_NAME_BY_ID[series_id]
        if series_id not in bls_status["present"]:
            missing.append(label)
    invalid.extend(BLS_CANONICAL_NAME_BY_ID[series_id] for series_id in bls_status["invalid"])
    return {
        "required_series_present": sorted(set(required) - set(missing)),
        "required_series_missing": missing,
        "required_series_invalid": sorted(set(invalid)),
        "required_series_stale": sorted(set(stale)),
    }


def _flatten_macro(snapshot: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    output = []
    for value in snapshot.values():
        if not isinstance(value, dict):
            continue
        for key, item in value.items():
            if isinstance(item, dict):
                output.append((str(key), item))
    return output


def _macro_sources(model: dict[str, Any]) -> list[str]:
    sources = []
    for _, item in _flatten_macro(model.get("macro_snapshot") or {}):
        source = str(item.get("source") or "")
        if classify_source(source, item.get("source_url"))["is_official_source"] or source.upper() == "FRED":
            if "BLS" in source.upper():
                sources.append("BLS")
            elif "BEA" in source.upper():
                sources.append("BEA")
            elif "FEDERAL RESERVE" in source.upper():
                sources.append("Federal Reserve")
            elif source.upper() == "FRED":
                sources.append("FRED")
            else:
                sources.append(source)
    return sources


def _latest_news(model: dict[str, Any]) -> list[dict[str, Any]]:
    latest = (model.get("news_context") or {}).get("latest")
    if isinstance(latest, list):
        return [item for item in latest if isinstance(item, dict)]
    legacy = model.get("latest_news")
    if isinstance(legacy, dict):
        articles = legacy.get("articles") or []
        return [item for item in articles if isinstance(item, dict)]
    return []


def _qqq_holdings(nasdaq: dict[str, Any]) -> list[dict[str, Any]]:
    qqq = nasdaq.get("qqq_holdings") or {}
    return list(qqq.get("top_holdings") or qqq.get("holdings") or [])


def _qqq_holdings_count(nasdaq: dict[str, Any]) -> int:
    qqq = nasdaq.get("qqq_holdings") or {}
    return int(qqq.get("holdings_count") or len(_qqq_holdings(nasdaq)))


def _has_placeholder(value: Any) -> bool:
    text = str(value or "").upper()
    return any(token in text for token in PLACEHOLDER_TOKENS)


def _has_mojibake(value: Any) -> bool:
    text = str(value or "")
    return any(token in text for token in MOJIBAKE_TOKENS)


def _is_future_news(value: Any) -> bool:
    published = parse_datetime(value)
    return bool(published and published > datetime.now(UTC) + timedelta(minutes=15))


def _age_days(value: Any) -> int | None:
    parsed = parse_datetime(value)
    if not parsed:
        return None
    return max(0, (datetime.now(UTC).date() - parsed.date()).days)


def _date_within(value: Any, start, days: int) -> bool:
    parsed = parse_datetime(value)
    return bool(parsed and start <= parsed.date() <= start + timedelta(days=days))


def _duplicate_news_count(items: list[dict[str, Any]]) -> int:
    seen: set[str] = set()
    duplicates = 0
    for item in items:
        for key in (item.get("source_url"), item.get("title")):
            normalized = str(key or "").strip().lower()
            if not normalized:
                continue
            if normalized in seen:
                duplicates += 1
            seen.add(normalized)
    return duplicates


def _missing_summary_ratio(items: list[dict[str, Any]]) -> float:
    if not items:
        return 0.0
    missing = sum(1 for item in items if not item.get("summary"))
    return missing / len(items)


def _critical_events_not_high(model: dict[str, Any]) -> int:
    return sum(1 for event in ((model.get("event_calendar") or {}).get("critical_macro_events") or []) if str(event.get("impact") or "").upper() != "HIGH")


def _fed_events_in_critical(model: dict[str, Any]) -> int:
    count = 0
    for event in ((model.get("event_calendar") or {}).get("critical_macro_events") or []):
        text = f"{event.get('name') or ''} {event.get('category') or ''}".upper()
        if "FED" in text or "FOMC" in text or "SPEECH" in text:
            count += 1
    return count


def _incoherent_event_summaries(model: dict[str, Any]) -> int:
    count = 0
    for event in ((model.get("event_calendar") or {}).get("critical_macro_events") or []):
        enrichment = event.get("enrichment") or {}
        summary = enrichment.get("summary") or {}
        metrics = enrichment.get("metrics") or []
        if summary.get("has_actual") is False and any(item.get("actual") not in (None, "") for item in metrics if isinstance(item, dict)):
            count += 1
        if summary.get("has_previous") is False and any(item.get("previous") not in (None, "") for item in metrics if isinstance(item, dict)):
            count += 1
    return count


def _digest_drivers_without_sources(model: dict[str, Any]) -> int:
    drivers = (model.get("news_digest") or {}).get("drivers") or []
    return sum(1 for item in drivers if not item.get("source_urls"))


def _is_required_candidate(identifiers: set[str]) -> bool:
    joined = " ".join(identifiers)
    return any(series in identifiers for series in REQUIRED_FRED) or any(series in identifiers for series in REQUIRED_BEA) or bool(normalize_bls_series_id(joined))


def _bls_status_from_macro_pipeline(macro_pipeline: dict[str, Any]) -> dict[str, list[str]]:
    status = macro_pipeline.get("bls_required_series")
    if isinstance(status, dict):
        return {
            "required": list(status.get("required") or BLS_REQUIRED_SERIES_IDS),
            "present": list(status.get("present") or []),
            "missing": list(status.get("missing") or []),
            "invalid": list(status.get("invalid") or []),
            "materialized": list(status.get("materialized") or []),
        }
    return {
        "required": list(macro_pipeline.get("required_bls_series") or BLS_REQUIRED_SERIES_IDS),
        "present": list(macro_pipeline.get("required_bls_present") or []),
        "missing": list(macro_pipeline.get("required_bls_missing") or []),
        "invalid": list(macro_pipeline.get("required_bls_invalid") or []),
        "materialized": list(macro_pipeline.get("materialized_bls_series") or []),
    }


def _report_has_no_action_logic(payload: Any) -> bool:
    serialized = json.dumps(payload, default=str).lower()
    forbidden_keys = ('"action"', '"position"', '"order"', '"entry"', '"stop"', '"target"', '"recommendation"')
    return not any(key in serialized for key in forbidden_keys)


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    json.loads(text)
    temp.write_text(text + "\n", encoding="utf-8")
    json.loads(temp.read_text(encoding="utf-8"))
    os.replace(temp, path)
