from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from app.services.bls_required_series import BLS_REQUIRED_SERIES_IDS, bls_required_series_status_from_macro_snapshot
from app.services.health_report_service import HealthReportService


def clean_inputs():
    return {
        "base_url": "http://testserver",
        "refresh_mode": "false",
        "service_status": "ok",
        "db_health": {"status": "ok"},
        "market_context": market_context(),
        "temporal_integrity": {
            "future_actual_count": 0,
            "stale_as_recent_count": 0,
            "released_without_actual_count": 0,
            "awaiting_actual_count": 0,
            "invalid_period_mapping_count": 0,
            "duplicates_count": 0,
            "blocking_errors": [],
        },
        "release_refresh": {"awaiting_actual": [], "retry_seconds": [30, 120, 300], "max_attempts": 3},
        "news_freshness": {"total_news": 2, "latest_eligible_count": 2, "expired_count": 0, "invalid_content_count": 0, "stale_as_recent_count": 0},
        "source_classification": {
            "official_count": 1,
            "market_count": 1,
            "items": [
                {"source": "BLS", "is_official_source": True, "source_classification": "official_source"},
                {"source": "Reuters", "is_official_source": False, "source_classification": "market_source"},
            ],
        },
        "db_summary": {"market_facts": {"total": 20, "active": 20}, "market_news": {"total": 2}},
    }


def market_context():
    return {
        "service_role": "data provider only",
        "metadata": {"trading_logic": "not implemented; data service only"},
        "data_quality": {
            "db_hits": 12,
            "db_misses": 0,
            "provider_failures": 0,
            "warnings": [],
            "ai_research_called": False,
            "ai_research_requests": 0,
            "overall_data_quality": {
                "completeness_score": 1.0,
                "freshness_score": 0.95,
                "reliability_score": 0.98,
                "temporal_consistency_score": 1.0,
                "source_integrity_score": 1.0,
                "critical_missing_count": 0,
                "invalid_future_actual_count": 0,
                "stale_as_recent_count": 0,
                "is_ready_for_market_analysis": True,
                "blocking_reasons": [],
            },
        },
        "macro_snapshot": macro_snapshot(),
        "event_calendar": {
            "critical_macro_events": [
                {
                    "name": "Consumer Price Index",
                    "category": "CPI",
                    "impact": "HIGH",
                    "enrichment": {
                        "summary": {"has_previous": True, "has_forecast": True, "has_consensus": True, "has_actual": False},
                        "metrics": [{"previous": 0.3, "forecast": 0.4, "consensus": 0.4, "actual": None}],
                    },
                }
            ],
            "fed_communications": [{"name": "Fed speech", "impact": "MEDIUM"}],
            "other_economic_events": [],
        },
        "news_context": {
            "latest": [
                {
                    "title": "BLS official release",
                    "summary": "Official macro release.",
                    "source": "BLS",
                    "source_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                    "canonical_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                    "duplicate_group_id": "bls-cpi",
                    "published_at": "2026-07-10T10:00:00Z",
                    "freshness": "FRESH",
                },
                {
                    "title": "Reuters market story",
                    "summary": "Market context.",
                    "source": "Reuters",
                    "source_url": "https://www.reuters.com/story",
                    "canonical_url": "https://www.reuters.com/story",
                    "duplicate_group_id": "reuters-story",
                    "published_at": "2026-07-10T09:00:00Z",
                    "freshness": "FRESH",
                },
            ]
        },
        "nasdaq_context": {
            "qqq_holdings": {"top_holdings": [{"symbol": "NVDA", "weight": 7.0}]},
            "mega_cap_snapshot": {"tracked_count": 12, "resolved_count": 12, "stocks": [{} for _ in range(12)]},
            "mega_cap_breadth": {"positive_count": 5, "negative_count": 4},
            "earnings": {"data_quality": {"final_data_available": True}},
            "sector_exposure": {"classified_weight_pct": 96.0, "unknown_weight_pct": 4.0},
            "concentration": {"top_5_weight_pct": 20.0},
            "semiconductor_context": {"resolved_count": 2},
        },
        "positioning": {"cot": {"nasdaq_100": {"report_date": "2026-07-07"}}},
        "sentiment_context": {"aaii": {"survey_date": "2026-07-09", "freshness": "WEEKLY"}},
        "news_digest": {
            "drivers": [
                {
                    "driver_id": "driver-1",
                    "source_urls": ["https://www.bls.gov/news.release/cpi.nr0.htm", "https://www.reuters.com/story"],
                    "is_confirmed_by_multiple_sources": True,
                }
            ]
        },
        "db_summary": {"market_facts": {"total": 20, "active": 20}, "market_news": {"total": 2}},
    }


def macro_snapshot():
    def item(series_id, source, value=1.0, name=None):
        return {
            "series_id": series_id,
            "name": name or series_id,
            "value": value,
            "latest_released_value": value,
            "source": source,
            "source_url": "https://www.bls.gov/" if "BLS" in source else "https://www.bea.gov/" if "BEA" in source else "https://fred.stlouisfed.org/",
            "freshness": "FRESH",
        }

    return {
        "rates_and_yields": {
            "DGS2": item("DGS2", "FRED"),
            "DGS10": item("DGS10", "FRED"),
            "FEDFUNDS": item("FEDFUNDS", "FRED"),
            "SOFR": item("SOFR", "FRED"),
            "T10Y2Y": item("T10Y2Y", "FRED"),
        },
        "financial_conditions": {
            "VIXCLS": item("VIXCLS", "FRED"),
            "NFCI": item("NFCI", "FRED"),
        },
        "growth": {
            "BEA:GDP": item("BEA:GDP", "BEA"),
            "BEA:REAL_GDP": item("BEA:REAL_GDP", "BEA"),
            "BEA:PERSONAL_INCOME": item("BEA:PERSONAL_INCOME", "BEA"),
            "BEA:PERSONAL_SPENDING": item("BEA:PERSONAL_SPENDING", "BEA"),
        },
        "inflation": {
            "BEA:CORE_PCE": item("BEA:CORE_PCE", "BEA"),
            "BEA:PCE": item("BEA:PCE", "BEA"),
            "CUSR0000SA0": item("CUSR0000SA0", "BLS Consumer Price Index Summary", name="Consumer Price Index"),
            "WPUFD4": item("WPUFD4", "BLS Producer Price Index News Release", name="Producer Price Index"),
        },
        "labor": {
            "CES0000000001": item("CES0000000001", "BLS Employment Situation Summary", name="Nonfarm Payrolls"),
            "LNS14000000": item("LNS14000000", "BLS Employment Situation Summary", name="Unemployment Rate"),
        },
    }


def report(**overrides):
    inputs = clean_inputs()
    inputs.update(overrides)
    return HealthReportService().build_report(**inputs)


def check(report_data, check_id):
    return next(item for item in report_data["checks"] if item["id"] == check_id)


def test_clean_report_pass_exit_zero():
    data = report()

    assert data["status"] == "PASS"
    assert data["exit_code"] == 0
    assert data["summary"]["critical_error_count"] == 0


def test_warning_report_exit_one_and_fail_on_warning_exit_two():
    context = market_context()
    context["data_quality"]["overall_data_quality"]["freshness_score"] = 0.8

    data = report(market_context=context)
    strict = report(market_context=context, fail_on_warning=True)

    assert data["status"] == "WARNING"
    assert data["exit_code"] == 1
    assert strict["exit_code"] == 2


def test_critical_anomaly_fail_exit_two_and_unreachable_service():
    temporal = clean_inputs()["temporal_integrity"]
    temporal["future_actual_count"] = 1

    data = report(temporal_integrity=temporal)
    unreachable = report(service_status="unreachable", db_health={"status": "error"})

    assert data["status"] == "FAIL"
    assert data["exit_code"] == 2
    assert unreachable["status"] == "FAIL"


def test_release_queue_fred_fail_and_awaiting_within_retry_warning_and_overdue_fail():
    waiting = {"fact_key": "US:CPI:2026:macro_event_enrichment", "attempt_count": 1}
    warning = report(release_refresh={"awaiting_actual": [waiting], "retry_seconds": [30], "max_attempts": 3})
    fred = report(release_refresh={"awaiting_actual": [{"fact_key": "FRED:VIXCLS:latest:official_macro_latest"}], "retry_seconds": [30], "max_attempts": 3})
    overdue = report(release_refresh={"awaiting_actual": [{"fact_key": "US:CPI", "attempt_count": 3}], "retry_seconds": [30], "max_attempts": 3})

    assert warning["status"] == "WARNING"
    assert fred["status"] == "FAIL"
    assert overdue["status"] == "FAIL"


def test_news_placeholder_fail_mojibake_warning_expired_latest_fail_and_official_zero_warning():
    context = market_context()
    context["news_context"]["latest"][0]["title"] = "META_TITLE_QUOTE - Yahoo Finance"
    assert report(market_context=context)["status"] == "FAIL"

    context = market_context()
    context["news_context"]["latest"][0]["title"] = "Here Ã¢ bad title"
    assert check(report(market_context=context), "NEWS_MOJIBAKE_ZERO")["status"] == "WARNING"

    context = market_context()
    context["news_context"]["latest"][0]["freshness"] = "EXPIRED"
    assert report(market_context=context)["status"] == "FAIL"

    sources = clean_inputs()["source_classification"]
    sources["official_count"] = 0
    sources["items"] = [sources["items"][1]]
    assert report(source_classification=sources)["status"] == "WARNING"


def test_official_news_zero_generates_single_warning_without_duplicate_classification_warning():
    sources = clean_inputs()["source_classification"]
    sources["official_count"] = 0
    sources["items"] = [sources["items"][1]]

    data = report(source_classification=sources)
    warning_ids = [item["id"] for item in data["checks"] if item["status"] == "WARNING"]

    assert warning_ids.count("NEWS_OFFICIAL_PRESENT") == 1
    assert "SOURCES_OFFICIAL_CLASSIFICATION" not in warning_ids
    assert check(data, "SOURCES_OFFICIAL_CLASSIFICATION")["status"] in {"PASS", "INFO"}


def test_provider_warning_detail_present_and_fallback_warning_is_not_critical():
    context = market_context()
    context["data_quality"]["macro"] = {
        "db_hits": 13,
        "db_misses": 0,
        "provider_hits": 0,
        "provider_failures": 0,
        "warnings": ["macro_loaded_from_db_preview_only"],
        "errors": [],
    }

    data = report(market_context=context)
    detail = data["provider_health"]["warning_details"][0]

    assert detail["provider"] == "macro"
    assert detail["code"] == "expected_provider_warning"
    assert detail["is_cached_failure"] is True
    assert detail["is_blocking"] is False
    assert check(data, "PROVIDER_FAILURES")["status"] == "PASS"


def test_degraded_provider_warning_message_includes_detail():
    context = market_context()
    context["data_quality"]["macro"] = {
        "db_hits": 0,
        "db_misses": 0,
        "provider_hits": 1,
        "provider_failures": 0,
        "warnings": ["temporary latency from macro provider"],
        "errors": [],
    }

    data = report(market_context=context)
    provider_check = check(data, "PROVIDER_WARNINGS")

    assert provider_check["status"] == "WARNING"
    assert "macro:degraded_provider_warning:temporary latency from macro provider" in provider_check["message"]
    assert data["provider_health"]["degraded_provider_warning"] == 1


def test_essential_provider_without_fallback_is_critical():
    context = market_context()
    context["data_quality"]["macro"] = {
        "db_hits": 0,
        "db_misses": 1,
        "provider_hits": 0,
        "provider_failures": 1,
        "warnings": [],
        "errors": ["macro provider unavailable"],
    }

    data = report(market_context=context)

    assert data["status"] == "FAIL"
    assert data["provider_health"]["blocking_provider_error"] == 1
    assert check(data, "PROVIDER_FAILURES")["status"] == "FAIL"


def test_nasdaq_and_macro_failures():
    context = market_context()
    context["nasdaq_context"]["qqq_holdings"]["top_holdings"] = []
    assert check(report(market_context=context), "NASDAQ_QQQ_HOLDINGS_PRESENT")["status"] == "FAIL"

    context = market_context()
    context["nasdaq_context"]["sector_exposure"]["unknown_weight_pct"] = 7.0
    assert check(report(market_context=context), "NASDAQ_SECTOR_UNKNOWN_WARNING")["status"] == "WARNING"

    context = market_context()
    context["nasdaq_context"]["sector_exposure"]["unknown_weight_pct"] = 10.0
    assert check(report(market_context=context), "NASDAQ_SECTOR_UNKNOWN_THRESHOLD")["status"] == "FAIL"

    context = market_context()
    del context["macro_snapshot"]["financial_conditions"]["VIXCLS"]
    assert check(report(market_context=context), "MACRO_REQUIRED_SERIES_PRESENT")["status"] == "FAIL"


def test_bls_cold_start_complete_uses_missing_not_present_list():
    context = market_context()
    context["data_quality"]["macro_pipeline"] = {
        "bls_required_series": {
            "required": list(BLS_REQUIRED_SERIES_IDS),
            "present": list(BLS_REQUIRED_SERIES_IDS),
            "missing": [],
            "invalid": [],
            "materialized": list(BLS_REQUIRED_SERIES_IDS),
        }
    }

    bls_check = check(report(market_context=context), "MACRO_BLS_COLD_START_COMPLETE")

    assert bls_check["status"] == "PASS"
    assert bls_check["actual_value"] == {"missing": [], "invalid": []}


def test_bls_logical_aliases_are_normalized():
    snapshot = {
        "inflation": {
            "CPI": {"series_id": "CPI", "value": 1.0},
            "Producer Price Index": {"name": "Producer Price Index", "value": 1.0},
        },
        "labor": {
            "NFP": {"metric": "NFP", "value": 1.0},
            "Unemployment": {"name": "Unemployment Rate", "value": 1.0},
        },
    }

    status = bls_required_series_status_from_macro_snapshot(snapshot)

    assert status["present"] == list(BLS_REQUIRED_SERIES_IDS)
    assert status["missing"] == []


def test_bls_cold_start_fails_for_really_missing_series():
    context = market_context()
    context["data_quality"]["macro_pipeline"] = {
        "bls_required_series": {
            "required": list(BLS_REQUIRED_SERIES_IDS),
            "present": ["CUSR0000SA0", "CES0000000001", "LNS14000000"],
            "missing": ["WPUFD4"],
            "invalid": [],
            "materialized": ["CUSR0000SA0", "CES0000000001", "LNS14000000"],
        }
    }

    bls_check = check(report(market_context=context), "MACRO_BLS_COLD_START_COMPLETE")

    assert bls_check["status"] == "FAIL"
    assert bls_check["actual_value"] == {"missing": ["WPUFD4"], "invalid": []}


def test_cache_refresh_false_ai_warning_and_db_miss_warning():
    context = market_context()
    context["data_quality"]["ai_research_called"] = True
    context["data_quality"]["ai_research_requests"] = 1
    assert check(report(market_context=context), "CACHE_REFRESH_FALSE_NO_AI")["status"] == "WARNING"

    context = market_context()
    context["data_quality"]["db_misses"] = 2
    assert check(report(market_context=context), "CACHE_REFRESH_FALSE_LOW_DB_MISSES")["status"] == "WARNING"


def test_report_files_latest_history_json_and_retention(tmp_path):
    service = HealthReportService()
    old = tmp_path / "20000101_000000.json"
    old.write_text("{}", encoding="utf-8")
    old_time = (datetime.now() - timedelta(days=60)).timestamp()
    os.utime(old, (old_time, old_time))

    written = service.write_report(report(), output_directory=tmp_path, retention_days=30)

    assert (tmp_path / "latest.json").exists()
    assert written["files"]["history_report"]
    assert len(list(tmp_path.glob("*.json"))) == 2
    json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))
    assert not old.exists()


def test_no_operational_market_action_fields_in_report():
    data = report()

    assert check(data, "NO_TRADING_LOGIC")["status"] == "PASS"
    serialized = json.dumps(data).lower()
    assert '"entry"' not in serialized
    assert '"stop"' not in serialized
    assert '"target"' not in serialized
