from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from app.core.config import Settings
from app.main import app
from app.services.ai_trader_consumer_v2_service import (
    EXCLUDED_DEBUG_SECTIONS,
    INCLUDED_SECTIONS,
    build_ai_trader_consumer_v2,
)
from app.services.fed_expectations_service import build_fed_sanity_check
from app.services.market_context_hardening_service import (
    apply_news_semantics,
    classify_freshness,
    classify_semiconductor_contribution,
    deduplicate_issuer_earnings,
    harden_market_context,
)
from app.services.market_session_service import build_session_aware_schedule
from app.services.news_intelligence_service import classify_news_source, normalize_news_article


NY = ZoneInfo("America/New_York")
SATURDAY = datetime(2026, 7, 11, 12, tzinfo=UTC)


def minimal_full(*, now: datetime = SATURDAY) -> dict:
    return {
        "symbol": "MNQ",
        "generated_at_utc": now.isoformat(),
        "macro_snapshot": {
            "rates_and_yields": {
                "DGS2": {"value": 3.5, "data_as_of": "2026-07-10", "source": "FRED", "frequency": "daily"},
                "DGS10": {"value": 4.0, "data_as_of": "2026-07-10", "source": "FRED", "frequency": "daily"},
            },
            "financial_conditions": {"VIXCLS": {"value": 16.0, "data_as_of": "2026-07-10", "source": "FRED"}},
        },
        "event_calendar": {"critical_macro_events": [], "fed_communications": [], "other_economic_events": []},
        "events_today": [],
        "event_windows": {"active": [], "upcoming": []},
        "nasdaq_context": {
            "status": "available",
            "qqq_holdings": {
                "status": "found",
                "holdings_count": 104,
                "holdings": [
                    {"symbol": "NVDA", "name": "NVIDIA", "weight": 9.0, "sector": "Information Technology"},
                    {"symbol": "GOOGL", "name": "Alphabet A", "weight": 3.0, "sector": "Communication Services"},
                    {"symbol": "GOOG", "name": "Alphabet C", "weight": 2.8, "sector": "Communication Services"},
                ],
                "weight_method": "reconstructed_unadjusted_market_cap_weight",
                "weight_verified": True,
                "weight_is_official": False,
            },
            "semiconductor_context": {
                "semiconductor_net_contribution": 0.02,
                "semiconductor_positive_contribution": 0.03,
                "semiconductor_negative_contribution": -0.01,
            },
            "earnings": {
                "upcoming": [
                    {"symbol": "GOOG", "date": "2026-07-22", "source": "Nasdaq"},
                    {"symbol": "GOOGL", "date": "2026-07-22", "source": "Nasdaq"},
                ]
            },
        },
        "risk_context": {
            "status": "COMPLETE",
            "vix": {"status": "found", "value": 16.0},
            "vvix": {"status": "found", "value": 88.0},
            "skew": {"status": "found", "value": 145.0},
            "vix_term_structure": {
                "status": "found",
                "structure": "CONTANGO",
                "contracts": [
                    {"contract_symbol": "VX/N6", "expiration_date": "2026-07-22", "last_price": 17.0},
                    {"contract_symbol": "VX/Q6", "expiration_date": "2026-08-19", "last_price": 18.0},
                    {"contract_symbol": "VX/U6", "expiration_date": "2026-09-16", "last_price": 19.0},
                ],
            },
            "put_call": {"by_id": {}},
            "quality": {"quality_score": 0.9},
        },
        "rates_expectations": {
            "status": "available",
            "current_fed_state": {
                "current_target_lower_bound": 4.25,
                "current_target_upper_bound": 4.5,
                "current_target_midpoint": 4.375,
            },
            "meetings": [],
            "quality": {"quality_score": 0.75},
            "source_summary": {"ranking_class": "secondary_monitor"},
        },
        "news_context": {"latest": [], "diagnostics": {"raw_article_count": 10, "excluded_count": 10}},
        "positioning": {},
        "sentiment_context": {},
        "social_sentiment": {},
        "market_schedule": {"holidays": []},
        "data_quality": {
            "news_pipeline": {"fetched_count": 10, "provider_success_count": 1},
            "pipeline_integrity": {"snapshot_built_from_db": True},
            "section_quality": {"macro_snapshot": {"completeness_score": 0.9}},
        },
        "metadata": {"multi_source_runtime": {"refresh_mode": "false", "provider_calls": 0, "cache_used": True}},
    }


@pytest.mark.parametrize("hour", range(24))
@pytest.mark.parametrize("day", [11, 12, 13])
def test_cash_and_mnq_session_matrix_for_weekend_and_monday(day: int, hour: int) -> None:
    local = datetime(2026, 7, day, hour, tzinfo=NY)
    schedule = build_session_aware_schedule({}, now=local)
    cash = schedule["nasdaq_cash_session"]["status"]
    futures = schedule["mnq_session"]["status"]
    if day == 11:
        assert cash == "weekend"
        assert futures == "weekend"
    elif day == 12:
        assert cash == "weekend"
        assert futures == ("open" if hour >= 18 else "weekend")
    else:
        assert cash == ("open" if 10 <= hour < 16 else "market_closed")
        assert futures == ("maintenance_break" if hour == 17 else "open")


@pytest.mark.parametrize(
    ("session", "payload", "expected"),
    [
        ("weekend", {}, "MARKET_CLOSED_NO_FRESH_NEWS"),
        ("holiday", {}, "MARKET_CLOSED_NO_FRESH_NEWS"),
        ("market_closed", {}, "MARKET_CLOSED_NO_FRESH_NEWS"),
        ("open", {}, "NO_RELEVANT_NEWS"),
        ("open", {"provider_failure_count": 2}, "PROVIDER_UNAVAILABLE"),
        ("open", {"errors": ["boom"]}, "PIPELINE_ERROR"),
        ("open", {"configured": False}, "NOT_CONFIGURED"),
        ("open", {"latest": [{"article_id": "a", "published_at": "2026-07-11T11:00:00Z"}]}, "AVAILABLE"),
        ("open", {"latest": [{"article_id": "a", "published_at": "2026-07-11T11:00:00Z"}], "errors": ["partial"]}, "PARTIAL"),
    ],
)
def test_news_semantic_status_matrix(session: str, payload: dict, expected: str) -> None:
    result = apply_news_semantics(
        payload,
        pipeline={},
        market_schedule={"market_session_status": session, "last_market_session_date": "2026-07-10"},
        settings=Settings(_env_file=None),
        now=SATURDAY,
    )
    assert result["status"] == expected
    assert result["blocking"] is False
    assert result["search_completed"] is (expected not in {"PROVIDER_UNAVAILABLE", "PIPELINE_ERROR", "NOT_CONFIGURED"})


@pytest.mark.parametrize(
    ("field", "value", "expected_source"),
    [
        ("json_ld", {"datePublished": "2026-07-11T10:00:00Z"}, "json_ld"),
        ("opengraph", {"article:published_time": "2026-07-11T10:00:00Z"}, "opengraph"),
        ("article_metadata", {"published_at": "2026-07-11T10:00:00Z"}, "article_metadata"),
        ("rss_published_at", "2026-07-11T10:00:00Z", "rss"),
        ("api_published_at", "2026-07-11T10:00:00Z", "structured_api"),
        ("published_at", "2026-07-11T10:00:00Z", "provider_timestamp"),
        ("aggregator_published_at", "2026-07-11T10:00:00Z", "aggregator_timestamp"),
        ("source_page_published_at", "2026-07-11T10:00:00Z", "source_page"),
        ("retrieved_at", "2026-07-11T10:00:00Z", "retrieved_at_fallback"),
    ],
)
def test_news_timestamp_recovery_priority_fields(field: str, value: object, expected_source: str) -> None:
    raw = {
        "title": "Federal Reserve policy update",
        "summary": "The Federal Reserve published a policy update for financial markets.",
        "source": "Reuters",
        "source_url": "https://reuters.test/markets/fed-update",
        "retrieved_at": None,
        field: value,
    }
    item = normalize_news_article(raw, now=SATURDAY)
    assert item["published_at_source"] == expected_source
    assert item["published_at"] is not None
    assert item["timestamp_status"] in {"VERIFIED", "INFERRED"}


def test_news_timestamp_recovery_from_url_is_inferred() -> None:
    item = normalize_news_article(
        {
            "title": "Federal Reserve policy update",
            "summary": "Federal Reserve officials discussed rates and inflation.",
            "source": "Reuters",
            "source_url": "https://reuters.test/2026/07/11/fed-update",
            "retrieved_at": None,
        },
        now=SATURDAY,
    )
    assert item["published_at_source"] == "url_date"
    assert item["timestamp_inferred"] is True
    assert item["published_at_verified"] is False


@pytest.mark.parametrize(
    ("source", "url", "tier"),
    [
        ("Reuters", "https://reuters.com/a", 1),
        ("Associated Press", "https://apnews.com/a", 1),
        ("Federal Reserve", "https://federalreserve.gov/a", 1),
        ("BLS", "https://bls.gov/a", 1),
        ("BEA", "https://bea.gov/a", 1),
        ("Cboe", "https://cboe.com/a", 1),
        ("Nasdaq Official", "https://nasdaq.com/market-activity/a", 1),
        ("CNBC", "https://cnbc.com/a", 2),
        ("Wall Street Journal", "https://wsj.com/a", 2),
        ("MarketBeat", "https://marketbeat.com/a", 3),
    ],
)
def test_news_source_ranking_tiers(source: str, url: str, tier: int) -> None:
    assert classify_news_source({"source": source, "source_url": url, "title": "Market update"})["source_tier"] == tier


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Best CD rates today", "deposit_rates"),
        ("Best high yield savings rates", "deposit_rates"),
        ("Mortgage refinancing rates", "mortgage"),
        ("HELOC guide for homeowners", "personal_finance"),
        ("Retirement planning guide", "personal_finance"),
        ("Analyst reiterates Nvidia price target", "analyst_rating_only"),
        ("10 AI stocks to buy now", "low_relevance"),
        ("The best growth stocks for your portfolio", "low_relevance"),
    ],
)
def test_news_noise_exclusion_reasons(title: str, expected: str) -> None:
    item = normalize_news_article(
        {
            "title": title,
            "summary": title,
            "source": "MarketBeat",
            "source_url": "https://marketbeat.test/article",
            "published_at": "2026-07-11T10:00:00Z",
            "retrieved_at": "2026-07-11T10:05:00Z",
        },
        now=SATURDAY,
    )
    assert item["exclusion_reason"] == expected


@pytest.mark.parametrize("missing", ["macro_snapshot", "event_risk", "market_schedule", "risk_context", "nasdaq_context"])
def test_each_critical_readiness_section_blocks(missing: str) -> None:
    full = minimal_full()
    if missing == "event_risk":
        full["data_quality"]["event_pipeline"] = {"errors": ["calendar failed"]}
    else:
        full[missing] = {}
    hardened = harden_market_context(full, settings=Settings(_env_file=None), now=SATURDAY)
    if missing == "market_schedule":
        assert hardened["readiness"]["ready_for_trading_context"] is True
        assert "official_cme_calendar_crosscheck_unavailable" in hardened["market_schedule"]["warnings"]
        return
    assert hardened["readiness"]["ready_for_trading_context"] is False
    assert any(missing in reason for reason in hardened["readiness"]["blocking_reasons"])


@pytest.mark.parametrize(
    ("field", "section"),
    [
        ("readiness_require_news", "news_context"),
        ("readiness_require_rates", "rates_expectations"),
        ("readiness_require_positioning", "positioning"),
        ("readiness_require_sentiment", "sentiment"),
        ("readiness_require_prediction_markets", "prediction_markets"),
    ],
)
def test_optional_readiness_configuration_can_make_section_required(field: str, section: str) -> None:
    full = minimal_full()
    if section == "rates_expectations":
        full["rates_expectations"] = {}
    cfg = Settings(_env_file=None, **{field: True})
    hardened = harden_market_context(full, settings=cfg, now=SATURDAY)
    assert f"{section}_required" in hardened["readiness"]["blocking_reasons"]


@pytest.mark.parametrize(
    ("frequency", "age", "session", "expected"),
    [
        ("daily", timedelta(minutes=5), "open", "LIVE"),
        ("daily", timedelta(hours=2), "open", "RECENT"),
        ("daily", timedelta(days=1), "open", "LAST_SESSION"),
        ("daily", timedelta(days=2), "weekend", "LAST_SESSION"),
        ("daily", timedelta(days=8), "open", "STALE"),
        ("daily", timedelta(days=20), "open", "VERY_STALE"),
        ("weekly", timedelta(days=7), "open", "CURRENT_RELEASE"),
        ("weekly", timedelta(days=20), "open", "STALE"),
        ("monthly", timedelta(days=30), "open", "CURRENT_RELEASE"),
        ("monthly", timedelta(days=70), "open", "STALE"),
        ("quarterly", timedelta(days=100), "open", "CURRENT_RELEASE"),
        ("quarterly", timedelta(days=300), "open", "VERY_STALE"),
    ],
)
def test_freshness_frequency_matrix(frequency: str, age: timedelta, session: str, expected: str) -> None:
    now = datetime(2026, 7, 11, 12, tzinfo=UTC)
    assert classify_freshness(data_as_of=now - age, frequency=frequency, session_status=session, now=now) == expected


@pytest.mark.parametrize(
    ("net", "positive", "negative", "expected"),
    [
        (0.02, 0.03, -0.01, "POSITIVE_CONTRIBUTION"),
        (-0.02, 0.01, -0.03, "NEGATIVE_CONTRIBUTION"),
        (0.0, 0.02, -0.02, "MIXED"),
        (0.0, 0.0, 0.0, "FLAT"),
        (None, 0.0, 0.0, "UNKNOWN"),
    ],
)
def test_semiconductor_classification_matrix(net: float | None, positive: float, negative: float, expected: str) -> None:
    context = {
        "semiconductor_net_contribution": net,
        "semiconductor_positive_contribution": positive,
        "semiconductor_negative_contribution": negative,
    }
    assert classify_semiconductor_contribution(context) == expected


@pytest.mark.parametrize("symbols", [["GOOG", "GOOGL"], ["GOOGL", "GOOG"], ["GOOG"], ["GOOGL"], ["AAPL", "MSFT"]])
def test_earnings_are_aggregated_at_issuer_level(symbols: list[str]) -> None:
    events = [{"symbol": symbol, "date": "2026-07-22"} for symbol in symbols]
    result = deduplicate_issuer_earnings(events)
    expected_count = 1 if set(symbols).issubset({"GOOG", "GOOGL"}) else len(symbols)
    assert len(result) == expected_count
    if set(symbols).issubset({"GOOG", "GOOGL"}):
        assert result[0]["issuer_name"] == "Alphabet Inc."
        assert result[0]["symbols"] == sorted(set(symbols))


def fed_snapshot(*, ranking: str = "secondary_monitor") -> dict:
    return {
        "current_fed_state": {
            "current_target_lower_bound": 4.25,
            "current_target_upper_bound": 4.5,
            "current_target_midpoint": 4.375,
        },
        "source_summary": {"ranking_class": ranking},
        "meetings": [
            {
                "meeting_date": "2026-07-29",
                "outcomes": [{"target_midpoint": 4.375, "probability": 1.0}],
                "expected_target_midpoint": 4.375,
                "expected_change_bps": 0.0,
                "future_price": 95.625,
                "validation": {"meeting_date_match": True},
            }
        ],
    }


@pytest.mark.parametrize(
    ("mutation", "ranking", "expected"),
    [
        (None, "official_futures_derived_complete", "PASS"),
        (None, "secondary_monitor", "WARN"),
        (("outcomes", [{"target_midpoint": 4.375, "probability": 0.8}]), "official_futures_derived_complete", "FAIL"),
        (("expected_target_midpoint", 4.0), "official_futures_derived_complete", "FAIL"),
        (("expected_change_bps", 25.0), "official_futures_derived_complete", "FAIL"),
        (("future_price", 90.0), "official_futures_derived_complete", "FAIL"),
        (("validation", {"meeting_date_match": False}), "official_futures_derived_complete", "FAIL"),
    ],
)
def test_fed_sanity_matrix(mutation: tuple[str, object] | None, ranking: str, expected: str) -> None:
    snapshot = fed_snapshot(ranking=ranking)
    if mutation:
        snapshot["meetings"][0][mutation[0]] = mutation[1]
    sanity = build_fed_sanity_check(snapshot, macro_snapshot=minimal_full()["macro_snapshot"])
    assert sanity["status"] == expected
    assert sanity["probability_semantics"] == "probability_target_range_after_meeting_relative_to_current_range"
    assert sanity["is_single_meeting_action_probability"] is False


@pytest.mark.parametrize("section", INCLUDED_SECTIONS)
def test_consumer_v2_contains_each_required_section(section: str) -> None:
    consumer = build_ai_trader_consumer_v2(minimal_full(), settings=Settings(_env_file=None))
    assert section in consumer


@pytest.mark.parametrize("section", EXCLUDED_DEBUG_SECTIONS)
def test_consumer_v2_does_not_materialize_debug_section(section: str) -> None:
    consumer = build_ai_trader_consumer_v2(minimal_full(), settings=Settings(_env_file=None))
    assert section not in consumer


def test_consumer_v2_contract_name_schema_and_payload_size() -> None:
    consumer = build_ai_trader_consumer_v2(minimal_full(), settings=Settings(_env_file=None))
    assert consumer["contract"] == "ai_trader_market_context_consumer"
    assert consumer["schema_version"] == "2.0"
    assert consumer["payload_view"] == "consumer"
    assert len(json.dumps(consumer, default=str).encode()) < 100_000
    assert consumer["snapshot_summary"]["consumer_payload_under_100kb"] is True


def test_consumer_v2_limits_holdings_to_twenty() -> None:
    full = minimal_full()
    full["nasdaq_context"]["qqq_holdings"]["holdings"] = [
        {"symbol": f"S{i}", "weight": 1.0} for i in range(103)
    ]
    consumer = build_ai_trader_consumer_v2(full, settings=Settings(_env_file=None))
    assert len(consumer["nasdaq"]["top_20_holdings"]) == 20
    assert "holdings" not in consumer["nasdaq"]


def test_consumer_v2_has_no_trading_decision_fields() -> None:
    encoded = json.dumps(build_ai_trader_consumer_v2(minimal_full(), settings=Settings(_env_file=None))).lower()
    for forbidden in ("trade_score", "long_bias", "short_bias", "entry_signal", "position_size", "stop_loss", "take_profit", "order_request"):
        assert forbidden not in encoded


def test_weekend_zero_news_and_events_is_ready() -> None:
    hardened = harden_market_context(minimal_full(), settings=Settings(_env_file=None), now=SATURDAY)
    assert hardened["news_context"]["status"] == "MARKET_CLOSED_NO_FRESH_NEWS"
    assert hardened["events_today_context"]["status"] == "NO_EVENTS_SCHEDULED"
    assert hardened["readiness"]["ready_for_trading_context"] is True
    assert hardened["readiness"]["critical_errors"] == []


def test_market_open_zero_news_is_degraded_but_ready() -> None:
    open_time = datetime(2026, 7, 13, 14, tzinfo=UTC)
    hardened = harden_market_context(minimal_full(now=open_time), settings=Settings(_env_file=None), now=open_time)
    assert hardened["news_context"]["status"] == "NO_RELEVANT_NEWS"
    assert hardened["readiness"]["status"] == "DEGRADED"
    assert hardened["readiness"]["ready_for_trading_context"] is True


def test_news_provider_failure_is_distinct_from_zero_results() -> None:
    full = minimal_full()
    full["news_context"] = {"provider_failure_count": 4}
    full["data_quality"]["news_pipeline"] = {"provider_failure_count": 4}
    hardened = harden_market_context(full, settings=Settings(_env_file=None), now=SATURDAY)
    assert hardened["news_context"]["status"] == "PROVIDER_UNAVAILABLE"
    assert hardened["news_context"]["search_completed"] is False
    assert "news_provider_unavailable" in hardened["readiness"]["degrading_reasons"]


def test_nfp_period_mapping_fields_are_explicit() -> None:
    full = minimal_full()
    full["event_calendar"]["critical_macro_events"] = [
        {"event_id": "nfp", "name": "Employment Situation (July 2026)", "category": "NFP", "date": "2026-08-07"}
    ]
    event = harden_market_context(full, settings=Settings(_env_file=None), now=SATURDAY)["event_calendar"]["critical_macro_events"][0]
    assert event["release_period"] == "July 2026"
    assert event["period_date_consistent"] is True
    assert event["invalid_period_mapping"] is False


def test_official_origin_and_redistributor_semantics_are_separate() -> None:
    hardened = harden_market_context(minimal_full(), settings=Settings(_env_file=None), now=SATURDAY)
    dgs2 = hardened["macro_snapshot"]["rates_and_yields"]["DGS2"]
    assert dgs2["data_origin_is_official"] is True
    assert dgs2["distribution_source_is_official"] is True
    assert dgs2["source_is_primary_originator"] is False
    assert dgs2["source_is_official_redistributor"] is True


def test_nasdaq_weight_validation_is_not_official_verification() -> None:
    hardened = harden_market_context(minimal_full(), settings=Settings(_env_file=None), now=SATURDAY)
    qqq = hardened["nasdaq_context"]["qqq_holdings"]
    assert qqq["weight_calculation_validated"] is True
    assert qqq["official_weight_verified"] is False
    assert qqq["weight_method_classification"] == "reconstructed_market_cap_proxy"


def test_materialization_flags_reflect_valid_empty_sections() -> None:
    consumer = build_ai_trader_consumer_v2(minimal_full(), settings=Settings(_env_file=None))
    flags = consumer["snapshot_summary"]["materialization"]
    assert flags["snapshot_built_from_db"] is True
    assert flags["snapshot_materialization_completed"] is True
    assert flags["snapshot_serialization_completed"] is True
    assert flags["snapshot_contract_validation_completed"] is True
    assert flags["consumer_materialization_completed"] is True


def test_cache_only_runtime_io_is_explicitly_zero() -> None:
    consumer = build_ai_trader_consumer_v2(minimal_full(), settings=Settings(_env_file=None))
    runtime = consumer["snapshot_summary"]["runtime_io"]
    assert runtime["provider_calls"] == 0
    assert runtime["actual_network_calls"] == 0
    assert runtime["browser_calls"] == 0
    assert runtime["AI_called"] is False
    assert runtime["cache_used"] is True


def test_consumer_route_is_registered_separately_from_debug() -> None:
    paths = set(app.openapi()["paths"])
    assert "/market-context/mnq/consumer" in paths
    assert "/market-context/mnq/debug" in paths
