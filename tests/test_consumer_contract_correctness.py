from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.services.ai_trader_consumer_v2_service import (
    _macro,
    build_ai_trader_consumer_v2,
)
from app.services.market_context_hardening_service import harden_market_context
from app.services.risk_context_normalization_service import normalize_put_call


NOW = datetime(2026, 7, 11, 12, tzinfo=UTC)
MEETING_FIELDS = (
    "meeting_id",
    "meeting_date",
    "meeting_time_utc",
    "expected_target_midpoint",
    "expected_change_bps",
    "cut_probability",
    "hold_probability",
    "hike_probability",
    "most_likely_target_range",
    "most_likely_probability",
    "probability_semantics",
    "is_single_meeting_action_probability",
    "source",
    "freshness",
    "outcomes",
)


def base_full() -> dict:
    return {
        "symbol": "MNQ",
        "generated_at_utc": NOW.isoformat(),
        "macro_snapshot": {
            "rates_and_yields": {
                "DGS2": {"value": 4.1, "frequency": "daily", "data_as_of": "2026-07-10", "source": "FRED"},
                "DGS10": {"value": 4.5, "frequency": "daily", "data_as_of": "2026-07-10", "source": "FRED"},
            },
            "financial_conditions": {"VIXCLS": {"value": 16.0, "frequency": "daily", "source": "FRED"}},
        },
        "event_calendar": {"critical_macro_events": [], "fed_communications": [], "other_economic_events": []},
        "events_today": [],
        "event_windows": {"active": [], "upcoming": []},
        "nasdaq_context": {
            "status": "available",
            "qqq_holdings": {"status": "found", "holdings_count": 104, "holdings": [{"symbol": "NVDA", "weight": 9.0}]},
            "earnings": {"upcoming": []},
            "weight_quality": {"weight_quality_score": 0.81},
        },
        "risk_context": {
            "status": "COMPLETE",
            "vix": {"status": "found", "value": 16.0},
            "vvix": {"status": "found", "value": 90.0},
            "skew": {"status": "found", "value": 145.0},
            "vix_term_structure": {"status": "found", "contracts": []},
            "put_call": {"ratios": [], "by_id": {}},
            "quality": {"quality_score": 0.82},
        },
        "rates_expectations": {"status": "available", "meetings": [], "quality": {"quality_score": 0.79}},
        "news_context": {"latest": []},
        "news_digest": {"drivers": []},
        "positioning": {"status": "available"},
        "sentiment_context": {},
        "social_sentiment": {},
        "market_schedule": {"holidays": []},
        "data_quality": {"section_quality": {"macro_snapshot": {"completeness_score": 0.95}}, "news_pipeline": {}},
        "metadata": {"multi_source_runtime": {"refresh_mode": "false", "provider_calls": 0, "cache_used": True}},
    }


def meeting() -> dict:
    return {
        "meeting_id": "fomc-1",
        "meeting_date": "2026-07-29",
        "meeting_time_utc": "2026-07-29T18:00:00Z",
        "expected_target_midpoint": 3.7,
        "expected_change_bps": 7.5,
        "cut_probability": 0.1,
        "hold_probability": 0.6,
        "hike_probability": 0.3,
        "most_likely_target_range": "3.50-3.75",
        "most_likely_probability": 0.6,
        "source": "Fed monitor",
        "freshness": "RECENT",
        "outcomes": [{"target_lower_bound": 3.5, "target_upper_bound": 3.75, "probability": 0.6}],
    }


def consumer(full: dict) -> dict:
    hardened = harden_market_context(full, settings=Settings(_env_file=None), now=NOW)
    return build_ai_trader_consumer_v2(hardened, settings=Settings(_env_file=None))


@pytest.mark.parametrize("field", MEETING_FIELDS)
def test_next_meeting_is_canonical_first_meeting_projection(field: str) -> None:
    full = base_full()
    full["rates_expectations"]["meetings"] = [meeting()]
    full["rates_expectations"]["next_meeting"] = {"meeting_id": "stale-summary"}
    rates = consumer(full)["rates"]
    assert rates["next_meeting"][field] == rates["meetings"][0][field]


def test_next_meeting_is_null_without_meetings() -> None:
    assert consumer(base_full())["rates"]["next_meeting"] is None


def test_next_meeting_projection_is_exactly_equal() -> None:
    full = base_full()
    full["rates_expectations"]["meetings"] = [meeting()]
    rates = consumer(full)["rates"]
    assert rates["next_meeting"] == rates["meetings"][0]


def current_ratio(value: float = 1.0, observed: str = "2026-07-10") -> dict:
    return {
        "ratio_id": "equity_volume_put_call",
        "scope": "equity",
        "basis": "volume",
        "ratio": value,
        "data_as_of": observed,
    }


def history(values: list[tuple[str, float]]) -> list[dict]:
    return [
        {"put_call": {"by_id": {"equity_volume_put_call": {"ratio": value, "data_as_of": observed}}}}
        for observed, value in values
    ]


def normalized_ratio(values: list[tuple[str, float]], *, current: float = 1.0, history_min: int = 60) -> dict:
    result = normalize_put_call(
        [current_ratio(current)],
        qqq_options={},
        snapshot_history=history(values),
        history_min=history_min,
        now=NOW,
    )
    return result["by_id"]["equity_volume_put_call"]


@pytest.mark.parametrize(
    "field",
    ("change_1d", "change_5d", "moving_average_5d", "moving_average_20d", "percentile_1y", "z_score_1y"),
)
def test_one_snapshot_has_null_statistics(field: str) -> None:
    ratio = normalized_ratio([])
    assert ratio[field] is None
    assert ratio["history_depth"] == 1
    assert ratio["history_status"] == "INSUFFICIENT"


def test_two_distinct_snapshots_calculate_real_zero_change() -> None:
    ratio = normalized_ratio([("2026-07-09", 1.0)])
    assert ratio["change_1d"] == 0.0
    assert ratio["history_depth"] == 2


def test_five_distinct_snapshots_calculate_five_day_average() -> None:
    ratio = normalized_ratio([("2026-07-09", 0.9), ("2026-07-08", 0.8), ("2026-07-07", 0.7), ("2026-07-06", 0.6)])
    assert ratio["moving_average_5d"] == 0.8


def test_change_5d_requires_observation_at_least_five_days_old() -> None:
    ratio = normalized_ratio([("2026-07-09", 0.9), ("2026-07-05", 0.5)])
    assert ratio["change_5d"] == 0.5


def test_duplicate_observation_dates_do_not_increase_history_depth() -> None:
    ratio = normalized_ratio([("2026-07-09", 0.9), ("2026-07-09", 0.8)])
    assert ratio["history_depth"] == 2
    assert ratio["change_1d"] == 0.1


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        ({"event_id": "release", "impact": "HIGH", "release_at_utc": "2026-07-14T12:30:00Z"}, "2026-07-14T12:30:00Z"),
        ({"event_id": "date-time", "impact": "HIGH", "date": "2026-07-14", "time_utc": "12:30:00"}, "2026-07-14T12:30:00Z"),
    ],
)
def test_scheduled_event_forms_are_included_in_windows(event: dict, expected: str) -> None:
    full = base_full()
    full["event_windows"]["upcoming"] = [event]
    projected = consumer(full)["event_risk"]
    assert projected["upcoming_high_impact_windows"][0]["release_at"] == expected


def test_unscheduled_event_is_preserved_outside_windows() -> None:
    full = base_full()
    full["event_windows"]["upcoming"] = [{"event_id": "unknown", "event_name": "High event", "impact": "HIGH", "source": "Calendar"}]
    projected = consumer(full)["event_risk"]
    assert projected["upcoming_high_impact_windows"] == []
    assert projected["upcoming_high_impact_events_unscheduled"][0]["schedule_status"] == "UNSCHEDULED"
    assert projected["event_risk_window_status"] == "NO_ACTIVE_WINDOW"
    assert "high_impact_events_present_but_unscheduled" in projected["warnings"]


def test_next_critical_event_never_uses_unscheduled_event() -> None:
    full = base_full()
    full["event_calendar"]["critical_macro_events"] = [
        {"event_id": "missing", "impact": "HIGH"},
        {"event_id": "cpi", "impact": "HIGH", "date": "2026-07-14", "time_utc": "2026-07-14T12:30:00Z"},
    ]
    assert consumer(full)["event_risk"]["next_critical_event"]["event_id"] == "cpi"


@pytest.mark.parametrize(
    ("source_status", "expected"),
    [
        ("found", "AVAILABLE"),
        ("not_found", "NO_RELEVANT_MARKETS"),
        ("not_configured", "NOT_CONFIGURED"),
        ("provider_unavailable", "PROVIDER_UNAVAILABLE"),
        ("ssl_error", "SSL_ERROR"),
        ("pipeline_error", "PIPELINE_ERROR"),
    ],
)
def test_prediction_market_status_is_preserved(source_status: str, expected: str) -> None:
    full = base_full()
    full["sentiment_context"]["prediction_markets"] = {"status": source_status, "source": "Prediction provider"}
    hardened = harden_market_context(full, settings=Settings(_env_file=None), now=NOW)
    assert hardened["sentiment_context"]["prediction_markets"]["status"] == expected
    assert hardened["readiness"]["section_status"]["prediction_markets"] == expected


def test_no_relevant_prediction_markets_are_non_blocking() -> None:
    full = base_full()
    full["sentiment_context"]["prediction_markets"] = {"status": "not_found"}
    hardened = harden_market_context(full, settings=Settings(_env_file=None), now=NOW)
    assert hardened["readiness"]["ready_for_trading_context"] is True
    assert "prediction_markets_required" not in hardened["readiness"]["blocking_reasons"]


def test_weekend_expected_news_does_not_block_trading_but_missing_sentiment_blocks_full() -> None:
    readiness = consumer(base_full())["readiness"]
    assert readiness["ready_for_trading_context"] is True
    assert readiness["ready_for_full_analysis"] is False
    assert readiness["section_status"]["news_context"] == "NO_DATA_EXPECTED"


def test_quality_weighted_confidence_is_not_artificially_one() -> None:
    readiness = consumer(base_full())["readiness"]
    assert 0 < readiness["full_analysis_confidence"] < readiness["available_data_confidence"] < 1
    assert readiness["confidence"] == readiness["available_data_confidence"]


def test_all_sections_available_enable_full_analysis() -> None:
    full = base_full()
    full["news_context"] = {"latest": [{"article_id": "today", "published_at": "2026-07-11T10:00:00Z"}]}
    full["nasdaq_context"]["earnings"] = {"upcoming": [{"symbol": "NVDA", "date": "2026-07-20"}]}
    full["sentiment_context"] = {"aaii": {"status": "found"}, "prediction_markets": {"status": "found"}}
    full["social_sentiment"] = {"status": "found"}
    assert consumer(full)["readiness"]["ready_for_full_analysis"] is True


def test_provider_failure_reduces_full_analysis_confidence() -> None:
    available = base_full()
    available["sentiment_context"] = {"aaii": {"status": "found"}, "prediction_markets": {"status": "found"}}
    failed = deepcopy(available)
    failed["sentiment_context"]["prediction_markets"] = {"status": "provider_unavailable"}
    assert consumer(failed)["readiness"]["full_analysis_confidence"] < consumer(available)["readiness"]["full_analysis_confidence"]


def test_news_drivers_are_split_by_context_date() -> None:
    full = base_full()
    full["news_digest"]["drivers"] = [
        {"driver_id": "current", "published_at_latest": "2026-07-11T10:00:00Z"},
        {"driver_id": "previous", "published_at_latest": "2026-07-10T20:00:00Z"},
    ]
    news = consumer(full)["news"]
    assert news["current_drivers"][0]["context_classification"] == "CURRENT_DAY"
    assert news["previous_session_drivers"][0]["context_classification"] == "PREVIOUS_SESSION"
    assert news["previous_session_drivers"][0]["usable_for_current_news_analysis"] is False
    assert news["accepted_article_count"] == 0


def test_historical_news_driver_is_excluded() -> None:
    full = base_full()
    full["news_digest"]["drivers"] = [{"driver_id": "old", "published_at_latest": "2026-07-01T10:00:00Z"}]
    news = consumer(full)["news"]
    assert news["current_drivers"] == []
    assert news["previous_session_drivers"] == []


@pytest.mark.parametrize(
    ("series", "frequency", "policy_ref"),
    [
        ("2Y", "daily", "daily_market"),
        ("NFCI", "weekly", "weekly_release"),
        ("CPI", "monthly", "monthly_release"),
        ("GDP", "quarterly", "quarterly_release"),
        ("Fed target lower", "daily", "fed_target"),
    ],
)
def test_macro_series_lifecycle_uses_compact_policy_reference(series: str, frequency: str, policy_ref: str) -> None:
    snapshot = {
        "rates_and_yields": {
            "DGS2": {"value": 4.1, "frequency": "daily"},
            "DFEDTARL": {"value": 3.5, "frequency": "daily"},
        },
        "financial_conditions": {"NFCI": {"value": -0.5, "frequency": "weekly"}},
        "growth": {"BEA:GDP": {"value": 100, "frequency": "quarterly"}},
        "inflation": {"CUSR0000SA0": {"value": 300, "frequency": "monthly"}},
    }
    macro = _macro(snapshot)
    lifecycle = macro["series_lifecycle"][series]
    assert lifecycle["frequency"] == frequency
    assert lifecycle["policy_ref"] == policy_ref
    assert lifecycle["lifecycle_status"] == "POLICY_DEFINED_DATE_UNKNOWN"
    assert macro["series_lifecycle_policies"][policy_ref]["refresh_policy"]


def test_known_macro_dates_are_exposed_without_duplication() -> None:
    macro = _macro({"rates_and_yields": {"DGS2": {"value": 4.1, "frequency": "daily", "valid_until": "2026-07-12T00:00:00Z", "next_refresh_at": "2026-07-12T00:00:00Z"}}})
    lifecycle = macro["series_lifecycle"]["2Y"]
    assert lifecycle["lifecycle_status"] == "KNOWN"
    assert lifecycle["valid_until"] == lifecycle["next_refresh_at"]


def test_consumer_payload_stays_below_preferred_95kb() -> None:
    payload = consumer(base_full())
    assert len(json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")) <= 95_000


def test_cache_only_materialization_is_stable_across_restart_equivalent_rebuild() -> None:
    first = consumer(base_full())
    second = consumer(base_full())
    for payload in (first, second):
        payload.pop("generated_at", None)
        payload.get("snapshot_summary", {}).pop("generated_at", None)
    assert first == second


def test_lifecycle_without_source_timestamp_is_stable_within_context_date() -> None:
    morning = harden_market_context(base_full(), settings=Settings(_env_file=None), now=NOW)
    evening = harden_market_context(base_full(), settings=Settings(_env_file=None), now=NOW + timedelta(hours=8))
    assert morning["metadata"]["data_lifecycle"]["earnings"] == evening["metadata"]["data_lifecycle"]["earnings"]
    assert morning["metadata"]["data_lifecycle"]["macro_actual"] == evening["metadata"]["data_lifecycle"]["macro_actual"]
