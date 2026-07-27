from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from app.core.config import Settings
from app.services.ai_trader_consumer_v2_service import (
    build_ai_trader_consumer_v2,
)
from app.services.event_calendar_window_service import (
    build_event_calendar_window,
    classify_event_change,
    coalesce_event_changes,
    compact_event_calendar_window,
)
from app.services.market_session_service import build_session_aware_schedule
from scripts.replay_three_week_event_calendar_offline import replay


NY = ZoneInfo("America/New_York")


def cfg(tmp_path: Path, **overrides: object) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "calendar-window.db",
        **overrides,
    )


def event(
    occurrence_id: str,
    scheduled_at: str,
    *,
    impact: str = "HIGH",
    actual: object = None,
    forecast: object = None,
    previous: object = None,
    **extra: object,
) -> dict[str, object]:
    return {
        "event_id": occurrence_id,
        "occurrence_id": occurrence_id,
        "name": occurrence_id,
        "category": "MACRO",
        "country": "US",
        "currency": "USD",
        "impact": impact,
        "release_at": scheduled_at,
        "actual": actual,
        "forecast": forecast,
        "previous": previous,
        "source": "BLS",
        **extra,
    }


def full(*events: dict[str, object]) -> dict[str, object]:
    return {
        "event_calendar": {
            "critical_macro_events": list(events),
            "fed_communications": [],
            "other_economic_events": [],
            "source_coverage": {
                "by_bucket": {
                    bucket: {"status": "VERIFIED_COMPLETE"}
                    for bucket in (
                        "PREVIOUS_WEEK",
                        "CURRENT_WEEK",
                        "NEXT_WEEK",
                    )
                }
            },
        }
    }


def all_events(window: dict[str, object]) -> list[dict[str, object]]:
    return [
        item
        for key in ("previous_week", "current_week", "next_week")
        for item in window[key]["events"]  # type: ignore[index]
    ]


@pytest.mark.parametrize(
    ("now", "expected_current_start", "expected_current_end"),
    [
        (
            datetime(2026, 7, 20, 12, tzinfo=NY),
            "2026-07-20T00:00:00-04:00",
            "2026-07-26T23:59:59-04:00",
        ),
        (
            datetime(2026, 7, 26, 23, 59, tzinfo=NY),
            "2026-07-20T00:00:00-04:00",
            "2026-07-26T23:59:59-04:00",
        ),
        (
            datetime(2026, 8, 1, 12, tzinfo=NY),
            "2026-07-27T00:00:00-04:00",
            "2026-08-02T23:59:59-04:00",
        ),
        (
            datetime(2027, 1, 1, 12, tzinfo=NY),
            "2026-12-28T00:00:00-05:00",
            "2027-01-03T23:59:59-05:00",
        ),
        (
            datetime(2026, 3, 8, 12, tzinfo=NY),
            "2026-03-02T00:00:00-05:00",
            "2026-03-08T23:59:59-04:00",
        ),
        (
            datetime(2026, 11, 1, 12, tzinfo=NY),
            "2026-10-26T00:00:00-04:00",
            "2026-11-01T23:59:59-05:00",
        ),
    ],
)
def test_complete_monday_sunday_bounds_across_calendar_edges(
    tmp_path: Path,
    now: datetime,
    expected_current_start: str,
    expected_current_end: str,
) -> None:
    window = build_event_calendar_window(
        full(),
        settings=cfg(tmp_path),
        now=now,
    )

    assert window["current_week"]["start"] == expected_current_start
    assert window["current_week"]["end"] == expected_current_end
    assert window["previous_week"]["end"][:10] < expected_current_start[:10]
    assert window["next_week"]["start"][:10] > expected_current_end[:10]


def test_bucket_membership_today_past_future_and_outside(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 22, 12, tzinfo=NY)
    window = build_event_calendar_window(
        full(
            event("previous", "2026-07-15T08:30:00-04:00", actual="1"),
            event("current-past", "2026-07-20T08:30:00-04:00", actual="2"),
            event("today", "2026-07-22T14:00:00-04:00"),
            event("current-future", "2026-07-24T10:00:00-04:00"),
            event("next", "2026-07-28T08:30:00-04:00"),
            event("outside", "2026-08-10T08:30:00-04:00"),
        ),
        settings=cfg(tmp_path),
        now=now,
    )

    by_id = {item["occurrence_id"]: item for item in all_events(window)}
    assert by_id["previous"]["week_bucket"] == "PREVIOUS_WEEK"
    assert by_id["current-past"]["week_bucket"] == "CURRENT_WEEK"
    assert by_id["current-past"]["is_past"] is True
    assert by_id["today"]["is_today"] is True
    assert by_id["today"]["is_future"] is True
    assert by_id["current-future"]["week_bucket"] == "CURRENT_WEEK"
    assert by_id["next"]["week_bucket"] == "NEXT_WEEK"
    assert by_id["next"]["actual"] is None
    assert by_id["next"]["release_status"] == "SCHEDULED"
    assert "outside" not in by_id
    assert window["coverage"]["outside_window_count"] == 1


def test_date_without_time_remains_date_only_and_never_invents_utc(
    tmp_path: Path,
) -> None:
    window = build_event_calendar_window(
        full(event("date-only", "2026-07-22")),
        settings=cfg(tmp_path),
        now=datetime(2026, 7, 22, 12, tzinfo=NY),
    )

    item = all_events(window)[0]
    assert item["scheduled_at"] == "2026-07-22"
    assert item["scheduled_at_utc"] is None
    assert item["scheduled_time_precision"] == "DATE"
    assert item["temporal_state"] == "TODAY_TIME_UNKNOWN"
    assert item["release_status"] == "AWAITING_RELEASE"


def test_source_timezone_is_converted_to_new_york_bucket(tmp_path: Path) -> None:
    raw = event(
        "london-source",
        "2026-07-27T00:30:00",
        source_timezone="Europe/London",
    )
    window = build_event_calendar_window(
        full(raw),
        settings=cfg(tmp_path),
        now=datetime(2026, 7, 22, 12, tzinfo=NY),
    )

    item = all_events(window)[0]
    assert item["scheduled_at"] == "2026-07-26T19:30:00-04:00"
    assert item["week_bucket"] == "CURRENT_WEEK"
    assert item["source_timezone"] == "Europe/London"


@pytest.mark.parametrize(
    ("timestamp", "reason"),
    [
        ("2026-03-08T02:30:00", "scheduled_timestamp_nonexistent"),
        ("2026-11-01T01:30:00", "scheduled_timestamp_ambiguous"),
        ("not-a-timestamp", "scheduled_timestamp_invalid"),
    ],
)
def test_invalid_or_ambiguous_local_timestamp_fails_closed(
    tmp_path: Path,
    timestamp: str,
    reason: str,
) -> None:
    window = build_event_calendar_window(
        full(event("bad-time", timestamp, timezone="America/New_York")),
        settings=cfg(tmp_path),
        now=datetime(2026, 7, 22, 12, tzinfo=NY),
    )

    assert all_events(window) == []
    assert window["telemetry"]["temporal_anomaly_count"] == 1
    assert window["audit"]["temporal_anomalies"][0]["reason"] == reason


def test_values_status_surprise_revision_and_real_nulls(tmp_path: Path) -> None:
    window = build_event_calendar_window(
        full(
            event(
                "published",
                "2026-07-21T08:30:00-04:00",
                actual="3.2",
                forecast="3.0",
                previous="2.9",
            ),
            event(
                "revised",
                "2026-07-15T08:30:00-04:00",
                actual="4",
                forecast="5",
                previous="",
                release_status="REVISED",
                revision={"from": "3", "to": "4"},
            ),
            event(
                "missing",
                "2026-07-14T08:30:00-04:00",
                actual="N/A",
                forecast="-",
            ),
        ),
        settings=cfg(tmp_path),
        now=datetime(2026, 7, 22, 12, tzinfo=NY),
    )
    by_id = {item["occurrence_id"]: item for item in all_events(window)}

    assert by_id["published"]["release_status"] == "PUBLISHED"
    assert by_id["published"]["surprise"] == {
        "value": 0.2,
        "direction": "ABOVE",
        "method": "actual_minus_forecast",
    }
    assert by_id["revised"]["release_status"] == "REVISED"
    assert by_id["revised"]["revision"] == {"from": "3", "to": "4"}
    assert by_id["revised"]["previous"] is None
    assert by_id["missing"]["actual"] is None
    assert by_id["missing"]["forecast"] is None
    assert by_id["missing"]["release_status"] == "AWAITING_ACTUAL"


def test_deterministic_ordering_deduplication_limits_and_coverage(
    tmp_path: Path,
) -> None:
    duplicate = event("dup", "2026-07-22T08:30:00-04:00", impact="LOW")
    window = build_event_calendar_window(
        full(
            duplicate,
            {**duplicate, "forecast": "1"},
            event("medium", "2026-07-22T08:30:00-04:00", impact="MEDIUM"),
            event("high", "2026-07-22T08:30:00-04:00", impact="HIGH"),
            event("later", "2026-07-23T08:30:00-04:00", impact="HIGH"),
        ),
        settings=cfg(
            tmp_path,
            event_calendar_consumer_max_events=3,
        ),
        now=datetime(2026, 7, 22, 7, tzinfo=NY),
    )

    ids = [item["occurrence_id"] for item in all_events(window)]
    assert ids == ["high", "medium", "dup", "later"]
    assert window["coverage"]["status"] == "COMPLETE"
    assert window["coverage"]["overflow_count"] == 0
    assert window["telemetry"]["duplicate_occurrence_count"] == 0
    assert window["coverage"]["delivered_valid_source_record_count"] == 5


def test_unscheduled_news_is_never_projected_as_future_event(
    tmp_path: Path,
) -> None:
    payload = full(event("scheduled", "2026-07-22T08:30:00-04:00"))
    payload["event_calendar"]["other_economic_events"].append(
        event(
            "breaking",
            "2026-07-23T08:30:00-04:00",
            event_kind="unscheduled_news",
        )
    )
    payload["event_windows"] = {
        "upcoming_unscheduled": [
            {"event_id": "geopolitical", "impact": "HIGH"}
        ]
    }

    window = build_event_calendar_window(
        payload,
        settings=cfg(tmp_path),
        now=datetime(2026, 7, 22, 7, tzinfo=NY),
    )

    assert [item["occurrence_id"] for item in all_events(window)] == [
        "scheduled"
    ]


def test_earnings_occurrence_is_included_without_inventing_time(
    tmp_path: Path,
) -> None:
    payload = full()
    payload["nasdaq_context"] = {
        "earnings": {
            "upcoming": [
                {
                    "issuer_event_id": "earnings:nvda:2026-07-28",
                    "issuer_name": "NVIDIA",
                    "symbol": "NVDA",
                    "date": "2026-07-28",
                    "impact": "HIGH",
                    "eps_estimate": "1.25",
                    "eps_actual": None,
                    "source": "NASDAQ",
                }
            ]
        }
    }

    window = build_event_calendar_window(
        payload,
        settings=cfg(tmp_path),
        now=datetime(2026, 7, 22, 7, tzinfo=NY),
    )
    item = all_events(window)[0]

    assert item["event_type"] == "EARNINGS"
    assert item["occurrence_id"] == "earnings:nvda:2026-07-28"
    assert item["week_bucket"] == "NEXT_WEEK"
    assert item["scheduled_at_utc"] is None
    assert item["actual"] is None
    assert item["forecast"] == "1.25"


def test_consumer_21_projection_is_compact_complete_and_under_90kb(
    tmp_path: Path,
) -> None:
    payload = {
        **full(
            event(
                "cpi",
                "2026-07-22T08:30:00-04:00",
                forecast="2.7",
                previous="2.6",
                field_lineage={"forecast": {"source": "BLS"}},
            )
        ),
        "generated_at_utc": "2026-07-22T11:00:00+00:00",
        "macro_snapshot": {},
        "market_schedule": {},
        "nasdaq_context": {},
        "news_context": {},
        "risk_context": {},
    }
    consumer = build_ai_trader_consumer_v2(
        payload,
        settings=cfg(tmp_path),
    )

    window = consumer["event_calendar_window"]
    assert consumer["schema_version"] == "2.1"
    assert set(window) >= {
        "timezone",
        "generated_at",
        "window_start",
        "window_end",
        "previous_week",
        "current_week",
        "next_week",
        "counts",
        "coverage",
    }
    assert window["current_week"]["events"][0]["forecast"] == "2.7"
    assert window["current_week"]["events"][0]["lineage"]
    assert (
        len(
            json.dumps(
                consumer,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        < 90_000
    )


def test_large_calendar_reports_true_overflow_without_hidden_compaction(
    tmp_path: Path,
) -> None:
    start = datetime(2026, 7, 20, 8, tzinfo=NY)
    events = [
        event(
            f"event-{index:03d}",
            (start + timedelta(minutes=index)).isoformat(),
            impact=("HIGH" if index % 3 == 0 else "MEDIUM"),
            forecast=str(index),
            previous=str(index - 1),
            title=("scheduled-event-" + str(index)) * 4,
        )
        for index in range(100)
    ]
    payload = {
        **full(*events),
        "generated_at_utc": "2026-07-20T11:00:00+00:00",
        "macro_snapshot": {},
        "market_schedule": {},
        "nasdaq_context": {},
        "news_context": {},
        "risk_context": {},
    }

    consumer = build_ai_trader_consumer_v2(
        payload,
        settings=cfg(
            tmp_path,
            event_calendar_consumer_max_events=90,
        ),
    )
    window = consumer["event_calendar_window"]
    visible = sum(
        window[key]["event_count"]
        for key in ("previous_week", "current_week", "next_week")
    )

    assert visible == window["coverage"]["retained_count"]
    assert window["coverage"]["candidate_count"] == 100
    assert window["coverage"]["overflow_count"] == 0
    assert window["coverage"]["status"] == "COMPLETE"
    assert window["coverage"]["size_limit_applied"] is False
    assert visible == 100


def test_compact_projection_preserves_nulls_and_drops_debug_audit(
    tmp_path: Path,
) -> None:
    debug = build_event_calendar_window(
        full(event("future", "2026-07-28")),
        settings=cfg(tmp_path),
        now=datetime(2026, 7, 22, 7, tzinfo=NY),
    )

    consumer = compact_event_calendar_window(debug)
    item = consumer["next_week"]["events"][0]
    assert item["actual"] is None
    assert item["forecast"] is None
    assert item["lineage"]
    assert "audit" in consumer


@pytest.mark.parametrize(
    ("previous", "current", "expected_cause"),
    [
        (
            {"actual": None, "scheduled_at": "2026-07-22T08:30:00-04:00"},
            {
                "actual": "2.5",
                "release_status": "PUBLISHED",
                "scheduled_at": "2026-07-22T08:30:00-04:00",
            },
            "ACTUAL_FIRST_PUBLICATION",
        ),
        (
            {"actual": "2.4", "scheduled_at": "2026-07-22T08:30:00-04:00"},
            {
                "actual": "2.5",
                "release_status": "REVISED",
                "scheduled_at": "2026-07-22T08:30:00-04:00",
            },
            "ACTUAL_MATERIAL_REVISION",
        ),
        (
            {"release_status": "SCHEDULED", "scheduled_at": "2026-07-22"},
            {"release_status": "CANCELLED", "scheduled_at": "2026-07-22"},
            "EVENT_CANCELLED",
        ),
        (
            {"release_status": "SCHEDULED", "scheduled_at": "2026-07-22"},
            {"release_status": "POSTPONED", "scheduled_at": "2026-07-22"},
            "EVENT_POSTPONED",
        ),
        (
            {"release_status": "SCHEDULED", "scheduled_at": "2026-07-22"},
            {"release_status": "SCHEDULED", "scheduled_at": "2026-07-23"},
            "MATERIAL_TIME_CHANGE",
        ),
    ],
)
def test_material_event_changes_trigger_once(
    previous: dict[str, object],
    current: dict[str, object],
    expected_cause: str,
) -> None:
    change = classify_event_change(
        {**previous, "occurrence_id": "event-1"},
        {**current, "occurrence_id": "event-1"},
    )
    assert change["trigger_class"] == "TRIGGERING"
    assert expected_cause in change["causes"]


def test_new_high_impact_future_event_triggers_but_missing_actual_does_not() -> None:
    high = classify_event_change(
        None,
        {
            "occurrence_id": "high",
            "impact": "HIGH",
            "is_future": True,
            "actual": None,
        },
    )
    normal_refresh = classify_event_change(
        {
            "occurrence_id": "same",
            "impact": "HIGH",
            "is_future": True,
            "actual": None,
            "forecast": "2.5",
        },
        {
            "occurrence_id": "same",
            "impact": "HIGH",
            "is_future": True,
            "actual": None,
            "forecast": "2.5",
            "freshness_state": "FRESH",
        },
    )

    assert high["causes"] == ["NEW_HIGH_IMPACT_FUTURE_EVENT"]
    assert normal_refresh["trigger_class"] == "NON_TRIGGERING"


def test_vix_options_ttl_and_idempotent_refreshes_are_non_triggering() -> None:
    baseline = {
        "occurrence_id": "event-1",
        "actual": "2.5",
        "scheduled_at": "2026-07-22T08:30:00-04:00",
    }
    for volatile_field in ("vix", "option_chain", "valid_until"):
        change = classify_event_change(
            baseline,
            {**baseline, volatile_field: "changed"},
        )
        assert change["trigger_class"] == "NON_TRIGGERING"


def test_trigger_batch_is_coalesced_and_idempotent() -> None:
    changes = [
        {
            "trigger_class": "TRIGGERING",
            "changed_event_id": "b",
            "causes": ["ACTUAL_FIRST_PUBLICATION"],
        },
        {
            "trigger_class": "TRIGGERING",
            "changed_event_id": "a",
            "causes": ["ACTUAL_MATERIAL_REVISION"],
        },
        {
            "trigger_class": "NON_TRIGGERING",
            "changed_event_id": "vix",
            "causes": [],
        },
    ]
    first = coalesce_event_changes(changes)
    second = coalesce_event_changes(reversed(changes))

    assert first["changed_event_ids"] == ["a", "b"]
    assert first["coalesced"] is True
    assert first["trigger_count"] == 2
    assert first["idempotency_fingerprint"] == second["idempotency_fingerprint"]


def test_saturday_sessions_are_both_closed_with_explicit_reasons() -> None:
    schedule = build_session_aware_schedule(
        {},
        now=datetime(2026, 7, 25, 12, tzinfo=NY),
    )

    assert schedule["nasdaq_cash_session"]["is_open"] is False
    assert schedule["nasdaq_cash_session"]["closed_reason"] == "WEEKEND"
    assert schedule["mnq_futures_session"]["is_open"] is False
    assert (
        schedule["mnq_futures_session"]["closed_reason"]
        == "WEEKEND"
    )
    assert (
        schedule["mnq_futures_session"]["verification_scope"]
        == "BASE_WEEKLY_RULE"
    )


def test_nasdaq_holiday_is_named_and_macro_calendar_remains_independent() -> None:
    schedule = build_session_aware_schedule(
        {
            "holidays": [
                {
                    "date": "2026-07-03",
                    "session_status": "closed",
                    "holiday_name": "Independence Day observed",
                }
            ]
        },
        now=datetime(2026, 7, 3, 10, tzinfo=NY),
    )

    cash = schedule["nasdaq_cash_session"]
    assert cash["status"] == "holiday"
    assert cash["closed_reason"] == "HOLIDAY"
    assert cash["holiday_name"] == "Independence Day observed"
    assert schedule["status"] == "UNVERIFIED"
    assert schedule["session_state_verified"] is False


def test_early_close_is_explicit_and_not_a_full_holiday() -> None:
    schedule = build_session_aware_schedule(
        {
            "holidays": [
                {
                    "date": "2026-11-27",
                    "session_status": "early_close",
                    "holiday_name": "Day after Thanksgiving",
                    "early_close_time_local": "13:00:00",
                }
            ]
        },
        now=datetime(2026, 11, 27, 12, tzinfo=NY),
    )

    cash = schedule["nasdaq_cash_session"]
    assert cash["is_open"] is True
    assert cash["is_early_close"] is True
    assert cash["holiday_name"] == "Day after Thanksgiving"


def test_cash_closed_while_futures_open_on_sunday_evening() -> None:
    schedule = build_session_aware_schedule(
        {},
        now=datetime(2026, 7, 26, 20, tzinfo=NY),
    )

    assert schedule["nasdaq_cash_session"]["is_open"] is False
    assert schedule["mnq_futures_session"]["is_open"] is True
    assert schedule["mnq_futures_session"]["session_reason"] == "GLOBEX_OPEN"
    assert schedule["mnq_futures_session"]["closed_reason"] is None


def test_futures_maintenance_break_is_distinct_from_cash_close() -> None:
    schedule = build_session_aware_schedule(
        {},
        now=datetime(2026, 7, 21, 17, 30, tzinfo=NY),
    )

    futures = schedule["mnq_futures_session"]
    assert futures["status"] == "maintenance_break"
    assert futures["calculated_status"] == "maintenance_break"
    assert futures["is_open"] is False
    assert futures["closed_reason"] == "MAINTENANCE_BREAK"
    assert futures["verification_scope"] == "BASE_WEEKLY_RULE"
    assert futures["maintenance_break"]["start"] == "17:00:00"
    assert futures["next_open_at"] == futures["next_open"]


def test_offline_replay_is_idempotent_under_90kb_and_has_zero_live_calls(
    tmp_path: Path,
) -> None:
    result = replay(workspace=tmp_path / "replay")

    assert result["schema_version"] == "2.1"
    assert result["timezone"] == "America/New_York"
    assert result["bucket_counts"] == {
        "PREVIOUS_WEEK": 1,
        "CURRENT_WEEK": 3,
        "NEXT_WEEK": 2,
    }
    assert result["idempotent"] is True
    assert result["under_90kb"] is True
    assert result["consumer_payload_bytes"] < 90_000
    assert result["provider_calls"] == 0
    assert result["ai_invocations"] == 0
    assert result["browser_calls"] == 0
    assert result["delivery_attempts"] == 0
    assert result["trading_calls"] == 0
    assert result["catchup"] == {
        "seeded": 45,
        "first_tick_claimed": 40,
        "first_tick_backlog_after": 5,
        "second_tick_claimed": 5,
        "second_tick_backlog_after": 0,
        "completion_status": "COMPLETED",
        "repeat_status": "ALREADY_COMPLETE",
        "repeat_writes": 0,
        "tick_count": 2,
        "live_provider_calls": 0,
        "ai_invocations": 0,
        "research_backend_invocations": 0,
    }
