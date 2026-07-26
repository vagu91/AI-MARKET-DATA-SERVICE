from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.bls_calendar import BlsReleaseCalendarProvider
from app.services.ai_trader_consumer_v2_service import _news
from app.services.event_calendar_window_service import (
    build_event_calendar_window,
)
from app.services.market_context_hardening_service import (
    apply_news_semantics,
    evaluate_readiness,
)
from app.services.market_context_sync_service import extract_sync_sections
from app.services.market_session_service import build_session_aware_schedule
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.temporal_validation_service import (
    PERIOD_RELEASE_DATE_INCONSISTENT,
    REFERENCE_PERIOD_AFTER_RELEASE_DATE,
    RELEASE_ON_IMPLAUSIBLE_WEEKEND,
    TemporalPolicy,
)
from scripts.replay_snapshot91_sync_offline import replay


ROOT = Path(__file__).resolve().parents[1]
NY = ZoneInfo("America/New_York")
NOW = datetime(2026, 7, 22, 12, tzinfo=NY)


def cfg(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "environment": "test",
        "database_path": tmp_path / "content-closure.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "model_pricing_path": ROOT / "config" / "model_pricing.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def occurrence(
    occurrence_id: str,
    release_at: str,
    **overrides: object,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_id": occurrence_id,
        "occurrence_id": occurrence_id,
        "name": "Consumer Price Index",
        "event_type": "CPI",
        "category": "MACRO",
        "country": "US",
        "impact": "HIGH",
        "release_at": release_at,
        "source": "BLS",
        "provider": "BLS",
        "provider_event_id": occurrence_id,
        "source_event_id": occurrence_id,
        "source_url": "https://www.bls.gov/cpi/",
        "source_timezone": "America/New_York",
        "retrieved_at": "2026-07-20T12:00:00+00:00",
        "validation": {"status": "accepted"},
    }
    payload.update(overrides)
    return payload


def calendar_payload(
    rows: list[dict[str, object]],
    *,
    coverage: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "event_calendar": {
            "critical_macro_events": rows,
            "fed_communications": [],
            "other_economic_events": [],
            "source_coverage": coverage
            or {
                "by_bucket": {
                    name: {"status": "VERIFIED_COMPLETE"}
                    for name in (
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
        for bucket in ("previous_week", "current_week", "next_week")
        for item in window[bucket]["events"]  # type: ignore[index]
    ]


def official_schedule(
    *,
    overrides: list[dict[str, object]] | None = None,
    valid_until: str = "2027-01-01T00:00:00+00:00",
) -> dict[str, object]:
    return {
        "nasdaq_cash_session": {
            "status": "found",
            "source": "Nasdaq Official Trading Schedule",
            "validation": {"status": "accepted"},
        },
        "cme_calendar": {
            "status": "available",
            "official_document_discovered": True,
            "official_schedule_parsed": True,
            "source": "CME Group Trading Hours",
            "source_url": "https://www.cmegroup.com/trading-hours.html",
            "retrieved_at": "2026-07-20T00:00:00+00:00",
            "valid_until": valid_until,
            "equity_index_schedule": {
                "coverage_start": "2026-01-01",
                "coverage_end": "2026-12-31",
                "overrides": overrides or [],
            },
        },
    }


def article(
    article_id: str,
    published_at: str,
    *,
    provider: str = "SEC",
) -> dict[str, object]:
    return {
        "article_id": article_id,
        "headline": f"Original headline {article_id}",
        "title": f"Original headline {article_id}",
        "summary": f"Original summary {article_id}",
        "content": f"Original content {article_id}",
        "source": provider,
        "provider": provider,
        "source_url": f"https://www.sec.gov/news/{article_id}",
        "canonical_url": f"https://www.sec.gov/news/{article_id}",
        "published_at": published_at,
        "retrieved_at": "2026-07-22T15:00:00+00:00",
        "symbols": ["QQQ"],
        "topics": ["technology"],
        "validation": {"status": "accepted"},
        "freshness": "CURRENT",
        "lineage": {"provider_record_id": article_id},
        "cluster_id": "cluster-1",
    }


def test_lossless_calendar_has_no_count_or_byte_limit_and_exceeds_5mb(
    tmp_path: Path,
) -> None:
    large_text = "Mercati Ω漢字🚀" * 14_000
    rows = [
        occurrence(
            f"large-{index}",
            (
                datetime(2026, 7, 20, 8, tzinfo=NY)
                + timedelta(minutes=index)
            ).isoformat(),
            name=f"{large_text}-{index}",
        )
        for index in range(36)
    ]
    window = build_event_calendar_window(
        calendar_payload(rows),
        settings=cfg(
            tmp_path,
            event_calendar_consumer_max_events=3,
        ),
        now=NOW,
    )

    encoded = json.dumps(window, ensure_ascii=False).encode("utf-8")
    assert len(encoded) > 5_000_000
    assert len(all_events(window)) == 36
    assert window["coverage"]["source_candidate_count"] == 36
    assert window["coverage"]["delivered_occurrence_count"] == 36
    assert window["coverage"]["omitted_for_size_count"] == 0
    assert window["coverage"]["omitted_for_count_count"] == 0
    assert window["coverage"]["unexplained_loss"] == 0


def test_three_week_coverage_requires_affirmative_empty_verification(
    tmp_path: Path,
) -> None:
    rows = [
        occurrence("previous", "2026-07-15T08:30:00-04:00"),
        occurrence("current", "2026-07-22T08:30:00-04:00"),
        occurrence("next", "2026-07-29T08:30:00-04:00"),
    ]
    verified = build_event_calendar_window(
        calendar_payload(rows),
        settings=cfg(tmp_path),
        now=NOW,
    )
    unverified = build_event_calendar_window(
        calendar_payload(
            rows[1:],
            coverage={
                "by_bucket": {
                    "PREVIOUS_WEEK": {"status": "UNVERIFIED_EMPTY"},
                    "CURRENT_WEEK": {"status": "VERIFIED_COMPLETE"},
                    "NEXT_WEEK": {"status": "VERIFIED_COMPLETE"},
                }
            },
        ),
        settings=cfg(tmp_path),
        now=NOW,
    )

    assert verified["counts"]["by_bucket"] == {
        "PREVIOUS_WEEK": 1,
        "CURRENT_WEEK": 1,
        "NEXT_WEEK": 1,
    }
    assert verified["coverage"]["status"] == "COMPLETE"
    assert (
        unverified["coverage"]["by_bucket"]["PREVIOUS_WEEK"][
            "source_coverage_status"
        ]
        == "UNVERIFIED_EMPTY"
    )
    assert unverified["coverage"]["status"] == "PARTIAL"


def test_occurrence_identity_preserves_times_sources_and_exact_duplicates(
    tmp_path: Path,
) -> None:
    first = occurrence("series-release", "2026-07-22T08:30:00-04:00")
    second_time = occurrence(
        "series-release-later",
        "2026-07-22T10:00:00-04:00",
    )
    other_source = {
        **first,
        "provider": "Census",
        "source": "Census",
        "provider_event_id": "census-series-release",
        "source_event_id": "census-series-release",
        "source_url": "https://www.census.gov/economic-indicators/",
    }
    window = build_event_calendar_window(
        calendar_payload([first, second_time, other_source, dict(first)]),
        settings=cfg(tmp_path),
        now=NOW,
    )
    events = {
        str(item["occurrence_id"]): item for item in all_events(window)
    }

    assert set(events) == {"series-release", "series-release-later"}
    assert len(events["series-release"]["source_evidence"]) == 2
    assert window["coverage"]["source_candidate_count"] == 4
    assert window["coverage"]["delivered_valid_source_record_count"] == 3
    assert window["coverage"]["delivered_occurrence_count"] == 2
    assert window["coverage"]["exact_duplicate_count"] == 1
    assert window["coverage"]["unexplained_loss"] == 0


def test_bls_list_parser_rejects_adjacent_month_and_temporal_anomalies(
    tmp_path: Path,
) -> None:
    html = """
    <table>
      <tr><th>Date</th><th>Time</th><th>Release</th></tr>
      <tr><td rowspan="2">Friday, July 31, 2026</td><td>08:30 AM</td>
          <td>Employment Situation for July 2026</td></tr>
      <tr><td>10:00 AM</td>
          <td>Compensation Costs for Second Quarter 2026</td></tr>
      <tr><td>Saturday, August 1, 2026</td><td>08:30 AM</td>
          <td>Consumer Price Index for September 2026</td></tr>
    </table>
    """
    provider = BlsReleaseCalendarProvider(
        ProviderCacheRepository(tmp_path / "provider-cache.sqlite"),
        cfg(tmp_path),
    )
    parsed = provider._parse_month(
        html,
        "https://www.bls.gov/schedule/2026/07_sched_list.htm",
        2026,
        7,
        retrieved_at=datetime(2026, 7, 20, tzinfo=UTC),
    )

    assert len(parsed) == 2
    assert {item["date"] for item in parsed} == {"2026-07-31"}
    policy = TemporalPolicy(clock=lambda: NOW.astimezone(UTC))
    weekend = policy.evaluate(
        occurrence(
            "weekend",
            "2026-08-01T08:30:00-04:00",
            source="BLS Release Calendar",
            provider="BLS Release Calendar",
        ),
        domain="macro_calendar",
    )
    future_period = policy.evaluate(
        occurrence(
            "future-period",
            "2026-08-03T08:30:00-04:00",
            reference_period="September 2026",
        ),
        domain="macro_calendar",
    )
    stale_period = policy.evaluate(
        occurrence(
            "stale-period",
            "2026-08-03T08:30:00-04:00",
            reference_period="January 2020",
        ),
        domain="macro_calendar",
    )

    assert weekend.reason_code == RELEASE_ON_IMPLAUSIBLE_WEEKEND
    assert future_period.reason_code == REFERENCE_PERIOD_AFTER_RELEASE_DATE
    assert stale_period.reason_code == PERIOD_RELEASE_DATE_INCONSISTENT


def test_semantically_expected_weekend_event_is_valid() -> None:
    decision = TemporalPolicy(clock=lambda: NOW.astimezone(UTC)).evaluate(
        occurrence(
            "weekend-election",
            "2026-07-25T10:00:00-04:00",
            event_type="ELECTION",
            category="GEOPOLITICAL",
            weekend_release_expected=True,
        ),
        domain="macro_calendar",
    )
    assert decision.accepted is True


def test_raw_news_and_historical_records_are_delivered_with_lineage(
    tmp_path: Path,
) -> None:
    current = article("current", "2026-07-22T14:00:00+00:00")
    historical_a = article("history-a", "2026-07-16T14:00:00+00:00")
    historical_b = article(
        "history-a",
        "2026-07-16T14:00:00+00:00",
        provider="Federal Reserve",
    )
    context = apply_news_semantics(
        {
            "candidate_article_count": 3,
            "latest": [current],
            "historical_articles": [historical_a, historical_b],
            "directly_relevant": [current],
            "supporting": [historical_a, historical_b],
            "clusters": [
                {
                    "cluster_id": "cluster-1",
                    "article_ids": [
                        "current",
                        "history-a",
                        "history-a",
                    ],
                }
            ],
            "historical_search_completed": True,
        },
        pipeline={},
        market_schedule={
            "context_date": "2026-07-22",
            "market_session_status": "open",
            "last_market_session_date": "2026-07-21",
        },
        settings=cfg(tmp_path),
        now=NOW,
    )
    consumer_news = _news(context, {}, {})

    assert len(consumer_news["articles"]) == 1
    assert len(consumer_news["historical_articles"]) == 2
    assert consumer_news["historical_article_count"] == 2
    assert consumer_news["historical_context_available"] is True
    assert (
        consumer_news["historical_coverage_status"]
        == "VERIFIED_COMPLETE"
    )
    delivered = [
        *consumer_news["articles"],
        *consumer_news["historical_articles"],
    ]
    assert all(item["headline"].startswith("Original headline") for item in delivered)
    assert all(item["lineage"] for item in delivered)


def test_news_empty_history_is_unverified_and_quarantine_is_not_operational(
    tmp_path: Path,
) -> None:
    empty = apply_news_semantics(
        {
            "provider_success_count": 1,
            "candidate_article_count": 0,
            "latest": [],
        },
        pipeline={},
        market_schedule={
            "context_date": "2026-07-22",
            "market_session_status": "open",
        },
        settings=cfg(tmp_path),
        now=NOW,
    )
    rejected = article("rejected", "2026-07-22T14:00:00+00:00")
    rejected["validation"] = {
        "status": "rejected",
        "reason_code": "SOURCE_NOT_ALLOWED",
    }
    news_section = extract_sync_sections(
        {
            "news_context": {
                "status": "AVAILABLE",
                "search_completed": True,
                "articles": [rejected],
                "latest": [rejected],
                "historical_articles": [],
            }
        }
    )["news"]

    assert empty["historical_coverage_status"] == "UNVERIFIED_EMPTY"
    assert empty["historical_context_available"] is False
    assert empty["historical_article_count"] == 0
    assert news_section["context"]["articles"] == []
    assert news_section["context"]["accepted_article_count"] == 0
    assert news_section["context"]["status"] == "QUARANTINED"
    assert news_section["producer_disclosures"]["quarantine"][
        "record_count"
    ] == 1


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(2026, 7, 25, 12, tzinfo=NY), ("weekend", False)),
        (datetime(2026, 7, 26, 17, tzinfo=NY), ("weekend", False)),
        (datetime(2026, 7, 26, 20, tzinfo=NY), ("open", True)),
        (
            datetime(2026, 7, 21, 17, 30, tzinfo=NY),
            ("maintenance_break", False),
        ),
    ],
)
def test_verified_cme_weekend_open_and_maintenance_states(
    now: datetime,
    expected: tuple[str, bool],
) -> None:
    schedule = build_session_aware_schedule(
        official_schedule(),
        now=now,
    )
    futures = schedule["mnq_futures_session"]
    assert (futures["status"], futures["is_open"]) == expected
    assert futures["session_state_verified"] is True


def test_cme_timeout_fails_closed_and_valid_lkg_is_used() -> None:
    now = datetime(2026, 7, 26, 20, tzinfo=NY)
    unavailable = build_session_aware_schedule(
        {
            "cme_calendar": {
                "status": "timeout",
                "next_retry_at": "2026-07-26T21:00:00-04:00",
            }
        },
        now=now,
    )
    lkg = build_session_aware_schedule(
        {
            "cme_calendar": {
                "status": "timeout",
                "last_known_good": official_schedule()["cme_calendar"],
            }
        },
        now=now,
    )

    assert unavailable["status"] == "PARTIAL"
    assert unavailable["mnq_futures_session"]["is_open"] is None
    assert (
        unavailable["mnq_futures_session"]["closed_reason"]
        == "UNVERIFIED_SCHEDULE"
    )
    assert lkg["mnq_futures_session"]["is_open"] is True
    assert lkg["last_verified_cme_calendar_used"] is True


def test_holiday_observed_early_close_and_cash_closed_mnq_open() -> None:
    observed = build_session_aware_schedule(
        {
            **official_schedule(
                overrides=[
                    {
                        "date": "2026-07-03",
                        "session_status": "regular",
                    }
                ]
            ),
            "holidays": [
                {
                    "date": "2026-07-03",
                    "session_status": "closed",
                    "holiday_name": "Independence Day observed",
                }
            ],
        },
        now=datetime(2026, 7, 3, 10, tzinfo=NY),
    )
    early = build_session_aware_schedule(
        {
            **official_schedule(
                overrides=[
                    {
                        "date": "2026-11-27",
                        "session_status": "early_close",
                        "close_time_local": "13:00:00",
                    }
                ]
            ),
            "holidays": [
                {
                    "date": "2026-11-27",
                    "session_status": "early_close",
                    "holiday_name": "Day after Thanksgiving",
                    "early_close_time_local": "13:00:00",
                }
            ],
        },
        now=datetime(2026, 11, 27, 14, tzinfo=NY),
    )

    assert observed["nasdaq_cash_session"]["is_open"] is False
    assert observed["nasdaq_cash_session"]["closed_reason"] == "HOLIDAY"
    assert observed["mnq_futures_session"]["is_open"] is True
    assert early["nasdaq_cash_session"]["is_open"] is False
    assert early["nasdaq_cash_session"]["is_early_close"] is True
    assert early["mnq_futures_session"]["closed_reason"] == "EARLY_CLOSE"


@pytest.mark.parametrize(
    "downtime",
    [timedelta(days=1), timedelta(days=31), timedelta(days=366)],
)
def test_provider_first_schedule_catchup_is_restart_independent_and_idempotent(
    tmp_path: Path,
    downtime: timedelta,
) -> None:
    now = datetime(2026, 7, 22, 12, tzinfo=UTC) + downtime
    local_now = now.astimezone(NY)
    current_monday = local_now.date() - timedelta(
        days=local_now.weekday()
    )
    release = datetime.combine(
        current_monday,
        datetime.min.time(),
        NY,
    ) + timedelta(hours=10)
    calls: list[dict[str, object]] = []

    def acquire(**kwargs: object) -> list[dict[str, object]]:
        calls.append(kwargs)
        return [
            {
                **occurrence(
                    f"schedule-{downtime.days}",
                    release.isoformat(),
                    event_type="REGULATORY",
                    category="SCHEDULED_REGULATORY_EVENT",
                    source="Federal Register",
                    provider="Federal Register",
                    source_url="https://www.federalregister.gov/",
                ),
                "actual": None,
            }
        ]

    settings = cfg(
        tmp_path,
        event_calendar_catchup_enabled=True,
        event_calendar_catchup_lookback_days=730,
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: now)

    def ai_unreachable(_: object) -> None:
        pytest.fail("AI must remain unreachable")

    first = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("schedule-only item is not due"),
        ai_enqueue=ai_unreachable,
        schedule_acquire=acquire,
    )
    count_after_first = len(scheduler.lifecycle.list_items())
    second = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("schedule-only item is not due"),
        ai_enqueue=ai_unreachable,
        schedule_acquire=acquire,
    )

    assert first["source_coverage"]["status"] == "VERIFIED_COMPLETE"
    assert first["source_coverage"]["persisted_gap_count"] == 1
    assert second["source_coverage"]["persisted_gap_count"] == 0
    assert second["source_coverage"]["unchanged_occurrence_count"] == 1
    assert len(scheduler.lifecycle.list_items()) == count_after_first == 1
    assert calls[0]["start"].astimezone(NY).date() == (
        current_monday - timedelta(days=7)
    )
    assert first["ai_invocations"] == second["ai_invocations"] == 0


def test_schedule_provider_failure_enters_persistent_backoff(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 22, 12, tzinfo=UTC)

    class FailedAcquirer:
        def __init__(self) -> None:
            self.last_provider_results: list[object] = []
            self.calls = 0

        async def list_events(self, **_: object) -> list[object]:
            self.calls += 1
            self.last_provider_results = [
                SimpleNamespace(errors=["controlled timeout"])
            ]
            return []

    acquirer = FailedAcquirer()
    scheduler = ResearchSchedulerService(
        cfg(
            tmp_path,
            event_calendar_catchup_enabled=True,
        ),
        clock=lambda: now,
    )
    first = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("no due items expected"),
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        schedule_acquire=acquirer.list_events,
    )
    second = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("no due items expected"),
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        schedule_acquire=acquirer.list_events,
    )

    assert first["source_coverage"]["status"] == "PROVIDER_UNAVAILABLE"
    assert first["source_coverage"]["next_retry_at"]
    assert second["source_coverage"]["reason"] == (
        "persistent_provider_backoff"
    )
    assert acquirer.calls == 1


def test_schedule_catchup_uses_persistent_single_flight(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 22, 12, tzinfo=UTC)
    entered = threading.Event()
    release = threading.Event()
    calls = 0

    def acquire(**_: object) -> list[object]:
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(timeout=5)
        return []

    scheduler = ResearchSchedulerService(
        cfg(tmp_path),
        clock=lambda: now,
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            scheduler._seed_canonical_schedule_gaps,
            schedule_acquire=acquire,
            now=now,
        )
        assert entered.wait(timeout=5)
        second = scheduler._seed_canonical_schedule_gaps(
            schedule_acquire=acquire,
            now=now,
        )
        release.set()
        completed = first.result(timeout=5)

    assert second["status"] == "PARTIAL"
    assert second["reason"] == "schedule_catchup_single_flight_active"
    assert completed["status"] == "VERIFIED_COMPLETE"
    assert calls == 1


def test_readiness_uses_delivered_unverified_schedule_state(
    tmp_path: Path,
) -> None:
    readiness = evaluate_readiness(
        {
            "market_schedule": {
                "status": "UNVERIFIED",
                "validation": {"status": "unverified"},
                "market_session_status": "unknown",
            },
            "macro_snapshot": {"series": {"CPI": {"value": 1}}},
            "events_today_context": {"status": "NO_EVENTS_SCHEDULED"},
            "risk_context": {"status": "AVAILABLE"},
            "nasdaq_context": {"status": "AVAILABLE"},
            "news_context": {"status": "NO_RELEVANT_NEWS"},
        },
        settings=cfg(tmp_path),
    )

    assert readiness["section_status"]["market_schedule"] == "UNVERIFIED"
    assert "market_schedule_missing" in readiness["blocking_reasons"]
    assert readiness["ready_for_trading_context"] is False


def test_snapshot91_replay_has_no_live_side_effects_or_silent_loss() -> None:
    result = replay()
    invariants = result["invariants"]

    assert result["calendar"]["actual_missing_ids"] == [
        "xtb:146392:2026-07-24",
        "xtb:146945:2026-07-24",
    ]
    assert len(result["calendar"]["formerly_omitted_ids"]) == 8
    assert len(result["calendar"]["quarantined_ids"]) == 2
    assert invariants["candidate_equation_holds"] is True
    assert invariants["omitted_for_size_is_zero"] is True
    assert invariants["omitted_for_count_is_zero"] is True
    assert invariants["unexplained_loss_is_zero"] is True
    assert invariants["news_historical_state_matches_content"] is True
    assert invariants["payload_size_exact"] is True
    assert set(result["side_effects"].values()) == {0}
