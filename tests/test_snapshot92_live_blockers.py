from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.event_calendar_window_service import (
    build_event_calendar_window,
)
from app.services.execution_context import ExecutionContext
from app.services.lifecycle_due_resolver import (
    ExactOccurrenceActualProviderAdapter,
)
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import (
    MarketContextSyncService,
    canonical_json,
    extract_sync_sections,
    reconcile_delivered_section,
)
from app.services.market_session_service import build_session_aware_schedule
from app.services.news_intelligence_service import build_news_context
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_domain_service import canonical_event_key


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "snapshot_92_live_blockers_redacted.json"
)


def fixture() -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def cfg(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "environment": "test",
        "database_path": tmp_path / "snapshot92.sqlite",
        "event_calendar_catchup_enabled": True,
        "event_calendar_catchup_batch_size": 7,
        "event_calendar_catchup_max_per_tick": 40,
        "event_calendar_catchup_lookback_days": 730,
    }
    values.update(overrides)
    return Settings(**values)


def next_week_rows(count: int = 25) -> list[dict[str, object]]:
    return [
        {
            "provider": "BLS",
            "provider_event_id": f"redacted-next-{index:02d}",
            "name": f"Redacted next-week occurrence {index:02d}",
            "country": "US",
            "currency": "USD",
            "impact": "HIGH" if index < 3 else "MEDIUM",
            "event_type": "MACRO",
            "reference_period": "2026-07",
            "frequency": "monthly",
            "release_at": (
                datetime(2026, 7, 27, 12, 30, tzinfo=UTC)
                + timedelta(hours=index)
            ).isoformat(),
            "actual": None,
            "source": "BLS",
            "source_url": "https://www.bls.gov/",
            "retrieved_at": "2026-07-26T15:55:00+00:00",
        }
        for index in range(count)
    ]


def baseline_payload(now: datetime) -> dict[str, object]:
    return {
        "symbol": "MNQ",
        "generated_at_utc": now.isoformat(),
        "event_calendar": {
            "critical_macro_events": next_week_rows(),
            "fed_communications": [],
            "other_economic_events": [],
            "source_coverage": {
                "by_bucket": {
                    "PREVIOUS_WEEK": {"status": "UNVERIFIED_EMPTY"},
                    "CURRENT_WEEK": {"status": "UNVERIFIED_EMPTY"},
                    "NEXT_WEEK": {"status": "VERIFIED_COMPLETE"},
                }
            },
        },
        "macro_snapshot": {},
        "market_schedule": {},
        "nasdaq_context": {"earnings": {}},
        "news_context": {},
        "risk_context": {},
    }


def full_window_input(payload: dict[str, object]) -> dict[str, object]:
    discovery = list(payload["discovery"])  # type: ignore[arg-type]
    discovered_ids = [
        canonical_event_key(item)
        for item in discovery
        if isinstance(item, dict)
    ]
    return {
        **baseline_payload(
            datetime.fromisoformat(str(payload["reference_now"]))
        ),
        "event_calendar": {
            "critical_macro_events": [*discovery, *next_week_rows()],
            "fed_communications": [],
            "other_economic_events": [],
            "source_coverage": {
                "discovered_occurrence_ids": discovered_ids,
                "by_bucket": {
                    bucket: {"status": "VERIFIED_COMPLETE"}
                    for bucket in (
                        "PREVIOUS_WEEK",
                        "CURRENT_WEEK",
                        "NEXT_WEEK",
                    )
                },
            },
        },
        "event_calendar_window": {
            "audit": {
                "comparison": {
                    "removals": payload["unconfirmed_removals"],
                }
            }
        },
    }


def test_three_week_projection_retains_discovery_and_unconfirmed_actuals(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    window = build_event_calendar_window(
        full_window_input(payload),
        settings=cfg(tmp_path),
        now=now,
    )

    assert window["previous_week"]["event_count"] == 14
    assert window["current_week"]["event_count"] == 5
    assert window["next_week"]["event_count"] == 25
    missing = set(window["actual_missing_ids"])
    assert set(payload["expectations"]["actual_missing_ids"]) <= missing  # type: ignore[index]
    retained_removals = {
        item["occurrence_id"]: item
        for item in window["current_week"]["events"]
        if item.get("removal_status") == "UNCONFIRMED_REMOVAL"
    }
    assert set(retained_removals) == set(
        payload["expectations"]["actual_missing_ids"]  # type: ignore[index]
    )
    assert all(
        item["comparison_lineage"]["previous_source"] == "XTB"
        for item in retained_removals.values()
    )
    reconciliation = window["coverage"]["cross_stage_reconciliation"]
    assert reconciliation["discovered_occurrence_count"] == 17
    assert reconciliation["delivered_occurrence_count"] == 17
    assert reconciliation["unexplained_loss"] == 0


def test_discovery_gaps_are_processed_same_tick_and_wait_in_backoff(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="snapshot92-redacted-baseline",
        debug_payload=baseline_payload(now),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )

    class ScheduleAcquire:
        last_provider_results = [
            SimpleNamespace(errors=[]) for _ in range(5)
        ]

        def __call__(self, **_: object) -> list[dict[str, object]]:
            return list(payload["discovery"])  # type: ignore[arg-type]

    resolver_calls: list[str] = []

    def no_data(item: dict[str, object]) -> dict[str, object]:
        resolver_calls.append(str(item["entity_key"]))
        return {
            "status": "NO_DATA",
            "reason": "redacted_provider_envelope_no_data",
            "provider_request_attempted": True,
            "provider_request_completed": True,
            "ai_eligible": True,
        }

    scheduler = ResearchSchedulerService(settings, clock=lambda: now)
    context = ExecutionContext.provider_only(
        correlation_id="snapshot92-redacted-catchup",
        allow_live_providers=True,
    )
    first = scheduler.startup_catch_up(
        resolver=no_data,
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI enqueue must remain unreachable")
        ),
        schedule_acquire=ScheduleAcquire(),
        execution_context=context,
    )
    with connect_sqlite(settings.database_path) as conn:
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0]
        ai_jobs = conn.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0]
        ai_backends = conn.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0]
    second = scheduler.startup_catch_up(
        resolver=lambda _: (_ for _ in ()).throw(
            AssertionError("backoff must suppress the resolver")
        ),
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI enqueue must remain unreachable")
        ),
        schedule_acquire=ScheduleAcquire(),
        execution_context=context,
    )

    assert len(resolver_calls) == 17
    assert first["catch_up_backlog_before"] == 17
    assert first["claimed"] == 17
    assert first["catch_up_backlog_after"] == 17
    assert first["catch_up_due_after"] == 0
    assert first["residual_count"] == 17
    assert first["status"] == "WAITING_BACKOFF"
    assert first["catch_up_pending_backoff"] == 17
    assert first["source_coverage"]["persisted_gap_count"] == 17
    assert first["source_coverage"]["by_bucket"]["PREVIOUS_WEEK"][
        "candidate_count"
    ] == 14
    assert first["source_coverage"]["by_bucket"]["CURRENT_WEEK"][
        "candidate_count"
    ] == 3
    assert first["source_coverage"]["provider_result_count"] == 5
    assert first["source_coverage"]["provider_success_count"] == 5
    assert first["lifecycle_writes"] == 34
    assert first["snapshot_writes"] == 1
    assert first["writes"] == 35
    assert len(first["rematerialized_snapshot_ids"]) == 1
    assert first["ai_invocations"] == first["ai_jobs_created"] == 0
    assert ai_jobs == ai_backends == 0
    assert second["status"] == "WAITING_BACKOFF"
    assert second["claimed"] == second["writes"] == 0
    assert second["catch_up_backlog_after"] == 17
    with connect_sqlite(settings.database_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM market_context_snapshots"
            ).fetchone()[0]
            == snapshot_count
        )

    sync = MarketContextSyncService(settings)
    full = sync.full()
    revision = int(full["snapshot_revision"])
    selective = sync.sections(
        consumer_id="snapshot92-fixture",
        target_snapshot_revision=revision,
        sections=["event_calendar"],
        include_lineage=True,
    )
    assert (
        selective["sections"]["event_calendar"]["sync"]["fingerprint"]
        == full["sections"]["event_calendar"]["sync"]["fingerprint"]
    )
    selective_payload = dict(selective["sections"]["event_calendar"])
    full_payload = dict(full["sections"]["event_calendar"])
    selective_payload.pop("sync")
    full_payload.pop("sync")
    assert selective_payload == full_payload


def test_fixed_point_counts_initial_discovery_due_and_backoff_bounded(
    tmp_path: Path,
) -> None:
    from app.services.event_driven_lifecycle_service import (
        compute_datum_lifecycle,
    )

    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(
        tmp_path,
        event_calendar_catchup_batch_size=7,
        event_calendar_catchup_max_per_tick=20,
    )
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="fixed-point-baseline",
        debug_payload=baseline_payload(now),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: now)
    seed = dict(payload["discovery"][0])  # type: ignore[index]
    for index in range(18):
        entity_key = f"fixture:initial:{index:02d}"
        row = {
            **seed,
            "occurrence_id": entity_key,
            "canonical_event_key": entity_key,
            "provider_event_id": f"initial-{index:02d}",
        }
        scheduler.lifecycle.upsert(
            compute_datum_lifecycle(
                "macro_actual",
                entity_key,
                row,
                settings=settings,
                now=now,
                fields_attempted=["actual"],
            ),
            payload=row,
            work_status="READY",
        )
    assert scheduler.lifecycle.count_due(now=now) == 18

    class Acquire:
        last_provider_results = [SimpleNamespace(errors=[])]

        def __call__(self, **_: object) -> list[dict[str, object]]:
            return list(payload["discovery"])  # type: ignore[arg-type]

    calls: list[str] = []

    def no_data(item: dict[str, object]) -> dict[str, object]:
        calls.append(str(item["entity_key"]))
        return {
            "status": "NO_DATA",
            "reason": "offline_no_data",
            "provider_request_attempted": True,
            "provider_request_completed": True,
        }

    kwargs = {
        "resolver": no_data,
        "ai_enqueue": lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        "schedule_acquire": Acquire(),
        "execution_context": ExecutionContext.provider_only(
            correlation_id="fixed-point-18-plus-17",
            allow_live_providers=True,
        ),
    }
    first = scheduler.startup_catch_up(**kwargs)
    second = scheduler.startup_catch_up(**kwargs)
    snapshot_count = MarketContextSnapshotRepository(settings).latest(
        "MNQ"
    )["revision"]
    third = scheduler.startup_catch_up(
        **{
            **kwargs,
            "resolver": lambda _: (_ for _ in ()).throw(
                AssertionError("backoff must suppress resolver")
            ),
        }
    )

    assert first["catch_up_backlog_before"] == 35
    assert first["claimed"] == 20
    assert first["catch_up_due_after"] == 15
    assert first["catch_up_pending_backoff"] == 20
    assert first["catch_up_backlog_after"] == 35
    assert first["status"] == "IN_PROGRESS"
    assert second["claimed"] == 15
    assert second["catch_up_due_after"] == 0
    assert second["catch_up_pending_backoff"] == 35
    assert second["catch_up_backlog_after"] == 35
    assert second["status"] == "WAITING_BACKOFF"
    assert third["claimed"] == third["writes"] == 0
    assert third["catch_up_backlog_after"] == 35
    assert third["checkpoint_written"] is False
    assert len(calls) == 35
    assert (
        MarketContextSnapshotRepository(settings).latest("MNQ")["revision"]
        == snapshot_count
    )
    assert all(
        result["actuals_recovered"] == 0
        for result in (first, second, third)
    )
    assert all(
        result["ai_invocations"] == result["ai_jobs_created"] == 0
        for result in (first, second, third)
    )


def test_early_backoff_tick_is_byte_idempotent_with_advancing_clock(
    tmp_path: Path,
) -> None:
    payload = fixture()
    current_time = [datetime.fromisoformat(str(payload["reference_now"]))]
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="byte-idempotent-backoff-baseline",
        debug_payload=baseline_payload(current_time[0]),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )

    class Acquire:
        last_provider_results = [SimpleNamespace(errors=[])]

        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, **_: object) -> list[dict[str, object]]:
            self.calls += 1
            return list(payload["discovery"])  # type: ignore[arg-type]

    acquire = Acquire()
    scheduler = ResearchSchedulerService(
        settings,
        clock=lambda: current_time[0],
    )
    first = scheduler.startup_catch_up(
        resolver=lambda _: {
            "status": "NO_DATA",
            "reason": "offline_no_data",
            "provider_request_attempted": True,
            "provider_request_completed": True,
        },
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        schedule_acquire=acquire,
        execution_context=ExecutionContext.provider_only(
            correlation_id="byte-idempotent-backoff",
            allow_live_providers=True,
        ),
    )
    database_path = Path(settings.database_path)
    before = database_path.read_bytes()
    current_time[0] += timedelta(seconds=1)

    second = scheduler.startup_catch_up(
        resolver=lambda _: (_ for _ in ()).throw(
            AssertionError("early backoff must suppress the resolver")
        ),
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        schedule_acquire=acquire,
        execution_context=ExecutionContext.provider_only(
            correlation_id="byte-idempotent-backoff",
            allow_live_providers=True,
        ),
    )

    assert first["status"] == second["status"] == "WAITING_BACKOFF"
    assert second["claimed"] == second["writes"] == 0
    assert second["checkpoint_written"] is False
    assert acquire.calls == 1
    assert database_path.read_bytes() == before


def test_expired_backoff_is_not_double_counted_as_due_and_pending(
    tmp_path: Path,
) -> None:
    from app.services.event_driven_lifecycle_service import (
        compute_datum_lifecycle,
    )

    payload = fixture()
    current_time = [datetime.fromisoformat(str(payload["reference_now"]))]
    settings = cfg(tmp_path)
    scheduler = ResearchSchedulerService(
        settings,
        clock=lambda: current_time[0],
    )
    row = dict(payload["discovery"][0])  # type: ignore[index]
    scheduler.lifecycle.upsert(
        compute_datum_lifecycle(
            "macro_actual",
            "fixture:expired-backoff",
            row,
            settings=settings,
            now=current_time[0],
            fields_attempted=["actual"],
        ),
        payload=row,
        work_status="READY",
    )
    scheduler.scan_due_items(
        owner="seed-expired-backoff",
        resolver=lambda _: {
            "status": "NO_DATA",
            "reason": "offline_no_data",
        },
        ai_enqueue=None,
        force=True,
        allow_ai_residual=False,
    )
    lifecycle = {
        item["entity_key"]: item for item in scheduler.lifecycle.list_items()
    }
    current_time[0] = datetime.fromisoformat(
        str(lifecycle["fixture:expired-backoff"]["next_retry_at"])
    )

    result = scheduler.startup_catch_up(
        resolver=lambda _: {
            "status": "NO_DATA",
            "reason": "offline_no_data_again",
        },
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        schedule_acquire=None,
        execution_context=ExecutionContext.provider_only(
            correlation_id="expired-backoff-non-overlap",
            allow_live_providers=True,
        ),
    )

    assert result["catch_up_backlog_before"] == 1
    assert result["claimed"] == 1
    assert result["catch_up_due_after"] == 0
    assert result["catch_up_pending_retry"] == 1
    assert result["catch_up_pending_backoff"] == 1
    assert result["catch_up_backlog_after"] == 1
    assert result["status"] == "WAITING_BACKOFF"


def test_idle_retry_remains_in_real_backlog_instead_of_completed(
    tmp_path: Path,
) -> None:
    from app.services.event_driven_lifecycle_service import (
        compute_datum_lifecycle,
    )

    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    scheduler = ResearchSchedulerService(settings, clock=lambda: now)
    row = dict(payload["discovery"][0])  # type: ignore[index]
    scheduler.lifecycle.upsert(
        compute_datum_lifecycle(
            "macro_actual",
            "fixture:idle-retry",
            row,
            settings=settings,
            now=now,
            fields_attempted=["actual"],
        ),
        payload=row,
        work_status="READY",
    )

    result = scheduler.startup_catch_up(
        resolver=lambda _: {
            "status": "NOT_CONFIGURED",
            "reason": "offline_provider_temporarily_not_configured",
        },
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        schedule_acquire=None,
        execution_context=ExecutionContext.provider_only(
            correlation_id="idle-retry-backlog",
            allow_live_providers=True,
        ),
    )

    assert result["resolved"] == []
    assert result["residual_count"] == 1
    assert result["catch_up_due_after"] == 0
    assert result["catch_up_pending_retry"] == 1
    assert result["catch_up_pending_backoff"] == 0
    assert result["catch_up_backlog_after"] == 1
    assert result["status"] == "WAITING_BACKOFF"


@pytest.mark.parametrize(
    ("provider_result", "expected_status"),
    [
        (
            {
                "status": "NO_DATA",
                "reason": "offline_retry_deadline",
                "retry_deadline_exhausted": True,
            },
            "EXHAUSTED_NO_DATA",
        ),
        (
            {
                "status": "NOT_CONFIGURED",
                "reason": "offline_provider_disabled",
                "agent_status": "DISABLED",
            },
            "COMPLETED_WITH_GAPS",
        ),
    ],
)
def test_terminal_gap_statuses_are_disjoint_from_completed(
    tmp_path: Path,
    provider_result: dict[str, object],
    expected_status: str,
) -> None:
    from app.services.event_driven_lifecycle_service import (
        compute_datum_lifecycle,
    )

    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    scheduler = ResearchSchedulerService(settings, clock=lambda: now)
    row = dict(payload["discovery"][0])  # type: ignore[index]
    scheduler.lifecycle.upsert(
        compute_datum_lifecycle(
            "macro_actual",
            "fixture:terminal-gap",
            row,
            settings=settings,
            now=now,
            fields_attempted=["actual"],
        ),
        payload=row,
        work_status="READY",
    )

    result = scheduler.startup_catch_up(
        resolver=lambda _: provider_result,
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        schedule_acquire=None,
        execution_context=ExecutionContext.provider_only(
            correlation_id=f"terminal-gap-{expected_status.lower()}",
            allow_live_providers=True,
        ),
    )

    assert result["resolved"] == []
    assert result["residual_count"] == 1
    assert result["catch_up_backlog_after"] == 0
    assert result["catch_up_terminal_gap_count"] == 1
    assert result["status"] == expected_status
    assert result["catch_up_completion_status"] == expected_status


def test_mixed_resolution_and_terminal_gap_is_partial(
    tmp_path: Path,
) -> None:
    from app.services.event_driven_lifecycle_service import (
        compute_datum_lifecycle,
    )

    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    scheduler = ResearchSchedulerService(settings, clock=lambda: now)
    seed = dict(payload["discovery"][0])  # type: ignore[index]
    for suffix in ("resolved", "disabled"):
        row = {
            **seed,
            "occurrence_id": f"fixture:mixed:{suffix}",
            "canonical_event_key": f"fixture:mixed:{suffix}",
            "provider_event_id": f"fixture-mixed-{suffix}",
        }
        scheduler.lifecycle.upsert(
            compute_datum_lifecycle(
                "macro_actual",
                f"fixture:mixed:{suffix}",
                row,
                settings=settings,
                now=now,
                fields_attempted=["actual"],
            ),
            payload=row,
            work_status="READY",
        )

    result = scheduler.startup_catch_up(
        resolver=lambda item: (
            {
                "status": "RESOLVED",
                "next_refresh_at": (now + timedelta(days=1)).isoformat(),
            }
            if str(item["entity_key"]).endswith("resolved")
            else {
                "status": "NOT_CONFIGURED",
                "reason": "offline_provider_disabled",
                "agent_status": "DISABLED",
            }
        ),
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        schedule_acquire=None,
        execution_context=ExecutionContext.provider_only(
            correlation_id="mixed-resolution-terminal-gap",
            allow_live_providers=True,
        ),
    )

    assert len(result["resolved"]) == 1
    assert result["residual_count"] == 1
    assert result["catch_up_terminal_gap_count"] == 1
    assert result["status"] == "PARTIAL"
    assert result["catch_up_completion_status"] == "PARTIAL"


def test_retry_exhaustion_is_not_reported_as_completed(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    scheduler = ResearchSchedulerService(settings, clock=lambda: now)
    lifecycle = scheduler.lifecycle
    row = dict(payload["discovery"][0])  # type: ignore[index]
    from app.services.event_driven_lifecycle_service import (
        compute_datum_lifecycle,
    )

    lifecycle.upsert(
        compute_datum_lifecycle(
            "macro_actual",
            "fixture:exhausted",
            row,
            settings=settings,
            now=now,
            fields_attempted=["actual"],
        ),
        payload=row,
        work_status="READY",
    )
    result = scheduler.scan_due_items(
        owner="snapshot92-exhaustion",
        resolver=lambda _: {
            "status": "NO_DATA",
            "reason": "redacted_retry_deadline",
            "retry_deadline_exhausted": True,
        },
        ai_enqueue=None,
        force=True,
        allow_ai_residual=False,
    )

    assert result["status"] == "EXHAUSTED_NO_DATA"
    assert result["exhausted_no_data"]
    assert result["item_outcomes"][0]["status"] == "EXHAUSTED_NO_DATA"
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0


def test_news_policy_and_sync_are_lossless_and_count_coherent() -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    context = build_news_context(
        list(payload["news"]),  # type: ignore[arg-type]
        now=now,
    )
    by_title = {item["title"]: item for item in context["latest"]}
    ibd = by_title[
        "Nvidia earnings outlook lifts semiconductor shares"
    ]
    reuters = [
        by_title["US expands Nvidia chip export controls"],
        by_title["US expands Nvidia chip export controls — update"],
    ]
    rejected = by_title[
        "Unknown publisher claims Nvidia development"
    ]

    assert ibd["validation"]["status"] == "accepted"
    assert ibd["confirmation"]["confirmed"] is False
    assert ibd["confirmed_by_multiple_sources"] is False
    assert all(
        item["validation"]["status"] == "accepted"
        and item["original_publisher"] == "Reuters"
        and item["distribution_source"] == "Yahoo Finance"
        and item["distribution_url"].startswith(
            "https://finance.yahoo.com/"
        )
        for item in reuters
    )
    assert len({item["published_at"] for item in reuters}) == 2
    assert rejected["validation"]["status"] == "rejected"
    policy = SourcePolicyService()
    assert policy.policy_version == "source-policy-v5"

    news = extract_sync_sections(
        {
            "news_context": context,
            "latest_news": context["latest"],
            "news_digest": context["digest"],
        }
    )["news"]
    delivered = news["context"]["articles"]
    assert len(delivered) == 3
    assert {item["original_publisher"] for item in delivered} == {
        "Investor's Business Daily",
        "Reuters",
    }
    assert news["context"]["accepted_article_count"] == 3
    assert news["context"]["delivered_raw_article_count"] == 3
    assert news["context"]["historical_article_count"] == 0
    assert news["context"]["diagnostics"]["excluded_count"] == 1
    assert news["context"]["rejected_article_count"] == 1
    assert news["context"]["usable_for_analysis"] is True
    assert news["context"]["status"] == "PARTIAL"
    assert news["digest"]["status"] == "PARTIAL"
    assert news["digest"]["accepted_article_count"] == 3
    assert news["producer_disclosures"]["quarantine"][
        "record_count"
    ] >= 1


def test_multi_megabyte_news_reconciliation_does_not_cap_or_deduplicate() -> None:
    rows = [
        {
            "article_id": f"large-{index}",
            "source": "Reuters",
            "summary": f"{index}:€漢字🙂" + ("x" * 450_000),
        }
        for index in range(13)
    ]
    section = reconcile_delivered_section(
        "news",
        {
            "context": {
                "articles": rows,
                "latest": rows,
                "historical_articles": [],
                "search_completed": True,
            },
            "latest": rows,
            "digest": {},
        },
    )

    assert len(canonical_json(section).encode("utf-8")) > 5_000_000
    assert len(section["context"]["articles"]) == 13
    assert section["context"]["delivered_raw_article_count"] == 13


def test_weekend_closure_survives_override_timeout_without_inventing_holiday() -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    schedule = build_session_aware_schedule(
        dict(payload["market_schedule"]),  # type: ignore[arg-type]
        now=now,
    )

    assert schedule["status"] == "PARTIAL"
    for key in ("nasdaq_cash_session", "mnq_futures_session"):
        session = schedule[key]
        assert session["status"] == "weekend"
        assert session["is_open"] is False
        assert session["closed_reason"] == "WEEKEND"
        assert session["verification_scope"] == "BASE_WEEKLY_RULE"
        assert session["holiday_override_status"] == "UNVERIFIED"
        assert session["holiday_name"] is None
        assert session["is_early_close"] is False


def test_rates_temporal_invariant_uses_delivered_observation(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="snapshot92-rates-temporal-fixture",
        debug_payload={
            **baseline_payload(now),
            "rates_context": payload["rates"],
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    rates = MarketContextSyncService(settings).manifest()["sections"][
        "rates"
    ]

    assert datetime.fromisoformat(rates["valid_until"]) >= datetime.fromisoformat(
        str(payload["rates"]["retrieved_at"])  # type: ignore[index]
    )
    assert rates["freshness"] == "EXPIRED"


def test_empty_news_cannot_remain_available_or_usable() -> None:
    section = reconcile_delivered_section(
        "news",
        {
            "context": {
                "status": "AVAILABLE",
                "articles": [],
                "latest": [],
                "historical_articles": [],
                "accepted_article_count": 4,
                "delivered_raw_article_count": 0,
                "candidate_article_count": 4,
                "search_completed": True,
                "historical_coverage_status": "VERIFIED_COMPLETE",
                "usable_for_analysis": True,
                "diagnostics": {
                    "raw_article_count": 4,
                    "accepted_count": 4,
                },
            },
            "latest": [],
            "digest": {
                "status": "AVAILABLE",
                "accepted_article_count": 4,
            },
        },
    )

    context = section["context"]
    assert context["status"] == "NO_DATA"
    assert context["reason"] == "NO_DELIVERED_ARTICLES"
    assert context["accepted_article_count"] == 0
    assert context["delivered_raw_article_count"] == 0
    assert context["usable_for_analysis"] is False
    assert context["rejected_article_count"] == 4
    assert section["digest"]["status"] == "NO_DATA_AVAILABLE"
    assert section["digest"]["accepted_article_count"] == 0


def test_weekly_futures_rule_is_informative_without_holiday_override() -> None:
    scenarios = (
        (datetime(2026, 7, 25, 12, tzinfo=UTC), False, "WEEKEND"),
        (datetime(2026, 7, 26, 20, tzinfo=UTC), False, "WEEKEND"),
        (datetime(2026, 7, 26, 23, tzinfo=UTC), True, "GLOBEX_OPEN"),
        (datetime(2026, 7, 28, 2, tzinfo=UTC), True, "GLOBEX_OPEN"),
        (
            datetime(2026, 7, 27, 21, 30, tzinfo=UTC),
            False,
            "MAINTENANCE_BREAK",
        ),
    )
    for now, expected_open, reason in scenarios:
        session = build_session_aware_schedule({}, now=now)[
            "mnq_futures_session"
        ]
        assert session["is_open"] is expected_open
        assert session["session_reason"] == reason
        assert session["verification_scope"] == "BASE_WEEKLY_RULE"
        assert session["holiday_override_status"] == "UNVERIFIED"
        assert session["holiday_name"] is None
        assert session["is_early_close"] is False


def test_temporal_corrections_are_inside_delivered_records_and_fingerprint(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 26, 16, tzinfo=UTC)
    settings = cfg(tmp_path)
    records = [
        {
            "series": "daily",
            "data_as_of": "2026-07-24T00:00:00+00:00",
            "valid_until": "2026-07-13T00:00:00+00:00",
            "value": 1,
        },
        {
            "series": "weekly",
            "observed_at": "2026-07-25T00:00:00+00:00",
            "valid_until": "2026-07-20T00:00:00+00:00",
            "value": 2,
        },
        {
            "series": "already-valid",
            "data_as_of": "2026-07-23T00:00:00+00:00",
            "valid_until": "2026-07-30T00:00:00+00:00",
            "value": 3,
        },
    ]
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="temporal-adversarial",
        debug_payload={
            **baseline_payload(now),
            "rates_context": {"status": "AVAILABLE", "records": records},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    sync = MarketContextSyncService(settings)
    full = sync.full()
    revision = int(full["snapshot_revision"])
    selective = sync.sections(
        consumer_id="temporal-adversarial",
        target_snapshot_revision=revision,
        sections=["rates"],
        include_lineage=True,
    )
    full_rates = dict(full["sections"]["rates"])
    selective_rates = dict(selective["sections"]["rates"])
    full_sync = full_rates.pop("sync")
    selective_sync = selective_rates.pop("sync")

    assert full_rates == selective_rates
    assert full_sync["fingerprint"] == selective_sync["fingerprint"]
    for record in full_rates["context"]["records"]:
        floor = record.get("data_as_of") or record.get("observed_at")
        assert datetime.fromisoformat(record["valid_until"]) >= datetime.fromisoformat(
            floor
        )
    disclosure = full_rates["producer_disclosures"][
        "temporal_reconciliation"
    ]
    assert disclosure["correction_count"] == 2
    assert full_sync["fingerprint"]


def _baseline_with_named_unconfirmed(
    payload: dict[str, object],
    now: datetime,
) -> dict[str, object]:
    prior = [
        dict(item["previous_occurrence"])
        for item in payload["unconfirmed_removals"]  # type: ignore[index]
    ]
    baseline = baseline_payload(now)
    baseline["event_calendar"] = {
        **dict(baseline["event_calendar"]),  # type: ignore[arg-type]
        "critical_macro_events": [*prior, *next_week_rows()],
    }
    return baseline


def test_real_discovery_retains_named_unconfirmed_actuals_and_backoff(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="named-unconfirmed-baseline",
        debug_payload=_baseline_with_named_unconfirmed(payload, now),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )

    class Acquire:
        last_provider_results = [SimpleNamespace(errors=[])]

        def __call__(self, **_: object) -> list[dict[str, object]]:
            return list(payload["discovery"])  # type: ignore[arg-type]

    result = ResearchSchedulerService(
        settings, clock=lambda: now
    ).startup_catch_up(
        resolver=lambda _: {
            "status": "NO_DATA",
            "reason": "offline_no_data",
            "provider_request_attempted": True,
            "provider_request_completed": True,
        },
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        schedule_acquire=Acquire(),
        execution_context=ExecutionContext.provider_only(
            correlation_id="named-unconfirmed-no-data",
            allow_live_providers=True,
        ),
    )
    components = MarketContextSnapshotRepository(settings).latest_components(
        "MNQ"
    )
    window = build_event_calendar_window(
        components,
        settings=settings,
        now=now,
    )
    named_ids = set(payload["expectations"]["actual_missing_ids"])  # type: ignore[index]
    visible = {
        item["occurrence_id"]: item
        for item in window["current_week"]["events"]
        if item.get("occurrence_id") in named_ids
    }
    lifecycle = {
        item["entity_key"]: item
        for item in ResearchSchedulerService(
            settings, clock=lambda: now
        ).lifecycle.list_items()
    }

    assert set(visible) == named_ids
    assert all(item["actual"] is None for item in visible.values())
    assert all(
        item["release_status"] == "AWAITING_ACTUAL"
        for item in visible.values()
    )
    assert all(
        item["removal_status"] == "UNCONFIRMED_REMOVAL"
        for item in visible.values()
    )
    assert named_ids <= set(window["actual_missing_ids"])
    assert all(lifecycle[item]["work_status"] == "BACKOFF" for item in named_ids)
    assert all(lifecycle[item]["next_retry_at"] for item in named_ids)
    assert result["status"] == "WAITING_BACKOFF"
    assert result["actuals_recovered"] == 0
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0


def test_named_actuals_are_recovered_by_exact_occurrence_without_ai(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="named-actual-baseline",
        debug_payload=_baseline_with_named_unconfirmed(payload, now),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    named_ids = set(payload["expectations"]["actual_missing_ids"])  # type: ignore[index]

    class Acquire:
        last_provider_results = [SimpleNamespace(errors=[])]

        def __call__(self, **_: object) -> list[dict[str, object]]:
            return list(payload["discovery"])  # type: ignore[arg-type]

    adapter = ExactOccurrenceActualProviderAdapter(
        lambda _: payload["actual_provider_observations"]  # type: ignore[index]
    )
    recovered: dict[str, str] = {}

    def resolve(item: dict[str, object]) -> dict[str, object]:
        entity_key = str(item["entity_key"])
        result = adapter.resolve(item)  # type: ignore[arg-type]
        if result["status"] == "RESOLVED":
            recovered[entity_key] = str(
                result["datum"]["reference_period"]
            )
        return result

    result = ResearchSchedulerService(
        settings, clock=lambda: now
    ).startup_catch_up(
        resolver=resolve,
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI must remain unreachable")
        ),
        schedule_acquire=Acquire(),
        execution_context=ExecutionContext.provider_only(
            correlation_id="named-actual-recovery",
            allow_live_providers=True,
        ),
    )
    components = MarketContextSnapshotRepository(settings).latest_components(
        "MNQ"
    )
    window = build_event_calendar_window(
        components,
        settings=settings,
        now=now,
    )
    current = {
        item["occurrence_id"]: item
        for item in window["current_week"]["events"]
        if item.get("occurrence_id") in named_ids
    }
    sync = MarketContextSyncService(settings)
    full = sync.full()
    revision = int(full["snapshot_revision"])
    selective = sync.sections(
        consumer_id="named-actual-recovery",
        target_snapshot_revision=revision,
        sections=["event_calendar"],
        include_lineage=True,
    )

    assert set(recovered) == named_ids
    assert recovered["xtb:146392:2026-07-24"] == "2026-06"
    assert recovered["xtb:146945:2026-07-24"] == "2026-07"
    assert current["xtb:146392:2026-07-24"]["actual"] == 628.0
    assert current["xtb:146945:2026-07-24"]["actual"] == 53.6
    assert all(item["actual"] not in (None, "") for item in current.values())
    assert all(
        item["release_status"] == "PUBLISHED" for item in current.values()
    )
    assert named_ids.isdisjoint(window["actual_missing_ids"])
    assert result["actuals_recovered"] == 2
    assert result["snapshot_writes"] == 2
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0
    full_section = dict(full["sections"]["event_calendar"])
    selective_section = dict(selective["sections"]["event_calendar"])
    assert full_section.pop("sync") == selective_section.pop("sync")
    assert full_section == selective_section
