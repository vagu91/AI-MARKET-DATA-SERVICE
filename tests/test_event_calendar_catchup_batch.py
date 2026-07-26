from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import (
    _split_sql,
    migrate_database,
)
from app.infrastructure.persistence.schema import MIGRATIONS
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
)
from app.services.execution_context import ExecutionContext
from app.services.market_context_outbox_service import (
    MarketContextOutboxRepository,
)
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.research_scheduler_service import ResearchSchedulerService


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 24, 14, tzinfo=UTC)


def cfg(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "calendar-catchup.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "model_pricing_path": ROOT / "config" / "model_pricing.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "environment": "test",
        "enable_scheduler": False,
        "research_scheduler_enabled": False,
        "lifecycle_due_scanner_enabled": False,
        "event_calendar_catchup_enabled": True,
        "event_calendar_catchup_batch_size": 20,
        "event_calendar_catchup_max_per_tick": 40,
        "event_calendar_catchup_lookback_days": 730,
        "event_calendar_notification_horizon_days": 21,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def occurrence(key: str, release: datetime) -> dict[str, Any]:
    return {
        "event_id": key,
        "occurrence_id": key,
        "canonical_event_key": key,
        "name": key,
        "country": "US",
        "category": "CPI",
        "impact": "HIGH",
        "date": release.date().isoformat(),
        "release_at": release.isoformat(),
        "time_utc": release.isoformat(),
        "actual": None,
        "forecast": "2.5",
        "previous": "2.4",
        "source": "BLS",
        "source_url": "https://www.bls.gov/schedule/",
        "valid_until": (release - timedelta(minutes=1)).isoformat(),
    }


def seed_baseline(
    settings: Settings,
    events: list[dict[str, Any]],
) -> None:
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="offline_baseline",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": (NOW - timedelta(hours=1)).isoformat(),
            "event_calendar": {
                "critical_macro_events": events,
                "fed_communications": [],
                "other_economic_events": [],
            },
            "macro_snapshot": {},
            "market_schedule": {},
            "nasdaq_context": {},
            "news_context": {},
            "risk_context": {},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )


def seed_due(
    settings: Settings,
    payload: dict[str, Any],
) -> dict[str, Any]:
    lifecycle = compute_datum_lifecycle(
        "macro_actual",
        str(payload["canonical_event_key"]),
        payload,
        settings=settings,
        now=NOW,
        fields_attempted=["actual"],
        triggering_event="macro_actual",
    )
    return LifecycleRepository(settings, clock=lambda: NOW).upsert(
        lifecycle,
        payload=payload,
        work_status="READY",
    )


def resolved(payload: dict[str, Any], actual: str) -> dict[str, Any]:
    return {
        **payload,
        "actual": actual,
        "published_at": (
            payload.get("release_at")
            or payload.get("scheduled_at_utc")
            or payload.get("scheduled_at")
        ),
        "retrieved_at": NOW.isoformat(),
        "valid_until": (NOW + timedelta(days=365)).isoformat(),
        "source": "BLS",
        "source_url": "https://www.bls.gov/news.release/",
        "source_lineage": [
            {
                "source": "BLS",
                "source_url": "https://www.bls.gov/news.release/",
                "source_classification": "official_source",
                "verification_status": "VERIFIED",
            }
        ],
    }


def provider_only_context() -> ExecutionContext:
    return ExecutionContext.provider_only(
        correlation_id="offline-event-calendar-catchup",
        allow_live_providers=True,
    )


def test_snapshot_atomically_seeds_past_and_future_occurrence_lifecycle(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    past = occurrence("event:past", NOW - timedelta(days=1))
    future = occurrence("event:future", NOW + timedelta(days=3))

    seed_baseline(settings, [past, future])

    by_key = {
        item["entity_key"]: item
        for item in LifecycleRepository(settings).list_items()
    }
    assert by_key["event:past"]["freshness_state"] == "AWAITING_ACTUAL"
    assert by_key["event:past"]["work_status"] == "READY"
    assert by_key["event:future"]["freshness_state"] == "FRESH"
    assert by_key["event:future"]["work_status"] == "IDLE"
    assert by_key["event:future"]["next_refresh_at"] == future["release_at"]


def test_provider_only_batch_commits_one_snapshot_and_one_outbox(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    events = [
        occurrence("event:a", NOW - timedelta(days=1)),
        occurrence("event:b", NOW - timedelta(days=8)),
    ]
    seed_baseline(settings, events)
    for item in events:
        seed_due(settings, item)
    calls: list[str] = []

    def resolver(item: dict[str, Any]) -> dict[str, Any]:
        calls.append(str(item["entity_key"]))
        datum = resolved(item["payload"], "2.7")
        return {
            "status": "RESOLVED",
            "datum": datum,
            "provider_request_attempted": True,
            "provider_request_completed": True,
            "ai_eligible": True,
        }

    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    result = scheduler.startup_catch_up(
        resolver=resolver,
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )

    assert result["claimed"] == 2
    assert result["actual_provider_requests"] == 2
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0
    assert result["provider_resolutions_coalesced"] is True
    assert len(result["rematerialized_snapshot_ids"]) == 1
    assert sorted(calls) == ["event:a", "event:b"]
    latest = MarketContextSnapshotRepository(settings).latest("MNQ")
    assert latest is not None
    batch = latest["debug_payload"]["event_change_batch"]
    assert batch["changed_event_ids"] == ["event:a", "event:b"]
    assert batch["coalesced"] is True
    assert batch["ai_invocations"] == 0
    outbox = MarketContextOutboxRepository(settings).list_events()
    assert len(outbox) == 1
    assert outbox[0]["changed_event_ids"] == ["event:a", "event:b"]
    assert outbox[0]["trigger_causes"] == ["ACTUAL_FIRST_PUBLICATION"]
    assert outbox[0]["coalesced"] is True
    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0] == 0


def test_second_tick_is_idempotent_without_duplicate_snapshot_or_outbox(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    payload = occurrence("event:once", NOW - timedelta(days=1))
    seed_baseline(settings, [payload])
    seed_due(settings, payload)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)

    first = scheduler.startup_catch_up(
        resolver=lambda item: {
            "status": "RESOLVED",
            "datum": resolved(item["payload"], "2.7"),
        },
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )
    second = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("resolved item must not be reclaimed"),
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )

    assert first["claimed"] == 1
    assert second["claimed"] == 0
    assert len(MarketContextOutboxRepository(settings).list_events()) == 1
    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0] == 2


@pytest.mark.parametrize("offline_days", [7, 30, 365])
def test_long_downtime_reconciles_but_old_history_does_not_notify(
    tmp_path: Path,
    offline_days: int,
) -> None:
    settings = cfg(tmp_path)
    payload = occurrence(
        f"event:old:{offline_days}",
        NOW - timedelta(days=offline_days),
    )
    seed_baseline(settings, [payload])
    seed_due(settings, payload)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)

    result = scheduler.startup_catch_up(
        resolver=lambda item: {
            "status": "RESOLVED",
            "datum": resolved(item["payload"], "2.7"),
        },
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )

    assert result["claimed"] == 1
    assert result["ai_invocations"] == 0
    latest = MarketContextSnapshotRepository(settings).latest("MNQ")
    assert latest is not None
    if offline_days <= settings.event_calendar_notification_horizon_days:
        assert len(MarketContextOutboxRepository(settings).list_events()) == 1
    else:
        assert MarketContextOutboxRepository(settings).list_events() == []
        assert (
            latest["debug_payload"]["event_change_batch"]["trigger_class"]
            == "NON_TRIGGERING"
        )


def test_bounded_partial_batch_uses_persisted_cursor_and_backlog(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        event_calendar_catchup_batch_size=2,
        event_calendar_catchup_max_per_tick=2,
    )
    events = [
        occurrence(f"event:{index}", NOW - timedelta(days=index + 1))
        for index in range(5)
    ]
    seed_baseline(settings, events)
    for item in events:
        seed_due(settings, item)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)

    first = scheduler.startup_catch_up(
        resolver=lambda item: {
            "status": "RESOLVED",
            "datum": resolved(item["payload"], "2.7"),
        },
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )
    second = scheduler.startup_catch_up(
        resolver=lambda item: {
            "status": "RESOLVED",
            "datum": resolved(item["payload"], "2.8"),
        },
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )
    third = scheduler.startup_catch_up(
        resolver=lambda item: {
            "status": "RESOLVED",
            "datum": resolved(item["payload"], "2.9"),
        },
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )

    assert first["claimed"] == second["claimed"] == 2
    assert third["claimed"] == 1
    assert first["catch_up_cursor"]
    assert first["catch_up_backlog_before"] == 5
    assert first["catch_up_backlog_after"] == 3
    assert second["catch_up_backlog_after"] == 1
    assert third["catch_up_backlog_after"] == 0


def test_temporary_provider_failure_persists_backoff_without_ai_or_outbox(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    payload = occurrence("event:backoff", NOW - timedelta(days=1))
    seed_baseline(settings, [payload])
    seeded = seed_due(settings, payload)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)

    result = scheduler.startup_catch_up(
        resolver=lambda _: {
            "status": "DEFERRED",
            "reason": "official_provider_timeout",
            "provider_request_attempted": True,
            "provider_request_failed": True,
            "ai_eligible": True,
        },
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )

    assert result["backoff"] == [seeded["item_id"]]
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0
    assert result["rematerialized_snapshot_ids"] == []
    assert MarketContextOutboxRepository(settings).list_events() == []
    stored = LifecycleRepository(settings).list_items()[0]
    assert stored["work_status"] == "BACKOFF"
    assert stored["next_retry_at"] is not None


def test_catchup_disabled_is_read_only_even_with_due_work(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        event_calendar_catchup_enabled=False,
    )
    payload = occurrence("event:disabled", NOW - timedelta(days=1))
    seed_baseline(settings, [payload])
    seed_due(settings, payload)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    with connect_sqlite(settings.database_path) as conn:
        before = conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0]
        provider_state_before = conn.execute(
            "SELECT COUNT(*) FROM provider_state"
        ).fetchone()[0]
        telemetry_before = conn.execute(
            "SELECT COUNT(*) FROM service_telemetry_events"
        ).fetchone()[0]

    result = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("resolver must stay unreachable"),
        ai_enqueue=lambda _: pytest.fail("AI enqueue must stay unreachable"),
        execution_context=provider_only_context(),
    )

    assert result["status"] == "DISABLED"
    assert result["writes"] == 0
    with connect_sqlite(settings.database_path) as conn:
        after = conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0]
        assert conn.execute(
            "SELECT COUNT(*) FROM provider_state"
        ).fetchone()[0] == provider_state_before
        assert conn.execute(
            "SELECT COUNT(*) FROM service_telemetry_events"
        ).fetchone()[0] == telemetry_before
    assert after == before


def test_new_high_impact_future_event_creates_one_schedule_outbox(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path, event_calendar_catchup_enabled=False)
    baseline = occurrence("event:baseline", NOW + timedelta(days=1))
    seed_baseline(settings, [baseline])
    added = occurrence("event:new-high", NOW + timedelta(days=2))

    saved = MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="offline_schedule_refresh",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "event_calendar": {
                "critical_macro_events": [baseline, added],
                "fed_communications": [],
                "other_economic_events": [],
            },
            "macro_snapshot": {},
            "market_schedule": {},
            "nasdaq_context": {},
            "news_context": {},
            "risk_context": {},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
        trigger_type="macro_schedule",
    )

    outbox = MarketContextOutboxRepository(settings).list_events()
    assert len(outbox) == 1
    assert outbox[0]["trigger_type"] == "high_impact_event_added"
    assert outbox[0]["changed_event_ids"] == ["event:new-high"]
    assert outbox[0]["trigger_causes"] == [
        "NEW_HIGH_IMPACT_FUTURE_EVENT"
    ]
    assert saved["consumer_payload"]["event_calendar_window"]["coverage"][
        "status"
    ] == "COMPLETE"


@pytest.mark.parametrize(
    ("mutation", "expected_trigger"),
    [
        ({"release_status": "CANCELLED"}, "event_cancelled"),
        ({"release_status": "POSTPONED"}, "event_postponed"),
        (
            {"release_at": (NOW + timedelta(days=2, hours=1)).isoformat()},
            "event_time_changed",
        ),
    ],
)
def test_schedule_state_changes_are_detected_end_to_end(
    tmp_path: Path,
    mutation: dict[str, Any],
    expected_trigger: str,
) -> None:
    settings = cfg(tmp_path, event_calendar_catchup_enabled=False)
    baseline = occurrence("event:changed", NOW + timedelta(days=2))
    seed_baseline(settings, [baseline])
    updated = {**baseline, **mutation}

    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="offline_schedule_refresh",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "event_calendar": {
                "critical_macro_events": [updated],
                "fed_communications": [],
                "other_economic_events": [],
            },
            "macro_snapshot": {},
            "market_schedule": {},
            "nasdaq_context": {},
            "news_context": {},
            "risk_context": {},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
        trigger_type="macro_schedule",
    )

    outbox = MarketContextOutboxRepository(settings).list_events()
    assert len(outbox) == 1
    assert outbox[0]["trigger_type"] == expected_trigger
    assert outbox[0]["changed_event_ids"] == ["event:changed"]


@pytest.mark.parametrize("source_version", range(1, 21))
def test_schema_1_through_20_migration_matrix_remains_idempotent(
    tmp_path: Path,
    source_version: int,
) -> None:
    database = tmp_path / f"schema-{source_version}.sqlite"
    with connect_sqlite(database) as conn:
        conn.execute(
            """
            CREATE TABLE schema_migrations (
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              applied_at TEXT NOT NULL
            )
            """
        )
        for version, (name, sql) in enumerate(
            MIGRATIONS[:source_version],
            start=1,
        ):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                """
                INSERT INTO schema_migrations(version,name,applied_at)
                VALUES (?,?,?)
                """,
                (version, name, NOW.isoformat()),
            )
        conn.execute(f"PRAGMA user_version={source_version}")
        conn.commit()

    first = migrate_database(database)
    second = migrate_database(database)

    assert first["schema_version"] == 21
    assert second["schema_version"] == 21
    assert second["applied"] == []
