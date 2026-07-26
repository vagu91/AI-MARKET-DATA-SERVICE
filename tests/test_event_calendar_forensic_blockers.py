from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.main import run_event_calendar_catchup_loop
from app.services.ai_trader_consumer_v2_service import (
    build_ai_trader_consumer_v2,
)
from app.services.event_calendar_window_service import (
    build_event_calendar_window,
)
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
from app.services.lifecycle_due_resolver import (
    MacroActualLifecycleProviderAdapter,
    existing_lifecycle_provider_adapters,
)
from app.services.research_scheduler_service import ResearchSchedulerService


NY = ZoneInfo("America/New_York")
NOW = datetime(2026, 7, 22, 16, tzinfo=UTC)


def cfg(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "environment": "test",
        "database_path": tmp_path / "forensic-calendar.sqlite",
        "event_calendar_catchup_enabled": True,
        "event_calendar_catchup_batch_size": 20,
        "event_calendar_catchup_max_per_tick": 40,
        "event_calendar_catchup_lookback_days": 730,
        "telemetry_trace_detail_enabled": True,
    }
    values.update(overrides)
    return Settings(**values)


def calendar_event(
    occurrence_id: str,
    scheduled_at: str,
    *,
    event_type: str,
    category: str,
    source: str,
    source_url: str,
    impact: str = "HIGH",
    **extra: object,
) -> dict[str, object]:
    return {
        "event_id": occurrence_id,
        "occurrence_id": occurrence_id,
        "name": occurrence_id,
        "event_type": event_type,
        "category": category,
        "country": "US",
        "currency": "USD",
        "impact": impact,
        "release_at": scheduled_at,
        "source": source,
        "source_url": source_url,
        **extra,
    }


def snapshot_payload(
    *,
    critical: list[dict[str, object]] | None = None,
    fed: list[dict[str, object]] | None = None,
    regulatory: list[dict[str, object]] | None = None,
    geopolitical: list[dict[str, object]] | None = None,
    earnings: list[dict[str, object]] | None = None,
    removals: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "symbol": "MNQ",
        "generated_at_utc": NOW.isoformat(),
        "event_calendar": {
            "critical_macro_events": critical or [],
            "fed_communications": fed or [],
            "other_economic_events": [],
            "scheduled_regulatory_events": regulatory or [],
            "scheduled_geopolitical_events": geopolitical or [],
            "removal_confirmations": removals or [],
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
        },
        "macro_snapshot": {},
        "market_schedule": {},
        "nasdaq_context": {"earnings": {"upcoming": earnings or []}},
        "news_context": {},
        "risk_context": {},
    }


def test_mixed_snapshot_persists_typed_fail_closed_lifecycles(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    repository = MarketContextSnapshotRepository(settings)
    repository.save_next(
        symbol="MNQ",
        refresh_mode="forensic_mixed_snapshot",
        debug_payload=snapshot_payload(
            critical=[
                calendar_event(
                    "macro:cpi",
                    "2026-07-21T08:30:00-04:00",
                    event_type="CPI",
                    category="MACRO",
                    source="BLS",
                    source_url="https://www.bls.gov/cpi/",
                    actual=None,
                    forecast="2.7",
                ),
                calendar_event(
                    "fomc:decision",
                    "2026-07-21T14:00:00-04:00",
                    event_type="FOMC_DECISION",
                    category="FOMC",
                    source="Federal Reserve",
                    source_url="https://www.federalreserve.gov/monetarypolicy/",
                    actual=None,
                ),
            ],
            fed=[
                calendar_event(
                    "fomc:minutes",
                    "2026-07-21T10:00:00-04:00",
                    event_type="FOMC_MINUTES",
                    category="FOMC",
                    source="Federal Reserve",
                    source_url="https://www.federalreserve.gov/monetarypolicy/fomcminutes.htm",
                )
            ],
            regulatory=[
                calendar_event(
                    "reg:hearing",
                    "2026-07-21T10:00:00-04:00",
                    event_type="REGULATORY",
                    category="REGULATORY",
                    source="SEC",
                    source_url="https://www.sec.gov/news/upcoming-events",
                )
            ],
            geopolitical=[
                calendar_event(
                    "geo:summit",
                    "2026-07-21",
                    event_type="GEOPOLITICAL",
                    category="GEOPOLITICAL",
                    source="U.S. Department of State",
                    source_url="https://www.state.gov/",
                )
            ],
            earnings=[
                {
                    "issuer_event_id": "earnings:nvda:2026-07-21",
                    "event_id": "earnings:nvda:2026-07-21",
                    "date": "2026-07-21",
                    "release_at": "2026-07-21T08:00:00-04:00",
                    "event_type": "EARNINGS",
                    "issuer_name": "NVIDIA",
                    "symbol": "NVDA",
                    "impact": "HIGH",
                    "eps_actual": None,
                    "eps_estimate": "1.25",
                    "source": "NASDAQ",
                    "source_url": "https://www.nasdaq.com/market-activity/earnings",
                }
            ],
        ),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )

    items = {
        item["entity_key"]: item
        for item in LifecycleRepository(settings).list_items()
    }
    assert items["macro:cpi"]["entity_type"] == "macro_actual"
    assert items["earnings:nvda:2026-07-21"]["entity_type"] == (
        "earnings_actual"
    )
    assert items["fomc:decision"]["entity_type"] == "fomc_decision"
    assert items["fomc:minutes"]["entity_type"] == "fomc_communication"
    assert items["reg:hearing"]["entity_type"] == "schedule_only"
    assert items["geo:summit"]["entity_type"] == "schedule_only"
    assert items["reg:hearing"]["work_status"] == "IDLE"
    assert items["geo:summit"]["work_status"] == "IDLE"
    assert items["geo:summit"]["event_at"] is None
    assert items["reg:hearing"]["fields_attempted"] == []
    resolved_types: list[str] = []
    result = ResearchSchedulerService(
        settings,
        clock=lambda: NOW,
    ).startup_catch_up(
        resolver=lambda item: (
            resolved_types.append(str(item["entity_type"]))
            or {
                "status": "DEFERRED",
                "reason": "forensic_provider_fixture_deferred",
            }
        ),
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        execution_context=ExecutionContext.provider_only(
            correlation_id="mixed-provider-only",
            allow_live_providers=True,
        ),
    )
    assert set(resolved_types) == {
        "macro_actual",
        "earnings_actual",
        "fomc_decision",
        "fomc_communication",
    }
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0
    assert "schedule_only" not in resolved_types


def test_production_resolver_routing_never_maps_non_macro_to_macro_adapter(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    adapters = existing_lifecycle_provider_adapters(
        macro_service=SimpleNamespace(latest=lambda: {}),
        event_service=SimpleNamespace(
            upcoming=lambda **_: [],
            list_events=lambda **_: [],
        ),
        nasdaq_data_service=SimpleNamespace(
            earnings=lambda: {},
            qqq_holdings=lambda: {},
            mega_cap_snapshot=lambda: {},
            latest_news=lambda: {},
        ),
        settings=settings,
        official_actual_resolver=SimpleNamespace(resolve_event=lambda **_: {}),
    )

    assert isinstance(adapters["macro_actual"], MacroActualLifecycleProviderAdapter)
    assert adapters["earnings_actual"].name == "earnings_provider"
    assert adapters["earnings_actual"] is not adapters["macro_actual"]
    assert "schedule_only" not in adapters
    assert "regulatory" not in adapters
    assert "geopolitical" not in adapters


def _seed_due_macro(
    settings: Settings,
    *,
    key: str,
    release: datetime,
) -> None:
    payload = {
        "event_id": key,
        "occurrence_id": key,
        "canonical_event_key": key,
        "event_type": "CPI",
        "category": "MACRO",
        "country": "US",
        "impact": "HIGH",
        "release_at": release.isoformat(),
        "scheduled_at_utc": release.isoformat(),
        "actual": None,
        "forecast": "2.5",
        "source": "BLS",
        "source_url": "https://www.bls.gov/cpi/",
    }
    lifecycle = compute_datum_lifecycle(
        "macro_actual",
        key,
        payload,
        settings=settings,
        now=NOW,
        fields_attempted=["actual"],
        triggering_event="macro_actual",
    )
    LifecycleRepository(settings, clock=lambda: NOW).upsert(
        lifecycle,
        payload=payload,
        work_status="READY",
    )


def test_persistent_provider_only_ticks_drain_more_than_forty_without_restart(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="forensic_catchup_baseline",
        debug_payload=snapshot_payload(),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    for index in range(45):
        _seed_due_macro(
            settings,
            key=f"macro:backlog:{index:02d}",
            release=NOW - timedelta(days=1 + index * 16),
        )
    resolver_types: list[str] = []

    def resolver(item: dict[str, object]) -> dict[str, object]:
        resolver_types.append(str(item["entity_type"]))
        payload = dict(item["payload"])  # type: ignore[arg-type]
        return {
            "status": "RESOLVED",
            "datum": {
                **payload,
                "actual": "2.7",
                "published_at": payload["release_at"],
                "retrieved_at": NOW.isoformat(),
                "valid_until": (NOW + timedelta(days=365)).isoformat(),
                "source_lineage": [
                    {
                        "source": "BLS",
                        "source_url": "https://www.bls.gov/cpi/",
                        "source_classification": "official_source",
                        "verification_status": "VERIFIED",
                    }
                ],
            },
            "provider_request_attempted": True,
            "provider_request_completed": True,
            "ai_eligible": True,
        }

    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    context = ExecutionContext.provider_only(
        correlation_id="forensic-provider-only",
        allow_live_providers=True,
    )
    first = scheduler.startup_catch_up(
        resolver=resolver,
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        execution_context=context,
    )
    second = scheduler.startup_catch_up(
        resolver=resolver,
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        execution_context=context,
    )
    with connect_sqlite(settings.database_path) as conn:
        snapshot_count_after_completion = conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0]
        outbox_count_after_completion = conn.execute(
            "SELECT COUNT(*) FROM market_context_outbox"
        ).fetchone()[0]
    third = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("completed backlog must not resolve"),
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        execution_context=context,
    )

    assert first["claimed"] == 40
    assert first["status"] == "IN_PROGRESS"
    assert first["catch_up_backlog_before"] == 45
    assert first["catch_up_backlog_after"] == 5
    assert first["catch_up_completion_status"] == "IN_PROGRESS"
    assert second["claimed"] == 5
    assert second["status"] == "COMPLETED"
    assert second["catch_up_backlog_after"] == 0
    assert second["catch_up_completion_status"] == "COMPLETED"
    assert third["status"] == "ALREADY_COMPLETE"
    assert third["writes"] == 0
    assert third["checkpoint_written"] is False
    assert resolver_types == ["macro_actual"] * 45
    assert first["ai_invocations"] == second["ai_invocations"] == 0
    assert first["ai_jobs_created"] == second["ai_jobs_created"] == 0
    assert third["ai_invocations"] == third["ai_jobs_created"] == 0
    with connect_sqlite(settings.database_path) as conn:
        checkpoint = conn.execute(
            """
            SELECT payload_json FROM provider_state
            WHERE state_key='event_calendar_lifecycle_catchup'
            """
        ).fetchone()
        assert checkpoint is not None
        payload = json.loads(checkpoint["payload_json"])
        assert payload["backlog_after"] == 0
        assert payload["tick_count"] == 2
        assert payload["completion_status"] == "COMPLETED"
        tick_telemetry = conn.execute(
            """
            SELECT stop_reason,payload_json
            FROM service_telemetry_events
            WHERE event_name='startup_catch_up'
            ORDER BY rowid
            """
        ).fetchall()
        assert [row["stop_reason"] for row in tick_telemetry] == [
            "IN_PROGRESS",
            "COMPLETED",
        ]
        assert [
            json.loads(row["payload_json"])["payload"]["status"]
            for row in tick_telemetry
        ] == ["IN_PROGRESS", "COMPLETED"]
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0] == snapshot_count_after_completion
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_outbox"
        ).fetchone()[0] == outbox_count_after_completion
        assert conn.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_production_catchup_loop_runs_provider_only_ticks_without_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = cfg(
        tmp_path,
        lifecycle_due_scanner_interval_seconds=5,
    )
    contexts: list[ExecutionContext] = []

    class FakeScheduler:
        def __init__(self) -> None:
            self.settings = settings

        def startup_catch_up(self, **kwargs: object) -> dict[str, object]:
            contexts.append(kwargs["execution_context"])  # type: ignore[arg-type]
            return {
                "status": "COMPLETED",
                "catch_up_backlog_after": (
                    1 if len(contexts) == 1 else 0
                ),
                "ai_invocations": 0,
            }

        def enqueue_due_residuals(self, *_: object, **__: object) -> None:
            pytest.fail("provider-only loop must never enqueue AI")

    sleeps = 0

    async def bounded_sleep(_: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", bounded_sleep)
    state = {
        "settings": settings,
        "research_scheduler": FakeScheduler(),
        "lifecycle_due_resolver": SimpleNamespace(resolve=lambda _: {}),
    }
    with pytest.raises(asyncio.CancelledError):
        await run_event_calendar_catchup_loop(state)

    assert len(contexts) == 2
    assert all(context.allow_ai is False for context in contexts)
    assert all(context.allow_live_providers is True for context in contexts)


@pytest.mark.asyncio
async def test_catchup_loop_recovers_after_transient_tick_without_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = cfg(
        tmp_path,
        lifecycle_due_scanner_interval_seconds=5,
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    contexts: list[ExecutionContext] = []
    tick_calls = 0

    def transient_then_success(**kwargs: object) -> dict[str, object]:
        nonlocal tick_calls
        tick_calls += 1
        contexts.append(kwargs["execution_context"])  # type: ignore[arg-type]
        if tick_calls == 1:
            raise RuntimeError("temporary provider fixture failure")
        return {
            "status": "COMPLETED",
            "catch_up_completion_status": "COMPLETED",
            "ai_invocations": 0,
            "ai_jobs_created": 0,
        }

    monkeypatch.setattr(
        scheduler,
        "startup_catch_up",
        transient_then_success,
    )
    sleep_delays: list[float] = []

    async def bounded_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        if len(sleep_delays) >= 4:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", bounded_sleep)
    caplog.set_level(logging.INFO, logger="app.main")
    state = {
        "settings": settings,
        "research_scheduler": scheduler,
        "lifecycle_due_resolver": SimpleNamespace(
            resolve=lambda _: pytest.fail(
                "fixture scheduler must not invoke a provider"
            )
        ),
    }

    with pytest.raises(asyncio.CancelledError):
        await run_event_calendar_catchup_loop(state)

    assert tick_calls == 2
    assert sleep_delays == [5, 1, 5, 5]
    assert all(context.allow_ai is False for context in contexts)
    assert all(context.allow_live_providers is True for context in contexts)
    with connect_sqlite(settings.database_path) as conn:
        error_row = conn.execute(
            """
            SELECT correlation_id,stop_reason,payload_json
            FROM service_telemetry_events
            WHERE event_name='startup_catch_up'
              AND stop_reason='TRANSIENT_ERROR'
            """
        ).fetchone()
        assert error_row is not None
        error_payload = json.loads(error_row["payload_json"])
        assert str(error_row["correlation_id"]).startswith(
            "event-calendar-catch-up-"
        )
        assert error_payload["payload"]["status"] == "IN_PROGRESS"
        assert error_payload["payload"]["error_type"] == "RuntimeError"
        assert error_payload["payload"]["retry_delay_seconds"] == 1
        assert "RuntimeError" in str(error_payload["redacted_error"])
        assert conn.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0] == 0
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "error_type=RuntimeError catch_up_status=IN_PROGRESS" in message
        and "correlation_id=event-calendar-catch-up-" in message
        for message in messages
    )
    assert any("status=COMPLETED" in message for message in messages)


@pytest.mark.asyncio
async def test_catchup_loop_always_propagates_cancelled_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = cfg(
        tmp_path,
        lifecycle_due_scanner_interval_seconds=5,
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    contexts: list[ExecutionContext] = []

    def cancelled_tick(**kwargs: object) -> dict[str, object]:
        contexts.append(kwargs["execution_context"])  # type: ignore[arg-type]
        raise asyncio.CancelledError

    async def immediate_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(scheduler, "startup_catch_up", cancelled_tick)
    monkeypatch.setattr(asyncio, "sleep", immediate_sleep)
    state = {
        "settings": settings,
        "research_scheduler": scheduler,
        "lifecycle_due_resolver": SimpleNamespace(resolve=lambda _: {}),
    }

    with pytest.raises(asyncio.CancelledError):
        await run_event_calendar_catchup_loop(state)

    assert len(contexts) == 1
    assert contexts[0].allow_ai is False
    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM service_telemetry_events
            WHERE event_name='startup_catch_up'
            """
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0] == 0


def test_waiting_backoff_early_tick_is_fully_idempotent_and_provider_free(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    _seed_due_macro(
        settings,
        key="macro:deferred:idempotent",
        release=NOW - timedelta(days=1),
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    resolver_calls = 0

    def deferred(_: dict[str, object]) -> dict[str, object]:
        nonlocal resolver_calls
        resolver_calls += 1
        return {
            "status": "DEFERRED",
            "reason": "temporary_provider_fixture_failure",
            "provider_request_attempted": True,
            "provider_request_failed": True,
            "ai_eligible": True,
        }

    context = ExecutionContext.provider_only(
        correlation_id="waiting-backoff-fixture",
        allow_live_providers=True,
    )
    first = scheduler.startup_catch_up(
        resolver=deferred,
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        execution_context=context,
    )
    with connect_sqlite(settings.database_path) as conn:
        checkpoint_before = conn.execute(
            """
            SELECT payload_json,updated_at,next_retry_at
            FROM provider_state
            WHERE state_key='event_calendar_lifecycle_catchup'
            """
        ).fetchone()
        telemetry_before = conn.execute(
            "SELECT COUNT(*) FROM service_telemetry_events"
        ).fetchone()[0]
        snapshots_before = conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0]
        outbox_before = conn.execute(
            "SELECT COUNT(*) FROM market_context_outbox"
        ).fetchone()[0]
    assert checkpoint_before is not None

    second = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail(
            "resolver must not run before next_retry_at"
        ),
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        execution_context=context,
    )

    assert resolver_calls == 1
    assert first["status"] == "WAITING_BACKOFF"
    assert first["catch_up_completion_status"] == "WAITING_BACKOFF"
    assert first["catch_up_pending_backoff"] == 1
    assert first["catch_up_next_retry_at"] is not None
    assert first["ai_invocations"] == first["ai_jobs_created"] == 0
    with connect_sqlite(settings.database_path) as conn:
        waiting_telemetry = conn.execute(
            """
            SELECT stop_reason,payload_json
            FROM service_telemetry_events
            WHERE event_name='startup_catch_up'
            """
        ).fetchone()
        assert waiting_telemetry is not None
        assert waiting_telemetry["stop_reason"] == "WAITING_BACKOFF"
        assert (
            json.loads(waiting_telemetry["payload_json"])["payload"]["status"]
            == "WAITING_BACKOFF"
        )
    assert second["status"] == "WAITING_BACKOFF"
    assert second["catch_up_completion_status"] == "WAITING_BACKOFF"
    assert second["claimed"] == 0
    assert second["provider_calls"] == 0
    assert second["resolver_evaluations"] == 0
    assert second["actual_provider_requests"] == 0
    assert second["ai_invocations"] == second["ai_jobs_created"] == 0
    assert second["writes"] == 0
    assert second["checkpoint_written"] is False
    assert second["telemetry_emitted"] is False
    assert second["catch_up_next_retry_at"] == first["catch_up_next_retry_at"]
    with connect_sqlite(settings.database_path) as conn:
        checkpoint_after = conn.execute(
            """
            SELECT payload_json,updated_at,next_retry_at
            FROM provider_state
            WHERE state_key='event_calendar_lifecycle_catchup'
            """
        ).fetchone()
        assert checkpoint_after is not None
        assert dict(checkpoint_after) == dict(checkpoint_before)
        assert conn.execute(
            "SELECT COUNT(*) FROM service_telemetry_events"
        ).fetchone()[0] == telemetry_before
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0] == snapshots_before
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_outbox"
        ).fetchone()[0] == outbox_before
        assert conn.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0] == 0


@pytest.mark.parametrize("offline_days", [7, 30, 365, 730])
def test_catchup_lookback_accepts_required_downtime_boundaries(
    tmp_path: Path,
    offline_days: int,
) -> None:
    settings = cfg(tmp_path)
    release = NOW - timedelta(days=offline_days)
    _seed_due_macro(
        settings,
        key=f"macro:downtime:{offline_days}",
        release=release,
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    claimed: list[str] = []
    result = scheduler.startup_catch_up(
        resolver=lambda item: (
            claimed.append(str(item["entity_key"]))
            or {
                "status": "DEFERRED",
                "reason": "offline_fixture_provider_delay",
            }
        ),
        ai_enqueue=lambda _: pytest.fail("AI must remain unreachable"),
        execution_context=ExecutionContext.provider_only(
            correlation_id=f"downtime-{offline_days}",
            allow_live_providers=True,
        ),
    )

    assert result["claimed"] == 1
    assert claimed == [f"macro:downtime:{offline_days}"]
    assert result["status"] == "WAITING_BACKOFF"
    assert result["ai_invocations"] == 0
    assert result["catch_up_completion_status"] == "WAITING_BACKOFF"


def test_bucket_aware_retention_preserves_each_nonempty_week_and_exact_counts(
    tmp_path: Path,
) -> None:
    events = [
        calendar_event(
            "previous:published",
            "2026-07-15T08:30:00-04:00",
            event_type="CPI",
            category="MACRO",
            source="BLS",
            source_url="https://www.bls.gov/cpi/",
            actual="2.7",
            release_status="REVISED",
            impact="LOW",
        ),
        calendar_event(
            "previous:low",
            "2026-07-16T08:30:00-04:00",
            event_type="CPI",
            category="MACRO",
            source="BLS",
            source_url="https://www.bls.gov/cpi/",
            impact="HIGH",
        ),
        calendar_event(
            "current:today",
            "2026-07-22T14:00:00-04:00",
            event_type="CPI",
            category="MACRO",
            source="BLS",
            source_url="https://www.bls.gov/cpi/",
            impact="LOW",
        ),
        calendar_event(
            "current:other",
            "2026-07-23T14:00:00-04:00",
            event_type="CPI",
            category="MACRO",
            source="BLS",
            source_url="https://www.bls.gov/cpi/",
            impact="HIGH",
        ),
        calendar_event(
            "next:high",
            "2026-07-28T08:30:00-04:00",
            event_type="CPI",
            category="MACRO",
            source="BLS",
            source_url="https://www.bls.gov/cpi/",
        ),
        calendar_event(
            "next:low",
            "2026-07-29T08:30:00-04:00",
            event_type="CPI",
            category="MACRO",
            source="BLS",
            source_url="https://www.bls.gov/cpi/",
            impact="LOW",
        ),
    ]
    settings = cfg(
        tmp_path,
        event_calendar_consumer_max_events=3,
    )
    window = build_event_calendar_window(
        snapshot_payload(critical=events),
        settings=settings,
        now=NOW,
    )

    assert [
        window[key]["retained_count"]
        for key in ("previous_week", "current_week", "next_week")
    ] == [2, 2, 2]
    assert window["previous_week"]["events"][0]["occurrence_id"] == (
        "previous:published"
    )
    assert window["current_week"]["events"][0]["occurrence_id"] == (
        "current:today"
    )
    assert window["next_week"]["events"][0]["occurrence_id"] == "next:high"
    assert window["coverage"]["candidate_count"] == 6
    assert window["coverage"]["retained_count"] == 6
    assert window["coverage"]["omitted_count"] == 0
    assert window["coverage"]["status"] == "COMPLETE"
    assert window["coverage"]["minimum_per_nonempty_bucket_preserved"] is True


def test_impossible_bucket_minimum_is_explicitly_degraded_under_byte_budget(
    tmp_path: Path,
) -> None:
    huge = "X" * 20_000
    events = [
        calendar_event(
            f"{bucket}:huge",
            when,
            event_type="CPI",
            category="MACRO",
            source="BLS",
            source_url="https://www.bls.gov/cpi/",
            title=huge,
        )
        for bucket, when in (
            ("previous", "2026-07-15T08:30:00-04:00"),
            ("current", "2026-07-22T08:30:00-04:00"),
            ("next", "2026-07-28T08:30:00-04:00"),
        )
    ]
    settings = cfg(tmp_path)
    payload = snapshot_payload(critical=events)
    window = build_event_calendar_window(payload, settings=settings, now=NOW)
    payload["event_calendar_window"] = window
    consumer = build_ai_trader_consumer_v2(payload, settings=settings)

    assert window["coverage"]["status"] == "COMPLETE"
    assert window["coverage"]["minimum_per_nonempty_bucket_preserved"] is True
    assert window["coverage"]["truncation_reason"] is None
    assert len(json.dumps(consumer).encode("utf-8")) > 90_000


def test_missing_event_is_unconfirmed_until_allowed_source_confirms_removal(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path, event_calendar_catchup_enabled=False)
    scheduled = calendar_event(
        "macro:removed",
        "2026-07-24T08:30:00-04:00",
        event_type="CPI",
        category="MACRO",
        source="BLS",
        source_url="https://www.bls.gov/cpi/",
    )
    repository = MarketContextSnapshotRepository(settings)
    repository.save_next(
        symbol="MNQ",
        refresh_mode="removal_baseline",
        debug_payload=snapshot_payload(critical=[scheduled]),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    unconfirmed = repository.save_next(
        symbol="MNQ",
        refresh_mode="removal_unconfirmed",
        debug_payload=snapshot_payload(),
        ai_enrichment={"status": "NOT_REQUIRED"},
        trigger_type="macro_schedule",
    )

    assert MarketContextOutboxRepository(settings).list_events() == []
    removal = unconfirmed["debug_payload"]["event_calendar_window"]["audit"][
        "comparison"
    ]["removals"][0]
    assert removal["status"] == "UNCONFIRMED_REMOVAL"
    assert removal["trigger_class"] == "NON_TRIGGERING"

    rejected = repository.save_next(
        symbol="MNQ",
        refresh_mode="removal_rejected_source",
        debug_payload=snapshot_payload(
            removals=[
                {
                    "occurrence_id": "macro:removed",
                    "release_status": "CANCELLED",
                    "source": "untrusted fixture",
                    "source_url": "https://untrusted.invalid/cancelled",
                    "retrieved_at": NOW.isoformat(),
                }
            ]
        ),
        ai_enrichment={"status": "NOT_REQUIRED"},
        trigger_type="macro_schedule",
    )
    assert MarketContextOutboxRepository(settings).list_events() == []
    rejected_removal = rejected["debug_payload"]["event_calendar_window"][
        "audit"
    ]["comparison"]["removals"][0]
    assert rejected_removal["status"] == "UNCONFIRMED_REMOVAL"

    repository.save_next(
        symbol="MNQ",
        refresh_mode="removal_confirmed",
        debug_payload=snapshot_payload(
            removals=[
                {
                    "occurrence_id": "macro:removed",
                    "release_status": "CANCELLED",
                    "source": "BLS",
                    "source_url": "https://www.bls.gov/cpi/",
                    "retrieved_at": NOW.isoformat(),
                }
            ]
        ),
        ai_enrichment={"status": "NOT_REQUIRED"},
        trigger_type="macro_schedule",
    )

    outbox = MarketContextOutboxRepository(settings).list_events()
    assert len(outbox) == 1
    assert outbox[0]["trigger_type"] == "event_cancelled"
    assert outbox[0]["changed_event_ids"] == ["macro:removed"]
