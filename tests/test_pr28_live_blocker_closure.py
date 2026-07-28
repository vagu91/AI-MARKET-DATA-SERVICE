from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.api import routes
from app.core.config import Settings
from app.models.events import EconomicEvent
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.diagnostics_service import DiagnosticsService
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.event_value_candidate_repository import (
    EventValueCandidateRepository,
)
from app.services.execution_context import ExecutionContext
from app.services.market_fact_repository import MarketFactRepository
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.temporal_domain_service import (
    exact_occurrence_key,
    reconcile_calendar_events,
    temporal_event_state,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    ROOT
    / "tests"
    / "fixtures"
    / "pr28_live_actual_reconciliation_redacted.json"
)
NOW = datetime(2026, 7, 27, 17, 48, 3, tzinfo=UTC)


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "environment": "test",
        "database_path": tmp_path / "pr28-live-blockers.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": False,
        "research_agents_enabled": False,
        "research_agent_macro_events_enabled": False,
        "ai_worker_enabled": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def fixture() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def initial_events() -> list[EconomicEvent]:
    return [
        EconomicEvent.model_validate(item)
        for item in fixture()["initial_events"]
    ]


def table_count(cfg: Settings, table: str) -> int:
    with sqlite3.connect(cfg.database_path) as connection:
        return int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
        )


def test_non_official_calendar_actuals_remain_fail_closed_before_snapshot(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    evidence = fixture()
    facts = MarketFactRepository(cfg, clock=lambda: NOW)
    for event in initial_events():
        assert facts.upsert_economic_event(
            event,
            event_key=exact_occurrence_key(event),
        )
    EventValueCandidateRepository(cfg).persist_provider_payload(
        evidence["provider_payload"]
    )

    reconciled = reconcile_calendar_events(
        initial_events(),
        [evidence["provider_payload"]],
        now=NOW,
        temporal_validation=facts.temporal_validation,
    )
    by_occurrence = {
        event.occurrence_id: event
        for event in reconciled
    }
    expected = {
        "xtb:146392:2026-07-24": (610.0, 580.0),
        "xtb:146945:2026-07-24": (51.5, 51.2),
    }
    assert set(expected).issubset(by_occurrence)
    for occurrence_id, values in expected.items():
        event = by_occurrence[occurrence_id]
        forecast, previous = values
        assert event.actual is None
        assert float(event.enrichment.forecast) == forecast
        assert float(event.enrichment.previous) == previous
        assert (
            temporal_event_state(event, now=NOW)["temporal_status"]
            == "AWAITING_ACTUAL"
        )
        audit = event.enrichment.summary[
            "provider_actual_reconciliation"
        ]
        assert audit["status"] == "REJECTED"
        assert audit["atomic_record"] is True
        assert audit["official_actual"] is False
        assert "actual_requires_official_source" in audit[
            "rejected_candidates"
        ][0]["reasons"]
        assert facts.upsert_economic_event(
            event,
            event_key=exact_occurrence_key(event),
        )

    restored = {
        item["occurrence_id"]: item
        for item in facts.economic_event_payloads(
            country="US",
            start_date="2026-07-24",
            end_date="2026-07-24",
        )
    }
    for occurrence_id, values in expected.items():
        forecast, previous = values
        item = restored[occurrence_id]
        assert item["actual"] is None
        assert float(item["forecast"]) == forecast
        assert float(item["previous"]) == previous


def test_discordant_complete_provider_records_fail_closed_and_are_audited() -> None:
    evidence = fixture()
    conflicting = json.loads(
        json.dumps(evidence["provider_payload"])
    )
    conflicting["source"] = "Independent Calendar"
    conflicting["source_url"] = "https://calendar.example.test/releases"
    conflicting["items"] = [conflicting["items"][0]]
    conflicting["items"][0]["source"] = "Independent Calendar"
    conflicting["items"][0][
        "source_url"
    ] = "https://calendar.example.test/releases"
    conflicting["items"][0]["actual"] = 629.0
    conflicting["items"][0]["lineage"]["actual"][
        "source"
    ] = "Independent Calendar"

    reconciled = reconcile_calendar_events(
        initial_events(),
        [evidence["provider_payload"], conflicting],
        now=NOW,
    )
    event = next(
        item
        for item in reconciled
        if item.occurrence_id == "xtb:146392:2026-07-24"
    )
    assert event.actual is None
    assert (
        temporal_event_state(event, now=NOW)["temporal_status"]
        == "AWAITING_ACTUAL"
    )
    audit = event.enrichment.summary[
        "provider_actual_reconciliation"
    ]
    assert audit["status"] == "REJECTED"
    assert audit["candidate_count"] == 0
    assert audit["rejected_candidate_count"] == 2
    assert all(
        "actual_requires_official_source" in item["reasons"]
        for item in audit["rejected_candidates"]
    )


def test_provider_only_context_creates_zero_jobs_at_service_and_repository(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    context = ExecutionContext.provider_only(
        correlation_id="pr28-provider-force",
        allow_live_providers=True,
    )
    jobs = AIResearchJobService(cfg).enqueue_temporal_refreshes(
        initial_events(),
        correlation_id=context.correlation_id,
        now=NOW,
        execution_context=context,
    )
    assert jobs == []
    direct, created = AIResearchJobRepository(cfg).enqueue(
        idempotency_key="pr28-provider-force-direct",
        job_type="RELEASE_ACTUAL_REFRESH",
        symbol="MNQ",
        event_key="xtb:146392:2026-07-24",
        correlation_id=context.correlation_id,
        request_payload={
            "execution_context": context.as_payload(),
        },
        policy_version="fixture",
        prompt_version="fixture",
    )
    assert created is False
    assert direct["last_error"] == "AI_NOT_AUTHORIZED"
    assert table_count(cfg, "ai_research_jobs") == 0
    assert table_count(cfg, "research_runs") == 0
    assert table_count(cfg, "research_backend_invocations") == 0
    with sqlite3.connect(cfg.database_path) as connection:
        telemetry = connection.execute(
            """
            SELECT payload_json
            FROM service_telemetry_events
            WHERE event_name='ai_authorization'
            ORDER BY rowid DESC
            LIMIT 1
            """
        ).fetchone()
    assert telemetry is not None
    persisted_context = json.loads(telemetry[0])["payload"][
        "execution_context"
    ]
    assert persisted_context == context.as_payload()


@pytest.mark.asyncio
async def test_force_preflight_defers_snapshot_to_the_route_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = settings(tmp_path)
    calls: list[dict[str, Any]] = []

    class Scheduler:
        def __init__(self, _settings: Settings) -> None:
            pass

        def _seed_canonical_schedule_gaps(self, **kwargs: Any) -> dict[str, Any]:
            calls.append(kwargs)
            return {
                "status": "VERIFIED_COMPLETE",
                "snapshot_writes": 0,
                "outbox_writes": 0,
                "materialization_deferred": True,
            }

    monkeypatch.setattr(
        "app.services.diagnostics_service.ResearchSchedulerService",
        Scheduler,
    )
    orchestrator = EnrichmentOrchestrator(
        cfg,
        event_enrichment_service=None,
    )
    materializations: list[str] = []

    class RouteDiagnostics:
        def __init__(self, *_args: Any, **kwargs: Any) -> None:
            self.delegate = object.__new__(DiagnosticsService)
            self.delegate.settings = cfg
            self.delegate.event_service = kwargs["event_service"]

        async def full_model(self, **_kwargs: Any) -> dict[str, Any]:
            preflight = await self.delegate._force_schedule_catch_up(
                now=NOW
            )
            assert preflight["materialization_deferred"] is True
            context = ExecutionContext.provider_only(
                correlation_id="pr28-route-provider-force",
                allow_live_providers=True,
            )
            events, metadata = await orchestrator.enrich_events(
                events=initial_events(),
                country="US",
                start=NOW,
                end=NOW,
                trigger="diagnostics_full_model_force",
                force=True,
                execution_context=context,
            )
            assert metadata["data_quality"]["ai_research_requests"] == 0
            return {
                "symbol": "MNQ",
                "generated_at_utc": NOW.isoformat(),
                "upcoming_events": [
                    event.model_dump(mode="json")
                    for event in events
                ],
            }

    class Runtime:
        async def enrich_market_context(
            self,
            contract: dict[str, Any],
            *,
            refresh: str,
            trigger_type: str | None = None,
        ) -> dict[str, Any]:
            del refresh, trigger_type
            return contract

    def materialize(
        contract: dict[str, Any],
        **_kwargs: Any,
    ) -> dict[str, Any]:
        materializations.append("final")
        return contract

    monkeypatch.setattr(routes, "DiagnosticsService", RouteDiagnostics)
    monkeypatch.setattr(routes, "_materialize_market_context", materialize)
    result = await routes.market_context_mnq(
        refresh="force",
        view="debug",
        macro_service=object(),
        event_service=SimpleNamespace(
            list_events=lambda **_kwargs: []
        ),
        event_window_service=object(),
        nasdaq_service=object(),
        enrichment_orchestrator=orchestrator,
        deterministic_runtime=Runtime(),
    )
    assert result["symbol"] == "MNQ"
    assert len(calls) == 1
    assert calls[0]["materialize_snapshot"] is False
    assert materializations == ["final"]
    assert table_count(cfg, "market_context_snapshots") == 0
    assert table_count(cfg, "market_context_outbox") == 0
    assert table_count(cfg, "ai_research_jobs") == 0
    assert table_count(cfg, "research_backend_invocations") == 0


def test_schedule_fixed_point_has_zero_writes_and_keeps_twenty_one_dates(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    calls: list[dict[str, Any]] = []

    class EmptyCompleteAcquire:
        last_provider_results: list[Any] = []
        coverage_proof = {
            "request_succeeded": True,
            "scope_match": True,
            "pagination_complete": True,
            "parsing_succeeded": True,
            "records_valid": True,
            "expected_sources_complete": True,
            "authentic_empty": True,
        }

        def __call__(self, **kwargs: Any) -> list[Any]:
            calls.append(kwargs)
            return []

    scheduler = ResearchSchedulerService(cfg, clock=lambda: NOW)
    first = scheduler._seed_canonical_schedule_gaps_unleased(
        schedule_acquire=EmptyCompleteAcquire(),
        now=NOW,
        materialize_snapshot=False,
    )
    before_provider_state = table_count(cfg, "provider_state")
    before_lifecycle = table_count(cfg, "datum_lifecycle_items")
    second = scheduler._seed_canonical_schedule_gaps_unleased(
        schedule_acquire=EmptyCompleteAcquire(),
        now=NOW,
        materialize_snapshot=False,
    )
    assert len(first["requested_dates"]) == 21
    assert len(second["requested_dates"]) == 21
    assert second["provider_calls_due"] == 0
    assert second["provider_calls_executed"] == 0
    assert second["canonical_writes"] == 0
    assert second["persisted_gap_count"] == 0
    assert second["coverage_metadata_writes"] == 0
    assert second["snapshot_writes"] == 0
    assert second["outbox_writes"] == 0
    assert table_count(cfg, "ai_research_jobs") == 0
    assert table_count(cfg, "research_backend_invocations") == 0
    assert table_count(cfg, "provider_state") == before_provider_state
    assert table_count(cfg, "datum_lifecycle_items") == before_lifecycle
    with sqlite3.connect(cfg.database_path) as connection:
        covered_dates = connection.execute(
            """
            SELECT COUNT(DISTINCT coverage_date)
            FROM event_calendar_coverage
            """
        ).fetchone()[0]
    assert covered_dates == 21
    assert len(calls) == 1
