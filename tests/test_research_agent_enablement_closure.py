from __future__ import annotations

from datetime import UTC, datetime, timedelta
import importlib
import sqlite3
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.main import run_lifecycle_due_scan
from app.models.events import EconomicEvent, EventEnrichment, Impact
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_worker import AIResearchWorker
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
    persist_lifecycle_in_transaction,
)
from app.services.execution_context import ExecutionContext
from app.services.db_only_market_context_materializer import (
    DBOnlyMarketContextMaterializer,
)
from app.services.lifecycle_due_resolver import DeterministicLifecycleDueResolver
from app.services.market_context_outbox_service import MarketContextOutboxRepository
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_fact_repository import MarketFactRepository
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.temporal_domain_service import canonical_event_key
from app.services.research_agent_enablement import (
    RESEARCH_AGENT_REGISTRY,
    is_research_agent_enabled,
    research_agent_enablement,
    validate_research_agent_mapping,
)
from app.services.research_gap_manifest import ResearchGapManifestBuilder


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 24, 12, tzinfo=UTC)


def cfg(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "model_pricing_path": ROOT / "config" / "model_pricing.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "environment": "test",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_registry_exactly_maps_all_specialized_profiles_and_defaults(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    validate_research_agent_mapping()
    assert len(RESEARCH_AGENT_REGISTRY) == 13
    assert {item.topic for item in RESEARCH_AGENT_REGISTRY} == {
        "macro_events",
        "fed_rates",
        "vix_risk",
        "cot_positioning",
        "nasdaq_100",
        "mega_cap_semiconductors",
        "earnings",
        "news",
        "geopolitical_regulatory_risk",
        "options_positioning",
        "market_internals",
        "cross_asset_context",
        "earnings_intelligence",
    }
    assert all(
        is_research_agent_enabled(settings, topic=item.topic)
        for item in RESEARCH_AGENT_REGISTRY[:9]
    )
    assert all(
        not is_research_agent_enabled(settings, topic=item.topic)
        for item in RESEARCH_AGENT_REGISTRY[9:]
    )


def test_master_switch_disables_every_agent(tmp_path: Path) -> None:
    settings = cfg(tmp_path, research_agents_enabled=False)
    for item in RESEARCH_AGENT_REGISTRY:
        decision = research_agent_enablement(settings, profile_id=item.profile_id)
        assert decision["agent_enabled"] is False
        assert decision["reason"] == "research_agents_master_disabled"


def test_disabled_optional_topics_are_not_requested_or_blocking(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    manifest = ResearchGapManifestBuilder(settings, clock=lambda: NOW).build(
        snapshot=None,
        components={},
    )
    by_topic = {item["topic"]: item for item in manifest["items"]}
    for topic in (
        "options_positioning",
        "market_internals",
        "cross_asset_context",
        "earnings_intelligence",
    ):
        assert by_topic[topic]["agent_enabled"] is False
        assert by_topic[topic]["required_action"] == "NONE"
        assert by_topic[topic]["ai_eligible"] is False
        assert by_topic[topic]["execution_status"] == "NOT_REQUESTED"
        assert by_topic[topic]["data_outcome"] == "DISABLED"
    assert set(manifest["disabled_topics"]) == {
        "options_positioning",
        "market_internals",
        "cross_asset_context",
        "earnings_intelligence",
    }
    assert set(manifest["requested_topics"]).isdisjoint(manifest["disabled_topics"])


def test_disabled_service_and_repository_do_not_create_jobs(tmp_path: Path) -> None:
    settings = cfg(tmp_path, research_agent_news_enabled=False)
    service = AIResearchJobService(settings, clock=lambda: NOW)
    job, created = service.enqueue_explicit(
        job_type="NEWS_RESEARCH",
        symbol="MNQ",
        correlation_id="disabled-service",
        request_payload={"missing_fields": ["articles"]},
        specialized_topic="news",
        execution_context=ExecutionContext.explicit_ai(
            correlation_id="disabled-service",
        ),
    )
    assert created is False
    assert job["status"] == "REJECTED"
    assert job["last_error"] == "AGENT_DISABLED"

    direct, direct_created = AIResearchJobRepository(settings).enqueue(
        idempotency_key="disabled-repository",
        job_type="NEWS_RESEARCH",
        symbol="MNQ",
        correlation_id="disabled-repository",
        request_payload={
            "execution_context": ExecutionContext.explicit_ai(
                correlation_id="disabled-repository",
            ).as_payload()
        },
        policy_version="test",
        prompt_version="test",
        profile_id="NEWS_RESEARCH",
        specialized_topic="news",
    )
    assert direct_created is False
    assert direct["status"] == "REJECTED"
    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM ai_research_jobs").fetchone()[0] == 0


def test_worker_rejects_preexisting_disabled_job_before_backend(tmp_path: Path) -> None:
    enabled = cfg(tmp_path)
    repository = AIResearchJobRepository(enabled, clock=lambda: NOW)
    job, created = repository.enqueue(
        idempotency_key="queued-before-disable",
        job_type="NEWS_RESEARCH",
        symbol="MNQ",
        correlation_id="queued-before-disable",
        request_payload={
            "execution_context": ExecutionContext.explicit_ai(
                correlation_id="queued-before-disable",
            ).as_payload()
        },
        policy_version="test",
        prompt_version="test",
        profile_id="NEWS_RESEARCH",
        specialized_topic="news",
    )
    assert created is True
    calls: list[str] = []
    disabled = cfg(tmp_path, research_agent_news_enabled=False)
    worker = AIResearchWorker(
        disabled,
        repository=AIResearchJobRepository(disabled, clock=lambda: NOW),
        executor=lambda *_: calls.append("backend") or {"status": "SUCCEEDED"},
        worker_id="disabled-worker",
    )
    assert worker.process_once() is False
    assert calls == []
    stored = repository.get(job["job_id"])
    assert stored is not None
    assert stored["status"] == "REJECTED"
    assert stored["last_error"] == "AGENT_DISABLED"
    assert stored["attempts"] == 0
    assert stored["retry_class"] == "NON_RETRYABLE"


async def test_real_async_due_scan_wiring_enqueues_residual_ai_once(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
    )
    lifecycle = compute_datum_lifecycle(
        "macro_actual",
        "CPI:2026-07",
        {
            "value": 20,
            "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
        },
        settings=settings,
        now=NOW,
    )
    LifecycleRepository(settings, clock=lambda: NOW).upsert(lifecycle)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    state = {
        "research_scheduler": scheduler,
        "lifecycle_due_resolver": DeterministicLifecycleDueResolver(
            settings,
            clock=lambda: NOW,
        ),
    }
    result = await run_lifecycle_due_scan(state)
    assert result["resolver_evaluations"] == 1
    assert result["actual_provider_requests"] == result["provider_calls"] == 0
    assert result["ai_invocations"] == 1
    stored = LifecycleRepository(settings).list_items()[0]
    assert stored["work_status"] == "QUEUED"
    assert stored["refresh_reason"] == "provider_exhausted_ai_queued"


async def test_real_lifespan_registers_and_runs_ap_scheduler_due_job(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
        research_premarket_enabled=False,
        research_session_enabled=False,
        research_postmarket_enabled=False,
        research_event_triggers_enabled=False,
        research_news_enabled=False,
        storage_cleanup_interval_hours=24,
    )
    main_module = importlib.import_module("app.main")
    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    async with main_module.lifespan(main_module.app):
        scheduler = main_module.app.state.scheduler
        job = scheduler.get_job("lifecycle_due_scanner")
        assert job is not None
        assert job.func is main_module.run_lifecycle_due_scan
        assert job.args[0]["lifecycle_due_resolver"].__class__ is (
            DeterministicLifecycleDueResolver
        )
        result = await job.func(*job.args, **job.kwargs)
        assert result["status"] == "COMPLETED"
        assert result["claimed"] == 0


def test_future_earnings_without_actual_waits_until_event_window(tmp_path: Path) -> None:
    settings = cfg(tmp_path, lifecycle_no_data_retry_seconds="30,120")
    event_at = NOW + timedelta(days=7)
    item = compute_datum_lifecycle(
        "earnings_intelligence",
        "GOOGL:2026-07-31",
        {
            "ticker": "GOOGL",
            "event_at": event_at.isoformat(),
            "expected_eps": 2.0,
            "actual_eps": None,
        },
        settings=settings,
        now=NOW,
    )
    assert item.freshness_state == "FRESH"
    assert item.next_retry_at is None
    assert datetime.fromisoformat(str(item.next_refresh_at)) >= event_at


def test_earnings_issuer_events_are_distinct_and_restart_deduplicated(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    for offset, ticker in enumerate(("GOOGL", "TSLA", "AMD"), start=1):
        event_at = NOW + timedelta(days=offset)
        item = compute_datum_lifecycle(
            "earnings_intelligence",
            f"{ticker}:{event_at.date().isoformat()}",
            {
                "ticker": ticker,
                "event_at": event_at.isoformat(),
                "expected_eps": 0,
            },
            settings=settings,
            now=NOW,
        )
        repository.upsert(item, payload={"ticker": ticker})
    restarted = LifecycleRepository(settings, clock=lambda: NOW)
    amd = next(
        item for item in restarted.list_items() if item["entity_key"].startswith("AMD:")
    )
    restarted.upsert(
        compute_datum_lifecycle(
            "earnings_intelligence",
            amd["entity_key"],
            {
                "ticker": "AMD",
                "event_at": (NOW + timedelta(days=3)).isoformat(),
                "expected_eps": 0,
            },
            settings=settings,
            now=NOW,
        ),
        payload={"ticker": "AMD"},
    )
    stored = restarted.list_items()
    assert len(stored) == 3
    assert {item["payload"]["ticker"] for item in stored} == {
        "GOOGL",
        "TSLA",
        "AMD",
    }


def test_numeric_zero_is_material_for_all_lifecycle_domains(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    for entity_type, datum in (
        (
            "earnings_actual",
            {
                "event_at": (NOW - timedelta(days=30)).isoformat(),
                "published_at": NOW.isoformat(),
                "actual_eps": 0,
                "eps_surprise": 0,
            },
        ),
        ("cot", {"report_date": "2026-07-21", "net_position": 0, "open_interest": 0}),
        ("fed_rates", {"rate": 0}),
        ("options_positioning", {"open_interest": 0, "change": 0}),
    ):
        item = compute_datum_lifecycle(
            entity_type,
            f"{entity_type}:zero",
            datum,
            settings=settings,
            now=NOW,
        )
        assert item.freshness_state == "FRESH"


def test_atomic_upsert_refreshes_every_authoritative_field(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    first = compute_datum_lifecycle(
        "earnings_actual",
        "TSLA:2026-Q2",
        {
            "event_at": (NOW - timedelta(hours=1)).isoformat(),
            "actual_eps": None,
            "source_lineage": [{"source": "schedule"}],
            "acquisition_method": "provider",
        },
        settings=settings,
        now=NOW,
    )
    repository.upsert(first, payload={"stage": "schedule"})
    second = compute_datum_lifecycle(
        "earnings_actual",
        "TSLA:2026-Q2",
        {
            "event_at": (NOW - timedelta(hours=1)).isoformat(),
            "published_at": NOW.isoformat(),
            "actual_eps": 0,
            "source_lineage": [{"source": "issuer"}],
            "acquisition_method": "official_endpoint",
        },
        settings=settings,
        now=NOW + timedelta(minutes=1),
        triggering_event="earnings_actual",
    )
    stamp = (NOW + timedelta(minutes=1)).isoformat()
    with connect_sqlite(settings.database_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        persist_lifecycle_in_transaction(
            conn,
            second,
            payload={"stage": "actual"},
            work_status="COMPLETED",
            timestamp=stamp,
        )
        conn.commit()
    stored = repository.list_items()[0]
    assert stored["freshness_state"] == "FRESH"
    assert stored["published_at"] == NOW.isoformat()
    assert stored["source_lineage"] == [{"source": "issuer"}]
    assert stored["acquisition_method"] == "official_endpoint"
    assert stored["triggering_event"] == "earnings_actual"
    assert stored["payload"] == {"stage": "actual"}


def test_worker_materialization_propagates_real_trigger_to_atomic_outbox(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    release = NOW - timedelta(minutes=5)
    event = EconomicEvent(
        event_id="cpi-trigger",
        name="Consumer Price Index",
        country="US",
        category="CPI",
        metric_id="headline_cpi_mom",
        normalized_event_family="CPI",
        reference_period="2026-07",
        frequency="monthly",
        date=release.date().isoformat(),
        time_utc=release,
        impact=Impact.HIGH,
        source="BLS",
        source_url="https://www.bls.gov/cpi/",
        reliability=0.99,
        event_risk_level=Impact.HIGH,
        enrichment=EventEnrichment(
            forecast="0.3",
            consensus="0.3",
            previous="0.2",
            metrics=[
                {
                    "metric_id": "headline_cpi_mom",
                    "period": "2026-07",
                    "frequency": "monthly",
                    "unit": "percent",
                    "seasonal_adjustment": "SA",
                }
            ],
        ),
    )
    key = canonical_event_key(event)
    facts = MarketFactRepository(settings)
    facts.upsert_economic_event(event, key)
    debug = {
        "symbol": "MNQ",
        "generated_at_utc": (NOW - timedelta(minutes=10)).isoformat(),
        "market_schedule": {
            "status": "AVAILABLE",
            "context_date": NOW.date().isoformat(),
            "market_session_status": "open",
        },
        "event_calendar": {
            "critical_macro_events": [event.model_dump(mode="json")],
            "fed_communications": [],
            "other_economic_events": [],
        },
        "events_today": [event.model_dump(mode="json")],
        "macro_snapshot": {},
        "risk_context": {},
        "nasdaq_context": {},
        "news_context": {},
        "rates_expectations": {},
        "positioning": {},
        "sentiment_context": {},
        "data_quality": {},
    }
    snapshots = MarketContextSnapshotRepository(settings)
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="seed",
        debug_payload=debug,
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    facts.apply_official_event_actual(
        canonical_event_key=key,
        candidate={
            "value": "0.5",
            "source": "BLS",
            "publisher": "BLS",
            "source_url": "https://www.bls.gov/cpi/",
            "canonical_url": "https://www.bls.gov/cpi/",
            "source_domain": "bls.gov",
            "source_tier": 1,
            "source_classification": "OFFICIAL",
            "metric_id": "headline_cpi_mom",
            "event_metric_id": "headline_cpi_mom",
            "source_series_id": "CUSR0000SA0",
            "transformation": "pct_change_mom",
            "seasonal_adjustment": "SA",
            "period": "2026-07",
            "frequency": "monthly",
            "unit": "percent",
            "evidence_text": "Official CPI value.",
            "retrieved_at": NOW.isoformat(),
            "reliability": 0.99,
        },
        policy_version="source-policy-v1",
    )
    materialized = DBOnlyMarketContextMaterializer(
        settings,
        facts=facts,
        snapshots=snapshots,
        clock=lambda: NOW,
    ).materialize_for_job(
        job={
            "job_id": "actual-trigger-job",
            "symbol": "MNQ",
            "event_key": key,
            "request_payload": {
                "trigger_envelope": {
                    "trigger_type": "macro_actual",
                    "trigger_entity": key,
                    "trace_id": "trace-actual",
                    "correlation_id": "correlation-actual",
                }
            },
        },
        ai_enrichment={"status": "SUCCEEDED"},
    )
    assert materialized is not None
    events = MarketContextOutboxRepository(settings).list_events(status=None)
    assert len(events) == 1
    assert events[0]["trigger_type"] == "macro_actual"
    assert events[0]["snapshot_id"] == materialized["snapshot_id"]


def test_full_supported_migration_matrix_reopens_idempotently(
    tmp_path: Path,
) -> None:
    for source_version in range(1, len(MIGRATIONS) + 1):
        database = tmp_path / f"source-{source_version}.sqlite"
        with sqlite3.connect(database) as conn:
            conn.execute(
                "CREATE TABLE schema_migrations("
                "version INTEGER PRIMARY KEY,name TEXT,applied_at TEXT)"
            )
            for version, (name, sql) in enumerate(
                MIGRATIONS[:source_version],
                start=1,
            ):
                for statement in _split_sql(sql):
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO schema_migrations VALUES (?,?,?)",
                    (version, name, NOW.isoformat()),
                )
            conn.execute(f"PRAGMA user_version={source_version}")
            conn.commit()
        first = migrate_database(database)
        second = migrate_database(database)
        assert first["schema_version"] == len(MIGRATIONS)
        assert second["applied"] == []
        with sqlite3.connect(database) as conn:
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
