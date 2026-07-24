from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.core.config import Settings
from app.infrastructure.persistence.migrations import migrate_database, _split_sql
from app.infrastructure.persistence.schema import MIGRATIONS
from app.models.common import Freshness, Impact, ProviderMetadata, ProviderResult, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_worker import AIResearchWorker
from app.services.ai_trader_consumer_v2_service import build_ai_trader_consumer_v2
from app.services.market_fact_repository import MarketFactRepository, connect_market_db
from app.services.market_news_repository import MarketNewsRepository
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.temporal_domain_service import (
    canonical_event_key,
    reconcile_calendar_events,
    temporal_event_state,
)


POLICY = Path(__file__).resolve().parents[1] / "config" / "source_policy.json"


def settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": True,
        "ai_worker_enabled": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def event(*, release: datetime, name: str = "Consumer Price Index", actual=None) -> EconomicEvent:
    return EconomicEvent(
        event_id="cpi-1",
        name=name,
        country="US",
        category="CPI",
        date=release.date().isoformat(),
        metric_id="headline_cpi_mom",
        normalized_event_family="CPI",
        reference_period=release.strftime("%Y-%m"),
        frequency="monthly",
        time_utc=release,
        impact=Impact.HIGH,
        actual=actual,
        source="BLS",
        source_url="https://bls.gov/cpi",
        reliability=0.99,
        event_risk_level=Impact.HIGH,
        enrichment=EventEnrichment(
            forecast="0.3",
            consensus="0.3",
            previous="0.2",
            actual=actual,
            metrics=[
                {
                    "metric_id": "headline_cpi_mom",
                    "period": release.strftime("%Y-%m"),
                    "frequency": "monthly",
                    "unit": "percent",
                    "seasonal_adjustment": "SA",
                }
            ],
        ),
    )


def test_job_queue_is_persistent_idempotent_and_recovers_expired_lease(tmp_path: Path) -> None:
    now = [datetime(2026, 7, 22, 10, 0, tzinfo=UTC)]
    cfg = settings(tmp_path, ai_job_lease_seconds=5)
    repo = AIResearchJobRepository(cfg, clock=lambda: now[0])
    service = AIResearchJobService(cfg, repository=repo)
    first, created = service.enqueue_explicit(
        job_type="MISSING_EVENT_RESEARCH",
        symbol="MNQ",
        correlation_id="corr-1",
        request_payload={"pending_fields": ["forecast"]},
        pending_fields=["forecast"],
    )
    duplicate, created_again = service.enqueue_explicit(
        job_type="MISSING_EVENT_RESEARCH",
        symbol="MNQ",
        correlation_id="corr-2",
        request_payload={"pending_fields": ["forecast"]},
        pending_fields=["forecast"],
    )
    assert created is True and created_again is False
    assert duplicate["job_id"] == first["job_id"]
    assert repo.acquire_next("worker-a")["status"] == "RUNNING"
    now[0] += timedelta(seconds=6)
    assert repo.recover_abandoned() == 1
    assert repo.get(first["job_id"])["status"] == "RETRY_SCHEDULED"


def test_release_retry_backoff_is_exact_and_survives_repository_reopen(tmp_path: Path) -> None:
    now = [datetime(2026, 7, 22, 10, 0, tzinfo=UTC)]
    cfg = settings(tmp_path, release_refresh_retry_seconds="30,120,300,900,1800,3600")
    repo = AIResearchJobRepository(cfg, clock=lambda: now[0])
    job, _ = repo.enqueue(
        idempotency_key="release",
        job_type="RELEASE_ACTUAL_REFRESH",
        symbol="MNQ",
        correlation_id="release-corr",
        event_key="event:1",
        request_payload={},
        policy_version="source-policy-v1",
        prompt_version="v1",
        max_attempts=7,
    )
    observed = []
    for delay in (30, 120, 300, 900, 1800, 3600):
        running = repo.acquire_next("worker")
        assert running and running["job_id"] == job["job_id"]
        retried = repo.retry_or_fail(
            job["job_id"], "worker", error="not yet", delays=[30, 120, 300, 900, 1800, 3600]
        )
        retry_at = datetime.fromisoformat(retried["next_retry_at"])
        observed.append(int((retry_at - now[0]).total_seconds()))
        now[0] = retry_at
        repo = AIResearchJobRepository(cfg, clock=lambda: now[0])
    assert observed == [30, 120, 300, 900, 1800, 3600]


def test_temporal_event_states_reject_future_actual_and_handle_speech(tmp_path: Path) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    future = event(release=now + timedelta(hours=1), actual="9.9")
    assert temporal_event_state(future, now=now)["temporal_status"] == "PRE_RELEASE"
    assert temporal_event_state(future, now=now)["actual"] is None
    facts = MarketFactRepository(settings(tmp_path))
    facts.upsert_economic_event(future, canonical_event_key(future))
    with connect_market_db(facts.settings) as conn:
        assert (
            conn.execute("SELECT actual FROM economic_events_history").fetchone()["actual"] is None
        )
    released = event(release=now - timedelta(seconds=1))
    assert temporal_event_state(released, now=now)["temporal_status"] == "AWAITING_ACTUAL"
    speech = event(release=now - timedelta(seconds=1), name="Fed Chair speech")
    assert temporal_event_state(speech, now=now)["temporal_status"] == "AWAITING_OUTCOME"
    speech.enrichment.summary["outcome"] = "Transcript published"
    assert temporal_event_state(speech, now=now)["temporal_status"] == "COMPLETED"


def test_xtb_event_becomes_canonical_and_discordant_values_are_preserved() -> None:
    now = datetime(2026, 7, 22, 10, 0, tzinfo=UTC)
    release = now + timedelta(hours=2)
    base = event(release=release)
    payload = {
        "source": "XTB Economic Calendar",
        "source_url": "https://xtb.com/calendar",
        "items": [
            {
                "country": "US",
                "event_name": base.name,
                "category": "CPI",
                "release_at": release.isoformat(),
                "date": release.date().isoformat(),
                "forecast": "0.4",
                "consensus": "0.4",
                "previous": "0.2",
            }
        ],
    }
    merged = reconcile_calendar_events([base], [payload], now=now)
    assert len(merged) == 1
    conflicts = merged[0].enrichment.summary["discordant_candidates"]
    assert {row["field"] for row in conflicts} == {"forecast", "consensus"}
    only_xtb = reconcile_calendar_events([], [payload], now=now)
    assert len(only_xtb) == 1 and only_xtb[0].actual is None


def test_worker_uses_unique_workspaces_and_never_waits_in_http_path(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    service = AIResearchJobService(cfg)
    for index in range(2):
        service.enqueue_explicit(
            job_type="MISSING_EVENT_RESEARCH",
            symbol="MNQ",
            correlation_id=f"corr-{index}",
            request_payload={"pending_fields": [f"field-{index}"]},
            pending_fields=[f"field-{index}"],
        )
    seen: list[Path] = []

    def fake_executor(job, workspace, timeout):
        seen.append(workspace)
        return {"status": "NO_DATA", "results": []}

    worker = AIResearchWorker(cfg, executor=fake_executor, worker_id="worker-test")
    assert worker.process_once() and worker.process_once()
    assert len(set(seen)) == 2
    assert all(path.name.startswith("airj-") for path in seen)


def test_official_actual_worker_updates_history_and_fact_with_surprise(
    tmp_path: Path, monkeypatch
) -> None:
    cfg = settings(tmp_path)
    released = event(release=datetime.now(UTC) - timedelta(minutes=1))
    key = canonical_event_key(released)
    facts = MarketFactRepository(cfg)
    facts.upsert_economic_event(released, key)
    jobs = AIResearchJobService(cfg).enqueue_temporal_refreshes([released])
    assert len(jobs) == 1 and jobs[0]["job_type"] == "RELEASE_ACTUAL_REFRESH"
    snapshots = MarketContextSnapshotRepository(cfg)
    debug = {
        "symbol": "MNQ",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "market_schedule": {
            "status": "AVAILABLE",
            "context_date": released.date,
            "market_session_status": "open",
        },
        "event_calendar": {
            "critical_macro_events": [released.model_dump(mode="json")],
            "fed_communications": [],
            "other_economic_events": [],
        },
        "events_today": [],
        "macro_snapshot": {},
        "risk_context": {},
        "nasdaq_context": {},
        "news_context": {},
        "rates_expectations": {},
        "positioning": {},
        "sentiment_context": {},
        "data_quality": {},
    }
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="auto",
        debug_payload=debug,
        ai_enrichment={"status": "PENDING", "job_ids": [jobs[0]["job_id"]]},
        source_job_id=jobs[0]["job_id"],
        job_ids=[jobs[0]["job_id"]],
    )

    async def official_bls_fetch(_self):
        return ProviderResult(
            metadata=ProviderMetadata(
                source="BLS",
                provider_type=ProviderType.API,
                retrieved_at=datetime.now(UTC),
                data_as_of=released.time_utc.replace(day=1),
                freshness=Freshness.RECENT,
                reliability=0.99,
            ),
            data={
                "CUSR0000SA0": {
                    "value": 101.5,
                    "data_as_of": released.time_utc.strftime("%Y-%m-01"),
                    "units": "index",
                    "frequency": "monthly",
                    "seasonal_adjustment": "SA",
                    "source": "BLS",
                    "source_url": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                    "canonical_url": "https://www.bls.gov/developers/api_signature_v2.htm",
                    "source_domain": "bls.gov",
                    "provider_adapter": "BLS_OFFICIAL_API",
                    "official_adapter": True,
                    "observations": [
                        {
                            "period": (
                                released.time_utc.replace(day=1) - timedelta(days=1)
                            ).strftime("%Y-%m"),
                            "value": 101.0,
                        },
                        {"period": released.time_utc.strftime("%Y-%m"), "value": 101.5},
                    ],
                }
            },
        )

    monkeypatch.setattr(
        "app.services.deterministic_actual_resolver.BlsProvider.fetch", official_bls_fetch
    )

    worker = AIResearchWorker(cfg, facts=facts, snapshots=snapshots, worker_id="actual-worker")
    assert worker.process_once()
    completed = AIResearchJobRepository(cfg).get(jobs[0]["job_id"])
    assert completed["status"] == "SUCCEEDED", completed["last_error"]
    with connect_market_db(cfg) as conn:
        row = conn.execute(
            "SELECT * FROM economic_events_history WHERE canonical_event_key=?", (key,)
        ).fetchone()
        fact_row = conn.execute(
            "SELECT * FROM market_facts WHERE canonical_event_key=?", (key,)
        ).fetchone()
    assert (
        row["actual"] == "0.5"
        and row["forecast"] == "0.3"
        and row["consensus"] == "0.3"
        and row["previous"] == "0.2"
    )
    assert row["surprise_value"] == "0.2" and row["surprise_direction"] == "above_consensus"
    assert row["actual_source_url"] == "https://www.bls.gov/developers/api_signature_v2.htm"
    assert fact_row is not None and fact_row["actual"] == "0.5"
    revised = snapshots.latest("MNQ")
    assert revised["revision"] == 2
    assert revised["consumer_payload"]["event_risk"]["critical_events"][0]["actual"] == "0.5"


def test_expired_news_is_historical_not_current(tmp_path: Path) -> None:
    repository = MarketNewsRepository(settings(tmp_path, enable_ai_researcher=False))
    repository.upsert_news(
        {
            "title": "CPI release recap",
            "summary": "Published result",
            "source": "Reuters",
            "source_url": "https://reuters.com/example",
            "published_at": "2026-01-01T12:00:00Z",
            "retrieved_at": "2026-01-01T12:01:00Z",
            "valid_until": "2026-01-02T00:00:00Z",
            "topics": ["inflation"],
            "symbols": ["MNQ"],
        }
    )
    assert repository.current(days=365) == []
    historical = repository.stored(days=365)
    assert (
        historical
        and historical[0]["lifecycle_status"] == "EXPIRED"
        and historical[0]["historical"] is True
    )


def test_consumer_exposes_structured_ai_and_blocks_full_readiness_when_pending() -> None:
    full = {
        "symbol": "MNQ",
        "generated_at_utc": "2026-07-22T10:00:00Z",
        "snapshot_id": "mcs-1",
        "snapshot_revision": 3,
        "market_schedule": {
            "status": "AVAILABLE",
            "context_date": "2026-07-22",
            "market_session_status": "open",
        },
        "macro_snapshot": {"rates_and_yields": {"DGS10": {"value": 4.2}}},
        "event_calendar": {
            "critical_macro_events": [],
            "fed_communications": [],
            "other_economic_events": [],
        },
        "events_today": [],
        "event_windows": {},
        "risk_context": {"status": "AVAILABLE", "VIX": {"value": 16}},
        "nasdaq_context": {"qqq_holdings": {"holdings_count": 100}, "earnings": {}},
        "news_context": {"status": "NO_RELEVANT_NEWS", "latest": []},
        "rates_expectations": {},
        "positioning": {},
        "sentiment_context": {},
        "data_quality": {},
        "quality": {},
        "ai_enrichment": {
            "status": "PENDING",
            "job_ids": ["airj-1"],
            "pending_fields": ["consensus"],
        },
    }
    consumer = build_ai_trader_consumer_v2(full, settings=Settings(_env_file=None))
    assert consumer["schema_version"] == "2.1"
    assert consumer["snapshot_id"] == "mcs-1" and consumer["snapshot_revision"] == 3
    assert consumer["ai_enrichment"]["status"] == "PENDING"
    assert consumer["ready_for_full_analysis"] is False


def test_additive_migration_from_v6_preserves_existing_rows(tmp_path: Path) -> None:
    db = tmp_path / "legacy.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT,applied_at TEXT)"
        )
        for version, (name, sql) in enumerate(MIGRATIONS[:6], start=1):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)", (version, name, "2026-07-22")
            )
        conn.execute("PRAGMA user_version=6")
        conn.execute(
            "INSERT INTO market_news(news_key,title,source_url,retrieved_at) VALUES ('keep','Keep','https://example.com','2026-01-01')"
        )
        conn.commit()
    assert migrate_database(db)["schema_version"] == 19
    with sqlite3.connect(db) as conn:
        assert (
            conn.execute("SELECT title FROM market_news WHERE news_key='keep'").fetchone()[0]
            == "Keep"
        )
