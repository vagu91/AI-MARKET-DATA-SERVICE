from __future__ import annotations

import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api import routes
from app.core.config import Settings
from app.models.common import Impact
from app.models.events import EconomicEvent, EventEnrichment
from app.services.ai_research_job_executor import PersistentAIJobExecutor
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_worker import AIResearchWorker
from app.services.db_only_market_context_materializer import DBOnlyMarketContextMaterializer
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.market_fact_repository import MarketFactRepository, connect_market_db
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_domain_service import canonical_event_key, reconcile_calendar_events


POLICY = Path(__file__).resolve().parents[1] / "config" / "source_policy.json"


def _settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": True,
        "ai_worker_enabled": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def _event(release: datetime, *, name: str = "Consumer Price Index") -> EconomicEvent:
    return EconomicEvent(
        event_id="cpi-canonical", name=name, country="US", category="CPI",
        metric_id="headline_cpi_mom", normalized_event_family="CPI",
        reference_period=release.strftime("%Y-%m"), frequency="monthly",
        date=release.date().isoformat(), time_utc=release, impact=Impact.HIGH,
        source="BLS", source_url="https://www.bls.gov/cpi/", reliability=0.99,
        event_risk_level=Impact.HIGH, enrichment=EventEnrichment(),
    )


def _debug_payload() -> dict:
    return {
        "symbol": "MNQ", "generated_at_utc": "2026-07-22T10:00:00+00:00",
        "market_schedule": {
            "status": "AVAILABLE", "context_date": "2026-07-22",
            "market_session_status": "open",
        },
        "event_calendar": {
            "critical_macro_events": [], "fed_communications": [],
            "other_economic_events": [],
        },
        "events_today": [], "macro_snapshot": {}, "risk_context": {},
        "nasdaq_context": {}, "news_context": {}, "rates_expectations": {},
        "positioning": {}, "sentiment_context": {}, "data_quality": {},
    }


def _research_candidate(field: str, value: str, period: str) -> dict:
    return {
        "field": field, "field_semantics": field, "value": value,
        "source": "Investing", "publisher": "Investing",
        "source_url": "https://www.investing.com/economic-calendar/cpi-733",
        "canonical_url": "https://www.investing.com/economic-calendar/cpi-733",
        "evidence_text": f"Published {field} value {value}.",
        "metric_id": "headline_cpi_mom", "period": period, "frequency": "monthly",
        "unit": "percent", "retrieved_at": "2026-07-22T10:00:00+00:00",
        "reliability": 0.84, "confidence": 0.84,
        "verified_independent_domains": ["investing.com", "xtb.com"],
    }


def test_localized_titles_resolve_to_one_canonical_occurrence() -> None:
    release = datetime(2026, 7, 22, 12, 30, tzinfo=UTC)
    english = _event(release)
    english.enrichment = EventEnrichment(forecast="0.3", consensus="0.3")
    italian = _event(release, name="Indice dei prezzi al consumo")
    assert canonical_event_key(english) == canonical_event_key(italian)
    payload = {
        "source": "XTB", "source_url": "https://www.xtb.com/it/calendario-economico",
        "items": [{
            "country": "US", "event_name": italian.name, "category": "IPC",
            "release_at": release.isoformat(), "reference_period": "2026-07",
            "frequency": "monthly", "forecast": "0.4", "consensus": "0.4",
        }],
    }
    merged = reconcile_calendar_events([english], [payload], now=release - timedelta(hours=1))
    assert len(merged) == 1
    assert {row["field"] for row in merged[0].enrichment.summary["discordant_candidates"]} == {
        "forecast", "consensus",
    }


def test_worker_persists_and_reads_back_all_accepted_missing_fields(tmp_path: Path) -> None:
    cfg = _settings(tmp_path)
    release = datetime.now(UTC) + timedelta(hours=1)
    item = _event(release)
    key = canonical_event_key(item)
    facts = MarketFactRepository(cfg)
    facts.upsert_economic_event(item, key)
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MISSING_EVENT_RESEARCH", symbol="MNQ", correlation_id="all-fields",
        event_key=key, pending_fields=["forecast", "consensus", "previous"],
        request_payload={
            "event": item.model_dump(mode="json"),
            "temporal_state": {"release_at": release.isoformat()},
            "pending_fields": ["forecast", "consensus", "previous"],
        },
    )
    assert created

    def executor(_job, _workspace, _timeout):
        return {
            "status": "SUCCEEDED",
            "_service_evidence_verified": True,
            "results": [
                _research_candidate("forecast", "0.3", item.reference_period),
                _research_candidate("consensus", "0.3", item.reference_period),
                _research_candidate("previous", "0.2", item.reference_period),
            ],
        }

    assert AIResearchWorker(cfg, executor=executor, facts=facts, worker_id="persist-all").process_once()
    completed = AIResearchJobRepository(cfg).get(job["job_id"])
    assert completed["status"] == "SUCCEEDED"
    assert completed["result_payload"]["accepted_count"] == 3
    assert completed["result_payload"]["persisted_count"] == 3
    assert completed["result_payload"]["read_back_count"] == 3
    with connect_market_db(cfg) as conn:
        history = conn.execute(
            "SELECT forecast,consensus,previous FROM economic_events_history WHERE canonical_event_key=?",
            (key,),
        ).fetchone()
        candidates = conn.execute(
            "SELECT COUNT(*) FROM event_value_candidates WHERE canonical_event_key=? AND validation_status='accepted'",
            (key,),
        ).fetchone()[0]
    assert tuple(history) == ("0.3", "0.3", "0.2")
    assert candidates == 3


def test_later_calendar_upsert_preserves_terminal_actual_lineage_and_single_row(tmp_path: Path) -> None:
    cfg = _settings(tmp_path)
    release = datetime.now(UTC) - timedelta(minutes=5)
    item = _event(release)
    item.enrichment = EventEnrichment(
        forecast="0.3", consensus="0.3", previous="0.2",
        metrics=[{
            "metric_id": "headline_cpi_mom", "period": item.reference_period,
            "frequency": "monthly", "unit": "percent", "seasonal_adjustment": "SA",
        }],
    )
    key = canonical_event_key(item)
    facts = MarketFactRepository(cfg)
    facts.upsert_economic_event(item, "provider-row-1")
    initial = _debug_payload()
    initial["event_calendar"]["critical_macro_events"] = [item.model_dump(mode="json")]
    snapshots = MarketContextSnapshotRepository(cfg)
    snapshots.save_next(
        symbol="MNQ", refresh_mode="auto", debug_payload=initial,
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    facts.apply_official_event_actual(
        canonical_event_key=key,
        candidate={
            "value": "0.5", "source": "BLS", "publisher": "BLS",
            "source_url": "https://www.bls.gov/cpi/", "canonical_url": "https://www.bls.gov/cpi/",
            "source_domain": "bls.gov", "source_tier": 1, "source_classification": "OFFICIAL",
            "metric_id": "headline_cpi_mom", "event_metric_id": "headline_cpi_mom",
            "source_series_id": "CUSR0000SA0", "transformation": "pct_change_mom",
            "seasonal_adjustment": "SA", "period": item.reference_period,
            "frequency": "monthly", "unit": "percent", "evidence_text": "Official CPI value.",
            "retrieved_at": datetime.now(UTC).isoformat(), "reliability": 0.99,
        },
        policy_version="source-policy-v1",
    )
    replacement = _event(release, name="Indice prezzi al consumo")
    facts.upsert_economic_event(replacement, "provider-row-2")
    with connect_market_db(cfg) as conn:
        rows = conn.execute(
            "SELECT * FROM economic_events_history WHERE canonical_event_key=?", (key,)
        ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["actual"] == "0.5" and row["actual_source_url"] == "https://www.bls.gov/cpi/"
    assert row["surprise_value"] == "0.2" and row["temporal_status"] == "RELEASED"
    assert json.loads(row["field_lineage_json"])["actual"]["source_tier"] == 1
    assert json.loads(row["raw_payload_json"])["actual"] == "0.5"
    rebuilt = DBOnlyMarketContextMaterializer(cfg, facts=facts, snapshots=snapshots).materialize_for_job(
        job={"job_id": "test-db-only", "symbol": "MNQ", "event_key": key},
        ai_enrichment={"status": "SUCCEEDED", "job_ids": ["test-db-only"]},
    )
    rebuilt_event = rebuilt["debug_payload"]["event_calendar"]["critical_macro_events"][0]
    assert rebuilt_event["actual"] == "0.5"
    assert rebuilt_event["actual_source_url"] == "https://www.bls.gov/cpi/"
    assert rebuilt_event["surprise_value"] == "0.2"
    assert rebuilt_event["temporal_status"] == "RELEASED"
    worker_metadata = rebuilt["debug_payload"]["metadata"]["worker_materialization"]
    assert worker_metadata["mode"] == "DB_ONLY"
    assert worker_metadata["provider_calls"] == worker_metadata["browser_calls"] == worker_metadata["AI_calls"] == 0
    assert worker_metadata["source_snapshot_id"] and worker_metadata["source_job_id"] == "test-db-only"


def test_terminal_jobs_stay_idempotent_in_window_and_force_is_explicit(tmp_path: Path) -> None:
    now = datetime(2026, 7, 22, 10, 15, tzinfo=UTC)
    cfg = _settings(tmp_path)
    repo = AIResearchJobRepository(cfg, clock=lambda: now)
    service = AIResearchJobService(cfg, repository=repo, clock=lambda: now)
    kwargs = {
        "job_type": "MISSING_EVENT_RESEARCH", "symbol": "MNQ", "event_key": "event:scope",
        "request_payload": {"pending_fields": ["forecast"]}, "pending_fields": ["forecast"],
    }
    first, created = service.enqueue_explicit(correlation_id="first", **kwargs)
    duplicate, duplicate_created = service.enqueue_explicit(correlation_id="duplicate", force=True, **kwargs)
    assert created and not duplicate_created and duplicate["job_id"] == first["job_id"]
    repo.acquire_next("worker")
    repo.complete(first["job_id"], "worker", status="NO_DATA", result_payload={"status": "NO_DATA"})
    second, second_created = service.enqueue_explicit(correlation_id="second", **kwargs)
    assert not second_created and second["job_id"] == first["job_id"]
    forced, forced_created = service.enqueue_explicit(correlation_id="forced", force=True, **kwargs)
    assert forced_created and forced["job_id"] != first["job_id"]
    assert len(repo.latest(limit=10, event_keys=["event:scope"])) == 2


def test_ai_status_is_scoped_to_snapshot_or_current_event_set(tmp_path: Path) -> None:
    cfg = _settings(tmp_path)
    service = AIResearchJobService(cfg)
    first, _ = service.enqueue_explicit(
        job_type="MISSING_EVENT_RESEARCH", symbol="MNQ", correlation_id="one",
        event_key="event:one", request_payload={"pending_fields": ["forecast"]},
        pending_fields=["forecast"],
    )
    service.enqueue_explicit(
        job_type="MISSING_EVENT_RESEARCH", symbol="MNQ", correlation_id="two",
        event_key="event:two", request_payload={"pending_fields": ["previous"]},
        pending_fields=["previous"],
    )
    stored = MarketContextSnapshotRepository(cfg).save_next(
        symbol="MNQ", refresh_mode="auto", debug_payload=_debug_payload(),
        ai_enrichment={"status": "PENDING", "job_ids": [first["job_id"]]},
        job_ids=[first["job_id"]],
    )
    scoped = service.enrichment_status(snapshot_id=stored["snapshot_id"])
    assert scoped["job_ids"] == [first["job_id"]]
    assert service.enrichment_status(event_keys=[])["status"] == "NOT_REQUIRED"
    assert service.enrichment_status(event_keys=["event:two"])["pending_fields"] == ["previous"]


def test_snapshot_revisions_are_allocated_atomically_under_concurrency(tmp_path: Path) -> None:
    cfg = _settings(tmp_path)
    repository = MarketContextSnapshotRepository(cfg)

    def save(_index: int) -> tuple[int, str]:
        row = repository.save_next(
            symbol="MNQ", refresh_mode="test", debug_payload=_debug_payload(),
            ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
        )
        return row["revision"], row["snapshot_id"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(save, range(16)))
    assert sorted(revision for revision, _ in results) == list(range(1, 17))
    assert len({snapshot_id for _, snapshot_id in results}) == 16


def test_http_and_worker_materializers_allocate_distinct_atomic_revisions(tmp_path: Path) -> None:
    cfg = _settings(tmp_path)
    snapshots = MarketContextSnapshotRepository(cfg)
    snapshots.save_next(
        symbol="MNQ", refresh_mode="seed", debug_payload=_debug_payload(),
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    materializer = DBOnlyMarketContextMaterializer(cfg, snapshots=snapshots)

    def http_materialization() -> int:
        payload = routes._materialize_market_context(
            _debug_payload(), refresh="auto", view="consumer", settings=cfg,
        )
        return int(payload["snapshot_revision"])

    def worker_materialization() -> int:
        row = materializer.materialize_for_job(
            job={"job_id": "concurrent-worker", "symbol": "MNQ", "event_key": "event:concurrent"},
            ai_enrichment={"status": "SUCCEEDED", "job_ids": ["concurrent-worker"]},
        )
        return int(row["revision"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(http_materialization), pool.submit(worker_materialization)]
        revisions = sorted(future.result() for future in futures)
    assert revisions == [2, 3]


def test_recovery_closes_abandoned_attempt_and_bounds_exhausted_job(tmp_path: Path) -> None:
    now = [datetime(2026, 7, 22, 10, 0, tzinfo=UTC)]
    cfg = _settings(tmp_path, ai_job_lease_seconds=5)
    repo = AIResearchJobRepository(cfg, clock=lambda: now[0])
    job, _ = repo.enqueue(
        idempotency_key="lease", job_type="MISSING_EVENT_RESEARCH", symbol="MNQ",
        correlation_id="lease", request_payload={}, policy_version="v1", prompt_version="v1",
        max_attempts=1,
    )
    assert repo.acquire_next("worker")["status"] == "RUNNING"
    now[0] += timedelta(seconds=6)
    assert repo.recover_abandoned() == 1
    assert repo.get(job["job_id"])["status"] == "FAILED"
    with sqlite3.connect(cfg.database_path) as conn:
        attempt = conn.execute(
            "SELECT status,completed_at,error FROM ai_research_job_attempts WHERE job_id=?", (job["job_id"],)
        ).fetchone()
    assert attempt[0] == "ABANDONED" and attempt[1] and attempt[2] == "worker_lease_expired_recovered"


def test_executor_shutdown_targets_every_active_process_group(tmp_path: Path, monkeypatch) -> None:
    cfg = _settings(tmp_path)
    executor = PersistentAIJobExecutor(cfg)
    processes = [SimpleNamespace(pid=101), SimpleNamespace(pid=202)]
    executor._active = {process.pid: process for process in processes}
    terminated = []
    monkeypatch.setattr("app.services.ai_research_job_executor._terminate_process_group", terminated.append)
    executor.cancel_all()
    assert terminated == processes


def test_source_policy_requires_confirmation_and_rejects_spoofed_ir_domain() -> None:
    policy = SourcePolicyService(POLICY)
    candidate = {
        "source_url": "https://www.bloomberg.com/example", "publisher": "Bloomberg",
    }
    assert not policy.validate(candidate, field_semantics="consensus").accepted
    candidate["confirmation_count"] = 99
    assert not policy.validate(candidate, field_semantics="consensus").accepted
    candidate["_service_evidence_verified"] = True
    candidate["verified_independent_domains"] = ["bloomberg.com", "ft.com"]
    assert policy.validate(candidate, field_semantics="consensus").accepted
    spoofed = {"source_url": "https://evil.example/ir", "publisher": "NVIDIA Investor Relations"}
    assert not policy.validate(spoofed, field_semantics="actual").accepted
    official = {"source_url": "https://investor.nvidia.com/results", "publisher": "NVIDIA Investor Relations"}
    assert policy.validate(official, field_semantics="actual").accepted


def test_refresh_false_reuses_existing_snapshot_without_new_rows_or_payload_change(tmp_path: Path) -> None:
    cfg = _settings(tmp_path)
    repository = MarketContextSnapshotRepository(cfg)
    stored = repository.save_next(
        symbol="MNQ", refresh_mode="auto", debug_payload=_debug_payload(),
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    before = json.dumps(stored["consumer_payload"], sort_keys=True, separators=(",", ":"))
    result = asyncio.run(routes.market_context_mnq(
        refresh="false", view="consumer", macro_service=None, event_service=None,
        event_window_service=None, nasdaq_service=None,
        enrichment_orchestrator=SimpleNamespace(settings=cfg),
    ))
    after = json.dumps(result, sort_keys=True, separators=(",", ":"))
    with sqlite3.connect(cfg.database_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM market_context_snapshots").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM ai_research_jobs").fetchone()[0] == 0
    assert after == before


def test_refresh_false_without_snapshot_fails_closed_without_calls_or_writes(tmp_path: Path, monkeypatch) -> None:
    cfg = _settings(tmp_path)
    calls = []

    class FakeDiagnostics:
        def __init__(self, *_args, **_kwargs):
            pass

        async def full_model(self, **kwargs):
            calls.append(kwargs)
            return _debug_payload()

    monkeypatch.setattr(routes, "DiagnosticsService", FakeDiagnostics)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(routes.market_context_mnq(
            refresh="false", view="consumer", macro_service=None, event_service=None,
            event_window_service=None, nasdaq_service=None,
            enrichment_orchestrator=SimpleNamespace(settings=cfg),
        ))
    assert caught.value.status_code == 404
    assert calls == []
    with sqlite3.connect(cfg.database_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM market_context_snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM ai_research_jobs").fetchone()[0] == 0
