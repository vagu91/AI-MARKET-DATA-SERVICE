from __future__ import annotations

import json
import shutil
import socket
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.core.config import Settings
from app.models.common import Impact
from app.models.events import EconomicEvent, EventEnrichment
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_worker import AIResearchWorker
from app.services.execution_context import ExecutionContext
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.research_runtime_repository import ResearchRuntimeRepository
from scripts.reconcile_unauthorized_no_data_snapshots import (
    EXPECTED_SNAPSHOTS,
    reconcile,
)
from scripts.replay_provider_force_regression_offline import replay


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 25, 12, tzinfo=UTC)


def cfg(tmp_path: Path, **overrides) -> Settings:
    values = {
        "environment": "test",
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": True,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def missing_event(index: int = 1) -> EconomicEvent:
    release = NOW + timedelta(days=2, hours=index)
    return EconomicEvent(
        event_id=f"cpi-{index}",
        name="Consumer Price Index",
        country="US",
        category="CPI",
        date=release.date().isoformat(),
        time_utc=release,
        impact=Impact.HIGH,
        source="BLS",
        source_url="https://bls.gov/cpi",
        reliability=0.99,
        enrichment=EventEnrichment(
            forecast=None,
            consensus=None,
            previous="0.2",
        ),
    )


def table_count(settings: Settings, table: str) -> int:
    with sqlite3.connect(settings.database_path) as connection:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_force_provider_context_cannot_enqueue_or_create_runtime_rows(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    context = ExecutionContext.provider_only(
        correlation_id="force-provider-only",
        allow_live_providers=True,
    )
    jobs = AIResearchJobService(settings).enqueue_missing_events(
        [missing_event()],
        correlation_id=context.correlation_id,
        force=True,
        execution_context=context,
    )
    assert jobs == []
    assert table_count(settings, "ai_research_jobs") == 0
    assert table_count(settings, "research_runs") == 0
    assert table_count(settings, "research_backend_invocations") == 0
    with sqlite3.connect(settings.database_path) as connection:
        decision = connection.execute(
            """
            SELECT payload_json FROM service_telemetry_events
            WHERE event_name='ai_authorization'
            ORDER BY rowid DESC LIMIT 1
            """
        ).fetchone()
    assert decision is not None
    assert "AI_SUPPRESSED" in decision[0]


def test_explicit_context_is_persisted_and_worker_acquisition_is_fail_closed(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    service = AIResearchJobService(settings)
    context = ExecutionContext.explicit_ai(
        correlation_id="explicit-residual",
        allow_live_providers=True,
    )
    jobs = service.enqueue_missing_events(
        [missing_event(index) for index in range(1, 6)],
        correlation_id=context.correlation_id,
        execution_context=context,
    )
    assert len(jobs) == 1
    assert jobs[0]["request_payload"]["batch_size"] == 5
    assert len(jobs[0]["request_payload"]["event_keys"]) == 5
    assert jobs[0]["request_payload"]["execution_context"] == context.as_payload()
    repository = AIResearchJobRepository(settings)
    unauthorized, _ = repository.enqueue(
        idempotency_key="legacy-unauthorized",
        job_type="MISSING_EVENT_RESEARCH",
        symbol="MNQ",
        event_key="legacy",
        correlation_id="legacy",
        request_payload={"job_type": "MISSING_EVENT_RESEARCH"},
        policy_version="test",
        prompt_version="test",
    )
    acquired = repository.acquire_next(
        "worker",
        require_execution_authorization=True,
    )
    assert acquired is not None
    assert acquired["job_id"] == jobs[0]["job_id"]
    repository.complete(
        acquired["job_id"],
        "worker",
        status="NO_DATA",
        result_payload={"status": "NO_DATA"},
    )
    assert (
        repository.acquire_next(
            "worker",
            require_execution_authorization=True,
        )
        is None
    )
    rejected = repository.get(unauthorized["job_id"])
    assert rejected["status"] == "REJECTED"
    assert rejected["last_error"] == "AI_NOT_AUTHORIZED"
    assert rejected["completed_at"] is not None
    assert rejected["result_payload"]["diagnostic"] == {
        "backend_invocation_attempted": False,
        "category": "AI_NOT_AUTHORIZED",
        "decision": "AI_SUPPRESSED",
        "retryable": False,
        "terminalized_at": rejected["completed_at"],
    }
    terminal_timestamp = rejected["completed_at"]
    assert (
        repository.acquire_next(
            "worker",
            require_execution_authorization=True,
        )
        is None
    )
    assert repository.get(unauthorized["job_id"])["completed_at"] == terminal_timestamp
    assert table_count(settings, "ai_research_job_attempts") == 1
    assert table_count(settings, "research_backend_invocations") == 0
    assert table_count(settings, "market_context_snapshots") == 0
    assert table_count(settings, "market_context_outbox") == 0
    with sqlite3.connect(settings.database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM ai_research_job_attempts WHERE job_id=?",
            (unauthorized["job_id"],),
        ).fetchone()[0] == 0


def test_no_data_without_claims_does_not_create_snapshot(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    snapshots = MarketContextSnapshotRepository(settings)
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="baseline",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "market_schedule": {
                "context_date": "2026-07-25",
                "market_session_status": "weekend",
                "market_closed": True,
                "market_closed_reason": "weekend",
            },
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    context = ExecutionContext.explicit_ai(correlation_id="no-data")
    AIResearchJobService(settings).enqueue_explicit(
        job_type="MISSING_EVENT_RESEARCH",
        symbol="MNQ",
        correlation_id="no-data",
        request_payload={"pending_fields": ["forecast"]},
        execution_context=context,
    )
    worker = AIResearchWorker(
        settings,
        executor=lambda job, workspace, timeout: {
            "status": "NO_DATA",
            "results": [],
            "accepted_count": 0,
            "persisted_count": 0,
            "read_back_count": 0,
        },
        worker_id="offline-worker",
    )
    assert worker.process_once() is True
    assert table_count(settings, "market_context_snapshots") == 1


def test_offline_replay_reconstructs_overflow_and_closes_regression() -> None:
    result = replay()
    assert result["reconstructed_before_size_bytes"] > 90_000
    assert result["after_size_bytes"] < 90_000
    assert result["after_section_sizes"]
    assert result["provider_only_ai_jobs"] == 0
    assert result["database_counts"]["ai_research_jobs"] == 0
    assert result["database_counts"]["research_runs"] == 0
    assert result["database_counts"]["research_backend_invocations"] == 0
    assert result["database_counts"]["market_context_snapshots"] == 1
    assert result["weekend_preserved"] is True
    assert result["deterministic_output"] is True
    assert result["raw_contracts_absent"] is True
    assert result["secrets_absent"] is True
    assert result["live_calls"] == result["trading_endpoints_used"] == 0


def _seed_reconciliation_database(settings: Settings) -> None:
    snapshots = MarketContextSnapshotRepository(settings)
    snapshots.save(
        snapshot_id="mcs-baseline",
        revision=85,
        symbol="MNQ",
        refresh_mode="baseline",
        debug_payload={"generated_at_utc": NOW.isoformat()},
        consumer_payload={
            "contract": "ai_trader_market_context_consumer",
            "schema_version": "2.1",
            "data_as_of": NOW.isoformat(),
        },
        ai_status="NOT_REQUIRED",
    )
    service = AIResearchJobService(settings)
    runs = ResearchRuntimeRepository(settings)
    repository = service.repository
    for revision, snapshot_id in EXPECTED_SNAPSHOTS.items():
        context = ExecutionContext.explicit_ai(
            correlation_id=f"incident-{revision}",
        )
        job, created = service.enqueue_explicit(
            job_type="MISSING_EVENT_RESEARCH",
            symbol="MNQ",
            correlation_id=f"incident-{revision}",
            request_payload={"pending_fields": [f"field-{revision}"]},
            pending_fields=[f"field-{revision}"],
            force=True,
            execution_context=context,
        )
        assert created
        run = runs.ensure_run(job, "MISSING_EVENT_RESEARCH", "test")
        acquired = repository.acquire_next("seed-worker")
        assert acquired and acquired["job_id"] == job["job_id"]
        repository.complete(
            job["job_id"],
            "seed-worker",
            status="NO_DATA",
            result_payload={"status": "NO_DATA", "run_id": run["run_id"]},
        )
        with sqlite3.connect(settings.database_path) as connection:
            connection.execute(
                "UPDATE research_runs SET status='NO_DATA' WHERE run_id=?",
                (run["run_id"],),
            )
            connection.commit()
        snapshots.save(
            snapshot_id=snapshot_id,
            revision=revision,
            symbol="MNQ",
            refresh_mode="worker_db_only_materialization",
            debug_payload={"generated_at_utc": NOW.isoformat()},
            consumer_payload={
                "contract": "ai_trader_market_context_consumer",
                "schema_version": "2.1",
                "data_as_of": NOW.isoformat(),
            },
            ai_status="NO_DATA",
            source_job_id=job["job_id"],
        )


def test_reconciliation_is_read_only_guarded_and_idempotent(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    _seed_reconciliation_database(settings)
    before = settings.database_path.read_bytes()
    dry_run = reconcile(settings.database_path)
    assert dry_run["changed_snapshot_count"] == 0
    assert settings.database_path.read_bytes() == before
    with sqlite3.connect(settings.database_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    backup = tmp_path / "market.backup.sqlite"
    shutil.copy2(settings.database_path, backup)
    first = reconcile(settings.database_path, apply=True, backup=backup)
    assert first["changed_snapshot_count"] == 5
    with sqlite3.connect(settings.database_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    second_backup = tmp_path / "market.second.backup.sqlite"
    shutil.copy2(settings.database_path, second_backup)
    second = reconcile(settings.database_path, apply=True, backup=second_backup)
    assert second["changed_snapshot_count"] == 0
    assert MarketContextSnapshotRepository(settings).latest("MNQ")["snapshot_id"] == "mcs-baseline"
    with sqlite3.connect(settings.database_path) as connection:
        statuses = connection.execute(
            """
            SELECT audit_status,COUNT(*) FROM market_context_snapshots
            GROUP BY audit_status ORDER BY audit_status
            """
        ).fetchall()
    assert statuses == [("ACTIVE", 1), ("ORPHANED", 5)]
    assert first["tokens_or_telemetry_modified"] is False
    assert json.dumps(first["records"]).count("MISSING_EVENT_RESEARCH") == 5


def test_reconciliation_apply_refuses_configured_listening_port(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    _seed_reconciliation_database(settings)
    with sqlite3.connect(settings.database_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    backup = tmp_path / "market.backup.sqlite"
    shutil.copy2(settings.database_path, backup)
    before = settings.database_path.read_bytes()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    try:
        port = int(listener.getsockname()[1])
        with pytest.raises(
            RuntimeError,
            match="service_must_be_stopped_before_apply",
        ):
            reconcile(
                settings.database_path,
                apply=True,
                backup=backup,
                service_host="127.0.0.1",
                service_port=port,
            )
    finally:
        listener.close()
    assert settings.database_path.read_bytes() == before
