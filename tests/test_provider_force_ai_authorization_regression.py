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
    DEFAULT_SERVICE_PORT,
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


def released_event() -> EconomicEvent:
    release = datetime.now(UTC) - timedelta(minutes=1)
    return EconomicEvent(
        event_id="released-cpi",
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
            forecast="0.3",
            consensus="0.3",
            previous="0.2",
            actual=None,
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


def telemetry_counts(settings: Settings) -> dict[str, int]:
    with sqlite3.connect(settings.database_path) as connection:
        return {
            str(name): int(count)
            for name, count in connection.execute(
                """
                SELECT event_name,COUNT(*)
                FROM service_telemetry_events
                GROUP BY event_name
                """
            ).fetchall()
        }


def test_provider_only_release_actual_executes_resolver_without_ai(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        research_agents_enabled=False,
        research_agent_macro_events_enabled=False,
    )
    context = ExecutionContext.provider_only(
        correlation_id="provider-only-actual",
        allow_live_providers=True,
    )
    jobs = AIResearchJobService(settings).enqueue_temporal_refreshes(
        [released_event()],
        correlation_id=context.correlation_id,
        now=datetime.now(UTC),
        execution_context=context,
    )
    assert len(jobs) == 1
    assert jobs[0]["job_type"] == "RELEASE_ACTUAL_REFRESH"
    assert jobs[0]["request_payload"]["execution_context"] == context.as_payload()
    resolver_calls: list[str] = []
    backend_calls: list[str] = []

    def resolver(job, _workspace, _timeout):
        resolver_calls.append(str(job["job_id"]))
        return {
            "status": "NO_DATA",
            "retryable": False,
            "results": [],
            "error": "official_actual_not_available",
        }

    def forbidden_backend(job, _workspace, _timeout):
        backend_calls.append(str(job["job_id"]))
        raise AssertionError("provider-only actual reached AI backend")

    worker = AIResearchWorker(
        settings,
        executor=forbidden_backend,
        actual_resolver=resolver,
        worker_id="provider-only-worker",
    )
    assert worker.process_once() is True
    completed = AIResearchJobRepository(settings).get(jobs[0]["job_id"])
    assert completed["attempts"] == 1
    assert completed["status"] == "NO_DATA"
    assert resolver_calls == [jobs[0]["job_id"]]
    assert backend_calls == []
    counts = telemetry_counts(settings)
    assert counts.get("resolver_evaluation", 0) >= 1
    assert counts.get("provider_request_attempted", 0) == 1
    assert counts.get("provider_request_completed", 0) == 1
    assert counts.get("provider_request_failed", 0) == 0
    assert counts.get("ai_authorization", 0) == 0
    assert counts.get("ai_invocation_attempted", 0) == 0
    assert counts.get("ai_invocation_completed", 0) == 0
    assert counts.get("ai_invocation_aborted", 0) == 0
    assert table_count(settings, "research_backend_invocations") == 0
    with sqlite3.connect(settings.database_path) as connection:
        tokens = connection.execute(
            """
            SELECT
              COALESCE(SUM(json_extract(payload_json,'$.input_tokens')),0),
              COALESCE(SUM(json_extract(payload_json,'$.output_tokens')),0),
              COALESCE(SUM(json_extract(payload_json,'$.cached_tokens')),0)
            FROM service_telemetry_events
            WHERE event_name LIKE 'provider_request_%'
            """
        ).fetchone()
    assert tokens == (0, 0, 0)


def test_provider_failure_retries_deterministically_without_ai_fallback(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        official_actual_retry_seconds="17,31",
        research_agents_enabled=False,
        research_agent_macro_events_enabled=False,
    )
    fixed_now = datetime(2026, 7, 25, 12, tzinfo=UTC)
    repository = AIResearchJobRepository(settings, clock=lambda: fixed_now)
    context = ExecutionContext.provider_only(
        correlation_id="provider-retry",
        allow_live_providers=True,
    )
    jobs = AIResearchJobService(
        settings,
        repository=repository,
        clock=lambda: fixed_now,
    ).enqueue_temporal_refreshes(
        [released_event()],
        correlation_id=context.correlation_id,
        now=datetime.now(UTC),
        execution_context=context,
    )
    backend_calls: list[str] = []

    def failed_resolver(_job, _workspace, _timeout):
        return {
            "status": "FAILED",
            "retryable": True,
            "results": [],
            "error": "official_provider_transport_failed",
        }

    def forbidden_backend(job, _workspace, _timeout):
        backend_calls.append(str(job["job_id"]))
        raise AssertionError("provider failure triggered implicit AI fallback")

    assert AIResearchWorker(
        settings,
        repository=repository,
        executor=forbidden_backend,
        actual_resolver=failed_resolver,
        worker_id="provider-retry-worker",
    ).process_once()
    restored = repository.get(jobs[0]["job_id"])
    assert restored["status"] == "RETRY_SCHEDULED"
    assert restored["attempts"] == 1
    assert restored["next_retry_at"] == (
        fixed_now + timedelta(seconds=17)
    ).isoformat()
    assert restored["last_retry_reason"] == "official_provider_transport_failed"
    assert backend_calls == []
    counts = telemetry_counts(settings)
    assert counts.get("resolver_evaluation", 0) >= 1
    assert counts.get("provider_request_attempted", 0) == 1
    assert counts.get("provider_request_failed", 0) == 1
    assert counts.get("ai_authorization", 0) == 0
    assert counts.get("ai_invocation_attempted", 0) == 0
    assert counts.get("ai_invocation_completed", 0) == 0
    assert counts.get("ai_invocation_aborted", 0) == 0
    assert table_count(settings, "research_backend_invocations") == 0


def test_persisted_test_origin_cannot_authorize_ai_outside_test_environment(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path, environment="production")
    context_payload = {
        "allow_live_providers": False,
        "allow_ai": True,
        "request_origin": "test",
        "correlation_id": "persisted-test-origin",
    }
    context = ExecutionContext.from_payload(context_payload)
    assert context is not None
    assert AIResearchJobService(settings).enqueue_missing_events(
        [missing_event()],
        correlation_id="non-test-service-gate",
        execution_context=context,
    ) == []
    repository = AIResearchJobRepository(settings)
    job, created = repository.enqueue(
        idempotency_key="persisted-test-origin",
        job_type="MISSING_EVENT_RESEARCH",
        symbol="MNQ",
        correlation_id="persisted-test-origin",
        request_payload={
            "job_type": "MISSING_EVENT_RESEARCH",
            "execution_context": context_payload,
        },
        policy_version="test",
        prompt_version="test",
    )
    assert created is True
    backend_calls: list[str] = []

    def forbidden_backend(acquired, _workspace, _timeout):
        backend_calls.append(str(acquired["job_id"]))
        raise AssertionError("production accepted persisted test authority")

    worker = AIResearchWorker(
        settings,
        repository=repository,
        executor=forbidden_backend,
        worker_id="non-test-worker",
    )
    assert worker.process_once() is False
    rejected = repository.get(job["job_id"])
    assert rejected["status"] == "REJECTED"
    assert rejected["last_error"] == "AI_NOT_AUTHORIZED"
    assert rejected["result_payload"]["diagnostic"][
        "backend_invocation_attempted"
    ] is False
    assert backend_calls == []
    assert table_count(settings, "research_backend_invocations") == 0
    assert telemetry_counts(settings).get("ai_invocation_attempted", 0) == 0


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
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "scripts.reconcile_unauthorized_no_data_snapshots.socket.create_connection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()),
    )
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


def _closed_database_backup(settings: Settings, tmp_path: Path) -> Path:
    with sqlite3.connect(settings.database_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    backup = tmp_path / "guard.backup.sqlite"
    shutil.copy2(settings.database_path, backup)
    return backup


def _fake_listener_on(port: int):
    def connect(address, timeout):
        del timeout
        if int(address[1]) == port:
            return socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raise OSError("listener absent")

    return connect


def test_reconciliation_default_8053_listener_refuses_apply(
    tmp_path: Path,
    monkeypatch,
) -> None:
    assert DEFAULT_SERVICE_PORT == 8053
    settings = cfg(tmp_path)
    _seed_reconciliation_database(settings)
    backup = _closed_database_backup(settings, tmp_path)
    monkeypatch.setattr(
        "scripts.reconcile_unauthorized_no_data_snapshots.socket.create_connection",
        _fake_listener_on(8053),
    )
    with pytest.raises(
        RuntimeError,
        match="service_must_be_stopped_before_apply",
    ):
        reconcile(settings.database_path, apply=True, backup=backup)


def test_reconciliation_absent_8053_proceeds_to_expected_state_guard(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = cfg(tmp_path)
    _seed_reconciliation_database(settings)
    with sqlite3.connect(settings.database_path) as connection:
        connection.execute(
            """
            UPDATE market_context_snapshots
            SET refresh_mode='unexpected'
            WHERE revision=86
            """
        )
        connection.commit()
    backup = _closed_database_backup(settings, tmp_path)
    monkeypatch.setattr(
        "scripts.reconcile_unauthorized_no_data_snapshots.socket.create_connection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()),
    )
    with pytest.raises(
        RuntimeError,
        match="expected_state_guard_refresh_mode_mismatch",
    ):
        reconcile(settings.database_path, apply=True, backup=backup)


def test_ai_trader_listener_on_8000_does_not_affect_reconciliation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = cfg(tmp_path)
    _seed_reconciliation_database(settings)
    backup = _closed_database_backup(settings, tmp_path)
    monkeypatch.setattr(
        "scripts.reconcile_unauthorized_no_data_snapshots.socket.create_connection",
        _fake_listener_on(8000),
    )
    result = reconcile(settings.database_path, apply=True, backup=backup)
    assert result["service_listener_guard"]["port"] == 8053
    assert result["changed_snapshot_count"] == 5


def test_reconciliation_service_port_override_is_respected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = cfg(tmp_path)
    _seed_reconciliation_database(settings)
    backup = _closed_database_backup(settings, tmp_path)
    monkeypatch.setattr(
        "scripts.reconcile_unauthorized_no_data_snapshots.socket.create_connection",
        _fake_listener_on(18053),
    )
    with pytest.raises(
        RuntimeError,
        match="service_must_be_stopped_before_apply",
    ):
        reconcile(
            settings.database_path,
            apply=True,
            backup=backup,
            service_port=18053,
        )


def test_reconciliation_dry_run_is_read_only_and_skips_listener_guard(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = cfg(tmp_path)
    _seed_reconciliation_database(settings)
    before = settings.database_path.read_bytes()

    def forbidden_probe(*_args, **_kwargs):
        raise AssertionError("dry-run probed service listener")

    monkeypatch.setattr(
        "scripts.reconcile_unauthorized_no_data_snapshots._assert_service_not_listening",
        forbidden_probe,
    )
    result = reconcile(settings.database_path)
    assert result["mode"] == "DRY_RUN"
    assert result["changed_snapshot_count"] == 0
    assert settings.database_path.read_bytes() == before
