from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.services.ai_research_job_executor import PersistentAIJobExecutor
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_worker import AIResearchWorker
from app.services.codex_runtime_contract import (
    CodexCLIError,
    all_step_output_schemas,
    build_diagnostic,
    classify_codex_failure,
    legacy_research_output_schema,
    validate_output_schema,
)
from app.services.research_runtime_repository import ResearchRuntimeRepository


POLICY = Path(__file__).resolve().parents[1] / "config" / "source_policy.json"


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    values = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": True,
        "ai_research_web_access_enabled": True,
        "ai_worker_enabled": True,
        "codex_cli_command": str(tmp_path / "codex.CMD"),
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def make_job_and_run(cfg: Settings) -> tuple[dict[str, Any], dict[str, Any]]:
    job, _ = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="hardening-test",
        request_payload={"database_context": {}},
    )
    run = ResearchRuntimeRepository(cfg).ensure_run(
        job,
        "MNQ_MARKET_RESEARCH",
        "mnq_market_research_v1",
    )
    return job, run


def plan_payload(topic: str = "macro") -> dict[str, Any]:
    return {
        "status": "COMPLETED",
        "topics": [topic],
        "queries": [
            {
                "query": "bounded query",
                "purpose": "find primary sources",
                "topic": topic,
            }
        ],
        "stop_conditions": ["bounded"],
        "warnings": [],
    }


class FakeProcess:
    next_pid = 4100

    def __init__(
        self,
        command: list[str],
        *,
        payload: dict[str, Any] | None,
        stdout: str = "",
        stderr: str = "",
        returncode: int = 0,
    ) -> None:
        self.command = command
        self.payload = payload
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.pid = FakeProcess.next_pid
        FakeProcess.next_pid += 1
        self.stdin_value: str | None = None

    def communicate(self, input: str | None = None, timeout: int | None = None):
        del timeout
        self.stdin_value = input
        if self.payload is not None:
            index = self.command.index("--output-last-message")
            Path(self.command[index + 1]).write_text(json.dumps(self.payload), encoding="utf-8")
        return self.stdout, self.stderr

    def poll(self):
        return self.returncode


def test_windows_cmd_command_is_isolated_and_prompt_is_stdin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = settings(tmp_path)
    job, run = make_job_and_run(cfg)
    created: list[FakeProcess] = []

    def fake_popen(command, **kwargs):
        del kwargs
        process = FakeProcess(command, payload=plan_payload())
        created.append(process)
        return process

    monkeypatch.setattr(
        "app.services.ai_research_job_executor._resolve_command",
        lambda _command: [str(tmp_path / "codex.CMD")],
    )
    monkeypatch.setattr("app.services.ai_research_job_executor.subprocess.Popen", fake_popen)
    monkeypatch.setattr(
        "app.services.ai_research_job_executor.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            "codex-cli 0.144.1",
            "",
        ),
    )

    result = PersistentAIJobExecutor(cfg).execute_step(
        job=job,
        run=run,
        step_name="PLAN",
        context={},
        workspace=tmp_path / "isolated-job",
        watchdog_seconds=5,
    )

    command = created[0].command
    assert command[0].endswith("codex.CMD")
    for flag in (
        "--search",
        "read-only",
        "--skip-git-repo-check",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--json",
        "--output-schema",
        "--output-last-message",
        "--color",
        "never",
    ):
        assert flag in command
    assert command[-1] == "-"
    assert "PHASE\nPLAN" in str(created[0].stdin_value)
    assert all("PHASE\nPLAN" not in item for item in command)
    assert result["topics"] == ["macro"]


def test_output_file_is_primary_over_jsonl_agent_message(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = settings(tmp_path)
    job, run = make_job_and_run(cfg)
    fallback = plan_payload("wrong-jsonl")
    stdout = json.dumps(
        {
            "type": "item.completed",
            "item": {"type": "agent_message", "text": json.dumps(fallback)},
        }
    )

    monkeypatch.setattr(
        "app.services.ai_research_job_executor._resolve_command",
        lambda _command: [str(tmp_path / "codex.CMD")],
    )
    monkeypatch.setattr(
        "app.services.ai_research_job_executor.subprocess.Popen",
        lambda command, **kwargs: FakeProcess(
            command,
            payload=plan_payload("from-output-file"),
            stdout=stdout,
        ),
    )
    monkeypatch.setattr(
        PersistentAIJobExecutor,
        "_executable_version",
        lambda self, command: "codex-cli fake",
    )

    result = PersistentAIJobExecutor(cfg).execute_step(
        job=job,
        run=run,
        step_name="PLAN",
        context={},
        workspace=tmp_path / "job",
        watchdog_seconds=5,
    )
    assert result["topics"] == ["from-output-file"]


def test_all_step_schemas_are_closed_bounded_and_exclude_ai_actual() -> None:
    schemas = all_step_output_schemas()
    assert set(schemas) == {
        "PLAN",
        "SEARCH",
        "OPEN_SOURCE",
        "EXTRACT",
        "CROSS_CHECK",
        "VALIDATE",
    }
    for schema in schemas.values():
        validate_output_schema(schema)
        serialized = json.dumps(schema)
        assert '"additionalProperties": false' in serialized
        assert '"actual"' not in serialized
    legacy = legacy_research_output_schema()
    validate_output_schema(legacy)
    actual_nodes = [
        legacy["properties"]["results"]["items"]["properties"]["actual"],
        legacy["properties"]["results"]["items"]["properties"]["metrics"]["items"][
            "properties"
        ]["actual"],
    ]
    assert actual_nodes == [{"type": "null"}, {"type": "null"}]


def test_invalid_local_schema_prevents_process_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = settings(tmp_path)
    job, run = make_job_and_run(cfg)
    started = False

    def fake_popen(command, **kwargs):
        del command, kwargs
        nonlocal started
        started = True
        raise AssertionError("process must not start")

    monkeypatch.setattr(
        "app.services.ai_research_job_executor._resolve_command",
        lambda _command: [str(tmp_path / "codex.CMD")],
    )
    monkeypatch.setattr(
        "app.services.ai_research_job_executor.step_output_schema",
        lambda _step: {
            "type": "object",
            "additionalProperties": True,
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
        },
    )
    monkeypatch.setattr("app.services.ai_research_job_executor.subprocess.Popen", fake_popen)

    with pytest.raises(CodexCLIError) as raised:
        PersistentAIJobExecutor(cfg).execute_step(
            job=job,
            run=run,
            step_name="PLAN",
            context={},
            workspace=tmp_path / "job",
            watchdog_seconds=5,
        )
    assert raised.value.category == "SCHEMA_INVALID"
    assert raised.value.retryable is False
    assert started is False


def test_cli_failure_keeps_redacted_stderr_jsonl_and_classification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = settings(tmp_path)
    job, run = make_job_and_run(cfg)
    secret = "sk-super-secret-value"
    stdout = json.dumps(
        {
            "type": "turn.failed",
            "error": f'authorization: Bearer {secret}',
        }
    )
    monkeypatch.setattr(
        "app.services.ai_research_job_executor._resolve_command",
        lambda _command: [str(tmp_path / "codex.CMD")],
    )
    monkeypatch.setattr(
        "app.services.ai_research_job_executor.subprocess.Popen",
        lambda command, **kwargs: FakeProcess(
            command,
            payload=None,
            stdout=stdout,
            stderr=f"not logged in token={secret}",
            returncode=1,
        ),
    )
    monkeypatch.setattr(
        PersistentAIJobExecutor,
        "_executable_version",
        lambda self, command: "codex-cli fake",
    )

    with pytest.raises(CodexCLIError) as raised:
        PersistentAIJobExecutor(cfg).execute_step(
            job=job,
            run=run,
            step_name="PLAN",
            context={},
            workspace=tmp_path / "job",
            watchdog_seconds=5,
        )
    diagnostic = raised.value.diagnostic
    serialized = json.dumps(diagnostic)
    assert diagnostic["category"] == "AUTH_UNAVAILABLE"
    assert diagnostic["retry_classification"] == "NON_RETRYABLE"
    assert diagnostic["exit_code"] == 1
    assert diagnostic["error_events"]
    assert secret not in serialized
    assert "<redacted>" in serialized


@pytest.mark.parametrize(
    ("text", "category", "retryable"),
    [
        ("invalid output schema", "SCHEMA_INVALID", False),
        ("unexpected argument --bad", "UNSUPPORTED_ARGUMENT", False),
        ("not logged in", "AUTH_UNAVAILABLE", False),
        ("rate limit 429", "RATE_LIMIT", True),
        ("network temporarily unavailable", "NETWORK_TRANSIENT", True),
        ("status 503 server error", "BACKEND_5XX", True),
        ("deadline exceeded", "TIMEOUT", True),
        ("opaque exit", "CLI_EXIT", False),
    ],
)
def test_retry_classification_is_fail_closed(
    text: str,
    category: str,
    retryable: bool,
) -> None:
    assert classify_codex_failure(exit_code=1, stderr=text) == (category, retryable)


class LiveCapability:
    @staticmethod
    def probe(*, persist: bool = True) -> dict[str, Any]:
        del persist
        return {"status": "LIVE_VERIFIED"}


class FailingExecutor:
    def __init__(self, diagnostic: dict[str, Any]) -> None:
        self.diagnostic = diagnostic

    def execute_step(self, **kwargs):
        del kwargs
        raise CodexCLIError(self.diagnostic)


def diagnostic(
    tmp_path: Path,
    *,
    category: str,
    retryable: bool,
    secret: str | None = None,
) -> dict[str, Any]:
    stderr = f"token={secret}" if secret else category
    return build_diagnostic(
        category=category,
        retryable=retryable,
        command=[str(tmp_path / "codex.CMD"), "exec", "-"],
        step="PLAN",
        workspace=tmp_path / "workspace",
        duration_ms=12,
        executable_version="codex-cli fake",
        exit_code=1,
        stderr=stderr,
    )


def test_non_retryable_error_has_one_attempt_and_closes_job_run_step(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cfg = settings(tmp_path)
    job, _ = make_job_and_run(cfg)
    secret = "super-secret-token-value"
    caplog.set_level(logging.INFO)
    worker = AIResearchWorker(
        cfg,
        executor=FailingExecutor(
            diagnostic(
                tmp_path,
                category="SCHEMA_INVALID",
                retryable=False,
                secret=secret,
            )
        ),
        capabilities=LiveCapability(),
        worker_id="hardening-worker",
    )
    assert worker.process_once()

    restored = AIResearchJobRepository(cfg).get(job["job_id"])
    run = ResearchRuntimeRepository(cfg).latest("MNQ")
    assert restored["status"] == "FAILED"
    assert restored["attempts"] == 1
    assert restored["completed_at"]
    assert restored["attempt_history"][0]["retry_classification"] == "NON_RETRYABLE"
    assert run["status"] == "FAILED" and run["completed_at"]
    assert run["steps"][0]["status"] == "FAILED"
    assert run["missing_topics"] == run["required_topics"]
    persisted = json.dumps({"job": restored, "run": run})
    assert secret not in persisted
    assert secret not in caplog.text


def test_retryable_timeout_schedules_job_and_run_then_reacquires_correct_step(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    job, run = make_job_and_run(cfg)
    jobs = AIResearchJobRepository(cfg)
    acquired = jobs.acquire_next("retry-worker")
    runtime = ResearchRuntimeRepository(cfg)
    step, _ = runtime.begin_step(
        run["run_id"],
        "PLAN",
        1,
        {},
        backend="fake",
        tool="fake",
    )
    timeout_diagnostic = diagnostic(tmp_path, category="TIMEOUT", retryable=True)
    runtime.fail_step(step["step_id"], "timeout", diagnostic=timeout_diagnostic)
    retried = jobs.retry_or_fail(
        acquired["job_id"],
        "retry-worker",
        error="codex_cli:timeout",
        timed_out=True,
        retryable=True,
        diagnostic=timeout_diagnostic,
        delays=[0],
    )
    assert retried["status"] == "RETRY_SCHEDULED"
    assert runtime.get_run(run["run_id"])["status"] == "RETRY_SCHEDULED"

    reacquired = jobs.acquire_next("retry-worker")
    assert reacquired["attempts"] == 2
    resumed_run = runtime.get_run(run["run_id"])
    assert resumed_run["status"] == "RUNNING"
    retried_step, execute = runtime.begin_step(
        run["run_id"],
        "PLAN",
        1,
        {},
        backend="fake",
        tool="fake",
    )
    assert execute is True and retried_step["attempt"] == 2
    assert len(runtime.get_run(run["run_id"])["steps"][0]["attempt_history"]) == 2


def test_reconciliation_repairs_historical_running_run_for_failed_job(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    job, run = make_job_and_run(cfg)
    with connect_sqlite(cfg.database_path) as conn:
        conn.execute(
            """
            UPDATE ai_research_jobs
            SET status='FAILED',completed_at='2026-07-22T10:00:00+00:00',
                last_error='codex_cli:config_invalid'
            WHERE job_id=?
            """,
            (job["job_id"],),
        )
        conn.execute(
            "UPDATE research_runs SET status='RUNNING',completed_at=NULL WHERE run_id=?",
            (run["run_id"],),
        )
        conn.commit()

    repository = AIResearchJobRepository(cfg)
    restored = ResearchRuntimeRepository(cfg).get_run(run["run_id"])
    assert repository.reconcile_lifecycle() == 0
    assert restored["status"] == "FAILED"
    assert restored["completed_at"]
    assert restored["blocking_gaps"] == ["job_terminal:FAILED"]


def test_additive_migration_from_schema_10_preserves_rows_and_repairs_run(
    tmp_path: Path,
) -> None:
    database = tmp_path / "schema10.sqlite"
    with connect_sqlite(database) as conn:
        conn.execute(
            """
            CREATE TABLE schema_migrations(
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              applied_at TEXT NOT NULL
            )
            """
        )
        for version, (name, sql) in enumerate(MIGRATIONS[:10], start=1):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)",
                (version, name, "2026-07-22T00:00:00+00:00"),
            )
        conn.execute(
            """
            INSERT INTO ai_research_jobs(
              job_id,idempotency_key,job_type,symbol,correlation_id,status,priority,
              request_payload_json,policy_version,prompt_version,attempts,max_attempts,
              created_at,updated_at
            ) VALUES (
              'airj-preserved','idem-preserved','MNQ_MARKET_RESEARCH','MNQ','migration',
              'FAILED',100,'{}','v1','v1',1,3,
              '2026-07-22T09:00:00+00:00','2026-07-22T10:00:00+00:00'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO research_runs(
              run_id,job_id,symbol,profile_id,prompt_version,policy_version,status,
              input_fingerprint,request_json,required_topics_json,created_at,updated_at
            ) VALUES (
              'rrun-preserved','airj-preserved','MNQ','MNQ_MARKET_RESEARCH','v1','v1',
              'RUNNING','fingerprint','{}','["macro"]',
              '2026-07-22T09:00:00+00:00','2026-07-22T10:00:00+00:00'
            )
            """
        )
        conn.commit()

    result = migrate_database(database)
    assert result["schema_version"] == 11
    assert result["reconciled_research_runs"] == 1
    with connect_sqlite(database) as conn:
        job = conn.execute(
            "SELECT job_id FROM ai_research_jobs WHERE job_id='airj-preserved'"
        ).fetchone()
        run = conn.execute(
            "SELECT status,completed_at FROM research_runs WHERE run_id='rrun-preserved'"
        ).fetchone()
        attempt_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(ai_research_job_attempts)")
        }
    assert job["job_id"] == "airj-preserved"
    assert run["status"] == "FAILED" and run["completed_at"]
    assert {"error_category", "exit_code", "retry_classification", "diagnostic_json"} <= (
        attempt_columns
    )


def test_smoke_polling_helper_fails_fast_on_terminal_job(tmp_path: Path) -> None:
    del tmp_path
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    helper = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "market_research_smoke_helpers.ps1"
    )
    command = (
        f". '{helper}'; "
        "Get-SmokePollingDecision -RunStatus RUNNING -JobStatus FAILED "
        "-QueueDepth 0 -RunningJobs 0 -AttemptStatus FAILED | ConvertTo-Json -Compress"
    )
    completed = subprocess.run(
        [powershell, "-NoProfile", "-Command", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=20,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    decision = json.loads(completed.stdout)
    assert decision == {
        "done": True,
        "failed": True,
        "reason": "job_terminal_failure:FAILED",
    }
