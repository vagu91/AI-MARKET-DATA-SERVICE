from __future__ import annotations

import asyncio
import json
import logging
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.core.config import Settings
from app.api.routes import ai_research_jobs_latest
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.services.ai_research_job_executor import PersistentAIJobExecutor
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_worker import AIResearchWorker
from app.services.ai_research_capability_service import AIResearchCapabilityService
from app.services.codex_runtime_contract import (
    CodexCLIError,
    all_step_output_schemas,
    build_codex_exec_command,
    build_diagnostic,
    classify_codex_failure,
    legacy_research_output_schema,
    validate_isolated_command,
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


def test_relative_workspace_is_canonical_once_for_cwd_and_all_codex_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    cfg = settings(tmp_path, ai_job_workspace_root=Path("relative-jobs"))
    job, run = make_job_and_run(cfg)
    observed: dict[str, Any] = {}

    def fake_popen(command, **kwargs):
        cwd = Path(kwargs["cwd"])
        schema = Path(command[command.index("--output-schema") + 1])
        output = Path(command[command.index("--output-last-message") + 1])
        observed.update(command=command, cwd=cwd, schema=schema, output=output)
        assert cwd.is_absolute()
        assert schema.is_absolute() and schema.is_file()
        assert output.is_absolute() and output.parent == cwd
        assert schema.parent == cwd
        # A fake process starting in cwd can open the exact command paths.
        json.loads((cwd / schema).read_text(encoding="utf-8"))
        return FakeProcess(command, payload=plan_payload("canonical"))

    monkeypatch.setattr(
        "app.services.ai_research_job_executor._resolve_command",
        lambda _command: [str(tmp_path / "codex.CMD")],
    )
    monkeypatch.setattr(
        "app.services.ai_research_job_executor.subprocess.Popen",
        fake_popen,
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
        workspace=Path("relative-jobs") / job["job_id"],
        watchdog_seconds=5,
    )

    command = observed["command"]
    cd_path = Path(command[command.index("--cd") + 1])
    assert result["topics"] == ["canonical"]
    assert cd_path == observed["cwd"] == observed["cwd"].resolve()
    assert observed["schema"].relative_to(observed["cwd"]).parts == ("plan_output_schema.json",)
    assert observed["output"].relative_to(observed["cwd"]).parts == ("plan_output.json",)


def test_isolated_command_rejects_relative_divergent_and_traversing_paths(
    tmp_path: Path,
) -> None:
    workspace = (tmp_path / "workspace").resolve()
    workspace.mkdir()
    schema = workspace / "schema.json"
    schema.write_text("{}", encoding="utf-8")
    output = workspace / "output.json"
    command = build_codex_exec_command(
        ["codex"],
        workspace=workspace,
        schema_path=schema,
        output_path=output,
    )
    validate_isolated_command(command, cwd=workspace)

    relative = list(command)
    relative[relative.index("--cd") + 1] = "workspace"
    with pytest.raises(ValueError, match="workspace_path_relative"):
        validate_isolated_command(relative, cwd=workspace)

    divergent = tmp_path / "other"
    divergent.mkdir()
    with pytest.raises(ValueError, match="cwd_workspace_mismatch"):
        validate_isolated_command(command, cwd=divergent)

    traversing = list(command)
    traversing[traversing.index("--output-last-message") + 1] = str(
        workspace / "nested" / ".." / "escaped.json"
    )
    with pytest.raises(ValueError, match="output_path_traversal"):
        validate_isolated_command(traversing, cwd=workspace)

    outside_schema = tmp_path / "outside-schema.json"
    outside_schema.write_text("{}", encoding="utf-8")
    outside = list(command)
    outside[outside.index("--output-schema") + 1] = str(outside_schema)
    with pytest.raises(ValueError, match="schema_outside_workspace"):
        validate_isolated_command(outside, cwd=workspace)

    missing_schema = list(command)
    missing_schema[missing_schema.index("--output-schema") + 1] = str(
        workspace / "missing-schema.json"
    )
    with pytest.raises(ValueError, match="schema_path_invalid"):
        validate_isolated_command(missing_schema, cwd=workspace)

    missing_parent = list(command)
    missing_parent[missing_parent.index("--output-last-message") + 1] = str(
        workspace / "missing-parent" / "output.json"
    )
    with pytest.raises(ValueError, match="output_parent_missing"):
        validate_isolated_command(missing_parent, cwd=workspace)


def test_capability_contract_canonicalizes_relative_workspace_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    cfg = settings(
        tmp_path,
        ai_job_workspace_root=Path("relative-capability"),
        codex_cli_command=str(tmp_path / "codex.CMD"),
    )
    (tmp_path / "codex.CMD").write_text("@echo off", encoding="utf-8")

    def runner(command, **kwargs):
        del kwargs
        joined = " ".join(command)
        if command[-1] == "--version":
            return subprocess.CompletedProcess(command, 0, "codex-cli fake", "")
        if command[-2:] == ["login", "status"]:
            return subprocess.CompletedProcess(command, 0, "Logged in", "")
        if command[-2:] == ["exec", "--help"]:
            options = (
                "--skip-git-repo-check --ephemeral --ignore-user-config "
                "--ignore-rules --output-schema --output-last-message --color --json"
            )
            return subprocess.CompletedProcess(command, 0, options, "")
        return subprocess.CompletedProcess(command, 0, f"exec --search {joined}", "")

    report = AIResearchCapabilityService(cfg, runner=runner).probe(persist=False)
    probe = (tmp_path / "relative-capability" / "capability-offline-probe").resolve()
    assert report["status"] == "READY_TO_SMOKE"
    assert report["isolated_command_constructed"] is True
    assert probe.is_absolute()
    assert (probe / "output_schema.json").is_file()


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


def test_executor_maps_windows_os_error_3_to_non_retryable_path_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = settings(tmp_path)
    job, run = make_job_and_run(cfg)
    monkeypatch.setattr(
        "app.services.ai_research_job_executor._resolve_command",
        lambda _command: [str(tmp_path / "codex.CMD")],
    )
    monkeypatch.setattr(
        "app.services.ai_research_job_executor.subprocess.Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError("Error: Impossibile trovare il percorso specificato. (os error 3)")
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

    assert raised.value.category == "PATH_INVALID"
    assert raised.value.retryable is False
    assert raised.value.retry_classification == "NON_RETRYABLE"
    persisted_shape = raised.value.diagnostic["command_shape"]
    for path_flag in ("--cd", "--output-schema", "--output-last-message"):
        value = Path(persisted_shape[persisted_shape.index(path_flag) + 1])
        assert value.is_absolute()


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
        legacy["properties"]["results"]["items"]["properties"]["metrics"]["items"]["properties"][
            "actual"
        ],
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
            "error": f"authorization: Bearer {secret}",
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
        ("Error: Impossibile trovare il percorso specificato. (os error 3)", "PATH_INVALID", False),
        ("path not found", "PATH_INVALID", False),
        ("file not found", "PATH_INVALID", False),
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
                category=classify_codex_failure(
                    exit_code=1,
                    stderr="Error: Impossibile trovare il percorso specificato. (os error 3)",
                )[0],
                retryable=classify_codex_failure(
                    exit_code=1,
                    stderr="Error: Impossibile trovare il percorso specificato. (os error 3)",
                )[1],
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
    assert restored["last_diagnostic"]["category"] == "PATH_INVALID"
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


def test_latest_jobs_is_newest_first_and_compact_view_omits_payloads(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository = AIResearchJobRepository(cfg)
    common = {
        "job_type": "MNQ_MARKET_RESEARCH",
        "symbol": "MNQ",
        "correlation_id": "latest-order",
        "request_payload": {"large_market_payload": "x" * 10_000},
        "policy_version": "test-policy",
        "prompt_version": "test-prompt",
    }
    older, _ = repository.enqueue(idempotency_key="latest-older", **common)
    newer, _ = repository.enqueue(idempotency_key="latest-newer", **common)
    assert older["created_at"] == newer["created_at"]

    compact = repository.latest(limit=2, symbol="MNQ", view="compact")
    full = repository.latest(limit=2, symbol="MNQ")
    endpoint_compact = asyncio.run(
        ai_research_jobs_latest(
            limit=2,
            symbol="MNQ",
            view="compact",
            enrichment_orchestrator=SimpleNamespace(settings=cfg),
        )
    )

    assert [item["job_id"] for item in compact] == [
        newer["job_id"],
        older["job_id"],
    ]
    assert [item["job_id"] for item in full] == [
        newer["job_id"],
        older["job_id"],
    ]
    assert [item["job_id"] for item in endpoint_compact] == [
        newer["job_id"],
        older["job_id"],
    ]
    assert all("request_payload" not in item for item in compact)
    assert all("result_payload" not in item for item in compact)
    assert full[0]["request_payload"]["large_market_payload"] == "x" * 10_000


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
    assert result["schema_version"] == 19
    assert result["reconciled_research_runs"] == 1
    with connect_sqlite(database) as conn:
        job = conn.execute(
            "SELECT job_id FROM ai_research_jobs WHERE job_id='airj-preserved'"
        ).fetchone()
        run = conn.execute(
            "SELECT status,completed_at FROM research_runs WHERE run_id='rrun-preserved'"
        ).fetchone()
        attempt_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(ai_research_job_attempts)")
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
    helper = Path(__file__).resolve().parents[1] / "scripts" / "market_research_smoke_helpers.ps1"
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


def test_smoke_writes_compact_failure_report_before_rethrow_in_caller_location(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "smoke_test_market_research.ps1"
    caller_location = tmp_path / "caller-location"
    environment_location = tmp_path / "environment-location"
    caller_location.mkdir()
    environment_location.mkdir()
    output = caller_location / "relative-smoke-output"
    caught_error = tmp_path / "caught-error.txt"
    command = f"""
[Environment]::CurrentDirectory = '{environment_location}'
Set-Location -LiteralPath '{caller_location}'
function global:Invoke-RestMethod {{
    param(
        [string]$Method,
        [string]$Uri,
        [string]$ContentType,
        [string]$Body
    )
    if ($Uri -match '/ai-research/capabilities$') {{
        return [pscustomobject]@{{ status = 'READY_TO_SMOKE' }}
    }}
    if ($Method -eq 'Post') {{
        return [pscustomobject]@{{ run_id = 'run-test'; job_id = 'job-test' }}
    }}
    throw 'forced failure token=super-secret-market-payload'
}}
try {{
    & '{script}' -OutputDirectory '.\\relative-smoke-output' -TimeoutSeconds 1
    exit 2
}}
catch {{
    [System.IO.File]::WriteAllText(
        '{caught_error}',
        [string]$_.Exception.Message
    )
    exit 0
}}
"""
    wrapper = tmp_path / "smoke-wrapper.ps1"
    wrapper.write_text(command, encoding="utf-8")
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(wrapper),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert (output / "failure-report.json").is_file()
    assert (output / "capabilities.json").is_file()
    assert (output / "queued.json").is_file()
    assert caught_error.read_text(encoding="utf-8").startswith("forced failure")
    raw_report = (output / "failure-report.json").read_text(encoding="utf-8-sig").strip()
    report = json.loads(raw_report)
    assert Path(report["report_path"]) == (output / "failure-report.json").resolve()
    assert "super-secret-market-payload" not in raw_report
    assert "request_payload" not in raw_report
    assert "result_payload" not in raw_report
    assert "\n" not in raw_report and "\r" not in raw_report
    script_text = script.read_text(encoding="utf-8")
    catch_body = script_text[script_text.index("catch {") :]
    assert catch_body.index("Write-SmokeFailureReport") < catch_body.rindex("\n    throw\n")


def test_smoke_failure_report_contains_complete_budget_diagnostic(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    script = Path(__file__).resolve().parents[1] / "scripts" / "smoke_test_market_research.ps1"
    output = tmp_path / "budget-failure"
    command = f"""
function global:Invoke-RestMethod {{
    param(
        [string]$Method,
        [string]$Uri,
        [string]$ContentType,
        [string]$Body
    )
    if ($Uri -match '/ai-research/capabilities$') {{
        return [pscustomobject]@{{ status = 'READY_TO_SMOKE' }}
    }}
    if ($Method -eq 'Post') {{
        return [pscustomobject]@{{ run_id = 'run-budget'; job_id = 'job-budget' }}
    }}
    if ($Uri -match '/market-research/mnq/runs/run-budget$') {{
        return [pscustomobject]@{{
            status = 'FAILED'
            steps = @([pscustomobject]@{{ step_name = 'SEARCH'; status = 'FAILED' }})
        }}
    }}
    if ($Uri -match '/ai-research/jobs/job-budget$') {{
        $diagnostic = [pscustomobject]@{{
            category = 'BUDGET_EXCEEDED'
            resource = 'searches'
            configured_limit = 8
            observed_count = 9
            remaining_before_step = 8
            step = 'SEARCH'
            retry_classification = 'NON_RETRYABLE'
            timestamp = '2026-07-23T10:00:00+00:00'
            tool_events_observed = @(
                [pscustomobject]@{{
                    event_type = 'search'
                    query = 'bounded query'
                    source_url = $null
                    canonical_url = $null
                }}
            )
            effective_usage = [pscustomobject]@{{
                search_count = 9
                opened_source_count = 0
            }}
            effective_budget = [pscustomobject]@{{
                max_searches = 8
                max_opened_sources = 12
                remaining_searches = 0
                remaining_opened_sources = 12
                daily_runs_remaining = 7
                daily_searches_remaining = 55
                daily_opened_sources_remaining = 96
                remaining_runtime_seconds = 420
            }}
        }}
        return [pscustomobject]@{{
            status = 'FAILED'
            attempts = 1
            max_attempts = 3
            attempt_history = @(
                [pscustomobject]@{{ status = 'FAILED'; diagnostic = $diagnostic }}
            )
            last_diagnostic = $diagnostic
        }}
    }}
    if ($Uri -match '/ai-research/status$') {{
        return [pscustomobject]@{{
            metrics = [pscustomobject]@{{ queue_depth = 0; running_jobs = 0 }}
        }}
    }}
    throw "unexpected URI: $Uri"
}}
try {{
    & '{script}' -OutputDirectory '{output}' -TimeoutSeconds 1
    exit 2
}}
catch {{
    exit 0
}}
"""
    wrapper = tmp_path / "budget-failure-wrapper.ps1"
    wrapper.write_text(command, encoding="utf-8")
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(wrapper),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads((output / "failure-report.json").read_text(encoding="utf-8-sig"))
    assert report["error_category"] == "BUDGET_EXCEEDED"
    assert report["resource"] == "searches"
    assert report["configured_limit"] == 8
    assert report["observed_count"] == 9
    assert report["remaining_before_step"] == 8
    assert report["step"] == "SEARCH"
    assert report["retry_classification"] == "NON_RETRYABLE"
    assert len(report["tool_events_observed"]) == 1
    event = report["tool_events_observed"][0]
    assert event["event_type"] == "search"
    assert event["query"] == "bounded query"
    assert event["source_url"] == ""
    assert event["canonical_url"] == ""
    assert report["effective_usage"]["search_count"] == 9
    assert report["effective_budget"]["remaining_searches"] == 0
    assert report["diagnostic"]["category"] == "BUDGET_EXCEEDED"
