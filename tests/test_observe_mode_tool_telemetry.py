from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.services.agentic_research_runtime import (
    AgenticResearchRuntime,
    _reconcile_declared_sources,
)
from app.services.ai_research_job_executor import (
    _tool_events_from_jsonl_line,
    parse_codex_json_event_stream,
)
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_worker import AIResearchWorker
from app.services.codex_runtime_contract import step_output_schema
from app.services.research_budget import (
    ResearchBudgetExceeded,
    build_effective_budget,
)
from app.services.research_metrics_service import ResearchMetricsService
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.research_tool_telemetry import (
    ProgressLoopGuard,
    ResearchLoopDetected,
    normalize_codex_event,
)


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "source_policy.json"
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    tmp_path.mkdir(parents=True, exist_ok=True)
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": True,
        "ai_research_web_access_enabled": True,
        "ai_worker_enabled": True,
        "research_max_searches": 2,
        "research_max_opened_sources": 2,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def enqueue(cfg: Settings, identity: str) -> dict[str, Any]:
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id=identity,
        request_payload={"database_context": {}, "identity": identity},
        force=True,
    )
    assert created
    return job


def ensure_run(
    cfg: Settings,
    identity: str,
) -> tuple[ResearchRuntimeRepository, dict[str, Any], dict[str, Any]]:
    repository = ResearchRuntimeRepository(cfg)
    job = enqueue(cfg, identity)
    run = repository.ensure_run(
        job,
        "MNQ_MARKET_RESEARCH",
        "mnq_market_research_v1",
    )
    return repository, job, run


class NoopVerifier:
    def verify(self, evidence):
        del evidence
        return None


class PhaseEventsExecutor:
    def __init__(
        self,
        *,
        events_by_step: dict[str, list[dict[str, Any]]] | None = None,
        clock: Any = None,
        advance: float = 0,
    ) -> None:
        self.events_by_step = events_by_step or {}
        self.clock = clock
        self.advance = advance
        self.calls: list[str] = []

    def execute_step(self, **kwargs):
        step = kwargs["step_name"]
        self.calls.append(step)
        if self.clock is not None:
            self.clock.value += self.advance
        events = self.events_by_step.get(step, [])
        if step == "VALIDATE":
            return {
                "status": "NO_DATA",
                "claims": [],
                "_tool_events": events,
            }
        return {"status": "COMPLETED", "_tool_events": events}


class AdvancingClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class LiveCapability:
    @staticmethod
    def probe(*, persist: bool = True) -> dict[str, Any]:
        del persist
        return {"status": "LIVE_VERIFIED"}


def fixture_lines(name: str) -> list[str]:
    return (FIXTURES / name).read_text(encoding="utf-8").splitlines()


def test_observe_is_initial_default(tmp_path: Path) -> None:
    assert settings(tmp_path).research_budget_mode == "observe"


def test_observe_daily_threshold_does_not_zero_per_run_capacity(
    tmp_path: Path,
) -> None:
    cfg = settings(
        tmp_path,
        research_daily_budget_searches=1,
        research_daily_budget_opened_sources=1,
    )
    budget = build_effective_budget(
        cfg,
        required_topics=["macro", "risk"],
        daily_usage={"search_count": 5, "opened_source_count": 5},
        daily_runs=10,
        runtime_seconds=600,
    )

    assert budget["budget_mode"] == "observe"
    assert budget["max_searches"] == 2
    assert budget["max_opened_sources"] == 2
    assert budget["daily_searches_remaining"] == 0
    assert budget["daily_opened_sources_remaining"] == 0
    budget["remaining_searches"] = 0
    budget["remaining_opened_sources"] = 0
    assert step_output_schema("SEARCH", budget)["properties"]["searches"]["maxItems"] == 2
    assert step_output_schema("OPEN_SOURCE", budget)["properties"]["sources"]["maxItems"] == 2


def test_raw_fixture_normalizes_lifecycle_and_empty_events() -> None:
    lines = fixture_lines("codex_search_lifecycle.jsonl")
    envelopes = [
        envelope
        for line in lines
        for envelope in _tool_events_from_jsonl_line(
            line,
            step_name="SEARCH",
        )
    ]

    assert [item["lifecycle"] for item in envelopes] == [
        "started",
        "completed",
        "completed",
        "started",
        "completed",
        "completed",
    ]
    assert envelopes[0]["item_id"] == "search-1"
    assert envelopes[0]["tool_action_fingerprint"] == envelopes[1]["tool_action_fingerprint"]
    assert envelopes[1]["semantic_action"] == "search"
    assert envelopes[0]["counts_usage"] is False
    assert envelopes[1]["counts_usage"] is True
    empty = _tool_events_from_jsonl_line(lines[3], step_name="SEARCH")[0]
    assert empty["semantic_action"] == "non_operational"
    assert empty["counts_usage"] is False

    _payload, _events, usage, _error = parse_codex_json_event_stream(
        "\n".join(lines),
        step_name="SEARCH",
    )
    assert usage == {
        "input_tokens": 1200,
        "output_tokens": 300,
        "cached_tokens": 400,
        "reasoning_tokens": 90,
        "total_tokens": 1500,
    }


def test_lifecycle_and_replay_are_idempotent_in_repository(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = ensure_run(cfg, "replay")
    step, _ = repository.begin_step(
        run["run_id"],
        "SEARCH",
        2,
        {},
        backend="fixture",
        tool="fixture",
    )
    events = [
        envelope
        for line in fixture_lines("codex_search_lifecycle.jsonl")
        for envelope in _tool_events_from_jsonl_line(
            line,
            step_name="SEARCH",
        )
    ]

    first = repository.record_tool_events(run["run_id"], step["step_id"], events)
    replay = repository.record_tool_events(
        run["run_id"],
        step["step_id"],
        events,
    )
    restored = repository.get_run(run["run_id"])

    assert first["raw_event_count"] == 5
    assert first["normalized_action_count"] == 1
    assert first["deduplicated_tool_call_count"] == 1
    assert first["search_count"] == 1
    assert replay["raw_event_count"] == 5
    assert replay["event_inserted"] is False
    assert len(restored["steps"][0]["telemetry"]) == 5
    persisted_envelope = restored["steps"][0]["telemetry"][0]
    assert {
        "raw_event_type",
        "lifecycle",
        "item_id",
        "item_type",
        "phase",
        "run_id",
        "job_id",
        "provider_tool_type",
        "semantic_action",
        "observed_at",
        "tool_action_fingerprint",
        "status",
        "usage",
    } <= persisted_envelope.keys()


def test_phase_aware_url_classification_and_failed_action() -> None:
    open_events = [
        envelope
        for line in fixture_lines("codex_open_source_lifecycle.jsonl")
        for envelope in _tool_events_from_jsonl_line(
            line,
            step_name="OPEN_SOURCE",
        )
    ]
    cross_events = [
        envelope
        for line in fixture_lines("codex_cross_check_lifecycle.jsonl")
        for envelope in _tool_events_from_jsonl_line(
            line,
            step_name="CROSS_CHECK",
        )
    ]

    terminal_open = [item for item in open_events if item["lifecycle"] == "completed"]
    assert [item["semantic_action"] for item in terminal_open] == [
        "open_source",
        "fetch",
    ]
    assert all(item["event_type"] == "open_source" for item in terminal_open)
    failed = next(item for item in open_events if item["lifecycle"] == "failed")
    assert failed["counts_usage"] is False
    assert cross_events[0]["semantic_action"] in {
        "open_source",
        "verify_source",
    }
    assert cross_events[1]["semantic_action"] == "search"


def test_declared_sources_are_reconciled_and_claims_fail_closed(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = ensure_run(cfg, "sources")
    step, _ = repository.begin_step(
        run["run_id"],
        "OPEN_SOURCE",
        3,
        {},
        backend="fixture",
        tool="fixture",
    )
    observed = normalize_codex_event(
        {
            "type": "item.completed",
            "item": {
                "id": "bls-open",
                "type": "web_fetch",
                "url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                "http_status": 200,
                "content_hash": "verified-bls-content",
            },
        },
        step_name="OPEN_SOURCE",
    )
    repository.record_tool_events(run["run_id"], step["step_id"], observed)
    declared = {
        "status": "COMPLETED",
        "sources": [
            {
                "source_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                "canonical_url": None,
                "source_status": "OPENED",
                "evidence_available": True,
                "http_status": 200,
                "content_hash": "model-value",
            },
            {
                "source_url": "https://www.bea.gov/news/glance",
                "canonical_url": None,
                "source_status": "OPENED",
                "evidence_available": True,
                "http_status": 200,
                "content_hash": "invented",
            },
        ],
    }

    reconciled = _reconcile_declared_sources(
        declared,
        repository.observed_sources(run["run_id"]),
    )
    assert reconciled["sources"][0]["model_declared_status"] == "OPENED"
    assert reconciled["sources"][0]["observed_status"] == "OPENED"
    assert reconciled["sources"][0]["verified_status"] == "VERIFIED"
    assert reconciled["sources"][0]["content_hash"] == "verified-bls-content"
    assert reconciled["sources"][1]["observed_status"] == "UNVERIFIED"
    assert reconciled["sources"][1]["verified_status"] == "UNVERIFIED"
    assert reconciled["sources"][1]["evidence_available"] is False
    assert reconciled["sources"][1]["http_status"] is None

    persisted = repository.persist_claims(
        run,
        [
            {
                "topic": "macro",
                "field_semantics": "outcome",
                "value": "Observed BLS context",
                "confidence": 0.8,
                "evidence": [
                    {
                        "source_url": ("https://www.bls.gov/news.release/cpi.nr0.htm"),
                        "canonical_url": None,
                        "publisher": "BLS",
                        "evidence_text": "Observed and verified CPI source.",
                        "published_at": None,
                        "retrieved_at": "2026-07-23T08:00:00+00:00",
                    }
                ],
            },
            {
                "topic": "risk",
                "field_semantics": "outcome",
                "value": "Unobserved BEA context",
                "confidence": 0.8,
                "evidence": [
                    {
                        "source_url": "https://www.bea.gov/news/glance",
                        "canonical_url": None,
                        "publisher": "BEA",
                        "evidence_text": "This source was only model-declared.",
                        "published_at": None,
                        "retrieved_at": "2026-07-23T08:00:00+00:00",
                    }
                ],
            },
        ],
    )
    restored = repository.get_run(run["run_id"])
    linked_evidence = repository.evidence_for_claim(persisted["accepted_claims"][0]["claim_id"])

    assert persisted["accepted_count"] == 1
    assert len(persisted["rejected_claims"]) == 1
    assert "source_not_observed_or_opened" in persisted["rejected_claims"][0]["warnings"]
    assert restored["source_domains"] == ["bls.gov"]
    assert linked_evidence[0]["tool_event_id"].startswith("rtool-")


def _search_events(count: int, *, repeated: bool = False) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for index in range(count):
        query = "same stalled query" if repeated else f"progress query {index}"
        events.extend(
            normalize_codex_event(
                {
                    "type": "item.completed",
                    "item": {
                        "id": f"search-{index}",
                        "type": "web_search",
                        "query": query,
                        "urls": ([] if repeated else [f"https://example.com/new-{index}"]),
                    },
                },
                step_name="SEARCH",
            )
        )
    return events


def test_observe_mode_exceeds_threshold_but_continues_with_metrics(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, research_max_searches=2)
    job = enqueue(cfg, "observe-threshold")
    executor = PhaseEventsExecutor(events_by_step={"SEARCH": _search_events(4)})

    result = AgenticResearchRuntime(
        cfg,
        verifier=NoopVerifier(),
    ).run(job, tmp_path / "observe-work", executor, 30)
    run = ResearchRuntimeRepository(cfg).get_run(result["run_id"])

    assert result["status"] == "NO_DATA"
    assert run["search_count"] == 4
    assert run["threshold_warnings"][0]["resource"] == "searches"
    assert result["metrics"]["budget_mode"] == "observe"
    assert result["metrics"]["searches"] == 4
    assert result["metrics"]["new_sources"] == 0


def test_enforce_mode_uses_deduplicated_terminal_actions(
    tmp_path: Path,
) -> None:
    cfg = settings(
        tmp_path,
        research_budget_mode="enforce",
        research_max_searches=1,
    )
    job = enqueue(cfg, "enforce")
    lifecycle = [
        normalize_codex_event(
            {
                "type": lifecycle_type,
                "item": {
                    "id": "one-action",
                    "type": "web_search",
                    "query": "first query",
                },
            },
            step_name="SEARCH",
        )[0]
        for lifecycle_type in ("item.started", "item.completed")
    ]
    second = _search_events(1)
    second[0]["item_id"] = "second-action"
    second[0]["tool_action_fingerprint"] = "f" * 64
    executor = PhaseEventsExecutor(events_by_step={"SEARCH": [*lifecycle, *second]})

    with pytest.raises(ResearchBudgetExceeded) as raised:
        AgenticResearchRuntime(cfg, verifier=NoopVerifier()).run(
            job,
            tmp_path / "enforce-work",
            executor,
            30,
        )

    run = ResearchRuntimeRepository(cfg).latest("MNQ")
    assert raised.value.diagnostic["observed_count"] == 2
    assert run["search_count"] == 2
    assert run["steps"][1]["telemetry"][0]["lifecycle"] == "started"
    assert run["steps"][1]["telemetry"][1]["lifecycle"] == "completed"


def test_progress_loop_guard_stops_real_loop_but_allows_new_sources(
    tmp_path: Path,
) -> None:
    loop_cfg = settings(
        tmp_path / "loop",
        research_loop_repeat_action_threshold=3,
        research_loop_no_progress_action_threshold=5,
    )
    loop_job = enqueue(loop_cfg, "loop")
    loop_executor = PhaseEventsExecutor(events_by_step={"SEARCH": _search_events(3, repeated=True)})
    with pytest.raises(ResearchLoopDetected):
        AgenticResearchRuntime(loop_cfg, verifier=NoopVerifier()).run(
            loop_job,
            tmp_path / "loop-work",
            loop_executor,
            30,
        )
    loop_run = ResearchRuntimeRepository(loop_cfg).latest("MNQ")
    assert loop_run["loop_detection_count"] == 1
    assert loop_run["steps"][1]["diagnostic"]["category"] == "LOOP_DETECTED"

    progress_cfg = settings(
        tmp_path / "progress",
        research_loop_repeat_action_threshold=2,
        research_loop_no_progress_action_threshold=3,
    )
    progress_job = enqueue(progress_cfg, "progress")
    progress_result = AgenticResearchRuntime(
        progress_cfg,
        verifier=NoopVerifier(),
    ).run(
        progress_job,
        tmp_path / "progress-work",
        PhaseEventsExecutor(events_by_step={"SEARCH": _search_events(8)}),
        30,
    )
    assert progress_result["status"] == "NO_DATA"
    assert progress_result["metrics"]["loop_detections"] == 0
    assert progress_result["metrics"]["searches"] == 8


def test_progress_loop_guard_detects_repeated_tool_error(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, research_loop_repeat_action_threshold=3)
    guard = ProgressLoopGuard(cfg)
    reason = None
    for index in range(3):
        failed = normalize_codex_event(
            {
                "type": "item.failed",
                "item": {
                    "id": f"failed-{index}",
                    "type": "web_fetch",
                    "url": "https://example.com/repeated-error",
                    "status": "failed",
                },
            },
            step_name="OPEN_SOURCE",
        )[0]
        _progress, reason = guard.observe(failed)
    assert reason == "repeated_tool_error"


def test_checkpoint_resume_reuses_run_and_completed_steps(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    job = enqueue(cfg, "checkpoint")
    clock = AdvancingClock()
    first_executor = PhaseEventsExecutor(clock=clock, advance=6)
    runtime = AgenticResearchRuntime(
        cfg,
        verifier=NoopVerifier(),
        monotonic=clock,
    )

    first = runtime.run(job, tmp_path / "checkpoint-first", first_executor, 5)
    assert first["status"] == "CHECKPOINTED"
    assert first["checkpoint"]["next_step"] == "SEARCH"

    second_executor = PhaseEventsExecutor()
    second = runtime.run(
        job,
        tmp_path / "checkpoint-second",
        second_executor,
        30,
    )
    usage = ResearchRuntimeRepository(cfg).daily_budget_usage()
    restored = ResearchRuntimeRepository(cfg).get_run(first["run_id"])

    assert second["run_id"] == first["run_id"]
    assert second["status"] == "NO_DATA"
    assert "PLAN" not in second_executor.calls
    assert restored["continuation_count"] == 1
    assert usage["run_count"] == 1


def test_worker_treats_checkpoint_as_continuation_not_failure(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, ai_job_max_runtime_seconds=5)
    job = enqueue(cfg, "worker-checkpoint")
    clock = AdvancingClock()
    executor = PhaseEventsExecutor(clock=clock, advance=6)
    runtime = AgenticResearchRuntime(
        cfg,
        verifier=NoopVerifier(),
        monotonic=clock,
    )
    worker = AIResearchWorker(
        cfg,
        executor=executor,
        agentic_runtime=runtime,
        capabilities=LiveCapability(),
        worker_id="checkpoint-worker",
    )

    assert worker.process_once() is True
    restored = AIResearchJobRepository(cfg).get(job["job_id"])
    run = ResearchRuntimeRepository(cfg).latest("MNQ")

    assert restored["status"] == "RETRY_SCHEDULED"
    assert restored["last_error"] is None
    assert restored["attempt_history"][0]["status"] == "CHECKPOINTED"
    assert restored["attempt_history"][0]["retry_classification"] == ("CONTINUATION")
    assert run["status"] == "RETRY_SCHEDULED"
    assert run["continuation_count"] == 1


def test_worker_closes_real_loop_with_explicit_terminal_status(
    tmp_path: Path,
) -> None:
    cfg = settings(
        tmp_path,
        research_loop_repeat_action_threshold=3,
        research_loop_no_progress_action_threshold=5,
    )
    job = enqueue(cfg, "worker-loop")
    executor = PhaseEventsExecutor(events_by_step={"SEARCH": _search_events(3, repeated=True)})
    worker = AIResearchWorker(
        cfg,
        executor=executor,
        agentic_runtime=AgenticResearchRuntime(
            cfg,
            verifier=NoopVerifier(),
        ),
        capabilities=LiveCapability(),
        worker_id="loop-worker",
    )

    assert worker.process_once() is True
    restored = AIResearchJobRepository(cfg).get(job["job_id"])
    run = ResearchRuntimeRepository(cfg).latest("MNQ")

    assert restored["status"] == "LOOP_DETECTED"
    assert restored["attempts"] == 1
    assert restored["last_diagnostic"]["category"] == "LOOP_DETECTED"
    assert run["status"] == "LOOP_DETECTED"


def test_usage_metrics_and_cost_unavailable(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = ensure_run(cfg, "usage")
    step, _ = repository.begin_step(
        run["run_id"],
        "SEARCH",
        2,
        {},
        backend="fixture",
        tool="fixture",
    )
    repository.record_tool_events(
        run["run_id"],
        step["step_id"],
        _search_events(2),
        usage={
            "input_tokens": 100,
            "output_tokens": 40,
            "cached_tokens": 20,
            "reasoning_tokens": 10,
            "total_tokens": 140,
        },
    )

    metrics = ResearchMetricsService(cfg).snapshot(run["run_id"])

    assert metrics["usage"] == {
        "input_tokens": 100,
        "output_tokens": 40,
        "cached_tokens": 20,
        "reasoning_tokens": 10,
        "total_tokens": 140,
    }
    assert metrics["deduplicated_tool_calls"] == 2
    assert metrics["cost"] is None
    assert metrics["cost_status"] == "cost_unavailable"


def test_migration_12_is_additive_from_schema_11(tmp_path: Path) -> None:
    database = tmp_path / "schema11.sqlite"
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
        for version, (name, sql) in enumerate(MIGRATIONS[:11], start=1):
            for statement in [value.strip() for value in sql.split(";") if value.strip()]:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)",
                (version, name, "2026-07-23T00:00:00+00:00"),
            )
        conn.execute(
            """
            INSERT INTO ai_research_jobs(
              job_id,idempotency_key,job_type,symbol,correlation_id,status,
              priority,request_payload_json,policy_version,prompt_version,
              attempts,max_attempts,created_at,updated_at
            ) VALUES (
              'job-v11','idem-v11','MNQ_MARKET_RESEARCH','MNQ','migration',
              'PENDING',100,'{}','v1','v1',0,3,
              '2026-07-23T00:00:00+00:00','2026-07-23T00:00:00+00:00'
            )
            """
        )
        conn.commit()

    result = migrate_database(database)
    with connect_sqlite(database) as conn:
        event_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(research_tool_events)")
        }
        run_columns = {row["name"] for row in conn.execute("PRAGMA table_info(research_runs)")}
        evidence_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(research_evidence)")
        }
        preserved = conn.execute(
            "SELECT job_id FROM ai_research_jobs WHERE job_id='job-v11'"
        ).fetchone()

    assert result["schema_version"] == 15
    assert {
        "raw_event_type",
        "lifecycle",
        "item_id",
        "phase",
        "semantic_action",
        "tool_action_fingerprint",
        "counts_usage",
    } <= event_columns
    assert {
        "metrics_json",
        "checkpoint_json",
        "continuation_count",
        "threshold_warnings_json",
        "loop_detection_count",
    } <= run_columns
    assert "tool_event_id" in evidence_columns
    assert preserved["job_id"] == "job-v11"
