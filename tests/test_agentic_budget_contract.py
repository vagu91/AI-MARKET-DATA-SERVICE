from __future__ import annotations

import io
import json
import signal
from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.services.agentic_research_runtime import AgenticResearchRuntime
from app.services.ai_research_job_executor import (
    _communicate_jsonl_incrementally,
    build_step_prompt,
)
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_worker import AIResearchWorker
from app.services.codex_runtime_contract import step_output_schema
from app.services.research_budget import (
    ResearchBudgetExceeded,
    build_effective_budget,
    group_topics_for_budget,
)
from app.services.research_profiles import PROFILES, prompt_context
from app.services.research_runtime_repository import ResearchRuntimeRepository


POLICY = Path(__file__).resolve().parents[1] / "config" / "source_policy.json"


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    tmp_path.mkdir(parents=True, exist_ok=True)
    values = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": True,
        "ai_research_web_access_enabled": True,
        "ai_worker_enabled": True,
        "research_max_searches": 8,
        "research_max_opened_sources": 12,
        "research_budget_mode": "enforce",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def enqueue(cfg: Settings, correlation_id: str) -> dict[str, Any]:
    job, _ = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id=correlation_id,
        request_payload={
            "database_context": {},
            "test_correlation_id": correlation_id,
        },
        force=True,
    )
    return job


class NoopVerifier:
    def verify(self, evidence):
        del evidence
        return None


class PhaseEventExecutor:
    def __init__(self, *, step: str, resource: str, count: int) -> None:
        self.step = step
        self.resource = resource
        self.count = count
        self.calls: list[dict[str, Any]] = []

    def execute_step(self, **kwargs):
        self.calls.append(kwargs)
        events: list[dict[str, Any]] = []
        if kwargs["step_name"] == self.step:
            if self.resource == "searches":
                events = [
                    {"event_type": "search", "query": f"query-{index}"}
                    for index in range(self.count)
                ]
            else:
                events = [
                    {
                        "event_type": "open_source",
                        "source_url": f"https://example.com/source-{index}",
                    }
                    for index in range(self.count)
                ]
        if kwargs["step_name"] == "VALIDATE":
            return {"status": "NO_DATA", "claims": [], "_tool_events": events}
        return {"status": "COMPLETED", "_tool_events": events}


def test_mnq_ten_topics_are_grouped_into_default_eight_searches(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    topics = list(PROFILES["MNQ_MARKET_RESEARCH"].required_topics)
    usage = {"search_count": 0, "opened_source_count": 0}
    budget = build_effective_budget(
        cfg,
        required_topics=topics,
        daily_usage=usage,
        daily_runs=0,
        runtime_seconds=600,
    )

    assert len(topics) == 10
    assert cfg.research_max_searches == 8
    assert budget["max_searches"] == 8
    assert len(budget["query_topic_groups"]) == 8
    assert sorted(
        topic
        for group in budget["query_topic_groups"]
        for topic in group
    ) == sorted(topics)
    assert any(len(group) == 2 for group in budget["query_topic_groups"])
    assert group_topics_for_budget(topics, 3) == [
        [topics[0], topics[3], topics[6], topics[9]],
        [topics[1], topics[4], topics[7]],
        [topics[2], topics[5], topics[8]],
    ]


def test_prompt_and_schemas_use_numeric_effective_budget(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    profile = PROFILES["MNQ_MARKET_RESEARCH"]
    budget = build_effective_budget(
        cfg,
        required_topics=list(profile.required_topics),
        daily_usage={"search_count": 0, "opened_source_count": 0},
        daily_runs=0,
        runtime_seconds=600,
    )
    profile_payload = prompt_context(profile, {}, budget)
    prompt = build_step_prompt(
        {"job_id": "job", "job_type": "MNQ_MARKET_RESEARCH"},
        {"run_id": "run"},
        "PLAN",
        {"profile": profile_payload, "effective_budget": budget},
        profile_payload,
    )
    plan = step_output_schema("PLAN", budget)
    search = step_output_schema("SEARCH", budget)
    opened = step_output_schema("OPEN_SOURCE", budget)

    assert '"max_searches": 8' in prompt
    assert '"remaining_searches": 8' in prompt
    assert '"max_opened_sources": 12' in prompt
    assert "null" not in json.dumps(profile_payload["limits"])
    assert (
        plan["properties"]["queries"]["maxItems"]
        == search["properties"]["searches"]["maxItems"]
        == 8
    )
    assert search["properties"]["sources"]["maxItems"] == 12
    assert opened["properties"]["sources"]["maxItems"] == 12


@pytest.mark.parametrize(
    ("step", "resource", "limit"),
    [
        ("SEARCH", "searches", 2),
        ("OPEN_SOURCE", "opened_sources", 2),
    ],
)
def test_runtime_accepts_exact_budget_and_rejects_overshoot_with_audit(
    tmp_path: Path,
    step: str,
    resource: str,
    limit: int,
) -> None:
    setting_name = (
        "research_max_searches"
        if resource == "searches"
        else "research_max_opened_sources"
    )
    cfg = settings(tmp_path, **{setting_name: limit})
    runtime = AgenticResearchRuntime(cfg, verifier=NoopVerifier())
    exact_job = enqueue(cfg, f"exact-{resource}")
    exact = PhaseEventExecutor(step=step, resource=resource, count=limit)
    result = runtime.run(exact_job, tmp_path / f"exact-{resource}", exact, 30)
    exact_run = runtime.repository.get_run(result["run_id"])

    assert result["status"] == "NO_DATA"
    assert exact_run[
        "search_count" if resource == "searches" else "opened_source_count"
    ] == limit

    over_job = enqueue(cfg, f"over-{resource}")
    over = PhaseEventExecutor(step=step, resource=resource, count=limit + 1)
    with pytest.raises(ResearchBudgetExceeded) as raised:
        runtime.run(over_job, tmp_path / f"over-{resource}", over, 30)
    diagnostic = raised.value.diagnostic
    failed_run = runtime.repository.latest("MNQ")
    failed_step = next(item for item in failed_run["steps"] if item["step_name"] == step)

    assert diagnostic["category"] == "BUDGET_EXCEEDED"
    assert diagnostic["resource"] == resource
    assert diagnostic["configured_limit"] == limit
    assert diagnostic["observed_count"] == limit + 1
    assert diagnostic["remaining_before_step"] == limit
    assert diagnostic["retry_classification"] == "NON_RETRYABLE"
    assert len(diagnostic["tool_events_observed"]) == limit + 1
    assert failed_step["status"] == "FAILED"
    assert failed_step["diagnostic"]["category"] == "BUDGET_EXCEEDED"
    assert failed_run[
        "search_count" if resource == "searches" else "opened_source_count"
    ] == limit + 1


class FakeStreamingProcess:
    next_pid = 7200

    def __init__(self, lines: list[str]) -> None:
        self.args = ["fake-codex"]
        self.pid = FakeStreamingProcess.next_pid
        FakeStreamingProcess.next_pid += 1
        self.stdin = io.StringIO()
        self.stdout = io.StringIO("".join(f"{line}\n" for line in lines))
        self.stderr = io.StringIO("")
        self.returncode: int | None = None
        self.terminated = False

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        del timeout
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def send_signal(self, value):
        assert value == signal.CTRL_BREAK_EVENT
        self.terminated = True
        self.returncode = -1

    def kill(self):
        self.terminated = True
        self.returncode = -9


def _search_line(index: int) -> str:
    return json.dumps(
        {
            "type": "item.completed",
            "item": {"type": "web_search", "query": f"stream-query-{index}"},
        }
    )


def test_jsonl_events_are_counted_incrementally_and_overshoot_stops_process() -> None:
    process = FakeStreamingProcess([_search_line(index) for index in range(3)])
    observed: list[dict[str, Any]] = []

    def observer(event: dict[str, Any]) -> None:
        observed.append(event)
        if len(observed) > 2:
            raise RuntimeError("bounded-overshoot")

    with pytest.raises(RuntimeError, match="bounded-overshoot"):
        _communicate_jsonl_incrementally(process, "prompt", 10, observer)

    assert [item["query"] for item in observed] == [
        "stream-query-0",
        "stream-query-1",
        "stream-query-2",
    ]
    assert process.terminated is True


def test_recovery_prompt_excludes_completed_queries_and_opened_sources(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository = ResearchRuntimeRepository(cfg)
    job = enqueue(cfg, "recovery-context")
    run = repository.ensure_run(
        job,
        "MNQ_MARKET_RESEARCH",
        "mnq_market_research_v1",
    )
    step, _ = repository.begin_step(
        run["run_id"],
        "SEARCH",
        2,
        {},
        backend="fake",
        tool="fake",
    )
    repository.record_tool_events(
        run["run_id"],
        step["step_id"],
        [
            {"event_type": "search", "query": "completed query"},
            {
                "event_type": "open_source",
                "source_url": "https://example.com/already-opened",
            },
        ],
    )
    executor = PhaseEventExecutor(step="NEVER", resource="searches", count=0)

    AgenticResearchRuntime(
        cfg,
        repository=repository,
        verifier=NoopVerifier(),
    ).run(job, tmp_path / "recovery-context-workspace", executor, 30)

    plan_profile = executor.calls[0]["context"]["profile"]
    assert plan_profile["completed_queries"] == ["completed query"]
    assert plan_profile["completed_opened_sources"] == [
        "https://example.com/already-opened"
    ]


def test_daily_and_per_run_limits_combine_and_recovery_skips_queries(
    tmp_path: Path,
) -> None:
    cfg = settings(
        tmp_path,
        research_max_searches=2,
        research_daily_budget_searches=3,
    )
    repository = ResearchRuntimeRepository(cfg)
    seed_job = enqueue(cfg, "daily-seed")
    seed_run = repository.ensure_run(
        seed_job,
        "MNQ_MARKET_RESEARCH",
        "mnq_market_research_v1",
    )
    seed_step, _ = repository.begin_step(
        seed_run["run_id"],
        "SEARCH",
        2,
        {},
        backend="fake",
        tool="fake",
    )
    repository.record_tool_events(
        seed_run["run_id"],
        seed_step["step_id"],
        [{"event_type": "search", "query": "already-used"}],
    )

    runtime = AgenticResearchRuntime(
        cfg,
        repository=repository,
        verifier=NoopVerifier(),
    )
    job = enqueue(cfg, "daily-combined")
    executor = PhaseEventExecutor(step="SEARCH", resource="searches", count=2)
    result = runtime.run(job, tmp_path / "daily-combined", executor, 30)
    restored = repository.get_run(result["run_id"])
    assert restored["request"]["effective_budget"]["max_searches"] == 2
    assert restored["request"]["effective_budget"]["daily_searches_remaining"] == 2

    recovery_cfg = settings(
        tmp_path / "recovery",
        research_max_searches=2,
    )
    recovery_runtime = AgenticResearchRuntime(
        recovery_cfg,
        verifier=NoopVerifier(),
    )
    recovery_job = enqueue(recovery_cfg, "recovery")
    violating = PhaseEventExecutor(
        step="SEARCH",
        resource="searches",
        count=3,
    )
    with pytest.raises(ResearchBudgetExceeded):
        recovery_runtime.run(
            recovery_job,
            tmp_path / "recovery-first",
            violating,
            30,
        )
    resumed = PhaseEventExecutor(step="NEVER", resource="searches", count=0)
    recovery_runtime.run(
        recovery_job,
        tmp_path / "recovery-second",
        resumed,
        30,
    )
    assert "SEARCH" not in [call["step_name"] for call in resumed.calls]
    search_call_contexts = [
        call["context"]["profile"]["completed_queries"]
        for call in violating.calls
        if call["step_name"] == "SEARCH"
    ]
    assert search_call_contexts == [[]]


class LiveCapability:
    @staticmethod
    def probe(*, persist: bool = True) -> dict[str, Any]:
        del persist
        return {"status": "LIVE_VERIFIED"}


def test_budget_violation_is_nonretryable_and_queue_metrics_use_real_events(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, research_max_searches=1)
    job = enqueue(cfg, "worker-budget")
    executor = PhaseEventExecutor(step="SEARCH", resource="searches", count=2)
    worker = AIResearchWorker(
        cfg,
        executor=executor,
        capabilities=LiveCapability(),
        worker_id="budget-worker",
    )

    assert worker.process_once() is True
    restored = AIResearchJobRepository(cfg).get(job["job_id"])
    run = ResearchRuntimeRepository(cfg).latest("MNQ")
    metrics = AIResearchJobRepository(cfg).status()["metrics"]

    assert restored["status"] == "FAILED"
    assert restored["attempts"] == 1
    assert restored["last_diagnostic"]["category"] == "BUDGET_EXCEEDED"
    assert restored["attempt_history"][0]["retry_classification"] == "NON_RETRYABLE"
    assert run["status"] == "FAILED"
    assert run["search_count"] == 2
    assert metrics["searches_per_run"] == 2.0
