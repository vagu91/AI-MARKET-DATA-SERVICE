from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.ai_research_job_service import AIResearchJobService
from app.services.parallel_research_coordinator import _aggregate_parent_telemetry
from app.services.research_backend import ResearchBackendResult
from app.services.research_metrics_service import ResearchMetricsService
from app.services.research_profiles import PROFILES
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.research_semantics import (
    canonical_observation_key,
    normalize_research_claim,
    requires_event_value_projection,
)
from app.services.research_tool_telemetry import (
    ProgressLoopGuard,
    ResearchEmergencyCeilingExceeded,
    loop_guard_exception,
)
from app.services.source_policy_service import SourcePolicyService


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "source_policy.json"
FIXTURE_PATH = ROOT / "tests" / "fixtures" / "live_parallel_closure_20260724_redacted.json"
REFERENCE_NOW = datetime(2026, 7, 24, 8, 0, tzinfo=UTC)


def test_event_projection_routing_is_semantic_not_job_type() -> None:
    assert requires_event_value_projection(
        [{"field_semantics": "forecast", "value": "0.3"}]
    )
    assert not requires_event_value_projection(
        [{"field_semantics": "current_market_context", "value": "18.2"}]
    )


def fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "research_backend": "codex_cli",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


class OfflineEvidenceRepository(ResearchRuntimeRepository):
    def _validated_evidence(
        self,
        _semantics: str,
        items: list[dict[str, Any]],
        _observed_sources: list[dict[str, Any]],
        _acquired_sources: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        for item in items:
            url = str(item["canonical_url"])
            domain = (urlsplit(url).hostname or "").removeprefix("www.")
            text = str(item["evidence_text"])
            checksum = hashlib.sha256(text.encode()).hexdigest()
            rows.append(
                {
                    "query": "bounded offline replay",
                    "source_url": url,
                    "canonical_url": url,
                    "publisher": item["publisher"],
                    "source_domain": domain,
                    "source_tier": 1,
                    "evidence_text": text,
                    "published_at": None,
                    "retrieved_at": REFERENCE_NOW.isoformat(),
                    "redirect_url": None,
                    "source_status": "VERIFIED",
                    "independent_source_group": f"domain:{domain}",
                    "content_checksum": checksum,
                    "source_content_hash": checksum,
                    "tool_event_id": None,
                    "source_id": f"source-{domain}",
                    "verification_id": f"verify-{domain}",
                    "verification_method": "offline_replay",
                    "verification_reason": "bounded_fixture",
                    "verification_score": 1.0,
                }
            )
        return rows, []


def make_run(
    cfg: Settings,
    profile_id: str,
    topic: str,
    identity: str,
) -> tuple[OfflineEvidenceRepository, dict[str, Any], dict[str, Any]]:
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type=profile_id,
        symbol="MNQ",
        correlation_id=identity,
        request_payload={"gap": {"topic": topic}},
        force=True,
    )
    assert created
    repository = OfflineEvidenceRepository(cfg, now=lambda: REFERENCE_NOW)
    profile = PROFILES[profile_id]
    run = repository.ensure_run(job, profile.profile_id, profile.prompt_version)
    return repository, job, run


def observation_claim(kind: str) -> dict[str, Any]:
    raw = dict(fixture()[kind]["claim"])
    domain = "cftc.gov" if kind == "cot" else "cboe.com"
    raw.update(
        {
            "topic_status": "SUPPORTED",
            "confidence": 0.9,
            "symbol": "MNQ",
            "evidence": [
                {
                    "canonical_url": f"https://www.{domain}/official",
                    "publisher": "Official publisher",
                    "evidence_text": f"verified {raw['metric_id']} {raw['value']}",
                    "retrieved_at": REFERENCE_NOW.isoformat(),
                }
            ],
        }
    )
    return raw


@pytest.mark.parametrize(
    ("kind", "profile_id", "topic"),
    [
        ("cot", "COT_POSITIONING_RESEARCH", "cot_positioning"),
        ("vix", "VIX_RISK_RESEARCH", "vix_risk"),
    ],
)
def test_official_observations_use_service_owned_identity_without_event_classification(
    tmp_path: Path,
    kind: str,
    profile_id: str,
    topic: str,
) -> None:
    cfg = settings(tmp_path / kind)
    repository, _job, run = make_run(cfg, profile_id, topic, f"{kind}-observation")
    result = repository.persist_claims(run, [observation_claim(kind)])

    assert result["status"] == "SUCCEEDED"
    assert result["accepted_count"] == result["persisted_count"] == result["read_back_count"] == 1
    projected = result["results"][0]
    assert projected["field_semantics"] == "current_market_context"
    assert projected["observation_key"].startswith("observation:")
    assert projected["fact_key"].startswith("research:")
    if kind == "cot":
        assert projected["event_key"] == fixture()["cot"]["claim"]["event_key"]
    else:
        assert projected["event_key"] is None


def test_macro_event_requires_and_preserves_event_key(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = make_run(
        cfg,
        "MACRO_EVENTS_RESEARCH",
        "macro_events",
        "event-identity",
    )
    claim = observation_claim("cot")
    claim.update(
        {
            "topic": "macro_events",
            "field_semantics": "official_calendar_event",
            "event_key": None,
            "event_at": "2026-07-30T12:30:00+00:00",
        }
    )
    rejected = repository.persist_claims(run, [claim])
    assert rejected["status"] == "NO_DATA"
    assert "event_key_required" in rejected["rejected_claims"][0]["warnings"]

    second_cfg = settings(tmp_path / "present")
    second_repository, _, second_run = make_run(
        second_cfg,
        "MACRO_EVENTS_RESEARCH",
        "macro_events",
        "event-identity-present",
    )
    claim["event_key"] = "BEA_GDP_ADVANCE_2026_Q2"
    accepted = second_repository.persist_claims(second_run, [claim])
    assert accepted["results"][0]["event_key"] == claim["event_key"]


def test_observation_key_is_deterministic_and_not_topic_special_cased() -> None:
    policy = SourcePolicyService(POLICY)
    claim = observation_claim("vix")
    first = normalize_research_claim(claim, policy=policy, now=REFERENCE_NOW)
    second = normalize_research_claim(claim, policy=policy, now=REFERENCE_NOW)
    assert first["observation_key"] == second["observation_key"]
    assert first["observation_key"] == canonical_observation_key(first)


@pytest.mark.parametrize(
    ("kind", "profile_id", "topic"),
    [
        ("cot", "COT_POSITIONING_RESEARCH", "cot_positioning"),
        ("vix", "VIX_RISK_RESEARCH", "vix_risk"),
    ],
)
def test_failed_observation_projection_reconciliation_recovers_orphans_idempotently(
    tmp_path: Path,
    kind: str,
    profile_id: str,
    topic: str,
) -> None:
    cfg = settings(tmp_path / kind)
    repository, job, run = make_run(
        cfg,
        profile_id,
        topic,
        f"reconcile-{kind}-observation",
    )
    persist_step, execute = repository.begin_step(
        run["run_id"], "PERSIST", 7, {"claim_count": 1}, backend="service", tool="sqlite"
    )
    assert execute
    persisted = repository.persist_claims(
        run,
        [observation_claim(kind)],
        step_id=persist_step["step_id"],
    )
    read_step, execute = repository.begin_step(
        run["run_id"], "READ_BACK", 8, {"persisted_count": 1}, backend="service", tool="sqlite"
    )
    assert execute
    repository.complete_step(
        read_step["step_id"],
        {"persisted_count": 1, "read_back_count": 1, "verified": True},
    )
    diagnostic = {
        "category": "WORKER_ERROR",
        "message": fixture()[kind]["error"],
        "stack_fingerprint": fixture()[kind]["stack_fingerprint"],
    }
    with connect_sqlite(cfg.database_path) as conn:
        conn.execute(
            """
            UPDATE ai_research_jobs
            SET status='FAILED',last_error='worker:unknown:ValueError',
                last_diagnostic_json=?,completed_at=?,updated_at=?
            WHERE job_id=?
            """,
            (
                json.dumps(diagnostic),
                REFERENCE_NOW.isoformat(),
                REFERENCE_NOW.isoformat(),
                job["job_id"],
            ),
        )
        conn.execute(
            "UPDATE research_runs SET status='FAILED' WHERE run_id=?",
            (run["run_id"],),
        )
        conn.execute(
            "UPDATE research_claims SET materialization_status='ORPHANED' WHERE research_run_id=?",
            (run["run_id"],),
        )
        conn.execute(
            """
            UPDATE research_evidence SET audit_status='ORPHANED'
            WHERE claim_id IN (
              SELECT claim_id FROM research_claims WHERE research_run_id=?
            )
            """,
            (run["run_id"],),
        )
        conn.execute(
            """
            UPDATE market_facts SET status='orphaned'
            WHERE fact_key IN (
              SELECT 'research:' || claim_id FROM research_claims WHERE research_run_id=?
            )
            """,
            (run["run_id"],),
        )
        conn.commit()

    assert persisted["accepted_count"] == 1
    assert repository.reconcile_terminal_jobs() == 1
    assert repository.reconcile_terminal_jobs() == 0
    restored = repository.get_run(run["run_id"])
    assert restored is not None and restored["status"] == "SUCCEEDED"
    assert set(restored["completed_topics"]).isdisjoint(restored["missing_topics"])
    projected = restored["result"]["results"][0]
    assert projected["observation_key"].startswith("observation:")
    assert projected["event_key"] == fixture()[kind]["claim"].get("event_key")
    assert restored["result"]["reconciliation"]["transaction_outcome"] == "COMMITTED"
    with connect_sqlite(cfg.database_path) as conn:
        audit = conn.execute(
            """
            SELECT action,transaction_outcome FROM research_reconciliation_audit
            WHERE run_id=?
            """,
            (run["run_id"],),
        ).fetchone()
        claim_status = conn.execute(
            "SELECT materialization_status FROM research_claims WHERE research_run_id=?",
            (run["run_id"],),
        ).fetchone()[0]
        fact_status = conn.execute(
            "SELECT status FROM market_facts WHERE fact_key LIKE 'research:%'"
        ).fetchone()[0]
    assert dict(audit) == {
        "action": "REBUILT_OBSERVATION_PROJECTION",
        "transaction_outcome": "COMMITTED",
    }
    assert claim_status == "MATERIALIZED"
    assert fact_status == "active"


def search_event(identity: str, query: str | None = None) -> dict[str, Any]:
    return {
        "lifecycle": "completed",
        "counts_usage": True,
        "phase": "SEARCH",
        "semantic_action": "search",
        "query": query,
        "canonical_url": None,
        "source_url": None,
        "item_id": identity,
        "tool_action_fingerprint": identity,
        "status": "completed",
    }


def test_five_unique_forensic_searches_are_not_a_loop(tmp_path: Path) -> None:
    cfg = settings(tmp_path, research_loop_no_progress_action_threshold=3)
    guard = ProgressLoopGuard(cfg)
    reasons = [
        guard.observe(search_event(item["tool_action_fingerprint"], item["query"]))[1]
        for item in fixture()["mega_cap"]["searches"]
    ]
    assert reasons == [None] * 5


def test_repeated_fingerprint_and_abab_cycle_are_loops(tmp_path: Path) -> None:
    repeated = ProgressLoopGuard(
        settings(tmp_path / "repeat", research_loop_repeat_action_threshold=3)
    )
    reasons = [repeated.observe(search_event("same"))[1] for _ in range(3)]
    assert reasons[-1] == "repeated_action_without_progress"

    cyclic = ProgressLoopGuard(
        settings(
            tmp_path / "cycle",
            research_loop_repeat_action_threshold=3,
            research_loop_cycle_window=2,
            research_loop_cycle_repetitions=2,
        )
    )
    cycle_reasons = [
        cyclic.observe(search_event(identity))[1]
        for identity in ("A", "B", "A", "B")
    ]
    assert cycle_reasons[-1] == "cyclic_action_sequence"


def test_emergency_ceiling_has_distinct_category(tmp_path: Path) -> None:
    cfg = settings(tmp_path, research_emergency_max_tool_actions=20)
    guard = ProgressLoopGuard(cfg)
    reason = None
    for index in range(21):
        _, reason = guard.observe(search_event(f"unique-{index}"))
    error = loop_guard_exception(
        step="SEARCH",
        run_id="rrun-bounded",
        job_id="airj-bounded",
        reason=str(reason),
        evidence=guard.evidence(),
    )
    assert isinstance(error, ResearchEmergencyCeilingExceeded)
    assert error.diagnostic["category"] == "EMERGENCY_CEILING"


def test_backend_invocation_lifecycle_preserves_aborted_usage_state(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = make_run(
        cfg,
        "MEGA_CAP_SEMICONDUCTORS_RESEARCH",
        "mega_cap_semiconductors",
        "invocation-lifecycle",
    )
    aborted_id = repository.begin_backend_invocation(
        run["run_id"], backend="codex_cli"
    )
    repository.abort_backend_invocation(aborted_id, reason="loop guard interrupted")
    completed_id = repository.begin_backend_invocation(
        run["run_id"], backend="codex_cli"
    )
    repository.record_backend_invocation(
        run["run_id"],
        ResearchBackendResult(
            invocation_id="provider-redacted",
            backend="codex_cli",
            purpose="AGENTIC_RESEARCH",
            payload={"status": "NO_DATA"},
            usage={"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
        ),
        attempt_id=completed_id,
    )
    metrics = ResearchMetricsService(cfg).snapshot(run["run_id"], persist=False)
    assert metrics["backend"] == {
        "used": ["codex_cli"],
        "invocations": 1,
        "attempted": 2,
        "completed": 1,
        "aborted": 1,
        "usage_status": "partially_unavailable",
        "usage_unavailable_invocations": 1,
    }
    with connect_sqlite(cfg.database_path) as conn:
        aborted = conn.execute(
            """
            SELECT lifecycle_status,usage_status FROM research_backend_invocations
            WHERE invocation_id=?
            """,
            (aborted_id,),
        ).fetchone()
    assert dict(aborted) == {
        "lifecycle_status": "ABORTED",
        "usage_status": "UNAVAILABLE",
    }


def test_parent_telemetry_exposes_attempted_completed_and_aborted_counts(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = make_run(
        cfg,
        "MEGA_CAP_SEMICONDUCTORS_RESEARCH",
        "mega_cap_semiconductors",
        "parent-invocation-counts",
    )
    for index in range(5):
        attempt_id = repository.begin_backend_invocation(
            run["run_id"], backend="codex_cli"
        )
        repository.record_backend_invocation(
            run["run_id"],
            ResearchBackendResult(
                invocation_id=f"provider-{index}",
                backend="codex_cli",
                    purpose="AGENTIC_RESEARCH",
                    payload={"status": "NO_DATA"},
                    usage={"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
                ),
            attempt_id=attempt_id,
        )
    aborted_id = repository.begin_backend_invocation(
        run["run_id"], backend="codex_cli"
    )
    repository.abort_backend_invocation(aborted_id, reason="bounded loop interruption")
    child = {
        "topic": "mega_cap_semiconductors",
        "child_job_id": "airj-redacted",
        "child_run_id": run["run_id"],
        "metrics_json": "{}",
        "warnings_json": "[]",
        "cost_json": "{}",
        "last_error": "research_loop_detected",
    }
    with connect_sqlite(cfg.database_path) as conn:
        telemetry = _aggregate_parent_telemetry(
            conn,
            {"started_at": "2026-07-24T06:08:45+00:00"},
            [child],
            ["LOOP_DETECTED"],
            completed_at="2026-07-24T06:12:19+00:00",
        )
    oracle = fixture()["invocation_oracle"]
    assert telemetry["backend_invocations"] == oracle["completed"]
    assert telemetry["backend_invocations_attempted"] == oracle["attempted"]
    assert telemetry["backend_invocations_completed"] == oracle["completed"]
    assert telemetry["backend_invocations_aborted"] == oracle["aborted"]
    assert telemetry["backend_usage_status"] == "partially_unavailable"
    assert telemetry["backend_usage_unavailable_invocations"] == 1
    assert telemetry["wall_clock_seconds"] == fixture()["parent"]["wall_clock_seconds"]


def test_smoke_outcome_matrix_distinguishes_internal_failure_and_policy_no_data() -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    helper = ROOT / "scripts" / "market_research_smoke_helpers.ps1"
    command = (
        f". '{helper}'; "
        "$statuses=@('FAILED','LOOP_DETECTED','TIMED_OUT','SUCCEEDED','PARTIAL','NO_DATA'); "
        "$out=@(); foreach($s in $statuses){"
        "$d=Get-SmokeOutcomeClassification -ParentStatus 'PARTIAL' "
        "-Children @([pscustomobject]@{status=$s}); "
        "$out += [pscustomobject]@{status=$s;exit_code=$d.exit_code;category=$d.category}"
        "}; $warning=[pscustomobject]@{warnings=@('evidence_mismatch');threshold_warnings=@()}; "
        "[pscustomobject]@{decisions=$out;thresholds=@(Get-SmokeThresholdExceeded $warning)} "
        "| ConvertTo-Json -Depth 6 -Compress"
    )
    completed = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=20,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    output = json.loads(completed.stdout)
    decisions = {item["status"]: item for item in output["decisions"]}
    for status in ("FAILED", "LOOP_DETECTED", "TIMED_OUT"):
        assert decisions[status]["exit_code"] == 1
        assert decisions[status]["category"] == "internal_failure"
    for status in ("SUCCEEDED", "PARTIAL", "NO_DATA"):
        assert decisions[status]["exit_code"] == 0
    assert decisions["NO_DATA"]["category"] == "policy_no_data"
    assert output["thresholds"] == []
