from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from base64 import b64decode
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.ai_research_job_service import AIResearchJobService
from app.services.parallel_research_coordinator import ParallelResearchCoordinator
from app.services.research_backend import (
    OpenAIResponsesResearchBackend,
    ResearchBackendResult,
    normalize_backend_payload,
)
from app.services.research_profiles import PROFILES
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.research_source_gateway import (
    _extract_content,
    deterministic_official_numeric_value,
    match_official_structured_evidence,
    pdf_parser_available,
)


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "source_policy.json"
FIXTURE_PATH = ROOT / "tests" / "fixtures" / "parallel_research_final_20260724_redacted.json"
REFERENCE_NOW = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)


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
        rows = []
        for item in items:
            url = str(item["canonical_url"])
            text = str(item["evidence_text"])
            domain = (urlsplit(url).hostname or "").removeprefix("www.")
            checksum = hashlib.sha256(text.encode()).hexdigest()
            rows.append(
                {
                    "query": "offline bounded replay",
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
                    "source_id": None,
                    "verification_id": f"verify-{domain}",
                    "verification_method": "offline_replay",
                    "verification_reason": "bounded_fixture",
                    "verification_score": 1.0,
                }
            )
        return rows, []


def make_macro_run(
    cfg: Settings,
    identity: str,
) -> tuple[OfflineEvidenceRepository, dict[str, Any], dict[str, Any]]:
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MACRO_EVENTS_RESEARCH",
        symbol="MNQ",
        correlation_id=identity,
        request_payload={"gap": {"topic": "macro_events"}},
        force=True,
    )
    assert created
    repository = OfflineEvidenceRepository(cfg, now=lambda: REFERENCE_NOW)
    profile = PROFILES["MACRO_EVENTS_RESEARCH"]
    run = repository.ensure_run(job, profile.profile_id, profile.prompt_version)
    return repository, job, run


def event_claim() -> dict[str, Any]:
    return {
        "claim_ref": "candidate-1",
        "topic": "macro_events",
        "field_semantics": "official_calendar_event",
        "value": "Official release",
        "metric_id": "bea_gdp_advance",
        "event_key": "BEA_GDP_ADVANCE_2026-Q2",
        "event_at": "2026-07-30T12:30:00+00:00",
        "release_at": "2026-07-30T12:30:00+00:00",
        "period": "Q2 2026",
        "frequency": "quarterly",
        "confidence": 0.99,
        "topic_status": "SUPPORTED",
        "evidence": [
            {
                "canonical_url": "https://www.bea.gov/news/schedule",
                "publisher": "U.S. Bureau of Economic Analysis",
                "evidence_text": "July 30 8:30 AM GDP Advance Estimate 2nd Quarter 2026",
            }
        ],
    }


def test_real_queued_parent_contract_accepts_null_job_id() -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    helper = ROOT / "scripts" / "market_research_smoke_helpers.ps1"
    command = (
        f". '{helper}'; "
        f"$q=(Get-Content -Raw '{FIXTURE_PATH}'|ConvertFrom-Json).queued; "
        "Resolve-SmokeQueueContract -Queued $q -BaseUrl 'http://127.0.0.1:8000' "
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
    contract = json.loads(completed.stdout)
    assert contract["is_parent"] is True
    assert contract["job_id"] is None
    assert contract["parent_run_id"] == "prun-redacted"
    assert len(contract["child_job_ids"]) == 6
    assert contract["poll_url"].endswith("/market-research/mnq/runs/prun-redacted")


def test_parent_smoke_harness_replays_real_queue_shape_offline(tmp_path: Path) -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    script = ROOT / "scripts" / "smoke_test_market_research.ps1"
    output = tmp_path / "parent-smoke"
    wrapper = tmp_path / "parent-smoke-wrapper.ps1"
    wrapper.write_text(
        f"""
$global:queuedFixture = (Get-Content -Raw '{FIXTURE_PATH}' | ConvertFrom-Json).queued
$global:childrenFixture = @()
for ($index = 0; $index -lt $global:queuedFixture.child_job_ids.Count; $index++) {{
    $status = if ($index -eq 2) {{ 'FAILED' }} else {{ 'NO_DATA' }}
    $global:childrenFixture += [pscustomobject]@{{
        topic = @(
            'cot_positioning','geopolitical_regulatory_risk','macro_events',
            'mega_cap_semiconductors','nasdaq_100','vix_risk'
        )[$index]
        child_job_id = $global:queuedFixture.child_job_ids[$index]
        child_run_id = "rrun-child-$index"
        status = $status
    }}
}}
function global:Invoke-RestMethod {{
    param([string]$Method,[string]$Uri,[string]$ContentType,[string]$Body)
    if ($Uri -match '/ai-research/capabilities$') {{
        return [pscustomobject]@{{ status = 'READY_TO_SMOKE' }}
    }}
    if ($Method -eq 'Post') {{ return $global:queuedFixture }}
    if ($Uri -match '/market-research/mnq/runs/prun-redacted$') {{
        return [pscustomobject]@{{
            status = 'PARTIAL'
            expected_child_count = 6
            terminal_child_count = 6
            children = $global:childrenFixture
            telemetry = [pscustomobject]@{{
                warnings = @()
                backend_invocations = 6
                searches = 16
            }}
            checkpoint = [pscustomobject]@{{ terminal_count = 6 }}
        }}
    }}
    if ($Uri -match '/ai-research/status$') {{
        return [pscustomobject]@{{
            metrics = [pscustomobject]@{{ queue_depth = 0; running_jobs = 0 }}
        }}
    }}
    if ($Uri -match '/ai-research/jobs/(airj-[^/]+)$') {{
        return [pscustomobject]@{{
            job_id = $Matches[1]
            status = 'NO_DATA'
            attempt_history = @()
        }}
    }}
    if ($Uri -match '/market-research/mnq/runs/rrun-child-') {{
        return [pscustomobject]@{{ status = 'NO_DATA'; steps = @() }}
    }}
    if ($Uri -match '/market-research/mnq/latest$') {{
        return [pscustomobject]@{{ status = 'PARTIAL' }}
    }}
    if ($Uri -match '/market-context/mnq') {{
        return [pscustomobject]@{{
            snapshot_id = 'mcs-offline'
            snapshot_revision = 1
        }}
    }}
    throw "unexpected offline URI: $Uri"
}}
& '{script}' -OutputDirectory '{output}' -TimeoutSeconds 2 | Out-Null
""",
        encoding="utf-8",
    )
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
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8-sig"))
    assert summary["parent_run_id"] == "prun-redacted"
    assert summary["job_id"] is None
    assert summary["expected_child_count"] == summary["terminal_child_count"] == 6
    assert len(summary["child_statuses"]) == 6
    assert summary["failed_children"][0]["topic"] == "macro_events"
    assert summary["threshold_exceeded"] == []
    assert not (output / "failure-report.json").exists()


def test_projection_preserves_event_identity_and_lineage_atomically(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = make_macro_run(cfg, "projection")
    step, execute = repository.begin_step(
        run["run_id"],
        "PERSIST",
        7,
        {"claim_count": 1},
        backend="service",
        tool="sqlite",
    )
    assert execute

    result = repository.persist_claims(run, [event_claim()], step_id=step["step_id"])

    assert result["accepted_count"] == result["persisted_count"] == result["read_back_count"] == 1
    projected = result["results"][0]
    assert projected["claim_id"]
    assert projected["claim_ref"] == "candidate-1"
    assert projected["event_key"] == "BEA_GDP_ADVANCE_2026-Q2"
    assert projected["metric_id"] == "bea_gdp_advance"
    assert projected["topic"] == "macro_events"
    assert projected["lineage"]["research_run_id"] == run["run_id"]
    assert projected["source_references"][0]["canonical_url"].startswith("https://www.bea.gov/")
    restored = repository.get_run(run["run_id"])
    assert restored is not None
    assert restored["result"]["results"][0]["event_key"] == projected["event_key"]


def test_exact_event_projection_reconciliation_repairs_and_is_idempotent(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository, job, run = make_macro_run(cfg, "reconcile-exact")
    persist_step, execute = repository.begin_step(
        run["run_id"],
        "PERSIST",
        7,
        {"claim_count": 1},
        backend="service",
        tool="sqlite",
    )
    assert execute
    persisted = repository.persist_claims(
        run,
        [event_claim()],
        step_id=persist_step["step_id"],
    )
    read_step, execute = repository.begin_step(
        run["run_id"],
        "READ_BACK",
        8,
        {"persisted_count": 1},
        backend="service",
        tool="sqlite",
    )
    assert execute
    repository.complete_step(
        read_step["step_id"],
        {"persisted_count": 1, "read_back_count": 1, "verified": True},
    )
    diagnostic = {
        "category": "WORKER_ERROR",
        "exception_type": "ValueError",
        "message": "accepted event research result requires event_key",
        "step": "WORKER",
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
            "UPDATE research_runs SET status='FAILED',updated_at=? WHERE run_id=?",
            (REFERENCE_NOW.isoformat(), run["run_id"]),
        )
        conn.commit()

    assert persisted["accepted_count"] == 1
    assert repository.reconcile_terminal_jobs() == 1
    assert repository.reconcile_terminal_jobs() == 0
    restored = repository.get_run(run["run_id"])
    assert restored is not None and restored["status"] == "SUCCEEDED"
    assert restored["result"]["reconciliation"]["action"] == "REBUILT_EVENT_PROJECTION"
    assert restored["result"]["results"][0]["event_key"] == event_claim()["event_key"]
    with connect_sqlite(cfg.database_path) as conn:
        claim = conn.execute(
            "SELECT materialization_status FROM research_claims WHERE research_run_id=?",
            (run["run_id"],),
        ).fetchone()
        fact = conn.execute(
            "SELECT status FROM market_facts WHERE fact_key LIKE 'research:%'"
        ).fetchone()
    assert claim["materialization_status"] == "MATERIALIZED"
    assert fact["status"] == "active"


def test_parent_finalization_is_stable_and_aggregates_forensic_oracle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay = fixture()
    cfg = settings(tmp_path)
    migrate_database(cfg.database_path)
    manifest = {
        "manifest_id": "rgm-parent-oracle",
        "symbol": "MNQ",
        "source_snapshot_id": None,
        "generated_at": replay["parent"]["created_at"],
        "policy_version": "source-policy-v3",
        "agent_topics": replay["parent"]["required_topics"],
        "items": [
            {
                "topic": topic,
                "required_action": "AGENT_RESEARCH",
                "missing_fields": [topic],
            }
            for topic in replay["parent"]["required_topics"]
        ],
    }
    encoded_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    with connect_sqlite(cfg.database_path) as conn:
        conn.execute(
            """
            INSERT INTO research_gap_manifests(
              manifest_id,parent_run_id,symbol,source_snapshot_id,generated_at,
              policy_version,checksum,manifest_json,created_at
            ) VALUES (?,NULL,'MNQ',NULL,?,?,?,?,?)
            """,
            (
                manifest["manifest_id"],
                manifest["generated_at"],
                manifest["policy_version"],
                hashlib.sha256(encoded_manifest.encode()).hexdigest(),
                encoded_manifest,
                manifest["generated_at"],
            ),
        )
        conn.commit()
    coordinator = ParallelResearchCoordinator(cfg)
    created = coordinator.create_parent(manifest, correlation_id="parent-oracle", force=True)
    monkeypatch.setattr(
        "app.services.parallel_research_coordinator._now",
        lambda: replay["parent"]["first_completed_at"],
    )
    with connect_sqlite(cfg.database_path) as conn:
        conn.execute(
            """
            UPDATE research_parent_runs
            SET created_at=?,started_at=?,updated_at=?
            WHERE parent_run_id=?
            """,
            (
                replay["parent"]["created_at"],
                replay["parent"]["created_at"],
                replay["parent"]["created_at"],
                created["parent_run_id"],
            ),
        )
        conn.commit()
    parent = coordinator.get_parent(created["parent_run_id"])
    links = {item["topic"]: item for item in parent["children"]}
    with connect_sqlite(cfg.database_path) as conn:
        for child in replay["children"]:
            link = links[child["topic"]]
            status = child["status"]
            result = {
                "status": status,
                "accepted_count": child["metrics"]["claims_accepted"],
                "persisted_count": child["metrics"]["claims_accepted"],
                "read_back_count": child["metrics"]["claims_accepted"],
            }
            stamp = "2026-07-23T20:44:52+00:00"
            conn.execute(
                """
                UPDATE research_runs
                SET status=?,result_json=?,metrics_json=?,usage_json=?,
                    started_at=?,completed_at=?,updated_at=?
                WHERE run_id=?
                """,
                (
                    status,
                    json.dumps(result),
                    json.dumps(child["metrics"]),
                    json.dumps(child["metrics"]["usage"]),
                    replay["parent"]["created_at"],
                    stamp,
                    stamp,
                    link["child_run_id"],
                ),
            )
            conn.execute(
                """
                UPDATE ai_research_jobs
                SET status=?,result_payload_json=?,last_error=?,
                    completed_at=?,updated_at=?
                WHERE job_id=?
                """,
                (
                    status,
                    json.dumps(result),
                    child.get("error"),
                    stamp,
                    stamp,
                    link["child_job_id"],
                ),
            )
            conn.execute(
                """
                INSERT INTO research_backend_invocations(
                  invocation_id,run_id,backend,purpose,model,input_tokens,
                  output_tokens,cached_tokens,reasoning_tokens,total_tokens,
                  cost_json,duration_ms,output_checksum,output_json,created_at
                ) VALUES (?,?,?,'AGENTIC_RESEARCH',NULL,?,?,?,?,?,NULL,1,?,'{}',?)
                """,
                (
                    f"rinvoke-{child['topic']}",
                    link["child_run_id"],
                    "codex_cli",
                    child["metrics"]["usage"]["input_tokens"],
                    child["metrics"]["usage"]["output_tokens"],
                    child["metrics"]["usage"]["cached_tokens"],
                    child["metrics"]["usage"]["reasoning_tokens"],
                    child["metrics"]["usage"]["total_tokens"],
                    hashlib.sha256(child["topic"].encode()).hexdigest(),
                    stamp,
                ),
            )
        conn.commit()

    finalized = coordinator.reconcile_parent(created["parent_run_id"])
    assert finalized["status"] == finalized["research_status"] == "PARTIAL"
    assert finalized["snapshot_status"] == "NOT_MATERIALIZED"
    assert finalized["terminal_child_count"] == finalized["expected_child_count"] == 6
    assert finalized["required_topics"] == replay["parent"]["required_topics"]
    assert finalized["failed_topics"] == ["macro_events"]
    assert finalized["blocking_gaps"] == ["failed_topic:macro_events"]
    oracle = replay["telemetry_oracle"]
    for key in (
        "backend_invocations",
        "searches",
        "opened_sources",
        "fetched_sources",
        "verified_sources",
        "candidate_claims",
        "accepted_claims",
        "rejected_claims",
    ):
        assert finalized["telemetry"][key] == oracle[key]
    assert finalized["telemetry"]["usage"] == oracle["usage"]
    assert finalized["telemetry"]["wall_clock_seconds"] == 207.0
    assert finalized["telemetry"]["cost"] is None
    assert finalized["telemetry"]["cost_status"] == "cost_unavailable"
    timestamps = (
        finalized["completed_at"],
        finalized["updated_at"],
        [item["updated_at"] for item in finalized["children"]],
    )
    read_again = ParallelResearchCoordinator(cfg, read_only=True).get_parent(
        created["parent_run_id"]
    )
    reconciled_again = coordinator.reconcile_parent(created["parent_run_id"])
    assert (
        read_again["completed_at"],
        read_again["updated_at"],
        [item["updated_at"] for item in read_again["children"]],
    ) == timestamps
    assert (
        reconciled_again["completed_at"],
        reconciled_again["updated_at"],
        [item["updated_at"] for item in reconciled_again["children"]],
    ) == timestamps


def test_cftc_and_cboe_structured_official_verification_is_deterministic() -> None:
    official = fixture()["official_evidence"]
    cftc = official["cftc"]
    metadata = match_official_structured_evidence(
        cftc["metadata_anchor"],
        cftc["document"],
        source_domain=cftc["domain"],
    )
    numeric_claim = {
        "metric_id": "cot_noncommercial_net_position_futures_only",
        "event_key": "CFTC-209747-2026-07-14-FUTURES-ONLY",
        "value": cftc["derived_net"],
    }
    numeric = match_official_structured_evidence(
        cftc["numeric_anchor"],
        cftc["document"],
        source_domain=cftc["domain"],
        claim=numeric_claim,
    )
    assert metadata.accepted and metadata.method == "official_table_numeric"
    assert numeric.accepted and numeric.method == "official_table_numeric"
    assert (
        deterministic_official_numeric_value(
            numeric_claim,
            cftc["numeric_anchor"],
            source_domain=cftc["domain"],
        )
        == cftc["derived_net"]
    )
    cboe = official["cboe"]
    cboe_match = match_official_structured_evidence(
        cboe["anchor"],
        cboe["document"],
        source_domain=cboe["domain"],
    )
    assert cboe_match.accepted and cboe_match.method == "official_table_numeric"


@pytest.mark.parametrize(
    (
        "profile_id",
        "topic",
        "domain",
        "url",
        "metric_id",
        "value",
        "method",
        "deterministic_value",
        "expected_status",
    ),
    [
        (
            "COT_POSITIONING_RESEARCH",
            "cot_positioning",
            "cftc.gov",
            "https://www.cftc.gov/dea/futures/deacmesf.htm",
            "cot_report_date",
            "2026-07-14",
            "exact_normalized",
            None,
            "SUCCEEDED",
        ),
        (
            "COT_POSITIONING_RESEARCH",
            "cot_positioning",
            "cftc.gov",
            "https://www.cftc.gov/dea/futures/deacmesf.htm",
            "cot_noncommercial_net_position_futures_only",
            "-130338",
            "official_table_numeric",
            "-130338",
            "SUCCEEDED",
        ),
        (
            "COT_POSITIONING_RESEARCH",
            "cot_positioning",
            "cftc.gov",
            "https://www.cftc.gov/dea/futures/deacmesf.htm",
            "cot_noncommercial_net_position_futures_only",
            "-1",
            "official_table_numeric",
            "-130338",
            "NO_DATA",
        ),
        (
            "VIX_RISK_RESEARCH",
            "vix_risk",
            "cboe.com",
            "https://www.cboe.com/tradable-products/vix/vix-futures/",
            "vix",
            "19.36",
            "official_table_numeric",
            "19.36",
            "SUCCEEDED",
        ),
    ],
)
def test_official_observation_policy_is_narrow_and_numeric_aware(
    tmp_path: Path,
    profile_id: str,
    topic: str,
    domain: str,
    url: str,
    metric_id: str,
    value: str,
    method: str,
    deterministic_value: str | None,
    expected_status: str,
) -> None:
    cfg = settings(tmp_path)
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type=profile_id,
        symbol="MNQ",
        correlation_id=f"official-{metric_id}-{value}",
        request_payload={"gap": {"topic": topic}},
        force=True,
    )
    assert created
    repository = ResearchRuntimeRepository(cfg, now=lambda: REFERENCE_NOW)
    profile = PROFILES[profile_id]
    run = repository.ensure_run(job, profile.profile_id, profile.prompt_version)
    checksum = hashlib.sha256(f"{domain}-official-content".encode()).hexdigest()
    source = repository.persist_research_source(
        run["run_id"],
        {
            "source_id": f"source-{domain}-{metric_id}",
            "requested_url": url,
            "final_url": url,
            "canonical_url": url,
            "source_domain": domain,
            "source_tier": 1,
            "publisher": "Official publisher",
            "fetch_status": "FETCHED",
            "verification_status": "UNVERIFIED",
            "http_status": 200,
            "content_type": "text/html",
            "retrieved_at": REFERENCE_NOW.isoformat(),
            "content_sha256": checksum,
            "content_bytes": 100,
            "content_text": "bounded official fixture content",
        },
    )
    verification = {
        "verification_id": f"verify-{metric_id}",
        "accepted": True,
        "reason": (
            "verified_exact_normalized_match"
            if method == "exact_normalized"
            else "verified_official_table_numeric"
        ),
        "match_method": method,
        "match_score": 1.0,
        "source_id": source["source_id"],
        "content_sha256": checksum,
        "canonical_url": url,
        "retrieved_at": REFERENCE_NOW.isoformat(),
        "deterministic_value": deterministic_value,
    }
    claim = {
        "claim_ref": "official-1",
        "topic": topic,
        "field_semantics": "current_market_context",
        "value": value,
        "metric_id": metric_id,
        "event_key": f"OFFICIAL-{metric_id}-2026-07-14",
        "event_at": "2026-07-14",
        "period": "2026-07-14",
        "frequency": "weekly",
        "unit": "contracts" if metric_id.startswith("cot_") else "index points",
        "confidence": 0.99,
        "topic_status": "SUPPORTED",
        "evidence": [
            {
                "canonical_url": url,
                "source_url": url,
                "publisher": "Official publisher",
                "evidence_text": "official bounded evidence with enough tokens",
                "published_at": None,
                "retrieved_at": REFERENCE_NOW.isoformat(),
                "_service_verification": verification,
            }
        ],
    }

    result = repository.persist_claims(run, [claim])

    assert result["status"] == expected_status
    assert result["accepted_count"] == (1 if expected_status == "SUCCEEDED" else 0)
    warnings = {
        warning
        for rejected in result["rejected_claims"]
        for warning in rejected["warnings"]
    }
    if expected_status == "SUCCEEDED":
        assert "published_at_required" not in warnings
    else:
        assert "official_numeric_value_mismatch" in warnings


def test_real_pdf_parser_extracts_offline_nasdaq_fixture() -> None:
    encoded = (
        ROOT
        / "tests"
        / "fixtures"
        / "nasdaq_official_methodology_202607_redacted.pdf.b64"
    ).read_text(encoding="ascii")
    text, canonical, error = _extract_content(
        b64decode(encoded),
        "application/pdf",
        None,
    )
    assert pdf_parser_available() is True
    assert canonical is None and error is None
    assert "Nasdaq official index methodology July 2026" in text


def test_cli_and_api_fake_backends_share_contract_without_fallback(tmp_path: Path) -> None:
    raw = {
        "status": "NO_DATA",
        "claims": [],
        "searches": [],
        "warnings": ["bounded_no_data"],
    }
    expected = normalize_backend_payload(raw)
    cfg = settings(tmp_path, research_backend="openai_api")
    api = OpenAIResponsesResearchBackend(
        cfg,
        request_sender=lambda request: {
            "id": "resp-fake",
            "model": request["model"],
            "output_json": raw,
            "usage": {
                "input_tokens": 7,
                "output_tokens": 2,
                "cached_tokens": 1,
                "total_tokens": 9,
                "total_cost_usd": 0.01,
            },
        },
    )
    result = api.execute_research(
        job={"job_id": "job", "symbol": "MNQ", "request_payload": {}},
        run={"run_id": "run"},
        profile={"profile_id": "NEWS_RESEARCH"},
        workspace=tmp_path,
        watchdog_seconds=10,
        effective_budget={"max_searches": 1},
    )
    cli = ResearchBackendResult(
        invocation_id="cli-fake",
        backend="codex_cli",
        purpose="AGENTIC_RESEARCH",
        payload=normalize_backend_payload(raw),
        usage={"input_tokens": 99, "output_tokens": 3, "total_tokens": 102},
    )
    assert normalize_backend_payload(result.payload) == cli.payload == expected
    assert expected["contract"]["logical_phases"][-2:] == ["PERSIST", "READ_BACK"]
    assert expected["contract"]["source_verification"] == "server_owned"
    assert result.backend == "openai_api" and cli.backend == "codex_cli"
    assert result.usage != cli.usage
