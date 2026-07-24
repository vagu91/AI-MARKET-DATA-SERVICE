from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


NEW_DOMAINS = {
    "options_positioning",
    "market_internals",
    "cross_asset_context",
    "earnings_intelligence",
}


def replay(fixture_dir: Path) -> dict[str, Any]:
    """Replay immutable smoke artifacts without network, providers, or service calls."""
    summary = _load(fixture_dir / "summary.json")
    context = _load(fixture_dir / "market-context.json")
    metrics = summary["research_metrics"]
    children = list(metrics["child_statuses"])
    new_children = [item for item in children if item.get("topic") in NEW_DOMAINS]
    new_domain_tokens = sum(
        int(item.get("input_tokens") or 0) + int(item.get("output_tokens") or 0)
        for item in new_children
    )
    research = context.get("research") or {}
    earnings = context.get("earnings") or {}
    pending = [
        item
        for item in [
            *(earnings.get("upcoming") or []),
            *(earnings.get("released") or []),
            *(earnings.get("upcoming_mega_cap_earnings_14d") or []),
            *(earnings.get("released_earnings") or []),
        ]
        if item.get("temporal_status") == "AWAITING_ACTUAL"
        and str(item.get("symbol") or item.get("ticker")) in {"GOOGL", "TSLA"}
    ]
    child_runs = [
        _load(path)
        for path in sorted(fixture_dir.glob("child-run-*.json"))
    ]
    no_data_contracts = [
        run.get("result") or {}
        for run in child_runs
        if str(run.get("status")) == "NO_DATA"
    ]
    cot = next(
        (
            run
            for run in child_runs
            if str(run.get("profile_id")) == "COT_POSITIONING_RESEARCH"
        ),
        {},
    )
    cot_result = cot.get("result") or {}
    accepted_cot = list(cot_result.get("accepted_claims") or [])
    cot_metrics = {
        str(item.get("metric_id") or "")
        for item in accepted_cot
        if isinstance(item, dict)
    }
    amd = next(
        (
            claim
            for run in child_runs
            for claim in (run.get("result") or {}).get("accepted_claims") or []
            if isinstance(claim, dict)
            and str(claim.get("symbol") or "") == "AMD"
            and str(claim.get("field_semantics") or "") == "earnings_schedule"
        ),
        None,
    )
    findings = {
        "parent_id_verified": summary.get("parent_run_id")
        == "prun-68a91ef9-dcd0-4025-a848-6b396a96bd10",
        "snapshot_verified": (
            summary.get("snapshot_id")
            == "mcs-e75788ce-09fe-47b0-b70e-870bc1c624f3"
            and int(summary.get("snapshot_revision") or 0) == 82
        ),
        "backend_invocations": int(metrics.get("backend_invocations") or 0),
        "total_tokens": int((metrics.get("usage") or {}).get("total_tokens") or 0),
        "accepted_claims": int(metrics.get("accepted_claims") or 0),
        "new_domain_tokens": new_domain_tokens,
        "new_domain_accepted_claims": sum(
            int(item.get("accepted_claims") or 0) for item in new_children
        ),
        "coverage_score": float(research.get("coverage_score") or 0),
        "blocking_gap_count": len(research.get("blocking_gaps") or []),
        "legacy_research_complete": research.get("research_complete"),
        "no_data_retry_metadata_missing": any(
            not item.get("next_retry_at") for item in no_data_contracts
        ),
        "cot_metadata_only": cot_metrics.issubset(
            {"cot_report_date", "cot_contract"}
        ),
        "cot_was_marked_complete": "cot_positioning"
        in (cot.get("completed_topics") or []),
        "amd_official_confirmation_found": bool(amd),
        "amd_event_at": amd.get("event_at") if amd else None,
        "pending_actual_issuer_count": len(pending),
        "live_calls_executed": 0,
    }
    expected = {
        "backend_invocations": 10,
        "total_tokens": 1_084_879,
        "accepted_claims": 3,
        "new_domain_tokens": 374_408,
        "new_domain_accepted_claims": 0,
        "coverage_score": 0.2,
        "blocking_gap_count": 8,
        "pending_actual_issuer_count": 2,
        "live_calls_executed": 0,
    }
    mismatches = {
        key: {"expected": value, "actual": findings.get(key)}
        for key, value in expected.items()
        if findings.get(key) != value
    }
    return {
        "fixture_dir": str(fixture_dir),
        "offline": True,
        "findings": findings,
        "expected": expected,
        "mismatches": mismatches,
        "passed": not mismatches,
    }


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "fixture_dir",
        nargs="?",
        type=Path,
        default=Path("data/market-research-smoke-agentic-domains-20260724"),
    )
    args = parser.parse_args()
    result = replay(args.fixture_dir)
    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
