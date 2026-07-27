from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.market_context_sync_service import canonical_json
from scripts.replay_snapshot92_live_blockers_offline import replay_artifacts


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = (
    ROOT / "tests" / "fixtures" / "schema22_live_rollover_redacted.json"
)


def replay() -> tuple[dict[str, Any], dict[str, Any]]:
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    first_summary, first_full = replay_artifacts()
    second_summary, second_full = replay_artifacts()
    first_bytes = canonical_json(first_full).encode("utf-8")
    second_bytes = canonical_json(second_full).encode("utf-8")
    if first_bytes != second_bytes:
        raise AssertionError("schema22_offline_replay_not_byte_identical")

    calendar_after = first_summary["calendar"]["after"]
    coverage = calendar_after["source_coverage"]
    actuals = {
        item["occurrence_id"]: item for item in first_summary["actuals"]
    }
    equation = first_full["sections"]["event_calendar"]["window"][
        "coverage"
    ]
    summary = {
        "mode": "OFFLINE_SCHEMA22_LIVE_ROLLOVER_REPLAY",
        "evidence_fixture": evidence["fixture"],
        "before": {
            "snapshot_96": evidence["snapshot_96"],
            "calendar_counts": {
                "PREVIOUS_WEEK": 0,
                "CURRENT_WEEK": 25,
                "NEXT_WEEK": 7,
            },
            "actual_missing": 2,
            "news_delivered": 0,
            "market_schedule": "QUARANTINED",
        },
        "after": {
            "calendar_counts": calendar_after["bucket_counts"],
            "actual_missing_ids": calendar_after["actual_missing_ids"],
            "actuals": actuals,
            "news_admitted": first_summary["news"]["admitted_count"],
            "news_quarantined": first_summary["news"]["quarantined_count"],
            "temporal_distinct_reuters_delivered": first_summary["news"][
                "temporal_distinct_reuters_delivered"
            ],
            "market_schedule": first_full["sections"]["market_schedule"][
                "sync"
            ]["status"],
            "ledger_dates": len(coverage["requested_dates"]),
            "ledger_next_week_status": coverage["by_bucket"][
                "NEXT_WEEK"
            ]["status"],
            "candidate_accounting": {
                "source_candidate_count": equation[
                    "source_candidate_count"
                ],
                "delivered_occurrence_count": equation[
                    "delivered_occurrence_count"
                ],
                "quarantined_occurrence_count": equation[
                    "invalid_temporal_count"
                ],
                "exact_duplicate_count": equation[
                    "exact_duplicate_count"
                ],
                "unexplained_loss": equation["unexplained_loss"],
            },
        },
        "full_sync": {
            "section_count": len(first_full["sections"]),
            "payload_size_bytes": len(first_bytes),
            "sha256": hashlib.sha256(first_bytes).hexdigest().upper(),
            "checksum": first_full["checksum"],
            "two_independent_replays_byte_identical": True,
        },
        "fixed_point": {
            "two_consecutive_full_reads_byte_identical": first_summary[
                "full_sync"
            ]["two_consecutive_replays_byte_identical"],
        },
        "side_effects": first_summary["side_effects"],
        "second_replay_side_effects": second_summary["side_effects"],
    }
    return summary, first_full


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary-output", type=Path)
    parser.add_argument("--full-sync-output", type=Path)
    args = parser.parse_args()
    summary, full_sync = replay()
    summary_bytes = canonical_json(summary).encode("utf-8")
    full_bytes = canonical_json(full_sync).encode("utf-8")
    if args.summary_output:
        args.summary_output.write_bytes(summary_bytes)
    if args.full_sync_output:
        args.full_sync_output.write_bytes(full_bytes)
    if not args.summary_output and not args.full_sync_output:
        print(summary_bytes.decode("utf-8"))


if __name__ == "__main__":
    main()
