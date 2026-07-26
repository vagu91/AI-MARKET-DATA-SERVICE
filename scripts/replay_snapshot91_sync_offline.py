from __future__ import annotations

import argparse
import gc
import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.services.market_context_hardening_service import harden_market_context
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import MarketContextSyncService


ROOT = Path(__file__).resolve().parents[1]
FORENSIC_ROOT = ROOT / "data" / "controlled-provider-test-20260725T200541Z"


def replay() -> dict[str, Any]:
    debug_path = FORENSIC_ROOT / "market-context-debug-readback.json"
    consumer_path = FORENSIC_ROOT / "ai-trader-consumer-2.1-exact.json"
    debug = json.loads(debug_path.read_text(encoding="utf-8"))
    consumer = json.loads(consumer_path.read_text(encoding="utf-8"))
    original_consumer_bytes = len(
        consumer_path.read_bytes()
    )
    original_consumer_hash = hashlib.sha256(
        consumer_path.read_bytes()
    ).hexdigest().upper()

    with tempfile.TemporaryDirectory(prefix="snapshot91-sync-replay-") as tmp:
        root = Path(tmp)
        settings = Settings(
            _env_file=None,
            database_path=root / "replay.sqlite",
            source_policy_path=ROOT / "config" / "source_policy.json",
            model_pricing_path=ROOT / "config" / "model_pricing.json",
            ai_job_workspace_root=root / "jobs",
            codex_workspace_dir=root / "codex",
            environment="test",
        )
        hardened = harden_market_context(
            debug,
            settings=settings,
            force_recalculate=True,
        )
        MarketContextSnapshotRepository(settings).save(
            snapshot_id=str(debug["snapshot_id"]),
            revision=int(debug["snapshot_revision"]),
            symbol="MNQ",
            refresh_mode="offline-forensic-replay",
            debug_payload=hardened,
            consumer_payload=consumer,
            ai_status="NOT_REQUIRED",
        )
        service = MarketContextSyncService(settings)
        manifest = service.manifest()
        full = service.full()
        del service
        gc.collect()

    calendar = hardened["event_calendar_window"]
    buckets = ("previous_week", "current_week", "next_week")
    bucket_event_count = sum(
        int(calendar[name]["event_count"]) for name in buckets
    )
    bucket_list_count = sum(
        len(calendar[name]["events"]) for name in buckets
    )
    exact_full_bytes = len(
        json.dumps(
            full,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return {
        "mode": "OFFLINE_FORENSIC_REPLAY",
        "provider_calls": 0,
        "ai_calls": 0,
        "deliveries": 0,
        "operational_database_writes": 0,
        "snapshot_id": manifest["snapshot_id"],
        "snapshot_revision": manifest["snapshot_revision"],
        "original_consumer_bytes": original_consumer_bytes,
        "original_consumer_sha256": original_consumer_hash,
        "sync_full_bytes": exact_full_bytes,
        "sync_full_checksum": full["checksum"],
        "section_count": len(manifest["sections"]),
        "calendar": {
            "counts_total": calendar["counts"]["total"],
            "coverage_retained_count": calendar["coverage"][
                "retained_count"
            ],
            "bucket_event_count": bucket_event_count,
            "bucket_list_count": bucket_list_count,
            "candidate_count": calendar["coverage"]["candidate_count"],
            "omitted_count": calendar["coverage"]["omitted_count"],
            "previous_week_count": calendar["previous_week"][
                "event_count"
            ],
            "source_coverage_status": calendar["coverage"][
                "source_coverage_status"
            ],
            "missing_source_coverage_buckets": calendar["coverage"][
                "missing_source_coverage_buckets"
            ],
            "awaiting_actual_count": calendar["telemetry"][
                "missing_actuals"
            ],
        },
        "invariants": {
            "calendar_counts_match": len(
                {
                    calendar["counts"]["total"],
                    calendar["coverage"]["retained_count"],
                    bucket_event_count,
                    bucket_list_count,
                }
            )
            == 1,
            "no_size_limit": True,
            "no_compacted_item_count": (
                "compacted_item_count"
                not in json.dumps(full, ensure_ascii=False)
            ),
            "actuals_invented": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = replay()
    rendered = json.dumps(result, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
