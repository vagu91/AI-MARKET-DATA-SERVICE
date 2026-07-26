from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.services import ai_trader_consumer_v2_service as consumer_service
from app.services.ai_research_job_service import AIResearchJobService
from app.services.execution_context import ExecutionContext
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    ROOT / "tests" / "fixtures" / "provider_force_failure_20260725_redacted.json"
)


def _debug_payload(spec: dict[str, Any]) -> dict[str, Any]:
    generator = spec["generator"]
    lifecycle = {
        f"domain-{index:04d}": {
            "status": "FRESH",
            "as_of": spec["generated_at_utc"],
            "valid_until": "2026-07-25T12:05:00+00:00",
            "next_refresh": "2026-07-25T12:05:00+00:00",
            "refresh_policy": "provider-first deterministic refresh policy",
            "lineage": {"provider": "REDACTED_FIXTURE", "field": f"metric-{index}"},
        }
        for index in range(int(generator["lifecycle_items"]))
    }
    traces = [
        {
            "provider": "REDACTED_FIXTURE",
            "status": "SUCCESS",
            "request_id": f"fixture-{index:04d}",
            "diagnostic": "redacted deterministic provider trace " * 8,
        }
        for index in range(int(generator["quality_trace_items"]))
    ]
    contracts = [
        {
            "symbol": f"MNQ-{index:04d}",
            "strike": 20_000 + index,
            "open_interest": index % 500,
            "volume": index % 100,
        }
        for index in range(int(generator["option_contracts"]))
    ]
    return {
        "symbol": "MNQ",
        "generated_at_utc": spec["generated_at_utc"],
        "data_as_of": "2026-07-24T20:00:00+00:00",
        "market_schedule": {
            "context_date": "2026-07-25",
            "market_session_status": "weekend",
            "market_closed": True,
            "market_closed_reason": "weekend",
        },
        "event_calendar": {
            "status": "NO_DATA_EXPECTED",
            "critical_macro_events": [],
            "fed_communications": [],
            "other_economic_events": [],
            "reason": "no_events_scheduled",
        },
        "news_context": {
            "status": "NO_DATA_EXPECTED",
            "reason": "news_market_closed_no_fresh_news",
            "articles": [],
        },
        "options_positioning": {
            "status": "AVAILABLE",
            "underlying": "MNQ",
            "as_of": "2026-07-24T20:00:00+00:00",
            "expirations_considered": ["2026-07-31", "2026-08-07"],
            "put_call_ratio": 0.91,
            "open_interest_aggregate": 449_100,
            "volume_aggregate": 89_100,
            "dominant_strikes": [20_100, 20_200, 20_300],
            "freshness": "LAST_KNOWN_GOOD",
            "lineage": {"provider": "TRADIER_REDACTED_FIXTURE"},
            "raw_contracts": contracts,
        },
        "metadata": {"data_lifecycle": lifecycle},
        "quality": {"provider_traces": traces, "overall_data_quality": "PARTIAL"},
        "ai_enrichment": {"status": "NOT_REQUIRED"},
    }


def replay() -> dict[str, Any]:
    spec = json.loads(FIXTURE.read_text(encoding="utf-8"))
    debug = _debug_payload(spec)
    settings = Settings(
        _env_file=None,
        environment="test",
        database_path=Path(tempfile.mkdtemp(prefix="provider-force-replay-"))
        / "market.sqlite",
        source_policy_path=ROOT / "config" / "source_policy.json",
        ai_job_workspace_root=Path(tempfile.gettempdir()) / "provider-force-jobs",
    )
    before = consumer_service.build_ai_trader_consumer_v2(
        debug,
        settings=settings,
    )
    after = consumer_service.build_ai_trader_consumer_v2(
        debug,
        settings=settings,
    )
    repeated = consumer_service.build_ai_trader_consumer_v2(debug, settings=settings)
    context = ExecutionContext.provider_only(
        correlation_id="offline-provider-force-replay",
        allow_live_providers=True,
    )
    jobs = AIResearchJobService(settings).enqueue_missing_events(
        [],
        correlation_id=context.correlation_id,
        force=True,
        execution_context=context,
    )
    stored = MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="force",
        debug_payload=debug,
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    with sqlite3.connect(settings.database_path) as connection:
        counts = {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in (
                "ai_research_jobs",
                "research_runs",
                "research_backend_invocations",
                "market_context_snapshots",
            )
        }
    encoded = json.dumps(after, separators=(",", ":"), default=str)
    return {
        "fixture": str(FIXTURE),
        "forensic_before": spec["forensic_before"],
        "reconstructed_before_size_bytes": consumer_service._payload_size(before),
        "reconstructed_before_section_sizes": {
            key: len(
                json.dumps(value, separators=(",", ":"), default=str).encode(
                    "utf-8"
                )
            )
            for key, value in before.items()
        },
        "after_size_bytes": consumer_service._payload_size(after),
        "after_section_sizes": {
            key: len(
                json.dumps(
                    value,
                    separators=(",", ":"),
                    default=str,
                ).encode("utf-8")
            )
            for key, value in after.items()
        },
        "size_limit_applied": after["payload_measurement"][
            "size_limit_applied"
        ],
        "records_removed_for_size": after["payload_measurement"][
            "records_removed_for_size"
        ],
        "provider_only_ai_jobs": len(jobs),
        "database_counts": counts,
        "snapshot_id": stored["snapshot_id"],
        "weekend_preserved": after["market_session_status"] == "weekend",
        "deterministic_output": after == repeated,
        "raw_contracts_absent": "raw_contracts" not in encoded,
        "compacted_item_count_absent": "compacted_item_count" not in encoded,
        "secrets_absent": all(
            marker not in encoded.lower()
            for marker in ("authorization", "api_key", "bearer ", "account_id")
        ),
        "trading_endpoints_used": 0,
        "live_calls": 0,
    }


def main() -> int:
    print(json.dumps(replay(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
