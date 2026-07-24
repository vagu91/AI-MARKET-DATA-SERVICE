from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
)
from app.services.lifecycle_due_resolver import (
    DeterministicLifecycleDueResolver,
)
from app.services.research_scheduler_service import ResearchSchedulerService


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = (
    ROOT
    / "tests"
    / "fixtures"
    / "provider_only_lifecycle_forensic_redacted.json"
)


def replay(
    *,
    fixture_path: Path = DEFAULT_FIXTURE,
    workspace: Path,
) -> dict[str, Any]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    database = workspace / "provider-only-replay.sqlite"
    artifact = workspace / "ai-trader-consumer-payload.json"
    artifact.write_text(
        json.dumps(
            fixture["artifact_payload"],
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    artifact_before = _sha256(artifact)
    seeded_at = _timestamp(fixture["seeded_at"])
    scan_at = _timestamp(fixture["scan_at"])
    settings = Settings(
        _env_file=None,
        database_path=database,
        source_policy_path=ROOT / "config" / "source_policy.json",
        model_pricing_path=ROOT / "config" / "model_pricing.json",
        ai_job_workspace_root=workspace / "jobs",
        codex_workspace_dir=workspace / "codex",
        environment="test",
        lifecycle_due_scanner_enabled=True,
        lifecycle_due_max_concurrency=2,
        lifecycle_no_data_retry_seconds="900,3600,21600,86400",
    )
    seed_repository = LifecycleRepository(
        settings,
        clock=lambda: seeded_at,
    )
    target_ids: list[str] = []
    for target in fixture["targets"]:
        payload = dict(target["payload"])
        lifecycle = compute_datum_lifecycle(
            target["entity_type"],
            target["entity_key"],
            payload,
            settings=settings,
            now=seeded_at,
            no_data=True,
            fields_attempted=list(target["fields_attempted"]),
            retry_class="NO_DATA",
            refresh_reason=str(payload["reason"]),
        )
        stored = seed_repository.upsert(
            lifecycle,
            payload=payload,
            work_status="BACKOFF",
        )
        target_ids.append(str(stored["item_id"]))
    unrelated_ids: list[str] = []
    for index, entity_key in enumerate(fixture["unrelated_entity_keys"]):
        payload = {
            "ticker": entity_key.split(":", 1)[0],
            "actual_eps": float(index + 1),
            "event_at": (scan_at + timedelta(days=index + 1)).isoformat(),
            "valid_until": (scan_at + timedelta(days=8)).isoformat(),
            "source": "Issuer IR",
            "source_url": "https://ir.amd.com/",
            "source_lineage": [
                {
                    "source": "Issuer IR",
                    "source_url": "https://ir.amd.com/",
                    "verification_status": "VERIFIED",
                }
            ],
        }
        lifecycle = compute_datum_lifecycle(
            "earnings_schedule",
            entity_key,
            payload,
            settings=settings,
            now=seeded_at,
            fields_attempted=["actual_eps"],
        )
        stored = seed_repository.upsert(
            lifecycle,
            payload=payload,
            work_status="IDLE",
        )
        unrelated_ids.append(str(stored["item_id"]))
    before = _lifecycle_rows(settings)
    counts_before = _table_counts(settings)
    scheduler = ResearchSchedulerService(settings, clock=lambda: scan_at)
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: scan_at,
        adapters={},
    )
    result = scheduler.scan_due_items(
        owner=str(fixture["batch_id"]),
        resolver=resolver.resolve,
        ai_enqueue=None,
    )
    after = _lifecycle_rows(settings)
    counts_after = _table_counts(settings)
    changed_ids = sorted(
        item_id
        for item_id in set(before) | set(after)
        if before.get(item_id) != after.get(item_id)
    )
    with connect_sqlite(settings.database_path) as conn:
        event_names = [
            str(row["event_name"])
            for row in conn.execute(
                """
                SELECT event_name FROM service_telemetry_events
                WHERE correlation_id=?
                ORDER BY occurred_at,telemetry_id
                """,
                (fixture["batch_id"],),
            ).fetchall()
        ]
        lease_violations = int(
            conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM datum_lifecycle_items
                WHERE work_status<>'LEASED'
                  AND (
                    lease_owner IS NOT NULL
                    OR lease_expires_at IS NOT NULL
                    OR heartbeat_at IS NOT NULL
                  )
                """
            ).fetchone()["count"]
        )
    artifact_after = _sha256(artifact)
    unchanged_unrelated = [
        item_id
        for item_id in unrelated_ids
        if before[item_id] == after[item_id]
    ]
    return {
        "fixture": fixture["schema_version"],
        "batch_id": fixture["batch_id"],
        "claimed_count": int(result["claimed"]),
        "target_item_ids": target_ids,
        "changed_item_ids": changed_ids,
        "changed_items_are_targets_only": set(changed_ids).issubset(
            set(target_ids)
        ),
        "unrelated_count": len(unrelated_ids),
        "unrelated_unchanged_count": len(unchanged_unrelated),
        "lease_violations": lease_violations,
        "snapshot_delta": (
            counts_after["market_context_snapshots"]
            - counts_before["market_context_snapshots"]
        ),
        "outbox_delta": (
            counts_after["market_context_outbox"]
            - counts_before["market_context_outbox"]
        ),
        "ai_job_delta": (
            counts_after["ai_research_jobs"]
            - counts_before["ai_research_jobs"]
        ),
        "artifact_sha256_before": artifact_before,
        "artifact_sha256_after": artifact_after,
        "artifact_unchanged": artifact_before == artifact_after,
        "telemetry_event_names": event_names,
        "provider_call_event_count": event_names.count("provider_call"),
        "resolver_evaluations": int(result["resolver_evaluations"]),
        "committed_payload_hits": int(result["committed_payload_hits"]),
        "actual_provider_requests": int(result["actual_provider_requests"]),
        "ai_invocations": int(result["ai_invocations"]),
        "ai_jobs_created": int(result["ai_jobs_created"]),
    }


def _lifecycle_rows(settings: Settings) -> dict[str, dict[str, Any]]:
    with connect_sqlite(settings.database_path) as conn:
        rows = conn.execute(
            "SELECT * FROM datum_lifecycle_items ORDER BY item_id"
        ).fetchall()
    return {str(row["item_id"]): dict(row) for row in rows}


def _table_counts(settings: Settings) -> dict[str, int]:
    tables = (
        "market_context_snapshots",
        "market_context_outbox",
        "ai_research_jobs",
    )
    with connect_sqlite(settings.database_path) as conn:
        return {
            table: int(
                conn.execute(
                    f"SELECT COUNT(*) AS count FROM {table}"
                ).fetchone()["count"]
            )
            for table in tables
        }


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--workspace", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            replay(fixture_path=args.fixture, workspace=args.workspace),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
