from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


EXPECTED_SNAPSHOTS = {
    86: "mcs-e8969da5-93d1-47eb-8980-db2045cd27d9",
    87: "mcs-5d20bb45-83a9-4fe2-81a5-854bd7bd6eb1",
    88: "mcs-d8d62f83-1c2f-4640-9e94-e90a92ee44a7",
    89: "mcs-bec58c91-8405-46a5-9668-29fde9edc4c8",
    90: "mcs-faa3aff2-17e5-4578-b0c0-7066a1e0aecc",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def _open(database: Path, *, apply: bool) -> sqlite3.Connection:
    if not apply:
        connection = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)
        connection.execute("PRAGMA query_only=ON")
        return connection
    connection = sqlite3.connect(database, timeout=0)
    connection.execute("PRAGMA busy_timeout=0")
    connection.execute("BEGIN EXCLUSIVE")
    return connection


def _expected_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in EXPECTED_SNAPSHOTS)
    rows = connection.execute(
        f"""
        SELECT s.snapshot_id,s.revision,s.refresh_mode,s.ai_status,s.audit_status,
               s.source_job_id,j.job_type,j.status AS job_status,
               r.run_id,r.status AS run_status
        FROM market_context_snapshots s
        LEFT JOIN ai_research_jobs j ON j.job_id=s.source_job_id
        LEFT JOIN research_runs r ON r.job_id=j.job_id
        WHERE s.snapshot_id IN ({placeholders})
        ORDER BY s.revision
        """,
        tuple(EXPECTED_SNAPSHOTS.values()),
    ).fetchall()
    return [dict(row) for row in rows]


def _guard(rows: list[dict[str, Any]]) -> None:
    if len(rows) != len(EXPECTED_SNAPSHOTS):
        raise RuntimeError("expected_state_guard_snapshot_count_mismatch")
    for row in rows:
        revision = int(row["revision"])
        if EXPECTED_SNAPSHOTS.get(revision) != row["snapshot_id"]:
            raise RuntimeError("expected_state_guard_snapshot_identity_mismatch")
        expected = {
            "refresh_mode": "worker_db_only_materialization",
            "ai_status": "NO_DATA",
            "job_type": "MISSING_EVENT_RESEARCH",
            "job_status": "NO_DATA",
            "run_status": "NO_DATA",
        }
        for field, value in expected.items():
            if row.get(field) != value:
                raise RuntimeError(f"expected_state_guard_{field}_mismatch")
        if row.get("audit_status") not in {"ACTIVE", "ORPHANED"}:
            raise RuntimeError("expected_state_guard_audit_status_mismatch")
        if not row.get("source_job_id") or not row.get("run_id"):
            raise RuntimeError("expected_state_guard_related_job_run_missing")


def reconcile(
    database: Path,
    *,
    apply: bool = False,
    backup: Path | None = None,
) -> dict[str, Any]:
    database = database.resolve()
    if not database.is_file():
        raise FileNotFoundError(database)
    database_hash = _sha256(database)
    backup_hash = None
    if apply:
        sidecars = [
            Path(f"{database}{suffix}")
            for suffix in ("-wal", "-journal")
            if Path(f"{database}{suffix}").exists()
            and Path(f"{database}{suffix}").stat().st_size > 0
        ]
        if sidecars:
            raise RuntimeError("database_must_be_closed_before_apply")
        if backup is None:
            raise ValueError("backup_required_for_apply")
        backup = backup.resolve()
        if not backup.is_file():
            raise FileNotFoundError(backup)
        backup_hash = _sha256(backup)
        if backup_hash != database_hash:
            raise RuntimeError("backup_hash_does_not_match_closed_database")
    connection = _open(database, apply=apply)
    connection.row_factory = sqlite3.Row
    try:
        before = _expected_rows(connection)
        _guard(before)
        changed = 0
        if apply:
            active_ids = [
                str(row["snapshot_id"])
                for row in before
                if row["audit_status"] == "ACTIVE"
            ]
            if active_ids:
                placeholders = ",".join("?" for _ in active_ids)
                cursor = connection.execute(
                    f"""
                    UPDATE market_context_snapshots
                    SET audit_status='ORPHANED'
                    WHERE audit_status='ACTIVE'
                      AND snapshot_id IN ({placeholders})
                    """,
                    tuple(active_ids),
                )
                changed = int(cursor.rowcount or 0)
            connection.commit()
        after = _expected_rows(connection)
        _guard(after)
    except Exception:
        if apply:
            connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "database": str(database),
        "database_sha256": database_hash,
        "backup": str(backup) if backup else None,
        "backup_sha256": backup_hash,
        "mode": "APPLY" if apply else "DRY_RUN",
        "executed_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "expected_snapshot_count": len(EXPECTED_SNAPSHOTS),
        "matched_snapshot_count": len(before),
        "changed_snapshot_count": changed,
        "target_state": "ORPHANED",
        "destructive_changes": 0,
        "tokens_or_telemetry_modified": False,
        "records": after if apply else before,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Non-destructively orphan exactly the five unauthorized NO_DATA "
            "snapshot side effects. Default mode is read-only."
        )
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--backup",
        type=Path,
        help="Byte-identical closed-database backup; required with --apply.",
    )
    parser.add_argument("--audit-output", type=Path)
    args = parser.parse_args()
    if args.apply and (args.backup is None or args.audit_output is None):
        parser.error("--backup and --audit-output are required with --apply")
    result = reconcile(args.database, apply=args.apply, backup=args.backup)
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.audit_output is not None:
        output = args.audit_output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(f"{encoded}\n", encoding="utf-8")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
