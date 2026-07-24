from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SELECT_CANDIDATES = """
SELECT item_id,entity_type,entity_key,work_status,
       lease_owner,lease_expires_at,heartbeat_at
FROM datum_lifecycle_items
WHERE work_status<>'LEASED'
  AND (
    lease_owner IS NOT NULL
    OR lease_expires_at IS NOT NULL
    OR heartbeat_at IS NOT NULL
  )
ORDER BY item_id
"""


def reconcile_terminal_leases(
    database: Path,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    database = database.resolve()
    if not database.is_file():
        raise FileNotFoundError(database)
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    try:
        if apply:
            connection.execute("BEGIN IMMEDIATE")
        before = [
            dict(row)
            for row in connection.execute(SELECT_CANDIDATES).fetchall()
        ]
        changed = 0
        if apply and before:
            cursor = connection.execute(
                """
                UPDATE datum_lifecycle_items
                SET lease_owner=NULL,
                    lease_expires_at=NULL,
                    heartbeat_at=NULL
                WHERE work_status<>'LEASED'
                  AND (
                    lease_owner IS NOT NULL
                    OR lease_expires_at IS NOT NULL
                    OR heartbeat_at IS NOT NULL
                  )
                """
            )
            changed = int(cursor.rowcount or 0)
            connection.commit()
        after = [
            dict(row)
            for row in connection.execute(SELECT_CANDIDATES).fetchall()
        ]
    except Exception:
        if apply:
            connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "database": str(database),
        "mode": "APPLY" if apply else "DRY_RUN",
        "executed_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "candidate_count": len(before),
        "changed_count": changed,
        "remaining_candidate_count": len(after),
        "candidates": before,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Clear stale lease columns only when lifecycle work_status is not LEASED."
        )
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the idempotent reconciliation; default is read-only dry-run.",
    )
    parser.add_argument(
        "--audit-output",
        type=Path,
        help="Required with --apply; receives the JSON audit record.",
    )
    args = parser.parse_args()
    if args.apply and args.audit_output is None:
        parser.error("--audit-output is required with --apply")
    result = reconcile_terminal_leases(args.database, apply=args.apply)
    encoded = json.dumps(
        result,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    if args.audit_output is not None:
        audit_output = args.audit_output.resolve()
        audit_output.parent.mkdir(parents=True, exist_ok=True)
        audit_output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
