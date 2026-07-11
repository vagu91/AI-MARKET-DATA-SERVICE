from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database

CANONICAL_TABLES = (
    "market_facts",
    "economic_events_history",
    "market_news",
    "provider_observations",
    "enrichment_runs",
)
CACHE_TABLES = ("provider_cache_entries", "provider_state")


def _backup(path: Path) -> Path | None:
    if not path.exists():
        return None
    target = Path("data/backups") / f"{path.stem}_before_reset_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}{path.suffix}"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, target)
    return target


def _clear(path: Path, tables: tuple[str, ...], *, dry_run: bool) -> dict[str, int]:
    migrate_database(path)
    deleted: dict[str, int] = {}
    with connect_sqlite(path) as conn:
        for table in tables:
            count = int(conn.execute(f"SELECT COUNT(*) c FROM {table}").fetchone()["c"])
            deleted[table] = count
            if not dry_run:
                conn.execute(f"DELETE FROM {table}")
        if not dry_run:
            conn.commit()
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset canonical and/or provider cache tables.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--canonical-only", action="store_true")
    group.add_argument("--cache-only", action="store_true")
    group.add_argument("--full", action="store_true")
    parser.add_argument("--database", type=Path, help="Operational SQLite DB. Defaults to canonical store.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    settings = Settings()
    database = args.database or settings.canonical_store_db_path or settings.market_db_path
    tables = CACHE_TABLES if args.cache_only else CANONICAL_TABLES if args.canonical_only else CANONICAL_TABLES + CACHE_TABLES
    backup = None if args.dry_run or args.no_backup else _backup(database)
    report = {
        "database": str(database),
        "dry_run": args.dry_run,
        "backup": str(backup) if backup else None,
        "deleted_rows": _clear(database, tables, dry_run=args.dry_run),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
