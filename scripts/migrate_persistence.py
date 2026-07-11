from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

from app.core.config import Settings
from app.infrastructure.persistence.database import database_health
from app.infrastructure.persistence.migrations import migrate_database


def _copy_if_needed(source: Path, target: Path, *, dry_run: bool) -> bool:
    if not source.exists() or source.resolve() == target.resolve() or target.exists():
        return False
    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return True


def _legacy_rows(source: Path) -> list[sqlite3.Row]:
    if not source.exists():
        return []
    with sqlite3.connect(source) as conn:
        conn.row_factory = sqlite3.Row
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "cache_entries" not in tables:
            return []
        return conn.execute("SELECT cache_key, payload, created_at, updated_at FROM cache_entries").fetchall()


def _import_legacy_cache(source: Path, target: Path, *, dry_run: bool) -> dict[str, object]:
    rows = _legacy_rows(source)
    invalid = 0
    duplicates = 0
    migrated = 0
    if dry_run:
        return {
            "records_read": len(rows),
            "records_migrated": 0,
            "duplicates": 0,
            "invalid": 0,
            "errors": [],
            "checksum_comparison": "planned",
        }
    import_errors: list[str] = []
    with sqlite3.connect(target) as conn:
        conn.row_factory = sqlite3.Row
        for row in rows:
            try:
                json.loads(row["payload"])
            except (TypeError, json.JSONDecodeError):
                invalid += 1
                import_errors.append(str(row["cache_key"]))
                continue
            exists = conn.execute(
                "SELECT 1 FROM provider_cache_entries WHERE cache_key = ?",
                (row["cache_key"],),
            ).fetchone()
            if exists:
                duplicates += 1
            checksum = hashlib.sha256(str(row["payload"]).encode("utf-8")).hexdigest()
            conn.execute(
                """
                INSERT INTO provider_cache_entries(cache_key, payload_json, created_at, updated_at, status, checksum)
                VALUES (?, ?, ?, ?, 'valid_cache', ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                  payload_json=excluded.payload_json,
                  updated_at=excluded.updated_at,
                  checksum=excluded.checksum
                """,
                (row["cache_key"], row["payload"], row["created_at"], row["updated_at"], checksum),
            )
            migrated += 1
        conn.commit()
    return {
        "records_read": len(rows),
        "records_migrated": migrated,
        "duplicates": duplicates,
        "invalid": invalid,
        "errors": import_errors,
        "checksum_comparison": "sha256_payload_json",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate AI-MARKET-DATA-SERVICE SQLite persistence.")
    parser.add_argument("--source", type=Path, help="Optional legacy SQLite source to copy before migrating.")
    parser.add_argument("--target", type=Path, help="Target operational SQLite database.")
    parser.add_argument("--dry-run", action="store_true", help="Plan migration without changing the target.")
    parser.add_argument("--apply", action="store_true", help="Apply migrations. Without this flag, performs a dry run.")
    parser.add_argument("--report", type=Path, help="Optional JSON report path.")
    args = parser.parse_args()

    settings = Settings()
    target = args.target or settings.canonical_store_db_path or settings.market_db_path
    dry_run = args.dry_run or not args.apply
    copied = _copy_if_needed(args.source, target, dry_run=dry_run) if args.source else False
    migration = None if dry_run else migrate_database(target)
    legacy_cache = (
        _import_legacy_cache(args.source, target, dry_run=dry_run)
        if args.source and args.source.exists()
        else {
            "records_read": 0,
            "records_migrated": 0,
            "duplicates": 0,
            "invalid": 0,
            "errors": [],
            "checksum_comparison": None,
        }
    )
    report = {
        "dry_run": dry_run,
        "source": str(args.source) if args.source else None,
        "target": str(target),
        "copied_source_to_target": copied,
        "migration": migration,
        "legacy_cache": legacy_cache,
        "health": database_health(target) if target.exists() or not dry_run else None,
    }
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
