from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.core.config import Settings
from app.infrastructure.persistence.database_maintenance import run_database_maintenance
from app.infrastructure.storage_retention import cleanup_storage


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean diagnostics, backups, logs, temp files, and DB retention safely.")
    parser.add_argument(
        "--category",
        choices=("all", "diagnostics", "backups", "logs", "temp", "database"),
        default="all",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Plan cleanup without deleting. This is the default.")
    mode.add_argument("--apply", action="store_true", help="Apply cleanup.")
    parser.add_argument("--report", type=Path, help="Optional JSON report path.")
    args = parser.parse_args()

    settings = Settings()
    dry_run = not args.apply
    report: dict[str, object]
    if args.category == "database":
        report = {
            "storage": None,
            "database": run_database_maintenance(settings, dry_run=dry_run),
        }
    else:
        report = {
            "storage": cleanup_storage(settings, category=args.category, dry_run=dry_run),
            "database": None if args.category != "all" else run_database_maintenance(settings, dry_run=dry_run),
        }
    text = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
