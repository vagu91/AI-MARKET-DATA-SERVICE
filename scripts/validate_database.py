from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.core.config import Settings
from app.infrastructure.persistence.database import database_health
from app.infrastructure.persistence.migrations import migrate_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate SQLite database health and schema version.")
    parser.add_argument("--database", type=Path, help="Database path. Defaults to canonical store.")
    args = parser.parse_args()

    settings = Settings()
    database = args.database or settings.canonical_store_db_path or settings.market_db_path
    migration = migrate_database(database)
    health = database_health(database)
    report = {
        "status": "ok" if health["integrity_check"] == "ok" else "fail",
        "migration": migration,
        "health": health,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
