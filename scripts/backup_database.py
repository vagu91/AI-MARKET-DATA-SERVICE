from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

from app.core.config import Settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a timestamped SQLite database backup.")
    parser.add_argument("--database", type=Path, help="Database path. Defaults to canonical store.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/backups"))
    args = parser.parse_args()

    settings = Settings()
    database = args.database or settings.canonical_store_db_path or settings.market_db_path
    if not database.exists():
        raise SystemExit(f"Database not found: {database}")
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output = args.output_dir / f"{database.stem}_{timestamp}{database.suffix}"
    output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(database, output)
    print(json.dumps({"database": str(database), "backup": str(output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
