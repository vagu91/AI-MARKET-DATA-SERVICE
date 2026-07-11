from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.infrastructure.persistence.schema import MIGRATIONS


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def connect_sqlite(path: Path, *, timeout_seconds: float = 30.0) -> sqlite3.Connection:
    ensure_parent_dir(path)
    conn = sqlite3.connect(path, timeout=timeout_seconds)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.DatabaseError:
        pass
    return conn


@dataclass(frozen=True)
class Database:
    path: Path

    def connect(self, *, timeout_seconds: float = 30.0) -> sqlite3.Connection:
        return connect_sqlite(self.path, timeout_seconds=timeout_seconds)

    def migrate(self) -> dict[str, Any]:
        from app.infrastructure.persistence.migrations import migrate_database

        return migrate_database(self.path)

    def health(self) -> dict[str, Any]:
        return database_health(self.path)


def database_health(path: Path) -> dict[str, Any]:
    exists = path.exists()
    with connect_sqlite(path) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        busy_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        user_version = conn.execute("PRAGMA user_version").fetchone()[0]
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        table_counts: dict[str, int] = {}
        for row in tables:
            name = row["name"]
            table_counts[name] = int(conn.execute(f"SELECT COUNT(*) c FROM {name}").fetchone()["c"])
        migrations = []
        if "schema_migrations" in table_counts:
            migrations = [
                dict(row)
                for row in conn.execute(
                    "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
                ).fetchall()
            ]
        pending_migrations = max(0, len(MIGRATIONS) - len(migrations))
    return {
        "path": str(path),
        "exists": exists,
        "file_size": path.stat().st_size if path.exists() else 0,
        "integrity_check": integrity,
        "journal_mode": journal_mode,
        "foreign_keys": foreign_keys,
        "busy_timeout": busy_timeout,
        "user_version": user_version,
        "tables": table_counts,
        "schema_migrations": migrations,
        "pending_migrations": pending_migrations,
        "wal_file_exists": path.with_suffix(path.suffix + "-wal").exists(),
    }
