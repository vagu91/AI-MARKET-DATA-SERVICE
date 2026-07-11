from __future__ import annotations

import sqlite3
import hashlib
from datetime import UTC, datetime
from pathlib import Path

from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.schema import MIGRATIONS


def migrate_database(path: Path) -> dict[str, object]:
    applied: list[str] = []
    with connect_sqlite(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              applied_at TEXT NOT NULL
            )
            """
        )
        existing = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for index, (name, sql) in enumerate(MIGRATIONS, start=1):
            if index in existing:
                continue
            try:
                conn.execute("BEGIN")
                for statement in _split_sql(sql):
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                    (index, name, datetime.now(UTC).replace(microsecond=0).isoformat()),
                )
                conn.execute(f"PRAGMA user_version={index}")
                conn.commit()
                applied.append(name)
            except sqlite3.DatabaseError:
                conn.rollback()
                raise
        _migrate_legacy_cache_entries(conn)
        conn.commit()
    return {"path": str(path), "applied": applied, "schema_version": len(MIGRATIONS)}


def _split_sql(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _migrate_legacy_cache_entries(conn: sqlite3.Connection) -> None:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "cache_entries" not in tables or "provider_cache_entries" not in tables:
        return
    rows = conn.execute("SELECT cache_key, payload, created_at, updated_at FROM cache_entries").fetchall()
    for row in rows:
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
