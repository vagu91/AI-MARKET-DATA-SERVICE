from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database


class ProviderCacheRepositoryProtocol(Protocol):
    def get(self, cache_key: str) -> dict[str, Any] | list[dict[str, Any]] | None: ...
    def get_entry(self, cache_key: str) -> dict[str, Any] | None: ...
    def set(self, cache_key: str, payload: Any, **metadata: Any) -> None: ...
    def delete(self, cache_key: str) -> None: ...
    def purge_expired(self) -> int: ...
    def get_valid(self, cache_key: str) -> dict[str, Any] | None: ...
    def get_last_known_good(self, prefix: str) -> dict[str, Any] | None: ...


class ProviderCacheRepository:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        migrate_database(database_path)

    def set(self, cache_key: str, payload: Any, **metadata: Any) -> None:
        now = datetime.now(UTC).isoformat()
        encoded = json.dumps(payload, default=str)
        checksum = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        with connect_sqlite(self.database_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_cache_entries(
                  cache_key, provider_name, payload_json, created_at, updated_at,
                  valid_until, stale_until, status, checksum, last_error, source_url, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                  provider_name=excluded.provider_name,
                  payload_json=excluded.payload_json,
                  updated_at=excluded.updated_at,
                  valid_until=excluded.valid_until,
                  stale_until=excluded.stale_until,
                  status=excluded.status,
                  checksum=excluded.checksum,
                  last_error=excluded.last_error,
                  source_url=excluded.source_url,
                  metadata_json=excluded.metadata_json
                """,
                (
                    cache_key,
                    metadata.get("provider_name"),
                    encoded,
                    now,
                    now,
                    metadata.get("valid_until"),
                    metadata.get("stale_until"),
                    metadata.get("status") or "valid_cache",
                    checksum,
                    metadata.get("last_error"),
                    metadata.get("source_url"),
                    json.dumps(metadata, default=str, sort_keys=True) if metadata else None,
                ),
            )
            conn.commit()

    def get(self, cache_key: str) -> dict[str, Any] | list[dict[str, Any]] | None:
        entry = self.get_entry(cache_key)
        return entry["payload"] if entry else None

    def get_entry(self, cache_key: str) -> dict[str, Any] | None:
        with connect_sqlite(self.database_path) as conn:
            row = conn.execute(
                """
                SELECT cache_key, payload_json, created_at, updated_at, valid_until,
                       stale_until, status, checksum, last_error, source_url, metadata_json
                FROM provider_cache_entries WHERE cache_key = ?
                """,
                (cache_key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "cache_key": row["cache_key"],
            "payload": json.loads(row["payload_json"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "valid_until": row["valid_until"],
            "stale_until": row["stale_until"],
            "status": row["status"],
            "checksum": row["checksum"],
            "last_error": row["last_error"],
            "source_url": row["source_url"],
            "metadata": json.loads(row["metadata_json"]) if row["metadata_json"] else {},
        }

    def delete(self, cache_key: str) -> None:
        with connect_sqlite(self.database_path) as conn:
            conn.execute("DELETE FROM provider_cache_entries WHERE cache_key = ?", (cache_key,))
            conn.commit()

    def get_valid(self, cache_key: str) -> dict[str, Any] | None:
        entry = self.get_entry(cache_key)
        if not entry:
            return None
        if entry["status"] not in {"valid_cache", "last_known_good"}:
            return None
        valid_until = entry.get("valid_until")
        if valid_until and valid_until <= datetime.now(UTC).isoformat():
            return None
        return entry

    def purge_expired(self) -> int:
        now = datetime.now(UTC).isoformat()
        with connect_sqlite(self.database_path) as conn:
            cursor = conn.execute(
                "DELETE FROM provider_cache_entries WHERE valid_until IS NOT NULL AND valid_until <= ?",
                (now,),
            )
            conn.commit()
            return int(cursor.rowcount or 0)

    def get_last_known_good(self, prefix: str) -> dict[str, Any] | None:
        with connect_sqlite(self.database_path) as conn:
            row = conn.execute(
                """
                SELECT cache_key FROM provider_cache_entries
                WHERE cache_key LIKE ? AND status IN ('valid_cache', 'last_known_good')
                ORDER BY updated_at DESC LIMIT 1
                """,
                (f"{prefix}%",),
            ).fetchone()
        return self.get_entry(row["cache_key"]) if row else None

    def stats(self) -> dict[str, Any]:
        with connect_sqlite(self.database_path) as conn:
            total = conn.execute("SELECT COUNT(*) c FROM provider_cache_entries").fetchone()["c"]
            by_status = conn.execute(
                "SELECT status, COUNT(*) c FROM provider_cache_entries GROUP BY status"
            ).fetchall()
        statuses = {row["status"]: row["c"] for row in by_status}
        return {
            "total": total,
            "by_status": statuses,
            "valid_count": statuses.get("valid_cache", 0),
            "stale_count": statuses.get("stale_cache", 0),
            "last_known_good_count": statuses.get("last_known_good", 0),
            "negative_cache_count": statuses.get("negative_cache", 0),
        }
