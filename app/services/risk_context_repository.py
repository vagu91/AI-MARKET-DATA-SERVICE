from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.core.redaction import redact_payload
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database


class RiskContextHistoryRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        migrate_database(settings.database_path)

    def append(self, payload: dict[str, Any]) -> dict[str, Any]:
        compact = redact_payload(payload)
        encoded = json.dumps(compact, default=str, sort_keys=True, separators=(",", ":"))
        checksum = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        retrieved_at = str(payload.get("retrieved_at") or _now_iso())
        snapshot_key = hashlib.sha256(f"{retrieved_at}:{checksum}".encode("utf-8")).hexdigest()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO risk_context_snapshots (
                  snapshot_key, data_as_of, retrieved_at, valid_until, status,
                  quality_score, payload_json, checksum, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_key,
                    payload.get("data_as_of"),
                    retrieved_at,
                    payload.get("valid_until"),
                    payload.get("status") or "not_found",
                    float((payload.get("quality") or {}).get("quality_score") or 0),
                    encoded,
                    checksum,
                    _now_iso(),
                ),
            )
            conn.commit()
        return self.latest() or payload

    def latest(self) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT payload_json FROM risk_context_snapshots ORDER BY retrieved_at DESC, id DESC LIMIT 1"
            ).fetchone()
        return _decode(row["payload_json"]) if row else None

    def history(self, *, limit: int = 400) -> list[dict[str, Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                "SELECT payload_json FROM risk_context_snapshots ORDER BY retrieved_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [payload for row in rows if (payload := _decode(row["payload_json"]))]

    def count(self) -> int:
        with connect_sqlite(self.settings.database_path) as conn:
            return int(conn.execute("SELECT COUNT(*) AS count FROM risk_context_snapshots").fetchone()["count"])


def _decode(value: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
