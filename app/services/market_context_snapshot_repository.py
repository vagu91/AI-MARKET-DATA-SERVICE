from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database


class MarketContextSnapshotRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        migrate_database(settings.database_path)

    def save_next(
        self,
        *,
        symbol: str,
        refresh_mode: str,
        debug_payload: dict[str, Any],
        ai_enrichment: dict[str, Any],
        source_job_id: str | None = None,
        job_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Allocate revision and persist both payloads in one SQLite write transaction."""
        now = datetime.now(UTC).replace(microsecond=0).isoformat()
        snapshot_id = f"mcs-{uuid.uuid4()}"
        symbol = symbol.upper()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            revision = int(conn.execute(
                "SELECT COALESCE(MAX(revision),0)+1 AS revision FROM market_context_snapshots WHERE symbol=?",
                (symbol,),
            ).fetchone()["revision"])
            debug = dict(debug_payload)
            debug.update({
                "snapshot_id": snapshot_id,
                "snapshot_revision": revision,
                "ai_enrichment": ai_enrichment,
            })
            from app.services.ai_trader_consumer_v2_service import build_ai_trader_consumer_v2
            consumer = build_ai_trader_consumer_v2(debug, settings=self.settings)
            generated_at = str(debug.get("generated_at_utc") or debug.get("generated_at") or now)
            data_as_of = str(consumer.get("data_as_of") or generated_at)
            debug_json = self._json(debug)
            consumer_json = self._json(consumer)
            checksum = hashlib.sha256((debug_json + consumer_json).encode("utf-8")).hexdigest()
            conn.execute(
                """
                INSERT INTO market_context_snapshots(
                  snapshot_id,symbol,revision,generated_at,data_as_of,refresh_mode,
                  debug_payload_json,consumer_payload_json,ai_status,source_job_id,checksum,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    snapshot_id, symbol, revision, generated_at, data_as_of, refresh_mode,
                    debug_json, consumer_json, str(ai_enrichment.get("status") or "NOT_REQUIRED"),
                    source_job_id, checksum, now,
                ),
            )
            for job_id in dict.fromkeys(job_ids or ([source_job_id] if source_job_id else [])):
                job = conn.execute("SELECT event_key FROM ai_research_jobs WHERE job_id=?", (job_id,)).fetchone()
                if job is None:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO market_context_snapshot_jobs(snapshot_id,job_id,event_key,created_at) VALUES (?,?,?,?)",
                    (snapshot_id, job_id, job["event_key"], now),
                )
                conn.execute("UPDATE ai_research_jobs SET snapshot_id=? WHERE job_id=?", (snapshot_id, job_id))
            conn.commit()
        restored = self.get(snapshot_id)
        if restored is None or restored["checksum"] != checksum:
            raise RuntimeError("market context snapshot read-back failed")
        return restored

    def save(
        self,
        *,
        snapshot_id: str,
        revision: int,
        symbol: str,
        refresh_mode: str,
        debug_payload: dict[str, Any],
        consumer_payload: dict[str, Any],
        ai_status: str,
        source_job_id: str | None = None,
    ) -> dict[str, Any]:
        """Compatibility helper for fixtures importing an already allocated immutable snapshot."""
        now = datetime.now(UTC).replace(microsecond=0).isoformat()
        generated_at = str(debug_payload.get("generated_at_utc") or debug_payload.get("generated_at") or now)
        data_as_of = str(consumer_payload.get("data_as_of") or generated_at)
        debug_json = self._json(debug_payload)
        consumer_json = self._json(consumer_payload)
        checksum = hashlib.sha256((debug_json + consumer_json).encode("utf-8")).hexdigest()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO market_context_snapshots(
                  snapshot_id,symbol,revision,generated_at,data_as_of,refresh_mode,
                  debug_payload_json,consumer_payload_json,ai_status,source_job_id,checksum,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (snapshot_id, symbol.upper(), revision, generated_at, data_as_of, refresh_mode,
                 debug_json, consumer_json, ai_status, source_job_id, checksum, now),
            )
            conn.commit()
        restored = self.get(snapshot_id)
        if restored is None or restored["checksum"] != checksum:
            raise RuntimeError("market context snapshot read-back failed")
        return restored

    def latest(self, symbol: str = "MNQ") -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT * FROM market_context_snapshots WHERE symbol=? ORDER BY revision DESC LIMIT 1",
                (symbol.upper(),),
            ).fetchone()
        return self._row(row) if row else None

    def get(self, snapshot_id: str) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute("SELECT * FROM market_context_snapshots WHERE snapshot_id=?", (snapshot_id,)).fetchone()
        return self._row(row) if row else None

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _row(row: Any) -> dict[str, Any]:
        data = dict(row)
        data["debug_payload"] = json.loads(data.pop("debug_payload_json"))
        data["consumer_payload"] = json.loads(data.pop("consumer_payload_json"))
        return data
