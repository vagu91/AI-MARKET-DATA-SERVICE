from __future__ import annotations

from typing import Any

from app.core.config import Settings
from app.services.market_fact_repository import connect_market_db, encode, init_market_db, now_iso


class EnrichmentRunRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        init_market_db(settings)

    def start(self, *, run_id: str, trigger: str) -> None:
        with connect_market_db(self.settings) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO enrichment_runs (run_id, started_at, status, trigger) VALUES (?, ?, ?, ?)",
                (run_id, now_iso(), "running", trigger),
            )
            conn.commit()

    def finish(self, *, run_id: str, status: str, metrics: dict[str, Any]) -> None:
        payload = dict(metrics)
        for key in ("warnings_json", "errors_json"):
            if key in payload:
                payload[key] = encode(payload[key])
        allowed = {
            "events_checked", "db_hits", "db_misses", "provider_hits", "provider_misses",
            "ai_research_requests", "facts_written", "news_written", "warnings_json", "errors_json",
        }
        payload = {key: value for key, value in payload.items() if key in allowed}
        assignments = ", ".join(f"{key}=?" for key in payload)
        with connect_market_db(self.settings) as conn:
            conn.execute(
                f"UPDATE enrichment_runs SET finished_at=?, status=?{', ' if assignments else ''}{assignments} WHERE run_id=?",
                [now_iso(), status, *payload.values(), run_id],
            )
            conn.commit()

    def latest(self) -> dict[str, Any] | None:
        with connect_market_db(self.settings) as conn:
            row = conn.execute("SELECT * FROM enrichment_runs ORDER BY started_at DESC LIMIT 1").fetchone()
        return dict(row) if row else None
