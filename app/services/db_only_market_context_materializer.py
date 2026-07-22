from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.services.market_context_hardening_service import harden_market_context
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.market_fact_repository import MarketFactRepository
from app.services.temporal_domain_service import canonical_event_key


class DBOnlyMarketContextMaterializer:
    """Rebuild an immutable context revision using only persisted SQLite records."""

    def __init__(
        self,
        settings: Settings,
        *,
        facts: MarketFactRepository | None = None,
        snapshots: MarketContextSnapshotRepository | None = None,
    ) -> None:
        self.settings = settings
        self.facts = facts or MarketFactRepository(settings)
        self.snapshots = snapshots or MarketContextSnapshotRepository(settings)

    def materialize_for_job(
        self,
        *,
        job: dict[str, Any],
        ai_enrichment: dict[str, Any],
    ) -> dict[str, Any] | None:
        symbol = str(job.get("symbol") or "MNQ")
        previous = self.snapshots.latest(symbol)
        if previous is None:
            return None
        base = previous["debug_payload"]
        section_by_key = _section_index(base.get("event_calendar") or {})
        relevant_keys = set(section_by_key) | {str(job.get("event_key") or "")}
        records = [
            row for row in self.facts.economic_event_records(country="US")
            if str(row.get("canonical_event_key") or canonical_event_key(row)) in relevant_keys
        ]
        calendar = {"critical_macro_events": [], "fed_communications": [], "other_economic_events": []}
        for event in records:
            key = str(event.get("canonical_event_key") or canonical_event_key(event))
            section = section_by_key.get(key) or _event_section(event)
            calendar[section].append(event)
        debug = {
            key: value
            for key, value in base.items()
            if key not in {"snapshot_id", "snapshot_revision", "ai_enrichment", "event_calendar", "events_today", "events_today_context"}
        }
        debug["generated_at_utc"] = datetime.now(UTC).replace(microsecond=0).isoformat()
        debug["event_calendar"] = calendar
        debug["events_today"] = [
            event for event in records
            if str(event.get("date") or "") == str((base.get("market_schedule") or {}).get("context_date") or "")
        ]
        metadata = dict(debug.get("metadata") or {})
        metadata["worker_materialization"] = {
            "mode": "DB_ONLY",
            "provider_calls": 0,
            "browser_calls": 0,
            "AI_calls": 0,
            "source_snapshot_id": previous["snapshot_id"],
            "source_job_id": job.get("job_id"),
        }
        debug["metadata"] = metadata
        debug = harden_market_context(debug, settings=self.settings)
        return self.snapshots.save_next(
            symbol=symbol,
            refresh_mode="worker_db_only_materialization",
            debug_payload=debug,
            ai_enrichment=ai_enrichment,
            source_job_id=str(job.get("job_id")),
            job_ids=[str(job.get("job_id"))],
        )


def _section_index(calendar: dict[str, Any]) -> dict[str, str]:
    output: dict[str, str] = {}
    for section in ("critical_macro_events", "fed_communications", "other_economic_events"):
        for event in calendar.get(section) or []:
            if isinstance(event, dict):
                output[str(event.get("canonical_event_key") or canonical_event_key(event))] = section
    return output


def _event_section(event: dict[str, Any]) -> str:
    kind = str(event.get("event_kind") or "").lower()
    category = str(event.get("category") or "").upper()
    if kind == "scheduled_speech" or "FOMC" in category or "FED" in category:
        return "fed_communications"
    if str(event.get("impact") or "").upper() == "HIGH" or category in {
        "CPI", "PPI", "PCE", "GDP", "NFP", "INITIAL_JOBLESS_CLAIMS"
    }:
        return "critical_macro_events"
    return "other_economic_events"
