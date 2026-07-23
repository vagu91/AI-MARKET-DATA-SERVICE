from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable

from app.core.config import Settings
from app.services.market_context_hardening_service import harden_market_context
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.market_fact_repository import MarketFactRepository
from app.services.temporal_domain_service import canonical_event_key
from app.services.temporal_validation_service import (
    TemporalValidationService,
    normalize_event_semantics,
)


class DBOnlyMarketContextMaterializer:
    """Rebuild an immutable context revision using only persisted SQLite records."""

    def __init__(
        self,
        settings: Settings,
        *,
        facts: MarketFactRepository | None = None,
        snapshots: MarketContextSnapshotRepository | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.facts = facts or MarketFactRepository(settings)
        self.snapshots = snapshots or MarketContextSnapshotRepository(settings)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.temporal_validation = TemporalValidationService(settings, clock=self.clock)

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
        if job.get("parent_run_id"):
            return None
        components = self.snapshots.latest_components(symbol)
        if not components:
            raise RuntimeError("committed_market_context_components_missing")
        section_by_key = _section_index(components.get("event_calendar") or {})
        relevant_keys = set(section_by_key) | {str(job.get("event_key") or "")}
        raw_records = [
            row for row in self.facts.economic_event_records(country="US")
            if str(row.get("canonical_event_key") or canonical_event_key(row)) in relevant_keys
        ]
        audited_records, quarantined = self.temporal_validation.audit_economic_events(
            raw_records
        )
        records = [normalize_event_semantics(row) for row in audited_records]
        calendar = {"critical_macro_events": [], "fed_communications": [], "other_economic_events": []}
        for event in records:
            key = str(event.get("canonical_event_key") or canonical_event_key(event))
            section = section_by_key.get(key) or _event_section(event)
            calendar[section].append(event)
        debug = dict(components)
        generated_at = self.clock().astimezone(UTC).replace(microsecond=0)
        debug["generated_at_utc"] = generated_at.isoformat()
        debug["event_calendar"] = calendar
        debug["events_today"] = [
            event for event in records
            if str(event.get("date") or "") == generated_at.date().isoformat()
        ]
        metadata = dict(debug.get("metadata") or {})
        metadata["worker_materialization"] = {
            "mode": "DB_ONLY",
            "provider_calls": 0,
            "browser_calls": 0,
            "AI_calls": 0,
            "source_snapshot_id": previous["snapshot_id"],
            "source_job_id": job.get("job_id"),
            "research_run_id": _job_run_id(job),
            "parent_run_id": None,
            "committed_component_count": len(components),
            "temporal_quarantine": quarantined,
        }
        debug["metadata"] = metadata
        debug = harden_market_context(
            debug,
            settings=self.settings,
            now=generated_at,
            force_recalculate=True,
        )
        research_run_id = _job_run_id(job)
        job_type = str(job.get("job_type") or "")
        if not research_run_id and job_type not in {"", "RELEASE_ACTUAL_REFRESH"}:
            raise ValueError("completed_research_job_missing_run_link")
        return self.snapshots.save_next(
            symbol=symbol,
            refresh_mode="worker_db_only_materialization",
            debug_payload=debug,
            ai_enrichment=ai_enrichment,
            source_job_id=str(job.get("job_id")),
            job_ids=[str(job.get("job_id"))],
            research_run_id=research_run_id,
        )

    def materialize_for_parent(
        self,
        *,
        parent: dict[str, Any],
        ai_enrichment: dict[str, Any],
    ) -> dict[str, Any] | None:
        if str(parent.get("status") or "") not in {"SUCCEEDED", "PARTIAL", "NO_DATA"}:
            return None
        symbol = str(parent.get("symbol") or "MNQ")
        previous = self.snapshots.latest(symbol)
        if previous is None:
            return None
        components = self.snapshots.latest_components(symbol)
        if not components:
            raise RuntimeError("committed_market_context_components_missing")
        generated_at = self.clock().astimezone(UTC).replace(microsecond=0)
        records, quarantined = self.temporal_validation.audit_economic_events(
            self.facts.economic_event_records(country="US")
        )
        normalized = [normalize_event_semantics(row) for row in records]
        calendar = {
            "critical_macro_events": [],
            "fed_communications": [],
            "other_economic_events": [],
        }
        for event in normalized:
            calendar[_event_section(event)].append(event)
        debug = dict(components)
        debug["generated_at_utc"] = generated_at.isoformat()
        debug["event_calendar"] = calendar
        debug["events_today"] = [
            event
            for event in normalized
            if str(event.get("date") or "") == generated_at.date().isoformat()
        ]
        metadata = dict(debug.get("metadata") or {})
        metadata["worker_materialization"] = {
            "mode": "DB_ONLY_PARENT_AGGREGATION",
            "provider_calls": 0,
            "browser_calls": 0,
            "AI_calls": 0,
            "source_snapshot_id": previous["snapshot_id"],
            "source_job_id": parent.get("parent_job_id"),
            "research_run_id": parent.get("parent_run_id"),
            "parent_run_id": parent.get("parent_run_id"),
            "committed_component_count": len(components),
            "temporal_quarantine": quarantined,
        }
        debug["metadata"] = metadata
        debug = harden_market_context(
            debug,
            settings=self.settings,
            now=generated_at,
            force_recalculate=True,
        )
        child_job_ids = [
            str(item["child_job_id"]) for item in parent.get("children") or []
        ]
        return self.snapshots.save_next(
            symbol=symbol,
            refresh_mode="worker_db_only_parent_materialization",
            debug_payload=debug,
            ai_enrichment=ai_enrichment,
            source_job_id=None,
            job_ids=child_job_ids,
            parent_run_id=str(parent["parent_run_id"]),
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


def _job_run_id(job: dict[str, Any]) -> str | None:
    result = job.get("result_payload")
    if not isinstance(result, dict):
        result = {}
    value = result.get("run_id") or job.get("research_run_id")
    return str(value) if value else None
