from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.models.events import EconomicEvent
from app.services.ai_research_job_repository import ACTIVE_JOB_STATUSES, AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.temporal_domain_service import temporal_event_state
from app.services.temporal_domain_service import canonical_event_key
from app.services.data_freshness_service import parse_datetime


class ResearchSchedulerService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.jobs = AIResearchJobRepository(settings)
        self.service = AIResearchJobService(settings, repository=self.jobs)
        self.snapshots = MarketContextSnapshotRepository(settings)
        migrate_database(settings.database_path)

    def evaluate(self, trigger_name: str, *, force: bool = False) -> dict[str, Any]:
        snapshot = self.snapshots.latest("MNQ")
        payload = _fingerprint_payload(snapshot)
        fingerprint = hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()
        if not force and self._same_completed_decision(trigger_name, fingerprint):
            return self._decision(trigger_name, fingerprint, "NOT_REQUIRED", "input_fingerprint_unchanged")
        if self._active_count() >= self.settings.research_max_concurrent_jobs:
            return self._decision(trigger_name, fingerprint, "NOT_REQUIRED", "max_concurrent_jobs_reached")
        if self.settings.research_budget_mode == "enforce":
            if self._daily_runs() >= self.settings.research_daily_budget_runs:
                return self._decision(trigger_name, fingerprint, "NOT_REQUIRED", "daily_budget_exhausted")
            usage = self._daily_tool_usage()
            if usage["search_count"] >= self.settings.research_daily_budget_searches:
                return self._decision(trigger_name, fingerprint, "NOT_REQUIRED", "daily_search_budget_exhausted")
            if usage["opened_source_count"] >= self.settings.research_daily_budget_opened_sources:
                return self._decision(trigger_name, fingerprint, "NOT_REQUIRED", "daily_opened_source_budget_exhausted")
        event_jobs = self._event_jobs(trigger_name, snapshot)
        if event_jobs is not None:
            if not event_jobs:
                return self._decision(trigger_name, fingerprint, "NOT_REQUIRED", "no_eligible_event_work")
            return self._decision(
                trigger_name, fingerprint, "QUEUED", "eligible_event_work",
                job_id=str(event_jobs[0]["job_id"]), job_ids=[str(item["job_id"]) for item in event_jobs],
            )
        job_type = _job_type(trigger_name)
        job, created = self.service.enqueue_explicit(
            job_type=job_type, symbol="MNQ", correlation_id=f"scheduler-{trigger_name}-{uuid.uuid4()}",
            request_payload={
                "database_context": payload, "trigger_name": trigger_name,
                "max_searches": self.settings.research_max_searches,
                "max_opened_sources": self.settings.research_max_opened_sources,
                "context_date": payload.get("context_date"), "market_session": payload.get("market_session"),
            },
            force=force,
        )
        return self._decision(
            trigger_name, fingerprint, "QUEUED" if created else "NOT_REQUIRED",
            "input_changed" if created else "run_window_already_evaluated", job_id=job["job_id"],
        )

    def _event_jobs(
        self,
        trigger_name: str,
        snapshot: dict[str, Any] | None,
    ) -> list[dict[str, Any]] | None:
        if trigger_name not in {"pre_event", "post_release", "speech_outcome"}:
            return None
        events = _snapshot_events(snapshot)
        now = datetime.now(UTC)
        if trigger_name == "pre_event":
            eligible = []
            for event in events:
                state = temporal_event_state(event, now=now)
                release_at = parse_datetime(state.get("release_at"))
                minutes_until = (release_at - now).total_seconds() / 60 if release_at else None
                if (
                    state["temporal_status"] == "PRE_RELEASE"
                    and minutes_until is not None
                    and 0 <= minutes_until <= self.settings.research_pre_event_window_minutes
                ):
                    eligible.append(event)
            return self.service.enqueue_missing_events(
                eligible, correlation_id=f"scheduler-{trigger_name}-{uuid.uuid4()}"
            )
        states = [(event, temporal_event_state(event, now=now)) for event in events]
        target = "AWAITING_OUTCOME" if trigger_name == "speech_outcome" else "AWAITING_ACTUAL"
        eligible = [event for event, state in states if state["temporal_status"] == target]
        return self.service.enqueue_temporal_refreshes(
            eligible, correlation_id=f"scheduler-{trigger_name}-{uuid.uuid4()}", now=now,
        )

    def _same_completed_decision(self, trigger: str, fingerprint: str) -> bool:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT decision,created_at FROM research_scheduler_decisions
                WHERE trigger_name=? AND symbol='MNQ' AND input_fingerprint=?
                ORDER BY created_at DESC,rowid DESC LIMIT 1
                """,
                (trigger, fingerprint),
            ).fetchone()
        if row is None or row["decision"] not in {"QUEUED", "NOT_REQUIRED"}:
            return False
        created_at = datetime.fromisoformat(str(row["created_at"]).replace("Z", "+00:00"))
        age_minutes = (datetime.now(UTC) - created_at.astimezone(UTC)).total_seconds() / 60
        return age_minutes < self.settings.research_minimum_freshness_minutes

    def _active_count(self) -> int:
        status = self.jobs.status()
        return sum(int((status.get("by_status") or {}).get(item) or 0) for item in ACTIVE_JOB_STATUSES)

    def _daily_runs(self) -> int:
        today = datetime.now(UTC).date().isoformat()
        with connect_sqlite(self.settings.database_path) as conn:
            return int(conn.execute(
                """
                SELECT COUNT(*) FROM ai_research_jobs
                WHERE substr(created_at,1,10)=? AND job_type != 'RELEASE_ACTUAL_REFRESH'
                """,
                (today,),
            ).fetchone()[0])

    def _daily_tool_usage(self) -> dict[str, int]:
        today = datetime.now(UTC).date().isoformat()
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(search_count),0) AS searches,
                       COALESCE(SUM(opened_source_count),0) AS opened
                FROM research_runs WHERE substr(created_at,1,10)=?
                """,
                (today,),
            ).fetchone()
        return {"search_count": int(row["searches"]), "opened_source_count": int(row["opened"])}

    def _decision(
        self,
        trigger: str,
        fingerprint: str,
        decision: str,
        reason: str,
        *,
        job_id: str | None = None,
        job_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        created_at = datetime.now(UTC).replace(microsecond=0).isoformat()
        result = {
            "trigger_name": trigger, "symbol": "MNQ", "input_fingerprint": fingerprint,
            "decision": decision, "reason": reason, "job_id": job_id, "created_at": created_at,
            "job_ids": list(job_ids or ([job_id] if job_id else [])),
        }
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                "INSERT INTO research_scheduler_decisions VALUES (?,?,?,?,?,?,?,?)",
                (f"rsd-{uuid.uuid4()}", trigger, "MNQ", fingerprint, decision, reason, job_id, created_at),
            )
            conn.commit()
        return result


def _fingerprint_payload(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if snapshot is None:
        return {"snapshot_id": None, "missing_snapshot": True}
    debug = snapshot.get("debug_payload") or {}
    calendar = debug.get("event_calendar") or {}
    news = debug.get("news_context") or {}
    events = []
    for section in ("critical_macro_events", "fed_communications", "other_economic_events"):
        for item in calendar.get(section) or []:
            if not isinstance(item, dict):
                continue
            enrichment = item.get("enrichment") if isinstance(item.get("enrichment"), dict) else {}
            lifecycle = temporal_event_state(item)
            events.append({
                "event_key": str(item.get("canonical_event_key") or canonical_event_key(item)),
                "metric_id": item.get("metric_id"), "reference_period": item.get("reference_period"),
                "frequency": item.get("frequency"), "release_at": lifecycle.get("release_at"),
                "temporal_status": lifecycle.get("temporal_status"),
                "actual": item.get("actual") if item.get("actual") not in (None, "") else enrichment.get("actual"),
                "forecast": enrichment.get("forecast"), "consensus": enrichment.get("consensus"),
                "previous": enrichment.get("previous"), "outcome": lifecycle.get("outcome"),
            })
    news_rows = []
    for item in news.get("latest") or news.get("articles") or []:
        if isinstance(item, dict):
            news_rows.append({
                "news_key": item.get("news_key") or item.get("url"),
                "content_hash": item.get("content_hash") or item.get("checksum"),
            })
    quality = debug.get("quality") or debug.get("data_quality") or {}
    return {
        "context_date": (debug.get("market_schedule") or {}).get("context_date"),
        "market_session": (debug.get("market_schedule") or {}).get("market_session_status"),
        "events": sorted(events, key=lambda item: item["event_key"]),
        "news": sorted(news_rows, key=lambda item: str(item["news_key"])),
        "quality_gaps": sorted(quality.get("blocking_gaps") or quality.get("missing_critical_fields") or []),
        "conflicts": sorted((debug.get("data_quality") or {}).get("conflicts") or [], key=lambda item: _json(item)),
    }


def _snapshot_events(snapshot: dict[str, Any] | None) -> list[EconomicEvent]:
    if snapshot is None:
        return []
    calendar = ((snapshot.get("debug_payload") or {}).get("event_calendar") or {})
    events: list[EconomicEvent] = []
    for section in ("critical_macro_events", "fed_communications", "other_economic_events"):
        for item in calendar.get(section) or []:
            if not isinstance(item, dict):
                continue
            try:
                events.append(EconomicEvent.model_validate(item))
            except (TypeError, ValueError):
                continue
    return events


def _job_type(trigger: str) -> str:
    if trigger in {"news_refresh"}:
        return "NEWS_DRIVER_RESEARCH"
    if trigger in {"earnings_post_release"}:
        return "EARNINGS_CONTEXT"
    if trigger in {"speech_outcome"}:
        return "SPEECH_OUTCOME_REFRESH"
    return "MNQ_MARKET_RESEARCH"


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
