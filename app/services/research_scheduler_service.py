from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

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
from app.services.research_gap_manifest import ResearchGapManifestBuilder
from app.services.parallel_research_coordinator import ParallelResearchCoordinator
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    TRIGGER_CLASS_BY_ENTITY,
    compute_datum_lifecycle,
)
from app.services.research_agent_enablement import is_research_agent_enabled
from app.services.research_gap_manifest import TOPIC_PROFILES
from app.services.observability_contract_service import TelemetryRepository


class ResearchSchedulerService:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.jobs = AIResearchJobRepository(settings)
        self.service = AIResearchJobService(settings, repository=self.jobs)
        self.snapshots = MarketContextSnapshotRepository(settings)
        self.lifecycle = LifecycleRepository(settings, clock=self.clock)
        self.telemetry = TelemetryRepository(settings, clock=self.clock)
        migrate_database(settings.database_path)

    def scan_due_items(
        self,
        *,
        owner: str,
        resolver: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        ai_enqueue: Callable[[list[dict[str, Any]]], Any] | None = None,
        trigger_type: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Lease due work, run resolvers first, and enqueue AI once for residuals."""
        if not self.settings.lifecycle_due_scanner_enabled and not force:
            return {
                "status": "DISABLED",
                "provider_calls": 0,
                "ai_invocations": 0,
                "claimed": 0,
            }
        now = self.clock()
        claimed = self.lifecycle.claim_due(owner=owner, now=now)
        self.telemetry.emit(
            "lease",
            identifiers={"correlation_id": owner},
            decision_summary="lifecycle due scan leased bounded work",
            payload={"status": "LEASED", "reason": f"claimed:{len(claimed)}"},
        )
        explicit_trigger_class = TRIGGER_CLASS_BY_ENTITY.get(
            str(trigger_type or "").lower()
        )
        has_trigger = explicit_trigger_class == "TRIGGER" or any(
            item.get("trigger_class") == "TRIGGER" for item in claimed
        )
        provider_calls = 0
        resolved: list[str] = []
        rematerialized: list[str] = []
        residual: list[dict[str, Any]] = []
        ai_eligible: list[dict[str, Any]] = []
        deferred: list[str] = []
        for item in claimed:
            item_id = str(item["item_id"])
            if (
                item.get("trigger_class") == "REFRESH_ON_TRIGGER"
                and not has_trigger
            ):
                self.lifecycle.transition(
                    item_id,
                    owner=owner,
                    work_status="READY",
                    refresh_reason="waiting_for_material_trigger",
                    next_refresh_at=(
                        now
                        + timedelta(
                            seconds=int(
                                self.settings.lifecycle_due_scanner_interval_seconds
                            )
                        )
                    ).isoformat(),
                    now=now,
                )
                deferred.append(item_id)
                continue
            provider_result = (
                resolver(item)
                if resolver is not None
                else {"status": "NOT_CONFIGURED"}
            )
            provider_calls += int(resolver is not None)
            self.telemetry.emit(
                "provider_call",
                identifiers={"correlation_id": owner},
                decision_summary="deterministic lifecycle resolver evaluated due item",
                stop_reason=str(provider_result.get("status") or "NOT_CONFIGURED"),
                payload={
                    "status": str(
                        provider_result.get("status") or "NOT_CONFIGURED"
                    ),
                    "reason": str(provider_result.get("reason") or ""),
                },
            )
            if str(provider_result.get("status") or "").upper() in {
                "RESOLVED",
                "FRESH",
                "NOT_REQUIRED",
            }:
                datum = provider_result.get("datum")
                lifecycle = provider_result.get("lifecycle")
                if isinstance(datum, dict):
                    if lifecycle is None:
                        lifecycle = compute_datum_lifecycle(
                            str(item.get("entity_type") or "unknown"),
                            str(item.get("entity_key") or ""),
                            datum,
                            settings=self.settings,
                            now=now,
                            triggering_event=trigger_type,
                            refresh_reason="provider_resolution_completed",
                        )
                    self.lifecycle.upsert(
                        lifecycle,
                        payload=datum,
                        work_status="COMPLETED",
                    )
                    snapshot = self._rematerialize_provider_resolution(
                        item=item,
                        datum=datum,
                        lifecycle=lifecycle.as_dict(),
                        trigger_type=(
                            trigger_type
                            or item.get("triggering_event")
                            or (
                                item.get("entity_type")
                                if item.get("trigger_class") == "TRIGGER"
                                else None
                            )
                        ),
                        owner=owner,
                        now=now,
                    )
                    if snapshot is not None:
                        rematerialized.append(str(snapshot["snapshot_id"]))
                else:
                    self.lifecycle.complete(
                        item_id,
                        owner=owner,
                        next_refresh_at=provider_result.get("next_refresh_at"),
                        now=now,
                    )
                resolved.append(item_id)
                continue
            provider_status = str(
                provider_result.get("status") or "NOT_CONFIGURED"
            ).upper()
            unresolved = {
                **item,
                "provider_resolver_status": provider_status,
            }
            residual.append(unresolved)
            if (
                resolver is not None
                and provider_status in {"EXHAUSTED", "NOT_FOUND", "NO_DATA"}
                and provider_result.get("ai_eligible", True) is True
            ):
                ai_eligible.append(unresolved)
        ai_invocations = 0
        enqueue_result: Any = None
        if ai_eligible and ai_enqueue is not None:
            enqueue_result = ai_enqueue(ai_eligible)
            ai_invocations = 1
            self.telemetry.emit(
                "enqueue",
                identifiers={"correlation_id": owner},
                decision_summary="coalesced residual lifecycle gaps enqueued",
                payload={
                    "status": "QUEUED",
                    "reason": f"residual_count:{len(ai_eligible)}",
                },
            )
            for item in ai_eligible:
                self.lifecycle.transition(
                    str(item["item_id"]),
                    owner=owner,
                    work_status="QUEUED",
                    refresh_reason="provider_exhausted_ai_queued",
                    now=now,
                )
        else:
            for item in residual:
                if item in ai_eligible and ai_enqueue is not None:
                    continue
                self.lifecycle.transition(
                    str(item["item_id"]),
                    owner=owner,
                    work_status="IDLE",
                    refresh_reason=(
                        "provider_deferred_ai_not_requested"
                        if str(item.get("provider_resolver_status")) == "DEFERRED"
                        else "provider_unresolved_ai_not_configured"
                    ),
                    next_refresh_at=(
                        now
                        + timedelta(
                            seconds=int(
                                self.settings.lifecycle_due_scanner_interval_seconds
                            )
                        )
                    ).isoformat(),
                    now=now,
                )
                self.telemetry.emit(
                    "retry_backoff",
                    identifiers={"correlation_id": owner},
                    decision_summary=(
                        "unresolved lifecycle item deferred to bounded next check"
                    ),
                    stop_reason="DEFERRED",
                    payload={
                        "status": "IDLE",
                        "reason": "provider_unresolved_ai_not_requested",
                    },
                )
        return {
            "status": "COMPLETED",
            "claimed": len(claimed),
            "provider_calls": provider_calls,
            "resolved": resolved,
            "rematerialized_snapshot_ids": rematerialized,
            "deferred": deferred,
            "residual_count": len(residual),
            "ai_eligible_count": len(ai_eligible),
            "ai_invocations": ai_invocations,
            "enqueue_result": enqueue_result,
            "coalesced": len(ai_eligible) > 1,
        }

    def _rematerialize_provider_resolution(
        self,
        *,
        item: dict[str, Any],
        datum: dict[str, Any],
        lifecycle: dict[str, Any],
        trigger_type: str | None,
        owner: str,
        now: datetime,
    ) -> dict[str, Any] | None:
        previous = self.snapshots.latest("MNQ")
        if previous is None:
            return None
        components = self.snapshots.latest_components("MNQ")
        if not components:
            return None
        debug = dict(components)
        resolutions = dict(debug.get("lifecycle_resolutions") or {})
        resolutions[str(item.get("entity_key") or item["item_id"])] = {
            "entity_type": item.get("entity_type"),
            "value": datum,
            "lifecycle": lifecycle,
        }
        debug["lifecycle_resolutions"] = resolutions
        debug["generated_at_utc"] = now.astimezone(UTC).replace(
            microsecond=0
        ).isoformat()
        snapshot = self.snapshots.save_next(
            symbol="MNQ",
            refresh_mode="lifecycle_provider_resolution",
            debug_payload=debug,
            ai_enrichment={"status": "NOT_REQUIRED"},
            trigger_type=trigger_type,
            trigger_entity=str(item.get("entity_key") or ""),
            correlation_id=owner,
        )
        self.telemetry.emit(
            "materialization",
            identifiers={
                "correlation_id": owner,
                "snapshot_id": snapshot.get("snapshot_id"),
            },
            decision_summary="provider resolution rematerialized from committed data",
            payload={"status": "SUCCEEDED", "reason": str(trigger_type or "")},
        )
        return snapshot

    def enqueue_due_residuals(
        self,
        items: list[dict[str, Any]],
        *,
        trigger_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Use the normal persistent job service for resolver-exhausted gaps."""
        jobs: list[dict[str, Any]] = []
        for item in items:
            topic = _topic_for_entity(str(item.get("entity_type") or ""))
            profile_id = TOPIC_PROFILES.get(topic)
            if not profile_id or not is_research_agent_enabled(
                self.settings,
                topic=topic,
                profile_id=profile_id,
            ):
                continue
            job, created = self.service.enqueue_explicit(
                job_type=profile_id,
                symbol="MNQ",
                correlation_id=f"lifecycle-due-{uuid.uuid4()}",
                request_payload={
                    "missing_fields": list(item.get("fields_attempted") or []),
                    "lifecycle_item_id": item.get("item_id"),
                    "database_context": item.get("payload") or {},
                    "trigger_envelope": (
                        {
                            "trigger_type": trigger_type,
                            "trigger_entity": item.get("entity_key"),
                            "correlation_id": f"lifecycle-{item.get('item_id')}",
                        }
                        if trigger_type
                        else None
                    ),
                },
                pending_fields=list(item.get("fields_attempted") or []),
                specialized_topic=topic,
            )
            if created:
                jobs.append(job)
        return jobs

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
        if job_type == "MNQ_MARKET_RESEARCH":
            manifest = ResearchGapManifestBuilder(self.settings).build(
                snapshot=snapshot,
                components=self.snapshots.latest_components("MNQ"),
            )
            parent = ParallelResearchCoordinator(self.settings).create_parent(
                manifest,
                correlation_id=f"scheduler-{trigger_name}-{uuid.uuid4()}",
                force=force,
            )
            return self._decision(
                trigger_name,
                fingerprint,
                "QUEUED" if parent["child_job_ids"] else "NOT_REQUIRED",
                (
                    "gap_manifest_agent_children_created"
                    if parent["child_job_ids"]
                    else "all_topics_satisfied_by_committed_data"
                ),
                job_id=parent["child_job_ids"][0] if parent["child_job_ids"] else None,
                job_ids=parent["child_job_ids"],
            )
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


def _topic_for_entity(entity_type: str) -> str:
    normalized = entity_type.lower()
    if normalized in {"vix", "vvix", "vix_futures", "put_call", "skew"}:
        return "vix_risk"
    if normalized in {"cot", "cot_publication"}:
        return "cot_positioning"
    if normalized.startswith("earnings"):
        return (
            "earnings_intelligence"
            if normalized == "earnings_intelligence"
            else "earnings"
        )
    if normalized in TOPIC_PROFILES:
        return normalized
    if normalized in {"macro_actual", "macro_schedule", "macro_snapshot"}:
        return "macro_events"
    if normalized.startswith("fomc") or normalized == "fed_rates":
        return "fed_rates"
    if normalized in {"breaking_news", "news"}:
        return "news"
    return normalized


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
