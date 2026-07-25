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
    DatumLifecycle,
    LifecycleRepository,
    TRIGGER_CLASS_BY_ENTITY,
    compute_datum_lifecycle,
    material_changes,
)
from app.services.research_agent_enablement import is_research_agent_enabled
from app.services.research_gap_manifest import TOPIC_PROFILES
from app.services.observability_contract_service import TelemetryRepository
from app.services.execution_context import ExecutionContext, authorizes_ai


class ResearchSchedulerService:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
        deterministic_runtime=None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.jobs = AIResearchJobRepository(settings)
        self.service = AIResearchJobService(settings, repository=self.jobs)
        self.snapshots = MarketContextSnapshotRepository(settings)
        self.lifecycle = LifecycleRepository(settings, clock=self.clock)
        self.telemetry = TelemetryRepository(settings, clock=self.clock)
        self.deterministic_runtime = deterministic_runtime
        migrate_database(settings.database_path)

    def scan_due_items(
        self,
        *,
        owner: str,
        resolver: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        ai_enqueue: Callable[[list[dict[str, Any]]], Any] | None = None,
        trigger_type: str | None = None,
        force: bool = False,
        due_since: datetime | None = None,
        execution_context: ExecutionContext | None = None,
    ) -> dict[str, Any]:
        """Lease due work, run resolvers first, and enqueue AI once for residuals."""
        ai_authorized = self._scanner_ai_authorized(execution_context)
        if not ai_authorized:
            self.telemetry.emit(
                "ai_authorization",
                identifiers={"correlation_id": owner},
                decision_summary="AI_SUPPRESSED",
                stop_reason="AI_SUPPRESSED",
                payload={
                    "status": "AI_SUPPRESSED",
                    "reason": "scheduler_disabled_or_execution_context_invalid",
                },
            )
        if not self.settings.lifecycle_due_scanner_enabled and not force:
            return {
                "status": "DISABLED",
                "provider_calls": 0,
                "resolver_evaluations": 0,
                "committed_payload_hits": 0,
                "actual_provider_requests": 0,
                "successful_provider_requests": 0,
                "failed_provider_requests": 0,
                "ai_invocations": 0,
                "ai_jobs_created": 0,
                "claimed": 0,
            }
        now = self.clock()
        claimed = self.lifecycle.claim_due(
            owner=owner,
            now=now,
            due_since=due_since,
        )
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
        resolver_evaluations = 0
        committed_payload_hits = 0
        actual_provider_requests = 0
        successful_provider_requests = 0
        failed_provider_requests = 0
        resolved: list[str] = []
        rematerialized: list[str] = []
        residual: list[dict[str, Any]] = []
        ai_eligible: list[dict[str, Any]] = []
        ai_decisions: list[dict[str, str]] = []
        deferred: list[str] = []
        backoff: list[str] = []
        item_outcomes: list[dict[str, str | None]] = []
        effective_triggers: list[dict[str, str]] = []
        for item in claimed:
            item_id = str(item["item_id"])
            effective_trigger_type = _effective_trigger_type(
                item,
                explicit_trigger_type=trigger_type,
            )
            trigger_correlation_id = f"lifecycle-{item_id}"
            if effective_trigger_type:
                effective_triggers.append(
                    {
                        "item_id": item_id,
                        "trigger_type": effective_trigger_type,
                        "trigger_entity": str(item.get("entity_key") or ""),
                        "correlation_id": trigger_correlation_id,
                    }
                )
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
                item_outcomes.append(
                    {
                        "item_id": item_id,
                        "status": "READY",
                        "effective_trigger_type": effective_trigger_type,
                    }
                )
                continue
            provider_result = (
                resolver(item)
                if resolver is not None
                else {"status": "NOT_CONFIGURED"}
            )
            if resolver is not None:
                resolver_evaluations += 1
                self.telemetry.emit(
                    "resolver_evaluation",
                    identifiers={"correlation_id": owner},
                    decision_summary=(
                        "deterministic lifecycle resolver evaluated due item"
                    ),
                    stop_reason=str(
                        provider_result.get("status") or "NOT_CONFIGURED"
                    ),
                    payload={
                        "status": str(
                            provider_result.get("status")
                            or "NOT_CONFIGURED"
                        ),
                        "reason": str(
                            provider_result.get("reason") or ""
                        ),
                    },
                )
            if provider_result.get("committed_payload_hit") is True:
                committed_payload_hits += 1
                self.telemetry.emit(
                    "committed_payload_hit",
                    identifiers={"correlation_id": owner},
                    decision_summary=(
                        "persisted lifecycle payload was evaluated before provider I/O"
                    ),
                    stop_reason=str(
                        provider_result.get("committed_payload_reason") or ""
                    ),
                    payload={
                        "status": str(
                            provider_result.get("status") or ""
                        ),
                        "reason": str(
                            provider_result.get(
                                "committed_payload_reason"
                            )
                            or ""
                        ),
                    },
                )
            if provider_result.get("provider_negative_cache_hit") is True:
                self.telemetry.emit(
                    "provider_negative_cache_hit",
                    identifiers={"correlation_id": owner},
                    decision_summary=(
                        "provider request suppressed by active negative cache"
                    ),
                    stop_reason="NEGATIVE_CACHE",
                    payload={
                        "status": "DEFERRED",
                        "reason": str(
                            provider_result.get("reason") or ""
                        ),
                    },
                )
            if provider_result.get("provider_cache_hit") is True:
                self.telemetry.emit(
                    "provider_cache_hit",
                    identifiers={"correlation_id": owner},
                    decision_summary=(
                        "provider adapter completed from its deterministic cache"
                    ),
                    stop_reason="CACHE_HIT",
                    payload={
                        "status": str(
                            provider_result.get("status") or ""
                        ),
                        "reason": str(
                            provider_result.get("reason") or ""
                        ),
                    },
                )
            if provider_result.get("provider_request_attempted") is True:
                actual_provider_requests += 1
                self.telemetry.emit(
                    "provider_request_attempted",
                    identifiers={"correlation_id": owner},
                    decision_summary=(
                        "deterministic provider adapter attempted acquisition"
                    ),
                    payload={
                        "status": "ATTEMPTED",
                        "reason": str(
                            provider_result.get("reason") or ""
                        ),
                    },
                )
                if provider_result.get("provider_request_failed") is True:
                    failed_provider_requests += 1
                    self.telemetry.emit(
                        "provider_request_failed",
                        identifiers={"correlation_id": owner},
                        decision_summary=(
                            "deterministic provider acquisition failed"
                        ),
                        stop_reason=str(
                            provider_result.get("status") or "FAILED"
                        ),
                        payload={
                            "status": str(
                                provider_result.get("status") or "FAILED"
                            ),
                            "reason": str(
                                provider_result.get("reason") or ""
                            ),
                        },
                    )
                elif provider_result.get(
                    "provider_request_completed"
                ) is True:
                    successful_provider_requests += 1
                    self.telemetry.emit(
                        "provider_request_completed",
                        identifiers={"correlation_id": owner},
                        decision_summary=(
                            "deterministic provider acquisition completed"
                        ),
                        stop_reason=str(
                            provider_result.get("status") or "COMPLETED"
                        ),
                        payload={
                            "status": str(
                                provider_result.get("status")
                                or "COMPLETED"
                            ),
                            "reason": str(
                                provider_result.get("reason") or ""
                            ),
                        },
                    )
            provider_status = str(
                provider_result.get("status") or "NOT_CONFIGURED"
            ).upper()
            if provider_status == "PARTIAL":
                datum = provider_result.get("datum")
                lifecycle = provider_result.get("lifecycle")
                missing_fields = list(
                    provider_result.get("missing_fields")
                    or item.get("fields_attempted")
                    or []
                )
                if isinstance(lifecycle, DatumLifecycle):
                    lifecycle_value = lifecycle.as_dict()
                elif isinstance(lifecycle, dict):
                    lifecycle_value = lifecycle
                else:
                    lifecycle_value = compute_datum_lifecycle(
                        str(item.get("entity_type") or "unknown"),
                        str(item.get("entity_key") or ""),
                        datum if isinstance(datum, dict) else {},
                        settings=self.settings,
                        now=now,
                        triggering_event=effective_trigger_type,
                        refresh_reason="provider_partial_resolution",
                    ).as_dict()
                if isinstance(datum, dict) and datum:
                    snapshot = self._rematerialize_provider_resolution(
                        item=item,
                        datum=datum,
                        lifecycle=lifecycle_value,
                        trigger_type=effective_trigger_type,
                        owner=owner,
                        now=now,
                        final_resolution=False,
                    )
                    if snapshot is not None:
                        rematerialized.append(str(snapshot["snapshot_id"]))
                    else:
                        self.lifecycle.upsert(
                            DatumLifecycle(**lifecycle_value),
                            payload=datum,
                            work_status="PARTIAL",
                        )
                unresolved = {
                    **item,
                    "provider_resolver_status": "PARTIAL",
                    "lifecycle_finalized_as_partial": True,
                    "effective_trigger_type": effective_trigger_type,
                    "trigger_correlation_id": trigger_correlation_id,
                    "fields_attempted": missing_fields,
                }
                residual.append(unresolved)
                agent_status = str(
                    provider_result.get("agent_status")
                    or (
                        "ENABLED"
                        if provider_result.get("ai_eligible") is True
                        else "DISABLED"
                    )
                )
                ai_decisions.append(
                    {
                        "item_id": item_id,
                        "agent_status": agent_status,
                        "execution_status": str(
                            provider_result.get("execution_status")
                            or (
                                "ELIGIBLE"
                                if provider_result.get("ai_eligible") is True
                                else "NOT_REQUESTED"
                            )
                        ),
                    }
                )
                if (
                    missing_fields
                    and provider_result.get("ai_eligible") is True
                ):
                    ai_eligible.append(unresolved)
                continue
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
                            triggering_event=effective_trigger_type,
                            refresh_reason="provider_resolution_completed",
                        )
                    snapshot = self._rematerialize_provider_resolution(
                        item=item,
                        datum=datum,
                        lifecycle=lifecycle.as_dict(),
                        trigger_type=effective_trigger_type,
                        owner=owner,
                        now=now,
                    )
                    if snapshot is not None:
                        rematerialized.append(str(snapshot["snapshot_id"]))
                    else:
                        self.lifecycle.upsert(
                            lifecycle,
                            payload=datum,
                            work_status="COMPLETED",
                        )
                else:
                    self.lifecycle.complete(
                        item_id,
                        owner=owner,
                        next_refresh_at=provider_result.get("next_refresh_at"),
                        now=now,
                    )
                resolved.append(item_id)
                item_outcomes.append(
                    {
                        "item_id": item_id,
                        "status": "RESOLVED",
                        "effective_trigger_type": effective_trigger_type,
                    }
                )
                continue
            unresolved = {
                **item,
                "provider_resolver_status": provider_status,
                "effective_trigger_type": effective_trigger_type,
                "trigger_correlation_id": trigger_correlation_id,
                "retry_deadline_exhausted": bool(
                    provider_result.get("retry_deadline_exhausted")
                ),
            }
            if provider_status == "DEFERRED":
                deferred_lifecycle = provider_result.get("lifecycle")
                deferred_datum = provider_result.get("datum")
                if isinstance(deferred_lifecycle, dict):
                    deferred_lifecycle = DatumLifecycle(**deferred_lifecycle)
                if deferred_lifecycle is None:
                    deferred_lifecycle = compute_datum_lifecycle(
                        str(item.get("entity_type") or "unknown"),
                        str(item.get("entity_key") or ""),
                        {
                            "refresh_reason": str(
                                provider_result.get("reason")
                                or "provider_temporary_failure"
                            )
                        },
                        settings=self.settings,
                        now=now,
                        attempt_count=int(item.get("attempt_count") or 0) + 1,
                        no_data=True,
                        fields_attempted=list(
                            item.get("fields_attempted") or []
                        ),
                        session_state=item.get("session_state"),
                        triggering_event=effective_trigger_type,
                        retry_class="PROVIDER_TEMPORARY",
                        refresh_reason=str(
                            provider_result.get("reason")
                            or "provider_temporary_failure"
                        ),
                    )
                if not isinstance(deferred_datum, dict):
                    deferred_datum = {
                        "reason": str(
                            provider_result.get("reason")
                            or "provider_temporary_failure"
                        ),
                        "fields_attempted": list(
                            item.get("fields_attempted") or []
                        ),
                    }
                self.lifecycle.upsert(
                    deferred_lifecycle,
                    payload=deferred_datum,
                    work_status="BACKOFF",
                )
                deferred.append(item_id)
                backoff.append(item_id)
                item_outcomes.append(
                    {
                        "item_id": item_id,
                        "status": "BACKOFF",
                        "effective_trigger_type": effective_trigger_type,
                    }
                )
                self.telemetry.emit(
                    "retry_backoff",
                    identifiers={"correlation_id": owner},
                    decision_summary=(
                        "temporary provider failure persisted as negative-cache backoff"
                    ),
                    stop_reason="BACKOFF",
                    payload={
                        "status": "BACKOFF",
                        "reason": str(
                            provider_result.get("reason")
                            or "provider_temporary_failure"
                        ),
                    },
                )
                continue
            residual.append(unresolved)
            ai_decisions.append(
                {
                    "item_id": item_id,
                    "agent_status": str(
                        provider_result.get("agent_status")
                        or (
                            "ENABLED"
                            if provider_result.get("ai_eligible") is True
                            else "NOT_APPLICABLE"
                        )
                    ),
                    "execution_status": str(
                        provider_result.get("execution_status")
                        or (
                            "ELIGIBLE"
                            if provider_result.get("ai_eligible") is True
                            else "NOT_REQUESTED"
                        )
                    ),
                }
            )
            if (
                resolver is not None
                and provider_status in {"EXHAUSTED", "NOT_FOUND", "NO_DATA"}
                and provider_result.get("ai_eligible", True) is True
            ):
                ai_eligible.append(unresolved)
        ai_invocations = 0
        ai_jobs_created = 0
        enqueue_result: Any = None
        queued_item_ids: set[str] = set()
        if (
            ai_eligible
            and ai_enqueue is not None
            and ai_authorized
        ):
            enqueue_result = ai_enqueue(ai_eligible)
            ai_invocations = 1
            ai_jobs_created = _created_job_count(enqueue_result)
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
                transitioned = self.lifecycle.transition(
                    str(item["item_id"]),
                    owner=owner,
                    work_status="QUEUED",
                    refresh_reason="provider_exhausted_ai_queued",
                    now=now,
                )
                if (
                    not transitioned
                    and item.get("lifecycle_finalized_as_partial") is True
                ):
                    transitioned = self.lifecycle.transition_finalized(
                        str(item["item_id"]),
                        expected_work_status="PARTIAL",
                        work_status="QUEUED",
                        refresh_reason="provider_exhausted_ai_queued",
                        now=now,
                    )
                if transitioned:
                    queued_item_ids.add(str(item["item_id"]))
                    item_outcomes.append(
                        {
                            "item_id": str(item["item_id"]),
                            "status": "AI_QUEUED",
                            "effective_trigger_type": item.get(
                                "effective_trigger_type"
                            ),
                        }
                    )
        for item in residual:
            item_id = str(item["item_id"])
            if item_id in queued_item_ids:
                continue
            agent_status = next(
                (
                    decision["agent_status"]
                    for decision in ai_decisions
                    if decision["item_id"] == item_id
                ),
                "",
            )
            terminal_status = (
                "NO_DATA"
                if item.get("retry_deadline_exhausted")
                else "DISABLED"
                if agent_status == "DISABLED"
                else "IDLE"
            )
            terminal_reason = (
                "retry_deadline_exhausted_no_data"
                if item.get("retry_deadline_exhausted")
                else "agent_disabled_ai_not_requested"
                if agent_status == "DISABLED"
                else "provider_unresolved_ai_not_configured"
            )
            transitioned = self.lifecycle.transition(
                item_id,
                owner=owner,
                work_status=terminal_status,
                refresh_reason=terminal_reason,
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
            if (
                not transitioned
                and item.get("lifecycle_finalized_as_partial") is True
            ):
                self.lifecycle.transition_finalized(
                    item_id,
                    expected_work_status="PARTIAL",
                    work_status=terminal_status,
                    refresh_reason=terminal_reason,
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
            item_outcomes.append(
                {
                    "item_id": item_id,
                    "status": terminal_status,
                    "effective_trigger_type": item.get(
                        "effective_trigger_type"
                    ),
                }
            )
        return {
            "status": "COMPLETED",
            "claimed": len(claimed),
            "provider_calls": actual_provider_requests,
            "resolver_evaluations": resolver_evaluations,
            "committed_payload_hits": committed_payload_hits,
            "actual_provider_requests": actual_provider_requests,
            "successful_provider_requests": successful_provider_requests,
            "failed_provider_requests": failed_provider_requests,
            "resolved": resolved,
            "rematerialized_snapshot_ids": rematerialized,
            "deferred": deferred,
            "backoff": backoff,
            "residual_count": len(residual),
            "ai_eligible_count": len(ai_eligible),
            "ai_decisions": ai_decisions,
            "item_outcomes": item_outcomes,
            "effective_triggers": effective_triggers,
            "ai_invocations": ai_invocations,
            "ai_jobs_created": ai_jobs_created,
            "enqueue_result": enqueue_result,
            "coalesced": len(ai_eligible) > 1,
        }

    def startup_catch_up(
        self,
        *,
        resolver: Callable[[dict[str, Any]], dict[str, Any]],
        ai_enqueue: Callable[[list[dict[str, Any]]], Any],
        execution_context: ExecutionContext | None = None,
    ) -> dict[str, Any]:
        if not (
            self.settings.enable_scheduler
            and self.settings.research_scheduler_enabled
            and self.settings.lifecycle_due_scanner_enabled
        ):
            return {
                "status": "DISABLED",
                "reason": "scheduler_or_due_scanner_disabled",
                "claimed": 0,
                "provider_calls": 0,
                "resolver_evaluations": 0,
                "committed_payload_hits": 0,
                "actual_provider_requests": 0,
                "successful_provider_requests": 0,
                "failed_provider_requests": 0,
                "ai_invocations": 0,
                "ai_jobs_created": 0,
                "writes": 0,
            }
        now = self.clock()
        window_start = now - timedelta(
            hours=int(self.settings.lifecycle_startup_catchup_hours)
        )
        result = self.scan_due_items(
            owner="startup-lifecycle-catch-up",
            resolver=resolver,
            ai_enqueue=ai_enqueue,
            due_since=window_start,
            execution_context=execution_context,
        )
        self.telemetry.emit(
            "startup_catch_up",
            identifiers={"correlation_id": "startup-lifecycle-catch-up"},
            decision_summary="bounded startup lifecycle reconciliation completed",
            stop_reason=str(result.get("status") or "COMPLETED"),
            payload={
                "status": str(result.get("status") or "COMPLETED"),
                "reason": (
                    f"window_hours:{self.settings.lifecycle_startup_catchup_hours};"
                    f"claimed:{result.get('claimed', 0)}"
                ),
            },
        )
        return {
            **result,
            "catch_up_window_start": window_start.isoformat(),
            "catch_up_window_hours": int(
                self.settings.lifecycle_startup_catchup_hours
            ),
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
        final_resolution: bool = True,
    ) -> dict[str, Any] | None:
        previous = self.snapshots.latest("MNQ")
        if previous is None:
            return None
        components = self.snapshots.latest_components("MNQ")
        if not components:
            return None
        debug = dict(components)
        debug = _project_resolved_datum(
            debug,
            entity_type=str(item.get("entity_type") or ""),
            entity_key=str(item.get("entity_key") or ""),
            datum=datum,
            lifecycle=lifecycle,
        )
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
        if self.deterministic_runtime is not None:
            debug = self.deterministic_runtime.enrich_market_context_sync(
                debug,
                refresh="auto",
                trigger_type=trigger_type,
            )
        from app.services.ai_trader_consumer_v2_service import (
            build_ai_trader_consumer_v2,
        )

        candidate_consumer = build_ai_trader_consumer_v2(
            debug,
            settings=self.settings,
        )
        changed_sections, _ = material_changes(
            previous.get("consumer_payload") or {},
            candidate_consumer,
        )
        if not changed_sections:
            self.telemetry.emit(
                "materialization",
                identifiers={
                    "correlation_id": owner,
                    "snapshot_id": previous.get("snapshot_id"),
                },
                decision_summary=(
                    "provider resolution produced no material consumer change"
                ),
                stop_reason="NO_CHANGE",
                payload={
                    "status": "UNCHANGED",
                    "reason": str(trigger_type or ""),
                },
            )
            return None
        snapshot = self.snapshots.save_next(
            symbol="MNQ",
            refresh_mode="lifecycle_provider_resolution",
            debug_payload=debug,
            ai_enrichment={"status": "NOT_REQUIRED"},
            trigger_type=trigger_type,
            trigger_entity=str(item.get("entity_key") or ""),
            correlation_id=owner,
            resolved_lifecycle=(
                DatumLifecycle(**lifecycle)
            ),
            resolved_datum=datum,
            resolved_work_status=(
                "COMPLETED" if final_resolution else "PARTIAL"
            ),
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
        execution_context: ExecutionContext | None = None,
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
            effective_trigger_type = _effective_trigger_type(
                item,
                explicit_trigger_type=trigger_type,
            )
            trigger_correlation_id = str(
                item.get("trigger_correlation_id")
                or f"lifecycle-{item.get('item_id')}"
            )
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
                            "trigger_type": effective_trigger_type,
                            "trigger_entity": item.get("entity_key"),
                            "correlation_id": trigger_correlation_id,
                        }
                        if effective_trigger_type
                        else None
                    ),
                },
                pending_fields=list(item.get("fields_attempted") or []),
                specialized_topic=topic,
                execution_context=execution_context,
            )
            if created:
                jobs.append(job)
        return jobs

    def evaluate(
        self,
        trigger_name: str,
        *,
        force: bool = False,
        execution_context: ExecutionContext | None = None,
    ) -> dict[str, Any]:
        if not (
            self.settings.enable_scheduler
            and self.settings.research_scheduler_enabled
            and authorizes_ai(execution_context)
            and execution_context is not None
            and execution_context.request_origin == "research_scheduler"
        ):
            correlation_id = (
                execution_context.correlation_id
                if execution_context is not None
                else f"scheduler-{trigger_name}-suppressed"
            )
            self.telemetry.emit(
                "ai_authorization",
                identifiers={"correlation_id": correlation_id},
                decision_summary="AI_SUPPRESSED",
                stop_reason="AI_SUPPRESSED",
                payload={
                    "status": "AI_SUPPRESSED",
                    "reason": "scheduler_disabled_or_execution_context_invalid",
                },
            )
            return self._decision(
                trigger_name,
                "AI_SUPPRESSED",
                "NOT_REQUIRED",
                "AI_SUPPRESSED",
            )
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
        event_jobs = self._event_jobs(
            trigger_name,
            snapshot,
            execution_context=execution_context,
        )
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
                execution_context=execution_context,
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
            execution_context=execution_context,
        )
        return self._decision(
            trigger_name, fingerprint, "QUEUED" if created else "NOT_REQUIRED",
            "input_changed" if created else "run_window_already_evaluated", job_id=job["job_id"],
        )

    def _event_jobs(
        self,
        trigger_name: str,
        snapshot: dict[str, Any] | None,
        *,
        execution_context: ExecutionContext,
    ) -> list[dict[str, Any]] | None:
        if trigger_name not in {"pre_event", "post_release", "speech_outcome"}:
            return None
        events = _snapshot_events(snapshot)
        now = self.clock()
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
                eligible,
                correlation_id=f"scheduler-{trigger_name}-{uuid.uuid4()}",
                execution_context=execution_context,
            )
        states = [(event, temporal_event_state(event, now=now)) for event in events]
        target = "AWAITING_OUTCOME" if trigger_name == "speech_outcome" else "AWAITING_ACTUAL"
        eligible = [event for event, state in states if state["temporal_status"] == target]
        return self.service.enqueue_temporal_refreshes(
            eligible, correlation_id=f"scheduler-{trigger_name}-{uuid.uuid4()}", now=now,
            execution_context=execution_context,
        )

    def _scanner_ai_authorized(
        self,
        execution_context: ExecutionContext | None,
    ) -> bool:
        return bool(
            self.settings.enable_scheduler
            and self.settings.research_scheduler_enabled
            and self.settings.lifecycle_due_scanner_enabled
            and authorizes_ai(execution_context)
            and execution_context is not None
            and execution_context.request_origin in {"research_scheduler", "recovery"}
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


def _effective_trigger_type(
    item: dict[str, Any],
    *,
    explicit_trigger_type: str | None,
) -> str | None:
    if str(item.get("trigger_class") or "") == "NON_TRIGGERING":
        return None
    explicit = str(explicit_trigger_type or "").strip()
    if explicit:
        return explicit
    preserved = str(item.get("effective_trigger_type") or "").strip()
    if preserved:
        return preserved
    triggering_event = str(item.get("triggering_event") or "").strip()
    if triggering_event:
        return triggering_event
    if str(item.get("trigger_class") or "") == "TRIGGER":
        entity_type = str(item.get("entity_type") or "").strip()
        return entity_type or None
    return None


def _project_resolved_datum(
    debug: dict[str, Any],
    *,
    entity_type: str,
    entity_key: str,
    datum: dict[str, Any],
    lifecycle: dict[str, Any],
) -> dict[str, Any]:
    """Project one provider resolution into its existing consumer-facing block."""
    output = dict(debug)
    normalized = entity_type.lower()
    value = {**datum, "lifecycle": lifecycle}
    if normalized in {"vix", "vvix", "skew"}:
        risk = dict(output.get("risk_context") or {})
        risk[normalized] = value
        risk["status"] = "AVAILABLE"
        output["risk_context"] = risk
        return output
    if normalized in {"vix_futures", "put_call"}:
        risk = dict(output.get("risk_context") or {})
        risk[
            "vix_term_structure" if normalized == "vix_futures" else "put_call"
        ] = value
        risk["status"] = "AVAILABLE"
        output["risk_context"] = risk
        return output
    if normalized in {"cot", "cot_positioning", "cot_publication"}:
        positioning = dict(output.get("positioning") or {})
        positioning.update(value)
        positioning["status"] = str(value.get("status") or "AVAILABLE")
        output["positioning"] = positioning
        return output
    if normalized.startswith("earnings") and normalized != "earnings_intelligence":
        nasdaq = dict(output.get("nasdaq_context") or {})
        earnings = dict(nasdaq.get("earnings") or {})
        rows = list(
            earnings.get("events")
            or earnings.get("upcoming")
            or earnings.get("released_earnings")
            or []
        )
        matching = [
            index
            for index, row in enumerate(rows)
            if isinstance(row, dict)
            and _datum_entity_key(row) == entity_key.upper()
        ]
        if matching:
            rows[matching[0]] = value
        else:
            rows.append(value)
        earnings["events"] = rows
        earnings["status"] = "AVAILABLE"
        earnings["lifecycle"] = lifecycle
        nasdaq["earnings"] = earnings
        output["nasdaq_context"] = nasdaq
        return output
    if normalized in {"macro_actual", "macro_schedule"} or normalized.startswith(
        "fomc"
    ):
        calendar = dict(output.get("event_calendar") or {})
        bucket = (
            "fed_communications"
            if normalized.startswith("fomc")
            else "critical_macro_events"
        )
        rows = list(calendar.get(bucket) or [])
        matching = [
            index
            for index, row in enumerate(rows)
            if isinstance(row, dict)
            and str(
                row.get("canonical_event_key")
                or row.get("event_key")
                or row.get("event_id")
                or ""
            )
            == entity_key
        ]
        if matching:
            rows[matching[0]] = value
        else:
            rows.append(value)
        calendar[bucket] = rows
        output["event_calendar"] = calendar
        return output
    if normalized == "macro_snapshot":
        output["macro_snapshot"] = value
        return output
    if normalized == "fed_rates":
        output["rates_expectations"] = value
        return output
    if normalized in {"nasdaq_100", "mega_cap_semiconductors"}:
        nasdaq = dict(output.get("nasdaq_context") or {})
        key = (
            "qqq_holdings"
            if normalized == "nasdaq_100"
            else "mega_cap_snapshot"
        )
        nasdaq[key] = value
        nasdaq["status"] = "AVAILABLE"
        output["nasdaq_context"] = nasdaq
        return output
    if normalized in {"breaking_news", "news"}:
        news = dict(output.get("news_context") or {})
        rows = list(news.get("articles") or news.get("latest") or [])
        rows = [
            row
            for row in rows
            if not isinstance(row, dict)
            or str(
                row.get("news_key")
                or row.get("canonical_url")
                or row.get("url")
                or ""
            )
            != entity_key
        ]
        rows.append(value)
        news["articles"] = rows
        news["status"] = "AVAILABLE"
        output["news_context"] = news
        return output
    if normalized in TOPIC_PROFILES:
        output[normalized] = value
        return output
    return output


def _datum_entity_key(value: dict[str, Any]) -> str:
    issuer = str(
        value.get("ticker")
        or value.get("symbol")
        or value.get("issuer")
        or value.get("company")
        or ""
    ).upper()
    event_at = str(
        value.get("event_at")
        or value.get("earnings_date")
        or value.get("date")
        or ""
    )[:10]
    return f"{issuer}:{event_at}".strip(":")


def _created_job_count(value: Any) -> int:
    if isinstance(value, (list, tuple, set)):
        return len(value)
    if isinstance(value, dict):
        for key in ("jobs", "created_jobs", "items"):
            jobs = value.get(key)
            if isinstance(jobs, (list, tuple, set)):
                return len(jobs)
        if value.get("created") is True or value.get("job_id"):
            return 1
    return 0


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
