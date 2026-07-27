from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.models.events import EconomicEvent
from app.services.ai_research_job_repository import ACTIVE_JOB_STATUSES, AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.market_fact_repository import MarketFactRepository
from app.services.event_calendar_coverage_repository import (
    EventCalendarCoverageRepository,
)
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
from app.services.event_calendar_window_service import (
    classify_event_change,
    coalesce_event_changes,
)
from app.services.event_occurrence_lifecycle_service import (
    classify_occurrence_lifecycle,
)
from app.services.temporal_validation_service import TemporalPolicy
from app.services.research_agent_enablement import is_research_agent_enabled
from app.services.research_gap_manifest import TOPIC_PROFILES
from app.services.observability_contract_service import TelemetryRepository
from app.services.execution_context import ExecutionContext, authorizes_ai


logger = logging.getLogger(__name__)


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
        self.market_facts = MarketFactRepository(settings, clock=self.clock)
        self.calendar_coverage = EventCalendarCoverageRepository(
            settings,
            clock=self.clock,
        )
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
        limit: int | None = None,
        entity_types: set[str] | frozenset[str] | None = None,
        priority_since: datetime | None = None,
        allow_ai_residual: bool = True,
        coalesce_provider_resolutions: bool = False,
    ) -> dict[str, Any]:
        """Lease due work, run resolvers first, and enqueue AI once for residuals."""
        ai_authorized = (
            allow_ai_residual
            and self._scanner_ai_authorized(execution_context)
        )
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
            limit=limit,
            entity_types=entity_types,
            priority_since=priority_since,
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
        exhausted_no_data: list[str] = []
        item_outcomes: list[dict[str, str | None]] = []
        partial_actuals_recovered = 0
        partial_revisions_reconciled = 0
        provider_only_finalized: set[str] = set()
        effective_triggers: list[dict[str, str]] = []
        provider_resolution_batch: list[
            tuple[
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
                str | None,
            ]
        ] = []
        for item in claimed:
            item_id = str(item["item_id"])
            effective_trigger_type = _effective_trigger_type(
                item,
                explicit_trigger_type=trigger_type,
            )
            if (
                coalesce_provider_resolutions
                and not _inside_notification_horizon(
                    item,
                    now=now,
                    horizon_days=int(
                        self.settings.event_calendar_notification_horizon_days
                    ),
                )
            ):
                effective_trigger_type = None
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
                    prior_actual = (
                        (item.get("payload") or {}).get("actual")
                        if isinstance(item.get("payload"), dict)
                        else None
                    )
                    current_actual = datum.get("actual")
                    if prior_actual in (None, "") and current_actual not in (
                        None,
                        "",
                    ):
                        partial_actuals_recovered += 1
                    elif (
                        prior_actual not in (None, "")
                        and current_actual not in (None, "")
                        and str(prior_actual) != str(current_actual)
                    ):
                        partial_revisions_reconciled += 1
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
                    and allow_ai_residual
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
                    if coalesce_provider_resolutions:
                        provider_resolution_batch.append(
                            (
                                item,
                                datum,
                                lifecycle.as_dict(),
                                effective_trigger_type,
                            )
                        )
                    else:
                        snapshot = self._rematerialize_provider_resolution(
                            item=item,
                            datum=datum,
                            lifecycle=lifecycle.as_dict(),
                            trigger_type=effective_trigger_type,
                            owner=owner,
                            now=now,
                        )
                        if snapshot is not None:
                            rematerialized.append(
                                str(snapshot["snapshot_id"])
                            )
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
            if (
                provider_status in {"EXHAUSTED", "NOT_FOUND", "NO_DATA"}
                and not allow_ai_residual
            ):
                exhausted = bool(
                    provider_result.get("retry_deadline_exhausted")
                    or provider_status == "EXHAUSTED"
                )
                no_data_payload = {
                    **(
                        item.get("payload")
                        if isinstance(item.get("payload"), dict)
                        else {}
                    ),
                    "actual": (
                        (item.get("payload") or {}).get("actual")
                        if isinstance(item.get("payload"), dict)
                        else None
                    ),
                    "release_status": (
                        (item.get("payload") or {}).get("release_status")
                        if isinstance(item.get("payload"), dict)
                        else None
                    )
                    or "AWAITING_ACTUAL",
                    "reason": str(
                        provider_result.get("reason")
                        or "provider_returned_no_data"
                    ),
                    "fields_attempted": list(
                        item.get("fields_attempted") or []
                    ),
                }
                no_data_lifecycle = compute_datum_lifecycle(
                    str(item.get("entity_type") or "unknown"),
                    str(item.get("entity_key") or ""),
                    no_data_payload,
                    settings=self.settings,
                    now=now,
                    attempt_count=int(item.get("attempt_count") or 0) + 1,
                    no_data=not exhausted,
                    fields_attempted=list(
                        item.get("fields_attempted") or []
                    ),
                    session_state=item.get("session_state"),
                    triggering_event=effective_trigger_type,
                    retry_class="NO_DATA",
                    refresh_reason=str(
                        provider_result.get("reason")
                        or "provider_returned_no_data"
                    ),
                )
                work_status = "NO_DATA" if exhausted else "BACKOFF"
                self.lifecycle.upsert(
                    no_data_lifecycle,
                    payload=no_data_payload,
                    work_status=work_status,
                )
                residual.append(unresolved)
                if exhausted:
                    exhausted_no_data.append(item_id)
                    outcome = "EXHAUSTED_NO_DATA"
                else:
                    backoff.append(item_id)
                    deferred.append(item_id)
                    outcome = "WAITING_BACKOFF"
                item_outcomes.append(
                    {
                        "item_id": item_id,
                        "status": outcome,
                        "effective_trigger_type": effective_trigger_type,
                    }
                )
                provider_only_finalized.add(item_id)
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
                and allow_ai_residual
            ):
                ai_eligible.append(unresolved)
        if provider_resolution_batch:
            snapshot = self._rematerialize_provider_resolution_batch(
                resolutions=provider_resolution_batch,
                owner=owner,
                now=now,
            )
            if snapshot is not None:
                rematerialized.append(str(snapshot["snapshot_id"]))
            else:
                for _, datum, lifecycle, _ in provider_resolution_batch:
                    self.lifecycle.upsert(
                        DatumLifecycle(**lifecycle),
                        payload=datum,
                        work_status="COMPLETED",
                    )
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
            if (
                item_id in queued_item_ids
                or item_id in provider_only_finalized
            ):
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
        outcome_statuses = {
            str(item.get("status") or "") for item in item_outcomes
        }
        scan_status = (
            "PARTIAL"
            if len(outcome_statuses) > 1
            and outcome_statuses.difference({"RESOLVED"})
            else "WAITING_BACKOFF"
            if outcome_statuses.intersection({"BACKOFF", "WAITING_BACKOFF"})
            else "EXHAUSTED_NO_DATA"
            if outcome_statuses == {"EXHAUSTED_NO_DATA"}
            else "COMPLETED_WITH_GAPS"
            if outcome_statuses.intersection(
                {"NO_DATA", "DISABLED", "IDLE", "EXHAUSTED_NO_DATA"}
            )
            else "COMPLETED"
        )
        return {
            "status": scan_status,
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
            "exhausted_no_data": exhausted_no_data,
            "residual_count": len(residual),
            "ai_eligible_count": len(ai_eligible),
            "ai_decisions": ai_decisions,
            "item_outcomes": item_outcomes,
            "effective_triggers": effective_triggers,
            "ai_invocations": ai_invocations,
            "ai_jobs_created": ai_jobs_created,
            "enqueue_result": enqueue_result,
            "coalesced": len(ai_eligible) > 1,
            "provider_resolutions_coalesced": (
                len(provider_resolution_batch) > 1
            ),
            "lifecycle_writes": len(claimed),
            "snapshot_writes": len(rematerialized),
            "writes": len(claimed) + len(rematerialized),
            "actuals_recovered": partial_actuals_recovered + sum(
                1
                for item, datum, _, _ in provider_resolution_batch
                if (
                    (item.get("payload") or {}).get("actual")
                    if isinstance(item.get("payload"), dict)
                    else None
                )
                in (None, "")
                and datum.get("actual") not in (None, "")
            ),
            "revisions_reconciled": partial_revisions_reconciled + sum(
                1
                for item, datum, _, _ in provider_resolution_batch
                if (
                    (item.get("payload") or {}).get("actual")
                    if isinstance(item.get("payload"), dict)
                    else None
                )
                not in (None, "")
                and datum.get("actual") not in (None, "")
                and str((item.get("payload") or {}).get("actual"))
                != str(datum.get("actual"))
            ),
            "catch_up_cursor": (
                str(claimed[-1]["item_id"]) if claimed else None
            ),
        }

    def startup_catch_up(
        self,
        *,
        resolver: Callable[[dict[str, Any]], dict[str, Any]],
        ai_enqueue: Callable[[list[dict[str, Any]]], Any],
        schedule_acquire: Callable[..., Any] | None = None,
        execution_context: ExecutionContext | None = None,
    ) -> dict[str, Any]:
        event_calendar_catchup = bool(
            self.settings.event_calendar_catchup_enabled
        )
        if not event_calendar_catchup and not (
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
        if event_calendar_catchup:
            return self._event_calendar_catchup_tick(
                resolver=resolver,
                ai_enqueue=ai_enqueue,
                schedule_acquire=schedule_acquire,
                execution_context=execution_context,
            )
        now = self.clock()
        window_start = now - (
            timedelta(
                days=int(
                    self.settings.event_calendar_catchup_lookback_days
                )
            )
            if event_calendar_catchup
            else timedelta(
                hours=int(self.settings.lifecycle_startup_catchup_hours)
            )
        )
        event_entity_types = (
            frozenset(
                {
                    "macro_actual",
                    "earnings_actual",
                    "fomc_decision",
                    "fomc_communication",
                }
            )
            if event_calendar_catchup
            else None
        )
        priority_since = (
            now
            - timedelta(
                days=int(
                    self.settings.event_calendar_notification_horizon_days
                )
            )
            if event_calendar_catchup
            else None
        )
        backlog_before = self.lifecycle.count_due(
            now=now,
            due_since=window_start,
            entity_types=event_entity_types,
        )
        result = self.scan_due_items(
            owner="startup-lifecycle-catch-up",
            resolver=resolver,
            ai_enqueue=ai_enqueue,
            due_since=window_start,
            execution_context=execution_context,
            force=event_calendar_catchup,
            limit=(
                min(
                    int(self.settings.event_calendar_catchup_batch_size),
                    int(
                        self.settings.event_calendar_catchup_max_per_tick
                    ),
                )
                if event_calendar_catchup
                else None
            ),
            entity_types=event_entity_types,
            priority_since=priority_since,
            allow_ai_residual=not event_calendar_catchup,
            coalesce_provider_resolutions=event_calendar_catchup,
        )
        backlog_after = self.lifecycle.count_due(
            now=now,
            due_since=window_start,
            entity_types=event_entity_types,
        )
        catch_up_window_hours = (
            int(self.settings.lifecycle_startup_catchup_hours)
            if not event_calendar_catchup
            else int(self.settings.event_calendar_catchup_lookback_days)
            * 24
        )
        self.telemetry.emit(
            "startup_catch_up",
            identifiers={"correlation_id": "startup-lifecycle-catch-up"},
            decision_summary="bounded startup lifecycle reconciliation completed",
            stop_reason=str(result.get("status") or "COMPLETED"),
            payload={
                "status": str(result.get("status") or "COMPLETED"),
                "reason": (
                    f"window_hours:{catch_up_window_hours};"
                    f"claimed:{result.get('claimed', 0)};"
                    f"backlog_before:{backlog_before};"
                    f"backlog_after:{backlog_after};"
                    f"cursor:{result.get('catch_up_cursor') or 'none'}"
                ),
            },
        )
        return {
            **result,
            "catch_up_window_start": window_start.isoformat(),
            "catch_up_window_hours": catch_up_window_hours,
            "catch_up_backlog_before": backlog_before,
            "catch_up_backlog_after": backlog_after,
            "catch_up_batch_size": int(result.get("claimed") or 0),
            "catch_up_provider_only": event_calendar_catchup,
        }

    def _event_calendar_catchup_tick(
        self,
        *,
        resolver: Callable[[dict[str, Any]], dict[str, Any]],
        ai_enqueue: Callable[[list[dict[str, Any]]], Any],
        schedule_acquire: Callable[..., Any] | None,
        execution_context: ExecutionContext | None,
    ) -> dict[str, Any]:
        now = self.clock()
        window_start = now - timedelta(
            days=int(self.settings.event_calendar_catchup_lookback_days)
        )
        event_entity_types = frozenset(
            {
                "macro_actual",
                "earnings_actual",
                "fomc_decision",
                "fomc_communication",
            }
        )
        priority_since = now - timedelta(
            days=int(
                self.settings.event_calendar_notification_horizon_days
            )
        )
        checkpoint = self._read_event_calendar_catchup_checkpoint()
        preexisting_due = self.lifecycle.count_due(
            now=now,
            due_since=window_start,
            entity_types=event_entity_types,
        )
        (
            preexisting_pending_retry,
            preexisting_pending_backoff,
            preexisting_next_retry_at,
        ) = self._event_calendar_catchup_pending_state(
            now=now,
            window_start=window_start,
            entity_types=event_entity_types,
        )
        stable_early_backoff = (
            preexisting_due == 0
            and preexisting_pending_retry > 0
            and checkpoint.get("completion_status") == "WAITING_BACKOFF"
            and int(
                checkpoint.get(
                    "pending_retry",
                    checkpoint.get("pending_backoff") or 0,
                )
                or 0
            )
            == preexisting_pending_retry
            and checkpoint.get("next_retry_at")
            == preexisting_next_retry_at
        )
        if stable_early_backoff:
            source_coverage = (
                dict(checkpoint.get("source_coverage") or {})
                if isinstance(checkpoint.get("source_coverage"), dict)
                else {}
            )
            source_coverage.update(
                {
                    "provider_calls": 0,
                    "provider_calls_due": 0,
                    "provider_calls_executed": 0,
                    "canonical_writes": 0,
                    "coverage_metadata_writes": 0,
                    "snapshot_writes": 0,
                    "outbox_writes": 0,
                    "targeted_gap_dates": [],
                }
            )
            return {
                **_empty_catchup_result(),
                "status": "WAITING_BACKOFF",
                "checkpoint_written": False,
                "catch_up_window_start": window_start.isoformat(),
                "catch_up_window_hours": int(
                    self.settings.event_calendar_catchup_lookback_days
                )
                * 24,
                "catch_up_backlog_before": preexisting_pending_retry,
                "catch_up_backlog_after": preexisting_pending_retry,
                "catch_up_due_after": 0,
                "catch_up_pending_retry": preexisting_pending_retry,
                "catch_up_pending_backoff": preexisting_pending_backoff,
                "catch_up_next_retry_at": preexisting_next_retry_at,
                "catch_up_batch_size": int(
                    self.settings.event_calendar_catchup_batch_size
                ),
                "catch_up_max_per_tick": int(
                    self.settings.event_calendar_catchup_max_per_tick
                ),
                "catch_up_cursor": checkpoint.get("cursor"),
                "catch_up_tick_count": int(
                    checkpoint.get("tick_count") or 0
                ),
                "catch_up_completion_status": "WAITING_BACKOFF",
                "catch_up_provider_only": True,
                "source_coverage": source_coverage,
                "telemetry_emitted": False,
            }

        schedule_coverage = self._seed_canonical_schedule_gaps(
            schedule_acquire=schedule_acquire,
            now=now,
        )
        due_before = self.lifecycle.count_due(
            now=now,
            due_since=window_start,
            entity_types=event_entity_types,
        )
        (
            pending_retry_before,
            pending_backoff_before,
            next_retry_at_before,
        ) = self._event_calendar_catchup_pending_state(
            now=now,
            window_start=window_start,
            entity_types=event_entity_types,
        )
        schedule_lifecycle_writes = int(
            schedule_coverage.get("persisted_gap_count") or 0
        )
        schedule_snapshot_writes = int(
            bool(schedule_coverage.get("rematerialized_snapshot_id"))
        )
        if (
            due_before == 0
            and pending_retry_before == 0
            and schedule_lifecycle_writes == 0
            and schedule_snapshot_writes == 0
            and checkpoint.get("completion_status") == "COMPLETED"
        ):
            return {
                "status": "ALREADY_COMPLETE",
                "claimed": 0,
                "resolved": [],
                "backoff": [],
                "provider_calls": 0,
                "resolver_evaluations": 0,
                "actual_provider_requests": 0,
                "successful_provider_requests": 0,
                "failed_provider_requests": 0,
                "ai_invocations": 0,
                "ai_jobs_created": 0,
                "writes": 0,
                "lifecycle_writes": 0,
                "snapshot_writes": 0,
                "checkpoint_written": False,
                "catch_up_cursor": checkpoint.get("cursor"),
                "catch_up_backlog_before": 0,
                "catch_up_backlog_after": 0,
                "catch_up_due_after": 0,
                "catch_up_pending_retry": 0,
                "catch_up_pending_backoff": 0,
                "catch_up_next_retry_at": None,
                "catch_up_tick_count": int(
                    checkpoint.get("tick_count") or 0
                ),
                "catch_up_completion_status": "COMPLETED",
                "catch_up_provider_only": True,
                "source_coverage": schedule_coverage,
                "telemetry_emitted": False,
            }
        backlog_before = due_before + pending_retry_before
        if due_before == 0 and pending_retry_before > 0:
            transition_required = (
                checkpoint.get("completion_status") != "WAITING_BACKOFF"
                or int(
                    checkpoint.get(
                        "pending_retry",
                        checkpoint.get("pending_backoff") or 0,
                    )
                    or 0
                )
                != pending_retry_before
                or checkpoint.get("next_retry_at")
                != next_retry_at_before
                or schedule_lifecycle_writes > 0
                or schedule_snapshot_writes > 0
            )
            tick_count = int(checkpoint.get("tick_count") or 0)
            checkpoint_written = False
            telemetry_emitted = False
            if transition_required:
                tick_count += 1
                checkpoint_payload = {
                    "backlog_before": backlog_before,
                    "claimed": 0,
                    "resolved": 0,
                    "backoff": 0,
                    "backlog_after": pending_retry_before,
                    "due_after": 0,
                    "pending_retry": pending_retry_before,
                    "pending_backoff": pending_backoff_before,
                    "next_retry_at": next_retry_at_before,
                    "cursor": checkpoint.get("cursor"),
                    "tick_count": tick_count,
                    "completion_status": "WAITING_BACKOFF",
                    "source_coverage": schedule_coverage,
                    "updated_at": now.astimezone(UTC).replace(
                        microsecond=0
                    ).isoformat(),
                }
                self._write_event_calendar_catchup_checkpoint(
                    checkpoint_payload
                )
                self._emit_event_calendar_catchup_tick(
                    checkpoint_payload,
                    correlation_id=self._catchup_correlation_id(
                        execution_context
                    ),
                )
                checkpoint_written = True
                telemetry_emitted = True
            return {
                **_empty_catchup_result(),
                "status": "WAITING_BACKOFF",
                "lifecycle_writes": schedule_lifecycle_writes,
                "snapshot_writes": schedule_snapshot_writes,
                "writes": (
                    schedule_lifecycle_writes + schedule_snapshot_writes
                ),
                "rematerialized_snapshot_ids": (
                    [
                        str(schedule_coverage["rematerialized_snapshot_id"])
                    ]
                    if schedule_coverage.get("rematerialized_snapshot_id")
                    else []
                ),
                "checkpoint_written": checkpoint_written,
                "catch_up_window_start": window_start.isoformat(),
                "catch_up_window_hours": int(
                    self.settings.event_calendar_catchup_lookback_days
                )
                * 24,
                "catch_up_backlog_before": backlog_before,
                "catch_up_backlog_after": pending_retry_before,
                "catch_up_due_after": 0,
                "catch_up_pending_retry": pending_retry_before,
                "catch_up_pending_backoff": pending_backoff_before,
                "catch_up_next_retry_at": next_retry_at_before,
                "catch_up_batch_size": int(
                    self.settings.event_calendar_catchup_batch_size
                ),
                "catch_up_max_per_tick": int(
                    self.settings.event_calendar_catchup_max_per_tick
                ),
                "catch_up_cursor": checkpoint.get("cursor"),
                "catch_up_tick_count": tick_count,
                "catch_up_completion_status": "WAITING_BACKOFF",
                "catch_up_provider_only": True,
                "source_coverage": schedule_coverage,
                "telemetry_emitted": telemetry_emitted,
            }

        aggregate = _empty_catchup_result()
        max_per_tick = int(
            self.settings.event_calendar_catchup_max_per_tick
        )
        batch_size = int(
            self.settings.event_calendar_catchup_batch_size
        )
        cursor = checkpoint.get("cursor")
        due_after = due_before
        while (
            aggregate["claimed"] < max_per_tick
            and due_after > 0
        ):
            remaining = max_per_tick - int(aggregate["claimed"])
            batch = self.scan_due_items(
                owner="event-calendar-catch-up",
                resolver=resolver,
                ai_enqueue=None,
                due_since=window_start,
                execution_context=execution_context,
                force=True,
                limit=min(batch_size, remaining),
                entity_types=event_entity_types,
                priority_since=priority_since,
                allow_ai_residual=False,
                coalesce_provider_resolutions=True,
            )
            _accumulate_catchup_result(aggregate, batch)
            if batch.get("catch_up_cursor"):
                cursor = batch["catch_up_cursor"]
            due_after = self.lifecycle.count_due(
                now=now,
                due_since=window_start,
                entity_types=event_entity_types,
            )
            if int(batch.get("claimed") or 0) == 0:
                break

        (
            pending_retry,
            pending_backoff,
            next_retry_at,
        ) = self._event_calendar_catchup_pending_state(
            now=now,
            window_start=window_start,
            entity_types=event_entity_types,
        )
        (
            terminal_gap_count,
            exhausted_no_data_count,
        ) = self._event_calendar_catchup_terminal_gap_state(
            window_start=window_start,
            entity_types=event_entity_types,
        )
        backlog_after = due_after + pending_retry
        completion_status = (
            "IN_PROGRESS"
            if due_after > 0
            else "WAITING_BACKOFF"
            if pending_retry > 0
            else "PARTIAL"
            if aggregate["resolved"]
            and (
                int(aggregate["residual_count"]) > 0
                or terminal_gap_count > 0
            )
            else "EXHAUSTED_NO_DATA"
            if terminal_gap_count > 0
            and exhausted_no_data_count == terminal_gap_count
            else "COMPLETED_WITH_GAPS"
            if terminal_gap_count > 0
            or int(aggregate["residual_count"]) > 0
            else "COMPLETED"
        )
        tick_count = int(checkpoint.get("tick_count") or 0) + 1
        checkpoint_payload = {
            "backlog_before": backlog_before,
            "claimed": int(aggregate["claimed"]),
            "resolved": len(aggregate["resolved"]),
            "backoff": len(aggregate["backoff"]),
            "backlog_after": backlog_after,
            "due_after": due_after,
            "pending_retry": pending_retry,
            "pending_backoff": pending_backoff,
            "terminal_gap_count": terminal_gap_count,
            "exhausted_no_data_count": exhausted_no_data_count,
            "next_retry_at": next_retry_at,
            "cursor": cursor,
            "tick_count": tick_count,
            "completion_status": completion_status,
            "source_coverage": schedule_coverage,
            "updated_at": now.astimezone(UTC).replace(
                microsecond=0
            ).isoformat(),
        }
        self._write_event_calendar_catchup_checkpoint(checkpoint_payload)
        correlation_id = self._catchup_correlation_id(execution_context)
        self._emit_event_calendar_catchup_tick(
            checkpoint_payload,
            correlation_id=correlation_id,
        )
        logger.info(
            "event calendar catch-up checkpoint advanced; "
            "correlation_id=%s status=%s backlog_after=%s "
            "pending_backoff=%s next_retry_at=%s",
            correlation_id,
            completion_status,
            backlog_after,
            pending_backoff,
            next_retry_at,
        )
        return {
            **aggregate,
            "status": completion_status,
            "lifecycle_writes": (
                int(aggregate.get("lifecycle_writes") or 0)
                + schedule_lifecycle_writes
            ),
            "snapshot_writes": (
                int(aggregate.get("snapshot_writes") or 0)
                + schedule_snapshot_writes
            ),
            "writes": (
                int(aggregate.get("lifecycle_writes") or 0)
                + schedule_lifecycle_writes
                + int(aggregate.get("snapshot_writes") or 0)
                + schedule_snapshot_writes
            ),
            "rematerialized_snapshot_ids": [
                *(
                    [str(schedule_coverage["rematerialized_snapshot_id"])]
                    if schedule_coverage.get("rematerialized_snapshot_id")
                    else []
                ),
                *aggregate["rematerialized_snapshot_ids"],
            ],
            "checkpoint_written": True,
            "catch_up_window_start": window_start.isoformat(),
            "catch_up_window_hours": int(
                self.settings.event_calendar_catchup_lookback_days
            )
            * 24,
            "catch_up_backlog_before": backlog_before,
            "catch_up_backlog_after": backlog_after,
            "catch_up_due_after": due_after,
            "catch_up_pending_retry": pending_retry,
            "catch_up_pending_backoff": pending_backoff,
            "catch_up_terminal_gap_count": terminal_gap_count,
            "catch_up_exhausted_no_data_count": exhausted_no_data_count,
            "catch_up_next_retry_at": next_retry_at,
            "catch_up_batch_size": batch_size,
            "catch_up_max_per_tick": max_per_tick,
            "catch_up_cursor": cursor,
            "catch_up_tick_count": tick_count,
            "catch_up_completion_status": completion_status,
            "catch_up_provider_only": True,
            "source_coverage": schedule_coverage,
            "telemetry_emitted": True,
        }

    def _seed_canonical_schedule_gaps(
        self,
        *,
        schedule_acquire: Callable[..., Any] | None,
        now: datetime,
    ) -> dict[str, Any]:
        if schedule_acquire is None:
            return self._seed_canonical_schedule_gaps_unleased(
                schedule_acquire=None,
                now=now,
            )
        if not self._canonical_schedule_has_due_dates(now=now):
            backoff = self._canonical_schedule_backoff(now=now)
            if backoff is not None:
                return backoff
            return self._seed_canonical_schedule_gaps_unleased(
                schedule_acquire=schedule_acquire,
                now=now,
            )
        lease_owner = f"schedule-catchup-{uuid.uuid4()}"
        lease = self._acquire_schedule_seed_lease(
            owner=lease_owner,
            now=now,
        )
        if not lease["acquired"]:
            return {
                "status": str(lease["status"]),
                "reason": str(lease["reason"]),
                "provider_calls": 0,
                "persisted_gap_count": 0,
                "unchanged_occurrence_count": 0,
                "quarantined_occurrence_count": 0,
                "next_retry_at": lease.get("next_retry_at"),
            }
        try:
            result = self._seed_canonical_schedule_gaps_unleased(
                schedule_acquire=schedule_acquire,
                now=now,
            )
        except BaseException:
            self._complete_schedule_seed_lease(
                owner=lease_owner,
                now=now,
                status="PROVIDER_UNAVAILABLE",
                next_retry_at=(
                    now + timedelta(minutes=5)
                ).astimezone(UTC).isoformat(),
            )
            raise
        self._complete_schedule_seed_lease(
            owner=lease_owner,
            now=now,
            status=str(result["status"]),
            next_retry_at=result.get("next_retry_at"),
        )
        return result

    def _canonical_schedule_has_due_dates(
        self,
        *,
        now: datetime,
    ) -> bool:
        timezone = ZoneInfo(
            str(
                self.settings.event_calendar_timezone
                or "America/New_York"
            )
        )
        local_now = now.astimezone(timezone)
        current_start = local_now.date() - timedelta(
            days=local_now.weekday()
        )
        previous_start = current_start - timedelta(days=7)
        requested_days = [
            previous_start + timedelta(days=offset)
            for offset in range(21)
        ]
        return bool(
            self.calendar_coverage.missing_dates(
                requested_days,
                provider_name="economic_calendar_composite",
                query_scope="country=US",
                now=now,
                policy_version=(
                    self.market_facts.source_policy.policy_version
                ),
            )
        )

    def _canonical_schedule_backoff(
        self,
        *,
        now: datetime,
    ) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT next_retry_at FROM provider_state
                WHERE state_key='provider_first_schedule_catchup'
                """
            ).fetchone()
        retry_at = parse_datetime(
            row["next_retry_at"] if row is not None else None
        )
        if retry_at is None or retry_at <= now.astimezone(UTC):
            return None
        return {
            "acquired": False,
            "status": "PROVIDER_UNAVAILABLE",
            "reason": "persistent_provider_backoff",
            "provider_calls": 0,
            "persisted_gap_count": 0,
            "unchanged_occurrence_count": 0,
            "quarantined_occurrence_count": 0,
            "next_retry_at": retry_at.isoformat(),
        }

    def _seed_canonical_schedule_gaps_unleased(
        self,
        *,
        schedule_acquire: Callable[..., Any] | None,
        now: datetime,
    ) -> dict[str, Any]:
        timezone = ZoneInfo(
            str(
                self.settings.event_calendar_timezone
                or "America/New_York"
            )
        )
        local_now = now.astimezone(timezone)
        current_start = local_now.date() - timedelta(
            days=local_now.weekday()
        )
        previous_start = current_start - timedelta(days=7)
        window_start = datetime.combine(
            previous_start,
            datetime.min.time(),
            timezone,
        ).astimezone(UTC)
        next_end_date = current_start + timedelta(days=13)
        window_end = datetime.combine(
            next_end_date + timedelta(days=1),
            datetime.min.time(),
            timezone,
        ).astimezone(UTC)
        provider_name = "economic_calendar_composite"
        query_scope = "country=US"
        policy_version = self.market_facts.source_policy.policy_version
        requested_days = [
            previous_start + timedelta(days=offset)
            for offset in range((next_end_date - previous_start).days + 1)
        ]
        missing_days = self.calendar_coverage.missing_dates(
            requested_days,
            provider_name=provider_name,
            query_scope=query_scope,
            now=now,
            policy_version=policy_version,
        )
        coverage = {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "provider_calls": 0,
            "persisted_gap_count": 0,
            "unchanged_occurrence_count": 0,
            "quarantined_occurrence_count": 0,
            "by_bucket": {
                "PREVIOUS_WEEK": {
                    "status": "UNVERIFIED_EMPTY",
                    "candidate_count": 0,
                },
                "CURRENT_WEEK": {
                    "status": "UNVERIFIED_EMPTY",
                    "candidate_count": 0,
                },
                "NEXT_WEEK": {
                    "status": "UNVERIFIED_EMPTY",
                    "candidate_count": 0,
                },
            },
            "requested_dates": [
                day.isoformat() for day in requested_days
            ],
            "targeted_gap_dates": [
                day.isoformat() for day in missing_days
            ],
            "provider_calls_avoided": len(requested_days) - len(missing_days),
            "provider_calls_due": len(
                _contiguous_date_segments(
                    missing_days,
                    timezone=timezone,
                    upper_bound=window_end,
                )
            ),
            "provider_calls_executed": 0,
            "canonical_writes": 0,
            "coverage_metadata_writes": 0,
            "snapshot_writes": 0,
            "outbox_writes": 0,
        }
        if missing_days and schedule_acquire is None:
            coverage["status"] = "UNVERIFIED_EMPTY"
            coverage["reason"] = "schedule_acquirer_not_configured"
            return coverage
        rows: list[dict[str, Any]] = []
        provider_results: list[Any] = []
        provider_successes = 0
        failed_segments: list[tuple[datetime, datetime, str]] = []
        segment_proofs: list[
            tuple[datetime, datetime, dict[str, Any]]
        ] = []
        for segment_start, segment_end in _contiguous_date_segments(
            missing_days,
            timezone=timezone,
            upper_bound=window_end,
        ):
            coverage["provider_calls"] += 1
            coverage["provider_calls_executed"] += 1
            try:
                output = schedule_acquire(
                    country="US",
                    start=segment_start,
                    end=segment_end,
                    enrich=False,
                )
                if inspect.isawaitable(output):
                    output = asyncio.run(output)
            except Exception as exc:
                failed_segments.append(
                    (segment_start, segment_end, type(exc).__name__)
                )
                continue
            owner = getattr(schedule_acquire, "__self__", None) or schedule_acquire
            proof = _normalized_coverage_proof(
                getattr(owner, "last_coverage_proof", None)
                or getattr(owner, "coverage_proof", None)
            )
            segment_proofs.append((segment_start, segment_end, proof))
            segment_results = list(
                getattr(owner, "last_provider_results", []) or []
            )
            provider_results.extend(segment_results)
            segment_successes = sum(
                1
                for result in segment_results
                if not list(getattr(result, "errors", []) or [])
            )
            if segment_results and segment_successes == 0:
                failed_segments.append(
                    (
                        segment_start,
                        segment_end,
                        "all_schedule_providers_failed",
                    )
                )
                continue
            provider_successes += max(segment_successes, 1)
            rows.extend(
                (
                    item.model_dump(mode="json")
                    if hasattr(item, "model_dump")
                    else dict(item)
                )
                for item in (output or [])
                if hasattr(item, "model_dump") or isinstance(item, dict)
            )
        coverage["provider_result_count"] = len(provider_results)
        coverage["provider_success_count"] = provider_successes
        if missing_days and failed_segments and not rows:
            retry_at = (now + timedelta(minutes=5)).astimezone(UTC)
            for day in missing_days:
                day_start, day_end = _local_day_bounds(
                    day,
                    timezone,
                    window_end,
                )
                changed = self.calendar_coverage.record_day(
                    day,
                    provider_name=provider_name,
                    query_scope=query_scope,
                    window_start=day_start,
                    window_end=day_end,
                    status="PROVIDER_UNAVAILABLE",
                    record_count=0,
                    provider_called=True,
                    scope_verified=False,
                    proof={},
                    next_retry_at=retry_at,
                    lineage={"errors": [item[2] for item in failed_segments]},
                    policy_version=policy_version,
                )
                coverage["coverage_metadata_writes"] += int(changed)
            coverage["status"] = "PROVIDER_UNAVAILABLE"
            coverage["reason"] = failed_segments[0][2]
            coverage["next_retry_at"] = retry_at.isoformat()
            for bucket in coverage["by_bucket"].values():
                bucket["status"] = "PROVIDER_UNAVAILABLE"
            return coverage

        temporal_policy = TemporalPolicy(clock=lambda: now)
        existing_keys = {
            (str(item["entity_type"]), str(item["entity_key"]))
            for item in self.lifecycle.list_items()
        }
        persisted = 0
        unchanged = 0
        quarantined = 0
        quarantined_days: set[str] = set()
        discovered_occurrence_ids: list[str] = []
        discovered_rows: list[dict[str, Any]] = []
        discovered_lifecycles: list[
            tuple[DatumLifecycle, dict[str, Any], str]
        ] = []
        for payload in rows:
            release = parse_datetime(
                payload.get("release_at")
                or payload.get("time_utc")
                or payload.get("date")
            )
            if release is None:
                quarantined += 1
                continue
            release_local = release.astimezone(timezone)
            local_day_key = release_local.date().isoformat()
            bucket_name = (
                "PREVIOUS_WEEK"
                if previous_start
                <= release_local.date()
                < current_start
                else "CURRENT_WEEK"
                if current_start
                <= release_local.date()
                < current_start + timedelta(days=7)
                else "NEXT_WEEK"
                if current_start + timedelta(days=7)
                <= release_local.date()
                <= next_end_date
                else None
            )
            if bucket_name is None:
                continue
            coverage["by_bucket"][bucket_name]["candidate_count"] += 1
            decision = temporal_policy.evaluate(
                payload,
                domain="macro_calendar",
            )
            if not decision.accepted or not _schedule_record_complete(payload):
                quarantined += 1
                quarantined_days.add(local_day_key)
                coverage["by_bucket"][bucket_name]["status"] = "QUARANTINED"
                continue
            occurrence_key = str(
                payload.get("canonical_event_key")
                or payload.get("occurrence_id")
                or canonical_event_key(payload)
            )
            canonical_payload = {
                **payload,
                "canonical_event_key": occurrence_key,
                "occurrence_id": (
                    payload.get("occurrence_id") or occurrence_key
                ),
                "release_at": release.isoformat(),
            }
            classification = classify_occurrence_lifecycle(
                canonical_payload
            )
            entity_type = classification.entity_type
            lifecycle = compute_datum_lifecycle(
                entity_type,
                occurrence_key,
                canonical_payload,
                settings=self.settings,
                now=now,
                fields_attempted=list(classification.outcome_fields),
                triggering_event=entity_type,
                refresh_reason="provider_first_canonical_schedule_catchup",
            )
            existed = (entity_type, occurrence_key) in existing_keys
            actual_present = canonical_payload.get("actual") not in (None, "")
            work_status = (
                "COMPLETED"
                if actual_present or not classification.operational
                else "READY"
            )
            discovered_occurrence_ids.append(occurrence_key)
            discovered_rows.append(canonical_payload)
            if not existed:
                persisted += 1
                existing_keys.add((entity_type, occurrence_key))
                discovered_lifecycles.append(
                    (lifecycle, canonical_payload, work_status)
                )
            else:
                unchanged += 1
            wrote = self.market_facts.upsert_economic_event(
                canonical_payload,
                occurrence_key,
                valid_until=(
                    now + timedelta(days=35)
                ).astimezone(UTC).isoformat(),
            )
            coverage["canonical_writes"] += int(bool(wrote))
        rows_by_day: dict[str, int] = {}
        for payload in discovered_rows:
            release = parse_datetime(
                payload.get("release_at")
                or payload.get("time_utc")
                or payload.get("date")
            )
            if release is not None:
                local_day = release.astimezone(timezone).date().isoformat()
                rows_by_day[local_day] = rows_by_day.get(local_day, 0) + 1
        for day in missing_days:
            day_start, day_end = _local_day_bounds(
                day,
                timezone,
                window_end,
            )
            count = rows_by_day.get(day.isoformat(), 0)
            day_proof = next(
                (
                    proof
                    for start, end, proof in segment_proofs
                    if start
                    <= datetime.combine(
                        day,
                        datetime.min.time(),
                        timezone,
                    ).astimezone(UTC)
                    < end
                ),
                {},
            )
            if day.isoformat() in quarantined_days:
                day_proof = {
                    **day_proof,
                    "records_valid": False,
                    "authentic_empty": False,
                }
            positive_proof = _coverage_proof_complete(
                day_proof,
                empty=count == 0,
            )
            if (
                day > local_now.date()
                and count == 0
                and day.isoformat()
                not in set(day_proof.get("authentic_empty_dates") or [])
            ):
                positive_proof = False
            status = (
                "VERIFIED_COMPLETE"
                if count and positive_proof
                else "VERIFIED_EMPTY"
                if not count and positive_proof
                else "PARTIAL"
            )
            next_retry_at = (
                None
                if status in {"VERIFIED_COMPLETE", "VERIFIED_EMPTY"}
                else now + timedelta(hours=2)
            )
            changed = self.calendar_coverage.record_day(
                day,
                provider_name=provider_name,
                query_scope=query_scope,
                window_start=day_start,
                window_end=day_end,
                status=status,
                record_count=count,
                provider_called=True,
                scope_verified=bool(day_proof.get("scope_match")),
                proof=day_proof,
                valid_until=(
                    now + timedelta(hours=24)
                    if day >= local_now.date()
                    else now + timedelta(days=30)
                ),
                next_revision_check_at=(
                    now + timedelta(days=7)
                    if day < local_now.date()
                    else now + timedelta(hours=2)
                ),
                next_retry_at=next_retry_at,
                lineage={
                    "provider_result_count": len(provider_results),
                    "query_scope": query_scope,
                    "coverage_proof": day_proof,
                },
                policy_version=policy_version,
            )
            coverage["coverage_metadata_writes"] += int(changed)
        matrix = self.calendar_coverage.matrix(
            start_date=previous_start,
            end_date=next_end_date,
            provider_name=provider_name,
            query_scope=query_scope,
            now=now,
            policy_version=policy_version,
        )
        coverage["daily_matrix"] = matrix
        coverage["unknown_coverage_days"] = matrix[
            "unknown_coverage_days"
        ]
        coverage["partial_coverage_days"] = matrix[
            "partial_coverage_days"
        ]
        coverage["authoritative_dates"] = sorted(
            day
            for day, proof in matrix["by_date"].items()
            if proof["status"]
            in {"VERIFIED_COMPLETE", "VERIFIED_EMPTY"}
            and proof["provider_called"]
            and proof["scope_verified"]
        )
        for bucket_name, (first, last) in {
            "PREVIOUS_WEEK": (
                previous_start,
                current_start - timedelta(days=1),
            ),
            "CURRENT_WEEK": (
                current_start,
                current_start + timedelta(days=6),
            ),
            "NEXT_WEEK": (
                current_start + timedelta(days=7),
                next_end_date,
            ),
        }.items():
            statuses = [
                matrix["by_date"].get(
                    (first + timedelta(days=offset)).isoformat(),
                    {"status": "UNKNOWN"},
                )["status"]
                for offset in range((last - first).days + 1)
            ]
            coverage["by_bucket"][bucket_name]["status"] = (
                "VERIFIED_COMPLETE"
                if all(status in {"VERIFIED_COMPLETE", "VERIFIED_EMPTY"} for status in statuses)
                else "PARTIAL"
            )
        canonical_rows = self.market_facts.economic_event_payloads(
            country="US",
            start_date=previous_start.isoformat(),
            end_date=next_end_date.isoformat(),
        )
        canonical_keys = {
            str(
                item.get("canonical_event_key")
                or item.get("occurrence_id")
                or canonical_event_key(item)
            )
            for item in canonical_rows
        }
        for item in canonical_rows:
            item.setdefault(
                "occurrence_id",
                item.get("canonical_event_key")
                or canonical_event_key(item),
            )
        discovered_occurrence_ids = sorted(canonical_keys)
        coverage.update(
            {
                "status": (
                    "QUARANTINED"
                    if quarantined
                    else str(matrix["status"])
                ),
                "persisted_gap_count": persisted,
                "unchanged_occurrence_count": unchanged,
                "quarantined_occurrence_count": quarantined,
                "discovered_occurrence_ids": sorted(
                    set(discovered_occurrence_ids)
                ),
            }
        )
        rematerialized = self._rematerialize_schedule_discovery(
            rows=canonical_rows,
            lifecycles=discovered_lifecycles,
            source_coverage=coverage,
            now=now,
        )
        coverage["rematerialized_snapshot_id"] = (
            rematerialized.get("snapshot_id")
            if rematerialized is not None
            else None
        )
        coverage["snapshot_writes"] = int(rematerialized is not None)
        coverage["outbox_writes"] = int(rematerialized is not None)
        return coverage

    def _rematerialize_schedule_discovery(
        self,
        *,
        rows: list[dict[str, Any]],
        lifecycles: list[
            tuple[DatumLifecycle, dict[str, Any], str]
        ],
        source_coverage: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any] | None:
        """Atomically commit discovered schedule rows and their lifecycle state."""

        components = self.snapshots.latest_components("MNQ")
        previous = self.snapshots.latest("MNQ")
        if not components or previous is None:
            for lifecycle, payload, work_status in lifecycles:
                self.lifecycle.upsert(
                    lifecycle,
                    payload=payload,
                    work_status=work_status,
                )
            return None
        debug = dict(components)
        calendar = dict(debug.get("event_calendar") or {})
        section_names = (
            "critical_macro_events",
            "fed_communications",
            "other_economic_events",
        )
        existing: dict[str, dict[str, Any]] = {}
        section_by_key: dict[str, str] = {}
        calendar_changed = False
        for section_name in section_names:
            for item in calendar.get(section_name) or []:
                if not isinstance(item, dict):
                    continue
                key = str(
                    item.get("canonical_event_key")
                    or item.get("occurrence_id")
                    or canonical_event_key(item)
                )
                existing[key] = item
                section_by_key[key] = section_name
        for item in rows:
            key = str(
                item.get("canonical_event_key")
                or item.get("occurrence_id")
                or canonical_event_key(item)
            )
            merged = {**existing.get(key, {}), **item}
            if "removal_status" not in item:
                merged.pop("removal_status", None)
                merged.pop("comparison_lineage", None)
            if existing.get(key) != merged:
                calendar_changed = True
            existing[key] = merged
            if key not in section_by_key:
                category = str(
                    item.get("category")
                    or item.get("event_type")
                    or ""
                ).upper()
                section_by_key[key] = (
                    "fed_communications"
                    if "FOMC" in category or "FED" in category
                    else "critical_macro_events"
                    if str(item.get("impact") or "").upper() == "HIGH"
                    else "other_economic_events"
                )
                calendar_changed = True
        discovered_keys = {
            str(
                item.get("canonical_event_key")
                or item.get("occurrence_id")
                or canonical_event_key(item)
            )
            for item in rows
        }
        coverage_start = parse_datetime(source_coverage.get("window_start"))
        coverage_end = parse_datetime(source_coverage.get("window_end"))
        authoritative_dates = {
            str(item)
            for item in source_coverage.get("authoritative_dates") or []
            if item
        }
        calendar_timezone = ZoneInfo(
            str(
                self.settings.event_calendar_timezone
                or "America/New_York"
            )
        )
        unconfirmed_removals: list[str] = []
        lifecycle_keys = {
            (str(item["entity_type"]), str(item["entity_key"]))
            for item in self.lifecycle.list_items()
        }
        retained_actual_gaps = 0
        if (
            coverage_start is not None
            and coverage_end is not None
        ):
            for key, prior in list(existing.items()):
                release_at = parse_datetime(
                    prior.get("release_at")
                    or prior.get("time_utc")
                    or prior.get("date")
                )
                if (
                    key in discovered_keys
                    or release_at is None
                    or release_at < coverage_start
                    or release_at > coverage_end
                    or release_at.astimezone(
                        calendar_timezone
                    ).date().isoformat()
                    not in authoritative_dates
                ):
                    continue
                annotated = {
                    **prior,
                    "removal_status": "UNCONFIRMED_REMOVAL",
                    "comparison_lineage": {
                        "previous_source": (
                            prior.get("source") or prior.get("provider")
                        ),
                        "previous_source_domain": prior.get("source_domain"),
                        "confirmation_source": None,
                    },
                }
                unconfirmed_removals.append(key)
                if annotated != prior:
                    existing[key] = annotated
                    calendar_changed = True
                classification = classify_occurrence_lifecycle(annotated)
                lifecycle_key = (classification.entity_type, key)
                if (
                    classification.operational
                    and annotated.get("actual") in (None, "")
                    and lifecycle_key not in lifecycle_keys
                ):
                    lifecycles.append(
                        (
                            compute_datum_lifecycle(
                                classification.entity_type,
                                key,
                                annotated,
                                settings=self.settings,
                                now=now,
                                fields_attempted=list(
                                    classification.outcome_fields
                                ),
                                triggering_event=classification.entity_type,
                                refresh_reason=(
                                    "unconfirmed_removal_actual_catchup"
                                ),
                            ),
                            annotated,
                            "READY",
                        )
                    )
                    lifecycle_keys.add(lifecycle_key)
                    retained_actual_gaps += 1
        source_coverage["unconfirmed_removal_occurrence_ids"] = sorted(
            unconfirmed_removals
        )
        source_coverage["persisted_unconfirmed_actual_count"] = (
            retained_actual_gaps
        )
        source_coverage["persisted_gap_count"] = int(
            source_coverage.get("persisted_gap_count") or 0
        ) + retained_actual_gaps
        if not calendar_changed and not lifecycles:
            return None
        for section_name in section_names:
            calendar[section_name] = sorted(
                [
                    item
                    for key, item in existing.items()
                    if section_by_key.get(key) == section_name
                ],
                key=lambda item: (
                    str(
                        item.get("release_at")
                        or item.get("time_utc")
                        or item.get("date")
                        or ""
                    ),
                    str(
                        item.get("canonical_event_key")
                        or item.get("occurrence_id")
                        or ""
                    ),
                ),
            )
        previous_coverage = (
            dict(calendar.get("source_coverage") or {})
            if isinstance(calendar.get("source_coverage"), dict)
            else {}
        )
        previous_by_bucket = (
            dict(previous_coverage.get("by_bucket") or {})
            if isinstance(previous_coverage.get("by_bucket"), dict)
            else {}
        )
        discovered_by_bucket = (
            dict(source_coverage.get("by_bucket") or {})
            if isinstance(source_coverage.get("by_bucket"), dict)
            else {}
        )
        calendar["source_coverage"] = {
            **previous_coverage,
            **source_coverage,
            "by_bucket": {
                **previous_by_bucket,
                **discovered_by_bucket,
            },
        }
        debug["event_calendar"] = calendar
        debug["generated_at_utc"] = (
            now.astimezone(UTC).replace(microsecond=0).isoformat()
        )
        return self.snapshots.save_next(
            symbol="MNQ",
            refresh_mode="event_calendar_schedule_discovery",
            debug_payload=debug,
            ai_enrichment={"status": "NOT_REQUIRED"},
            trigger_type=None,
            correlation_id="event-calendar-schedule-discovery",
            resolved_items=lifecycles,
        )

    def _acquire_schedule_seed_lease(
        self,
        *,
        owner: str,
        now: datetime,
    ) -> dict[str, Any]:
        now_utc = now.astimezone(UTC)
        lease_until = now_utc + timedelta(minutes=5)
        state_key = "provider_first_schedule_catchup"
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT payload_json,next_retry_at
                FROM provider_state
                WHERE state_key=?
                """,
                (state_key,),
            ).fetchone()
            payload: dict[str, Any] = {}
            if row is not None:
                try:
                    payload = json.loads(row["payload_json"] or "{}")
                except (json.JSONDecodeError, TypeError):
                    payload = {}
                retry_at = parse_datetime(row["next_retry_at"])
                if retry_at is not None and retry_at > now_utc:
                    conn.commit()
                    return {
                        "acquired": False,
                        "status": "PROVIDER_UNAVAILABLE",
                        "reason": "persistent_provider_backoff",
                        "next_retry_at": retry_at.isoformat(),
                    }
                active_until = parse_datetime(payload.get("lease_until"))
                if (
                    payload.get("lease_owner")
                    and active_until is not None
                    and active_until > now_utc
                ):
                    conn.commit()
                    return {
                        "acquired": False,
                        "status": "PARTIAL",
                        "reason": "schedule_catchup_single_flight_active",
                        "next_retry_at": active_until.isoformat(),
                    }
            timestamp = now_utc.replace(microsecond=0).isoformat()
            lease_payload = {
                **payload,
                "lease_owner": owner,
                "lease_until": lease_until.isoformat(),
                "last_started_at": timestamp,
            }
            conn.execute(
                """
                INSERT INTO provider_state(
                  state_key,provider_name,state_type,status,reason,retryable,
                  next_retry_at,payload_json,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(state_key) DO UPDATE SET
                  status=excluded.status,
                  reason=excluded.reason,
                  retryable=excluded.retryable,
                  next_retry_at=NULL,
                  payload_json=excluded.payload_json,
                  updated_at=excluded.updated_at
                """,
                (
                    state_key,
                    "event_service",
                    "persistent_schedule_catchup_lease",
                    "LEASED",
                    "provider_first_canonical_schedule_catchup",
                    1,
                    None,
                    json.dumps(
                        lease_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()
        return {
            "acquired": True,
            "status": "LEASED",
            "reason": "schedule_catchup_lease_acquired",
        }

    def _complete_schedule_seed_lease(
        self,
        *,
        owner: str,
        now: datetime,
        status: str,
        next_retry_at: str | None,
    ) -> None:
        timestamp = now.astimezone(UTC).replace(
            microsecond=0
        ).isoformat()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT payload_json FROM provider_state
                WHERE state_key='provider_first_schedule_catchup'
                """
            ).fetchone()
            if row is None:
                conn.commit()
                return
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (json.JSONDecodeError, TypeError):
                payload = {}
            if payload.get("lease_owner") != owner:
                conn.commit()
                return
            payload.update(
                {
                    "lease_owner": None,
                    "lease_until": None,
                    "last_completed_at": timestamp,
                    "last_status": status,
                }
            )
            conn.execute(
                """
                UPDATE provider_state
                SET status=?,reason=?,retryable=?,next_retry_at=?,
                    payload_json=?,updated_at=?
                WHERE state_key='provider_first_schedule_catchup'
                """,
                (
                    status,
                    "provider_first_canonical_schedule_catchup",
                    int(next_retry_at is not None),
                    next_retry_at,
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    timestamp,
                ),
            )
            conn.commit()

    def _catchup_correlation_id(
        self,
        execution_context: ExecutionContext | None,
    ) -> str:
        return str(
            getattr(execution_context, "correlation_id", None)
            or "event-calendar-catch-up"
        )

    def _emit_event_calendar_catchup_tick(
        self,
        checkpoint_payload: dict[str, Any],
        *,
        correlation_id: str,
    ) -> None:
        telemetry_reason = ";".join(
            f"{key}:{checkpoint_payload.get(key)}"
            for key in (
                "backlog_before",
                "claimed",
                "resolved",
                "backoff",
                "backlog_after",
                "pending_backoff",
                "next_retry_at",
                "cursor",
                "tick_count",
                "completion_status",
            )
        )
        completion_status = str(
            checkpoint_payload["completion_status"]
        )
        self.telemetry.emit(
            "startup_catch_up",
            identifiers={"correlation_id": correlation_id},
            decision_summary=(
                "persistent provider-only event calendar catch-up tick completed"
            ),
            stop_reason=completion_status,
            payload={
                "status": completion_status,
                "reason": telemetry_reason,
                **checkpoint_payload,
            },
        )

    def record_event_calendar_catchup_error(
        self,
        *,
        correlation_id: str,
        error: Exception,
        retry_delay_seconds: int,
    ) -> str:
        checkpoint = self._read_event_calendar_catchup_checkpoint()
        catch_up_status = str(
            checkpoint.get("completion_status") or "IN_PROGRESS"
        )
        error_type = type(error).__name__
        self.telemetry.emit(
            "startup_catch_up",
            identifiers={"correlation_id": correlation_id},
            decision_summary=(
                "transient event calendar catch-up tick failed; retry scheduled"
            ),
            stop_reason="TRANSIENT_ERROR",
            error=f"{error_type}: {error}",
            payload={
                "status": catch_up_status,
                "reason": "transient_tick_error",
                "catch_up_completion_status": catch_up_status,
                "error_type": error_type,
                "retry_delay_seconds": retry_delay_seconds,
            },
        )
        return catch_up_status

    def _read_event_calendar_catchup_checkpoint(self) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT payload_json FROM provider_state
                WHERE state_key='event_calendar_lifecycle_catchup'
                """
            ).fetchone()
        if row is None:
            return {}
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except (json.JSONDecodeError, TypeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _write_event_calendar_catchup_checkpoint(
        self,
        payload: dict[str, Any],
    ) -> None:
        timestamp = str(payload["updated_at"])
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_state(
                  state_key,provider_name,state_type,status,reason,retryable,
                  next_retry_at,payload_json,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(state_key) DO UPDATE SET
                  status=excluded.status,
                  reason=excluded.reason,
                  retryable=excluded.retryable,
                  next_retry_at=excluded.next_retry_at,
                  payload_json=excluded.payload_json,
                  updated_at=excluded.updated_at
                """,
                (
                    "event_calendar_lifecycle_catchup",
                    "deterministic_lifecycle_due_resolver",
                    "persistent_catchup_checkpoint",
                    str(payload["completion_status"]),
                    "provider_only_bounded_tick",
                    int(payload["completion_status"] == "WAITING_BACKOFF"),
                    payload.get("next_retry_at"),
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()

    def _event_calendar_catchup_pending_state(
        self,
        *,
        now: datetime,
        window_start: datetime,
        entity_types: frozenset[str],
    ) -> tuple[int, int, str | None]:
        placeholders = ",".join("?" for _ in entity_types)
        reference = now.astimezone(UTC).replace(microsecond=0).isoformat()
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS pending_count,
                       COALESCE(SUM(
                         CASE WHEN work_status='BACKOFF' THEN 1 ELSE 0 END
                       ),0) AS pending_backoff_count,
                       MIN(
                         CASE
                           WHEN work_status='LEASED' THEN lease_expires_at
                           ELSE COALESCE(next_retry_at,next_refresh_at)
                         END
                       ) AS next_retry_at
                FROM datum_lifecycle_items
                WHERE (
                    work_status IN ('BACKOFF','LEASED')
                    OR (
                      work_status='IDLE'
                      AND refresh_reason='provider_unresolved_ai_not_configured'
                    )
                  )
                  AND entity_type IN ({placeholders})
                  AND (
                    CASE
                      WHEN work_status='LEASED' THEN lease_expires_at
                      ELSE COALESCE(next_retry_at,next_refresh_at)
                    END
                  )>?
                  AND COALESCE(event_at,updated_at)>=?
                """,
                (
                    *sorted(entity_types),
                    reference,
                    window_start.astimezone(UTC).replace(
                        microsecond=0
                    ).isoformat(),
                ),
            ).fetchone()
        return (
            int(row["pending_count"] if row else 0),
            int(row["pending_backoff_count"] if row else 0),
            str(row["next_retry_at"])
            if row is not None and row["next_retry_at"]
            else None,
        )

    def _event_calendar_catchup_terminal_gap_state(
        self,
        *,
        window_start: datetime,
        entity_types: frozenset[str],
    ) -> tuple[int, int]:
        placeholders = ",".join("?" for _ in entity_types)
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS terminal_gap_count,
                       COALESCE(SUM(
                         CASE WHEN work_status='NO_DATA' THEN 1 ELSE 0 END
                       ),0) AS exhausted_no_data_count
                FROM datum_lifecycle_items
                WHERE work_status IN ('DISABLED','NO_DATA')
                  AND entity_type IN ({placeholders})
                  AND COALESCE(event_at,updated_at)>=?
                """,
                (
                    *sorted(entity_types),
                    window_start.astimezone(UTC).replace(
                        microsecond=0
                    ).isoformat(),
                ),
            ).fetchone()
        return (
            int(row["terminal_gap_count"] if row else 0),
            int(row["exhausted_no_data_count"] if row else 0),
        )

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

    def _rematerialize_provider_resolution_batch(
        self,
        *,
        resolutions: list[
            tuple[
                dict[str, Any],
                dict[str, Any],
                dict[str, Any],
                str | None,
            ]
        ],
        owner: str,
        now: datetime,
    ) -> dict[str, Any] | None:
        """Commit one snapshot/outbox envelope for a bounded catch-up batch."""

        previous = self.snapshots.latest("MNQ")
        if previous is None:
            return None
        components = self.snapshots.latest_components("MNQ")
        if not components:
            return None
        debug = dict(components)
        changes: list[dict[str, Any]] = []
        persisted: list[
            tuple[DatumLifecycle, dict[str, Any], str]
        ] = []
        trigger_types: list[str] = []
        lifecycle_resolutions = dict(
            debug.get("lifecycle_resolutions") or {}
        )
        for item, datum, lifecycle, trigger_type in resolutions:
            entity_key = str(item.get("entity_key") or "")
            debug = _project_resolved_datum(
                debug,
                entity_type=str(item.get("entity_type") or ""),
                entity_key=entity_key,
                datum=datum,
                lifecycle=lifecycle,
            )
            lifecycle_resolutions[entity_key or str(item["item_id"])] = {
                "entity_type": item.get("entity_type"),
                "value": datum,
                "lifecycle": lifecycle,
            }
            persisted.append(
                (DatumLifecycle(**lifecycle), datum, "COMPLETED")
            )
            if trigger_type:
                trigger_types.append(trigger_type)
                previous_occurrence = {
                    **(
                        item.get("payload")
                        if isinstance(item.get("payload"), dict)
                        else {}
                    ),
                    "occurrence_id": entity_key,
                }
                current_occurrence = {
                    **datum,
                    "occurrence_id": entity_key,
                    "release_status": (
                        datum.get("release_status")
                        or (
                            "PUBLISHED"
                            if datum.get("actual") not in (None, "")
                            else None
                        )
                    ),
                    "is_future": False,
                }
                changes.append(
                    classify_event_change(
                        previous_occurrence,
                        current_occurrence,
                        consensus_trigger_enabled=(
                            self.settings
                            .event_calendar_consensus_trigger_enabled
                        ),
                    )
                )
        debug["lifecycle_resolutions"] = lifecycle_resolutions
        trigger_metadata = coalesce_event_changes(changes)
        debug["event_change_batch"] = {
            **trigger_metadata,
            "batch_size": len(resolutions),
            "provider_first": True,
            "ai_invocations": 0,
            "delivery_attempted": False,
        }
        debug["generated_at_utc"] = now.astimezone(UTC).replace(
            microsecond=0
        ).isoformat()
        coalesced_trigger = (
            trigger_types[0]
            if trigger_metadata["trigger_class"] == "TRIGGERING"
            else None
        )
        if (
            self.deterministic_runtime is not None
            and coalesced_trigger is not None
        ):
            debug = self.deterministic_runtime.enrich_market_context_sync(
                debug,
                refresh="auto",
                trigger_type=coalesced_trigger,
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
            return None
        snapshot = self.snapshots.save_next(
            symbol="MNQ",
            refresh_mode="event_calendar_catchup_batch",
            debug_payload=debug,
            ai_enrichment={"status": "NOT_REQUIRED"},
            trigger_type=coalesced_trigger,
            trigger_entity=(
                ",".join(trigger_metadata["changed_event_ids"])
                if coalesced_trigger
                else None
            ),
            correlation_id=owner,
            resolved_items=persisted,
            trigger_metadata=trigger_metadata,
        )
        self.telemetry.emit(
            "event_trigger_batch",
            identifiers={
                "correlation_id": owner,
                "snapshot_id": snapshot.get("snapshot_id"),
            },
            decision_summary=(
                "provider resolutions committed in one coalesced batch"
            ),
            stop_reason=(
                "TRIGGERING"
                if coalesced_trigger
                else "ARCHIVED_WITHOUT_NOTIFICATION"
            ),
            payload={
                "status": "COMPLETED",
                "reason": (
                    f"batch_size:{len(resolutions)};"
                    f"trigger_count:{trigger_metadata['trigger_count']}"
                ),
            },
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
            and authorizes_ai(
                execution_context,
                environment=self.settings.environment,
            )
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
            and authorizes_ai(
                execution_context,
                environment=self.settings.environment,
            )
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


def _contiguous_date_segments(
    days: list[Any],
    *,
    timezone: ZoneInfo,
    upper_bound: datetime,
) -> list[tuple[datetime, datetime]]:
    if not days:
        return []
    segments: list[tuple[Any, Any]] = []
    start = previous = days[0]
    for day in days[1:]:
        if day != previous + timedelta(days=1):
            segments.append((start, previous))
            start = day
        previous = day
    segments.append((start, previous))
    return [
        (
            datetime.combine(
                first,
                datetime.min.time(),
                timezone,
            ).astimezone(UTC),
            min(
                datetime.combine(
                    last + timedelta(days=1),
                    datetime.min.time(),
                    timezone,
                ).astimezone(UTC),
                upper_bound.astimezone(UTC),
            ),
        )
        for first, last in segments
    ]


def _normalized_coverage_proof(value: Any) -> dict[str, Any]:
    source = dict(value) if isinstance(value, dict) else {}
    proof: dict[str, Any] = {
        key: bool(source.get(key))
        for key in (
            "request_succeeded",
            "scope_match",
            "pagination_complete",
            "parsing_succeeded",
            "records_valid",
            "expected_sources_complete",
            "authentic_empty",
        )
    }
    proof["authentic_empty_dates"] = sorted(
        {
            str(item)
            for item in source.get("authentic_empty_dates") or []
            if item
        }
    )
    return proof


def _schedule_record_complete(payload: dict[str, Any]) -> bool:
    return bool(
        (payload.get("name") or payload.get("event_name") or payload.get("title"))
        and payload.get("country")
        and (payload.get("source") or payload.get("provider"))
        and payload.get("source_url")
    )


def _coverage_proof_complete(
    proof: dict[str, Any],
    *,
    empty: bool,
) -> bool:
    required = (
        "request_succeeded",
        "scope_match",
        "pagination_complete",
        "parsing_succeeded",
        "records_valid",
        "expected_sources_complete",
    )
    return all(proof.get(key) for key in required) and (
        not empty or bool(proof.get("authentic_empty"))
    )


def _local_day_bounds(
    day: Any,
    timezone: ZoneInfo,
    upper_bound: datetime,
) -> tuple[datetime, datetime]:
    start = datetime.combine(
        day,
        datetime.min.time(),
        timezone,
    ).astimezone(UTC)
    end = min(
        datetime.combine(
            day + timedelta(days=1),
            datetime.min.time(),
            timezone,
        ).astimezone(UTC),
        upper_bound.astimezone(UTC),
    )
    return start, end


def _empty_catchup_result() -> dict[str, Any]:
    return {
        "claimed": 0,
        "provider_calls": 0,
        "resolver_evaluations": 0,
        "committed_payload_hits": 0,
        "actual_provider_requests": 0,
        "successful_provider_requests": 0,
        "failed_provider_requests": 0,
        "resolved": [],
        "rematerialized_snapshot_ids": [],
        "deferred": [],
        "backoff": [],
        "exhausted_no_data": [],
        "item_outcomes": [],
        "effective_triggers": [],
        "residual_count": 0,
        "ai_eligible_count": 0,
        "ai_decisions": [],
        "ai_invocations": 0,
        "ai_jobs_created": 0,
        "provider_resolutions_coalesced": False,
        "lifecycle_writes": 0,
        "snapshot_writes": 0,
        "writes": 0,
        "actuals_recovered": 0,
        "revisions_reconciled": 0,
    }


def _accumulate_catchup_result(
    aggregate: dict[str, Any],
    batch: dict[str, Any],
) -> None:
    for field in (
        "claimed",
        "provider_calls",
        "resolver_evaluations",
        "committed_payload_hits",
        "actual_provider_requests",
        "successful_provider_requests",
        "failed_provider_requests",
        "residual_count",
        "ai_eligible_count",
        "ai_invocations",
        "ai_jobs_created",
        "actuals_recovered",
        "revisions_reconciled",
        "lifecycle_writes",
        "snapshot_writes",
        "writes",
    ):
        aggregate[field] = int(aggregate.get(field) or 0) + int(
            batch.get(field) or 0
        )
    for field in (
        "resolved",
        "rematerialized_snapshot_ids",
        "deferred",
        "backoff",
        "exhausted_no_data",
        "item_outcomes",
        "effective_triggers",
        "ai_decisions",
    ):
        aggregate[field].extend(list(batch.get(field) or []))
    aggregate["provider_resolutions_coalesced"] = bool(
        aggregate["provider_resolutions_coalesced"]
        or batch.get("provider_resolutions_coalesced")
    )


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


def _inside_notification_horizon(
    item: dict[str, Any],
    *,
    now: datetime,
    horizon_days: int,
) -> bool:
    event_at = parse_datetime(
        item.get("event_at")
        or (
            (item.get("payload") or {}).get("release_at")
            if isinstance(item.get("payload"), dict)
            else None
        )
    )
    if event_at is None:
        return False
    return event_at >= now - timedelta(days=max(int(horizon_days), 1))


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
