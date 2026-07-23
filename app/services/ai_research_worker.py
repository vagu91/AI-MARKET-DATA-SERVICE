from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
import traceback
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from app.core.config import Settings
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.market_fact_repository import MarketFactRepository
from app.services.source_policy_service import SourcePolicyService
from app.services.deterministic_actual_resolver import DeterministicActualResolver
from app.services.event_value_candidate_repository import EventValueCandidateRepository
from app.services.agentic_research_runtime import AgenticResearchRuntime
from app.services.ai_research_capability_service import AIResearchCapabilityService
from app.services.db_only_market_context_materializer import DBOnlyMarketContextMaterializer
from app.services.codex_runtime_contract import classify_codex_failure, sanitize_diagnostic
from app.services.research_backend import ResearchBackend, select_research_backend
from app.services.parallel_research_coordinator import ParallelResearchCoordinator


logger = logging.getLogger(__name__)


class AIResearchWorker:
    def __init__(
        self,
        settings: Settings,
        *,
        repository: AIResearchJobRepository | None = None,
        executor: Callable[[dict[str, Any], Path, int], dict[str, Any]] | None = None,
        actual_resolver: Callable[[dict[str, Any], Path, int], dict[str, Any]] | None = None,
        source_policy: SourcePolicyService | None = None,
        facts: MarketFactRepository | None = None,
        snapshots: MarketContextSnapshotRepository | None = None,
        agentic_runtime: AgenticResearchRuntime | None = None,
        capabilities: AIResearchCapabilityService | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository or AIResearchJobRepository(settings)
        self.executor = executor or select_research_backend(settings)
        self.actual_resolver = actual_resolver or DeterministicActualResolver(settings)
        self.source_policy = source_policy or SourcePolicyService(settings.source_policy_path)
        self.facts = facts or MarketFactRepository(settings)
        self.snapshots = snapshots or MarketContextSnapshotRepository(settings)
        self.candidates = EventValueCandidateRepository(settings, policy=self.source_policy)
        self.materializer = DBOnlyMarketContextMaterializer(settings, facts=self.facts, snapshots=self.snapshots)
        self.agentic_runtime = agentic_runtime or AgenticResearchRuntime(settings)
        self.capabilities = capabilities or AIResearchCapabilityService(settings)
        self.parallel_coordinator = ParallelResearchCoordinator(settings)
        self._capability_report: dict[str, Any] | None = None
        self._capability_lock = threading.Lock()
        self.worker_id = worker_id or f"ai-worker-{uuid.uuid4()}"
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        recovered = self.repository.recover_abandoned()
        if recovered:
            logger.info("ai_job_recovered_after_restart", extra={"job_id": None, "correlation_id": None, "count": recovered})
        runtime_repository = getattr(self.agentic_runtime, "repository", None)
        reconcile = getattr(runtime_repository, "reconcile_terminal_jobs", None)
        reconciled = int(reconcile()) if callable(reconcile) else 0
        if reconciled:
            logger.warning(
                "research_terminal_state_reconciled",
                extra={"job_id": None, "correlation_id": None, "count": reconciled},
            )
        while not self._stopping.is_set():
            processed = await asyncio.gather(
                *[
                    asyncio.to_thread(self.process_once)
                    for _ in range(int(self.settings.research_parallelism))
                ]
            )
            if not any(processed):
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=self.settings.ai_worker_poll_seconds)
                except TimeoutError:
                    pass

    def stop(self) -> None:
        self._stopping.set()
        cancel = getattr(self.executor, "cancel_all", None)
        if callable(cancel):
            cancel()

    def process_once(self) -> bool:
        allowed_job_types = None
        authorized_smoke_only = False
        if (
            getattr(self.executor, "backend_name", None) == "codex_cli"
            and hasattr(self.executor, "execute_step")
        ):
            if self._capability_report is None:
                with self._capability_lock:
                    if self._capability_report is None:
                        self._capability_report = self.capabilities.probe(persist=True)
            capability_status = str(self._capability_report["status"])
            if capability_status == "READY_TO_SMOKE":
                authorized_smoke_only = True
            elif capability_status != "LIVE_VERIFIED":
                allowed_job_types = ["RELEASE_ACTUAL_REFRESH"]
                self.repository.mark_pending_capability(
                    str(self._capability_report["status"]), excluded_job_types=allowed_job_types
                )
        job = self.repository.acquire_next(
            self.worker_id,
            allowed_job_types=allowed_job_types,
            authorized_smoke_only=authorized_smoke_only,
        )
        if job is None:
            return False
        context = {"job_id": job["job_id"], "correlation_id": job["correlation_id"]}
        logger.info("ai_job_lease_acquired", extra=context)
        workspace = Path(self.settings.ai_job_workspace_root) / str(job["job_id"])
        workspace.mkdir(parents=True, exist_ok=True)
        stop_heartbeat = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(str(job["job_id"]), stop_heartbeat),
            daemon=True,
        )
        heartbeat.start()
        try:
            logger.info("ai_job_provider_started", extra=context)
            if job["job_type"] == "RELEASE_ACTUAL_REFRESH":
                result = self.actual_resolver(job, workspace, self.settings.ai_job_max_runtime_seconds)
            elif isinstance(self.executor, ResearchBackend) or hasattr(
                self.executor, "execute_step"
            ):
                result = self.agentic_runtime.run(
                    job, workspace, self.executor, self.settings.ai_job_max_runtime_seconds
                )
            else:
                result = self.executor(job, workspace, self.settings.ai_job_max_runtime_seconds)
            logger.info("ai_job_validation_started", extra=context)
            accepted, rejected = self._validate_results(job, result)
            status = str(result.get("status") or "FAILED").upper()
            if status == "CHECKPOINTED":
                updated = self.repository.checkpoint(
                    job["job_id"],
                    self.worker_id,
                    checkpoint=result.get("checkpoint") or {},
                    workspace_path=str(workspace),
                )
                logger.info(
                    "ai_job_checkpointed",
                    extra={
                        **context,
                        "run_id": result.get("run_id"),
                        "status": updated["status"],
                    },
                )
                return True
            if status == "TIMED_OUT":
                updated = self.repository.retry_or_fail(
                    job["job_id"], self.worker_id, error=str(result.get("error") or "watchdog_timeout"),
                    timed_out=True, delays=self._retry_delays(job),
                    retryable=bool(result.get("retryable")),
                )
                logger.warning(
                    "ai_job_timed_out",
                    extra={
                        **context,
                        "run_id": result.get("run_id"),
                        "error_code": result.get("error"),
                        "exit_code": None,
                        "step": result.get("deadline_step"),
                        "retry_classification": result.get(
                            "retry_classification", "NON_RETRYABLE"
                        ),
                        "status": updated["status"],
                    },
                )
                self._reconcile_terminal_parent(job, updated)
                return True
            if status in {"FAILED"}:
                updated = self.repository.retry_or_fail(
                    job["job_id"], self.worker_id, error=str(result.get("error") or "provider_failed"),
                    delays=self._retry_delays(job),
                    retryable=bool(result.get("retryable")),
                    diagnostic=result.get("diagnostic"),
                )
                logger.warning(
                    "ai_job_provider_failed",
                    extra={
                        **context,
                        "run_id": result.get("run_id"),
                        "error_code": result.get("error"),
                        "exit_code": (result.get("diagnostic") or {}).get("exit_code"),
                        "step": (result.get("diagnostic") or {}).get("step"),
                        "retry_classification": (result.get("diagnostic") or {}).get(
                            "retry_classification", "NON_RETRYABLE"
                        ),
                        "status": updated["status"],
                    },
                )
                self._reconcile_terminal_parent(job, updated)
                return True
            if status in {"NO_DATA", "OFFICIAL_FEED_DELAYED"} and job["job_type"] == "RELEASE_ACTUAL_REFRESH":
                retryable = status == "OFFICIAL_FEED_DELAYED" or result.get("retryable") is True
                deadline_open = self._official_retry_deadline_open(job)
                if not retryable or not deadline_open:
                    self.facts.mark_event_actual_unavailable(str(job.get("event_key") or ""))
                    completed = self.repository.complete(
                        job["job_id"], self.worker_id, status="NO_DATA",
                        result_payload=result, rejected_fields=[], accepted_fields=[],
                        workspace_path=str(workspace),
                        error=(
                            "official_feed_delay_deadline_expired" if retryable
                            else str(result.get("error") or "official_actual_not_available")
                        ),
                    )
                    self.materializer.materialize_for_job(
                        job=completed, ai_enrichment=_job_enrichment(completed)
                    )
                    logger.info("ai_job_completed", extra={**context, "status": "NO_DATA"})
                else:
                    self.repository.retry_or_fail(
                        job["job_id"], self.worker_id,
                        error=f"OFFICIAL_FEED_DELAYED:{result.get('error') or 'official_actual_not_yet_available'}",
                        delays=self._retry_delays(job),
                    )
                    logger.info("ai_job_retry_scheduled", extra=context)
                return True
            runtime_managed = bool(result.get("run_id"))
            runtime_projection_complete = (
                runtime_managed
                and job.get("job_type") == "MNQ_MARKET_RESEARCH"
            )
            persistence = (
                {
                    "persisted_count": int(result.get("persisted_count") or 0),
                    "read_back_count": int(result.get("read_back_count") or 0),
                }
                if runtime_projection_complete
                else self._persist_accepted(job, accepted)
            )
            accepted_count = int(result.get("accepted_count") or len(accepted)) if runtime_managed else len(accepted)
            if runtime_managed and (
                int(result.get("persisted_count") or 0) != accepted_count
                or int(result.get("read_back_count") or 0) != accepted_count
            ):
                raise RuntimeError("accepted research claims were not fully persisted and read back")
            if accepted and not runtime_projection_complete and (
                persistence["persisted_count"] != len(accepted)
                or persistence["read_back_count"] != len(accepted)
            ):
                raise RuntimeError("accepted AI results were not fully persisted and read back")
            terminal = (
                "PARTIAL" if status == "PARTIAL" else "SUCCEEDED" if accepted_count
                else "NO_DATA" if status == "NO_DATA" else "REJECTED"
            )
            logger.info("ai_job_persistence_completed", extra={**context, **persistence, "accepted_count": accepted_count})
            completed = self.repository.complete(
                job["job_id"], self.worker_id, status=terminal,
                result_payload={
                    **result,
                    **persistence,
                    "accepted_results": accepted,
                    "rejected_results": rejected,
                    "accepted_count": accepted_count,
                    "event_projection": persistence,
                },
                accepted_fields=sorted({
                    str(item.get("field") or item.get("field_semantics"))
                    for item in (result.get("accepted_claims") or accepted)
                }),
                rejected_fields=sorted({
                    str(item.get("field") or item.get("field_semantics"))
                    for item in (result.get("rejected_claims") or rejected)
                }),
                workspace_path=str(workspace),
                error=str(result.get("error")) if result.get("error") else None,
            )
            logger.info("ai_job_read_back_completed", extra=context)
            materialized = None
            parent_run_id = completed.get("parent_run_id")
            if parent_run_id:
                parent = self.parallel_coordinator.reconcile_parent(
                    str(parent_run_id)
                )
                if (
                    parent["status"] in {"SUCCEEDED", "PARTIAL", "NO_DATA"}
                    and self.parallel_coordinator.claim_materialization(
                        str(parent_run_id)
                    )
                ):
                    try:
                        materialized = self.materializer.materialize_for_parent(
                            parent=parent,
                            ai_enrichment=_parent_enrichment(parent),
                        )
                    finally:
                        self.parallel_coordinator.finish_materialization(
                            str(parent_run_id),
                            (
                                str(materialized["snapshot_id"])
                                if materialized
                                else None
                            ),
                        )
            else:
                materialized = self.materializer.materialize_for_job(
                    job=completed,
                    ai_enrichment=_job_enrichment(completed),
                )
            if result.get("run_id"):
                self.agentic_runtime.record_materialization(
                    str(result["run_id"]),
                    {"snapshot_id": materialized.get("snapshot_id") if materialized else None},
                )
                self.agentic_runtime.record_complete(
                    str(result["run_id"]), {"status": terminal, "job_id": completed["job_id"]},
                )
                if self._capability_report and self._capability_report.get("status") == "READY_TO_SMOKE":
                    run = self.agentic_runtime.repository.get_run(str(result["run_id"])) or {}
                    if int(run.get("search_count") or 0) and int(run.get("opened_source_count") or 0):
                        self.capabilities.record_live_verification({
                            "observed_search_count": run["search_count"],
                            "opened_source_count": run["opened_source_count"],
                            "source_domains": run.get("source_domains") or [],
                            "run_id": run.get("run_id"),
                        }, executable_version=self._capability_report.get("executable_version"))
                        self._capability_report = None
            logger.info("ai_job_completed", extra={**context, "status": terminal})
            return True
        except Exception as exc:
            diagnostic = getattr(exc, "diagnostic", None)
            if diagnostic is None:
                runtime_repository = getattr(self.agentic_runtime, "repository", None)
                get_run = getattr(runtime_repository, "get_run_for_job", None)
                linked_run = get_run(str(job["job_id"])) if callable(get_run) else None
                running_step = next(
                    (
                        step
                        for step in reversed((linked_run or {}).get("steps") or [])
                        if step.get("status") == "RUNNING"
                    ),
                    None,
                )
                trace = "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                )
                diagnostic = sanitize_diagnostic(
                    {
                        "category": "WORKER_ERROR",
                        "exception_type": type(exc).__name__,
                        "message": str(exc)[:500],
                        "step": (running_step or {}).get("step_name") or "WORKER",
                        "failing_step": (running_step or {}).get("step_name") or "WORKER",
                        "claim_ref": None,
                        "topic": None,
                        "field_semantics": None,
                        "run_id": (linked_run or {}).get("run_id"),
                        "job_id": job["job_id"],
                        "timestamp": datetime.now(UTC).replace(microsecond=0).isoformat(),
                        "retryable": False,
                        "retry_classification": "NON_RETRYABLE",
                        "stack_fingerprint": hashlib.sha256(
                            trace.encode("utf-8")
                        ).hexdigest()[:24],
                        "transaction_outcome": "NOT_STARTED",
                    }
                )
            if hasattr(exc, "code") and hasattr(exc, "retryable"):
                error_code = str(exc.code)
                retryable = bool(exc.retryable)
            else:
                category, retryable = classify_codex_failure(
                    exit_code=None,
                    stderr=str(exc),
                )
                error_code = f"worker:{category.lower()}:{type(exc).__name__}"
            logger.exception(
                "ai_job_failed",
                extra={
                    **context,
                    "run_id": (diagnostic or {}).get("run_id"),
                    "error_code": error_code,
                    "exit_code": (diagnostic or {}).get("exit_code"),
                    "step": (diagnostic or {}).get("step"),
                    "retry_classification": (
                        diagnostic.get("retry_classification")
                        if diagnostic
                        else "RETRYABLE"
                        if retryable
                        else "NON_RETRYABLE"
                    ),
                },
            )
            current = self.repository.get(job["job_id"])
            if current and current.get("status") == "RUNNING" and current.get("worker_id") == self.worker_id:
                updated = self.repository.retry_or_fail(
                    job["job_id"],
                    self.worker_id,
                    error=error_code,
                    delays=self._retry_delays(job),
                    retryable=retryable,
                    timed_out=bool(diagnostic and diagnostic.get("category") == "TIMEOUT"),
                    diagnostic=diagnostic,
                )
                if updated.get("status") in {
                    "FAILED",
                    "LOOP_DETECTED",
                    "TIMED_OUT",
                    "CANCELLED",
                    "REJECTED",
                }:
                    runtime_repository = getattr(
                        self.agentic_runtime,
                        "repository",
                        None,
                    )
                    reconcile = getattr(
                        runtime_repository,
                        "reconcile_terminal_jobs",
                        None,
                    )
                    if callable(reconcile):
                        reconcile()
                    self._reconcile_terminal_parent(job, updated)
            return True
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=2)

    def _heartbeat_loop(self, job_id: str, stop: threading.Event) -> None:
        interval = max(float(self.settings.ai_job_lease_seconds) / 3.0, 1.0)
        while not stop.wait(interval):
            if not self.repository.heartbeat(job_id, self.worker_id):
                return
            logger.info("ai_job_heartbeat", extra={"job_id": job_id, "correlation_id": None})

    def _reconcile_terminal_parent(
        self,
        job: dict[str, Any],
        updated: dict[str, Any],
    ) -> None:
        parent_run_id = job.get("parent_run_id")
        if (
            not parent_run_id
            or str(updated.get("status") or "")
            not in {"FAILED", "LOOP_DETECTED", "TIMED_OUT", "CANCELLED", "REJECTED"}
        ):
            return
        parent = self.parallel_coordinator.reconcile_parent(str(parent_run_id))
        if (
            parent["status"] in {"SUCCEEDED", "PARTIAL", "NO_DATA"}
            and self.parallel_coordinator.claim_materialization(str(parent_run_id))
        ):
            materialized = None
            try:
                materialized = self.materializer.materialize_for_parent(
                    parent=parent,
                    ai_enrichment=_parent_enrichment(parent),
                )
            finally:
                self.parallel_coordinator.finish_materialization(
                    str(parent_run_id),
                    str(materialized["snapshot_id"]) if materialized else None,
                )

    def _validate_results(
        self,
        job: dict[str, Any],
        result: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        release_at = (job.get("request_payload") or {}).get("release_at")
        service_evidence_verified = result.get("_service_evidence_verified") is True
        for raw in result.get("results") or []:
            if not isinstance(raw, dict):
                continue
            item = dict(raw)
            if service_evidence_verified:
                item["_service_evidence_verified"] = True
            else:
                item.pop("verified_independent_domains", None)
                item.pop("_service_evidence_verified", None)
            field = str(item.get("field") or item.get("field_semantics") or "")
            if field == "actual" and release_at:
                from app.services.data_freshness_service import parse_datetime
                from datetime import UTC, datetime
                parsed = parse_datetime(release_at)
                if parsed and datetime.now(UTC) < parsed:
                    item["validation_status"] = "rejected_future_actual"
                    rejected.append(item)
                    continue
            decision = self.source_policy.validate(
                item,
                field_semantics=field,
                numerical=field in {"actual", "forecast", "consensus", "previous"},
            )
            enriched = {
                **item,
                "source_domain": decision.domain,
                "source_tier": decision.tier,
                "source_classification": decision.classification,
                "policy_version": decision.policy_version,
                "validation_status": "accepted" if decision.accepted else "rejected",
                "validation_reasons": list(decision.reasons),
            }
            (accepted if decision.accepted else rejected).append(enriched)
        logger.info(
            "ai_job_validation_completed",
            extra={"job_id": job["job_id"], "correlation_id": job["correlation_id"], "accepted": len(accepted), "rejected": len(rejected)},
        )
        return accepted, rejected

    def _persist_accepted(self, job: dict[str, Any], accepted: list[dict[str, Any]]) -> dict[str, int]:
        persisted_count = 0
        read_back_count = 0
        request = job.get("request_payload") or {}
        event = request.get("event") or {}
        temporal = request.get("temporal_state") or {}
        event_key = str(job.get("event_key") or "")
        for item in accepted:
            if not event_key:
                raise ValueError("accepted event research result requires event_key")
            candidate_row = self.candidates.persist_candidate(
                event_key=event_key,
                candidate=item,
                release_at=temporal.get("release_at") or event.get("time_utc"),
                expected_metric_id=event.get("metric_id") or request.get("expected_metric_id"),
                expected_period=event.get("reference_period") or event.get("period") or request.get("expected_period"),
                expected_unit=event.get("unit") or request.get("expected_unit"),
            )
            if candidate_row["validation_status"] != "accepted":
                raise ValueError(f"accepted candidate failed persistence validation: {candidate_row['warnings']}")
            field = str(item.get("field") or item.get("field_semantics") or "")
            if field == "actual":
                self.facts.apply_official_event_actual(
                    canonical_event_key=event_key,
                    candidate=item,
                    policy_version=self.source_policy.policy_version,
                )
            elif field in {"forecast", "consensus", "previous"}:
                self.facts.apply_event_research_field(
                    canonical_event_key=event_key,
                    candidate=item,
                    policy_version=self.source_policy.policy_version,
                )
            elif field in {"outcome", "transcript_url"}:
                self.facts.apply_speech_outcome(event_key, item)
            else:
                raise ValueError(f"unsupported accepted result field: {field}")
            persisted_count += 1
            history = self.facts._event_history_row(event_key)
            expected_value = item.get("value")
            if history is not None and (
                (field in {"forecast", "consensus", "previous", "actual"} and str(history[field]) == str(expected_value))
                or (field in {"outcome", "transcript_url"} and history["outcome_json"])
            ):
                read_back_count += 1
        return {"persisted_count": persisted_count, "read_back_count": read_back_count}

    def _retry_delays(self, job: dict[str, Any]) -> list[int]:
        raw = (
            self.settings.official_actual_retry_seconds
            if job.get("job_type") == "RELEASE_ACTUAL_REFRESH"
            else "30,120,300"
        )
        return [int(item.strip()) for item in raw.split(",") if item.strip()]

    @staticmethod
    def _official_retry_deadline_open(job: dict[str, Any]) -> bool:
        from app.services.data_freshness_service import parse_datetime

        deadline = parse_datetime(job.get("retry_deadline_at"))
        return deadline is not None and datetime.now(UTC) < deadline


def _job_enrichment(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": job.get("status"),
        "job_ids": [job.get("job_id")],
        "requested_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
        "pending_fields": job.get("pending_fields") or [],
        "accepted_fields": job.get("accepted_fields") or [],
        "rejected_fields": job.get("rejected_fields") or [],
        "policy_version": job.get("policy_version"),
        "prompt_version": job.get("prompt_version"),
        "last_error": job.get("last_error"),
    }


def _parent_enrichment(parent: dict[str, Any]) -> dict[str, Any]:
    children = list(parent.get("children") or [])
    return {
        "status": parent.get("status"),
        "job_ids": [item.get("child_job_id") for item in children],
        "requested_at": parent.get("created_at"),
        "completed_at": parent.get("completed_at"),
        "pending_fields": [],
        "accepted_fields": [],
        "rejected_fields": [],
        "policy_version": None,
        "prompt_version": "gap_aware_parallel_research_v1",
        "last_error": None,
    }
