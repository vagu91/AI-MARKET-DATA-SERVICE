from __future__ import annotations

import logging
import math
import time
from pathlib import Path
from typing import Any, Callable

from app.core.config import Settings
from app.services.research_profiles import profile_for_job, prompt_context
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.evidence_verification_service import (
    DeterministicEvidenceVerifier,
    EvidenceVerifierProtocol,
)
from app.services.codex_runtime_contract import CodexCLIError
from app.services.research_budget import (
    ResearchBudgetExceeded,
    build_effective_budget,
    refresh_effective_budget,
)
from app.services.research_tool_telemetry import (
    ProgressLoopGuard,
    ResearchLoopDetected,
)
from app.services.research_metrics_service import ResearchMetricsService
from app.services.research_backend import ResearchBackend
from app.services.research_source_gateway import ResearchSourceGateway
from app.services.research_semantics import document_not_applicable_claims


logger = logging.getLogger(__name__)
EXTERNAL_STEPS = ("PLAN", "SEARCH", "OPEN_SOURCE", "EXTRACT", "CROSS_CHECK", "VALIDATE")
ALL_STEPS = (*EXTERNAL_STEPS, "PERSIST", "READ_BACK", "MATERIALIZE", "COMPLETE")


class AgenticResearchRuntime:
    def __init__(
        self,
        settings: Settings,
        *,
        repository: ResearchRuntimeRepository | None = None,
        verifier: EvidenceVerifierProtocol | None = None,
        source_gateway: ResearchSourceGateway | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository or ResearchRuntimeRepository(settings)
        self.verifier = verifier or DeterministicEvidenceVerifier(settings)
        self.monotonic = monotonic or time.monotonic
        self.metrics = ResearchMetricsService(settings)
        self.source_gateway = source_gateway or ResearchSourceGateway(
            settings,
            repository=self.repository,
            policy=self.repository.policy,
        )

    def run(
        self,
        job: dict[str, Any],
        workspace: Path,
        executor: Any,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        profile = profile_for_job(str(job["job_type"]))
        run = self.repository.ensure_run(job, profile.profile_id, profile.prompt_version)
        deadline = self.monotonic() + max(float(timeout_seconds), 0.0)
        daily_usage = self.repository.daily_budget_usage(exclude_run_id=str(run["run_id"]))
        base_budget = self.repository.ensure_effective_budget(
            str(run["run_id"]),
            build_effective_budget(
                self.settings,
                required_topics=list(run.get("required_topics") or []),
                daily_usage=daily_usage,
                daily_runs=daily_usage["run_count"],
                runtime_seconds=timeout_seconds,
                elapsed_seconds=0,
            ),
        )
        budget_mode = str(base_budget.get("budget_mode") or "observe")
        known_sources = [
            str(item.get("canonical_url") or item.get("source_url") or "")
            for item in self.repository.observed_sources(str(run["run_id"]))
            if item.get("canonical_url") or item.get("source_url")
        ]
        guard = ProgressLoopGuard(
            self.settings,
            known_queries=self.repository.observed_queries(str(run["run_id"])),
            known_sources=known_sources,
        )
        invocation_progress = False
        completed_steps: list[str] = []
        context: dict[str, Any] = {
            "job_id": job["job_id"],
            "run_id": run["run_id"],
        }
        if self.settings.research_single_invocation_enabled and isinstance(
            executor, ResearchBackend
        ):
            return self._run_single_invocation(
                job=job,
                workspace=workspace,
                backend=executor,
                profile=profile,
                run=run,
                deadline=deadline,
                base_budget=base_budget,
                guard=guard,
            )
        for ordinal, step_name in enumerate(EXTERNAL_STEPS, start=1):
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                return self._checkpoint_or_deadline(
                    run,
                    step_name,
                    invocation_progress,
                    completed_steps,
                    guard,
                )
            current_run = self.repository.get_run(run["run_id"]) or run
            completed_queries = self.repository.observed_queries(str(run["run_id"]))
            completed_opened_sources = [
                str(item.get("canonical_url") or item.get("source_url") or "")
                for item in self.repository.observed_sources(str(run["run_id"]))
                if item.get("canonical_url") or item.get("source_url")
            ]
            effective_budget = refresh_effective_budget(
                base_budget,
                search_count=int(current_run.get("search_count") or 0),
                opened_source_count=int(current_run.get("opened_source_count") or 0),
                remaining_runtime_seconds=remaining,
                completed_queries=completed_queries,
                completed_opened_sources=completed_opened_sources,
            )
            context["effective_budget"] = effective_budget
            context["profile"] = prompt_context(
                profile,
                job.get("request_payload") or {},
                effective_budget,
            )
            step, execute = self.repository.begin_step(
                run["run_id"],
                step_name,
                ordinal,
                context,
                backend="codex_cli",
                tool=_step_tool(step_name),
            )
            if not execute:
                completed_steps.append(step_name)
                context[step_name.lower()] = step.get("output") or {}
                continue
            budget_resource = (
                "searches"
                if step_name == "SEARCH"
                else "opened_sources"
                if step_name == "OPEN_SOURCE"
                else None
            )
            remaining_searches_before_step = int(effective_budget["remaining_searches"])
            remaining_opens_before_step = int(effective_budget["remaining_opened_sources"])
            remaining_before_step = (
                remaining_searches_before_step
                if budget_resource == "searches"
                else remaining_opens_before_step
                if budget_resource == "opened_sources"
                else 0
            )
            if budget_mode == "enforce" and budget_resource and remaining_before_step <= 0:
                bounded = {
                    "status": "NO_DATA",
                    "warnings": [f"{budget_resource}_budget_exhausted"],
                    "effective_budget": effective_budget,
                }
                if step_name == "SEARCH":
                    bounded.update({"searches": [], "sources": []})
                else:
                    bounded["sources"] = []
                self.repository.complete_step(step["step_id"], bounded)
                context[step_name.lower()] = bounded
                continue
            logger.info(
                "research_step_started",
                extra={"job_id": job["job_id"], "run_id": run["run_id"], "step": step_name},
            )
            observed_step_events: list[dict[str, Any]] = []

            def observe_tool_event(event: dict[str, Any]) -> None:
                nonlocal invocation_progress
                observed_step_events.append(event)
                counts = self.repository.record_tool_events(
                    str(run["run_id"]),
                    str(step["step_id"]),
                    [event],
                )
                current_budget = refresh_effective_budget(
                    base_budget,
                    search_count=counts["search_count"],
                    opened_source_count=counts["opened_source_count"],
                    remaining_runtime_seconds=deadline - self.monotonic(),
                    completed_queries=self.repository.observed_queries(str(run["run_id"])),
                    completed_opened_sources=[
                        str(item.get("canonical_url") or item.get("source_url") or "")
                        for item in self.repository.observed_sources(str(run["run_id"]))
                        if item.get("canonical_url") or item.get("source_url")
                    ],
                )
                resource: str | None = None
                event_resource = str(event.get("event_type") or "")
                configured_limit = 0
                observed_count = 0
                if counts["daily_search_count"] > int(base_budget["daily_searches_limit"]):
                    resource = "daily_budget"
                    configured_limit = int(base_budget["daily_searches_limit"])
                    observed_count = counts["daily_search_count"]
                elif counts["daily_opened_source_count"] > int(
                    base_budget["daily_opened_sources_limit"]
                ):
                    resource = "daily_budget"
                    configured_limit = int(base_budget["daily_opened_sources_limit"])
                    observed_count = counts["daily_opened_source_count"]
                elif counts["search_count"] > int(base_budget["max_searches"]):
                    resource = "searches"
                    configured_limit = int(base_budget["max_searches"])
                    observed_count = counts["search_count"]
                elif counts["opened_source_count"] > int(base_budget["max_opened_sources"]):
                    resource = "opened_sources"
                    configured_limit = int(base_budget["max_opened_sources"])
                    observed_count = counts["opened_source_count"]
                if resource:
                    if budget_mode == "observe":
                        self.repository.record_threshold_warning(
                            str(run["run_id"]),
                            {
                                "resource": resource,
                                "configured_limit": configured_limit,
                                "observed_count": observed_count,
                                "step": step_name,
                            },
                        )
                    else:
                        raise ResearchBudgetExceeded(
                            step=step_name,
                            resource=resource,
                            configured_limit=configured_limit,
                            observed_count=observed_count,
                            remaining_before_step=(
                                remaining_opens_before_step
                                if event_resource
                                in {
                                    "open_source",
                                    "server_source_verified",
                                }
                                else remaining_searches_before_step
                            ),
                            run_id=str(run["run_id"]),
                            job_id=str(job["job_id"]),
                            effective_budget=current_budget,
                            tool_events=observed_step_events,
                        )
                for inserted in counts.get("inserted_events") or []:
                    progress, loop_reason = guard.observe(inserted)
                    invocation_progress = invocation_progress or progress
                    if loop_reason:
                        loop_error = ResearchLoopDetected(
                            step=step_name,
                            run_id=str(run["run_id"]),
                            job_id=str(job["job_id"]),
                            reason=loop_reason,
                            evidence=guard.evidence(),
                        )
                        self.repository.record_loop_detection(
                            str(run["run_id"]),
                            loop_error.diagnostic,
                        )
                        raise loop_error

            try:
                output = executor.execute_step(
                    job=job,
                    run=run,
                    step_name=step_name,
                    context=context,
                    workspace=workspace,
                    watchdog_seconds=max(1, math.ceil(remaining)),
                    effective_budget=effective_budget,
                    event_observer=observe_tool_event,
                )
                if not isinstance(output, dict):
                    raise ValueError("agent_step_output_not_object")
                _enforce_step_limits(output)
                events = [item for item in output.pop("_tool_events", []) if isinstance(item, dict)]
                usage = output.pop("_usage", None)
                incrementally_persisted = bool(output.pop("_events_persisted_incrementally", False))
                if not incrementally_persisted:
                    for event in events:
                        observe_tool_event(event)
                self.repository.record_tool_events(
                    run["run_id"],
                    step["step_id"],
                    [],
                    usage=usage if isinstance(usage, dict) else None,
                )
                if step_name == "OPEN_SOURCE":
                    output = _reconcile_declared_sources(
                        output,
                        self.repository.observed_sources(str(run["run_id"])),
                    )
                domains = _source_domains(output)
                self.repository.complete_step(step["step_id"], output, source_domains=domains)
                completed_steps.append(step_name)
                invocation_progress = True
                guard.mark_phase_progress()
                context[step_name.lower()] = output
                logger.info(
                    "research_step_completed",
                    extra={
                        "job_id": job["job_id"],
                        "run_id": run["run_id"],
                        "step": step_name,
                        "source_domain_count": len(domains),
                    },
                )
            except Exception as exc:
                diagnostic = getattr(exc, "diagnostic", None)
                if diagnostic is not None:
                    diagnostic["run_id"] = str(run["run_id"])
                    diagnostic["job_id"] = str(job["job_id"])
                self.repository.fail_step(
                    step["step_id"],
                    f"{type(exc).__name__}:{exc}",
                    diagnostic=diagnostic,
                )
                self.metrics.snapshot(str(run["run_id"]), persist=True)
                logger.exception(
                    "research_step_failed",
                    extra={
                        "job_id": job["job_id"],
                        "run_id": run["run_id"],
                        "step": step_name,
                        "error_code": exc.code
                        if isinstance(exc, CodexCLIError)
                        else type(exc).__name__,
                        "exit_code": diagnostic.get("exit_code") if diagnostic else None,
                        "retry_classification": (
                            diagnostic.get("retry_classification")
                            if diagnostic
                            else "NON_RETRYABLE"
                        ),
                    },
                )
                if isinstance(
                    exc,
                    (
                        CodexCLIError,
                        ResearchBudgetExceeded,
                        ResearchLoopDetected,
                    ),
                ):
                    raise
                if "watchdog" in str(exc).lower() or self.monotonic() >= deadline:
                    return _deadline_result(run, step_name)
                raise
        validation = context.get("validate") or {}
        claims = [item for item in validation.get("claims") or [] if isinstance(item, dict)]
        self._verify_unobserved_evidence(
            run,
            step["step_id"],
            claims,
            max_opened_sources=(
                int(base_budget["max_opened_sources"]) if budget_mode == "enforce" else None
            ),
        )
        if self.monotonic() >= deadline:
            return self._checkpoint_or_deadline(
                run,
                "PERSIST",
                invocation_progress,
                completed_steps,
                guard,
            )
        persist_step, should_persist = self.repository.begin_step(
            run["run_id"],
            "PERSIST",
            7,
            {"claim_count": len(claims)},
            backend="service",
            tool="sqlite",
        )
        if should_persist:
            persisted = self.repository.persist_claims(
                run,
                claims,
                step_id=str(persist_step["step_id"]),
            )
        else:
            persisted = persist_step.get("output") or {}
        read_step, should_read = self.repository.begin_step(
            run["run_id"],
            "READ_BACK",
            8,
            {"persisted_count": persisted.get("persisted_count")},
            backend="service",
            tool="sqlite",
        )
        if should_read:
            read_result = {
                "persisted_count": persisted.get("persisted_count", 0),
                "read_back_count": persisted.get("read_back_count", 0),
                "verified": persisted.get("persisted_count", 0)
                == persisted.get("read_back_count", 0),
            }
            self.repository.complete_step(read_step["step_id"], read_result)
        return {
            **persisted,
            "run_id": run["run_id"],
            "profile_id": profile.profile_id,
            "prompt_version": profile.prompt_version,
            "research_steps": list(ALL_STEPS),
            "_service_evidence_verified": True,
            "effective_budget": refresh_effective_budget(
                base_budget,
                search_count=int(
                    (self.repository.get_run(run["run_id"]) or {}).get("search_count") or 0
                ),
                opened_source_count=int(
                    (self.repository.get_run(run["run_id"]) or {}).get("opened_source_count") or 0
                ),
                remaining_runtime_seconds=deadline - self.monotonic(),
                completed_queries=self.repository.observed_queries(str(run["run_id"])),
                completed_opened_sources=[
                    str(item.get("canonical_url") or item.get("source_url") or "")
                    for item in self.repository.observed_sources(str(run["run_id"]))
                    if item.get("canonical_url") or item.get("source_url")
                ],
            ),
            "usage_status": "available"
            if (self.repository.get_run(run["run_id"]) or {}).get("usage")
            else "usage_unavailable",
            "metrics": self.metrics.snapshot(
                str(run["run_id"]),
                persist=True,
            ),
        }

    def _run_single_invocation(
        self,
        *,
        job: dict[str, Any],
        workspace: Path,
        backend: ResearchBackend,
        profile: Any,
        run: dict[str, Any],
        deadline: float,
        base_budget: dict[str, Any],
        guard: ProgressLoopGuard,
    ) -> dict[str, Any]:
        """Optimized production path: one model call, deterministic service phases."""

        run_id = str(run["run_id"])
        job_id = str(job["job_id"])
        current_run = self.repository.get_run(run_id) or run
        effective_budget = refresh_effective_budget(
            base_budget,
            search_count=int(current_run.get("search_count") or 0),
            opened_source_count=int(current_run.get("opened_source_count") or 0),
            remaining_runtime_seconds=deadline - self.monotonic(),
            completed_queries=self.repository.observed_queries(run_id),
            completed_opened_sources=[
                str(item.get("canonical_url") or item.get("source_url") or "")
                for item in self.repository.observed_sources(run_id)
                if item.get("canonical_url") or item.get("source_url")
            ],
        )
        model_profile = prompt_context(
            profile,
            job.get("request_payload") or {},
            effective_budget,
        )
        plan_step, execute_plan = self.repository.begin_step(
            run_id,
            "PLAN",
            1,
            {"profile": profile.profile_id, "mode": "single_invocation"},
            backend=backend.backend_name,
            tool="agentic_plan",
        )
        search_step, execute_search = self.repository.begin_step(
            run_id,
            "SEARCH",
            2,
            {"effective_budget": effective_budget, "mode": "single_invocation"},
            backend=backend.backend_name,
            tool="agentic_search",
        )
        invocation = self.repository.latest_backend_invocation(run_id)
        payload = dict((invocation or {}).get("output") or {})
        observed_events: list[dict[str, Any]] = []

        def observe_tool_event(event: dict[str, Any]) -> None:
            observed_events.append(event)
            counts = self.repository.record_tool_events(
                run_id,
                str(search_step["step_id"]),
                [event],
            )
            resource: str | None = None
            configured_limit = 0
            observed_count = 0
            if counts["search_count"] > int(base_budget["max_searches"]):
                resource = "searches"
                configured_limit = int(base_budget["max_searches"])
                observed_count = counts["search_count"]
            elif counts["opened_source_count"] > int(base_budget["max_opened_sources"]):
                resource = "opened_sources"
                configured_limit = int(base_budget["max_opened_sources"])
                observed_count = counts["opened_source_count"]
            if resource:
                if str(base_budget.get("budget_mode") or "observe") == "observe":
                    self.repository.record_threshold_warning(
                        run_id,
                        {
                            "resource": resource,
                            "configured_limit": configured_limit,
                            "observed_count": observed_count,
                            "step": "SEARCH",
                        },
                    )
                else:
                    raise ResearchBudgetExceeded(
                        step="SEARCH",
                        resource=resource,
                        configured_limit=configured_limit,
                        observed_count=observed_count,
                        remaining_before_step=int(effective_budget.get("remaining_searches") or 0),
                        run_id=run_id,
                        job_id=job_id,
                        effective_budget=effective_budget,
                        tool_events=observed_events,
                    )
            for inserted in counts.get("inserted_events") or []:
                _progress, loop_reason = guard.observe(inserted)
                if loop_reason:
                    loop_error = ResearchLoopDetected(
                        step="SEARCH",
                        run_id=run_id,
                        job_id=job_id,
                        reason=loop_reason,
                        evidence=guard.evidence(),
                    )
                    self.repository.record_loop_detection(run_id, loop_error.diagnostic)
                    raise loop_error

        if execute_plan or execute_search or not payload:
            remaining_value = deadline - self.monotonic()
            if remaining_value <= 0:
                return _deadline_result(run, "SEARCH")
            remaining = max(1, math.ceil(remaining_value))
            try:
                result = backend.execute_research(
                    job=job,
                    run=run,
                    profile=model_profile,
                    workspace=workspace,
                    watchdog_seconds=remaining,
                    effective_budget=effective_budget,
                    event_observer=observe_tool_event,
                )
            except Exception as exc:
                diagnostic = getattr(exc, "diagnostic", None)
                if execute_plan:
                    self.repository.fail_step(
                        str(plan_step["step_id"]),
                        f"{type(exc).__name__}:{exc}",
                        diagnostic=diagnostic,
                    )
                if execute_search:
                    self.repository.fail_step(
                        str(search_step["step_id"]),
                        f"{type(exc).__name__}:{exc}",
                        diagnostic=diagnostic,
                    )
                self.metrics.snapshot(run_id, persist=True)
                raise
            payload = dict(result.payload)
            if not isinstance(payload, dict):
                raise ValueError("agentic_backend_output_not_object")
            for event in result.tool_events:
                observe_tool_event(dict(event))
            self.repository.record_backend_invocation(run_id, result)
            plan_output = {
                "status": payload.get("status") or "COMPLETED",
                **dict(payload.get("plan") or {}),
                "warnings": list(payload.get("warnings") or []),
                "invocation_id": result.invocation_id,
            }
            search_output = {
                "status": payload.get("status") or "COMPLETED",
                "searches": list(payload.get("searches") or []),
                "sources": list(payload.get("acquisition_requests") or []),
                "warnings": list(payload.get("warnings") or []),
                "invocation_id": result.invocation_id,
            }
            if execute_plan:
                self.repository.complete_step(str(plan_step["step_id"]), plan_output)
            if execute_search:
                self.repository.complete_step(str(search_step["step_id"]), search_output)
        if self.monotonic() >= deadline:
            return self._checkpoint_or_deadline(
                run,
                "OPEN_SOURCE",
                True,
                ["PLAN", "SEARCH"],
                guard,
            )

        acquisition_requests = _acquisition_requests(payload)
        if str(base_budget.get("budget_mode") or "observe") == "enforce":
            acquisition_requests = acquisition_requests[
                : max(
                    int(effective_budget.get("remaining_opened_sources") or 0),
                    0,
                )
            ]
        open_step, execute_open = self.repository.begin_step(
            run_id,
            "OPEN_SOURCE",
            3,
            {"request_count": len(acquisition_requests)},
            backend="service",
            tool="research_source_gateway",
        )
        if execute_open:
            acquired: list[dict[str, Any]] = []
            for request in acquisition_requests:
                if self.monotonic() >= deadline:
                    return self._checkpoint_or_deadline(
                        run,
                        "OPEN_SOURCE",
                        bool(acquired),
                        ["PLAN", "SEARCH"],
                        guard,
                    )
                source = self.source_gateway.acquire(run_id, request)
                acquired.append(source)
                fetched = source.get("fetch_status") == "FETCHED"
                gateway_event = {
                    "raw_event_type": "service.source_fetch",
                    "lifecycle": "completed" if fetched else "failed",
                    "item_id": f"gateway:{source['source_id']}",
                    "item_type": "service_http_fetch",
                    "phase": "OPEN_SOURCE",
                    "provider_tool_type": "research_source_gateway",
                    "semantic_action": "fetch",
                    "source_url": source.get("requested_url"),
                    "canonical_url": source.get("canonical_url"),
                    "redirect_url": (
                        source.get("final_url")
                        if source.get("final_url") != source.get("requested_url")
                        else None
                    ),
                    "observed_at": source.get("retrieved_at"),
                    "content_hash": source.get("content_sha256"),
                    "http_status": source.get("http_status"),
                    "status": (
                        "fetched" if fetched else source.get("rejection_reason") or "rejected"
                    ),
                    "counts_usage": fetched,
                }
                counts = self.repository.record_tool_events(
                    run_id,
                    str(open_step["step_id"]),
                    [gateway_event],
                )
                for inserted in counts.get("inserted_events") or []:
                    _progress, loop_reason = guard.observe(inserted)
                    if loop_reason:
                        raise ResearchLoopDetected(
                            step="OPEN_SOURCE",
                            run_id=run_id,
                            job_id=job_id,
                            reason=loop_reason,
                            evidence=guard.evidence(),
                        )
            self.repository.complete_step(
                str(open_step["step_id"]),
                {
                    "status": "COMPLETED",
                    "sources": [_public_source(source) for source in acquired],
                    "warnings": [
                        str(source["rejection_reason"])
                        for source in acquired
                        if source.get("rejection_reason")
                    ],
                },
                source_domains=sorted(
                    {
                        str(source.get("source_domain") or "")
                        for source in acquired
                        if source.get("fetch_status") == "FETCHED" and source.get("source_domain")
                    }
                ),
            )
        acquired = self.repository.research_sources(run_id)

        claims = document_not_applicable_claims(
            [item for item in payload.get("claims") or [] if isinstance(item, dict)],
            payload,
        )
        claims = self.repository.normalize_claims(claims)
        extract_step, execute_extract = self.repository.begin_step(
            run_id,
            "EXTRACT",
            4,
            {
                "candidate_count": len(claims),
                "fetched_source_count": sum(
                    item.get("fetch_status") == "FETCHED" for item in acquired
                ),
            },
            backend="service",
            tool="deterministic_content_extractor",
        )
        if execute_extract:
            verified_claims = self.source_gateway.verify_claims(run_id, claims)
            self.repository.complete_step(
                str(extract_step["step_id"]),
                {
                    "status": "COMPLETED" if verified_claims else "NO_DATA",
                    "claims": verified_claims,
                    "warnings": [],
                },
            )
        else:
            verified_claims = list((extract_step.get("output") or {}).get("claims") or [])

        cross_step, execute_cross = self.repository.begin_step(
            run_id,
            "CROSS_CHECK",
            5,
            {"claim_count": len(verified_claims)},
            backend="service",
            tool="deterministic_confirmation_policy",
        )
        cross_checked = _deterministic_cross_check(
            verified_claims,
            acquired,
            self.repository.policy,
        )
        if execute_cross:
            self.repository.complete_step(
                str(cross_step["step_id"]),
                {
                    "status": ("COMPLETED" if cross_checked else "NO_DATA"),
                    "claims": cross_checked,
                    "warnings": [],
                },
            )

        validate_step, execute_validate = self.repository.begin_step(
            run_id,
            "VALIDATE",
            6,
            {"claim_count": len(verified_claims)},
            backend="service",
            tool="deterministic_schema_policy_validator",
        )
        supported_refs = {
            str(item.get("claim_ref") or "")
            for item in cross_checked
            if item.get("resolution") == "SUPPORTED"
        }
        validation_output = {
            "status": "PARTIAL" if supported_refs else "NO_DATA",
            "claims": verified_claims,
            "missing_topics": [],
            "blocking_gaps": [],
            "warnings": [],
        }
        if execute_validate:
            self.repository.complete_step(
                str(validate_step["step_id"]),
                validation_output,
            )
        if self.monotonic() >= deadline:
            return self._checkpoint_or_deadline(
                run,
                "PERSIST",
                True,
                list(EXTERNAL_STEPS),
                guard,
            )

        persist_step, should_persist = self.repository.begin_step(
            run_id,
            "PERSIST",
            7,
            {"claim_count": len(verified_claims)},
            backend="service",
            tool="sqlite",
        )
        current_run = self.repository.get_run(run_id) or run
        if should_persist:
            persisted = self.repository.persist_claims(
                current_run,
                verified_claims,
                step_id=str(persist_step["step_id"]),
            )
        else:
            persisted = persist_step.get("output") or {}
        read_step, should_read = self.repository.begin_step(
            run_id,
            "READ_BACK",
            8,
            {"persisted_count": persisted.get("persisted_count")},
            backend="service",
            tool="sqlite",
        )
        if should_read:
            self.repository.complete_step(
                str(read_step["step_id"]),
                {
                    "persisted_count": persisted.get("persisted_count", 0),
                    "read_back_count": persisted.get("read_back_count", 0),
                    "verified": persisted.get("persisted_count", 0)
                    == persisted.get("read_back_count", 0),
                },
            )
        return {
            **persisted,
            "run_id": run_id,
            "profile_id": profile.profile_id,
            "prompt_version": profile.prompt_version,
            "research_steps": list(ALL_STEPS),
            "_service_evidence_verified": True,
            "backend": backend.backend_name,
            "backend_invocation_count": 1,
            "effective_budget": effective_budget,
            "usage_status": (
                "available"
                if (self.repository.get_run(run_id) or {}).get("usage")
                else "usage_unavailable"
            ),
            "metrics": self.metrics.snapshot(run_id, persist=True),
        }

    def _checkpoint_or_deadline(
        self,
        run: dict[str, Any],
        next_step: str,
        invocation_progress: bool,
        completed_steps: list[str],
        guard: ProgressLoopGuard,
    ) -> dict[str, Any]:
        if invocation_progress and self.settings.research_checkpoint_on_deadline:
            checkpoint = self.repository.checkpoint_run(
                str(run["run_id"]),
                {
                    "next_step": next_step,
                    "completed_steps": completed_steps,
                    "progress": guard.snapshot(),
                },
            )
            metrics = self.metrics.snapshot(str(run["run_id"]), persist=True)
            return {
                "status": "CHECKPOINTED",
                "run_id": run["run_id"],
                "checkpoint": checkpoint,
                "continuation_required": True,
                "retryable": True,
                "retry_classification": "CONTINUATION",
                "metrics": metrics,
            }
        return _deadline_result(run, next_step)

    def _verify_unobserved_evidence(
        self,
        run: dict[str, Any],
        step_id: str,
        claims: list[dict[str, Any]],
        *,
        max_opened_sources: int | None,
    ) -> None:
        observed = self.repository.observed_sources(str(run["run_id"]))
        observed_urls = {
            str(item.get("canonical_url") or item.get("source_url") or "")
            for item in observed
            if item.get("content_hash")
            or (item.get("payload") or {}).get("evidence_text_verified") is True
        }
        verified_events: list[dict[str, Any]] = []
        current_run = self.repository.get_run(str(run["run_id"])) or {}
        remaining_open_budget = max(
            (
                max_opened_sources
                if max_opened_sources is not None
                else int(current_run.get("opened_source_count") or 0)
                + sum(
                    len(claim.get("evidence") or []) for claim in claims if isinstance(claim, dict)
                )
            )
            - int(current_run.get("opened_source_count") or 0),
            0,
        )
        for claim in claims:
            for evidence in claim.get("evidence") or []:
                if not isinstance(evidence, dict):
                    continue
                url = str(evidence.get("canonical_url") or evidence.get("source_url") or "")
                if url in observed_urls:
                    continue
                if len(verified_events) >= remaining_open_budget:
                    continue
                if (
                    not url.startswith("https://")
                    or self.repository.policy.rule_for(url, evidence.get("publisher")) is None
                ):
                    continue
                verified = self.verifier.verify(evidence)
                if verified:
                    verified_events.append(verified)
        if verified_events:
            self.repository.record_tool_events(run["run_id"], step_id, verified_events)

    def record_materialization(self, run_id: str, payload: dict[str, Any]) -> None:
        self._record_service_step(run_id, "MATERIALIZE", 9, payload)

    def record_complete(self, run_id: str, payload: dict[str, Any]) -> None:
        self._record_service_step(run_id, "COMPLETE", 10, payload)
        self.repository.finish_run(run_id, str(payload.get("status") or "SUCCEEDED"), payload)

    def _record_service_step(
        self, run_id: str, name: str, ordinal: int, payload: dict[str, Any]
    ) -> None:
        step, execute = self.repository.begin_step(
            run_id,
            name,
            ordinal,
            payload,
            backend="service",
            tool="sqlite",
        )
        if execute:
            self.repository.complete_step(step["step_id"], payload)


def _step_tool(step_name: str) -> str:
    return {
        "PLAN": "planner",
        "SEARCH": "web_search",
        "OPEN_SOURCE": "web_open",
        "EXTRACT": "structured_extractor",
        "CROSS_CHECK": "evidence_cross_check",
        "VALIDATE": "schema_policy_validator",
    }[step_name]


def _source_domains(payload: Any) -> list[str]:
    domains: set[str] = set()
    if isinstance(payload, dict):
        if payload.get("source_domain"):
            domains.add(str(payload["source_domain"]).lower())
        for value in payload.values():
            domains.update(_source_domains(value))
    elif isinstance(payload, list):
        for value in payload:
            domains.update(_source_domains(value))
    return sorted(domains)


def _reconcile_declared_sources(
    output: dict[str, Any],
    observed_sources: list[dict[str, Any]],
) -> dict[str, Any]:
    reconciled = dict(output)
    sources: list[dict[str, Any]] = []
    for raw in output.get("sources") or []:
        if not isinstance(raw, dict):
            continue
        source = dict(raw)
        model_declared_status = source.get("source_status")
        model_declared_evidence = source.get("evidence_available")
        model_declared_http_status = source.get("http_status")
        model_declared_content_hash = source.get("content_hash")
        url = str(source.get("canonical_url") or source.get("source_url") or "")
        observation = next(
            (
                item
                for item in observed_sources
                if _source_key(item.get("canonical_url") or item.get("source_url"))
                == _source_key(url)
            ),
            None,
        )
        payload = (observation or {}).get("payload") or {}
        content_verified = bool(
            observation
            and (observation.get("content_hash") or payload.get("evidence_text_verified") is True)
        )
        source.update(
            {
                "model_declared_status": model_declared_status,
                "model_declared_evidence_available": model_declared_evidence,
                "model_declared_http_status": model_declared_http_status,
                "model_declared_content_hash": model_declared_content_hash,
                "observed_status": "OPENED" if observation else "UNVERIFIED",
                "verified_status": ("VERIFIED" if content_verified else "UNVERIFIED"),
                "source_status": "OPENED" if observation else "UNVERIFIED",
                "evidence_available": content_verified,
                "http_status": (observation.get("http_status") if observation else None),
                "content_hash": (observation.get("content_hash") if observation else None),
            }
        )
        sources.append(source)
    reconciled["sources"] = sources
    return reconciled


def _source_key(value: Any) -> str:
    return str(value or "").strip().lower().rstrip("/")


def _enforce_step_limits(output: dict[str, Any]) -> None:
    # Model-declared arrays are intentionally not treated as observed usage.
    if not isinstance(output, dict):
        raise ValueError("agent_step_output_not_object")


def _acquisition_requests(payload: dict[str, Any]) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = [
        dict(item) for item in payload.get("acquisition_requests") or [] if isinstance(item, dict)
    ]
    known = {
        str(item.get("source_url") or "").strip() for item in requests if item.get("source_url")
    }
    for search in payload.get("searches") or []:
        if not isinstance(search, dict):
            continue
        for url in search.get("discovered_urls") or []:
            value = str(url or "").strip()
            if not value or value in known:
                continue
            known.add(value)
            requests.append(
                {
                    "source_url": value,
                    "title": None,
                    "publisher": None,
                    "published_at": None,
                }
            )
    for claim in payload.get("claims") or []:
        if not isinstance(claim, dict):
            continue
        for evidence in claim.get("evidence") or []:
            if not isinstance(evidence, dict):
                continue
            value = str(evidence.get("canonical_url") or evidence.get("source_url") or "").strip()
            if not value or value in known:
                continue
            known.add(value)
            requests.append(
                {
                    "source_url": value,
                    "title": None,
                    "publisher": evidence.get("publisher"),
                    "published_at": evidence.get("published_at"),
                }
            )
    return requests


def _public_source(source: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "source_id",
        "requested_url",
        "final_url",
        "canonical_url",
        "source_domain",
        "source_tier",
        "publisher",
        "title",
        "fetch_status",
        "verification_status",
        "rejection_reason",
        "http_status",
        "content_type",
        "retrieved_at",
        "content_sha256",
        "content_bytes",
        "redirect_chain",
        "duplicate_of_source_id",
        "acquisition_backend",
        "fetch_duration_ms",
    )
    return {key: source.get(key) for key in keys}


def _deterministic_cross_check(
    claims: list[dict[str, Any]],
    acquired_sources: list[dict[str, Any]],
    policy: Any,
) -> list[dict[str, Any]]:
    by_id = {str(source.get("source_id") or ""): source for source in acquired_sources}
    results: list[dict[str, Any]] = []
    for index, claim in enumerate(claims):
        claim_ref = str(claim.get("claim_ref") or claim.get("claim_id") or f"candidate-{index + 1}")
        accepted_sources: list[dict[str, Any]] = []
        rejected_reasons: list[str] = []
        for evidence in claim.get("evidence") or []:
            if not isinstance(evidence, dict):
                continue
            verification = (
                evidence.get("_service_verification")
                if isinstance(evidence.get("_service_verification"), dict)
                else {}
            )
            if verification.get("accepted") is True:
                source = by_id.get(str(verification.get("source_id") or ""))
                if source is not None:
                    accepted_sources.append(source)
            else:
                rejected_reasons.append(
                    str(verification.get("reason") or "evidence_not_service_verified")
                )
        groups = {
            f"domain:{source.get('source_domain')}"
            for source in accepted_sources
            if source.get("source_domain")
        }
        content_domains: dict[str, set[str]] = {}
        for source in accepted_sources:
            content_hash = str(source.get("content_sha256") or "")
            domain = str(source.get("source_domain") or "")
            if content_hash and domain:
                content_domains.setdefault(content_hash, set()).add(domain)
        for content_hash, domains in content_domains.items():
            if len(domains) <= 1:
                continue
            groups.difference_update(f"domain:{domain}" for domain in domains)
            groups.add(f"content:{content_hash}")
        semantics = str(claim.get("field_semantics") or "exploratory_context").lower()
        required = policy.required_confirmations(semantics)
        resolution = "SUPPORTED" if len(groups) >= required else "INSUFFICIENT"
        warnings = sorted(set(rejected_reasons))
        if len(groups) < required:
            warnings.append("insufficient_independent_evidence")
        results.append(
            {
                "claim_ref": claim_ref,
                "conflict": False,
                "independent_source_urls": sorted(
                    {
                        str(source.get("canonical_url") or "")
                        for source in accepted_sources
                        if source.get("canonical_url")
                    }
                ),
                "syndication_suspected": any(
                    len(domains) > 1 for domains in content_domains.values()
                ),
                "resolution": resolution,
                "warnings": warnings,
            }
        )
    return results


def _deadline_result(run: dict[str, Any], step_name: str) -> dict[str, Any]:
    return {
        "status": "TIMED_OUT",
        "error": "overall_job_deadline_expired",
        "results": [],
        "run_id": run["run_id"],
        "deadline_step": step_name,
        "usage_status": "usage_unavailable",
        "error_category": "OVERALL_DEADLINE",
        "retryable": False,
        "retry_classification": "NON_RETRYABLE",
    }
