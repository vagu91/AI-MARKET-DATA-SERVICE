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
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository or ResearchRuntimeRepository(settings)
        self.verifier = verifier or DeterministicEvidenceVerifier(settings)
        self.monotonic = monotonic or time.monotonic
        self.metrics = ResearchMetricsService(settings)

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
        daily_usage = self.repository.daily_budget_usage(
            exclude_run_id=str(run["run_id"])
        )
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
            "job_id": job["job_id"], "run_id": run["run_id"],
        }
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
                str(
                    item.get("canonical_url")
                    or item.get("source_url")
                    or ""
                )
                for item in self.repository.observed_sources(str(run["run_id"]))
                if item.get("canonical_url") or item.get("source_url")
            ]
            effective_budget = refresh_effective_budget(
                base_budget,
                search_count=int(current_run.get("search_count") or 0),
                opened_source_count=int(
                    current_run.get("opened_source_count") or 0
                ),
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
                run["run_id"], step_name, ordinal, context,
                backend="codex_cli", tool=_step_tool(step_name),
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
            remaining_searches_before_step = int(
                effective_budget["remaining_searches"]
            )
            remaining_opens_before_step = int(
                effective_budget["remaining_opened_sources"]
            )
            remaining_before_step = (
                remaining_searches_before_step
                if budget_resource == "searches"
                else remaining_opens_before_step
                if budget_resource == "opened_sources"
                else 0
            )
            if (
                budget_mode == "enforce"
                and budget_resource
                and remaining_before_step <= 0
            ):
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
            logger.info("research_step_started", extra={"job_id": job["job_id"], "run_id": run["run_id"], "step": step_name})
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
                    completed_queries=self.repository.observed_queries(
                        str(run["run_id"])
                    ),
                    completed_opened_sources=[
                        str(
                            item.get("canonical_url")
                            or item.get("source_url")
                            or ""
                        )
                        for item in self.repository.observed_sources(
                            str(run["run_id"])
                        )
                        if item.get("canonical_url") or item.get("source_url")
                    ],
                )
                resource: str | None = None
                event_resource = str(event.get("event_type") or "")
                configured_limit = 0
                observed_count = 0
                if (
                    counts["daily_search_count"]
                    > int(base_budget["daily_searches_limit"])
                ):
                    resource = "daily_budget"
                    configured_limit = int(base_budget["daily_searches_limit"])
                    observed_count = counts["daily_search_count"]
                elif (
                    counts["daily_opened_source_count"]
                    > int(base_budget["daily_opened_sources_limit"])
                ):
                    resource = "daily_budget"
                    configured_limit = int(
                        base_budget["daily_opened_sources_limit"]
                    )
                    observed_count = counts["daily_opened_source_count"]
                elif counts["search_count"] > int(base_budget["max_searches"]):
                    resource = "searches"
                    configured_limit = int(base_budget["max_searches"])
                    observed_count = counts["search_count"]
                elif counts["opened_source_count"] > int(
                    base_budget["max_opened_sources"]
                ):
                    resource = "opened_sources"
                    configured_limit = int(
                        base_budget["max_opened_sources"]
                    )
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
                    job=job, run=run, step_name=step_name, context=context,
                    workspace=workspace, watchdog_seconds=max(1, math.ceil(remaining)),
                    effective_budget=effective_budget,
                    event_observer=observe_tool_event,
                )
                if not isinstance(output, dict):
                    raise ValueError("agent_step_output_not_object")
                _enforce_step_limits(output)
                events = [item for item in output.pop("_tool_events", []) if isinstance(item, dict)]
                usage = output.pop("_usage", None)
                incrementally_persisted = bool(
                    output.pop("_events_persisted_incrementally", False)
                )
                if not incrementally_persisted:
                    for event in events:
                        observe_tool_event(event)
                self.repository.record_tool_events(
                    run["run_id"], step["step_id"], [],
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
                logger.info("research_step_completed", extra={
                    "job_id": job["job_id"], "run_id": run["run_id"], "step": step_name,
                    "source_domain_count": len(domains),
                })
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
                        "error_code": exc.code if isinstance(exc, CodexCLIError) else type(exc).__name__,
                        "exit_code": diagnostic.get("exit_code") if diagnostic else None,
                        "retry_classification": (
                            diagnostic.get("retry_classification") if diagnostic else "NON_RETRYABLE"
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
                int(base_budget["max_opened_sources"])
                if budget_mode == "enforce"
                else None
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
            run["run_id"], "PERSIST", 7, {"claim_count": len(claims)},
            backend="service", tool="sqlite",
        )
        if should_persist:
            persisted = self.repository.persist_claims(run, claims)
            self.repository.complete_step(
                persist_step["step_id"], persisted, source_domains=persisted["source_domains"]
            )
        else:
            persisted = persist_step.get("output") or {}
        read_step, should_read = self.repository.begin_step(
            run["run_id"], "READ_BACK", 8,
            {"persisted_count": persisted.get("persisted_count")}, backend="service", tool="sqlite",
        )
        if should_read:
            read_result = {
                "persisted_count": persisted.get("persisted_count", 0),
                "read_back_count": persisted.get("read_back_count", 0),
                "verified": persisted.get("persisted_count", 0) == persisted.get("read_back_count", 0),
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
                    (self.repository.get_run(run["run_id"]) or {}).get(
                        "search_count"
                    )
                    or 0
                ),
                opened_source_count=int(
                    (self.repository.get_run(run["run_id"]) or {}).get(
                        "opened_source_count"
                    )
                    or 0
                ),
                remaining_runtime_seconds=deadline - self.monotonic(),
                completed_queries=self.repository.observed_queries(
                    str(run["run_id"])
                ),
                completed_opened_sources=[
                    str(
                        item.get("canonical_url")
                        or item.get("source_url")
                        or ""
                    )
                    for item in self.repository.observed_sources(
                        str(run["run_id"])
                    )
                    if item.get("canonical_url") or item.get("source_url")
                ],
            ),
            "usage_status": "available" if (self.repository.get_run(run["run_id"]) or {}).get("usage") else "usage_unavailable",
            "metrics": self.metrics.snapshot(
                str(run["run_id"]),
                persist=True,
            ),
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
            str(item.get("canonical_url") or item.get("source_url") or "") for item in observed
            if item.get("content_hash") or (item.get("payload") or {}).get("evidence_text_verified") is True
        }
        verified_events: list[dict[str, Any]] = []
        current_run = self.repository.get_run(str(run["run_id"])) or {}
        remaining_open_budget = max(
            (
                max_opened_sources
                if max_opened_sources is not None
                else int(current_run.get("opened_source_count") or 0)
                + sum(
                    len(claim.get("evidence") or [])
                    for claim in claims
                    if isinstance(claim, dict)
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
                if not url.startswith("https://") or self.repository.policy.rule_for(url, evidence.get("publisher")) is None:
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

    def _record_service_step(self, run_id: str, name: str, ordinal: int, payload: dict[str, Any]) -> None:
        step, execute = self.repository.begin_step(
            run_id, name, ordinal, payload, backend="service", tool="sqlite",
        )
        if execute:
            self.repository.complete_step(step["step_id"], payload)


def _step_tool(step_name: str) -> str:
    return {
        "PLAN": "planner", "SEARCH": "web_search", "OPEN_SOURCE": "web_open",
        "EXTRACT": "structured_extractor", "CROSS_CHECK": "evidence_cross_check",
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
                if _source_key(
                    item.get("canonical_url") or item.get("source_url")
                )
                == _source_key(url)
            ),
            None,
        )
        payload = (observation or {}).get("payload") or {}
        content_verified = bool(
            observation
            and (
                observation.get("content_hash")
                or payload.get("evidence_text_verified") is True
            )
        )
        source.update(
            {
                "model_declared_status": model_declared_status,
                "model_declared_evidence_available": model_declared_evidence,
                "model_declared_http_status": model_declared_http_status,
                "model_declared_content_hash": model_declared_content_hash,
                "observed_status": "OPENED" if observation else "UNVERIFIED",
                "verified_status": (
                    "VERIFIED" if content_verified else "UNVERIFIED"
                ),
                "source_status": "OPENED" if observation else "UNVERIFIED",
                "evidence_available": content_verified,
                "http_status": (
                    observation.get("http_status") if observation else None
                ),
                "content_hash": (
                    observation.get("content_hash") if observation else None
                ),
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


def _deadline_result(run: dict[str, Any], step_name: str) -> dict[str, Any]:
    return {
        "status": "TIMED_OUT", "error": "overall_job_deadline_expired",
        "results": [], "run_id": run["run_id"], "deadline_step": step_name,
        "usage_status": "usage_unavailable",
        "error_category": "OVERALL_DEADLINE",
        "retryable": False,
        "retry_classification": "NON_RETRYABLE",
    }
