from __future__ import annotations

import logging
import math
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from app.core.config import Settings
from app.services.research_profiles import profile_for_job, prompt_context
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.evidence_verification_service import (
    DeterministicEvidenceVerifier,
    EvidenceVerifierProtocol,
)
from app.services.data_freshness_service import parse_datetime


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

    def run(
        self,
        job: dict[str, Any],
        workspace: Path,
        executor: Any,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        profile = profile_for_job(str(job["job_type"]))
        run = self.repository.ensure_run(job, profile.profile_id, profile.prompt_version)
        elapsed = _elapsed_job_seconds(job)
        deadline = self.monotonic() + max(float(timeout_seconds) - elapsed, 0.0)
        context: dict[str, Any] = {
            "job_id": job["job_id"], "run_id": run["run_id"],
            "profile": prompt_context(profile, job.get("request_payload") or {}),
        }
        for ordinal, step_name in enumerate(EXTERNAL_STEPS, start=1):
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                return _deadline_result(run, step_name)
            step, execute = self.repository.begin_step(
                run["run_id"], step_name, ordinal, context,
                backend="codex_cli", tool=_step_tool(step_name),
            )
            if not execute:
                context[step_name.lower()] = step.get("output") or {}
                continue
            logger.info("research_step_started", extra={"job_id": job["job_id"], "run_id": run["run_id"], "step": step_name})
            try:
                output = executor.execute_step(
                    job=job, run=run, step_name=step_name, context=context,
                    workspace=workspace, watchdog_seconds=max(1, math.ceil(remaining)),
                )
                if not isinstance(output, dict):
                    raise ValueError("agent_step_output_not_object")
                _enforce_step_limits(output, self.settings)
                events = [item for item in output.pop("_tool_events", []) if isinstance(item, dict)]
                usage = output.pop("_usage", None)
                current_run = self.repository.get_run(run["run_id"]) or {}
                new_searches, new_opens = _observed_event_counts(events)
                if int(current_run.get("search_count") or 0) + new_searches > self.settings.research_max_searches:
                    raise ValueError("research_max_searches_exceeded")
                if int(current_run.get("opened_source_count") or 0) + new_opens > self.settings.research_max_opened_sources:
                    raise ValueError("research_max_opened_sources_exceeded")
                counts = self.repository.record_tool_events(
                    run["run_id"], step["step_id"], events,
                    usage=usage if isinstance(usage, dict) else None,
                )
                if counts["search_count"] > self.settings.research_max_searches:
                    raise ValueError("research_max_searches_exceeded")
                if counts["opened_source_count"] > self.settings.research_max_opened_sources:
                    raise ValueError("research_max_opened_sources_exceeded")
                domains = _source_domains(output)
                self.repository.complete_step(step["step_id"], output, source_domains=domains)
                context[step_name.lower()] = output
                logger.info("research_step_completed", extra={
                    "job_id": job["job_id"], "run_id": run["run_id"], "step": step_name,
                    "source_domain_count": len(domains),
                })
            except Exception as exc:
                self.repository.fail_step(step["step_id"], f"{type(exc).__name__}:{exc}")
                logger.exception("research_step_failed", extra={"job_id": job["job_id"], "run_id": run["run_id"], "step": step_name})
                if "watchdog" in str(exc).lower() or self.monotonic() >= deadline:
                    return _deadline_result(run, step_name)
                raise
        validation = context.get("validate") or {}
        claims = [item for item in validation.get("claims") or [] if isinstance(item, dict)]
        self._verify_unobserved_evidence(run, step["step_id"], claims)
        if self.monotonic() >= deadline:
            return _deadline_result(run, "PERSIST")
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
            "usage_status": "available" if (self.repository.get_run(run["run_id"]) or {}).get("usage") else "usage_unavailable",
        }

    def _verify_unobserved_evidence(
        self,
        run: dict[str, Any],
        step_id: str,
        claims: list[dict[str, Any]],
    ) -> None:
        observed = self.repository.observed_sources(str(run["run_id"]))
        observed_urls = {
            str(item.get("canonical_url") or item.get("source_url") or "") for item in observed
            if item.get("content_hash") or (item.get("payload") or {}).get("evidence_text_verified") is True
        }
        verified_events: list[dict[str, Any]] = []
        current_run = self.repository.get_run(str(run["run_id"])) or {}
        remaining_open_budget = max(
            self.settings.research_max_opened_sources - int(current_run.get("opened_source_count") or 0),
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


def _enforce_step_limits(output: dict[str, Any], settings: Settings) -> None:
    # Model-declared arrays are intentionally not treated as observed usage.
    if not isinstance(output, dict):
        raise ValueError("agent_step_output_not_object")


def _elapsed_job_seconds(job: dict[str, Any]) -> float:
    started = parse_datetime(job.get("started_at"))
    if started is None:
        return 0.0
    return max((datetime.now(UTC) - started).total_seconds(), 0.0)


def _deadline_result(run: dict[str, Any], step_name: str) -> dict[str, Any]:
    return {
        "status": "TIMED_OUT", "error": "overall_job_deadline_expired",
        "results": [], "run_id": run["run_id"], "deadline_step": step_name,
        "usage_status": "usage_unavailable",
    }


def _observed_event_counts(events: list[dict[str, Any]]) -> tuple[int, int]:
    searches = sum(1 for item in events if item.get("event_type") == "search")
    opened = sum(
        1 for item in events
        if item.get("event_type") in {"open_source", "server_source_verified"}
    )
    return searches, opened
