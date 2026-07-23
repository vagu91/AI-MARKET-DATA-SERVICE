from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import json
import time
from typing import Any, Callable, Protocol, runtime_checkable

import httpx

from app.core.config import Settings


@dataclass(frozen=True)
class ResearchBackendResult:
    """Backend-neutral result for one bounded agentic research invocation."""

    invocation_id: str
    backend: str
    purpose: str
    payload: dict[str, Any]
    usage: dict[str, Any] = field(default_factory=dict)
    tool_events: tuple[dict[str, Any], ...] = ()
    duration_ms: int = 0
    model: str | None = None


@runtime_checkable
class ResearchBackend(Protocol):
    """Portable model backend. Source acquisition and verification stay service-owned."""

    backend_name: str

    def execute_research(
        self,
        *,
        job: dict[str, Any],
        run: dict[str, Any],
        profile: dict[str, Any],
        workspace: Path,
        watchdog_seconds: int,
        effective_budget: dict[str, Any],
        event_observer: Callable[[dict[str, Any]], None] | None = None,
    ) -> ResearchBackendResult: ...


class OpenAIResponsesResearchBackend:
    """Responses API adapter for the same normalized backend-neutral contract."""

    backend_name = "openai_api"

    def __init__(
        self,
        settings: Settings,
        *,
        request_sender: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        self.settings = settings
        self.request_sender = request_sender or self._send

    def execute_research(
        self,
        *,
        job: dict[str, Any],
        run: dict[str, Any],
        profile: dict[str, Any],
        workspace: Path,
        watchdog_seconds: int,
        effective_budget: dict[str, Any],
        event_observer: Callable[[dict[str, Any]], None] | None = None,
    ) -> ResearchBackendResult:
        del workspace
        normalized = normalized_backend_input(
            job=job,
            run=run,
            profile=profile,
            effective_budget=effective_budget,
        )
        request = {
            "model": self.settings.openai_research_model or "gpt-5-mini",
            "input": json.dumps(normalized, ensure_ascii=False, sort_keys=True),
            "text": {"format": {"type": "json_object"}},
            "temperature": self.settings.openai_research_temperature,
            "metadata": {
                "job_id": str(job.get("job_id") or ""),
                "run_id": str(run.get("run_id") or ""),
                "profile_id": str(normalized["profile_id"]),
            },
            "timeout_seconds": min(
                int(watchdog_seconds),
                int(self.settings.openai_research_timeout_seconds),
            ),
        }
        started = time.perf_counter()
        response = self.request_sender(request)
        duration_ms = int((time.perf_counter() - started) * 1000)
        payload = _responses_payload(response)
        usage = dict(response.get("usage") or {})
        invocation_id = str(response.get("id") or f"openai-{run.get('run_id')}")
        if event_observer:
            event_observer(
                {
                    "event_type": "backend_invocation",
                    "raw_event_type": "openai.responses.completed",
                    "lifecycle": "completed",
                    "status": "completed",
                    "item_id": invocation_id,
                    "item_type": "response",
                    "usage": usage,
                    "payload": {"backend": self.backend_name},
                }
            )
        return ResearchBackendResult(
            invocation_id=invocation_id,
            backend=self.backend_name,
            purpose="AGENTIC_RESEARCH",
            payload=payload,
            usage=usage,
            duration_ms=duration_ms,
            model=str(response.get("model") or request["model"]),
        )

    def _send(self, request: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.openai_api_key:
            raise RuntimeError("openai_research_api_key_missing")
        body = {key: value for key, value in request.items() if key != "timeout_seconds"}
        response = httpx.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {self.settings.openai_api_key}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=float(request["timeout_seconds"]),
        )
        response.raise_for_status()
        value = response.json()
        if not isinstance(value, dict):
            raise ValueError("openai_responses_output_not_object")
        return value


def normalized_backend_input(
    *,
    job: dict[str, Any],
    run: dict[str, Any],
    profile: Any,
    effective_budget: dict[str, Any],
) -> dict[str, Any]:
    if hasattr(profile, "__dict__"):
        profile_value = {
            key: value
            for key, value in vars(profile).items()
            if not key.startswith("_")
        }
    else:
        profile_value = dict(profile)
    return {
        "contract_version": "research_backend_v1",
        "job_id": job.get("job_id"),
        "run_id": run.get("run_id"),
        "symbol": job.get("symbol") or "MNQ",
        "profile_id": profile_value.get("profile_id"),
        "prompt_version": profile_value.get("prompt_version"),
        "objective": profile_value.get("objective"),
        "required_topics": list(profile_value.get("required_topics") or []),
        "gap": (job.get("request_payload") or {}).get("gap"),
        "planned_queries": list(profile_value.get("planned_queries") or []),
        "priority_domains": list(profile_value.get("priority_domains") or []),
        "required_fields": list(profile_value.get("required_fields") or []),
        "effective_budget": dict(effective_budget),
        "output_schema": {
            "status": "SUCCEEDED|PARTIAL|NO_DATA|FAILED",
            "plan": {},
            "searches": [],
            "acquisition_requests": [],
            "claims": [],
            "topic_statuses": {},
            "warnings": [],
        },
    }


def select_research_backend(
    settings: Settings,
    *,
    openai_request_sender: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> ResearchBackend:
    backend = str(settings.research_backend).lower()
    if backend == "codex_cli":
        from app.services.ai_research_job_executor import PersistentAIJobExecutor

        return PersistentAIJobExecutor(settings)
    if backend == "openai_api":
        return OpenAIResponsesResearchBackend(
            settings,
            request_sender=openai_request_sender,
        )
    raise ValueError(f"unsupported_research_backend:{backend}")


def _responses_payload(response: dict[str, Any]) -> dict[str, Any]:
    if isinstance(response.get("output_json"), dict):
        return dict(response["output_json"])
    text = response.get("output_text")
    if not text:
        for item in response.get("output") or []:
            for content in item.get("content") or []:
                if content.get("type") in {"output_text", "text"} and content.get("text"):
                    text = content["text"]
                    break
            if text:
                break
    try:
        value = json.loads(str(text or ""))
    except json.JSONDecodeError as exc:
        raise ValueError("openai_responses_invalid_json") from exc
    if not isinstance(value, dict):
        raise ValueError("openai_responses_output_not_object")
    return value
