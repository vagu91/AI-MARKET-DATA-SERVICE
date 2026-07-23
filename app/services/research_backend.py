from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol, runtime_checkable


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
