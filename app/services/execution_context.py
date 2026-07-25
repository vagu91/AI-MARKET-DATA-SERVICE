from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal


AI_AUTHORIZATION_DECISIONS = frozenset(
    {"AI_ALLOWED", "AI_SUPPRESSED", "AI_NOT_REQUIRED"}
)
RequestOrigin = Literal[
    "provider_refresh",
    "explicit_ai_api",
    "research_scheduler",
    "lifecycle_resolver",
    "recovery",
    "test",
]


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Immutable authority propagated from request entry point to execution."""

    allow_live_providers: bool = False
    allow_ai: bool = False
    request_origin: RequestOrigin = "provider_refresh"
    correlation_id: str = ""

    def __post_init__(self) -> None:
        if not str(self.correlation_id).strip():
            raise ValueError("execution_context_correlation_id_required")

    def as_payload(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def provider_only(
        cls,
        *,
        correlation_id: str,
        allow_live_providers: bool,
    ) -> ExecutionContext:
        return cls(
            allow_live_providers=allow_live_providers,
            allow_ai=False,
            request_origin="provider_refresh",
            correlation_id=correlation_id,
        )

    @classmethod
    def explicit_ai(
        cls,
        *,
        correlation_id: str,
        request_origin: RequestOrigin = "explicit_ai_api",
        allow_live_providers: bool = False,
    ) -> ExecutionContext:
        return cls(
            allow_live_providers=allow_live_providers,
            allow_ai=True,
            request_origin=request_origin,
            correlation_id=correlation_id,
        )

    @classmethod
    def from_payload(cls, value: Any) -> ExecutionContext | None:
        if not isinstance(value, dict):
            return None
        try:
            return cls(
                allow_live_providers=value.get("allow_live_providers") is True,
                allow_ai=value.get("allow_ai") is True,
                request_origin=str(value.get("request_origin") or "provider_refresh"),  # type: ignore[arg-type]
                correlation_id=str(value.get("correlation_id") or ""),
            )
        except (TypeError, ValueError):
            return None


def ai_authorization_decision(
    context: ExecutionContext | None,
    *,
    ai_required: bool,
) -> str:
    if not ai_required:
        return "AI_NOT_REQUIRED"
    if context is None or not context.allow_ai:
        return "AI_SUPPRESSED"
    return "AI_ALLOWED"
