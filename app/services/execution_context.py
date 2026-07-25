from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal


AI_AUTHORIZATION_DECISIONS = frozenset(
    {"AI_ALLOWED", "AI_SUPPRESSED", "AI_NOT_REQUIRED"}
)
REQUEST_ORIGINS = frozenset(
    {
        "provider_refresh",
        "explicit_ai_api",
        "research_scheduler",
        "lifecycle_resolver",
        "recovery",
        "test",
    }
)
AI_AUTHORIZED_REQUEST_ORIGINS = frozenset(
    {"explicit_ai_api", "research_scheduler", "recovery", "test"}
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
        if type(self.allow_live_providers) is not bool:
            raise ValueError("execution_context_allow_live_providers_must_be_boolean")
        if type(self.allow_ai) is not bool:
            raise ValueError("execution_context_allow_ai_must_be_boolean")
        if self.request_origin not in REQUEST_ORIGINS:
            raise ValueError("execution_context_request_origin_invalid")
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
        if request_origin not in AI_AUTHORIZED_REQUEST_ORIGINS:
            raise ValueError("execution_context_request_origin_not_ai_authorized")
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
        required = {
            "allow_live_providers",
            "allow_ai",
            "request_origin",
            "correlation_id",
        }
        if not required.issubset(value):
            return None
        if type(value["allow_live_providers"]) is not bool:
            return None
        if type(value["allow_ai"]) is not bool:
            return None
        try:
            return cls(
                allow_live_providers=value["allow_live_providers"],
                allow_ai=value["allow_ai"],
                request_origin=str(value["request_origin"]),  # type: ignore[arg-type]
                correlation_id=str(value["correlation_id"]),
            )
        except (TypeError, ValueError):
            return None


def authorizes_ai(
    context: ExecutionContext | None,
    *,
    environment: str | None,
) -> bool:
    return bool(
        context
        and context.allow_ai
        and context.request_origin in AI_AUTHORIZED_REQUEST_ORIGINS
        and (
            context.request_origin != "test"
            or str(environment or "").strip().lower() == "test"
        )
    )


def authorizes_live_providers(context: ExecutionContext | None) -> bool:
    return bool(context and context.allow_live_providers)


def ai_authorization_decision(
    context: ExecutionContext | None,
    *,
    ai_required: bool,
    environment: str | None,
) -> str:
    if ai_required:
        return (
            "AI_ALLOWED"
            if authorizes_ai(context, environment=environment)
            else "AI_SUPPRESSED"
        )
    if not authorizes_live_providers(context):
        return "AI_SUPPRESSED"
    return "AI_NOT_REQUIRED"
