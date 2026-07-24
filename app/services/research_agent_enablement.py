from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from app.core.config import Settings


@dataclass(frozen=True)
class ResearchAgentRegistration:
    topic: str
    profile_id: str
    job_type: str
    settings_field: str
    env_name: str
    default_enabled: bool


def _registration(topic: str, profile_id: str, *, default: bool) -> ResearchAgentRegistration:
    suffix = topic.upper()
    return ResearchAgentRegistration(
        topic=topic,
        profile_id=profile_id,
        job_type=profile_id,
        settings_field=f"research_agent_{topic}_enabled",
        env_name=f"AI_MARKET_RESEARCH_AGENT_{suffix}_ENABLED",
        default_enabled=default,
    )


RESEARCH_AGENT_REGISTRY: tuple[ResearchAgentRegistration, ...] = (
    _registration("macro_events", "MACRO_EVENTS_RESEARCH", default=True),
    _registration("fed_rates", "FED_RATES_RESEARCH", default=True),
    _registration("vix_risk", "VIX_RISK_RESEARCH", default=True),
    _registration("cot_positioning", "COT_POSITIONING_RESEARCH", default=True),
    _registration("nasdaq_100", "NASDAQ_100_RESEARCH", default=True),
    _registration(
        "mega_cap_semiconductors",
        "MEGA_CAP_SEMICONDUCTORS_RESEARCH",
        default=True,
    ),
    _registration("earnings", "EARNINGS_RESEARCH", default=True),
    _registration("news", "NEWS_RESEARCH", default=True),
    _registration(
        "geopolitical_regulatory_risk",
        "GEOPOLITICAL_REGULATORY_RISK_RESEARCH",
        default=True,
    ),
    _registration("options_positioning", "OPTIONS_POSITIONING_RESEARCH", default=False),
    _registration("market_internals", "MARKET_INTERNALS_RESEARCH", default=False),
    _registration("cross_asset_context", "CROSS_ASSET_CONTEXT_RESEARCH", default=False),
    _registration(
        "earnings_intelligence",
        "EARNINGS_INTELLIGENCE_RESEARCH",
        default=False,
    ),
)

_BY_TOPIC = {item.topic: item for item in RESEARCH_AGENT_REGISTRY}
_BY_PROFILE = {item.profile_id: item for item in RESEARCH_AGENT_REGISTRY}
_BY_JOB_TYPE = {item.job_type: item for item in RESEARCH_AGENT_REGISTRY}
_GENERAL_JOB_TOPICS = {
    "MISSING_EVENT_RESEARCH": "macro_events",
    "RELEASE_ACTUAL_REFRESH": "macro_events",
    "SPEECH_OUTCOME_REFRESH": "fed_rates",
    "FED_SPEECH_OUTCOME": "fed_rates",
    "EARNINGS_CONTEXT": "earnings",
    "NEWS_DRIVER_RESEARCH": "news",
}


def registration_for(
    *,
    topic: str | None = None,
    profile_id: str | None = None,
    job_type: str | None = None,
) -> ResearchAgentRegistration | None:
    if topic:
        found = _BY_TOPIC.get(str(topic).lower())
        if found is not None:
            return found
    if profile_id:
        found = _BY_PROFILE.get(str(profile_id).upper())
        if found is not None:
            return found
    if job_type:
        normalized = str(job_type).upper()
        found = _BY_JOB_TYPE.get(normalized)
        if found is not None:
            return found
        alias_topic = _GENERAL_JOB_TOPICS.get(normalized)
        if alias_topic:
            return _BY_TOPIC[alias_topic]
    return None


def research_agent_enablement(
    settings: Settings,
    *,
    topic: str | None = None,
    profile_id: str | None = None,
    job_type: str | None = None,
) -> dict[str, Any]:
    registration = registration_for(
        topic=topic,
        profile_id=profile_id,
        job_type=job_type,
    )
    master_enabled = bool(settings.research_agents_enabled)
    configured_enabled = (
        bool(getattr(settings, registration.settings_field))
        if registration is not None
        else True
    )
    enabled = master_enabled and configured_enabled
    if not master_enabled:
        reason = "research_agents_master_disabled"
    elif not configured_enabled:
        reason = "research_agent_disabled"
    elif registration is None:
        reason = "general_research_job_enabled"
    else:
        reason = "research_agent_enabled"
    return {
        "agent_enabled": enabled,
        "master_enabled": master_enabled,
        "configured_enabled": configured_enabled,
        "reason": reason,
        "topic": registration.topic if registration else topic,
        "profile_id": registration.profile_id if registration else profile_id,
        "job_type": registration.job_type if registration else job_type,
        "settings_field": registration.settings_field if registration else None,
        "env_name": registration.env_name if registration else None,
    }


def is_research_agent_enabled(
    settings: Settings,
    *,
    topic: str | None = None,
    profile_id: str | None = None,
    job_type: str | None = None,
) -> bool:
    return bool(
        research_agent_enablement(
            settings,
            topic=topic,
            profile_id=profile_id,
            job_type=job_type,
        )["agent_enabled"]
    )


def disabled_job_result(
    settings: Settings,
    *,
    topic: str | None = None,
    profile_id: str | None = None,
    job_type: str | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    decision = research_agent_enablement(
        settings,
        topic=topic,
        profile_id=profile_id,
        job_type=job_type,
    )
    return {
        "job_id": None,
        "job_type": job_type,
        "profile_id": profile_id,
        "specialized_topic": decision.get("topic"),
        "correlation_id": correlation_id,
        "status": "REJECTED",
        "last_error": "AGENT_DISABLED",
        "retry_class": "NON_RETRYABLE",
        "attempts": 0,
        "created": False,
        "enablement": decision,
    }


def safe_research_agent_capabilities(settings: Settings) -> dict[str, Any]:
    agents = []
    for registration in RESEARCH_AGENT_REGISTRY:
        decision = research_agent_enablement(settings, topic=registration.topic)
        agents.append(
            {
                **asdict(registration),
                "agent_enabled": decision["agent_enabled"],
                "reason": decision["reason"],
            }
        )
    return {
        "research_agents_enabled": bool(settings.research_agents_enabled),
        "configured_agent_count": len(agents),
        "enabled_agent_count": sum(bool(item["agent_enabled"]) for item in agents),
        "disabled_agent_count": sum(not bool(item["agent_enabled"]) for item in agents),
        "agents": agents,
    }


def validate_research_agent_mapping() -> None:
    from app.services.research_gap_manifest import TOPIC_PROFILES

    expected = {(topic, profile) for topic, profile in TOPIC_PROFILES.items()}
    actual = {(item.topic, item.profile_id) for item in RESEARCH_AGENT_REGISTRY}
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise RuntimeError(
            f"invalid_research_agent_mapping:missing={missing}:extra={extra}"
        )
    if len({item.settings_field for item in RESEARCH_AGENT_REGISTRY}) != len(
        RESEARCH_AGENT_REGISTRY
    ):
        raise RuntimeError("duplicate_research_agent_settings_field")
    if len({item.env_name for item in RESEARCH_AGENT_REGISTRY}) != len(
        RESEARCH_AGENT_REGISTRY
    ):
        raise RuntimeError("duplicate_research_agent_env_name")
