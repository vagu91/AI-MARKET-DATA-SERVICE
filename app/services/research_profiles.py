from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ResearchProfile:
    profile_id: str
    prompt_version: str
    objective: str
    required_topics: tuple[str, ...]
    no_data_criteria: str


PROFILES: dict[str, ResearchProfile] = {
    "MNQ_MARKET_RESEARCH": ResearchProfile(
        "MNQ_MARKET_RESEARCH", "mnq_market_research_v2",
        "Verify temporally relevant macro and Fed schedules, Nasdaq-100, mega-cap, semiconductor, earnings, current news, geopolitical, regulatory, volatility and positioning drivers for MNQ. Search every required topic and prefer current official, Reuters/AP, CFTC/CME/Cboe, Nasdaq/Invesco, issuer and SEC sources.",
        ("macro", "fed_rates", "events", "nasdaq_100", "mega_cap_semiconductors", "earnings", "news", "risk", "volatility_positioning", "conflicts"),
        "Return NO_DATA for a topic only after bounded searches and source-policy validation produce no current verifiable evidence.",
    ),
    "EVENT_MISSING_FIELDS": ResearchProfile(
        "EVENT_MISSING_FIELDS", "event_missing_fields_v2",
        "Resolve only missing forecast, consensus and previous fields with exact metric, period and unit validation.",
        ("event_identity", "metric_semantics", "missing_fields", "source_confirmation"),
        "Return NO_DATA when metric, period, unit or independent-source requirements cannot be proved.",
    ),
    "FED_SPEECH_OUTCOME": ResearchProfile(
        "FED_SPEECH_OUTCOME", "fed_speech_outcome_v1",
        "Verify an official Federal Reserve speech or statement, summarize sourced themes and changes, and capture outcome/transcript URL without trading advice.",
        ("official_text", "themes", "changes", "outcome", "transcript"),
        "Return NO_DATA when no official text or attributable transcript is available.",
    ),
    "EARNINGS_CONTEXT": ResearchProfile(
        "EARNINGS_CONTEXT", "earnings_context_v1",
        "Verify earnings timing, released results, guidance and Nasdaq/MNQ relevance from issuer IR or SEC evidence.",
        ("timing", "results", "guidance", "official_filing", "nasdaq_relevance"),
        "Return NO_DATA for numerical fields without issuer or SEC evidence.",
    ),
    "NEWS_DRIVER_RESEARCH": ResearchProfile(
        "NEWS_DRIVER_RESEARCH", "news_driver_research_v1",
        "Identify recent material MNQ/Nasdaq drivers, verify timestamps and original sources, deduplicate syndication and assign expiry.",
        ("recent_candidates", "original_sources", "independent_confirmation", "mnq_relevance", "expiry"),
        "Return NO_DATA when bounded recent-source searches yield no material verified driver.",
    ),
    "CONFLICT_RESOLUTION": ResearchProfile(
        "CONFLICT_RESOLUTION", "conflict_resolution_v1",
        "Resolve discordant values through metric, period, unit and source ranking without averaging.",
        ("conflicting_claims", "metric_semantics", "period", "unit", "source_ranking"),
        "Return NO_DATA when no candidate can be validated without arbitrary reconciliation.",
    ),
}


JOB_PROFILE = {
    "MISSING_EVENT_RESEARCH": "EVENT_MISSING_FIELDS",
    "SPEECH_OUTCOME_REFRESH": "FED_SPEECH_OUTCOME",
    "EARNINGS_CONTEXT": "EARNINGS_CONTEXT",
    "NEWS_DRIVER_RESEARCH": "NEWS_DRIVER_RESEARCH",
    "CONFLICT_RESOLUTION": "CONFLICT_RESOLUTION",
    "MNQ_MARKET_RESEARCH": "MNQ_MARKET_RESEARCH",
}


def profile_for_job(job_type: str) -> ResearchProfile:
    return PROFILES[JOB_PROFILE.get(job_type, "MNQ_MARKET_RESEARCH")]


def prompt_context(
    profile: ResearchProfile,
    request: dict[str, Any],
    effective_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    budget = dict(effective_budget or {})
    max_searches = int(
        budget.get("max_searches")
        if budget.get("max_searches") is not None
        else request.get("max_searches") or 0
    )
    max_opened_sources = int(
        budget.get("max_opened_sources")
        if budget.get("max_opened_sources") is not None
        else request.get("max_opened_sources") or 0
    )
    return {
        "profile_id": profile.profile_id,
        "prompt_version": profile.prompt_version,
        "objective": profile.objective,
        "required_topics": list(profile.required_topics),
        "database_context": request.get("database_context") or request.get("existing_database_results") or {},
        "missing_fields": request.get("missing_fields") or request.get("pending_fields") or [],
        "sources_already_queried": request.get("sources_already_queried") or [],
        "source_policy": request.get("source_policy") or {},
        "time_bounds": request.get("time_bounds") or {
            "release_at": request.get("release_at"), "valid_until": request.get("valid_until"),
        },
        "market_context": request.get("market_context") or {
            "context_date": request.get("context_date"), "market_session": request.get("market_session"),
        },
        "limits": {
            "budget_mode": str(budget.get("budget_mode") or "observe"),
            "max_searches": max_searches,
            "max_opened_sources": max_opened_sources,
            "remaining_searches": int(
                budget.get("remaining_searches", max_searches)
            ),
            "remaining_opened_sources": int(
                budget.get(
                    "remaining_opened_sources",
                    max_opened_sources,
                )
            ),
            "daily_runs_remaining": int(
                budget.get("daily_runs_remaining") or 0
            ),
            "daily_searches_remaining": int(
                budget.get("daily_searches_remaining") or 0
            ),
            "daily_opened_sources_remaining": int(
                budget.get("daily_opened_sources_remaining") or 0
            ),
            "remaining_runtime_seconds": int(
                budget.get("remaining_runtime_seconds") or 0
            ),
        },
        "query_topic_groups": budget.get("query_topic_groups") or [],
        "completed_queries": budget.get("completed_queries") or [],
        "completed_opened_sources": (
            budget.get("completed_opened_sources") or []
        ),
        "threshold_exceeded": budget.get("threshold_exceeded") or {},
        "no_data_criteria": profile.no_data_criteria,
        "prohibitions": [
            "no invented data", "no trading recommendations", "no orders", "no buy/sell",
            "no long/short", "no entry/stop/target/sizing", "no secrets",
        ],
    }
