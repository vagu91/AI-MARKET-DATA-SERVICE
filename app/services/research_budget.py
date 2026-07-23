from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.services.codex_runtime_contract import sanitize_diagnostic


def group_topics_for_budget(
    topics: list[str] | tuple[str, ...],
    max_searches: int,
) -> list[list[str]]:
    normalized = [str(topic) for topic in topics if str(topic)]
    if not normalized or max_searches <= 0:
        return []
    groups = [[] for _ in range(min(max_searches, len(normalized)))]
    for index, topic in enumerate(normalized):
        groups[index % len(groups)].append(topic)
    return groups


def build_effective_budget(
    settings: Settings,
    *,
    required_topics: list[str],
    daily_usage: dict[str, int],
    daily_runs: int,
    runtime_seconds: int,
    elapsed_seconds: float = 0,
) -> dict[str, Any]:
    daily_searches_remaining = max(
        int(settings.research_daily_budget_searches)
        - int(daily_usage.get("search_count") or 0),
        0,
    )
    daily_opens_remaining = max(
        int(settings.research_daily_budget_opened_sources)
        - int(daily_usage.get("opened_source_count") or 0),
        0,
    )
    budget_mode = str(settings.research_budget_mode).lower()
    max_searches = int(settings.research_max_searches)
    max_opened_sources = int(settings.research_max_opened_sources)
    if budget_mode == "enforce":
        max_searches = min(max_searches, daily_searches_remaining)
        max_opened_sources = min(max_opened_sources, daily_opens_remaining)
    return {
        "budget_mode": budget_mode,
        "max_searches": max_searches,
        "max_opened_sources": max_opened_sources,
        "remaining_searches": max_searches,
        "remaining_opened_sources": max_opened_sources,
        "daily_runs_limit": int(settings.research_daily_budget_runs),
        "daily_runs_remaining": max(
            int(settings.research_daily_budget_runs) - int(daily_runs),
            0,
        ),
        "daily_searches_limit": int(settings.research_daily_budget_searches),
        "daily_searches_remaining": daily_searches_remaining,
        "daily_opened_sources_limit": int(
            settings.research_daily_budget_opened_sources
        ),
        "daily_opened_sources_remaining": daily_opens_remaining,
        "runtime_limit_seconds": max(int(runtime_seconds), 0),
        "remaining_runtime_seconds": max(
            int(runtime_seconds - elapsed_seconds),
            0,
        ),
        "query_topic_groups": group_topics_for_budget(
            required_topics,
            max_searches,
        ),
        "budget_created_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
    }


def refresh_effective_budget(
    budget: dict[str, Any],
    *,
    search_count: int,
    opened_source_count: int,
    remaining_runtime_seconds: float,
    completed_queries: list[str] | None = None,
    completed_opened_sources: list[str] | None = None,
) -> dict[str, Any]:
    refreshed = dict(budget)
    refreshed["remaining_searches"] = max(
        int(budget.get("max_searches") or 0) - int(search_count),
        0,
    )
    refreshed["remaining_opened_sources"] = max(
        int(budget.get("max_opened_sources") or 0) - int(opened_source_count),
        0,
    )
    refreshed["remaining_runtime_seconds"] = max(
        int(remaining_runtime_seconds),
        0,
    )
    refreshed["observed_searches"] = int(search_count)
    refreshed["observed_opened_sources"] = int(opened_source_count)
    refreshed["threshold_exceeded"] = {
        "searches": int(search_count) > int(budget.get("max_searches") or 0),
        "opened_sources": int(opened_source_count)
        > int(budget.get("max_opened_sources") or 0),
        "daily_searches": int(search_count)
        > int(budget.get("daily_searches_remaining") or 0),
        "daily_opened_sources": int(opened_source_count)
        > int(budget.get("daily_opened_sources_remaining") or 0),
    }
    refreshed["completed_queries"] = sorted(
        {str(query) for query in completed_queries or [] if str(query)}
    )
    refreshed["completed_opened_sources"] = sorted(
        {
            str(source)
            for source in completed_opened_sources or []
            if str(source)
        }
    )
    return refreshed


class ResearchBudgetExceeded(RuntimeError):
    def __init__(
        self,
        *,
        step: str,
        resource: str,
        configured_limit: int,
        observed_count: int,
        remaining_before_step: int,
        run_id: str,
        job_id: str,
        effective_budget: dict[str, Any],
        tool_events: list[dict[str, Any]] | None = None,
    ) -> None:
        self.category = "BUDGET_EXCEEDED"
        self.retryable = False
        self.retry_classification = "NON_RETRYABLE"
        self.code = f"research_budget_exceeded:{resource}"
        self.diagnostic = sanitize_diagnostic(
            {
                "category": self.category,
                "resource": resource,
                "configured_limit": int(configured_limit),
                "observed_count": int(observed_count),
                "remaining_before_step": int(remaining_before_step),
                "step": step,
                "run_id": run_id,
                "job_id": job_id,
                "retryable": False,
                "retry_classification": self.retry_classification,
                "timestamp": datetime.now(UTC).replace(microsecond=0).isoformat(),
                "tool_events_observed": [
                    _compact_tool_event(event)
                    for event in (tool_events or [])[-20:]
                ],
                "effective_usage": {
                    "search_count": int(
                        effective_budget.get("observed_searches") or 0
                    ),
                    "opened_source_count": int(
                        effective_budget.get("observed_opened_sources") or 0
                    ),
                },
                "effective_budget": effective_budget,
            }
        )
        super().__init__(self.code)


def _compact_tool_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": str(event.get("event_type") or "")[:80],
        "query": str(event.get("query") or "")[:500],
        "source_url": str(event.get("source_url") or "")[:1000],
        "canonical_url": str(event.get("canonical_url") or "")[:1000],
    }
