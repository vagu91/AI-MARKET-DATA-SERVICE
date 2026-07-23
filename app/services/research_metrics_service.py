from __future__ import annotations

import json
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database


class ResearchMetricsService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        migrate_database(settings.database_path)

    def snapshot(
        self,
        run_id: str,
        *,
        persist: bool = True,
    ) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            run = conn.execute(
                """
                SELECT request_json,usage_json,cost_json,threshold_warnings_json,
                       checkpoint_json,continuation_count,loop_detection_count
                FROM research_runs WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            if run is None:
                raise ValueError("research_run_not_found")
            tool = conn.execute(
                """
                SELECT COUNT(*) AS raw_events,
                       COUNT(DISTINCT CASE
                         WHEN semantic_action!='non_operational'
                         THEN tool_action_fingerprint END
                       ) AS normalized_actions,
                       COUNT(DISTINCT CASE
                         WHEN counts_usage=1
                         THEN tool_action_fingerprint END
                       ) AS tool_calls,
                       COUNT(DISTINCT CASE
                         WHEN counts_usage=1 AND semantic_action='search'
                         THEN tool_action_fingerprint END
                       ) AS searches,
                       COUNT(DISTINCT CASE
                         WHEN counts_usage=1
                          AND semantic_action IN ('open_source','fetch','verify_source')
                         THEN tool_action_fingerprint END
                       ) AS opens,
                       COUNT(DISTINCT CASE
                         WHEN counts_usage=1
                          AND semantic_action IN ('open_source','fetch','verify_source')
                         THEN COALESCE(canonical_url,source_url) END
                       ) AS new_sources
                FROM research_tool_events WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            claims = conn.execute(
                """
                SELECT COUNT(*) AS extracted,
                       SUM(CASE WHEN validation_status='accepted' THEN 1 ELSE 0 END)
                         AS accepted,
                       SUM(CASE WHEN validation_status!='accepted' THEN 1 ELSE 0 END)
                         AS rejected
                FROM research_claims WHERE research_run_id=?
                """,
                (run_id,),
            ).fetchone()
            steps = conn.execute(
                """
                SELECT step_name,status,duration_ms,output_json
                FROM research_run_steps WHERE run_id=? ORDER BY ordinal
                """,
                (run_id,),
            ).fetchall()
            verified_sources = conn.execute(
                """
                SELECT COUNT(DISTINCT canonical_url)
                FROM research_evidence
                WHERE claim_id IN (
                  SELECT claim_id FROM research_claims WHERE research_run_id=?
                ) AND source_status='VERIFIED'
                """,
                (run_id,),
            ).fetchone()[0]
        usage = json.loads(run["usage_json"] or "{}")
        cost = json.loads(run["cost_json"] or "{}")
        request = json.loads(run["request_json"] or "{}")
        accepted = int(claims["accepted"] or 0)
        extracted = max(
            int(claims["extracted"] or 0),
            _declared_claims(steps),
        )
        token_total = sum(
            int(usage.get(key) or 0)
            for key in ("input_tokens", "output_tokens")
        )
        declared_sources = _declared_sources(steps)
        observed_sources = int(tool["new_sources"] or 0)
        metrics = {
            "budget_mode": (
                (request.get("effective_budget") or {}).get("budget_mode")
                or self.settings.research_budget_mode
            ),
            "raw_events_observed": int(tool["raw_events"] or 0),
            "normalized_actions": int(tool["normalized_actions"] or 0),
            "deduplicated_tool_calls": int(tool["tool_calls"] or 0),
            "searches": int(tool["searches"] or 0),
            "opened_sources": int(tool["opens"] or 0),
            "new_sources": observed_sources,
            "claims_extracted": extracted,
            "claims_accepted": accepted,
            "claims_rejected": int(claims["rejected"] or 0),
            "usage": {
                "input_tokens": int(usage.get("input_tokens") or 0),
                "output_tokens": int(usage.get("output_tokens") or 0),
                "cached_tokens": int(usage.get("cached_tokens") or 0),
                "reasoning_tokens": int(usage.get("reasoning_tokens") or 0),
                "total_tokens": int(
                    usage.get("total_tokens") or token_total
                ),
            },
            "cost": cost or None,
            "cost_status": "available" if cost else "cost_unavailable",
            "phase_duration_ms": {
                str(step["step_name"]): int(step["duration_ms"] or 0)
                for step in steps
            },
            "tokens_per_accepted_claim": (
                token_total / accepted if accepted else None
            ),
            "cost_per_accepted_claim": (
                _cost_value(cost) / accepted
                if cost and accepted
                else None
            ),
            "searches_per_new_source": (
                int(tool["searches"] or 0) / observed_sources
                if observed_sources
                else None
            ),
            "threshold_warnings": json.loads(
                run["threshold_warnings_json"] or "[]"
            ),
            "loop_detections": int(run["loop_detection_count"] or 0),
            "continuation_count": int(run["continuation_count"] or 0),
            "checkpoint": json.loads(run["checkpoint_json"] or "{}"),
            "progress": {
                "completed_phases": sum(
                    1 for step in steps if step["status"] == "COMPLETED"
                ),
                "recorded_phases": len(steps),
                "latest_phase": (
                    str(steps[-1]["step_name"]) if steps else None
                ),
            },
            "sources": {
                "model_declared": declared_sources,
                "observed": observed_sources,
                "verified": int(verified_sources or 0),
                "unverified": max(
                    declared_sources - int(verified_sources or 0),
                    0,
                ),
            },
        }
        if persist:
            with connect_sqlite(self.settings.database_path) as conn:
                conn.execute(
                    "UPDATE research_runs SET metrics_json=? WHERE run_id=?",
                    (
                        json.dumps(
                            metrics,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            default=str,
                        ),
                        run_id,
                    ),
                )
                conn.commit()
        return metrics


def _declared_sources(steps: list[Any]) -> int:
    for step in steps:
        if str(step["step_name"]) != "OPEN_SOURCE":
            continue
        try:
            output = json.loads(step["output_json"] or "{}")
        except json.JSONDecodeError:
            return 0
        return len(
            [item for item in output.get("sources") or [] if isinstance(item, dict)]
        )
    return 0


def _declared_claims(steps: list[Any]) -> int:
    for step in steps:
        if str(step["step_name"]) != "EXTRACT":
            continue
        try:
            output = json.loads(step["output_json"] or "{}")
        except json.JSONDecodeError:
            return 0
        return len(
            [item for item in output.get("claims") or [] if isinstance(item, dict)]
        )
    return 0


def _cost_value(cost: dict[str, Any]) -> float:
    for key in ("total_cost_usd", "cost_usd", "cost"):
        if cost.get(key) is not None:
            return float(cost[key])
    return 0.0
