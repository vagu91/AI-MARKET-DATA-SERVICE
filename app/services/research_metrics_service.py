from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlsplit

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.observability_contract_service import ModelPricingService


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
                SELECT request_json,result_json,usage_json,cost_json,threshold_warnings_json,
                       checkpoint_json,continuation_count,loop_detection_count,
                       started_at,completed_at,warnings_json
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
                FROM research_claims
                WHERE research_run_id=? AND materialization_status!='ORPHANED'
                """,
                (run_id,),
            ).fetchone()
            rejected_claim_rows = conn.execute(
                """
                SELECT claim_id,warnings_json FROM research_claims
                WHERE research_run_id=? AND validation_status!='accepted'
                  AND materialization_status!='ORPHANED'
                ORDER BY claim_id
                """,
                (run_id,),
            ).fetchall()
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
                  SELECT claim_id FROM research_claims
                  WHERE research_run_id=? AND materialization_status!='ORPHANED'
                ) AND source_status='VERIFIED' AND audit_status='ACTIVE'
                  AND source_audit_status='ACTIVE'
                """,
                (run_id,),
            ).fetchone()[0]
            source_stats = conn.execute(
                """
                SELECT COUNT(*) AS discovered,
                       SUM(CASE WHEN fetch_status='FETCHED' THEN 1 ELSE 0 END)
                         AS fetched,
                       SUM(CASE WHEN verification_status='VERIFIED' THEN 1 ELSE 0 END)
                         AS verified,
                       SUM(CASE WHEN fetch_status='REJECTED' THEN 1 ELSE 0 END)
                         AS rejected,
                       COALESCE(SUM(fetch_duration_ms),0) AS fetch_duration_ms
                FROM research_sources WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            verification_stats = conn.execute(
                """
                SELECT status,reason,COUNT(*) AS count,
                       COALESCE(SUM(verification_duration_ms),0) AS duration_ms
                FROM research_evidence_verifications
                WHERE run_id=? GROUP BY status,reason
                ORDER BY status,reason
                """,
                (run_id,),
            ).fetchall()
            invocation_stats = conn.execute(
                """
                WITH deduplicated AS (
                  SELECT invocation_id,
                         MAX(lifecycle_status) AS lifecycle_status,
                         MAX(usage_status) AS usage_status,
                         MAX(backend) AS backend,
                         MAX(model) AS model,
                         MAX(input_tokens) AS input_tokens,
                         MAX(output_tokens) AS output_tokens,
                         MAX(cached_tokens) AS cached_tokens,
                         MAX(reasoning_tokens) AS reasoning_tokens,
                         MAX(total_tokens) AS total_tokens,
                         MAX(duration_ms) AS duration_ms
                  FROM research_backend_invocations
                  WHERE run_id=?
                  GROUP BY invocation_id
                )
                SELECT COUNT(*) AS attempted_count,
                       SUM(CASE WHEN lifecycle_status='COMPLETED' THEN 1 ELSE 0 END)
                         AS completed_count,
                       SUM(CASE WHEN lifecycle_status='ABORTED' THEN 1 ELSE 0 END)
                         AS aborted_count,
                       SUM(CASE WHEN usage_status='UNAVAILABLE' THEN 1 ELSE 0 END)
                         AS usage_unavailable_count,
                       GROUP_CONCAT(DISTINCT backend) AS backends,
                       GROUP_CONCAT(DISTINCT model) AS models,
                       COALESCE(SUM(input_tokens),0) AS input_tokens,
                       COALESCE(SUM(output_tokens),0) AS output_tokens,
                       COALESCE(SUM(cached_tokens),0) AS cached_tokens,
                       COALESCE(SUM(reasoning_tokens),0) AS reasoning_tokens,
                       COALESCE(SUM(total_tokens),0) AS total_tokens,
                       COALESCE(SUM(duration_ms),0) AS duration_ms
                FROM deduplicated
                """,
                (run_id,),
            ).fetchone()
            observed_domain_rows = conn.execute(
                """
                SELECT COALESCE(canonical_url,source_url) AS url
                FROM research_tool_events
                WHERE run_id=? AND COALESCE(canonical_url,source_url) IS NOT NULL
                """,
                (run_id,),
            ).fetchall()
            fetched_domain_rows = conn.execute(
                """
                SELECT source_domain,fetch_status,verification_status
                FROM research_sources WHERE run_id=?
                """,
                (run_id,),
            ).fetchall()
            accepted_domain_rows = conn.execute(
                """
                SELECT DISTINCT e.source_domain
                FROM research_evidence e
                JOIN research_claims c ON c.claim_id=e.claim_id
                WHERE c.research_run_id=? AND c.validation_status='accepted'
                  AND c.materialization_status!='ORPHANED'
                  AND e.audit_status='ACTIVE'
                  AND e.source_audit_status='ACTIVE'
                """,
                (run_id,),
            ).fetchall()
            claim_metadata_rows = conn.execute(
                """
                SELECT payload_json FROM research_claims
                WHERE research_run_id=? AND validation_status='accepted'
                  AND materialization_status='MATERIALIZED'
                  AND source_audit_status='ACTIVE'
                """,
                (run_id,),
            ).fetchall()
        usage = json.loads(run["usage_json"] or "{}")
        cost = json.loads(run["cost_json"] or "{}")
        request = json.loads(run["request_json"] or "{}")
        result = json.loads(run["result_json"] or "{}")
        accepted = int(claims["accepted"] or 0)
        extracted = max(
            int(claims["extracted"] or 0),
            _declared_claims(steps),
        )
        invocation_usage = {
            key: int(invocation_stats[key] or 0)
            for key in (
                "input_tokens",
                "output_tokens",
                "cached_tokens",
                "reasoning_tokens",
                "total_tokens",
            )
        }
        if int(invocation_stats["attempted_count"] or 0):
            usage = invocation_usage
        token_total = int(usage.get("total_tokens") or 0) or sum(
            int(usage.get(key) or 0) for key in ("input_tokens", "output_tokens")
        )
        claim_rejection_reasons: dict[str, int] = {}
        warning_count = len(json.loads(run["warnings_json"] or "[]"))
        for row in rejected_claim_rows:
            claim_warnings = json.loads(row["warnings_json"] or "[]")
            warning_count += len(claim_warnings)
            primary_reason = str(claim_warnings[0] if claim_warnings else "unspecified")
            claim_rejection_reasons[primary_reason] = (
                claim_rejection_reasons.get(primary_reason, 0) + 1
            )
        declared_sources = _declared_sources(steps)
        observed_sources = int(tool["new_sources"] or 0)
        gateway_discovered = int(source_stats["discovered"] or 0)
        gateway_fetched = int(source_stats["fetched"] or 0)
        gateway_verified = int(source_stats["verified"] or 0)
        gateway_rejected = int(source_stats["rejected"] or 0)
        observed_domains = sorted(
            {
                (urlsplit(str(row["url"])).hostname or "").lower().removeprefix("www.")
                for row in observed_domain_rows
                if row["url"]
            }
            - {""}
        )
        fetched_domains = sorted(
            {
                str(row["source_domain"])
                for row in fetched_domain_rows
                if row["fetch_status"] == "FETCHED" and row["source_domain"]
            }
        )
        verified_domains = sorted(
            {
                str(row["source_domain"])
                for row in fetched_domain_rows
                if row["verification_status"] == "VERIFIED" and row["source_domain"]
            }
        )
        accepted_domains = sorted(
            {str(row["source_domain"]) for row in accepted_domain_rows if row["source_domain"]}
        )
        freshness_distribution: dict[str, int] = {}
        acquisition_distribution: dict[str, int] = {}
        for row in claim_metadata_rows:
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, ValueError):
                continue
            freshness = str(payload.get("freshness_status") or "UNAVAILABLE")
            acquisition = str(payload.get("acquisition_method") or "unavailable")
            freshness_distribution[freshness] = (
                freshness_distribution.get(freshness, 0) + 1
            )
            acquisition_distribution[acquisition] = (
                acquisition_distribution.get(acquisition, 0) + 1
            )
        backend_values = sorted(
            {
                value
                for value in str(invocation_stats["backends"] or "").split(",")
                if value
            }
        )
        model_values = sorted(
            {
                value
                for value in str(invocation_stats["models"] or "").split(",")
                if value
            }
        )
        estimate = ModelPricingService(self.settings.model_pricing_path).estimate(
            backend=backend_values[0] if len(backend_values) == 1 else "mixed",
            model=model_values[0] if len(model_values) == 1 else None,
            input_tokens=int(usage.get("input_tokens") or 0),
            cached_tokens=int(usage.get("cached_tokens") or 0),
            output_tokens=int(usage.get("output_tokens") or 0),
        )
        cost_contract = (
            {
                "cost": cost,
                "cost_status": "actual",
                "billing_basis": "backend_reported",
            }
            if cost
            else estimate
        )
        if not cost and cost_contract["cost_status"] == "pricing_unavailable":
            cost_contract["cost_status"] = "cost_unavailable"
        exclusive_service_ms = sum(
            int(step["duration_ms"] or 0)
            for step in steps
            if str(step["step_name"])
            not in {
                "PLAN",
                "SEARCH",
                "OPEN_SOURCE",
                "VERIFY",
                "PERSIST",
                "READ_BACK",
                "MATERIALIZE",
            }
        )
        ai_ms = int(invocation_stats["duration_ms"] or 0)
        fetch_ms = int(source_stats["fetch_duration_ms"] or 0)
        verification_ms = sum(
            int(row["duration_ms"] or 0) for row in verification_stats
        )
        persistence_ms = sum(
            int(step["duration_ms"] or 0)
            for step in steps
            if str(step["step_name"]) in {"PERSIST", "READ_BACK", "MATERIALIZE"}
        )
        wall_clock_ms = None
        if run["started_at"] and run["completed_at"]:
            from datetime import datetime

            started = datetime.fromisoformat(
                str(run["started_at"]).replace("Z", "+00:00")
            )
            completed = datetime.fromisoformat(
                str(run["completed_at"]).replace("Z", "+00:00")
            )
            wall_clock_ms = max(int((completed - started).total_seconds() * 1000), 0)
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
                "total_tokens": int(usage.get("total_tokens") or token_total),
            },
            "cost": cost_contract.get("cost"),
            "cost_status": cost_contract["cost_status"],
            "billing_basis": cost_contract["billing_basis"],
            "no_data_reason": result.get("no_data_reason"),
            "phase_duration_ms": {
                str(step["step_name"]): int(step["duration_ms"] or 0) for step in steps
            },
            "duration_ms": {
                "ai": ai_ms,
                "fetch": fetch_ms,
                "verification": verification_ms,
                "persistence": persistence_ms,
                "service_exclusive": exclusive_service_ms,
                "exclusive_total": (
                    ai_ms
                    + fetch_ms
                    + verification_ms
                    + persistence_ms
                    + exclusive_service_ms
                ),
                "wall_clock": wall_clock_ms,
            },
            "backend": {
                "used": backend_values,
                "invocations": int(invocation_stats["completed_count"] or 0),
                "attempted": int(invocation_stats["attempted_count"] or 0),
                "completed": int(invocation_stats["completed_count"] or 0),
                "aborted": int(invocation_stats["aborted_count"] or 0),
                "usage_status": (
                    "partially_unavailable"
                    if int(invocation_stats["usage_unavailable_count"] or 0)
                    and int(invocation_stats["completed_count"] or 0)
                    else "unavailable"
                    if int(invocation_stats["usage_unavailable_count"] or 0)
                    else "available"
                ),
                "usage_unavailable_invocations": int(
                    invocation_stats["usage_unavailable_count"] or 0
                ),
            },
            "backend_invocations": int(
                invocation_stats["completed_count"] or 0
            ),
            "fetched_sources": gateway_fetched,
            "verified_sources": int(verified_sources or 0),
            "accepted_claims": accepted,
            "rejected_claims": int(claims["rejected"] or 0),
            "input_tokens": int(usage.get("input_tokens") or 0),
            "output_tokens": int(usage.get("output_tokens") or 0),
            "freshness_distribution": dict(
                sorted(freshness_distribution.items())
            ),
            "acquisition_method_distribution": dict(
                sorted(acquisition_distribution.items())
            ),
            "tokens_per_accepted_claim": (token_total / accepted if accepted else None),
            "cost_per_accepted_claim": (
                _cost_value(cost) / accepted if cost and accepted else None
            ),
            "searches_per_new_source": (
                int(tool["searches"] or 0) / observed_sources if observed_sources else None
            ),
            "threshold_warnings": json.loads(run["threshold_warnings_json"] or "[]"),
            "warning_count": warning_count,
            "loop_detections": int(run["loop_detection_count"] or 0),
            "continuation_count": int(run["continuation_count"] or 0),
            "checkpoint": json.loads(run["checkpoint_json"] or "{}"),
            "progress": {
                "completed_phases": sum(1 for step in steps if step["status"] == "COMPLETED"),
                "recorded_phases": len(steps),
                "latest_phase": (str(steps[-1]["step_name"]) if steps else None),
            },
            "sources": {
                "model_declared": declared_sources,
                "observed": observed_sources,
                "discovered": gateway_discovered,
                "fetched": gateway_fetched,
                "verified": int(verified_sources or 0),
                "gateway_verified": gateway_verified,
                "rejected": gateway_rejected,
                "unverified": max(
                    gateway_discovered - gateway_verified,
                    0,
                ),
                "gateway_rejection_reasons": [
                    {
                        "status": str(row["status"]),
                        "reason": str(row["reason"]),
                        "count": int(row["count"] or 0),
                    }
                    for row in verification_stats
                    if str(row["status"]) == "REJECTED"
                ],
                "rejection_reasons": [
                    {"status": "REJECTED", "reason": reason, "count": count}
                    for reason, count in sorted(claim_rejection_reasons.items())
                ],
                "observed_source_domains": observed_domains,
                "fetched_source_domains": fetched_domains,
                "verified_source_domains": verified_domains,
                "accepted_claim_source_domains": accepted_domains,
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
        if str(step["step_name"]) not in {"SEARCH", "OPEN_SOURCE"}:
            continue
        try:
            output = json.loads(step["output_json"] or "{}")
        except json.JSONDecodeError:
            continue
        count = len([item for item in output.get("sources") or [] if isinstance(item, dict)])
        if count:
            return count
    return 0


def _declared_claims(steps: list[Any]) -> int:
    for step in steps:
        if str(step["step_name"]) != "EXTRACT":
            continue
        try:
            output = json.loads(step["output_json"] or "{}")
        except json.JSONDecodeError:
            return 0
        return len([item for item in output.get("claims") or [] if isinstance(item, dict)])
    return 0


def _cost_value(cost: dict[str, Any]) -> float:
    for key in ("total_cost_usd", "cost_usd", "cost"):
        if cost.get(key) is not None:
            return float(cost[key])
    return 0.0
