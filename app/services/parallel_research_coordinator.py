from __future__ import annotations

import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, Callable

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.ai_research_job_repository import TERMINAL_JOB_STATUSES
from app.services.ai_research_job_service import AIResearchJobService
from app.services.research_gap_manifest import TOPIC_PROFILES
from app.services.research_profiles import PROFILES
from app.services.research_runtime_repository import ResearchRuntimeRepository


class ParallelResearchCoordinator:
    """Persistent parent/child coordinator; children exchange only DB artifacts."""

    def __init__(self, settings: Settings, *, read_only: bool = False) -> None:
        self.settings = settings
        if read_only:
            self.jobs = None
            self.runs = None
            return
        self.jobs = AIResearchJobService(settings)
        self.runs = ResearchRuntimeRepository(settings)
        migrate_database(settings.database_path)

    def create_parent(
        self,
        manifest: dict[str, Any],
        *,
        correlation_id: str,
        force: bool = False,
        authorized_live_smoke: bool = False,
    ) -> dict[str, Any]:
        backend = str(self.settings.research_backend).lower()
        if self.jobs is None or self.runs is None:
            raise RuntimeError("read_only_parallel_coordinator_cannot_create_parent")
        if backend not in {"codex_cli", "openai_api"}:
            raise ValueError(f"unsupported_research_backend:{backend}")
        if not force:
            existing = self._active_parent_for_correlation(correlation_id)
            if existing is not None:
                existing["created"] = False
                existing["child_jobs"] = []
                existing["child_job_ids"] = [
                    str(item["child_job_id"])
                    for item in existing.get("children") or []
                ]
                return existing
        parent_run_id = f"prun-{uuid.uuid4()}"
        now = _now()
        items = {
            str(item["topic"]): item
            for item in manifest.get("items") or []
            if item.get("required_action") == "AGENT_RESEARCH"
        }
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO research_parent_runs(
                  parent_run_id,parent_job_id,symbol,status,snapshot_id,manifest_id,
                  requested_backend,concurrency_limit,expected_child_count,
                  terminal_child_count,checkpoint_json,telemetry_json,created_at,
                  started_at,completed_at,updated_at
                ) VALUES (?,NULL,'MNQ',?,NULL,?,?,?,?,0,'{}','{}',?,?,?,?)
                """,
                (
                    parent_run_id,
                    "PENDING" if items else "SUCCEEDED",
                    manifest["manifest_id"],
                    backend,
                    int(self.settings.research_parallelism),
                    len(items),
                    now,
                    now if items else None,
                    now if not items else None,
                    now,
                ),
            )
            conn.execute(
                "UPDATE research_gap_manifests SET parent_run_id=? WHERE manifest_id=?",
                (parent_run_id, manifest["manifest_id"]),
            )
            conn.commit()
        child_jobs: list[dict[str, Any]] = []
        for ordinal, topic in enumerate(sorted(items), start=1):
            profile_id = TOPIC_PROFILES[topic]
            profile = PROFILES[profile_id]
            compact_item = items[topic]
            payload = {
                "parent_run_id": parent_run_id,
                "gap_manifest_id": manifest["manifest_id"],
                "gap": compact_item,
                "missing_fields": list(compact_item.get("missing_fields") or []),
                "planned_queries": list(profile.planned_queries),
                "priority_domains": list(profile.priority_domains),
                "source_snapshot_id": manifest.get("source_snapshot_id"),
                "context_date": str(manifest.get("generated_at") or "")[:10],
                "authorized_live_smoke": authorized_live_smoke,
            }
            job, _created = self.jobs.enqueue_explicit(
                job_type=profile_id,
                symbol="MNQ",
                correlation_id=correlation_id,
                request_payload=payload,
                pending_fields=list(compact_item.get("missing_fields") or []),
                force=force,
                parent_run_id=parent_run_id,
                specialized_topic=topic,
                child_ordinal=ordinal,
            )
            child_run = self.runs.ensure_run(job, profile.profile_id, profile.prompt_version)
            with connect_sqlite(self.settings.database_path) as conn:
                conn.execute(
                    """
                    INSERT INTO research_parent_children(
                      parent_run_id,child_job_id,child_run_id,topic,profile_id,
                      status,ordinal,result_checksum,created_at,updated_at
                    ) VALUES (?,?,?,?,?,'PENDING',?,NULL,?,?)
                    """,
                    (
                        parent_run_id,
                        job["job_id"],
                        child_run["run_id"],
                        topic,
                        profile_id,
                        ordinal,
                        now,
                        now,
                    ),
                )
                conn.commit()
            child_jobs.append(job)
        return {
            "created": True,
            "parent_run_id": parent_run_id,
            "run_id": parent_run_id,
            "status": "PENDING" if child_jobs else "SUCCEEDED",
            "manifest_id": manifest["manifest_id"],
            "backend": backend,
            "child_jobs": child_jobs,
            "child_job_ids": [str(job["job_id"]) for job in child_jobs],
            "concurrency_limit": int(self.settings.research_parallelism),
        }

    def _active_parent_for_correlation(
        self,
        correlation_id: str,
    ) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT DISTINCT p.parent_run_id
                FROM research_parent_runs p
                JOIN research_parent_children c
                  ON c.parent_run_id=p.parent_run_id
                JOIN ai_research_jobs j
                  ON j.job_id=c.child_job_id
                WHERE j.correlation_id=?
                  AND p.status IN ('PENDING','RUNNING')
                ORDER BY p.created_at DESC
                LIMIT 1
                """,
                (correlation_id,),
            ).fetchone()
        return self.get_parent(str(row["parent_run_id"])) if row else None

    def reconcile_parent(self, parent_run_id: str) -> dict[str, Any]:
        if self.jobs is None or self.runs is None:
            raise RuntimeError("read_only_parallel_coordinator_cannot_reconcile")
        now = _now()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            parent = conn.execute(
                "SELECT * FROM research_parent_runs WHERE parent_run_id=?",
                (parent_run_id,),
            ).fetchone()
            if parent is None:
                conn.rollback()
                raise ValueError("research_parent_run_not_found")
            children = conn.execute(
                """
                SELECT c.*,j.status AS job_status,j.last_error,
                       r.status AS run_status,r.result_json,r.metrics_json,
                       r.usage_json,r.cost_json,r.warnings_json,
                       r.started_at AS run_started_at,r.completed_at AS run_completed_at
                FROM research_parent_children c
                JOIN ai_research_jobs j ON j.job_id=c.child_job_id
                LEFT JOIN research_runs r ON r.run_id=c.child_run_id
                WHERE c.parent_run_id=? ORDER BY c.ordinal
                """,
                (parent_run_id,),
            ).fetchall()
            child_rows = [dict(child) for child in children]
            statuses: list[str] = []
            for child in children:
                status = _verified_child_status(dict(child))
                statuses.append(status)
                checksum = hashlib.sha256(
                    str(child["result_json"] or "").encode("utf-8")
                ).hexdigest() if child["result_json"] else None
                conn.execute(
                    """
                    UPDATE research_parent_children
                    SET status=?,result_checksum=?,updated_at=?
                    WHERE parent_run_id=? AND child_job_id=?
                      AND (status!=? OR COALESCE(result_checksum,'')!=COALESCE(?,''))
                    """,
                    (
                        status,
                        checksum,
                        now,
                        parent_run_id,
                        child["child_job_id"],
                        status,
                        checksum,
                    ),
                )
            parent_status = _parent_status(statuses)
            terminal_count = sum(status in TERMINAL_JOB_STATUSES for status in statuses)
            completed_at = (
                str(parent["completed_at"] or now)
                if parent_status in {"SUCCEEDED", "PARTIAL", "NO_DATA", "FAILED"}
                else None
            )
            checkpoint = {
                "child_statuses": statuses,
                "terminal_count": terminal_count,
            }
            telemetry = _aggregate_parent_telemetry(
                conn,
                dict(parent),
                child_rows,
                statuses,
                completed_at=completed_at,
            )
            checkpoint_json = json.dumps(checkpoint, sort_keys=True, separators=(",", ":"))
            telemetry_json = json.dumps(telemetry, sort_keys=True, separators=(",", ":"))
            conn.execute(
                """
                UPDATE research_parent_runs
                SET status=?,terminal_child_count=?,started_at=COALESCE(started_at,?),
                    completed_at=CASE
                      WHEN ? IS NULL THEN completed_at
                      ELSE COALESCE(completed_at,?)
                    END,
                    checkpoint_json=?,telemetry_json=?,updated_at=?
                WHERE parent_run_id=?
                  AND (
                    status!=?
                    OR terminal_child_count!=?
                    OR checkpoint_json!=?
                    OR telemetry_json!=?
                    OR (? IS NOT NULL AND completed_at IS NULL)
                  )
                """,
                (
                    parent_status,
                    terminal_count,
                    now,
                    completed_at,
                    completed_at,
                    checkpoint_json,
                    telemetry_json,
                    now,
                    parent_run_id,
                    parent_status,
                    terminal_count,
                    checkpoint_json,
                    telemetry_json,
                    completed_at,
                ),
            )
            conn.commit()
        return self.get_parent(parent_run_id)

    def get_parent(self, parent_run_id: str) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            parent = conn.execute(
                "SELECT * FROM research_parent_runs WHERE parent_run_id=?",
                (parent_run_id,),
            ).fetchone()
            children = conn.execute(
                """
                SELECT * FROM research_parent_children
                WHERE parent_run_id=? ORDER BY ordinal
                """,
                (parent_run_id,),
            ).fetchall()
            manifest = (
                conn.execute(
                    "SELECT manifest_json FROM research_gap_manifests WHERE manifest_id=?",
                    (parent["manifest_id"],),
                ).fetchone()
                if parent is not None
                else None
            )
            counts = (
                _materialized_parent_counts(conn, [str(row["child_run_id"]) for row in children])
                if parent is not None
                else {"claim_count": 0, "evidence_count": 0}
            )
        if parent is None:
            raise ValueError("research_parent_run_not_found")
        output = dict(parent)
        output["checkpoint"] = json.loads(output.pop("checkpoint_json") or "{}")
        output["telemetry"] = json.loads(output.pop("telemetry_json") or "{}")
        output["children"] = [dict(row) for row in children]
        manifest_payload = json.loads(manifest["manifest_json"] or "{}") if manifest else {}
        required_topics = _manifest_required_topics(manifest_payload)
        failed_topics = sorted(
            str(row["topic"])
            for row in children
            if str(row["status"]) in {"FAILED", "LOOP_DETECTED", "TIMED_OUT", "CANCELLED", "REJECTED"}
        )
        output.update(
            {
                "research_status": output["status"],
                "snapshot_status": (
                    "MATERIALIZING"
                    if output.get("snapshot_id") == "MATERIALIZING"
                    else "MATERIALIZED"
                    if output.get("snapshot_id")
                    else "NOT_MATERIALIZED"
                ),
                "required_topics": required_topics,
                "failed_topics": failed_topics,
                "blocking_gaps": [f"failed_topic:{topic}" for topic in failed_topics],
                **counts,
            }
        )
        return output

    def claim_materialization(self, parent_run_id: str) -> bool:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE research_parent_runs SET snapshot_id='MATERIALIZING',updated_at=?
                WHERE parent_run_id=? AND snapshot_id IS NULL
                  AND status IN ('SUCCEEDED','PARTIAL','NO_DATA')
                """,
                (_now(), parent_run_id),
            )
            conn.commit()
        return int(cursor.rowcount or 0) == 1

    def finish_materialization(
        self,
        parent_run_id: str,
        snapshot_id: str | None,
    ) -> None:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_parent_runs SET snapshot_id=?,updated_at=?
                WHERE parent_run_id=? AND snapshot_id='MATERIALIZING'
                """,
                (snapshot_id, _now(), parent_run_id),
            )
            conn.commit()

    def execute_children(
        self,
        child_jobs: list[dict[str, Any]],
        processor: Callable[[dict[str, Any]], Any],
    ) -> list[Any]:
        """Bounded test/embedded runner; production workers use the same DB leases."""
        with ThreadPoolExecutor(
            max_workers=int(self.settings.research_parallelism),
            thread_name_prefix="research-child",
        ) as pool:
            return list(pool.map(processor, child_jobs))


def _verified_child_status(child: dict[str, Any]) -> str:
    job_status = str(child.get("job_status") or child.get("status") or "PENDING")
    run_status = str(child.get("run_status") or "")
    if job_status not in TERMINAL_JOB_STATUSES:
        return job_status
    if job_status in {"SUCCEEDED", "PARTIAL"} and run_status != job_status:
        return "FAILED"
    if job_status in {"SUCCEEDED", "PARTIAL"}:
        try:
            result = json.loads(child.get("result_json") or "{}")
        except (TypeError, ValueError):
            return "FAILED"
        accepted = int(result.get("accepted_count") or 0)
        if (
            accepted != int(result.get("persisted_count") or 0)
            or accepted != int(result.get("read_back_count") or 0)
        ):
            return "FAILED"
    return job_status


def _parent_status(statuses: list[str]) -> str:
    if not statuses:
        return "SUCCEEDED"
    if any(status not in TERMINAL_JOB_STATUSES for status in statuses):
        return "RUNNING"
    successful = sum(status == "SUCCEEDED" for status in statuses)
    no_data = sum(status == "NO_DATA" for status in statuses)
    partial = sum(status == "PARTIAL" for status in statuses)
    if successful == 0 and partial == 0 and no_data == len(statuses):
        return "NO_DATA"
    if successful == 0 and partial == 0 and no_data == 0:
        return "FAILED"
    if partial or successful + no_data < len(statuses):
        return "PARTIAL"
    return "SUCCEEDED"


def _manifest_required_topics(manifest: dict[str, Any]) -> list[str]:
    topics = {
        str(item.get("topic"))
        for item in manifest.get("items") or []
        if isinstance(item, dict)
        and item.get("required_action") == "AGENT_RESEARCH"
        and item.get("topic")
    }
    topics.update(str(item) for item in manifest.get("agent_topics") or [] if item)
    return sorted(topics)


def _materialized_parent_counts(
    conn: Any,
    run_ids: list[str],
) -> dict[str, int]:
    active_run_ids = [item for item in run_ids if item]
    if not active_run_ids:
        return {"claim_count": 0, "evidence_count": 0}
    placeholders = ",".join("?" for _ in active_run_ids)
    claim_count = int(
        conn.execute(
            f"""
            SELECT COUNT(*) FROM research_claims
            WHERE research_run_id IN ({placeholders})
              AND validation_status='accepted'
              AND materialization_status='MATERIALIZED'
              AND source_audit_status='ACTIVE'
            """,
            active_run_ids,
        ).fetchone()[0]
    )
    evidence_count = int(
        conn.execute(
            f"""
            SELECT COUNT(*) FROM research_evidence e
            JOIN research_claims c ON c.claim_id=e.claim_id
            WHERE c.research_run_id IN ({placeholders})
              AND c.validation_status='accepted'
              AND c.materialization_status='MATERIALIZED'
              AND c.source_audit_status='ACTIVE'
              AND e.audit_status='ACTIVE'
              AND e.source_audit_status='ACTIVE'
            """,
            active_run_ids,
        ).fetchone()[0]
    )
    return {"claim_count": claim_count, "evidence_count": evidence_count}


def _aggregate_parent_telemetry(
    conn: Any,
    parent: dict[str, Any],
    children: list[dict[str, Any]],
    statuses: list[str],
    *,
    completed_at: str | None,
) -> dict[str, Any]:
    totals = {
        "searches": 0,
        "opened_sources": 0,
        "fetched_sources": 0,
        "verified_sources": 0,
        "candidate_claims": 0,
        "accepted_claims": 0,
        "rejected_claims": 0,
    }
    usage = {
        "input_tokens": 0,
        "output_tokens": 0,
        "cached_tokens": 0,
        "reasoning_tokens": 0,
        "total_tokens": 0,
    }
    warnings: set[str] = set()
    rejection_reasons: dict[tuple[str, str], int] = {}
    child_statuses: list[dict[str, Any]] = []
    costs: list[float] = []
    for index, child in enumerate(children):
        metrics = _load_json_object(child.get("metrics_json"))
        metrics_usage = metrics.get("usage") if isinstance(metrics.get("usage"), dict) else {}
        sources = metrics.get("sources") if isinstance(metrics.get("sources"), dict) else {}
        totals["searches"] += int(metrics.get("searches") or 0)
        totals["opened_sources"] += int(metrics.get("opened_sources") or 0)
        totals["fetched_sources"] += int(sources.get("fetched") or 0)
        totals["verified_sources"] += int(sources.get("verified") or 0)
        totals["candidate_claims"] += int(metrics.get("claims_extracted") or 0)
        totals["accepted_claims"] += int(metrics.get("claims_accepted") or 0)
        totals["rejected_claims"] += int(metrics.get("claims_rejected") or 0)
        for key in usage:
            usage[key] += int(metrics_usage.get(key) or 0)
        warnings.update(str(item) for item in metrics.get("threshold_warnings") or [] if item)
        warnings.update(str(item) for item in _load_json_list(child.get("warnings_json")) if item)
        for item in sources.get("rejection_reasons") or []:
            if not isinstance(item, dict):
                continue
            key = (str(item.get("status") or "REJECTED"), str(item.get("reason") or "unknown"))
            rejection_reasons[key] = rejection_reasons.get(key, 0) + int(item.get("count") or 0)
        cost = _load_json_object(child.get("cost_json"))
        if cost.get("total_cost_usd") is not None:
            costs.append(float(cost["total_cost_usd"]))
        child_statuses.append(
            {
                "topic": child.get("topic"),
                "job_id": child.get("child_job_id"),
                "run_id": child.get("child_run_id"),
                "status": statuses[index],
                "warning_count": len(metrics.get("threshold_warnings") or []),
                "last_error": child.get("last_error"),
            }
        )
    run_ids = [str(child.get("child_run_id")) for child in children if child.get("child_run_id")]
    invocation_counts = {
        "attempted": 0,
        "completed": 0,
        "aborted": 0,
        "usage_unavailable": 0,
    }
    if run_ids:
        placeholders = ",".join("?" for _ in run_ids)
        invocation_row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT invocation_id) AS attempted,
                   COUNT(DISTINCT CASE WHEN lifecycle_status='COMPLETED'
                                      THEN invocation_id END) AS completed,
                   COUNT(DISTINCT CASE WHEN lifecycle_status='ABORTED'
                                      THEN invocation_id END) AS aborted,
                   COUNT(DISTINCT CASE WHEN usage_status='UNAVAILABLE'
                                       THEN invocation_id END) AS usage_unavailable
            FROM research_backend_invocations
            WHERE run_id IN ({placeholders})
            """,
            run_ids,
        ).fetchone()
        invocation_counts = {
            key: int(invocation_row[key] or 0)
            for key in invocation_counts
        }
    if invocation_counts["attempted"] == 0:
        completed_fallback = sum(
            int(
                (
                    _load_json_object(child.get("metrics_json")).get("backend")
                    or {}
                ).get("invocations")
                or 0
            )
            for child in children
        )
        invocation_counts = {
            "attempted": completed_fallback,
            "completed": completed_fallback,
            "aborted": 0,
            "usage_unavailable": 0,
        }
    wall_clock_seconds = None
    if parent.get("started_at") and completed_at:
        started = datetime.fromisoformat(str(parent["started_at"]).replace("Z", "+00:00"))
        completed = datetime.fromisoformat(str(completed_at).replace("Z", "+00:00"))
        wall_clock_seconds = max(round((completed - started).total_seconds(), 3), 0)
    return {
        "aggregation_version": "parent-replay-v1",
        "deduplication": {
            "invocations": "invocation_id",
            "sources": "child_run_id/source_id",
            "tools": "child_run_id/tool_action_fingerprint",
        },
        "child_statuses": child_statuses,
        "backend_invocations": invocation_counts["completed"],
        "backend_invocations_attempted": invocation_counts["attempted"],
        "backend_invocations_completed": invocation_counts["completed"],
        "backend_invocations_aborted": invocation_counts["aborted"],
        "backend_usage_status": (
            "partially_unavailable"
            if invocation_counts["usage_unavailable"]
            and invocation_counts["completed"]
            else "unavailable"
            if invocation_counts["usage_unavailable"]
            else "available"
        ),
        "backend_usage_unavailable_invocations": invocation_counts[
            "usage_unavailable"
        ],
        **totals,
        "usage": usage,
        "wall_clock_seconds": wall_clock_seconds,
        "cost": {"total_cost_usd": round(sum(costs), 10)} if costs else None,
        "cost_status": "available" if costs else "cost_unavailable",
        "warnings": sorted(warnings),
        "rejection_reasons": [
            {"status": key[0], "reason": key[1], "count": count}
            for key, count in sorted(rejection_reasons.items())
        ],
    }


def _load_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        restored = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return restored if isinstance(restored, dict) else {}


def _load_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    try:
        restored = json.loads(value or "[]")
    except (TypeError, ValueError):
        return []
    return restored if isinstance(restored, list) else []


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
