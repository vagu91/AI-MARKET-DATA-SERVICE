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

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
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
                SELECT c.*,j.status AS job_status,r.status AS run_status,r.result_json
                FROM research_parent_children c
                JOIN ai_research_jobs j ON j.job_id=c.child_job_id
                LEFT JOIN research_runs r ON r.run_id=c.child_run_id
                WHERE c.parent_run_id=? ORDER BY c.ordinal
                """,
                (parent_run_id,),
            ).fetchall()
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
                    """,
                    (status, checksum, now, parent_run_id, child["child_job_id"]),
                )
            parent_status = _parent_status(statuses)
            terminal_count = sum(status in TERMINAL_JOB_STATUSES for status in statuses)
            conn.execute(
                """
                UPDATE research_parent_runs
                SET status=?,terminal_child_count=?,started_at=COALESCE(started_at,?),
                    completed_at=?,checkpoint_json=?,updated_at=?
                WHERE parent_run_id=?
                """,
                (
                    parent_status,
                    terminal_count,
                    now,
                    now if parent_status in {"SUCCEEDED", "PARTIAL", "NO_DATA", "FAILED"} else None,
                    json.dumps(
                        {"child_statuses": statuses, "terminal_count": terminal_count},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    now,
                    parent_run_id,
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
        if parent is None:
            raise ValueError("research_parent_run_not_found")
        output = dict(parent)
        output["checkpoint"] = json.loads(output.pop("checkpoint_json") or "{}")
        output["telemetry"] = json.loads(output.pop("telemetry_json") or "{}")
        output["children"] = [dict(row) for row in children]
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


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
