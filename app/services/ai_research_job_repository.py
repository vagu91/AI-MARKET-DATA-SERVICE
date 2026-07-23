from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.codex_runtime_contract import sanitize_diagnostic


ACTIVE_JOB_STATUSES = {"PENDING", "RUNNING", "RETRY_SCHEDULED"}
TERMINAL_JOB_STATUSES = {
    "SUCCEEDED",
    "PARTIAL",
    "NO_DATA",
    "REJECTED",
    "FAILED",
    "LOOP_DETECTED",
    "TIMED_OUT",
    "CANCELLED",
}
ALL_JOB_STATUSES = ACTIVE_JOB_STATUSES | TERMINAL_JOB_STATUSES


class AIResearchJobRepository:
    def __init__(self, settings: Settings, *, clock: Callable[[], datetime] | None = None) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        migrate_database(settings.database_path)

    def enqueue(
        self,
        *,
        idempotency_key: str,
        job_type: str,
        symbol: str,
        correlation_id: str,
        request_payload: dict[str, Any],
        policy_version: str,
        prompt_version: str,
        event_key: str | None = None,
        priority: int = 100,
        max_attempts: int | None = None,
        pending_fields: list[str] | None = None,
        scope_key: str | None = None,
        generation: str | None = None,
        run_window: str | None = None,
        snapshot_id: str | None = None,
        allow_requeue_terminal: bool = False,
        profile_id: str | None = None,
        input_fingerprint: str | None = None,
        capability_status: str | None = None,
        retry_class: str | None = None,
        retry_deadline_at: str | None = None,
        parent_job_id: str | None = None,
        parent_run_id: str | None = None,
        specialized_topic: str | None = None,
        child_ordinal: int | None = None,
    ) -> tuple[dict[str, Any], bool]:
        now = self._iso(self.clock())
        job_id = f"airj-{uuid.uuid4()}"
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if scope_key:
                active = conn.execute(
                    "SELECT * FROM ai_research_jobs WHERE scope_key=? AND status IN ('PENDING','RUNNING','RETRY_SCHEDULED') ORDER BY created_at DESC,rowid DESC LIMIT 1",
                    (scope_key,),
                ).fetchone()
                if active is not None:
                    conn.commit()
                    return self._row(active), False
            existing = conn.execute(
                "SELECT * FROM ai_research_jobs WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                if not allow_requeue_terminal or str(existing["status"]) not in TERMINAL_JOB_STATUSES:
                    conn.commit()
                    return self._row(existing), False
                idempotency_key = f"{idempotency_key}:{uuid.uuid4()}"
            conn.execute(
                """
                INSERT INTO ai_research_jobs(
                  job_id,idempotency_key,job_type,symbol,event_key,correlation_id,status,priority,
                  request_payload_json,policy_version,prompt_version,attempts,max_attempts,created_at,
                  pending_fields_json,updated_at,snapshot_id,generation,run_window,scope_key,
                  profile_id,input_fingerprint,capability_status,retry_class,retry_deadline_at
                  ,parent_job_id,parent_run_id,specialized_topic,child_ordinal
                ) VALUES (?,?,?,?,?,?,'PENDING',?,?,?,?,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    job_id, idempotency_key, job_type, symbol.upper(), event_key, correlation_id,
                    int(priority), self._json(request_payload), policy_version, prompt_version,
                    int(max_attempts or self.settings.ai_job_max_attempts), now,
                    self._json(pending_fields or []), now, snapshot_id, generation, run_window, scope_key,
                    profile_id, input_fingerprint, capability_status,
                    retry_class, retry_deadline_at,
                    parent_job_id, parent_run_id, specialized_topic, child_ordinal,
                ),
            )
            conn.commit()
        return self.get(job_id), True

    def get(self, job_id: str) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute("SELECT * FROM ai_research_jobs WHERE job_id=?", (job_id,)).fetchone()
            attempts = conn.execute(
                """
                SELECT attempt_number,worker_id,status,started_at,completed_at,error,
                       output_checksum,error_category,exit_code,retry_classification,
                       diagnostic_json
                FROM ai_research_job_attempts
                WHERE job_id=? ORDER BY attempt_number
                """,
                (job_id,),
            ).fetchall()
        if row is None:
            return None
        restored = self._row(row)
        restored["attempt_history"] = [self._attempt_row(item) for item in attempts]
        return restored

    def latest(
        self,
        *,
        limit: int = 20,
        symbol: str | None = None,
        snapshot_id: str | None = None,
        event_keys: list[str] | None = None,
        view: str = "full",
    ) -> list[dict[str, Any]]:
        if view not in {"full", "compact"}:
            raise ValueError("unsupported_latest_jobs_view")
        clauses: list[str] = ["source_audit_status='ACTIVE'"]
        values: list[Any] = []
        if symbol:
            clauses.append("symbol=?")
            values.append(symbol.upper())
        if snapshot_id:
            clauses.append("job_id IN (SELECT job_id FROM market_context_snapshot_jobs WHERE snapshot_id=?)")
            values.append(snapshot_id)
        if event_keys is not None:
            if not event_keys:
                return []
            clauses.append(f"event_key IN ({','.join('?' for _ in event_keys)})")
            values.extend(event_keys)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.append(limit)
        projection = (
            "job_id,job_type,symbol,event_key,correlation_id,status,priority,"
            "policy_version,prompt_version,attempts,max_attempts,created_at,"
            "started_at,completed_at,next_retry_at,last_error,updated_at,"
            "generation,run_window,capability_status,retry_class,last_retry_reason"
            if view == "compact"
            else "*"
        )
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                f"SELECT {projection} FROM ai_research_jobs "
                f"{where} ORDER BY created_at DESC,rowid DESC LIMIT ?",
                tuple(values),
            ).fetchall()
        if view == "compact":
            return [dict(row) for row in rows]
        return [self._row(row) for row in rows]

    def status(self) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            counts = conn.execute(
                """
                SELECT status,COUNT(*) AS count FROM ai_research_jobs
                WHERE source_audit_status='ACTIVE' GROUP BY status
                """
            ).fetchall()
            latest = conn.execute(
                "SELECT job_id,status,job_type,correlation_id,created_at,completed_at,last_error "
                "FROM ai_research_jobs WHERE source_audit_status='ACTIVE' "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            oldest = conn.execute(
                """
                SELECT MIN(created_at) oldest FROM ai_research_jobs
                WHERE status IN ('PENDING','RUNNING','RETRY_SCHEDULED')
                  AND source_audit_status='ACTIVE'
                """
            ).fetchone()["oldest"]
            run_metrics = conn.execute(
                """
                SELECT AVG((julianday(completed_at)-julianday(started_at))*86400.0) avg_duration,
                       MAX(CASE WHEN status IN ('SUCCEEDED','PARTIAL') THEN completed_at END) last_success,
                       MAX(data_as_of) latest_data_as_of
                FROM research_runs
                """
            ).fetchone()
            claim_metrics = conn.execute(
                """
                SELECT SUM(CASE WHEN validation_status='accepted' THEN 1 ELSE 0 END) accepted,
                       SUM(CASE WHEN validation_status!='accepted' THEN 1 ELSE 0 END) rejected
                FROM research_claims
                WHERE materialization_status!='ORPHANED'
                """
            ).fetchone()
            unique_domains = conn.execute(
                """
                SELECT COUNT(DISTINCT source_domain)
                FROM research_evidence
                WHERE audit_status='ACTIVE' AND source_audit_status='ACTIVE'
                """
            ).fetchone()[0]
            usage_metrics = conn.execute(
                """
                SELECT COUNT(*) AS run_count,
                       COALESCE(SUM(search_count),0) AS searches,
                       COALESCE(SUM(opened_source_count),0) AS opened_sources,
                       COALESCE(SUM(json_extract(usage_json,'$.input_tokens')),0)
                         AS input_tokens,
                       COALESCE(SUM(json_extract(usage_json,'$.output_tokens')),0)
                         AS output_tokens,
                       COALESCE(SUM(loop_detection_count),0) AS loop_detections,
                       COALESCE(SUM(json_array_length(threshold_warnings_json)),0)
                         AS threshold_warnings
                FROM research_runs
                """
            ).fetchone()
            daily_usage = conn.execute(
                """
                SELECT COUNT(*) AS runs,
                       COALESCE(SUM(search_count),0) AS searches,
                       COALESCE(SUM(opened_source_count),0) AS opened_sources
                FROM research_runs WHERE substr(created_at,1,10)=?
                """,
                (self._iso(self.clock())[:10],),
            ).fetchone()
        run_count = int(usage_metrics["run_count"] or 0)
        by_status = {status: 0 for status in sorted(ALL_JOB_STATUSES)}
        by_status.update({str(row["status"]): int(row["count"]) for row in counts})
        return {
            "worker_enabled": bool(self.settings.ai_worker_enabled),
            "by_status": by_status,
            "active_count": sum(by_status[item] for item in ACTIVE_JOB_STATUSES),
            "latest": dict(latest) if latest else None,
            "metrics": {
                "queue_depth": sum(by_status[item] for item in ACTIVE_JOB_STATUSES),
                "oldest_job_created_at": oldest,
                "running_jobs": by_status["RUNNING"],
                "terminal_counts": {item: by_status[item] for item in sorted(TERMINAL_JOB_STATUSES)},
                "average_duration_seconds": float(run_metrics["avg_duration"] or 0),
                "searches_per_run": (
                    float(usage_metrics["searches"] / run_count)
                    if run_count
                    else 0.0
                ),
                "opened_sources_per_run": (
                    float(usage_metrics["opened_sources"] / run_count)
                    if run_count
                    else 0.0
                ),
                "input_tokens": int(usage_metrics["input_tokens"] or 0),
                "output_tokens": int(usage_metrics["output_tokens"] or 0),
                "threshold_warnings": int(
                    usage_metrics["threshold_warnings"] or 0
                ),
                "loop_detections": int(
                    usage_metrics["loop_detections"] or 0
                ),
                "accepted_claims": int(claim_metrics["accepted"] or 0),
                "rejected_claims": int(claim_metrics["rejected"] or 0),
                "unique_source_domains": int(unique_domains or 0),
                "latest_data_as_of": run_metrics["latest_data_as_of"],
                "last_successful_research": run_metrics["last_success"],
                "daily_budget_consumption": {
                    "runs": int(daily_usage["runs"] or 0),
                    "limit": self.settings.research_daily_budget_runs,
                    "run_limit": self.settings.research_daily_budget_runs,
                    "searches": int(daily_usage["searches"] or 0),
                    "search_limit": self.settings.research_daily_budget_searches,
                    "opened_sources": int(
                        daily_usage["opened_sources"] or 0
                    ),
                    "opened_source_limit": (
                        self.settings.research_daily_budget_opened_sources
                    ),
                },
            },
        }

    def recover_abandoned(self) -> int:
        now = self._iso(self.clock())
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            abandoned = conn.execute(
                """
                SELECT job_id,attempts,max_attempts,last_retry_reason
                FROM ai_research_jobs
                WHERE status='RUNNING' AND lease_expires_at IS NOT NULL
                  AND lease_expires_at<=?
                """,
                (now,),
            ).fetchall()
            for row in abandoned:
                conn.execute(
                    """
                    UPDATE ai_research_job_attempts
                    SET status='ABANDONED',completed_at=?,error='worker_lease_expired_recovered'
                    WHERE job_id=? AND attempt_number=? AND status='RUNNING'
                    """,
                    (now, row["job_id"], row["attempts"]),
                )
                continuation = (
                    str(row["last_retry_reason"] or "")
                    == "technical_continuation"
                )
                terminal = (
                    int(row["attempts"]) >= int(row["max_attempts"])
                    and not continuation
                )
                conn.execute(
                    """
                    UPDATE ai_research_jobs SET status=?,worker_id=NULL,
                      lease_expires_at=NULL,next_retry_at=?,completed_at=?,
                      last_error='worker_lease_expired_recovered',updated_at=?
                    WHERE job_id=? AND status='RUNNING'
                    """,
                    (
                        "FAILED" if terminal else "RETRY_SCHEDULED",
                        None if terminal else now,
                        now if terminal else None,
                        now,
                        row["job_id"],
                    ),
                )
                conn.execute(
                    """
                    UPDATE research_runs
                    SET status=?,completed_at=CASE WHEN ? THEN ? ELSE NULL END,
                        missing_topics_json=CASE WHEN ? THEN required_topics_json ELSE missing_topics_json END,
                        blocking_gaps_json=CASE WHEN ? THEN '["job_terminal:FAILED"]' ELSE blocking_gaps_json END,
                        updated_at=?
                    WHERE job_id=? AND status IN ('PENDING','RUNNING','RETRY_SCHEDULED')
                    """,
                    (
                        "FAILED" if terminal else "RETRY_SCHEDULED",
                        terminal,
                        now,
                        terminal,
                        terminal,
                        now,
                        row["job_id"],
                    ),
                )
            conn.commit()
            return len(abandoned)

    def acquire_next(
        self,
        worker_id: str,
        *,
        allowed_job_types: list[str] | None = None,
        authorized_smoke_only: bool = False,
    ) -> dict[str, Any] | None:
        self.recover_abandoned()
        now_dt = self.clock()
        now = self._iso(now_dt)
        lease = self._iso(now_dt + timedelta(seconds=self.settings.ai_job_lease_seconds))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            type_clause = ""
            values: list[Any] = [now]
            if allowed_job_types is not None:
                if not allowed_job_types:
                    conn.commit()
                    return None
                type_clause = f" AND job_type IN ({','.join('?' for _ in allowed_job_types)})"
                values.extend(allowed_job_types)
            smoke_clause = (
                " AND (job_type='RELEASE_ACTUAL_REFRESH' OR json_extract(request_payload_json,'$.authorized_live_smoke')=1)"
                if authorized_smoke_only else ""
            )
            row = conn.execute(
                f"""
                SELECT * FROM ai_research_jobs
                WHERE (status='PENDING'
                   OR (status='RETRY_SCHEDULED' AND (next_retry_at IS NULL OR next_retry_at<=?)))
                   AND source_audit_status='ACTIVE'
                   {type_clause}
                   {smoke_clause}
                ORDER BY priority ASC,created_at ASC
                LIMIT 1
                """,
                tuple(values),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            attempt = int(row["attempts"] or 0) + 1
            cursor = conn.execute(
                """
                UPDATE ai_research_jobs
                SET status='RUNNING',worker_id=?,attempts=?,started_at=COALESCE(started_at,?),
                    heartbeat_at=?,lease_expires_at=?,next_retry_at=NULL,updated_at=?
                WHERE job_id=? AND status IN ('PENDING','RETRY_SCHEDULED')
                """,
                (worker_id, attempt, now, now, lease, now, row["job_id"]),
            )
            if int(cursor.rowcount or 0) != 1:
                conn.rollback()
                return None
            conn.execute(
                """
                INSERT INTO ai_research_job_attempts(job_id,attempt_number,worker_id,status,started_at)
                VALUES (?,?,?,'RUNNING',?)
                """,
                (row["job_id"], attempt, worker_id, now),
            )
            conn.execute(
                """
                UPDATE research_runs
                SET status='RUNNING',started_at=COALESCE(started_at,?),
                    completed_at=NULL,updated_at=?
                WHERE job_id=? AND status IN ('PENDING','RETRY_SCHEDULED','RUNNING')
                """,
                (now, now, row["job_id"]),
            )
            conn.commit()
        return self.get(str(row["job_id"]))

    def mark_pending_capability(self, status: str, *, excluded_job_types: list[str] | None = None) -> int:
        clauses = ["status IN ('PENDING','RETRY_SCHEDULED')"]
        values: list[Any] = [status]
        if excluded_job_types:
            clauses.append(f"job_type NOT IN ({','.join('?' for _ in excluded_job_types)})")
            values.extend(excluded_job_types)
        values.append(self._iso(self.clock()))
        with connect_sqlite(self.settings.database_path) as conn:
            cursor = conn.execute(
                f"UPDATE ai_research_jobs SET capability_status=?,updated_at=? WHERE {' AND '.join(clauses)}",
                (values[0], values[-1], *values[1:-1]),
            )
            conn.commit()
        return max(int(cursor.rowcount or 0), 0)

    def heartbeat(self, job_id: str, worker_id: str) -> bool:
        now_dt = self.clock()
        now = self._iso(now_dt)
        lease = self._iso(now_dt + timedelta(seconds=self.settings.ai_job_lease_seconds))
        with connect_sqlite(self.settings.database_path) as conn:
            cursor = conn.execute(
                """
                UPDATE ai_research_jobs SET heartbeat_at=?,lease_expires_at=?,updated_at=?
                WHERE job_id=? AND status='RUNNING' AND worker_id=?
                """,
                (now, lease, now, job_id, worker_id),
            )
            conn.commit()
            return int(cursor.rowcount or 0) == 1

    def complete(
        self,
        job_id: str,
        worker_id: str,
        *,
        status: str,
        result_payload: dict[str, Any] | None,
        accepted_fields: list[str] | None = None,
        rejected_fields: list[str] | None = None,
        workspace_path: str | None = None,
        error: str | None = None,
        diagnostic: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if status not in TERMINAL_JOB_STATUSES:
            raise ValueError(f"invalid terminal status: {status}")
        now = self._iso(self.clock())
        result_json = self._json(result_payload) if result_payload is not None else None
        checksum = hashlib.sha256((result_json or "").encode("utf-8")).hexdigest() if result_json else None
        safe_diagnostic = sanitize_diagnostic(diagnostic) if diagnostic else None
        diagnostic_json = self._json(safe_diagnostic) if safe_diagnostic else None
        compact_error = str(error or "")[:500] or None
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT attempts FROM ai_research_jobs WHERE job_id=? AND status='RUNNING' AND worker_id=?",
                (job_id, worker_id),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise RuntimeError("job lease is no longer owned by worker")
            conn.execute(
                """
                UPDATE ai_research_jobs
                SET status=?,result_payload_json=?,accepted_fields_json=?,rejected_fields_json=?,
                    pending_fields_json='[]',completed_at=?,heartbeat_at=?,lease_expires_at=NULL,
                    workspace_path=?,output_checksum=?,last_error=?,last_diagnostic_json=?,updated_at=?
                WHERE job_id=?
                """,
                (
                    status, result_json, self._json(accepted_fields or []),
                    self._json(rejected_fields or []), now, now, workspace_path,
                    checksum, compact_error, diagnostic_json, now, job_id,
                ),
            )
            conn.execute(
                """
                UPDATE ai_research_job_attempts
                SET status=?,completed_at=?,error=?,output_checksum=?,error_category=?,
                    exit_code=?,retry_classification=?,diagnostic_json=?
                WHERE job_id=? AND attempt_number=?
                """,
                (
                    status,
                    now,
                    compact_error,
                    checksum,
                    safe_diagnostic.get("category") if safe_diagnostic else None,
                    safe_diagnostic.get("exit_code") if safe_diagnostic else None,
                    safe_diagnostic.get("retry_classification") if safe_diagnostic else None,
                    diagnostic_json,
                    job_id,
                    int(row["attempts"]),
                ),
            )
            self._finish_linked_run(
                conn,
                job_id=job_id,
                status=status,
                completed_at=now,
                error=compact_error,
                diagnostic=safe_diagnostic,
            )
            conn.commit()
        restored = self.get(job_id)
        if restored is None or restored.get("output_checksum") != checksum:
            raise RuntimeError("job result read-back failed")
        return restored

    def checkpoint(
        self,
        job_id: str,
        worker_id: str,
        *,
        checkpoint: dict[str, Any],
        workspace_path: str | None = None,
    ) -> dict[str, Any]:
        now = self._iso(self.clock())
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT attempts FROM ai_research_jobs
                WHERE job_id=? AND status='RUNNING' AND worker_id=?
                """,
                (job_id, worker_id),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise RuntimeError("job lease is no longer owned by worker")
            conn.execute(
                """
                UPDATE ai_research_jobs SET status='RETRY_SCHEDULED',
                  worker_id=NULL,lease_expires_at=NULL,next_retry_at=?,
                  last_error=NULL,last_retry_reason='technical_continuation',
                  workspace_path=?,updated_at=? WHERE job_id=?
                """,
                (now, workspace_path, now, job_id),
            )
            conn.execute(
                """
                UPDATE ai_research_job_attempts SET status='CHECKPOINTED',
                  completed_at=?,error=NULL,error_category=NULL,
                  retry_classification='CONTINUATION',diagnostic_json=?
                WHERE job_id=? AND attempt_number=?
                """,
                (
                    now,
                    self._json(
                        {
                            "category": "CHECKPOINTED",
                            "retryable": True,
                            "retry_classification": "CONTINUATION",
                            "checkpoint": checkpoint,
                        }
                    ),
                    job_id,
                    int(row["attempts"]),
                ),
            )
            conn.execute(
                """
                UPDATE research_runs SET status='RETRY_SCHEDULED',
                  completed_at=NULL,updated_at=? WHERE job_id=?
                """,
                (now, job_id),
            )
            conn.commit()
        return self.get(job_id)

    def retry_or_fail(
        self,
        job_id: str,
        worker_id: str,
        *,
        error: str,
        timed_out: bool = False,
        delays: list[int] | None = None,
        retryable: bool = True,
        diagnostic: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        job = self.get(job_id)
        if job is None or job.get("status") != "RUNNING" or job.get("worker_id") != worker_id:
            raise RuntimeError("job lease is no longer owned by worker")
        attempts = int(job["attempts"])
        max_attempts = int(job["max_attempts"])
        if not retryable or attempts >= max_attempts:
            terminal_status = (
                "TIMED_OUT"
                if timed_out
                else "LOOP_DETECTED"
                if (diagnostic or {}).get("category") == "LOOP_DETECTED"
                else "FAILED"
            )
            return self.complete(
                job_id,
                worker_id,
                status=terminal_status,
                result_payload=None,
                error=error,
                workspace_path=job.get("workspace_path"),
                diagnostic=diagnostic,
            )
        retry_delays = delays or [30, 120, 300, 900, 1800, 3600]
        delay = retry_delays[min(attempts - 1, len(retry_delays) - 1)]
        now_dt = self.clock()
        now = self._iso(now_dt)
        next_retry = self._iso(now_dt + timedelta(seconds=delay))
        attempt_status = "TIMED_OUT" if timed_out else "FAILED"
        safe_diagnostic = sanitize_diagnostic(diagnostic) if diagnostic else None
        diagnostic_json = self._json(safe_diagnostic) if safe_diagnostic else None
        compact_error = str(error)[:500]
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE ai_research_jobs
                SET status='RETRY_SCHEDULED',worker_id=NULL,lease_expires_at=NULL,
                    next_retry_at=?,last_error=?,last_retry_reason=?,updated_at=?
                WHERE job_id=? AND status='RUNNING' AND worker_id=?
                """,
                (next_retry, compact_error, compact_error, now, job_id, worker_id),
            )
            conn.execute(
                """
                UPDATE ai_research_job_attempts
                SET status=?,completed_at=?,error=?,error_category=?,exit_code=?,
                    retry_classification=?,diagnostic_json=?
                WHERE job_id=? AND attempt_number=?
                """,
                (
                    attempt_status,
                    now,
                    compact_error,
                    safe_diagnostic.get("category") if safe_diagnostic else None,
                    safe_diagnostic.get("exit_code") if safe_diagnostic else None,
                    safe_diagnostic.get("retry_classification") if safe_diagnostic else None,
                    diagnostic_json,
                    job_id,
                    attempts,
                ),
            )
            conn.execute(
                """
                UPDATE ai_research_jobs SET last_diagnostic_json=? WHERE job_id=?
                """,
                (diagnostic_json, job_id),
            )
            conn.execute(
                """
                UPDATE research_runs
                SET status='RETRY_SCHEDULED',completed_at=NULL,updated_at=?
                WHERE job_id=? AND status IN ('PENDING','RUNNING','RETRY_SCHEDULED')
                """,
                (now, job_id),
            )
            conn.commit()
        return self.get(job_id)

    def reconcile_lifecycle(self) -> int:
        return int(migrate_database(self.settings.database_path)["reconciled_research_runs"])

    def _finish_linked_run(
        self,
        conn: Any,
        *,
        job_id: str,
        status: str,
        completed_at: str,
        error: str | None,
        diagnostic: dict[str, Any] | None,
    ) -> None:
        row = conn.execute(
            """
            SELECT run_id,result_json,required_topics_json
            FROM research_runs WHERE job_id=?
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            return
        try:
            result = json.loads(row["result_json"] or "{}")
        except json.JSONDecodeError:
            result = {}
        result["job_terminal_status"] = status
        if error:
            result["last_error"] = error
        if diagnostic:
            result["diagnostic"] = diagnostic
        failed = status in {
            "FAILED",
            "LOOP_DETECTED",
            "TIMED_OUT",
            "CANCELLED",
            "REJECTED",
        }
        conn.execute(
            """
            UPDATE research_runs
            SET status=?,result_json=?,completed_at=?,
                missing_topics_json=CASE WHEN ? THEN required_topics_json ELSE missing_topics_json END,
                blocking_gaps_json=CASE WHEN ? THEN ? ELSE blocking_gaps_json END,
                updated_at=?
            WHERE run_id=?
            """,
            (
                status,
                self._json(result),
                completed_at,
                failed,
                failed,
                self._json([f"job_terminal:{status}"]),
                completed_at,
                row["run_id"],
            ),
        )

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _iso(value: datetime) -> str:
        aware = value if value.tzinfo else value.replace(tzinfo=UTC)
        return aware.astimezone(UTC).replace(microsecond=0).isoformat()

    @staticmethod
    def _row(row: Any) -> dict[str, Any]:
        data = dict(row)
        for key in (
            "request_payload_json", "result_payload_json", "accepted_fields_json",
            "rejected_fields_json", "pending_fields_json", "last_diagnostic_json",
        ):
            target = key.removesuffix("_json")
            raw = data.pop(key, None)
            try:
                data[target] = json.loads(raw) if raw else ([] if target.endswith("fields") else None)
            except json.JSONDecodeError:
                data[target] = None
        return data

    @staticmethod
    def _attempt_row(row: Any) -> dict[str, Any]:
        data = dict(row)
        raw = data.pop("diagnostic_json", None)
        try:
            data["diagnostic"] = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            data["diagnostic"] = None
        return data
