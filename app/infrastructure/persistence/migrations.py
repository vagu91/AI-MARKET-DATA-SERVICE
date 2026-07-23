from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path

from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.schema import MIGRATIONS


def migrate_database(path: Path) -> dict[str, object]:
    applied: list[str] = []
    with connect_sqlite(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              applied_at TEXT NOT NULL
            )
            """
        )
        existing = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for index, (name, sql) in enumerate(MIGRATIONS, start=1):
            if index in existing:
                continue
            try:
                conn.execute("BEGIN")
                for statement in _split_sql(sql):
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                    (index, name, datetime.now(UTC).replace(microsecond=0).isoformat()),
                )
                conn.execute(f"PRAGMA user_version={index}")
                conn.commit()
                applied.append(name)
            except sqlite3.DatabaseError:
                conn.rollback()
                raise
        _migrate_legacy_cache_entries(conn)
        legacy_event_enrichment_facts_migrated = _migrate_legacy_event_enrichment_facts(conn)
        market_context_components_backfilled = (
            _backfill_market_context_components(conn)
            if "016_gap_aware_parallel_research_and_temporal_audit" in applied
            else 0
        )
        temporal_reconciliation = (
            _reconcile_temporal_quarantine(conn)
            if "017_temporal_quarantine_runtime_reconciliation" in applied
            else {
                "scanned_count": 0,
                "quarantined_count": 0,
                "errors": [],
            }
        )
        reconciled_research_runs = _reconcile_research_run_lifecycle(conn)
        conn.commit()
    return {
        "path": str(path),
        "applied": applied,
        "schema_version": len(MIGRATIONS),
        "legacy_event_enrichment_facts_migrated": legacy_event_enrichment_facts_migrated,
        "market_context_components_backfilled": market_context_components_backfilled,
        "temporal_reconciliation": temporal_reconciliation,
        "reconciled_research_runs": reconciled_research_runs,
    }


def _split_sql(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


_DERIVED_CONTEXT_COMPONENTS = {
    "ai_enrichment",
    "events_today",
    "events_today_context",
    "generated_at",
    "generated_at_utc",
    "lifecycle",
    "quality",
    "readiness",
    "research",
    "snapshot_id",
    "snapshot_revision",
    "snapshot_summary",
}


def _backfill_market_context_components(conn: sqlite3.Connection) -> int:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "market_context_components" not in tables:
        return 0
    rows = conn.execute(
        """
        SELECT snapshot_id,symbol,revision,data_as_of,debug_payload_json,created_at
        FROM market_context_snapshots ORDER BY symbol,revision
        """
    ).fetchall()
    inserted = 0
    for row in rows:
        try:
            payload = json.loads(str(row["debug_payload_json"]))
        except (TypeError, ValueError):
            continue
        for name, value in payload.items():
            if name in _DERIVED_CONTEXT_COMPONENTS:
                continue
            encoded = json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO market_context_components(
                  symbol,component_name,source_snapshot_id,source_revision,
                  data_as_of,valid_until,component_checksum,component_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(row["symbol"]).upper(),
                    str(name),
                    str(row["snapshot_id"]),
                    int(row["revision"]),
                    row["data_as_of"],
                    _component_valid_until(value),
                    hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                    encoded,
                    row["created_at"],
                ),
            )
            inserted += int(cursor.rowcount or 0)
    return inserted


def _component_valid_until(value: object) -> str | None:
    if isinstance(value, dict):
        candidate = value.get("valid_until") or value.get("fresh_until")
        return str(candidate) if candidate else None
    return None


def _reconcile_temporal_quarantine(conn: sqlite3.Connection) -> dict[str, object]:
    from app.services.temporal_validation_service import (
        QUARANTINED_STATUS,
        TemporalPolicy,
        quarantine_details,
        quarantine_identity,
    )

    started_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    policy = TemporalPolicy(clock=lambda: datetime.now(UTC))
    rows = conn.execute(
        "SELECT * FROM economic_events_history ORDER BY id"
    ).fetchall()
    quarantined_count = 0
    errors: list[dict[str, str]] = []
    for row in rows:
        record = dict(row)
        try:
            raw = json.loads(str(row["raw_payload_json"] or "{}"))
            if isinstance(raw, dict):
                record = {**raw, **record}
        except (TypeError, ValueError):
            pass
        try:
            decision = policy.evaluate(record, domain="macro_calendar")
            if decision.accepted:
                continue
            entity_key = str(row["event_key"])
            reason = str(decision.reason_code or "TEMPORALLY_INVALID")
            field = str(decision.timestamp_field or "unknown")
            value = str(decision.timestamp_value or record.get(field) or "")
            quarantine_id = quarantine_identity(
                entity_table="economic_events_history",
                entity_key=entity_key,
                timestamp_field=field,
                reason_code=reason,
            )
            cursor = conn.execute(
                """
                INSERT INTO temporal_quarantine(
                  quarantine_id,entity_table,entity_key,domain,timestamp_field,
                  timestamp_value,reason_code,detected_at,details_json
                ) VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(entity_table,entity_key,timestamp_field,reason_code)
                DO NOTHING
                """,
                (
                    quarantine_id,
                    "economic_events_history",
                    entity_key,
                    "macro_calendar",
                    field,
                    value,
                    reason,
                    started_at,
                    json.dumps(
                        quarantine_details(record),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ),
                ),
            )
            conn.execute(
                """
                UPDATE economic_events_history
                SET temporal_audit_status=?,
                    temporal_status=?,
                    status=?,
                    temporal_invalid_reason=?
                WHERE id=?
                """,
                (
                    QUARANTINED_STATUS,
                    QUARANTINED_STATUS,
                    QUARANTINED_STATUS,
                    reason,
                    row["id"],
                ),
            )
            conn.execute(
                """
                UPDATE market_facts
                SET temporal_audit_status=?,
                    temporal_invalid_reason=?
                WHERE canonical_event_key=?
                """,
                (
                    QUARANTINED_STATUS,
                    reason,
                    row["canonical_event_key"],
                ),
            )
            quarantined_count += int(cursor.rowcount or 0)
        except (KeyError, TypeError, ValueError, sqlite3.DatabaseError) as exc:
            errors.append(
                {
                    "entity_key": str(row["event_key"]),
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:300],
                }
            )
    completed_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    conn.execute(
        """
        INSERT INTO temporal_reconciliation_runs(
          reconciliation_id,source_schema_version,scanned_count,
          quarantined_count,errors_json,started_at,completed_at
        ) VALUES (?,?,?,?,?,?,?)
        """,
        (
            f"trr-{uuid.uuid4()}",
            16,
            len(rows),
            quarantined_count,
            json.dumps(errors, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            started_at,
            completed_at,
        ),
    )
    return {
        "scanned_count": len(rows),
        "quarantined_count": quarantined_count,
        "errors": errors,
    }


def _migrate_legacy_cache_entries(conn: sqlite3.Connection) -> None:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "cache_entries" not in tables or "provider_cache_entries" not in tables:
        return
    rows = conn.execute("SELECT cache_key, payload, created_at, updated_at FROM cache_entries").fetchall()
    for row in rows:
        checksum = hashlib.sha256(str(row["payload"]).encode("utf-8")).hexdigest()
        conn.execute(
            """
            INSERT INTO provider_cache_entries(cache_key, payload_json, created_at, updated_at, status, checksum)
            VALUES (?, ?, ?, ?, 'valid_cache', ?)
            ON CONFLICT(cache_key) DO UPDATE SET
              payload_json=excluded.payload_json,
              updated_at=excluded.updated_at,
              checksum=excluded.checksum
            """,
            (row["cache_key"], row["payload"], row["created_at"], row["updated_at"], checksum),
        )


def _migrate_legacy_event_enrichment_facts(conn: sqlite3.Connection) -> int:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "market_facts" not in tables:
        return 0
    pending = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM market_facts
            WHERE fact_type = 'ai_research_result'
              AND fact_key LIKE '%:macro_event_enrichment'
            """
        ).fetchone()["count"]
    )
    if pending == 0:
        return 0
    cursor = conn.execute(
        """
        UPDATE market_facts
        SET fact_type = 'macro_event_enrichment'
        WHERE fact_type = 'ai_research_result'
          AND fact_key LIKE '%:macro_event_enrichment'
        """
    )
    return max(int(cursor.rowcount or 0), 0)


def _reconcile_research_run_lifecycle(conn: sqlite3.Connection) -> int:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if not {"ai_research_jobs", "research_runs"} <= tables:
        return 0
    rows = conn.execute(
        """
        SELECT r.run_id,r.job_id,r.status AS run_status,r.required_topics_json,r.result_json,
               j.status AS job_status,j.completed_at,j.last_error,j.last_diagnostic_json
        FROM research_runs r
        JOIN ai_research_jobs j ON j.job_id=r.job_id
        WHERE (
          j.status IN (
            'SUCCEEDED','PARTIAL','NO_DATA','REJECTED','FAILED',
            'LOOP_DETECTED','TIMED_OUT','CANCELLED'
          )
          AND (
            r.status IN ('PENDING','RUNNING','RETRY_SCHEDULED')
            OR EXISTS (
              SELECT 1 FROM research_run_steps s
              WHERE s.run_id=r.run_id AND s.status='RUNNING'
            )
            OR (
              j.status IN ('REJECTED','FAILED','LOOP_DETECTED','TIMED_OUT','CANCELLED')
              AND EXISTS (
                SELECT 1 FROM research_claims c
                WHERE c.research_run_id=r.run_id
                  AND c.materialization_status!='ORPHANED'
              )
            )
            OR EXISTS (
              SELECT 1 FROM market_context_snapshots m
              WHERE m.source_job_id=r.job_id AND m.audit_status!='ORPHANED'
                AND j.status IN (
                  'REJECTED','FAILED','LOOP_DETECTED','TIMED_OUT','CANCELLED'
                )
            )
          )
        ) OR (
          j.status='RETRY_SCHEDULED' AND r.status='RUNNING'
        )
        """
    ).fetchall()
    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    repaired = 0
    failure_statuses = {
        "REJECTED",
        "FAILED",
        "LOOP_DETECTED",
        "TIMED_OUT",
        "CANCELLED",
    }
    for row in rows:
        job_status = str(row["job_status"])
        if job_status == "RETRY_SCHEDULED":
            conn.execute(
                """
                UPDATE research_runs
                SET status='RETRY_SCHEDULED',completed_at=NULL,updated_at=?
                WHERE run_id=? AND status='RUNNING'
                """,
                (now, row["run_id"]),
            )
            repaired += 1
            continue
        failed = job_status in failure_statuses
        running_steps = conn.execute(
            """
            SELECT step_id,step_name,attempt
            FROM research_run_steps
            WHERE run_id=? AND status='RUNNING'
            ORDER BY ordinal
            """,
            (row["run_id"],),
        ).fetchall()
        for step in running_steps:
            diagnostic = _reconciliation_diagnostic(
                conn,
                run_id=str(row["run_id"]),
                job_id=str(row["job_id"]),
                step_name=str(step["step_name"]),
                last_error=row["last_error"],
                timestamp=str(row["completed_at"] or now),
                failed=failed,
            )
            diagnostic_json = json.dumps(
                diagnostic,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            step_status = "FAILED" if failed else "ABANDONED"
            error = str(
                row["last_error"] or f"terminal_job_reconciled:{job_status}"
            )[:1000]
            conn.execute(
                """
                UPDATE research_run_steps
                SET status=?,completed_at=COALESCE(completed_at,?),error=?,
                    diagnostic_json=?,
                    duration_ms=COALESCE(
                      duration_ms,
                      CAST((julianday(?) - julianday(started_at))*86400000 AS INTEGER)
                    )
                WHERE step_id=? AND status='RUNNING'
                """,
                (
                    step_status,
                    str(row["completed_at"] or now),
                    error,
                    diagnostic_json,
                    str(row["completed_at"] or now),
                    step["step_id"],
                ),
            )
            conn.execute(
                """
                UPDATE research_step_attempts
                SET status=?,completed_at=COALESCE(completed_at,?),error=?,
                    diagnostic_json=?
                WHERE step_id=? AND attempt=? AND status='RUNNING'
                """,
                (
                    step_status,
                    str(row["completed_at"] or now),
                    error,
                    diagnostic_json,
                    step["step_id"],
                    step["attempt"],
                ),
            )
        try:
            result = json.loads(row["result_json"] or "{}")
        except json.JSONDecodeError:
            result = {}
        result["job_terminal_status"] = job_status
        if row["last_error"]:
            result["last_error"] = str(row["last_error"])[:500]
        if row["last_diagnostic_json"]:
            try:
                result["diagnostic"] = json.loads(row["last_diagnostic_json"])
            except json.JSONDecodeError:
                pass
        if running_steps:
            result["reconciliation_diagnostic"] = _reconciliation_diagnostic(
                conn,
                run_id=str(row["run_id"]),
                job_id=str(row["job_id"]),
                step_name=str(running_steps[-1]["step_name"]),
                last_error=row["last_error"],
                timestamp=str(row["completed_at"] or now),
                failed=failed,
            )
        if failed:
            for key in (
                "accepted_claims",
                "rejected_claims",
                "accepted_count",
                "persisted_count",
                "read_back_count",
                "evidence_count",
                "results",
            ):
                result.pop(key, None)
            conn.execute(
                """
                UPDATE research_claims
                SET materialization_status='ORPHANED'
                WHERE research_run_id=? AND materialization_status!='ORPHANED'
                """,
                (row["run_id"],),
            )
            conn.execute(
                """
                UPDATE research_evidence
                SET audit_status='ORPHANED'
                WHERE claim_id IN (
                  SELECT claim_id FROM research_claims WHERE research_run_id=?
                ) AND audit_status!='ORPHANED'
                """,
                (row["run_id"],),
            )
            conn.execute(
                """
                UPDATE market_facts
                SET status='orphaned',updated_at=?
                WHERE fact_key IN (
                  SELECT 'research:' || claim_id
                  FROM research_claims WHERE research_run_id=?
                ) AND status!='orphaned'
                """,
                (now, row["run_id"]),
            )
            conn.execute(
                """
                UPDATE market_context_snapshots
                SET audit_status='ORPHANED'
                WHERE source_job_id=? AND audit_status!='ORPHANED'
                """,
                (row["job_id"],),
            )
        required_topics = row["required_topics_json"] or "[]"
        missing_topics = required_topics if failed else None
        blocking_gaps = (
            json.dumps([f"job_terminal:{job_status}"], separators=(",", ":"))
            if failed
            else None
        )
        conn.execute(
            """
            UPDATE research_runs
            SET status=?,result_json=?,completed_at=COALESCE(?,?),
                missing_topics_json=COALESCE(?,missing_topics_json),
                blocking_gaps_json=COALESCE(?,blocking_gaps_json),updated_at=?
            WHERE run_id=?
            """,
            (
                job_status,
                json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                row["completed_at"],
                now,
                missing_topics,
                blocking_gaps,
                now,
                row["run_id"],
            ),
        )
        repaired += 1
    return repaired


def _reconciliation_diagnostic(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    job_id: str,
    step_name: str,
    last_error: object,
    timestamp: str,
    failed: bool,
) -> dict[str, object]:
    candidate = conn.execute(
        """
        SELECT c.claim_id,c.topic,c.field_semantics,c.payload_json
        FROM research_claims c
        WHERE c.research_run_id=? AND c.validation_status='accepted'
          AND NOT EXISTS (
            SELECT 1 FROM research_evidence e WHERE e.claim_id=c.claim_id
          )
        ORDER BY c.rowid LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    payload: dict[str, object] = {}
    if candidate is not None:
        try:
            payload = json.loads(candidate["payload_json"] or "{}")
        except json.JSONDecodeError:
            payload = {}
    error = str(last_error or f"terminal_job_reconciled:{step_name}")[:500]
    exception_type = error.rsplit(":", 1)[-1] if ":" in error else None
    transaction_outcome = (
        "PARTIALLY_COMMITTED"
        if conn.execute(
            "SELECT COUNT(*) FROM research_claims WHERE research_run_id=?",
            (run_id,),
        ).fetchone()[0]
        else "NOT_STARTED"
    )
    fingerprint = hashlib.sha256(
        f"{step_name}|{exception_type}|{error}".encode("utf-8")
    ).hexdigest()[:24]
    return {
        "category": "PERSISTENCE_ERROR" if failed else "TERMINAL_STATE_RECONCILIATION",
        "exception_type": exception_type,
        "message": error,
        "step": step_name,
        "failing_step": step_name,
        "claim_ref": payload.get("claim_ref"),
        "claim_id": candidate["claim_id"] if candidate is not None else None,
        "topic": candidate["topic"] if candidate is not None else None,
        "field_semantics": candidate["field_semantics"] if candidate is not None else None,
        "run_id": run_id,
        "job_id": job_id,
        "timestamp": timestamp,
        "retryable": False,
        "retry_classification": "NON_RETRYABLE",
        "stack_fingerprint": fingerprint,
        "transaction_outcome": transaction_outcome,
        "reconciled": True,
    }
