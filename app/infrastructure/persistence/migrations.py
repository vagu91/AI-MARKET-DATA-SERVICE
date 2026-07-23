from __future__ import annotations

import hashlib
import json
import sqlite3
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
        reconciled_research_runs = _reconcile_research_run_lifecycle(conn)
        conn.commit()
    return {
        "path": str(path),
        "applied": applied,
        "schema_version": len(MIGRATIONS),
        "legacy_event_enrichment_facts_migrated": legacy_event_enrichment_facts_migrated,
        "reconciled_research_runs": reconciled_research_runs,
    }


def _split_sql(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


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
        SELECT r.run_id,r.status AS run_status,r.required_topics_json,r.result_json,
               j.status AS job_status,j.completed_at,j.last_error,j.last_diagnostic_json
        FROM research_runs r
        JOIN ai_research_jobs j ON j.job_id=r.job_id
        WHERE (
          j.status IN ('SUCCEEDED','PARTIAL','NO_DATA','REJECTED','FAILED','TIMED_OUT','CANCELLED')
          AND r.status IN ('PENDING','RUNNING','RETRY_SCHEDULED')
        ) OR (
          j.status='RETRY_SCHEDULED' AND r.status='RUNNING'
        )
        """
    ).fetchall()
    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    repaired = 0
    failure_statuses = {"REJECTED", "FAILED", "TIMED_OUT", "CANCELLED"}
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
        required_topics = row["required_topics_json"] or "[]"
        missing_topics = required_topics if job_status in failure_statuses else None
        blocking_gaps = (
            json.dumps([f"job_terminal:{job_status}"], separators=(",", ":"))
            if job_status in failure_statuses
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
