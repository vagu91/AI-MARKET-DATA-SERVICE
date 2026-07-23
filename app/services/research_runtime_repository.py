from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import traceback
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

from app.core.config import Settings
from app.core.text_normalization import normalize_payload_text
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.data_freshness_service import parse_datetime
from app.services.codex_runtime_contract import sanitize_diagnostic
from app.services.market_fact_repository import MarketFactRepository
from app.services.research_profiles import PROFILES
from app.services.research_semantics import (
    is_not_applicable,
    normalize_research_claim,
    semantic_validation_warnings,
)
from app.services.source_policy_service import SourcePolicyService
from app.services.research_tool_telemetry import (
    COUNTED_SOURCE_ACTIONS,
    action_fingerprint,
)


logger = logging.getLogger(__name__)


class ResearchPersistenceError(RuntimeError):
    code = "research_persist:unexpected"
    retryable = False

    def __init__(self, diagnostic: dict[str, Any]) -> None:
        self.diagnostic = sanitize_diagnostic(diagnostic)
        self.retry_classification = "NON_RETRYABLE"
        exception_type = str(self.diagnostic.get("exception_type") or "Exception")
        self.code = f"research_persist:unexpected:{exception_type}"[:240]
        super().__init__(self.code)


class ResearchRuntimeRepository:
    def __init__(
        self,
        settings: Settings,
        *,
        source_policy: SourcePolicyService | None = None,
        facts: MarketFactRepository | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.policy = source_policy or SourcePolicyService(settings.source_policy_path)
        self.facts = facts or MarketFactRepository(settings)
        self.now = now or (lambda: datetime.now(UTC))
        migrate_database(settings.database_path)

    def normalize_claims(self, claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            normalize_research_claim(
                claim,
                policy=self.policy,
                now=self.now(),
            )
            for claim in claims
            if isinstance(claim, dict)
        ]

    def ensure_run(
        self, job: dict[str, Any], profile_id: str, prompt_version: str
    ) -> dict[str, Any]:
        now = _now()
        request = job.get("request_payload") or {}
        fingerprint = str(job.get("input_fingerprint") or _checksum(request))
        profile = PROFILES.get(profile_id)
        required_topics = (
            sorted({str(item) for item in request.get("pending_fields") or []})
            if profile_id == "EVENT_MISSING_FIELDS"
            else list(profile.required_topics)
            if profile
            else []
        )
        run_id = f"rrun-{uuid.uuid4()}"
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO research_runs(
                  run_id,job_id,symbol,event_key,profile_id,prompt_version,policy_version,status,
                  input_fingerprint,request_json,required_topics_json,created_at,updated_at,
                  parent_run_id
                ) VALUES (?,?,?,?,?,?,?,'PENDING',?,?,?,?,?,?)
                """,
                (
                    run_id,
                    job["job_id"],
                    job.get("symbol") or "MNQ",
                    job.get("event_key"),
                    profile_id,
                    prompt_version,
                    job.get("policy_version") or self.policy.policy_version,
                    fingerprint,
                    _json(request),
                    _json(required_topics),
                    now,
                    now,
                    job.get("parent_run_id"),
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM research_runs WHERE job_id=?", (job["job_id"],)
            ).fetchone()
        return self._run_row(row)

    def ensure_effective_budget(
        self,
        run_id: str,
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT request_json FROM research_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise ValueError("research_run_not_found")
            request = json.loads(row["request_json"] or "{}")
            budget = request.get("effective_budget")
            if not isinstance(budget, dict):
                budget = dict(candidate)
                request["effective_budget"] = budget
                conn.execute(
                    "UPDATE research_runs SET request_json=?,updated_at=? WHERE run_id=?",
                    (_json(request), _now(), run_id),
                )
            conn.commit()
        return dict(budget)

    def daily_budget_usage(
        self,
        *,
        exclude_run_id: str | None = None,
    ) -> dict[str, int]:
        today = datetime.now(UTC).date().isoformat()
        exclusion = " AND run_id!=?" if exclude_run_id else ""
        values: tuple[Any, ...] = (today, exclude_run_id) if exclude_run_id else (today,)
        with connect_sqlite(self.settings.database_path) as conn:
            usage = conn.execute(
                f"""
                SELECT COALESCE(SUM(search_count),0) AS searches,
                       COALESCE(SUM(opened_source_count),0) AS opened
                FROM research_runs
                WHERE substr(created_at,1,10)=?{exclusion}
                """,
                values,
            ).fetchone()
            runs = conn.execute(
                f"""
                SELECT COUNT(*) FROM research_runs
                WHERE substr(created_at,1,10)=?{exclusion}
                """,
                values,
            ).fetchone()[0]
        return {
            "search_count": int(usage["searches"] or 0),
            "opened_source_count": int(usage["opened"] or 0),
            "run_count": int(runs or 0),
        }

    def begin_step(
        self,
        run_id: str,
        step_name: str,
        ordinal: int,
        input_payload: dict[str, Any],
        *,
        backend: str,
        tool: str,
    ) -> tuple[dict[str, Any], bool]:
        now = _now()
        input_json = _json(input_payload)
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM research_run_steps WHERE run_id=? AND step_name=?",
                (run_id, step_name),
            ).fetchone()
            if row is not None and row["status"] == "COMPLETED":
                conn.commit()
                return self._step_row(row), False
            if row is None:
                step_id = f"rstep-{uuid.uuid4()}"
                conn.execute(
                    """
                    INSERT INTO research_run_steps(
                      step_id,run_id,step_name,ordinal,status,attempt,input_checksum,input_json,
                      backend,tool,started_at
                    ) VALUES (?,?,?,?,'RUNNING',1,?,?,?,?,?)
                    """,
                    (
                        step_id,
                        run_id,
                        step_name,
                        ordinal,
                        _checksum(input_payload),
                        input_json,
                        backend,
                        tool,
                        now,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE research_run_steps SET status='RUNNING',attempt=attempt+1,input_checksum=?,
                      input_json=?,backend=?,tool=?,started_at=?,completed_at=NULL,error=NULL,
                      diagnostic_json=NULL
                    WHERE step_id=?
                    """,
                    (_checksum(input_payload), input_json, backend, tool, now, row["step_id"]),
                )
            conn.execute(
                "UPDATE research_runs SET status='RUNNING',started_at=COALESCE(started_at,?),updated_at=? WHERE run_id=?",
                (now, now, run_id),
            )
            restored = conn.execute(
                "SELECT * FROM research_run_steps WHERE run_id=? AND step_name=?",
                (run_id, step_name),
            ).fetchone()
            conn.execute(
                """
                INSERT OR IGNORE INTO research_step_attempts(
                  step_id,attempt,run_id,step_name,status,started_at
                ) VALUES (?,?,?,?,'RUNNING',?)
                """,
                (
                    restored["step_id"],
                    restored["attempt"],
                    run_id,
                    step_name,
                    now,
                ),
            )
            conn.commit()
        return self._step_row(restored), True

    def complete_step(
        self, step_id: str, output: dict[str, Any], *, source_domains: list[str] | None = None
    ) -> dict[str, Any]:
        now = _now()
        output_json = _json(output)
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_run_steps SET status='COMPLETED',output_checksum=?,output_json=?,
                  source_domains_json=?,completed_at=?,duration_ms=CAST((julianday(?) - julianday(started_at))*86400000 AS INTEGER)
                WHERE step_id=?
                """,
                (
                    _checksum(output),
                    output_json,
                    _json(sorted(set(source_domains or []))),
                    now,
                    now,
                    step_id,
                ),
            )
            conn.execute(
                """
                UPDATE research_step_attempts
                SET status='COMPLETED',completed_at=?
                WHERE step_id=? AND attempt=(
                  SELECT attempt FROM research_run_steps WHERE step_id=?
                )
                """,
                (now, step_id, step_id),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM research_run_steps WHERE step_id=?", (step_id,)
            ).fetchone()
        return self._step_row(row)

    def fail_step(
        self,
        step_id: str,
        error: str,
        *,
        diagnostic: dict[str, Any] | None = None,
    ) -> None:
        now = _now()
        safe_diagnostic = sanitize_diagnostic(diagnostic) if diagnostic else None
        diagnostic_json = _json(safe_diagnostic) if safe_diagnostic else None
        compact_error = str(error)[:1000]
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_run_steps
                SET status='FAILED',error=?,completed_at=?,diagnostic_json=?,
                    duration_ms=CAST((julianday(?) - julianday(started_at))*86400000 AS INTEGER)
                WHERE step_id=?
                """,
                (compact_error, now, diagnostic_json, now, step_id),
            )
            conn.execute(
                """
                UPDATE research_step_attempts
                SET status='FAILED',completed_at=?,error=?,diagnostic_json=?
                WHERE step_id=? AND attempt=(
                  SELECT attempt FROM research_run_steps WHERE step_id=?
                )
                """,
                (now, compact_error, diagnostic_json, step_id, step_id),
            )
            conn.commit()

    def record_tool_events(
        self,
        run_id: str,
        step_id: str,
        events: list[dict[str, Any]],
        *,
        usage: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _now()
        inserted_events: list[dict[str, Any]] = []
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            run_row = conn.execute(
                """
                SELECT job_id,usage_json,cost_json,source_domains_json
                FROM research_runs WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            step_row = conn.execute(
                "SELECT step_name,telemetry_json FROM research_run_steps WHERE step_id=?",
                (step_id,),
            ).fetchone()
            if run_row is None or step_row is None:
                conn.rollback()
                raise ValueError("research_tool_event_parent_not_found")
            for event in events:
                if not isinstance(event, dict):
                    continue
                normalized = _event_envelope(
                    event,
                    phase=str(step_row["step_name"]),
                    run_id=run_id,
                    job_id=str(run_row["job_id"]),
                    observed_at=now,
                )
                normalized = {
                    **normalized,
                    "source_url": _canonical_url(str(event.get("source_url") or "")) or None,
                    "canonical_url": _canonical_url(
                        str(event.get("canonical_url") or event.get("source_url") or "")
                    )
                    or None,
                }
                event_seed = "|".join(
                    (
                        run_id,
                        str(normalized["phase"]),
                        str(normalized["tool_action_fingerprint"]),
                        str(normalized["lifecycle"]),
                    )
                )
                event_id = f"rtool-{hashlib.sha256(event_seed.encode()).hexdigest()[:24]}"
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO research_tool_events(
                      event_id,run_id,step_id,event_type,source_url,canonical_url,redirect_url,
                      observed_at,content_hash,http_status,usage_json,payload_json,created_at,
                      raw_event_type,lifecycle,item_id,item_type,phase,job_id,provider_tool_type,
                      semantic_action,tool_action_fingerprint,status,counts_usage
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id,
                        run_id,
                        step_id,
                        str(normalized.get("event_type") or "unknown"),
                        normalized.get("source_url"),
                        normalized.get("canonical_url"),
                        normalized.get("redirect_url"),
                        normalized["observed_at"],
                        normalized.get("content_hash"),
                        normalized.get("http_status"),
                        (_json(normalized["usage"]) if normalized.get("usage") else None),
                        _json(normalized),
                        now,
                        normalized.get("raw_event_type"),
                        normalized.get("lifecycle"),
                        normalized.get("item_id"),
                        normalized.get("item_type"),
                        normalized.get("phase"),
                        normalized.get("job_id"),
                        normalized.get("provider_tool_type"),
                        normalized.get("semantic_action"),
                        normalized.get("tool_action_fingerprint"),
                        normalized.get("status"),
                        1 if normalized.get("counts_usage") else 0,
                    ),
                )
                if int(cursor.rowcount or 0):
                    inserted_events.append(normalized)
            counts = conn.execute(
                """
                SELECT
                  COUNT(*) AS raw_events,
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
                  ) AS opened
                FROM research_tool_events WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            searches = int(counts["searches"] or 0)
            opened = int(counts["opened"] or 0)
            discovered_urls = int(
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT COALESCE(canonical_url,source_url))
                    FROM research_tool_events
                    WHERE run_id=? AND COALESCE(canonical_url,source_url) IS NOT NULL
                    """,
                    (run_id,),
                ).fetchone()[0]
                or 0
            )
            merged_usage = _merge_usage(
                json.loads(run_row["usage_json"] or "{}"),
                usage,
            )
            cost = _provided_cost(merged_usage) or (
                json.loads(run_row["cost_json"] or "{}") or None
            )
            domains = set(json.loads(run_row["source_domains_json"] or "[]"))
            domains.update(
                self.policy.domain(str(item.get("canonical_url") or ""))
                for item in inserted_events
                if item.get("counts_usage")
                and item.get("semantic_action") in COUNTED_SOURCE_ACTIONS
                and item.get("canonical_url")
                and self.policy.rule_for(str(item["canonical_url"])) is not None
            )
            domains.discard("")
            conn.execute(
                """
                UPDATE research_runs SET search_count=?,opened_source_count=?,
                  deduplicated_search_count=?,discovered_url_count=?,
                  usage_json=?,cost_json=?,source_domains_json=?,updated_at=?
                WHERE run_id=?
                """,
                (
                    searches,
                    opened,
                    searches,
                    discovered_urls,
                    _json(merged_usage) if merged_usage else None,
                    _json(cost) if cost else None,
                    _json(sorted(domains)),
                    now,
                    run_id,
                ),
            )
            daily = conn.execute(
                """
                SELECT COALESCE(SUM(search_count),0) AS searches,
                       COALESCE(SUM(opened_source_count),0) AS opened
                FROM research_runs
                WHERE substr(created_at,1,10)=substr(?,1,10)
                """,
                (now,),
            ).fetchone()
            existing_telemetry = json.loads(step_row["telemetry_json"] or "[]") if step_row else []
            conn.execute(
                "UPDATE research_run_steps SET telemetry_json=? WHERE step_id=?",
                (
                    _json([*existing_telemetry, *inserted_events][-100:]),
                    step_id,
                ),
            )
            conn.commit()
        return {
            "search_count": searches,
            "executed_search_count": searches,
            "deduplicated_search_count": searches,
            "discovered_url_count": discovered_urls,
            "opened_source_count": opened,
            "daily_search_count": int(daily["searches"] or 0),
            "daily_opened_source_count": int(daily["opened"] or 0),
            "raw_event_count": int(counts["raw_events"] or 0),
            "normalized_action_count": int(counts["normalized_actions"] or 0),
            "deduplicated_tool_call_count": int(counts["tool_calls"] or 0),
            "event_inserted": bool(inserted_events),
            "inserted_events": inserted_events,
            "source_domains": sorted(domains),
            "usage": merged_usage,
            "cost_status": "available" if cost else "cost_unavailable",
        }

    def record_query_plan(self, run_id: str, queries: list[str]) -> int:
        planned = len(
            {
                " ".join(str(query).split())
                for query in queries
                if str(query).strip()
            }
        )
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_runs
                SET planned_query_count=?,updated_at=? WHERE run_id=?
                """,
                (planned, _now(), run_id),
            )
            conn.commit()
        return planned

    def record_threshold_warning(
        self,
        run_id: str,
        warning: dict[str, Any],
    ) -> list[dict[str, Any]]:
        compact = {
            "resource": str(warning.get("resource") or "")[:80],
            "configured_limit": int(warning.get("configured_limit") or 0),
            "observed_count": int(warning.get("observed_count") or 0),
            "step": str(warning.get("step") or "")[:80],
            "observed_at": str(warning.get("observed_at") or _now())[:80],
        }
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT threshold_warnings_json FROM research_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            existing = json.loads(row["threshold_warnings_json"] or "[]")
            key = (
                compact["resource"],
                compact["configured_limit"],
                compact["step"],
            )
            if key not in {
                (
                    item.get("resource"),
                    item.get("configured_limit"),
                    item.get("step"),
                )
                for item in existing
            }:
                existing.append(compact)
            bounded = existing[-50:]
            conn.execute(
                "UPDATE research_runs SET threshold_warnings_json=?,updated_at=? WHERE run_id=?",
                (_json(bounded), _now(), run_id),
            )
            conn.commit()
        return bounded

    def checkpoint_run(
        self,
        run_id: str,
        checkpoint: dict[str, Any],
    ) -> dict[str, Any]:
        compact = {
            "checkpointed": True,
            "next_step": str(checkpoint.get("next_step") or "")[:80],
            "completed_steps": [
                str(value)[:80] for value in checkpoint.get("completed_steps") or []
            ][-20:],
            "progress": checkpoint.get("progress") or {},
            "checkpointed_at": _now(),
        }
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_runs SET status='RETRY_SCHEDULED',
                  checkpoint_json=?,continuation_count=continuation_count+1,
                  completed_at=NULL,updated_at=? WHERE run_id=?
                """,
                (_json(compact), _now(), run_id),
            )
            conn.commit()
        return compact

    def record_loop_detection(
        self,
        run_id: str,
        diagnostic: dict[str, Any],
    ) -> None:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_runs SET loop_detection_count=loop_detection_count+1,
                  metrics_json=?,updated_at=? WHERE run_id=?
                """,
                (
                    _json({"last_loop_detection": sanitize_diagnostic(diagnostic)}),
                    _now(),
                    run_id,
                ),
            )
            conn.commit()

    def observed_queries(self, run_id: str) -> list[str]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT payload_json FROM research_tool_events
                WHERE run_id=? AND event_type='search'
                  AND (counts_usage=1 OR lifecycle IS NULL)
                ORDER BY observed_at,event_id
                """,
                (run_id,),
            ).fetchall()
        queries = []
        for row in rows:
            payload = json.loads(row["payload_json"] or "{}")
            if payload.get("query"):
                queries.append(str(payload["query"]))
        return sorted(set(queries))

    def observed_sources(self, run_id: str) -> list[dict[str, Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM research_tool_events
                WHERE run_id=? AND event_type IN ('open_source','server_source_verified')
                  AND (counts_usage=1 OR lifecycle IS NULL)
                ORDER BY observed_at,event_id
                """,
                (run_id,),
            ).fetchall()
        output = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
            output.append(item)
        return output

    def persist_research_source(
        self,
        run_id: str,
        source: dict[str, Any],
    ) -> dict[str, Any]:
        now = _now()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO research_sources(
                  source_id,run_id,requested_url,final_url,canonical_url,source_domain,
                  source_tier,publisher,title,fetch_status,verification_status,
                  rejection_reason,http_status,content_type,retrieved_at,content_sha256,
                  content_bytes,content_text,redirect_chain_json,duplicate_of_source_id,
                  acquisition_backend,fetch_duration_ms,created_at,updated_at,
                  stage_status,stage_error,http_fetched_at,content_extracted_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source_id) DO UPDATE SET
                  final_url=excluded.final_url,
                  canonical_url=excluded.canonical_url,
                  source_domain=excluded.source_domain,
                  source_tier=excluded.source_tier,
                  publisher=excluded.publisher,
                  title=excluded.title,
                  fetch_status=excluded.fetch_status,
                  rejection_reason=excluded.rejection_reason,
                  http_status=excluded.http_status,
                  content_type=excluded.content_type,
                  retrieved_at=excluded.retrieved_at,
                  content_sha256=excluded.content_sha256,
                  content_bytes=excluded.content_bytes,
                  content_text=excluded.content_text,
                  redirect_chain_json=excluded.redirect_chain_json,
                  duplicate_of_source_id=excluded.duplicate_of_source_id,
                  acquisition_backend=excluded.acquisition_backend,
                  fetch_duration_ms=excluded.fetch_duration_ms,
                  stage_status=excluded.stage_status,
                  stage_error=excluded.stage_error,
                  http_fetched_at=excluded.http_fetched_at,
                  content_extracted_at=excluded.content_extracted_at,
                  updated_at=excluded.updated_at
                """,
                (
                    source["source_id"],
                    run_id,
                    str(source.get("requested_url") or "")[:2048],
                    str(source.get("final_url") or "")[:2048] or None,
                    str(source.get("canonical_url") or "")[:2048] or None,
                    str(source.get("source_domain") or "")[:255],
                    source.get("source_tier"),
                    str(source.get("publisher") or "")[:200] or None,
                    str(source.get("title") or "")[:500] or None,
                    str(source.get("fetch_status") or "REJECTED")[:40],
                    str(source.get("verification_status") or "UNVERIFIED")[:40],
                    str(source.get("rejection_reason") or "")[:300] or None,
                    source.get("http_status"),
                    str(source.get("content_type") or "")[:120] or None,
                    str(source.get("retrieved_at") or now)[:80],
                    str(source.get("content_sha256") or "")[:128] or None,
                    int(source.get("content_bytes") or 0),
                    str(source.get("content_text") or "")[
                        : self.settings.research_gateway_max_text_chars
                    ]
                    or None,
                    _json(list(source.get("redirect_chain") or [])[:10]),
                    str(source.get("duplicate_of_source_id") or "")[:80] or None,
                    str(source.get("acquisition_backend") or "service_http_gateway")[:120],
                    int(source.get("fetch_duration_ms") or 0),
                    now,
                    now,
                    str(source.get("stage_status") or "")[:40] or None,
                    str(source.get("stage_error") or "")[:300] or None,
                    source.get("http_fetched_at"),
                    source.get("content_extracted_at"),
                ),
            )
            domain = str(source.get("source_domain") or "")
            if source.get("fetch_status") == "FETCHED" and domain:
                run_row = conn.execute(
                    "SELECT source_domains_json FROM research_runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()
                domains = set(json.loads(run_row["source_domains_json"] or "[]"))
                domains.add(domain)
                conn.execute(
                    """
                    UPDATE research_runs
                    SET source_domains_json=?,updated_at=?
                    WHERE run_id=?
                    """,
                    (_json(sorted(domains)), now, run_id),
                )
            source_counts = conn.execute(
                """
                SELECT COUNT(*) AS attempts,
                       SUM(CASE WHEN fetch_status='FETCHED' THEN 1 ELSE 0 END) AS fetched,
                       SUM(CASE WHEN verification_status='VERIFIED' THEN 1 ELSE 0 END) AS verified
                FROM research_sources WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            conn.execute(
                """
                UPDATE research_runs
                SET acquisition_attempt_count=?,fetched_source_count=?,
                    verified_source_count=?,updated_at=?
                WHERE run_id=?
                """,
                (
                    int(source_counts["attempts"] or 0),
                    int(source_counts["fetched"] or 0),
                    int(source_counts["verified"] or 0),
                    now,
                    run_id,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM research_sources WHERE source_id=?",
                (source["source_id"],),
            ).fetchone()
        return self._research_source_row(row)

    def research_sources(self, run_id: str) -> list[dict[str, Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM research_sources
                WHERE run_id=? ORDER BY created_at,source_id
                """,
                (run_id,),
            ).fetchall()
        return [self._research_source_row(row) for row in rows]

    def research_source_for_url(
        self,
        run_id: str,
        url: str,
    ) -> dict[str, Any] | None:
        canonical = _canonical_url(url)
        if not canonical:
            return None
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM research_sources
                WHERE run_id=?
                ORDER BY
                  CASE WHEN verification_status='VERIFIED' THEN 0
                       WHEN fetch_status='FETCHED' THEN 1 ELSE 2 END,
                  created_at
                """,
                (run_id,),
            ).fetchall()
        for row in rows:
            restored = self._research_source_row(row)
            candidates = {
                _canonical_url(str(restored.get(key) or ""))
                for key in ("requested_url", "final_url", "canonical_url")
            }
            if canonical in candidates:
                return restored
        return None

    def research_source_for_hash(
        self,
        run_id: str,
        content_hash: str,
    ) -> dict[str, Any] | None:
        if not content_hash:
            return None
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM research_sources
                WHERE run_id=? AND content_sha256=? AND fetch_status='FETCHED'
                ORDER BY created_at LIMIT 1
                """,
                (run_id, content_hash),
            ).fetchone()
        return self._research_source_row(row) if row else None

    def record_evidence_verification(
        self,
        run_id: str,
        verification: dict[str, Any],
    ) -> dict[str, Any]:
        now = _now()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO research_evidence_verifications(
                  verification_id,run_id,claim_ref,source_id,evidence_url,status,
                  reason,match_method,match_score,evidence_anchor,
                  evidence_token_count,matched_token_count,
                  verification_duration_ms,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(run_id,claim_ref,evidence_url,evidence_anchor)
                DO UPDATE SET
                  source_id=excluded.source_id,
                  status=excluded.status,
                  reason=excluded.reason,
                  match_method=excluded.match_method,
                  match_score=excluded.match_score,
                  evidence_token_count=excluded.evidence_token_count,
                  matched_token_count=excluded.matched_token_count,
                  verification_duration_ms=excluded.verification_duration_ms
                """,
                (
                    verification["verification_id"],
                    run_id,
                    str(verification.get("claim_ref") or "")[:120],
                    verification.get("source_id"),
                    str(verification.get("evidence_url") or "")[:2048],
                    str(verification.get("status") or "REJECTED")[:40],
                    str(verification.get("reason") or "")[:300],
                    str(verification.get("match_method") or "")[:80] or None,
                    float(verification.get("match_score") or 0),
                    str(verification.get("evidence_anchor") or "")[:1000],
                    int(verification.get("evidence_token_count") or 0),
                    int(verification.get("matched_token_count") or 0),
                    int(verification.get("verification_duration_ms") or 0),
                    now,
                ),
            )
            if (
                verification.get("source_id")
                and str(verification.get("status") or "") != "VERIFIED"
            ):
                conn.execute(
                    """
                    UPDATE research_sources
                    SET verification_status='REJECTED',
                        stage_status='VERIFICATION_FAILED',
                        stage_error=?,updated_at=?
                    WHERE source_id=?
                    """,
                    (
                        str(verification.get("reason") or "verification_failed")[:300],
                        now,
                        verification.get("source_id"),
                    ),
                )
            conn.commit()
            row = conn.execute(
                """
                SELECT * FROM research_evidence_verifications
                WHERE run_id=? AND claim_ref=? AND evidence_url=?
                  AND evidence_anchor=?
                """,
                (
                    run_id,
                    str(verification.get("claim_ref") or "")[:120],
                    str(verification.get("evidence_url") or "")[:2048],
                    str(verification.get("evidence_anchor") or "")[:1000],
                ),
            ).fetchone()
        return dict(row)

    def mark_research_source_verified(
        self,
        source_id: str,
        verification: dict[str, Any],
    ) -> None:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_sources
                SET verification_status='VERIFIED',updated_at=?
                WHERE source_id=? AND fetch_status='FETCHED'
                """,
                (_now(), source_id),
            )
            conn.execute(
                """
                UPDATE research_runs
                SET verified_source_count=(
                  SELECT COUNT(*) FROM research_sources
                  WHERE research_sources.run_id=research_runs.run_id
                    AND verification_status='VERIFIED'
                ),updated_at=?
                WHERE run_id=(
                  SELECT run_id FROM research_sources WHERE source_id=?
                )
                """,
                (_now(), source_id),
            )
            conn.commit()

    def record_backend_invocation(
        self,
        run_id: str,
        invocation: Any,
    ) -> dict[str, Any]:
        usage = dict(invocation.usage or {})
        total = int(usage.get("total_tokens") or 0)
        if total <= 0:
            total = int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0)
        provided_cost = _provided_cost(usage)
        cost = {"total_cost_usd": _cost_value(provided_cost)} if provided_cost else None
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT OR IGNORE INTO research_backend_invocations(
                  invocation_id,run_id,backend,purpose,model,input_tokens,
                  output_tokens,cached_tokens,reasoning_tokens,total_tokens,
                  cost_json,duration_ms,output_checksum,output_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    invocation.invocation_id,
                    run_id,
                    str(invocation.backend)[:120],
                    str(invocation.purpose)[:120],
                    str(invocation.model or "")[:120] or None,
                    int(usage.get("input_tokens") or 0),
                    int(usage.get("output_tokens") or 0),
                    int(usage.get("cached_tokens") or 0),
                    int(usage.get("reasoning_tokens") or 0),
                    total,
                    _json(cost) if cost else None,
                    int(invocation.duration_ms or 0),
                    _checksum(invocation.payload),
                    _json(invocation.payload),
                    _now(),
                ),
            )
            totals = conn.execute(
                """
                SELECT COALESCE(SUM(input_tokens),0) AS input_tokens,
                       COALESCE(SUM(output_tokens),0) AS output_tokens,
                       COALESCE(SUM(cached_tokens),0) AS cached_tokens,
                       COALESCE(SUM(reasoning_tokens),0) AS reasoning_tokens,
                       COALESCE(SUM(total_tokens),0) AS total_tokens
                FROM research_backend_invocations WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            merged_usage = {key: int(totals[key] or 0) for key in totals.keys()}
            cost_rows = conn.execute(
                """
                SELECT cost_json FROM research_backend_invocations
                WHERE run_id=? AND cost_json IS NOT NULL
                """,
                (run_id,),
            ).fetchall()
            aggregate_cost = (
                {
                    "total_cost_usd": sum(
                        _cost_value(json.loads(row["cost_json"] or "{}")) for row in cost_rows
                    )
                }
                if cost_rows
                else None
            )
            conn.execute(
                """
                UPDATE research_runs
                SET usage_json=?,cost_json=?,updated_at=?
                WHERE run_id=?
                """,
                (
                    _json(merged_usage),
                    _json(aggregate_cost) if aggregate_cost else None,
                    _now(),
                    run_id,
                ),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT * FROM research_backend_invocations
                WHERE invocation_id=?
                """,
                (invocation.invocation_id,),
            ).fetchone()
        return self._backend_invocation_row(row)

    def latest_backend_invocation(
        self,
        run_id: str,
    ) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM research_backend_invocations
                WHERE run_id=? ORDER BY created_at DESC LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return self._backend_invocation_row(row) if row else None

    def persist_claims(
        self,
        run: dict[str, Any],
        claims: list[dict[str, Any]],
        *,
        step_id: str | None = None,
    ) -> dict[str, Any]:
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        evidence_by_claim: dict[str, list[dict[str, Any]]] = {}
        evidence_count = 0
        read_back_count = 0
        observed_sources = self.observed_sources(str(run["run_id"]))
        acquired_sources = self.research_sources(str(run["run_id"]))
        current_run = self.get_run(str(run["run_id"])) or run
        source_domains: set[str] = set(current_run.get("source_domains") or [])
        source_domains.update(
            self.policy.domain(str(item.get("canonical_url") or item.get("source_url") or ""))
            for item in observed_sources
            if self.policy.rule_for(str(item.get("canonical_url") or item.get("source_url") or ""))
            is not None
        )
        source_domains.discard("")
        current_claim: dict[str, Any] | None = None
        transaction_started = False
        conn = connect_sqlite(self.settings.database_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            transaction_started = True
            for raw_claim in claims:
                current_claim = normalize_research_claim(
                    raw_claim,
                    policy=self.policy,
                    now=self.now(),
                )
                restored, evidence_rows = self._persist_claim(
                    conn,
                    run,
                    current_claim,
                    observed_sources,
                    acquired_sources,
                )
                evidence_by_claim[str(restored["claim_id"])] = evidence_rows
                evidence_count += len(evidence_rows)
                source_domains.update(row["source_domain"] for row in evidence_rows)
                if restored["validation_status"] == "accepted":
                    accepted.append(restored)
                    if is_not_applicable(restored):
                        conn.execute(
                            """
                            UPDATE research_claims
                            SET materialization_status='NOT_APPLICABLE'
                            WHERE claim_id=?
                            """,
                            (restored["claim_id"],),
                        )
                        restored["materialization_status"] = "NOT_APPLICABLE"
                        read_back_count += 1
                    elif self._project_and_read_back(conn, restored, evidence_rows):
                        conn.execute(
                            """
                            UPDATE research_claims
                            SET materialization_status='MATERIALIZED'
                            WHERE claim_id=?
                            """,
                            (restored["claim_id"],),
                        )
                        restored["materialization_status"] = "MATERIALIZED"
                        read_back_count += 1
                    else:
                        raise RuntimeError("research_fact_read_back_failed")
                else:
                    rejected.append(restored)

            required_topics = {str(item) for item in run.get("required_topics") or []}
            completed_topics = {
                _resolved_topic(run, item)
                for item in accepted
                if not is_not_applicable(item)
            } & required_topics
            not_applicable_topics = {
                _resolved_topic(run, item)
                for item in accepted
                if is_not_applicable(item)
            } & required_topics
            resolved_topics = completed_topics | not_applicable_topics
            missing_topics = required_topics - resolved_topics
            coverage_score = len(resolved_topics) / max(len(required_topics), 1)
            status = (
                "NO_DATA"
                if not accepted
                else "PARTIAL"
                if missing_topics
                else "SUCCEEDED"
            )
            blocking_gaps = [f"missing_topic:{item}" for item in sorted(missing_topics)]
            now = _now()
            result_payload = {
                "accepted_claims": accepted,
                "rejected_claims": rejected,
                "accepted_count": len(accepted),
                "candidate_count": len(claims),
                "persisted_count": len(accepted),
                "read_back_count": read_back_count,
                "evidence_count": evidence_count,
                "source_domains": sorted(source_domains),
                "required_topics": sorted(required_topics),
                "completed_topics": sorted(completed_topics),
                "valid_not_applicable_topics": sorted(not_applicable_topics),
                "missing_topics": sorted(missing_topics),
                "blocking_gaps": blocking_gaps,
                "coverage_score": coverage_score,
            }
            results = [
                self._claim_result_from_evidence(
                    item,
                    evidence_by_claim[str(item["claim_id"])],
                )
                for item in accepted
                if not is_not_applicable(item)
                if item["field_semantics"]
                in {
                    "forecast",
                    "consensus",
                    "previous",
                    "outcome",
                    "transcript_url",
                    "scheduled_event",
                    "official_calendar_event",
                    "issuer_announcement",
                    "earnings_schedule",
                    "current_news",
                    "current_market_context",
                }
            ]
            output = {"status": status, **result_payload, "results": results}
            conn.execute(
                """
                UPDATE research_runs SET status=?,result_json=?,coverage_score=?,source_domains_json=?,
                  completed_topics_json=?,missing_topics_json=?,warnings_json=?,data_as_of=?,
                  completed_at=?,updated_at=?
                WHERE run_id=?
                """,
                (
                    status,
                    _json(result_payload),
                    coverage_score,
                    _json(sorted(source_domains)),
                    _json(sorted(completed_topics)),
                    _json(sorted(missing_topics)),
                    _json([warning for item in rejected for warning in item.get("warnings") or []]),
                    now,
                    now,
                    now,
                    run["run_id"],
                ),
            )
            conn.execute(
                """
                UPDATE research_runs SET valid_not_applicable_topics_json=?,blocking_gaps_json=?
                WHERE run_id=?
                """,
                (_json(sorted(not_applicable_topics)), _json(blocking_gaps), run["run_id"]),
            )
            if step_id:
                self._complete_step_in_transaction(
                    conn,
                    step_id,
                    output,
                    source_domains=sorted(source_domains),
                    completed_at=now,
                )
            conn.commit()
            return output
        except Exception as exc:
            if conn.in_transaction:
                conn.rollback()
            diagnostic = self._persistence_diagnostic(
                exc,
                run=run,
                claim=current_claim,
                transaction_outcome=(
                    "ROLLED_BACK" if transaction_started else "NOT_STARTED"
                ),
            )
            self._record_persist_failure(
                run,
                step_id=step_id,
                error=f"{type(exc).__name__}:{exc}",
                diagnostic=diagnostic,
            )
            raise ResearchPersistenceError(diagnostic) from exc
        finally:
            conn.close()

    def _complete_step_in_transaction(
        self,
        conn: sqlite3.Connection,
        step_id: str,
        output: dict[str, Any],
        *,
        source_domains: list[str],
        completed_at: str,
    ) -> None:
        output_json = _json(output)
        conn.execute(
            """
            UPDATE research_run_steps
            SET status='COMPLETED',output_checksum=?,output_json=?,
                source_domains_json=?,completed_at=?,
                duration_ms=CAST(
                  (julianday(?) - julianday(started_at))*86400000 AS INTEGER
                ),
                error=NULL,diagnostic_json=NULL
            WHERE step_id=?
            """,
            (
                _checksum(output),
                output_json,
                _json(sorted(set(source_domains))),
                completed_at,
                completed_at,
                step_id,
            ),
        )
        conn.execute(
            """
            UPDATE research_step_attempts
            SET status='COMPLETED',completed_at=?,error=NULL,diagnostic_json=NULL
            WHERE step_id=? AND attempt=(
              SELECT attempt FROM research_run_steps WHERE step_id=?
            )
            """,
            (completed_at, step_id, step_id),
        )

    def _persistence_diagnostic(
        self,
        exc: Exception,
        *,
        run: dict[str, Any],
        claim: dict[str, Any] | None,
        transaction_outcome: str,
    ) -> dict[str, Any]:
        trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        stack_fingerprint = hashlib.sha256(trace.encode("utf-8")).hexdigest()[:24]
        return sanitize_diagnostic(
            {
                "category": "PERSISTENCE_ERROR",
                "exception_type": type(exc).__name__,
                "message": str(exc)[:500],
                "step": "PERSIST",
                "failing_step": "PERSIST",
                "claim_ref": (claim or {}).get("claim_ref"),
                "claim_id": (claim or {}).get("claim_id"),
                "topic": (claim or {}).get("topic"),
                "field_semantics": (claim or {}).get("field_semantics"),
                "run_id": run.get("run_id"),
                "job_id": run.get("job_id"),
                "timestamp": _now(),
                "retryable": False,
                "retry_classification": "NON_RETRYABLE",
                "stack_fingerprint": stack_fingerprint,
                "transaction_outcome": transaction_outcome,
            }
        )

    def _record_persist_failure(
        self,
        run: dict[str, Any],
        *,
        step_id: str | None,
        error: str,
        diagnostic: dict[str, Any],
    ) -> None:
        now = _now()
        compact_error = str(error)[:1000]
        diagnostic_json = _json(sanitize_diagnostic(diagnostic))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            target_step_id = step_id
            if target_step_id is None:
                row = conn.execute(
                    """
                    SELECT step_id FROM research_run_steps
                    WHERE run_id=? AND step_name='PERSIST' AND status='RUNNING'
                    """,
                    (run["run_id"],),
                ).fetchone()
                target_step_id = str(row["step_id"]) if row else None
            if target_step_id:
                conn.execute(
                    """
                    UPDATE research_run_steps
                    SET status='FAILED',error=?,completed_at=?,diagnostic_json=?,
                        duration_ms=CAST(
                          (julianday(?) - julianday(started_at))*86400000 AS INTEGER
                        )
                    WHERE step_id=?
                    """,
                    (
                        compact_error,
                        now,
                        diagnostic_json,
                        now,
                        target_step_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE research_step_attempts
                    SET status='FAILED',completed_at=?,error=?,diagnostic_json=?
                    WHERE step_id=? AND attempt=(
                      SELECT attempt FROM research_run_steps WHERE step_id=?
                    )
                    """,
                    (
                        now,
                        compact_error,
                        diagnostic_json,
                        target_step_id,
                        target_step_id,
                    ),
                )
            existing = conn.execute(
                "SELECT result_json FROM research_runs WHERE run_id=?",
                (run["run_id"],),
            ).fetchone()
            try:
                result = json.loads(existing["result_json"] or "{}") if existing else {}
            except json.JSONDecodeError:
                result = {}
            for counter in (
                "accepted_claims",
                "rejected_claims",
                "accepted_count",
                "persisted_count",
                "read_back_count",
                "evidence_count",
                "results",
            ):
                result.pop(counter, None)
            result.update(
                {
                    "error": compact_error,
                    "diagnostic": sanitize_diagnostic(diagnostic),
                }
            )
            conn.execute(
                """
                UPDATE research_runs
                SET status='FAILED',result_json=?,completed_at=?,
                    missing_topics_json=required_topics_json,
                    blocking_gaps_json='["persist_failed"]',updated_at=?
                WHERE run_id=?
                """,
                (_json(result), now, now, run["run_id"]),
            )
            conn.commit()

    def reconcile_terminal_jobs(self) -> int:
        """Close stranded steps and quarantine partial output without deleting audit data."""
        return int(migrate_database(self.settings.database_path)["reconciled_research_runs"])

    def finish_run(self, run_id: str, status: str, result: dict[str, Any]) -> dict[str, Any]:
        now = _now()
        with connect_sqlite(self.settings.database_path) as conn:
            existing = conn.execute(
                "SELECT result_json FROM research_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            merged = {**(json.loads(existing["result_json"] or "{}") if existing else {}), **result}
            conn.execute(
                "UPDATE research_runs SET status=?,result_json=?,completed_at=?,updated_at=? WHERE run_id=?",
                (status, _json(merged), now, now, run_id),
            )
            conn.commit()
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute("SELECT * FROM research_runs WHERE run_id=?", (run_id,)).fetchone()
            steps = conn.execute(
                "SELECT * FROM research_run_steps WHERE run_id=? ORDER BY ordinal",
                (run_id,),
            ).fetchall()
            histories = conn.execute(
                """
                SELECT * FROM research_step_attempts
                WHERE run_id=? ORDER BY step_name,attempt
                """,
                (run_id,),
            ).fetchall()
        if row is None:
            return None
        restored = self._run_row(row)
        by_step: dict[str, list[dict[str, Any]]] = {}
        for item in histories:
            attempt = self._step_attempt_row(item)
            by_step.setdefault(str(attempt["step_id"]), []).append(attempt)
        restored["steps"] = [
            {**self._step_row(step), "attempt_history": by_step.get(str(step["step_id"]), [])}
            for step in steps
        ]
        return restored

    def get_run_for_job(self, job_id: str) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT run_id FROM research_runs WHERE job_id=?",
                (job_id,),
            ).fetchone()
        return self.get_run(str(row["run_id"])) if row else None

    def latest(self, symbol: str = "MNQ") -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM research_runs
                WHERE symbol=? ORDER BY created_at DESC,rowid DESC LIMIT 1
                """,
                (symbol.upper(),),
            ).fetchone()
        return self.get_run(str(row["run_id"])) if row else None

    def evidence_for_claim(self, claim_id: str) -> list[dict[str, Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                "SELECT * FROM research_evidence WHERE claim_id=? ORDER BY source_tier,source_domain",
                (claim_id,),
            ).fetchall()
        return [normalize_payload_text(dict(row)) for row in rows]

    def _persist_claim(
        self,
        conn: sqlite3.Connection,
        run: dict[str, Any],
        claim: dict[str, Any],
        observed_sources: list[dict[str, Any]],
        acquired_sources: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        now = _now()
        claim = normalize_research_claim(
            claim,
            policy=self.policy,
            now=self.now(),
        )
        semantics = str(
            claim.get("field_semantics") or claim.get("field") or "exploratory_context"
        ).lower()
        not_applicable = is_not_applicable(claim)
        evidence_input = [item for item in claim.get("evidence") or [] if isinstance(item, dict)]
        evidence_rows, warnings = self._validated_evidence(
            semantics,
            evidence_input,
            observed_sources,
            acquired_sources,
        )
        if not_applicable:
            # Evidence attached as historical context must not invalidate a
            # documented negative-coverage result.
            warnings = []
            if str(run.get("profile_id") or "") in {
                "MNQ_MARKET_RESEARCH",
                "MACRO_EVENTS_RESEARCH",
                "FED_RATES_RESEARCH",
                "VIX_RISK_RESEARCH",
                "COT_POSITIONING_RESEARCH",
                "NASDAQ_100_RESEARCH",
                "MEGA_CAP_SEMICONDUCTORS_RESEARCH",
                "EARNINGS_RESEARCH",
                "NEWS_RESEARCH",
                "GEOPOLITICAL_REGULATORY_RISK_RESEARCH",
            }:
                warnings.append("topic_is_applicable_not_applicable_forbidden")
        warnings.extend(
            semantic_validation_warnings(
                claim,
                policy=self.policy,
                now=self.now(),
            )
        )
        semantic_policy = self.policy.semantic_policy(semantics)
        groups = {row["independent_source_group"] for row in evidence_rows}
        required = self.policy.required_confirmations(semantics)
        if not not_applicable and len(groups) < required:
            warnings.append("insufficient_independent_evidence")
        if semantics in {"actual", "forecast", "consensus", "previous"}:
            for field in ("metric_id", "period", "frequency", "unit"):
                if claim.get(field) in (None, ""):
                    warnings.append(f"missing_{field}")
        if semantics in {"actual", "official_actual"}:
            warnings.append("official_actual_requires_deterministic_resolver")
        published = parse_datetime(claim.get("published_at"))
        if published and published > self.now():
            warnings.append("future_published_at_rejected")
        validation = (
            "accepted"
            if (evidence_rows or not_applicable) and not warnings
            else "rejected"
        )
        claim_payload = {**claim, "field_semantics": semantics, "warnings": sorted(set(warnings))}
        checksum = _checksum(claim_payload)
        claim_seed = f"{run['run_id']}|{checksum}"
        claim_id = str(
            claim.get("claim_id") or f"claim-{hashlib.sha256(claim_seed.encode()).hexdigest()[:24]}"
        )
        materialization_status = (
            "NOT_APPLICABLE"
            if not_applicable and validation == "accepted"
            else "PENDING"
            if validation == "accepted"
            else "REJECTED"
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO research_claims(
              claim_id,research_run_id,topic,field_semantics,value_json,metric_id,period,frequency,
              unit,event_key,event_at,release_at,issuer,symbol,valid_from,valid_until,
              next_refresh_at,lifecycle_status,post_event_semantics,published_at,retrieved_at,confidence,
              validation_status,materialization_status,warnings_json,policy_version,prompt_version,
              payload_json,checksum,created_at,event_type,event_start_at,event_end_at,
              decision_at,confirmation_status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                claim_id,
                run["run_id"],
                str(claim.get("topic") or semantics),
                semantics,
                _json(claim.get("value")),
                claim.get("metric_id"),
                claim.get("period"),
                claim.get("frequency"),
                claim.get("unit"),
                claim.get("event_key") or run.get("event_key"),
                claim.get("event_at"),
                claim.get("release_at"),
                claim.get("issuer"),
                claim.get("symbol") or run.get("symbol"),
                claim.get("valid_from"),
                claim.get("valid_until"),
                claim.get("next_refresh_at"),
                claim.get("lifecycle_status"),
                claim.get("post_event_semantics"),
                claim.get("published_at"),
                claim.get("retrieved_at") or now,
                min(
                    float(claim.get("confidence") or 0),
                    float(semantic_policy.get("max_reliability") or 1.0),
                ),
                validation,
                materialization_status,
                _json(sorted(set(warnings))),
                run["policy_version"],
                run["prompt_version"],
                _json(claim_payload),
                checksum,
                now,
                claim.get("event_type"),
                claim.get("event_start_at"),
                claim.get("event_end_at"),
                claim.get("decision_at"),
                claim.get("confirmation_status"),
            ),
        )
        for evidence in evidence_rows:
            evidence_seed = (
                f"{claim_id}|{evidence['canonical_url']}|{evidence['content_checksum']}"
            )
            evidence_id = f"evidence-{hashlib.sha256(evidence_seed.encode()).hexdigest()[:24]}"
            conn.execute(
                """
                INSERT INTO research_evidence(
                  evidence_id,claim_id,query_text,source_url,canonical_url,publisher,source_domain,
                  source_tier,evidence_text,published_at,retrieved_at,redirect_url,source_status,
                  independent_source_group,content_checksum,policy_version,created_at,
                  source_content_hash,tool_event_id,source_id,verification_id,
                  verification_method,verification_reason,verification_score,audit_status
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(evidence_id) DO UPDATE SET audit_status='ACTIVE'
                """,
                (
                    evidence_id,
                    claim_id,
                    evidence.get("query"),
                    evidence["source_url"],
                    evidence["canonical_url"],
                    evidence.get("publisher"),
                    evidence["source_domain"],
                    evidence["source_tier"],
                    evidence["evidence_text"],
                    evidence.get("published_at"),
                    evidence["retrieved_at"],
                    evidence.get("redirect_url"),
                    evidence.get("source_status"),
                    evidence["independent_source_group"],
                    evidence["content_checksum"],
                    run["policy_version"],
                    now,
                    evidence.get("source_content_hash"),
                    evidence.get("tool_event_id"),
                    evidence.get("source_id"),
                    evidence.get("verification_id"),
                    evidence.get("verification_method"),
                    evidence.get("verification_reason"),
                    evidence.get("verification_score"),
                    "ACTIVE",
                ),
            )
        row = conn.execute(
            "SELECT * FROM research_claims WHERE claim_id=?", (claim_id,)
        ).fetchone()
        restored = dict(row)
        restored["value"] = json.loads(restored.pop("value_json") or "null")
        restored["warnings"] = json.loads(restored.pop("warnings_json") or "[]")
        restored["payload"] = json.loads(restored.pop("payload_json"))
        restored["claim_ref"] = restored["payload"].get("claim_ref")
        restored["topic_status"] = restored["payload"].get("topic_status")
        restored["_bounded_search_documented"] = restored["payload"].get(
            "_bounded_search_documented"
        )
        logger.info(
            "research_claim_policy_validated",
            extra={
                "run_id": run["run_id"],
                "claim_id": claim_id,
                "field_semantics": semantics,
                "validation_status": validation,
                "evidence_count": len(evidence_rows),
            },
        )
        return restored, evidence_rows

    def _validated_evidence(
        self,
        semantics: str,
        items: list[dict[str, Any]],
        observed_sources: list[dict[str, Any]],
        acquired_sources: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        semantic_policy = self.policy.semantic_policy(semantics)
        allowed_tiers = {int(item) for item in semantic_policy.get("allowed_tiers") or range(1, 6)}
        ttl_minutes = int(semantic_policy.get("ttl_minutes") or 0)
        freshness_basis = str(semantic_policy.get("freshness_basis") or "published_at")
        published_at_required = bool(semantic_policy.get("published_at_required")) or semantics == "news"
        now = self.now()
        seen: set[tuple[str, str]] = set()
        for item in items:
            url = _canonical_url(str(item.get("canonical_url") or item.get("source_url") or ""))
            evidence_text = " ".join(str(item.get("evidence_text") or "").split())[:1000]
            rule = self.policy.rule_for(url, item.get("publisher"))
            if not url.startswith("https://") or not evidence_text or rule is None:
                warnings.append("invalid_or_unauthorized_evidence")
                continue
            tier = int(rule["tier"])
            if tier not in allowed_tiers:
                warnings.append("source_tier_not_allowed_for_semantics")
                continue
            if not self.policy.rule_supports(rule, semantics):
                warnings.append("field_semantics_not_allowed_for_source")
                continue
            domain = self.policy.domain(url)
            service_verification = (
                item.get("_service_verification")
                if isinstance(item.get("_service_verification"), dict)
                else {}
            )
            acquired = _matching_acquired_source(
                url,
                acquired_sources,
                source_id=service_verification.get("source_id"),
            )
            observation = _matching_observation(url, observed_sources)
            if service_verification:
                if service_verification.get("accepted") is not True:
                    warnings.append(
                        str(service_verification.get("reason") or "evidence_verification_rejected")
                    )
                    continue
                if (
                    acquired is None
                    or acquired.get("fetch_status") != "FETCHED"
                    or not acquired.get("content_sha256")
                ):
                    warnings.append("verified_source_acquisition_missing")
                    continue
            else:
                if observation is None:
                    warnings.append("source_not_observed_or_opened")
                    continue
                observed_payload = observation.get("payload") or {}
                if (
                    not observation.get("content_hash")
                    and observed_payload.get("evidence_text_verified") is not True
                ):
                    warnings.append("observed_source_content_not_verified")
                    continue
            content_checksum = hashlib.sha256(evidence_text.lower().encode("utf-8")).hexdigest()
            key = (url, content_checksum)
            if key in seen:
                continue
            seen.add(key)
            published = parse_datetime(item.get("published_at"))
            if published_at_required and published is None:
                warnings.append(
                    "current_news_published_at_required"
                    if semantics in {"news", "current_news"}
                    else "published_at_required"
                )
                continue
            if published and published > now:
                warnings.append("future_evidence_timestamp")
                continue
            if (
                freshness_basis == "published_at"
                and published
                and ttl_minutes
                and published < now - timedelta(minutes=ttl_minutes)
            ):
                warnings.append("stale_evidence")
                continue
            rows.append(
                {
                    "query": item.get("query"),
                    "source_url": str(item.get("source_url") or url),
                    "canonical_url": url,
                    "publisher": rule.get("publisher") or item.get("publisher"),
                    "source_domain": domain,
                    "source_tier": tier,
                    "evidence_text": evidence_text,
                    "published_at": item.get("published_at"),
                    "retrieved_at": item.get("retrieved_at") or _now(),
                    "redirect_url": (
                        (acquired or {}).get("final_url")
                        if acquired
                        and (acquired or {}).get("final_url")
                        != (acquired or {}).get("requested_url")
                        else (observation or {}).get("redirect_url")
                    ),
                    "source_status": "VERIFIED",
                    "independent_source_group": f"domain:{domain}",
                    "content_checksum": content_checksum,
                    "source_content_hash": (
                        (acquired or {}).get("content_sha256")
                        or (observation or {}).get("content_hash")
                    ),
                    "tool_event_id": (observation or {}).get("event_id"),
                    "source_id": (acquired or {}).get("source_id"),
                    "verification_id": service_verification.get("verification_id"),
                    "verification_method": service_verification.get("match_method"),
                    "verification_reason": service_verification.get("reason"),
                    "verification_score": service_verification.get("match_score"),
                }
            )
            logger.info(
                "research_source_opened_and_verified",
                extra={"source_domain": domain, "source_tier": tier, "field_semantics": semantics},
            )
        domains_by_content: dict[str, set[str]] = {}
        for row in rows:
            domains_by_content.setdefault(row["content_checksum"], set()).add(row["source_domain"])
        for row in rows:
            if len(domains_by_content[row["content_checksum"]]) > 1:
                row["independent_source_group"] = f"content:{row['content_checksum']}"
        return rows, warnings

    def _project_and_read_back(
        self,
        conn: sqlite3.Connection,
        claim: dict[str, Any],
        evidence: list[dict[str, Any]],
    ) -> bool:
        if not evidence:
            raise ValueError("accepted_materializable_claim_requires_evidence")
        primary = min(evidence, key=lambda item: item["source_tier"])
        fact_key = f"research:{claim['claim_id']}"
        self.facts.upsert_fact_in_transaction(
            conn,
            {
                "fact_key": fact_key,
                "fact_type": "agentic_research_claim",
                "symbol": claim.get("symbol") or "MNQ",
                "category": claim.get("topic"),
                "event_name": claim.get("field_semantics"),
                "period": claim.get("period"),
                "value": _json(claim.get("value")),
                "unit": claim.get("unit"),
                "source": primary.get("publisher"),
                "source_url": primary["canonical_url"],
                "provider_type": "AI_RESEARCHER",
                "reliability": _tier_reliability(int(primary["source_tier"])),
                "confidence": claim.get("confidence") or 0,
                "retrieved_at": claim.get("retrieved_at"),
                "release_at": claim.get("release_at") or claim.get("event_at"),
                "valid_from": claim.get("valid_from"),
                "valid_until": claim.get("valid_until"),
                "next_refresh_at": claim.get("next_refresh_at"),
                "status": "active",
                "raw_payload_json": claim.get("payload"),
                "warnings_json": claim.get("warnings"),
                "policy_version": claim.get("policy_version"),
                "source_tier": primary["source_tier"],
                "source_classification": _classification(primary["source_tier"]),
                "canonical_url": primary["canonical_url"],
                "canonical_event_key": claim.get("event_key"),
            }
        )
        restored = self.facts.get_fact_in_transaction(conn, fact_key)
        return restored is not None and restored.get("status") == "active"

    def _claim_result(self, claim: dict[str, Any]) -> dict[str, Any]:
        evidence = self.evidence_for_claim(claim["claim_id"])
        return self._claim_result_from_evidence(claim, evidence)

    @staticmethod
    def _claim_result_from_evidence(
        claim: dict[str, Any],
        evidence: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not evidence:
            raise ValueError("materialized_claim_read_back_requires_evidence")
        primary = min(evidence, key=lambda item: item["source_tier"])
        independent_domains = sorted(
            {
                min(
                    item["source_domain"]
                    for item in evidence
                    if item["independent_source_group"] == group
                )
                for group in {item["independent_source_group"] for item in evidence}
            }
        )
        return {
            "field": claim["field_semantics"],
            "field_semantics": claim["field_semantics"],
            "value": claim["value"],
            "metric_id": claim.get("metric_id"),
            "period": claim.get("period"),
            "frequency": claim.get("frequency"),
            "unit": claim.get("unit"),
            "event_at": claim.get("event_at"),
            "release_at": claim.get("release_at"),
            "issuer": claim.get("issuer"),
            "valid_until": claim.get("valid_until"),
            "next_refresh_at": claim.get("next_refresh_at"),
            "lifecycle_status": claim.get("lifecycle_status"),
            "post_event_semantics": claim.get("post_event_semantics"),
            "source": primary.get("publisher"),
            "publisher": primary.get("publisher"),
            "source_url": primary["canonical_url"],
            "canonical_url": primary["canonical_url"],
            "evidence_text": primary["evidence_text"],
            "published_at": primary.get("published_at"),
            "retrieved_at": primary["retrieved_at"],
            "confidence": claim.get("confidence") or 0,
            "reliability": 0.0,
            "verified_independent_domains": independent_domains,
        }

    @staticmethod
    def _research_source_row(row: Any) -> dict[str, Any]:
        data = dict(row)
        data["redirect_chain"] = json.loads(data.pop("redirect_chain_json") or "[]")
        return normalize_payload_text(data)

    @staticmethod
    def _backend_invocation_row(row: Any) -> dict[str, Any]:
        data = dict(row)
        data["cost"] = json.loads(data.pop("cost_json") or "null")
        data["output"] = json.loads(data.pop("output_json") or "{}")
        return normalize_payload_text(data)

    @staticmethod
    def _run_row(row: Any) -> dict[str, Any]:
        data = dict(row)
        for key in (
            "request_json",
            "result_json",
            "required_topics_json",
            "completed_topics_json",
            "missing_topics_json",
            "blocking_gaps_json",
            "non_blocking_gaps_json",
            "source_domains_json",
            "warnings_json",
            "valid_not_applicable_topics_json",
            "usage_json",
            "cost_json",
            "metrics_json",
            "checkpoint_json",
            "threshold_warnings_json",
        ):
            data[key.removesuffix("_json")] = json.loads(
                data.pop(key)
                or (
                    "{}"
                    if key
                    in {
                        "request_json",
                        "result_json",
                        "usage_json",
                        "cost_json",
                        "metrics_json",
                        "checkpoint_json",
                    }
                    else "[]"
                )
            )
        return normalize_payload_text(data)

    @staticmethod
    def _step_row(row: Any) -> dict[str, Any]:
        data = dict(row)
        for key in (
            "input_json",
            "output_json",
            "source_domains_json",
            "telemetry_json",
            "diagnostic_json",
        ):
            data[key.removesuffix("_json")] = json.loads(
                data.pop(key)
                or (
                    "[]"
                    if key in {"source_domains_json", "telemetry_json"}
                    else "null"
                    if key == "diagnostic_json"
                    else "{}"
                )
            )
        return normalize_payload_text(data)

    @staticmethod
    def _step_attempt_row(row: Any) -> dict[str, Any]:
        data = dict(row)
        data["diagnostic"] = json.loads(data.pop("diagnostic_json") or "null")
        return data


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _event_envelope(
    event: dict[str, Any],
    *,
    phase: str,
    run_id: str,
    job_id: str,
    observed_at: str,
) -> dict[str, Any]:
    semantic_action = str(
        event.get("semantic_action")
        or (
            "search"
            if event.get("event_type") == "search"
            else "open_source"
            if event.get("event_type") in {"open_source", "server_source_verified"}
            else "non_operational"
        )
    )
    lifecycle = str(event.get("lifecycle") or "completed")
    item_type = str(
        event.get("item_type")
        or event.get("provider_tool_type")
        or event.get("event_type")
        or "unknown"
    )
    fingerprint = str(
        event.get("tool_action_fingerprint")
        or action_fingerprint(
            item_id=(str(event["item_id"]) if event.get("item_id") else None),
            item_type=item_type,
            phase=phase,
            semantic_action=semantic_action,
            query=str(event.get("query") or "") or None,
            source_url=str(event.get("canonical_url") or event.get("source_url") or "") or None,
        )
    )
    operational = bool(
        event.get("item_id")
        or event.get("query")
        or event.get("canonical_url")
        or event.get("source_url")
    )
    return {
        "raw_event_type": str(event.get("raw_event_type") or f"legacy.{lifecycle}")[:120],
        "raw_event_digest": str(event.get("raw_event_digest") or "")[:64] or None,
        "raw_shape": {
            "event_keys": [
                str(value)[:80] for value in (event.get("raw_shape") or {}).get("event_keys", [])
            ][:40],
            "item_keys": [
                str(value)[:80] for value in (event.get("raw_shape") or {}).get("item_keys", [])
            ][:40],
        },
        "provider_payload": event.get("provider_payload") or {},
        "lifecycle": lifecycle[:40],
        "item_id": str(event.get("item_id") or "")[:200] or None,
        "item_type": item_type[:120],
        "phase": str(event.get("phase") or phase)[:80],
        "run_id": run_id,
        "job_id": job_id,
        "provider_tool_type": str(event.get("provider_tool_type") or item_type)[:120],
        "semantic_action": semantic_action[:80],
        "event_type": (
            "search"
            if semantic_action == "search"
            else "open_source"
            if semantic_action in COUNTED_SOURCE_ACTIONS
            else "observed"
        ),
        "observed_at": str(event.get("observed_at") or observed_at)[:80],
        "query": str(event.get("query") or "")[:1000] or None,
        "source_url": str(event.get("source_url") or "")[:2048] or None,
        "canonical_url": str(event.get("canonical_url") or event.get("source_url") or "")[:2048]
        or None,
        "redirect_url": str(event.get("redirect_url") or "")[:2048] or None,
        "tool_action_fingerprint": fingerprint[:64],
        "status": str(event.get("status") or lifecycle)[:80],
        "usage": {
            key: max(int(value or 0), 0)
            for key, value in (event.get("usage") or {}).items()
            if key
            in {
                "input_tokens",
                "output_tokens",
                "cached_tokens",
                "reasoning_tokens",
                "total_tokens",
            }
        },
        "counts_usage": bool(
            event.get("counts_usage")
            if event.get("counts_usage") is not None
            else (operational and lifecycle == "completed" and semantic_action != "non_operational")
        ),
        "discovered_urls": [
            str(value)[:2048]
            for value in event.get("discovered_urls") or []
            if str(value).startswith("https://")
        ][:20],
        "content_hash": str(event.get("content_hash") or "")[:128] or None,
        "http_status": event.get("http_status"),
    }


def _merge_usage(
    existing: dict[str, Any],
    incoming: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(existing or {})
    if not incoming:
        return merged
    additive = {
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "reasoning_tokens",
        "total_tokens",
    }
    for key, value in incoming.items():
        if key in additive and value is not None:
            merged[key] = int(merged.get(key) or 0) + max(int(value), 0)
        elif key in {"cost", "cost_usd", "total_cost_usd"} and value is not None:
            merged[key] = float(merged.get(key) or 0) + max(
                float(value),
                0.0,
            )
    return merged


def _provided_cost(usage: dict[str, Any]) -> dict[str, float] | None:
    provided = {
        key: float(usage[key])
        for key in ("cost", "cost_usd", "total_cost_usd")
        if usage.get(key) is not None
    }
    return provided or None


def _cost_value(cost: dict[str, Any]) -> float:
    for key in ("total_cost_usd", "cost_usd", "cost"):
        if cost.get(key) is not None:
            return max(float(cost[key]), 0.0)
    return 0.0


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _checksum(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _canonical_url(value: str) -> str:
    try:
        parts = urlsplit(value.strip())
    except ValueError:
        return ""
    if not parts.scheme or not parts.netloc:
        return ""
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path or "/",
            parts.query,
            "",
        )
    )


def _classification(tier: int) -> str:
    return {1: "OFFICIAL", 2: "PRIMARY_MARKET", 3: "FINANCIAL_MEDIA", 4: "CALENDAR_CONSENSUS"}.get(
        tier, "SECONDARY_CONTEXT"
    )


def _tier_reliability(tier: int) -> float:
    return {1: 0.95, 2: 0.88, 3: 0.78, 4: 0.62}.get(tier, 0.5)


def _matching_observation(url: str, observations: list[dict[str, Any]]) -> dict[str, Any] | None:
    canonical = _canonical_url(url)
    for observation in observations:
        candidates = {
            _canonical_url(str(observation.get("source_url") or "")),
            _canonical_url(str(observation.get("canonical_url") or "")),
            _canonical_url(str(observation.get("redirect_url") or "")),
        }
        if canonical in candidates:
            return observation
    return None


def _matching_acquired_source(
    url: str,
    sources: list[dict[str, Any]],
    *,
    source_id: Any = None,
) -> dict[str, Any] | None:
    if source_id:
        direct = next(
            (source for source in sources if str(source.get("source_id") or "") == str(source_id)),
            None,
        )
        if direct is not None:
            return direct
    canonical = _canonical_url(url)
    for source in sources:
        candidates = {
            _canonical_url(str(source.get(key) or ""))
            for key in ("requested_url", "final_url", "canonical_url")
        }
        if canonical and canonical in candidates:
            return source
    return None


def _resolved_topic(run: dict[str, Any], claim: dict[str, Any]) -> str:
    if str(run.get("profile_id") or "") == "EVENT_MISSING_FIELDS":
        return str(claim.get("field_semantics") or "")
    return str(claim.get("topic") or "")


def _is_not_applicable(claim: dict[str, Any]) -> bool:
    payload = claim.get("payload") if isinstance(claim.get("payload"), dict) else {}
    status = str(payload.get("topic_status") or payload.get("status") or "").upper()
    value = str(claim.get("value") or "").upper()
    return status == "NOT_APPLICABLE" or value == "NOT_APPLICABLE"
