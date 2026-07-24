from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.temporal_validation_service import TemporalValidationService
from app.services.data_freshness_service import parse_datetime
from app.services.source_policy_service import SourcePolicyService
from app.infrastructure.persistence.database_safety import assert_test_database_isolated
from app.services.research_domain_contracts import (
    DOMAIN_TOPICS,
    build_domain_projection,
)
from app.services.market_context_outbox_service import (
    MarketContextOutboxRepository,
)
from app.services.event_driven_lifecycle_service import (
    DatumLifecycle,
    compute_datum_lifecycle,
    persist_lifecycle_in_transaction,
)


class MarketContextSnapshotRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        assert_test_database_isolated(
            settings.database_path,
            environment=settings.environment,
        )
        migrate_database(settings.database_path)
        self.temporal_validation = TemporalValidationService(settings)
        self.source_policy = SourcePolicyService()
        self.outbox = MarketContextOutboxRepository(settings)
        self.allow_test_reserved_sources = settings.environment.lower() == "test"

    def save_next(
        self,
        *,
        symbol: str,
        refresh_mode: str,
        debug_payload: dict[str, Any],
        ai_enrichment: dict[str, Any],
        source_job_id: str | None = None,
        job_ids: list[str] | None = None,
        research_run_id: str | None = None,
        parent_run_id: str | None = None,
        trigger_type: str | None = None,
        trigger_entity: str | None = None,
        trace_id: str | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Allocate revision and persist both payloads in one SQLite write transaction."""
        now = datetime.now(UTC).replace(microsecond=0).isoformat()
        snapshot_id = f"mcs-{uuid.uuid4()}"
        symbol = symbol.upper()
        temporal_debug = self.temporal_validation.sanitize_payload(
            dict(debug_payload),
            entity_table="market_context_snapshot_input",
        )
        invalid_sources = self.source_policy.invalid_sources(
            temporal_debug,
            allow_test_reserved=self.allow_test_reserved_sources,
        )
        debug = self.source_policy.sanitize_operational_payload(
            temporal_debug,
            allow_test_reserved=self.allow_test_reserved_sources,
        ) or {}
        audit = dict(debug.get("audit") or {})
        audit["temporal_quarantine"] = (
            self.temporal_validation.quarantine_read_model()
        )
        audit["source_validation"] = {
            "status": "sanitized" if invalid_sources else "accepted",
            "invalid_source_count": len(invalid_sources),
            "reason_codes": sorted(
                {str(item.reason_code) for item in invalid_sources if item.reason_code}
            ),
            "policy_version": self.source_policy.policy_version,
        }
        debug["audit"] = audit
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute(
                """
                SELECT snapshot_id,debug_payload_json
                FROM market_context_snapshots
                WHERE symbol=? AND audit_status='ACTIVE'
                  AND source_audit_status='ACTIVE'
                ORDER BY revision DESC LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            revision = int(conn.execute(
                "SELECT COALESCE(MAX(revision),0)+1 AS revision FROM market_context_snapshots WHERE symbol=?",
                (symbol,),
            ).fetchone()["revision"])
            research = self._exact_research_projection(
                conn,
                research_run_id=research_run_id,
                parent_run_id=parent_run_id,
                source_job_id=source_job_id,
            )
            if research is not None:
                research["snapshot_id"] = snapshot_id
                debug["research"] = research
                self._reconcile_research_claims(
                    conn,
                    debug,
                    research_run_id=research_run_id,
                    parent_run_id=parent_run_id,
                )
                for topic, projection in (
                    research.get("domains") or {}
                ).items():
                    if topic in DOMAIN_TOPICS:
                        debug[topic] = projection
            elif research_run_id or parent_run_id:
                raise ValueError("snapshot_research_link_invalid")
            debug.update({
                "snapshot_id": snapshot_id,
                "snapshot_revision": revision,
                "ai_enrichment": ai_enrichment,
                "source_job_id": source_job_id,
                "research_run_id": research_run_id,
                "parent_run_id": parent_run_id,
            })
            final_invalid_sources = self.source_policy.invalid_sources(
                debug,
                allow_test_reserved=self.allow_test_reserved_sources,
            )
            if final_invalid_sources:
                debug = self.source_policy.sanitize_operational_payload(
                    debug,
                    allow_test_reserved=self.allow_test_reserved_sources,
                ) or {}
                source_audit = dict((debug.get("audit") or {}).get("source_validation") or {})
                source_audit["status"] = "sanitized"
                source_audit["invalid_source_count"] = (
                    int(source_audit.get("invalid_source_count") or 0)
                    + len(final_invalid_sources)
                )
                source_audit["reason_codes"] = sorted(
                    {
                        *list(source_audit.get("reason_codes") or []),
                        *[
                            str(item.reason_code)
                            for item in final_invalid_sources
                            if item.reason_code
                        ],
                    }
                )
                debug.setdefault("audit", {})["source_validation"] = source_audit
            from app.services.ai_trader_consumer_v2_service import build_ai_trader_consumer_v2
            consumer = build_ai_trader_consumer_v2(debug, settings=self.settings)
            consumer = self.source_policy.sanitize_operational_payload(
                consumer,
                allow_test_reserved=self.allow_test_reserved_sources,
            ) or {}
            generated_at = str(debug.get("generated_at_utc") or debug.get("generated_at") or now)
            data_as_of = str(consumer.get("data_as_of") or generated_at)
            debug_json = self._json(debug)
            consumer_json = self._json(consumer)
            checksum = hashlib.sha256((debug_json + consumer_json).encode("utf-8")).hexdigest()
            conn.execute(
                """
                INSERT INTO market_context_snapshots(
                  snapshot_id,symbol,revision,generated_at,data_as_of,refresh_mode,
                  debug_payload_json,consumer_payload_json,ai_status,source_job_id,checksum,created_at,
                  research_run_id,parent_run_id,research_link_status
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    snapshot_id, symbol, revision, generated_at, data_as_of, refresh_mode,
                    debug_json, consumer_json, str(ai_enrichment.get("status") or "NOT_REQUIRED"),
                    source_job_id, checksum, now, research_run_id, parent_run_id,
                    "LINKED" if research is not None else "NOT_REQUIRED",
                ),
            )
            if trigger_type:
                self.outbox.emit_in_transaction(
                    conn,
                    trigger_type=trigger_type,
                    trigger_entity=trigger_entity,
                    snapshot_id=snapshot_id,
                    snapshot_revision=revision,
                    current_payload=debug,
                    previous_snapshot_id=(
                        str(previous["snapshot_id"]) if previous else None
                    ),
                    previous_payload=(
                        json.loads(previous["debug_payload_json"] or "{}")
                        if previous
                        else None
                    ),
                    trace_id=trace_id,
                    correlation_id=correlation_id,
                    data_as_of=data_as_of,
                    created_at=now,
                )
            self._persist_projected_lifecycle(conn, debug, timestamp=now)
            self._persist_components(
                conn,
                symbol=symbol,
                snapshot_id=snapshot_id,
                revision=revision,
                data_as_of=data_as_of,
                debug=debug,
                created_at=now,
            )
            for job_id in dict.fromkeys(job_ids or ([source_job_id] if source_job_id else [])):
                job = conn.execute("SELECT event_key FROM ai_research_jobs WHERE job_id=?", (job_id,)).fetchone()
                if job is None:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO market_context_snapshot_jobs(snapshot_id,job_id,event_key,created_at) VALUES (?,?,?,?)",
                    (snapshot_id, job_id, job["event_key"], now),
                )
                conn.execute("UPDATE ai_research_jobs SET snapshot_id=? WHERE job_id=?", (snapshot_id, job_id))
            conn.commit()
        restored = self.get(snapshot_id)
        if restored is None or restored["checksum"] != checksum:
            raise RuntimeError("market context snapshot read-back failed")
        return restored

    def latest_components(self, symbol: str = "MNQ") -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT component_name,component_json FROM market_context_components c
                WHERE symbol=? AND source_audit_status='ACTIVE' AND source_revision=(
                  SELECT MAX(source_revision) FROM market_context_components newer
                  WHERE newer.symbol=c.symbol
                    AND newer.component_name=c.component_name
                    AND newer.source_audit_status='ACTIVE'
                )
                ORDER BY component_name
                """,
                (symbol.upper(),),
            ).fetchall()
        return {
            str(row["component_name"]): json.loads(str(row["component_json"]))
            for row in rows
        }

    def save(
        self,
        *,
        snapshot_id: str,
        revision: int,
        symbol: str,
        refresh_mode: str,
        debug_payload: dict[str, Any],
        consumer_payload: dict[str, Any],
        ai_status: str,
        source_job_id: str | None = None,
    ) -> dict[str, Any]:
        """Compatibility helper for fixtures importing an already allocated immutable snapshot."""
        now = datetime.now(UTC).replace(microsecond=0).isoformat()
        debug_payload = self.source_policy.sanitize_operational_payload(
            debug_payload,
            allow_test_reserved=self.allow_test_reserved_sources,
        ) or {}
        consumer_payload = self.source_policy.sanitize_operational_payload(
            consumer_payload,
            allow_test_reserved=self.allow_test_reserved_sources,
        ) or {}
        generated_at = str(debug_payload.get("generated_at_utc") or debug_payload.get("generated_at") or now)
        data_as_of = str(consumer_payload.get("data_as_of") or generated_at)
        debug_json = self._json(debug_payload)
        consumer_json = self._json(consumer_payload)
        checksum = hashlib.sha256((debug_json + consumer_json).encode("utf-8")).hexdigest()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO market_context_snapshots(
                  snapshot_id,symbol,revision,generated_at,data_as_of,refresh_mode,
                  debug_payload_json,consumer_payload_json,ai_status,source_job_id,checksum,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (snapshot_id, symbol.upper(), revision, generated_at, data_as_of, refresh_mode,
                 debug_json, consumer_json, ai_status, source_job_id, checksum, now),
            )
            conn.commit()
        restored = self.get(snapshot_id)
        if restored is None or restored["checksum"] != checksum:
            raise RuntimeError("market context snapshot read-back failed")
        return restored

    def latest(self, symbol: str = "MNQ") -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM market_context_snapshots
                WHERE symbol=? AND audit_status='ACTIVE'
                  AND source_audit_status='ACTIVE'
                ORDER BY revision DESC LIMIT 1
                """,
                (symbol.upper(),),
            ).fetchone()
        return self._row(row) if row else None

    def get(self, snapshot_id: str) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute("SELECT * FROM market_context_snapshots WHERE snapshot_id=?", (snapshot_id,)).fetchone()
        return self._row(row) if row else None

    @staticmethod
    def _exact_research_projection(
        conn: Any,
        *,
        research_run_id: str | None,
        parent_run_id: str | None,
        source_job_id: str | None,
    ) -> dict[str, Any] | None:
        if parent_run_id:
            parent = conn.execute(
                "SELECT * FROM research_parent_runs WHERE parent_run_id=?",
                (parent_run_id,),
            ).fetchone()
            if parent is None or str(parent["status"]) not in {
                "SUCCEEDED",
                "PARTIAL",
                "NO_DATA",
            }:
                return None
            children = conn.execute(
                """
                SELECT child_run_id,topic,status FROM research_parent_children
                WHERE parent_run_id=? ORDER BY ordinal
                """,
                (parent_run_id,),
            ).fetchall()
            run_ids = [str(row["child_run_id"]) for row in children if row["child_run_id"]]
            return MarketContextSnapshotRepository._aggregate_research_projection(
                conn,
                run_ids,
                parent_run_id=parent_run_id,
                parent_status=str(parent["status"]),
            )
        if not research_run_id:
            return None
        run = conn.execute(
            """
            SELECT * FROM research_runs
            WHERE run_id=? AND source_audit_status='ACTIVE'
            """,
            (research_run_id,),
        ).fetchone()
        if (
            run is None
            or str(run["status"]) not in {"SUCCEEDED", "PARTIAL", "NO_DATA"}
            or (source_job_id and str(run["job_id"]) != str(source_job_id))
        ):
            return None
        return MarketContextSnapshotRepository._single_research_projection(
            conn,
            dict(run),
        )

    @staticmethod
    def _single_research_projection(conn: Any, run: dict[str, Any]) -> dict[str, Any]:
        run_id = str(run["run_id"])
        claim_count = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM research_claims
                WHERE research_run_id=? AND validation_status='accepted'
                  AND materialization_status='MATERIALIZED'
                  AND source_audit_status='ACTIVE'
                """,
                (run_id,),
            ).fetchone()[0]
        )
        evidence_count = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM research_evidence e
                JOIN research_claims c ON c.claim_id=e.claim_id
                WHERE c.research_run_id=? AND c.validation_status='accepted'
                  AND c.materialization_status='MATERIALIZED' AND e.audit_status='ACTIVE'
                  AND e.source_audit_status='ACTIVE'
                """,
                (run_id,),
            ).fetchone()[0]
        )
        source_domains = [
            str(row[0])
            for row in conn.execute(
                """
                SELECT DISTINCT e.source_domain FROM research_evidence e
                JOIN research_claims c ON c.claim_id=e.claim_id
                WHERE c.research_run_id=? AND c.validation_status='accepted'
                  AND c.materialization_status='MATERIALIZED' AND e.audit_status='ACTIVE'
                  AND e.source_audit_status='ACTIVE'
                ORDER BY e.source_domain
                """,
                (run_id,),
            ).fetchall()
        ]
        result = json.loads(run.get("result_json") or "{}")
        claim_rows = conn.execute(
            """
            SELECT topic,metric_id,value_json,unit,symbol,issuer,payload_json
            FROM research_claims
            WHERE research_run_id=? AND validation_status='accepted'
              AND materialization_status='MATERIALIZED'
              AND source_audit_status='ACTIVE'
            ORDER BY rowid
            """,
            (run_id,),
        ).fetchall()
        claims = []
        for row in claim_rows:
            payload = json.loads(row["payload_json"] or "{}")
            claims.append(
                {
                    **dict(row),
                    "value": json.loads(row["value_json"] or "null"),
                    "payload": payload,
                }
            )
        required_topics = json.loads(run.get("required_topics_json") or "[]")
        domains = {
            topic: build_domain_projection(
                topic,
                [claim for claim in claims if claim.get("topic") == topic],
                status=str(run["status"]),
                no_data_reason=result.get("no_data_reason"),
            )
            for topic in required_topics
            if topic in DOMAIN_TOPICS
        }
        return {
            "status": str(run["status"]),
            "execution_status": str(run["status"]),
            "execution_complete": str(run["status"])
            in {"SUCCEEDED", "PARTIAL", "NO_DATA"},
            "data_outcome": (
                "COMPLETE"
                if float(run.get("coverage_score") or 0) >= 1
                else "NO_DATA"
                if str(run["status"]) == "NO_DATA"
                else "PARTIAL"
            ),
            "coverage_complete": float(run.get("coverage_score") or 0) >= 1,
            "run_id": run_id,
            "job_id": str(run["job_id"]),
            "parent_run_id": run.get("parent_run_id"),
            "started_at": run.get("started_at"),
            "completed_at": run.get("completed_at"),
            "data_as_of": run.get("data_as_of"),
            "fresh_until": run.get("fresh_until"),
            "coverage_score": float(run.get("coverage_score") or 0),
            "required_topics": required_topics,
            "completed_topics": json.loads(run.get("completed_topics_json") or "[]"),
            "missing_topics": json.loads(run.get("missing_topics_json") or "[]"),
            "blocking_gaps": json.loads(run.get("blocking_gaps_json") or "[]"),
            "non_blocking_gaps": json.loads(run.get("non_blocking_gaps_json") or "[]"),
            "claim_count": claim_count,
            "evidence_count": evidence_count,
            "key_verified_drivers": list(result.get("key_verified_drivers") or []),
            "critical_evidence_references": list(
                result.get("critical_evidence_references") or []
            ),
            "source_domains": source_domains,
            "warnings": json.loads(run.get("warnings_json") or "[]"),
            "domains": domains,
        }

    @staticmethod
    def _aggregate_research_projection(
        conn: Any,
        run_ids: list[str],
        *,
        parent_run_id: str,
        parent_status: str,
    ) -> dict[str, Any]:
        parent = conn.execute(
            "SELECT * FROM research_parent_runs WHERE parent_run_id=?",
            (parent_run_id,),
        ).fetchone()
        parent_value = dict(parent) if parent is not None else {}
        projections = []
        for run_id in run_ids:
            row = conn.execute(
                """
                SELECT * FROM research_runs
                WHERE run_id=? AND source_audit_status='ACTIVE'
                """,
                (run_id,),
            ).fetchone()
            if row is not None and str(row["status"]) in {
                "SUCCEEDED",
                "PARTIAL",
                "NO_DATA",
            }:
                projections.append(
                    MarketContextSnapshotRepository._single_research_projection(
                        conn,
                        dict(row),
                    )
                )
        required = sorted(
            {topic for item in projections for topic in item["required_topics"]}
        )
        completed = sorted(
            {topic for item in projections for topic in item["completed_topics"]}
        )
        missing = sorted(set(required) - set(completed))
        coverage_score = len(completed) / len(required) if required else 1.0
        return {
            "status": parent_status,
            "execution_status": (
                parent_value.get("execution_status") or parent_status
            ),
            "execution_complete": bool(
                parent_value.get("execution_complete")
            ),
            "data_outcome": (
                parent_value.get("data_outcome")
                or ("COMPLETE" if not missing else "PARTIAL")
            ),
            "coverage_complete": bool(
                parent_value.get("coverage_complete")
            ),
            "run_id": parent_run_id,
            "job_id": None,
            "parent_run_id": parent_run_id,
            "child_run_ids": run_ids,
            "started_at": min(
                (item["started_at"] for item in projections if item["started_at"]),
                default=None,
            ),
            "completed_at": max(
                (item["completed_at"] for item in projections if item["completed_at"]),
                default=None,
            ),
            "data_as_of": max(
                (item["data_as_of"] for item in projections if item["data_as_of"]),
                default=None,
            ),
            "fresh_until": min(
                (item["fresh_until"] for item in projections if item["fresh_until"]),
                default=None,
            ),
            "coverage_score": coverage_score,
            "required_topics": required,
            "completed_topics": completed,
            "missing_topics": missing,
            "blocking_gaps": sorted(
                {gap for item in projections for gap in item["blocking_gaps"]}
            ),
            "non_blocking_gaps": sorted(
                {gap for item in projections for gap in item["non_blocking_gaps"]}
            ),
            "policy_no_data_topics": json.loads(
                parent_value.get("policy_no_data_topics_json") or "[]"
            ),
            "ready_for_trading_context": bool(
                parent_value.get("ready_for_trading_context")
            ),
            "claim_count": sum(item["claim_count"] for item in projections),
            "evidence_count": sum(item["evidence_count"] for item in projections),
            "key_verified_drivers": [
                value
                for item in projections
                for value in item["key_verified_drivers"]
            ][:8],
            "critical_evidence_references": [
                value
                for item in projections
                for value in item["critical_evidence_references"]
            ][:8],
            "source_domains": sorted(
                {domain for item in projections for domain in item["source_domains"]}
            ),
            "warnings": sorted(
                {warning for item in projections for warning in item["warnings"]}
            ),
            "domains": {
                topic: projection
                for item in projections
                for topic, projection in (item.get("domains") or {}).items()
            },
        }

    @staticmethod
    def _persist_components(
        conn: Any,
        *,
        symbol: str,
        snapshot_id: str,
        revision: int,
        data_as_of: str,
        debug: dict[str, Any],
        created_at: str,
    ) -> None:
        derived = {
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
            "source_job_id",
            "research_run_id",
            "parent_run_id",
        }
        for name, value in debug.items():
            if name in derived:
                continue
            encoded = MarketContextSnapshotRepository._json(value)
            valid_until = (
                value.get("valid_until") or value.get("fresh_until")
                if isinstance(value, dict)
                else None
            )
            conn.execute(
                """
                INSERT INTO market_context_components(
                  symbol,component_name,source_snapshot_id,source_revision,
                  data_as_of,valid_until,component_checksum,component_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    symbol,
                    str(name),
                    snapshot_id,
                    revision,
                    data_as_of,
                    valid_until,
                    hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                    encoded,
                    created_at,
                ),
            )

    @staticmethod
    def _persist_projected_lifecycle(
        conn: Any,
        debug: dict[str, Any],
        *,
        timestamp: str,
    ) -> None:
        earnings = ((debug.get("nasdaq_context") or {}).get("earnings") or {})
        for bucket in (
            "upcoming",
            "recent",
            "items",
            "events",
            "upcoming_mega_cap_earnings_14d",
            "released_earnings",
        ):
            for item in earnings.get(bucket) or []:
                if not isinstance(item, dict):
                    continue
                lifecycle = item.get("lifecycle")
                if not isinstance(lifecycle, dict):
                    continue
                contract = DatumLifecycle(**lifecycle)
                persist_lifecycle_in_transaction(
                    conn,
                    contract,
                    payload=item,
                    work_status=(
                        "READY"
                        if contract.freshness_state in {
                            "DUE",
                            "AWAITING_ACTUAL",
                        }
                        else "IDLE"
                    ),
                    timestamp=timestamp,
                )

    def _reconcile_research_claims(
        self,
        conn: Any,
        debug: dict[str, Any],
        *,
        research_run_id: str | None,
        parent_run_id: str | None,
    ) -> None:
        run_ids: list[str] = []
        if parent_run_id:
            run_ids = [
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT child_run_id FROM research_parent_children
                    WHERE parent_run_id=? AND child_run_id IS NOT NULL
                    ORDER BY ordinal
                    """,
                    (parent_run_id,),
                ).fetchall()
            ]
        elif research_run_id:
            run_ids = [research_run_id]
        if not run_ids:
            return
        placeholders = ",".join("?" for _ in run_ids)
        rows = conn.execute(
            f"""
            SELECT claim_id,topic,field_semantics,metric_id,event_key,symbol,
                   issuer,event_at,release_at,valid_until,next_refresh_at,
                   confirmation_status,value_json,payload_json
            FROM research_claims
            WHERE research_run_id IN ({placeholders})
              AND validation_status='accepted'
              AND materialization_status='MATERIALIZED'
              AND source_audit_status='ACTIVE'
            ORDER BY created_at,claim_id
            """,
            run_ids,
        ).fetchall()
        claims: list[dict[str, Any]] = []
        for row in rows:
            claim = dict(row)
            claim["value"] = json.loads(claim.pop("value_json") or "null")
            claim["payload"] = json.loads(claim.pop("payload_json") or "{}")
            evidence_rows = conn.execute(
                """
                SELECT canonical_url,source_domain,source_tier,publisher,
                       source_status,retrieved_at,published_at
                FROM research_evidence
                WHERE claim_id=? AND audit_status='ACTIVE'
                  AND source_audit_status='ACTIVE'
                ORDER BY source_tier,canonical_url
                """,
                (claim["claim_id"],),
            ).fetchall()
            claim["lineage"] = [dict(item) for item in evidence_rows]
            claims.append(claim)
        self._reconcile_earnings(debug, claims)
        self._reconcile_cot(debug, claims)

    def _reconcile_earnings(
        self,
        debug: dict[str, Any],
        claims: list[dict[str, Any]],
    ) -> None:
        confirmations = [
            claim
            for claim in claims
            if str(claim.get("field_semantics") or "").lower()
            in {"earnings_schedule", "earnings", "guidance"}
            or str(claim.get("topic") or "").lower() == "earnings_intelligence"
        ]
        earnings = ((debug.get("nasdaq_context") or {}).get("earnings") or {})
        if not isinstance(earnings, dict):
            return
        for bucket in (
            "upcoming",
            "recent",
            "items",
            "events",
            "upcoming_mega_cap_earnings_14d",
            "released_earnings",
        ):
            items = earnings.get(bucket)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                match = next(
                    (
                        claim
                        for claim in confirmations
                        if _same_issuer_event(item, claim)
                    ),
                    None,
                )
                if match is None:
                    continue
                lineage = [
                    *list(item.get("source_lineage") or []),
                    *list(match.get("lineage") or []),
                ]
                deduplicated = {
                    str(entry.get("canonical_url") or entry.get("source_domain")): entry
                    for entry in lineage
                    if isinstance(entry, dict)
                }
                item["source_lineage"] = list(deduplicated.values())
                best = next(iter(item["source_lineage"]), {})
                rule = self.source_policy.rule_for(
                    best.get("canonical_url"),
                    best.get("publisher"),
                )
                item["source_tier"] = best.get("source_tier")
                if rule is not None:
                    item["reliability"] = float(rule["base_reliability"])
                item["event_at"] = match.get("event_at") or match.get("release_at")
                item["valid_until"] = match.get("valid_until")
                item["next_refresh_at"] = (
                    match.get("next_refresh_at") or match.get("event_at")
                )
                item["confirmation_status"] = (
                    match.get("confirmation_status") or "VERIFIED"
                )
                item["confirmation_claim_id"] = match.get("claim_id")
                item["lifecycle"] = compute_datum_lifecycle(
                    "earnings_schedule",
                    (
                        f"{item.get('ticker') or item.get('symbol') or item.get('issuer')}:"
                        f"{str(item['event_at'])[:10]}"
                    ),
                    item,
                    settings=self.settings,
                    now=datetime.now(UTC),
                    refresh_reason="verified_cross_child_reconciliation",
                ).as_dict()

    def _reconcile_cot(
        self,
        debug: dict[str, Any],
        claims: list[dict[str, Any]],
    ) -> None:
        cot_claims = [
            claim
            for claim in claims
            if str(claim.get("topic") or "").lower() == "cot_positioning"
        ]
        if not cot_claims:
            return
        values = {
            str(claim.get("metric_id") or "").lower(): claim.get("value")
            for claim in cot_claims
            if claim.get("metric_id")
        }
        report_date = values.get("cot_report_date") or values.get("report_date")
        contract_code = (
            values.get("cot_contract")
            or values.get("contract_code")
            or values.get("market_code")
        )
        open_interest = values.get("open_interest") or values.get(
            "cot_open_interest"
        )
        groups: dict[str, dict[str, Any]] = {}
        for metric, value in values.items():
            for group in ("asset_manager", "leveraged_fund", "dealer", "noncommercial"):
                if group not in metric:
                    continue
                target = groups.setdefault(group, {})
                for side in ("long", "short", "spread", "change"):
                    if side in metric:
                        target[side] = value
        complete_groups = {
            group: value
            for group, value in groups.items()
            if value.get("long") is not None and value.get("short") is not None
        }
        positioning = dict(debug.get("positioning") or {})
        if report_date is None or contract_code is None or not complete_groups:
            positioning.update(
                {
                    "status": "PARTIAL",
                    "coverage_complete": False,
                    "missing_fields": [
                        field
                        for field, present in (
                            ("report_date", report_date is not None),
                            ("contract_code", contract_code is not None),
                            ("positions", bool(complete_groups)),
                        )
                        if not present
                    ],
                }
            )
            debug["positioning"] = positioning
            return
        positioning.update(
            {
                "status": "AVAILABLE",
                "coverage_complete": True,
                "report_date": report_date,
                "contract_code": contract_code,
                "open_interest": open_interest,
                "asset_managers": complete_groups.get("asset_manager", {}),
                "leveraged_funds": complete_groups.get("leveraged_fund", {}),
                "dealers": complete_groups.get("dealer", {}),
                "noncommercial": complete_groups.get("noncommercial", {}),
                "source_lineage": [
                    evidence
                    for claim in cot_claims
                    for evidence in claim.get("lineage") or []
                ],
            }
        )
        positioning["lifecycle"] = compute_datum_lifecycle(
            "cot",
            str(contract_code),
            positioning,
            settings=self.settings,
            now=datetime.now(UTC),
            refresh_reason="verified_cot_claim_projection",
        ).as_dict()
        debug["positioning"] = positioning

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)

    def source_quarantine_read_model(self) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM source_quarantine").fetchone()[0])
            reasons = {
                str(row["reason_code"]): int(row["count"])
                for row in conn.execute(
                    "SELECT reason_code,COUNT(*) AS count FROM source_quarantine GROUP BY reason_code"
                )
            }
            domains = [
                str(row["source_domain"])
                for row in conn.execute(
                    "SELECT DISTINCT source_domain FROM source_quarantine ORDER BY source_domain"
                )
            ]
            latest = conn.execute("SELECT MAX(detected_at) FROM source_quarantine").fetchone()[0]
            snapshots = int(
                conn.execute(
                    "SELECT COUNT(*) FROM market_context_snapshots "
                    "WHERE source_audit_status='QUARANTINED'"
                ).fetchone()[0]
            )
        return {
            "invalid_source_count": total,
            "by_reason_code": reasons,
            "domains": domains,
            "last_detected_at": latest,
            "unusable_snapshot_count": snapshots,
        }

    @staticmethod
    def _row(row: Any) -> dict[str, Any]:
        data = dict(row)
        data["debug_payload"] = json.loads(data.pop("debug_payload_json"))
        data["consumer_payload"] = json.loads(data.pop("consumer_payload_json"))
        return data


def _same_issuer_event(item: dict[str, Any], claim: dict[str, Any]) -> bool:
    item_symbol = str(item.get("ticker") or item.get("symbol") or "").upper()
    claim_symbol = str(
        claim.get("symbol")
        or (claim.get("payload") or {}).get("ticker")
        or ""
    ).upper()
    item_issuer = _canonical_issuer(
        item.get("issuer") or item.get("company") or item.get("company_name")
    )
    claim_issuer = _canonical_issuer(claim.get("issuer"))
    identity_matches = bool(
        (item_symbol and claim_symbol and _share_class_root(item_symbol) == _share_class_root(claim_symbol))
        or (item_issuer and claim_issuer and item_issuer == claim_issuer)
    )
    if not identity_matches:
        return False
    item_time = parse_datetime(
        item.get("event_at")
        or item.get("earnings_date")
        or item.get("date")
    )
    claim_time = parse_datetime(claim.get("event_at") or claim.get("release_at"))
    return bool(
        item_time is None
        or claim_time is None
        or item_time.date() == claim_time.date()
    )


def _canonical_issuer(value: Any) -> str:
    text = "".join(character for character in str(value or "").upper() if character.isalnum())
    for suffix in ("INCORPORATED", "CORPORATION", "COMPANY", "INC", "CORP", "LTD"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _share_class_root(symbol: str) -> str:
    return "GOOG" if symbol in {"GOOG", "GOOGL"} else symbol.split(".", 1)[0]
