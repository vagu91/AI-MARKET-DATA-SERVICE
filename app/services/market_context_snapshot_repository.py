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


class MarketContextSnapshotRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        migrate_database(settings.database_path)
        self.temporal_validation = TemporalValidationService(settings)

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
    ) -> dict[str, Any]:
        """Allocate revision and persist both payloads in one SQLite write transaction."""
        now = datetime.now(UTC).replace(microsecond=0).isoformat()
        snapshot_id = f"mcs-{uuid.uuid4()}"
        symbol = symbol.upper()
        debug = self.temporal_validation.sanitize_payload(
            dict(debug_payload),
            entity_table="market_context_snapshot_input",
        )
        audit = dict(debug.get("audit") or {})
        audit["temporal_quarantine"] = (
            self.temporal_validation.quarantine_read_model()
        )
        debug["audit"] = audit
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
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
            from app.services.ai_trader_consumer_v2_service import build_ai_trader_consumer_v2
            consumer = build_ai_trader_consumer_v2(debug, settings=self.settings)
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
                WHERE symbol=? AND source_revision=(
                  SELECT MAX(source_revision) FROM market_context_components newer
                  WHERE newer.symbol=c.symbol
                    AND newer.component_name=c.component_name
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
            "SELECT * FROM research_runs WHERE run_id=?",
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
                  AND materialization_status='ELIGIBLE'
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
                  AND c.materialization_status='ELIGIBLE' AND e.audit_status='ACTIVE'
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
                  AND c.materialization_status='ELIGIBLE' AND e.audit_status='ACTIVE'
                ORDER BY e.source_domain
                """,
                (run_id,),
            ).fetchall()
        ]
        result = json.loads(run.get("result_json") or "{}")
        return {
            "status": str(run["status"]),
            "run_id": run_id,
            "job_id": str(run["job_id"]),
            "parent_run_id": run.get("parent_run_id"),
            "started_at": run.get("started_at"),
            "completed_at": run.get("completed_at"),
            "data_as_of": run.get("data_as_of"),
            "fresh_until": run.get("fresh_until"),
            "coverage_score": float(run.get("coverage_score") or 0),
            "required_topics": json.loads(run.get("required_topics_json") or "[]"),
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
        }

    @staticmethod
    def _aggregate_research_projection(
        conn: Any,
        run_ids: list[str],
        *,
        parent_run_id: str,
        parent_status: str,
    ) -> dict[str, Any]:
        projections = []
        for run_id in run_ids:
            row = conn.execute(
                "SELECT * FROM research_runs WHERE run_id=?",
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
        return {
            "status": parent_status,
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
            "coverage_score": (
                sum(item["coverage_score"] for item in projections) / len(projections)
                if projections
                else 0.0
            ),
            "required_topics": required,
            "completed_topics": completed,
            "missing_topics": sorted(set(required) - set(completed)),
            "blocking_gaps": sorted(
                {gap for item in projections for gap in item["blocking_gaps"]}
            ),
            "non_blocking_gaps": sorted(
                {gap for item in projections for gap in item["non_blocking_gaps"]}
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
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _row(row: Any) -> dict[str, Any]:
        data = dict(row)
        data["debug_payload"] = json.loads(data.pop("debug_payload_json"))
        data["consumer_payload"] = json.loads(data.pop("consumer_payload_json"))
        return data
