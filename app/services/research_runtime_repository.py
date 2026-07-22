from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.data_freshness_service import parse_datetime
from app.services.market_fact_repository import MarketFactRepository
from app.services.research_profiles import PROFILES
from app.services.source_policy_service import SourcePolicyService


logger = logging.getLogger(__name__)


class ResearchRuntimeRepository:
    def __init__(
        self,
        settings: Settings,
        *,
        source_policy: SourcePolicyService | None = None,
        facts: MarketFactRepository | None = None,
    ) -> None:
        self.settings = settings
        self.policy = source_policy or SourcePolicyService(settings.source_policy_path)
        self.facts = facts or MarketFactRepository(settings)
        migrate_database(settings.database_path)

    def ensure_run(self, job: dict[str, Any], profile_id: str, prompt_version: str) -> dict[str, Any]:
        now = _now()
        request = job.get("request_payload") or {}
        fingerprint = str(job.get("input_fingerprint") or _checksum(request))
        profile = PROFILES.get(profile_id)
        required_topics = (
            sorted({str(item) for item in request.get("pending_fields") or []})
            if profile_id == "EVENT_MISSING_FIELDS"
            else list(profile.required_topics) if profile else []
        )
        run_id = f"rrun-{uuid.uuid4()}"
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO research_runs(
                  run_id,job_id,symbol,event_key,profile_id,prompt_version,policy_version,status,
                  input_fingerprint,request_json,required_topics_json,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,'PENDING',?,?,?,?,?)
                """,
                (
                    run_id, job["job_id"], job.get("symbol") or "MNQ", job.get("event_key"),
                    profile_id, prompt_version, job.get("policy_version") or self.policy.policy_version,
                    fingerprint, _json(request), _json(required_topics), now, now,
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM research_runs WHERE job_id=?", (job["job_id"],)).fetchone()
        return self._run_row(row)

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
                "SELECT * FROM research_run_steps WHERE run_id=? AND step_name=?", (run_id, step_name)
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
                    (step_id, run_id, step_name, ordinal, _checksum(input_payload), input_json, backend, tool, now),
                )
            else:
                conn.execute(
                    """
                    UPDATE research_run_steps SET status='RUNNING',attempt=attempt+1,input_checksum=?,
                      input_json=?,backend=?,tool=?,started_at=?,completed_at=NULL,error=NULL
                    WHERE step_id=?
                    """,
                    (_checksum(input_payload), input_json, backend, tool, now, row["step_id"]),
                )
            conn.execute(
                "UPDATE research_runs SET status='RUNNING',started_at=COALESCE(started_at,?),updated_at=? WHERE run_id=?",
                (now, now, run_id),
            )
            conn.commit()
            restored = conn.execute(
                "SELECT * FROM research_run_steps WHERE run_id=? AND step_name=?", (run_id, step_name)
            ).fetchone()
        return self._step_row(restored), True

    def complete_step(self, step_id: str, output: dict[str, Any], *, source_domains: list[str] | None = None) -> dict[str, Any]:
        now = _now()
        output_json = _json(output)
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_run_steps SET status='COMPLETED',output_checksum=?,output_json=?,
                  source_domains_json=?,completed_at=?,duration_ms=CAST((julianday(?) - julianday(started_at))*86400000 AS INTEGER)
                WHERE step_id=?
                """,
                (_checksum(output), output_json, _json(sorted(set(source_domains or []))), now, now, step_id),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM research_run_steps WHERE step_id=?", (step_id,)).fetchone()
        return self._step_row(row)

    def fail_step(self, step_id: str, error: str) -> None:
        now = _now()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                "UPDATE research_run_steps SET status='FAILED',error=?,completed_at=? WHERE step_id=?",
                (error[:1000], now, step_id),
            )
            conn.commit()

    def record_tool_events(
        self,
        run_id: str,
        step_id: str,
        events: list[dict[str, Any]],
        *,
        usage: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        now = _now()
        with connect_sqlite(self.settings.database_path) as conn:
            for event in events:
                if not isinstance(event, dict):
                    continue
                normalized = {
                    **event,
                    "source_url": _canonical_url(str(event.get("source_url") or "")) or None,
                    "canonical_url": _canonical_url(str(event.get("canonical_url") or event.get("source_url") or "")) or None,
                    "observed_at": str(event.get("observed_at") or now),
                }
                checksum = _checksum(normalized)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO research_tool_events(
                      event_id,run_id,step_id,event_type,source_url,canonical_url,redirect_url,
                      observed_at,content_hash,http_status,usage_json,payload_json,created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        f"rtool-{hashlib.sha256(f'{run_id}|{checksum}'.encode()).hexdigest()[:24]}", run_id, step_id,
                        str(normalized.get("event_type") or "unknown"), normalized.get("source_url"),
                        normalized.get("canonical_url"), normalized.get("redirect_url"),
                        normalized["observed_at"], normalized.get("content_hash"),
                        normalized.get("http_status"), _json(usage) if usage else None,
                        _json(normalized), now,
                    ),
                )
            counts = conn.execute(
                """
                SELECT
                  SUM(CASE WHEN event_type='search' THEN 1 ELSE 0 END) AS searches,
                  SUM(CASE WHEN event_type IN ('open_source','server_source_verified') THEN 1 ELSE 0 END) AS opened
                FROM research_tool_events WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            searches = int(counts["searches"] or 0)
            opened = int(counts["opened"] or 0)
            cost = None
            if usage:
                provided_cost = {
                    key: usage[key]
                    for key in ("cost", "cost_usd", "total_cost_usd")
                    if usage.get(key) is not None
                }
                cost = provided_cost or None
            conn.execute(
                """
                UPDATE research_runs
                SET search_count=?,opened_source_count=?,usage_json=?,cost_json=?,updated_at=?
                WHERE run_id=?
                """,
                (
                    searches, opened, _json(usage) if usage else None,
                    _json(cost) if cost else None, now, run_id,
                ),
            )
            conn.execute(
                "UPDATE research_run_steps SET telemetry_json=? WHERE step_id=?",
                (_json(events), step_id),
            )
            conn.commit()
        return {"search_count": searches, "opened_source_count": opened}

    def observed_sources(self, run_id: str) -> list[dict[str, Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM research_tool_events
                WHERE run_id=? AND event_type IN ('open_source','server_source_verified')
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

    def persist_claims(
        self,
        run: dict[str, Any],
        claims: list[dict[str, Any]],
    ) -> dict[str, Any]:
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        evidence_count = 0
        read_back_count = 0
        source_domains: set[str] = set()
        observed_sources = self.observed_sources(str(run["run_id"]))
        for claim in claims:
            restored, evidence_rows = self._persist_claim(run, claim, observed_sources)
            evidence_count += len(evidence_rows)
            source_domains.update(row["source_domain"] for row in evidence_rows)
            if restored["validation_status"] == "accepted":
                accepted.append(restored)
                if self._project_and_read_back(restored, evidence_rows):
                    read_back_count += 1
            else:
                rejected.append(restored)
        required_topics = {str(item) for item in run.get("required_topics") or []}
        completed_topics = {
            _resolved_topic(run, item) for item in accepted if not _is_not_applicable(item)
        } & required_topics
        not_applicable_topics = {
            _resolved_topic(run, item) for item in accepted if _is_not_applicable(item)
        } & required_topics
        resolved_topics = completed_topics | not_applicable_topics
        missing_topics = required_topics - resolved_topics
        coverage_score = len(resolved_topics) / max(len(required_topics), 1)
        data_claim_count = sum(1 for item in accepted if not _is_not_applicable(item))
        status = (
            "NO_DATA" if data_claim_count == 0
            else "PARTIAL" if missing_topics
            else "SUCCEEDED"
        )
        blocking_gaps = [f"missing_topic:{item}" for item in sorted(missing_topics)]
        now = _now()
        result_payload = {
            "accepted_claims": accepted, "rejected_claims": rejected,
            "accepted_count": len(accepted), "candidate_count": len(claims),
            "persisted_count": len(accepted), "read_back_count": read_back_count,
            "evidence_count": evidence_count, "source_domains": sorted(source_domains),
            "required_topics": sorted(required_topics), "completed_topics": sorted(completed_topics),
            "valid_not_applicable_topics": sorted(not_applicable_topics),
            "missing_topics": sorted(missing_topics), "blocking_gaps": blocking_gaps,
            "coverage_score": coverage_score,
        }
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                UPDATE research_runs SET status=?,result_json=?,coverage_score=?,source_domains_json=?,
                  completed_topics_json=?,missing_topics_json=?,warnings_json=?,data_as_of=?,updated_at=?
                WHERE run_id=?
                """,
                (
                    status, _json(result_payload),
                    coverage_score, _json(sorted(source_domains)),
                    _json(sorted(completed_topics)), _json(sorted(missing_topics)),
                    _json([warning for item in rejected for warning in item.get("warnings") or []]),
                    now, now, run["run_id"],
                ),
            )
            conn.execute(
                """
                UPDATE research_runs SET valid_not_applicable_topics_json=?,blocking_gaps_json=?
                WHERE run_id=?
                """,
                (_json(sorted(not_applicable_topics)), _json(blocking_gaps), run["run_id"]),
            )
            conn.commit()
        return {
            "status": status, **result_payload,
            "results": [self._claim_result(item) for item in accepted if item["field_semantics"] in {
                "forecast", "consensus", "previous", "outcome", "transcript_url",
            }],
        }

    def finish_run(self, run_id: str, status: str, result: dict[str, Any]) -> dict[str, Any]:
        now = _now()
        with connect_sqlite(self.settings.database_path) as conn:
            existing = conn.execute("SELECT result_json FROM research_runs WHERE run_id=?", (run_id,)).fetchone()
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
        return self._run_row(row) if row else None

    def latest(self, symbol: str = "MNQ") -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT * FROM research_runs WHERE symbol=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
                (symbol.upper(),),
            ).fetchone()
        return self._run_row(row) if row else None

    def evidence_for_claim(self, claim_id: str) -> list[dict[str, Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                "SELECT * FROM research_evidence WHERE claim_id=? ORDER BY source_tier,source_domain", (claim_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def _persist_claim(
        self,
        run: dict[str, Any],
        claim: dict[str, Any],
        observed_sources: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        now = _now()
        semantics = str(claim.get("field_semantics") or claim.get("field") or "exploratory_context").lower()
        evidence_input = [item for item in claim.get("evidence") or [] if isinstance(item, dict)]
        evidence_rows, warnings = self._validated_evidence(semantics, evidence_input, observed_sources)
        semantic_policy = self.policy.semantic_policy(semantics)
        groups = {row["independent_source_group"] for row in evidence_rows}
        required = int(semantic_policy.get("required_confirmations") or 1)
        if len(groups) < required:
            warnings.append("insufficient_independent_evidence")
        if semantics in {"actual", "forecast", "consensus", "previous"}:
            for field in ("metric_id", "period", "frequency", "unit"):
                if claim.get(field) in (None, ""):
                    warnings.append(f"missing_{field}")
        if semantics == "actual":
            warnings.append("actual_requires_deterministic_official_resolver")
        published = parse_datetime(claim.get("published_at"))
        if published and published > datetime.now(UTC):
            warnings.append("future_published_at_rejected")
        validation = "accepted" if evidence_rows and not warnings else "rejected"
        claim_payload = {**claim, "field_semantics": semantics, "warnings": sorted(set(warnings))}
        checksum = _checksum(claim_payload)
        claim_seed = f"{run['run_id']}|{checksum}"
        claim_id = str(claim.get("claim_id") or f"claim-{hashlib.sha256(claim_seed.encode()).hexdigest()[:24]}")
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO research_claims(
                  claim_id,research_run_id,topic,field_semantics,value_json,metric_id,period,frequency,
                  unit,event_key,symbol,valid_from,valid_until,published_at,retrieved_at,confidence,
                  validation_status,warnings_json,policy_version,prompt_version,payload_json,checksum,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    claim_id, run["run_id"], str(claim.get("topic") or semantics), semantics,
                    _json(claim.get("value")), claim.get("metric_id"), claim.get("period"),
                    claim.get("frequency"), claim.get("unit"), claim.get("event_key") or run.get("event_key"),
                    claim.get("symbol") or run.get("symbol"), claim.get("valid_from"), claim.get("valid_until"),
                    claim.get("published_at"), claim.get("retrieved_at") or now,
                    min(
                        float(claim.get("confidence") or 0),
                        float(semantic_policy.get("max_reliability") or 1.0),
                    ), validation, _json(sorted(set(warnings))),
                    run["policy_version"], run["prompt_version"], _json(claim_payload), checksum, now,
                ),
            )
            for evidence in evidence_rows:
                evidence_seed = f"{claim_id}|{evidence['canonical_url']}|{evidence['content_checksum']}"
                evidence_id = f"evidence-{hashlib.sha256(evidence_seed.encode()).hexdigest()[:24]}"
                conn.execute(
                    """
                    INSERT OR IGNORE INTO research_evidence(
                      evidence_id,claim_id,query_text,source_url,canonical_url,publisher,source_domain,
                      source_tier,evidence_text,published_at,retrieved_at,redirect_url,source_status,
                      independent_source_group,content_checksum,policy_version,created_at,source_content_hash
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        evidence_id, claim_id, evidence.get("query"), evidence["source_url"],
                        evidence["canonical_url"], evidence.get("publisher"), evidence["source_domain"],
                        evidence["source_tier"], evidence["evidence_text"], evidence.get("published_at"),
                        evidence["retrieved_at"], evidence.get("redirect_url"), evidence.get("source_status"),
                        evidence["independent_source_group"], evidence["content_checksum"],
                        run["policy_version"], now, evidence.get("source_content_hash"),
                    ),
                )
            conn.commit()
            row = conn.execute("SELECT * FROM research_claims WHERE claim_id=?", (claim_id,)).fetchone()
        restored = dict(row)
        restored["value"] = json.loads(restored.pop("value_json") or "null")
        restored["warnings"] = json.loads(restored.pop("warnings_json") or "[]")
        restored["payload"] = json.loads(restored.pop("payload_json"))
        logger.info(
            "research_claim_policy_validated",
            extra={
                "run_id": run["run_id"], "claim_id": claim_id, "field_semantics": semantics,
                "validation_status": validation, "evidence_count": len(evidence_rows),
            },
        )
        return restored, evidence_rows

    def _validated_evidence(
        self,
        semantics: str,
        items: list[dict[str, Any]],
        observed_sources: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        semantic_policy = self.policy.semantic_policy(semantics)
        allowed_tiers = {int(item) for item in semantic_policy.get("allowed_tiers") or range(1, 6)}
        ttl_minutes = int(semantic_policy.get("ttl_minutes") or 0)
        now = datetime.now(UTC)
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
            domain = self.policy.domain(url)
            observation = _matching_observation(url, observed_sources)
            if observation is None:
                warnings.append("source_not_observed_or_opened")
                continue
            observed_payload = observation.get("payload") or {}
            if not observation.get("content_hash") and observed_payload.get("evidence_text_verified") is not True:
                warnings.append("observed_source_content_not_verified")
                continue
            content_checksum = hashlib.sha256(evidence_text.lower().encode("utf-8")).hexdigest()
            key = (url, content_checksum)
            if key in seen:
                continue
            seen.add(key)
            published = parse_datetime(item.get("published_at"))
            if semantics == "news" and published is None:
                warnings.append("news_published_at_required")
                continue
            if published and published > now:
                warnings.append("future_evidence_timestamp")
                continue
            if published and ttl_minutes and published < now - timedelta(minutes=ttl_minutes):
                warnings.append("stale_evidence")
                continue
            rows.append({
                "query": item.get("query"), "source_url": str(item.get("source_url") or url),
                "canonical_url": url, "publisher": rule.get("publisher") or item.get("publisher"),
                "source_domain": domain, "source_tier": tier, "evidence_text": evidence_text,
                "published_at": item.get("published_at"), "retrieved_at": item.get("retrieved_at") or _now(),
                "redirect_url": observation.get("redirect_url"), "source_status": "VERIFIED",
                "independent_source_group": f"domain:{domain}", "content_checksum": content_checksum,
                "source_content_hash": observation.get("content_hash"),
            })
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

    def _project_and_read_back(self, claim: dict[str, Any], evidence: list[dict[str, Any]]) -> bool:
        primary = min(evidence, key=lambda item: item["source_tier"])
        fact_key = f"research:{claim['claim_id']}"
        self.facts.upsert_fact({
            "fact_key": fact_key, "fact_type": "agentic_research_claim",
            "symbol": claim.get("symbol") or "MNQ", "category": claim.get("topic"),
            "event_name": claim.get("field_semantics"), "period": claim.get("period"),
            "value": _json(claim.get("value")), "unit": claim.get("unit"),
            "source": primary.get("publisher"), "source_url": primary["canonical_url"],
            "provider_type": "AI_RESEARCHER_CODEX_CLI", "reliability": 0.0,
            "confidence": claim.get("confidence") or 0, "retrieved_at": claim.get("retrieved_at"),
            "valid_from": claim.get("valid_from"), "valid_until": claim.get("valid_until"),
            "status": "active", "raw_payload_json": claim.get("payload"),
            "warnings_json": claim.get("warnings"), "policy_version": claim.get("policy_version"),
            "source_tier": primary["source_tier"], "source_classification": _classification(primary["source_tier"]),
            "canonical_url": primary["canonical_url"], "canonical_event_key": claim.get("event_key"),
        })
        return self.facts.get_fact(fact_key) is not None

    def _claim_result(self, claim: dict[str, Any]) -> dict[str, Any]:
        evidence = self.evidence_for_claim(claim["claim_id"])
        primary = min(evidence, key=lambda item: item["source_tier"])
        independent_domains = sorted({
            min(item["source_domain"] for item in evidence if item["independent_source_group"] == group)
            for group in {item["independent_source_group"] for item in evidence}
        })
        return {
            "field": claim["field_semantics"], "field_semantics": claim["field_semantics"],
            "value": claim["value"], "metric_id": claim.get("metric_id"), "period": claim.get("period"),
            "frequency": claim.get("frequency"), "unit": claim.get("unit"),
            "source": primary.get("publisher"), "publisher": primary.get("publisher"),
            "source_url": primary["canonical_url"], "canonical_url": primary["canonical_url"],
            "evidence_text": primary["evidence_text"], "published_at": primary.get("published_at"),
            "retrieved_at": primary["retrieved_at"], "confidence": claim.get("confidence") or 0,
            "reliability": 0.0,
            "verified_independent_domains": independent_domains,
        }

    @staticmethod
    def _run_row(row: Any) -> dict[str, Any]:
        data = dict(row)
        for key in (
            "request_json", "result_json", "required_topics_json", "completed_topics_json",
            "missing_topics_json", "blocking_gaps_json", "non_blocking_gaps_json",
            "source_domains_json", "warnings_json", "valid_not_applicable_topics_json",
            "usage_json", "cost_json",
        ):
            data[key.removesuffix("_json")] = json.loads(
                data.pop(key) or ("{}" if key in {"request_json", "result_json", "usage_json", "cost_json"} else "[]")
            )
        return data

    @staticmethod
    def _step_row(row: Any) -> dict[str, Any]:
        data = dict(row)
        for key in ("input_json", "output_json", "source_domains_json", "telemetry_json"):
            data[key.removesuffix("_json")] = json.loads(
                data.pop(key) or ("[]" if key in {"source_domains_json", "telemetry_json"} else "{}")
            )
        return data


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _checksum(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _canonical_url(value: str) -> str:
    try:
        parts = urlsplit(value.strip())
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", parts.query, ""))
    except ValueError:
        return ""


def _classification(tier: int) -> str:
    return {1: "OFFICIAL", 2: "PRIMARY_MARKET", 3: "FINANCIAL_MEDIA", 4: "CALENDAR_CONSENSUS"}.get(tier, "SECONDARY_CONTEXT")


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


def _resolved_topic(run: dict[str, Any], claim: dict[str, Any]) -> str:
    if str(run.get("profile_id") or "") == "EVENT_MISSING_FIELDS":
        return str(claim.get("field_semantics") or "")
    return str(claim.get("topic") or "")


def _is_not_applicable(claim: dict[str, Any]) -> bool:
    payload = claim.get("payload") if isinstance(claim.get("payload"), dict) else {}
    status = str(payload.get("topic_status") or payload.get("status") or "").upper()
    value = str(claim.get("value") or "").upper()
    return status == "NOT_APPLICABLE" or value == "NOT_APPLICABLE"
