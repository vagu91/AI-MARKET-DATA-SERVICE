from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.text_normalization import normalize_payload_text
from app.core.redaction import redact_payload
from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database

FACT_COLUMNS = [
    "fact_key", "fact_type", "country", "symbol", "category", "event_name", "period", "value", "unit",
    "forecast", "previous", "consensus", "actual", "source", "source_url", "provider_type", "reliability",
    "confidence", "retrieved_at", "release_at", "valid_from", "valid_until", "next_refresh_at", "status",
    "raw_payload_json", "notes", "warnings_json", "errors_json", "field_lineage_json",
    "policy_version", "source_tier", "source_classification", "canonical_url",
    "canonical_event_key", "created_at", "updated_at",
]
CANONICAL_EVENT_ENRICHMENT_TYPE = "macro_event_enrichment"
LEGACY_EVENT_ENRICHMENT_TYPE = "ai_research_result"
EVENT_ENRICHMENT_TYPES = (CANONICAL_EVENT_ENRICHMENT_TYPE, LEGACY_EVENT_ENRICHMENT_TYPE)


def now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def encode(value: Any) -> str | None:
    value = redact_payload(value)
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, default=str, sort_keys=True)


def decode(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def database_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _numeric(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value).replace("%", "").replace(",", ".").strip())
    except (InvalidOperation, ValueError):
        return None


def _surprise(actual: Any, baseline: Any) -> tuple[Decimal | None, str | None]:
    actual_number = _numeric(actual)
    baseline_number = _numeric(baseline)
    if actual_number is None or baseline_number is None:
        return None, None
    difference = actual_number - baseline_number
    direction = "above_consensus" if difference > 0 else "below_consensus" if difference < 0 else "in_line"
    return difference, direction


def _semantic_surprise(
    row: Any,
    raw_payload: dict[str, Any],
    candidate: dict[str, Any],
) -> tuple[Decimal | None, str | None, bool, list[str]]:
    baseline_field = "consensus" if row["consensus"] not in (None, "") else "forecast"
    baseline_value = row[baseline_field]
    if baseline_value in (None, ""):
        return None, None, False, ["surprise_baseline_missing"]
    actual_metric = str(candidate.get("event_metric_id") or candidate.get("metric_id") or "")
    enrichment = raw_payload.get("enrichment") if isinstance(raw_payload.get("enrichment"), dict) else {}
    metric = next(
        (
            item for item in enrichment.get("metrics") or []
            if isinstance(item, dict) and str(item.get("metric_id") or "") == actual_metric
        ),
        None,
    )
    lineage = (enrichment.get("field_lineage") or {}).get(baseline_field) or {}
    baseline = {
        "metric_id": (metric or {}).get("metric_id") or lineage.get("metric_id"),
        "period": (metric or {}).get("period") or lineage.get("period") or row["period"],
        "frequency": (metric or {}).get("frequency") or lineage.get("frequency"),
        "unit": (metric or {}).get("unit") or lineage.get("unit"),
        "seasonal_adjustment": (metric or {}).get("seasonal_adjustment") or lineage.get("seasonal_adjustment"),
    }
    actual = {
        "metric_id": actual_metric,
        "period": candidate.get("reference_period") or candidate.get("period"),
        "frequency": candidate.get("frequency"),
        "unit": candidate.get("unit"),
        "seasonal_adjustment": candidate.get("seasonal_adjustment"),
    }
    warnings: list[str] = []
    for field, normalizer in (
        ("metric_id", _semantic_token), ("period", _period_token),
        ("frequency", _frequency_token), ("unit", _unit_token),
        ("seasonal_adjustment", _semantic_token),
    ):
        baseline_value_semantic = baseline.get(field)
        if field == "seasonal_adjustment" and not baseline_value_semantic and baseline.get("metric_id") == actual_metric:
            baseline_value_semantic = actual.get(field)
        if normalizer(baseline_value_semantic) != normalizer(actual.get(field)):
            warnings.append(f"surprise_{field}_mismatch")
    if warnings:
        return None, None, False, warnings
    surprise_value, surprise_direction = _surprise(candidate.get("value"), baseline_value)
    return surprise_value, surprise_direction, surprise_value is not None, []


def _semantic_token(value: Any) -> str:
    return "_".join(str(value or "").strip().lower().replace("-", "_").split())


def _period_token(value: Any) -> str:
    text = str(value or "").strip().lower().replace("month:", "").replace("quarter:", "q")
    text = text.replace("/", "-").replace(":", "-")
    match = re.fullmatch(r"(20\d{2})-(\d{1,2})", text)
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"
    return text


def _frequency_token(value: Any) -> str:
    text = _semantic_token(value)
    if text in {"mom", "monthly", "month_over_month"}:
        return "monthly"
    if text in {"qoq", "quarterly", "qoq_annualized", "qoq_annualised"}:
        return "quarterly"
    if text in {"yoy", "annual", "year_over_year"}:
        return "monthly"
    return text


def _unit_token(value: Any) -> str:
    text = _semantic_token(value)
    if text in {"%", "percent", "percentage", "percentage_points"}:
        return "percent"
    if text in {"k", "thousand", "thousands", "thousands_of_jobs"}:
        return "thousands_of_jobs"
    return text


def _candidate_lineage(candidate: dict[str, Any], policy_version: str) -> dict[str, Any]:
    return {
        key: candidate.get(key)
        for key in (
            "source_domain", "source", "source_url", "canonical_url", "publisher",
            "source_tier", "source_classification", "published_at", "retrieved_at",
            "evidence_text", "metric_id", "period", "frequency", "unit",
            "field_semantics", "reliability", "confidence", "validation_status", "warnings",
        )
    } | {"policy_version": policy_version}


def _merge_event_payload(existing: dict[str, Any], incoming: dict[str, Any], row: Any) -> dict[str, Any]:
    merged = {**existing, **incoming}
    existing_enrichment = dict(existing.get("enrichment") or {})
    incoming_enrichment = dict(incoming.get("enrichment") or {})
    lineage = {
        **dict(existing_enrichment.get("field_lineage") or {}),
        **dict(incoming_enrichment.get("field_lineage") or {}),
    }
    enrichment = {**existing_enrichment, **incoming_enrichment, "field_lineage": lineage}
    for field in ("forecast", "previous", "consensus", "actual"):
        if incoming_enrichment.get(field) in (None, "") and existing_enrichment.get(field) not in (None, ""):
            enrichment[field] = existing_enrichment[field]
    merged["enrichment"] = enrichment
    if incoming.get("actual") in (None, "") and row["actual"] not in (None, ""):
        merged["actual"] = row["actual"]
    if row["actual_source"]:
        merged["actual_source"] = row["actual_source"]
        merged["actual_source_url"] = row["actual_source_url"]
        merged["surprise_value"] = row["surprise_value"]
        merged["surprise_direction"] = row["surprise_direction"]
    if row["outcome_json"]:
        merged["outcome"] = decode(row["outcome_json"], {})
    terminal = str(row["temporal_status"] or row["status"] or "").upper()
    if terminal in {"RELEASED", "COMPLETED", "ACTUAL_UNAVAILABLE"}:
        merged["temporal_status"] = terminal
    return merged


def _event_record_payload(row: Any) -> dict[str, Any]:
    payload = decode(row["raw_payload_json"], {})
    payload.update({
        "event_id": row["event_id"], "canonical_event_key": row["canonical_event_key"],
        "country": row["country"], "category": row["category"], "name": row["name"],
        "reference_period": row["period"], "date": row["date"], "time_utc": row["time_utc"],
        "time_local": row["time_local"], "impact": row["impact"], "source": row["source"],
        "source_url": row["source_url"], "reliability": row["official_reliability"],
        "actual": row["actual"], "actual_source": row["actual_source"],
        "actual_source_url": row["actual_source_url"], "surprise_value": row["surprise_value"],
        "surprise_direction": row["surprise_direction"], "release_at": row["release_at"],
        "event_kind": row["event_kind"], "temporal_status": str(row["temporal_status"] or row["status"] or "").upper(),
        "actual_semantics": {
            "event_metric_id": row["actual_metric_id"], "unit": row["actual_unit"],
            "frequency": row["actual_frequency"], "seasonal_adjustment": row["actual_seasonal_adjustment"],
            "reference_period": row["actual_reference_period"], "transformation": row["actual_transformation"],
            "semantic_compatible": bool(row["actual_semantic_compatible"]),
            "warnings": decode(row["semantic_warnings_json"], []),
        },
    })
    enrichment = dict(payload.get("enrichment") or {})
    enrichment.update({
        "forecast": row["forecast"], "previous": row["previous"], "consensus": row["consensus"],
        "actual": row["actual"], "field_lineage": decode(row["field_lineage_json"], {}),
    })
    payload["enrichment"] = enrichment
    if row["outcome_json"]:
        payload["outcome"] = decode(row["outcome_json"], {})
    return payload


def connect_market_db(settings: Settings) -> Any:
    return connect_sqlite(settings.database_path)


def init_market_db(settings: Settings) -> None:
    migrate_database(settings.database_path)


class MarketFactRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        init_market_db(settings)

    def upsert_fact(self, fact: dict[str, Any]) -> dict[str, Any]:
        payload = normalize_payload_text(dict(fact))
        if payload.get("fact_type") == LEGACY_EVENT_ENRICHMENT_TYPE:
            payload["fact_type"] = CANONICAL_EVENT_ENRICHMENT_TYPE
        if payload.get("fact_type") == "official_macro_latest" and payload.get("category"):
            payload["fact_key"] = _canonical_macro_fact_key(payload)
            existing = self.get_fact(payload["fact_key"])
            if existing and _official_macro_rank(existing) > _official_macro_rank(payload):
                return existing
        timestamp = now_iso()
        payload.setdefault("retrieved_at", timestamp)
        payload.setdefault("created_at", timestamp)
        payload["updated_at"] = timestamp
        payload.setdefault("status", "active")
        for key in ("warnings_json", "errors_json", "raw_payload_json", "field_lineage_json"):
            payload[key] = encode(payload.get(key))
        columns = [column for column in FACT_COLUMNS if column in payload]
        updates = ", ".join(
            f"{column}=excluded.{column}" for column in columns if column not in {"fact_key", "created_at"}
        )
        with connect_market_db(self.settings) as conn:
            conn.execute(
                f"""
                INSERT INTO market_facts ({", ".join(columns)}) VALUES ({", ".join("?" for _ in columns)})
                ON CONFLICT(fact_key) DO UPDATE SET {updates}
                """,
                [database_value(payload[column]) for column in columns],
            )
            conn.commit()
        return self.get_fact(payload["fact_key"]) or payload

    def get_fact(self, fact_key: str) -> dict[str, Any] | None:
        with connect_market_db(self.settings) as conn:
            row = conn.execute("SELECT * FROM market_facts WHERE fact_key = ?", (fact_key,)).fetchone()
        return self._row(row)

    def get_event_enrichment_fact(self, fact_key: str) -> dict[str, Any] | None:
        canonical_key = fact_key.replace(f":{LEGACY_EVENT_ENRICHMENT_TYPE}", f":{CANONICAL_EVENT_ENRICHMENT_TYPE}")
        legacy_key = canonical_key.replace(f":{CANONICAL_EVENT_ENRICHMENT_TYPE}", f":{LEGACY_EVENT_ENRICHMENT_TYPE}")
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                """
                SELECT * FROM market_facts
                WHERE fact_key IN (?, ?)
                  AND fact_type IN (?, ?)
                """,
                (canonical_key, legacy_key, *EVENT_ENRICHMENT_TYPES),
            ).fetchall()
        facts = [self._row(row) for row in rows if row]
        return max((fact for fact in facts if fact), key=_event_fact_rank, default=None)

    def search_facts(self, country: str | None = None, category: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if country:
            where.append("country = ?")
            params.append(country.upper())
        if category:
            where.append("category = ?")
            params.append(category.upper())
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                f"SELECT * FROM market_facts {clause} ORDER BY updated_at DESC LIMIT ?",
                [*params, limit],
            ).fetchall()
        return [self._row(row) for row in rows if row]

    def stale_facts(self, limit: int = 200) -> list[dict[str, Any]]:
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                "SELECT * FROM market_facts WHERE valid_until IS NOT NULL AND valid_until <= ? ORDER BY valid_until ASC LIMIT ?",
                (now_iso(), limit),
            ).fetchall()
        return [self._row(row) for row in rows if row]

    def coverage(self, *, country: str = "US", days: int = 30) -> dict[str, Any]:
        with connect_market_db(self.settings) as conn:
            total = conn.execute("SELECT COUNT(*) c FROM market_facts WHERE country = ?", (country.upper(),)).fetchone()["c"]
            active = conn.execute(
                """
                SELECT COUNT(*) c FROM market_facts
                WHERE country = ? AND status = 'active'
                  AND (valid_until IS NULL OR valid_until > ?)
                """,
                (country.upper(), now_iso()),
            ).fetchone()["c"]
            stale = conn.execute(
                "SELECT COUNT(*) c FROM market_facts WHERE country = ? AND valid_until IS NOT NULL AND valid_until <= ?",
                (country.upper(), now_iso()),
            ).fetchone()["c"]
            by_category = conn.execute(
                "SELECT category, COUNT(*) c FROM market_facts WHERE country = ? GROUP BY category",
                (country.upper(),),
            ).fetchall()
        return {
            "country": country.upper(),
            "days": days,
            "total_facts": total,
            "active_facts": active,
            "stale_facts": stale,
            "by_category": {row["category"] or "unknown": row["c"] for row in by_category},
        }

    def count(self, *, fact_type: str | None = None) -> int:
        if fact_type == CANONICAL_EVENT_ENRICHMENT_TYPE:
            clause = "WHERE fact_type IN (?, ?)"
            params = EVENT_ENRICHMENT_TYPES
        else:
            clause = "WHERE fact_type = ?" if fact_type else ""
            params = (fact_type,) if fact_type else ()
        with connect_market_db(self.settings) as conn:
            return int(conn.execute(f"SELECT COUNT(*) c FROM market_facts {clause}", params).fetchone()["c"])

    def active_count(self) -> int:
        with connect_market_db(self.settings) as conn:
            return int(
                conn.execute(
                    """
                    SELECT COUNT(*) c FROM market_facts
                    WHERE status = 'active' AND (valid_until IS NULL OR valid_until > ?)
                    """,
                    (now_iso(),),
                ).fetchone()["c"]
            )

    def get_valid_facts_by_type(self, fact_type: str, *, allow_stale: bool = False) -> list[dict[str, Any]]:
        fact_types = EVENT_ENRICHMENT_TYPES if fact_type == CANONICAL_EVENT_ENRICHMENT_TYPE else (fact_type,)
        placeholders = ", ".join("?" for _ in fact_types)
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                f"SELECT * FROM market_facts WHERE fact_type IN ({placeholders}) AND status = 'active' ORDER BY updated_at DESC",
                fact_types,
            ).fetchall()
        facts = [self._row(row) for row in rows if row]
        if fact_type == CANONICAL_EVENT_ENRICHMENT_TYPE:
            selected: dict[str, dict[str, Any]] = {}
            for fact in facts:
                if not fact:
                    continue
                identity = _canonical_event_fact_key(str(fact.get("fact_key") or ""))
                current = selected.get(identity)
                if current is None or _event_fact_rank(fact) > _event_fact_rank(current):
                    selected[identity] = fact
            facts = list(selected.values())
        now = datetime.now(UTC)
        if allow_stale:
            return [fact for fact in facts if fact]
        return [
            fact for fact in facts
            if fact and (
                fact.get("valid_until") is None
                or datetime.fromisoformat(str(fact["valid_until"]).replace("Z", "+00:00")) > now
            )
        ]

    def upsert_economic_event(self, event: Any, event_key: str, *, valid_until: str | None = None) -> None:
        from app.services.temporal_domain_service import canonical_event_key, temporal_event_state

        timestamp = now_iso()
        payload = normalize_payload_text(event.model_dump(mode="json") if hasattr(event, "model_dump") else dict(event))
        time_utc = payload.get("time_utc")
        forecast = (payload.get("enrichment") or {}).get("forecast") if isinstance(payload.get("enrichment"), dict) else None
        previous = (payload.get("enrichment") or {}).get("previous") if isinstance(payload.get("enrichment"), dict) else None
        consensus = (payload.get("enrichment") or {}).get("consensus") if isinstance(payload.get("enrichment"), dict) else None
        actual = payload.get("actual")
        if actual in (None, "") and isinstance(payload.get("enrichment"), dict):
            actual = (payload.get("enrichment") or {}).get("actual")
        temporal = temporal_event_state(payload)
        actual = temporal["actual"]
        canonical_key = canonical_event_key(payload)
        field_lineage = (payload.get("enrichment") or {}).get("field_lineage") if isinstance(payload.get("enrichment"), dict) else {}
        with connect_market_db(self.settings) as conn:
            existing = conn.execute(
                "SELECT * FROM economic_events_history WHERE event_key=? OR canonical_event_key=? ORDER BY updated_at DESC LIMIT 1",
                (event_key, canonical_key),
            ).fetchone()
            terminal_status = temporal["temporal_status"]
            if existing is not None:
                event_key = str(existing["event_key"])
                existing_raw = decode(existing["raw_payload_json"], {})
                payload = _merge_event_payload(existing_raw, payload, existing)
                if actual in (None, "") and existing["actual"] not in (None, ""):
                    actual = existing["actual"]
                if str(existing["temporal_status"] or existing["status"] or "").upper() in {
                    "RELEASED", "COMPLETED", "ACTUAL_UNAVAILABLE"
                }:
                    terminal_status = str(existing["temporal_status"] or existing["status"]).upper()
                existing_lineage = decode(existing["field_lineage_json"], {})
                field_lineage = {**existing_lineage, **field_lineage}
            conn.execute(
                """
                INSERT INTO economic_events_history (
                    event_id, event_key, country, category, name, period, date, time_utc, time_local,
                    impact, event_risk_level, source, source_url, official_reliability,
                    forecast, previous, consensus, actual, release_at, valid_until, status,
                    raw_payload_json, created_at, updated_at, canonical_event_key,event_kind,
                    temporal_status,field_lineage_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO UPDATE SET
                    country=excluded.country,
                    category=excluded.category,
                    name=excluded.name,
                    period=excluded.period,
                    date=excluded.date,
                    time_utc=excluded.time_utc,
                    time_local=excluded.time_local,
                    impact=excluded.impact,
                    event_risk_level=excluded.event_risk_level,
                    source=excluded.source,
                    source_url=excluded.source_url,
                    official_reliability=excluded.official_reliability,
                    forecast=COALESCE(excluded.forecast,economic_events_history.forecast),
                    previous=COALESCE(excluded.previous,economic_events_history.previous),
                    consensus=COALESCE(excluded.consensus,economic_events_history.consensus),
                    actual=COALESCE(excluded.actual,economic_events_history.actual),
                    release_at=excluded.release_at,
                    valid_until=excluded.valid_until,
                    status=excluded.status,
                    raw_payload_json=excluded.raw_payload_json,
                    canonical_event_key=excluded.canonical_event_key,
                    event_kind=excluded.event_kind,
                    temporal_status=excluded.temporal_status,
                    field_lineage_json=COALESCE(excluded.field_lineage_json,economic_events_history.field_lineage_json),
                    updated_at=excluded.updated_at
                """,
                (
                    payload.get("event_id"),
                    event_key,
                    payload.get("country"),
                    payload.get("category"),
                    payload.get("name"),
                    payload.get("reference_period") or payload.get("period"),
                    payload.get("date"),
                    time_utc,
                    payload.get("time_local"),
                    payload.get("impact"),
                    payload.get("event_risk_level"),
                    payload.get("source"),
                    payload.get("source_url"),
                    payload.get("reliability"),
                    forecast,
                    previous,
                    consensus,
                    actual,
                    time_utc,
                    valid_until,
                    terminal_status,
                    encode(payload),
                    timestamp,
                    timestamp,
                    canonical_key,
                    temporal["event_kind"],
                    terminal_status,
                    encode(field_lineage),
                ),
            )
            conn.commit()

    def apply_event_research_field(
        self,
        *,
        canonical_event_key: str,
        candidate: dict[str, Any],
        policy_version: str,
    ) -> dict[str, int]:
        field = str(candidate.get("field") or candidate.get("field_semantics") or "")
        if field not in {"forecast", "consensus", "previous"}:
            raise ValueError(f"unsupported research field: {field}")
        value = candidate.get("value")
        if value in (None, ""):
            raise ValueError("accepted research field has no value")
        timestamp = now_iso()
        with connect_market_db(self.settings) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM economic_events_history WHERE canonical_event_key=? ORDER BY updated_at DESC LIMIT 1",
                (canonical_event_key,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise ValueError("canonical economic event not found")
            lineage = decode(row["field_lineage_json"], {})
            lineage[field] = _candidate_lineage(candidate, policy_version)
            raw_payload = decode(row["raw_payload_json"], {})
            enrichment = dict(raw_payload.get("enrichment") or {})
            enrichment[field] = value
            enrichment["field_lineage"] = lineage
            raw_payload["enrichment"] = enrichment
            conn.execute(
                f"UPDATE economic_events_history SET {field}=?,field_lineage_json=?,policy_version=?,raw_payload_json=?,updated_at=? WHERE id=?",
                (str(value), encode(lineage), policy_version, encode(raw_payload), timestamp, row["id"]),
            )
            conn.commit()
        restored_history = self._event_history_row(canonical_event_key)
        if restored_history is None or str(restored_history[field]) != str(value):
            raise RuntimeError("event research history read-back failed")
        fact_key = f"{canonical_event_key}:research_enrichment"
        self.upsert_fact({
            "fact_key": fact_key,
            "fact_type": "macro_event_enrichment",
            "country": restored_history["country"],
            "category": restored_history["category"],
            "event_name": restored_history["name"],
            "period": candidate.get("period") or restored_history["period"],
            "unit": candidate.get("unit"),
            "forecast": restored_history["forecast"],
            "previous": restored_history["previous"],
            "consensus": restored_history["consensus"],
            "actual": restored_history["actual"],
            "source": candidate.get("source"),
            "source_url": candidate.get("canonical_url") or candidate.get("source_url"),
            "provider_type": "AI_RESEARCHER",
            "reliability": candidate.get("reliability") or 0,
            "confidence": candidate.get("confidence") or 0,
            "retrieved_at": candidate.get("retrieved_at") or timestamp,
            "release_at": restored_history["release_at"],
            "status": "active",
            "raw_payload_json": raw_payload,
            "field_lineage_json": lineage,
            "policy_version": policy_version,
            "source_tier": candidate.get("source_tier"),
            "source_classification": candidate.get("source_classification"),
            "canonical_url": candidate.get("canonical_url"),
            "canonical_event_key": canonical_event_key,
        })
        restored_fact = self.get_fact(fact_key)
        if restored_fact is None or str(restored_fact.get(field)) != str(value):
            raise RuntimeError("event research fact read-back failed")
        return {"persisted_count": 1, "read_back_count": 1}

    def apply_official_event_actual(
        self,
        *,
        canonical_event_key: str,
        candidate: dict[str, Any],
        policy_version: str,
    ) -> dict[str, Any]:
        actual = candidate.get("value") if candidate.get("value") not in (None, "") else candidate.get("actual")
        if actual in (None, ""):
            raise ValueError("official actual candidate has no value")
        timestamp = now_iso()
        source = str(candidate.get("source") or candidate.get("publisher") or "")
        source_url = str(candidate.get("canonical_url") or candidate.get("source_url") or "")
        if not source or not source_url:
            raise ValueError("official actual candidate requires source and URL")
        with connect_market_db(self.settings) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM economic_events_history WHERE canonical_event_key=? ORDER BY updated_at DESC LIMIT 1",
                (canonical_event_key,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise ValueError("canonical economic event not found")
            raw_payload = decode(row["raw_payload_json"], {})
            surprise_value, surprise_direction, semantic_compatible, semantic_warnings = _semantic_surprise(
                row, raw_payload, candidate
            )
            lineage = decode(row["field_lineage_json"], {})
            lineage["actual"] = {
                "source_domain": candidate.get("source_domain"),
                "source": source,
                "source_url": source_url,
                "canonical_url": candidate.get("canonical_url"),
                "publisher": candidate.get("publisher"),
                "source_tier": candidate.get("source_tier"),
                "source_classification": candidate.get("source_classification"),
                "published_at": candidate.get("published_at"),
                "retrieved_at": candidate.get("retrieved_at") or timestamp,
                "evidence_text": candidate.get("evidence_text"),
                "metric_id": candidate.get("metric_id"),
                "period": candidate.get("period"),
                "frequency": candidate.get("frequency"),
                "unit": candidate.get("unit"),
                "field_semantics": "actual",
                "reliability": candidate.get("reliability"),
                "confidence": candidate.get("confidence"),
                "validation_status": candidate.get("validation_status") or "accepted",
                "warnings": candidate.get("warnings") or [],
                "policy_version": policy_version,
            }
            raw_payload["actual"] = actual
            raw_payload["actual_semantics"] = {
                "event_metric_id": candidate.get("event_metric_id") or candidate.get("metric_id"),
                "source_series_id": candidate.get("source_series_id"),
                "transformation": candidate.get("transformation"),
                "unit": candidate.get("unit"), "frequency": candidate.get("frequency"),
                "seasonal_adjustment": candidate.get("seasonal_adjustment"),
                "reference_period": candidate.get("reference_period") or candidate.get("period"),
                "release_vintage": candidate.get("release_vintage"),
                "semantic_compatible": semantic_compatible,
                "warnings": semantic_warnings,
            }
            enrichment = dict(raw_payload.get("enrichment") or {})
            enrichment.update({"actual": actual, "field_lineage": lineage})
            raw_payload["enrichment"] = enrichment
            conn.execute(
                """
                UPDATE economic_events_history
                SET actual=?,actual_source=?,actual_source_url=?,surprise_value=?,surprise_direction=?,
                    actual_retrieved_at=?,field_lineage_json=?,policy_version=?,temporal_status='RELEASED',
                    status='RELEASED',raw_payload_json=?,updated_at=?,actual_metric_id=?,actual_unit=?,
                    actual_frequency=?,actual_seasonal_adjustment=?,actual_reference_period=?,
                    actual_transformation=?,actual_semantic_compatible=?,semantic_warnings_json=?
                WHERE id=?
                """,
                (
                    str(actual), source, source_url,
                    None if surprise_value is None else str(surprise_value), surprise_direction,
                    candidate.get("retrieved_at") or timestamp, encode(lineage), policy_version,
                    encode(raw_payload), timestamp,
                    candidate.get("event_metric_id") or candidate.get("metric_id"), candidate.get("unit"),
                    candidate.get("frequency"), candidate.get("seasonal_adjustment"),
                    candidate.get("reference_period") or candidate.get("period"), candidate.get("transformation"),
                    int(semantic_compatible), encode(semantic_warnings), row["id"],
                ),
            )
            fact_key = f"{canonical_event_key}:official_actual"
            fact_payload = {
                "fact_key": fact_key,
                "fact_type": "official_event_actual",
                "country": row["country"],
                "category": row["category"],
                "event_name": row["name"],
                "period": row["period"],
                "unit": candidate.get("unit"),
                "forecast": row["forecast"],
                "previous": row["previous"],
                "consensus": row["consensus"],
                "actual": str(actual),
                "source": source,
                "source_url": source_url,
                "provider_type": "API",
                "reliability": candidate.get("reliability") or 0,
                "confidence": candidate.get("confidence") or 0,
                "retrieved_at": candidate.get("retrieved_at") or timestamp,
                "release_at": row["release_at"],
                "status": "active",
                "raw_payload_json": encode({
                    **candidate, "surprise_value": surprise_value,
                    "surprise_direction": surprise_direction,
                    "semantic_compatible": semantic_compatible,
                    "semantic_warnings": semantic_warnings,
                }),
                "field_lineage_json": encode(lineage),
                "policy_version": policy_version,
                "source_tier": candidate.get("source_tier"),
                "source_classification": candidate.get("source_classification"),
                "canonical_url": candidate.get("canonical_url"),
                "canonical_event_key": canonical_event_key,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            columns = [column for column in FACT_COLUMNS if column in fact_payload]
            conn.execute(
                f"INSERT INTO market_facts ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)}) "
                f"ON CONFLICT(fact_key) DO UPDATE SET "
                + ", ".join(f"{column}=excluded.{column}" for column in columns if column not in {"fact_key", "created_at"}),
                [fact_payload[column] for column in columns],
            )
            conn.commit()
        restored = self.get_fact(fact_key)
        if restored is None or str(restored.get("actual")) != str(actual):
            raise RuntimeError("official actual read-back failed")
        return restored

    def mark_event_actual_unavailable(self, canonical_event_key: str) -> None:
        if not canonical_event_key:
            return
        timestamp = now_iso()
        with connect_market_db(self.settings) as conn:
            conn.execute(
                """
                UPDATE economic_events_history
                SET temporal_status='ACTUAL_UNAVAILABLE',status='ACTUAL_UNAVAILABLE',updated_at=?
                WHERE canonical_event_key=? AND actual IS NULL
                """,
                (timestamp, canonical_event_key),
            )
            conn.commit()

    def apply_speech_outcome(self, canonical_event_key: str, candidate: dict[str, Any]) -> None:
        timestamp = now_iso()
        with connect_market_db(self.settings) as conn:
            cursor = conn.execute(
                """
                UPDATE economic_events_history
                SET outcome_json=?,temporal_status='COMPLETED',status='COMPLETED',updated_at=?
                WHERE canonical_event_key=? AND event_kind='scheduled_speech'
                """,
                (encode(candidate), timestamp, canonical_event_key),
            )
            conn.commit()
        if int(cursor.rowcount or 0) == 0:
            raise ValueError("canonical speech event not found")

    def economic_event_payloads(self, *, country: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                """
                SELECT raw_payload_json
                FROM economic_events_history
                WHERE country = ? AND date >= ? AND date <= ?
                ORDER BY time_utc ASC
                """,
                (country.upper(), start_date, end_date),
            ).fetchall()
        return [payload for row in rows if isinstance((payload := decode(row["raw_payload_json"], None)), dict)]

    def economic_event_records(self, *, country: str = "US") -> list[dict[str, Any]]:
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                "SELECT * FROM economic_events_history WHERE country=? ORDER BY COALESCE(release_at,time_utc,date),name",
                (country.upper(),),
            ).fetchall()
        return [_event_record_payload(row) for row in rows]

    def _event_history_row(self, canonical_event_key: str) -> Any | None:
        with connect_market_db(self.settings) as conn:
            return conn.execute(
                "SELECT * FROM economic_events_history WHERE canonical_event_key=? ORDER BY updated_at DESC LIMIT 1",
                (canonical_event_key,),
            ).fetchone()

    def db_summary(self) -> dict[str, Any]:
        with connect_market_db(self.settings) as conn:
            facts_total = conn.execute("SELECT COUNT(*) c FROM market_facts").fetchone()["c"]
            facts_persisted_active = conn.execute("SELECT COUNT(*) c FROM market_facts WHERE status = 'active'").fetchone()["c"]
            facts_active = conn.execute(
                """
                SELECT COUNT(*) c FROM market_facts
                WHERE status = 'active' AND (valid_until IS NULL OR valid_until > ?)
                """,
                (now_iso(),),
            ).fetchone()["c"]
            facts_stale = conn.execute(
                "SELECT COUNT(*) c FROM market_facts WHERE valid_until IS NOT NULL AND valid_until <= ?",
                (now_iso(),),
            ).fetchone()["c"]
            events_total = conn.execute("SELECT COUNT(*) c FROM economic_events_history").fetchone()["c"]
            news_total = conn.execute("SELECT COUNT(*) c FROM market_news").fetchone()["c"]
            observations_total = conn.execute("SELECT COUNT(*) c FROM provider_observations").fetchone()["c"]
            runs_total = conn.execute("SELECT COUNT(*) c FROM enrichment_runs").fetchone()["c"]
            by_type = conn.execute("SELECT fact_type, COUNT(*) c FROM market_facts GROUP BY fact_type").fetchall()
            expirations = conn.execute(
                """
                SELECT fact_key, fact_type, valid_until, next_refresh_at
                FROM market_facts
                WHERE valid_until IS NOT NULL OR next_refresh_at IS NOT NULL
                ORDER BY COALESCE(valid_until, next_refresh_at) ASC
                LIMIT 10
                """
            ).fetchall()
        facts_by_type = {row["fact_type"] or "unknown": row["c"] for row in by_type}
        legacy_count = int(facts_by_type.pop(LEGACY_EVENT_ENRICHMENT_TYPE, 0))
        if legacy_count:
            facts_by_type[CANONICAL_EVENT_ENRICHMENT_TYPE] = int(facts_by_type.get(CANONICAL_EVENT_ENRICHMENT_TYPE, 0)) + legacy_count
        return {
            "market_facts": {
                "total": facts_total,
                "active": facts_active,
                "usable_active": facts_active,
                "persisted_active": facts_persisted_active,
                "stale": facts_stale,
            },
            "economic_events_history": {"total": events_total},
            "market_news": {"total": news_total},
            "provider_observations": {"total": observations_total},
            "enrichment_runs": {"total": runs_total},
            "facts_by_type": facts_by_type,
            "next_expirations": [dict(row) for row in expirations],
            "service_role": "data provider only",
        }

    def reset_data_tables(self) -> None:
        with connect_market_db(self.settings) as conn:
            for table in (
                "market_facts",
                "economic_events_history",
                "market_news",
                "provider_observations",
                "enrichment_runs",
            ):
                conn.execute(f"DELETE FROM {table}")
            conn.commit()

    def _row(self, row: Any | None) -> dict[str, Any] | None:
        if row is None:
            return None
        data = dict(row)
        data["warnings"] = decode(data.pop("warnings_json", None), [])
        data["errors"] = decode(data.pop("errors_json", None), [])
        data["raw_payload"] = decode(data.pop("raw_payload_json", None), None)
        return data


def _canonical_event_fact_key(fact_key: str) -> str:
    return fact_key.replace(f":{LEGACY_EVENT_ENRICHMENT_TYPE}", f":{CANONICAL_EVENT_ENRICHMENT_TYPE}")


def _canonical_macro_fact_key(fact: dict[str, Any]) -> str:
    country = str(fact.get("country") or "US").upper()
    category = str(fact.get("category") or "unknown").upper()
    return f"{country}:{category}:latest:official_macro_latest"


def _official_macro_rank(fact: dict[str, Any]) -> tuple[int, int, str, float]:
    source = str(fact.get("source") or "").lower()
    source_rank = 1 if " via fred" in source else 3 if any(token in source for token in ("bls", "bea")) else 2
    valid_until = _parse_datetime(fact.get("valid_until"))
    usable = valid_until is None or valid_until > datetime.now(UTC)
    return (
        int(usable),
        source_rank,
        str(fact.get("release_at") or fact.get("retrieved_at") or ""),
        float(fact.get("reliability") or 0),
    )


def _event_fact_rank(fact: dict[str, Any]) -> tuple[int, int, str, int]:
    valid_until = _parse_datetime(fact.get("valid_until"))
    usable = valid_until is None or valid_until > datetime.now(UTC)
    active = str(fact.get("status") or "").lower() in {"active", "no_data_available"}
    return (
        int(usable),
        int(active),
        str(fact.get("updated_at") or fact.get("retrieved_at") or ""),
        int(fact.get("fact_type") == CANONICAL_EVENT_ENRICHMENT_TYPE),
    )


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
