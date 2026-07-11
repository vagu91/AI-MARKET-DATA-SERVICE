from __future__ import annotations

import json
from datetime import UTC, datetime
from datetime import timedelta
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
    "raw_payload_json", "notes", "warnings_json", "errors_json", "created_at", "updated_at",
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
        for key in ("warnings_json", "errors_json", "raw_payload_json"):
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
        timestamp = now_iso()
        payload = normalize_payload_text(event.model_dump(mode="json") if hasattr(event, "model_dump") else dict(event))
        time_utc = payload.get("time_utc")
        forecast = (payload.get("enrichment") or {}).get("forecast") if isinstance(payload.get("enrichment"), dict) else None
        previous = (payload.get("enrichment") or {}).get("previous") if isinstance(payload.get("enrichment"), dict) else None
        consensus = (payload.get("enrichment") or {}).get("consensus") if isinstance(payload.get("enrichment"), dict) else None
        actual = payload.get("actual") or ((payload.get("enrichment") or {}).get("actual") if isinstance(payload.get("enrichment"), dict) else None)
        with connect_market_db(self.settings) as conn:
            conn.execute(
                """
                INSERT INTO economic_events_history (
                    event_id, event_key, country, category, name, period, date, time_utc, time_local,
                    impact, event_risk_level, source, source_url, official_reliability,
                    forecast, previous, consensus, actual, release_at, valid_until, status,
                    raw_payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    forecast=excluded.forecast,
                    previous=excluded.previous,
                    consensus=excluded.consensus,
                    actual=excluded.actual,
                    release_at=excluded.release_at,
                    valid_until=excluded.valid_until,
                    status=excluded.status,
                    raw_payload_json=excluded.raw_payload_json,
                    updated_at=excluded.updated_at
                """,
                (
                    payload.get("event_id"),
                    event_key,
                    payload.get("country"),
                    payload.get("category"),
                    payload.get("name"),
                    payload.get("period"),
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
                    "active",
                    encode(payload),
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()

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
