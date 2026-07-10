from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from datetime import timedelta
from typing import Any

from app.core.redaction import redact_payload
from app.core.config import Settings


SCHEMA = """
CREATE TABLE IF NOT EXISTS market_facts (
  id INTEGER PRIMARY KEY,
  fact_key TEXT UNIQUE NOT NULL,
  fact_type TEXT NOT NULL,
  country TEXT NULL,
  symbol TEXT NULL,
  category TEXT NULL,
  event_name TEXT NULL,
  period TEXT NULL,
  value TEXT NULL,
  unit TEXT NULL,
  forecast TEXT NULL,
  previous TEXT NULL,
  consensus TEXT NULL,
  actual TEXT NULL,
  source TEXT NULL,
  source_url TEXT NULL,
  provider_type TEXT NULL,
  reliability REAL DEFAULT 0,
  confidence REAL DEFAULT 0,
  retrieved_at TEXT NOT NULL,
  release_at TEXT NULL,
  valid_from TEXT NULL,
  valid_until TEXT NULL,
  next_refresh_at TEXT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  raw_payload_json TEXT NULL,
  notes TEXT NULL,
  warnings_json TEXT NULL,
  errors_json TEXT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS economic_events_history (
  id INTEGER PRIMARY KEY,
  event_id TEXT,
  event_key TEXT UNIQUE,
  country TEXT,
  category TEXT,
  name TEXT,
  period TEXT,
  date TEXT,
  time_utc TEXT,
  time_local TEXT,
  impact TEXT,
  event_risk_level TEXT,
  source TEXT,
  source_url TEXT,
  official_reliability REAL,
  forecast TEXT NULL,
  previous TEXT NULL,
  consensus TEXT NULL,
  actual TEXT NULL,
  actual_source TEXT NULL,
  actual_source_url TEXT NULL,
  forecast_source TEXT NULL,
  forecast_source_url TEXT NULL,
  surprise_value TEXT NULL,
  surprise_direction TEXT NULL,
  release_at TEXT NULL,
  valid_until TEXT NULL,
  status TEXT,
  raw_payload_json TEXT NULL,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS market_news (
  id INTEGER PRIMARY KEY,
  news_key TEXT UNIQUE,
  title TEXT NOT NULL,
  summary TEXT NULL,
  content_snippet TEXT NULL,
  source TEXT NULL,
  source_url TEXT NOT NULL,
  published_at TEXT NULL,
  retrieved_at TEXT NOT NULL,
  valid_from TEXT NULL,
  valid_until TEXT NULL,
  next_refresh_at TEXT NULL,
  symbols_json TEXT NULL,
  topics_json TEXT NULL,
  country TEXT NULL,
  category TEXT NULL,
  relevance TEXT NULL,
  reliability REAL DEFAULT 0,
  confidence REAL DEFAULT 0,
  provider_type TEXT NULL,
  is_official INTEGER DEFAULT 0,
  is_duplicate INTEGER DEFAULT 0,
  raw_payload_json TEXT NULL,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS provider_observations (
  id INTEGER PRIMARY KEY,
  run_id TEXT,
  provider_name TEXT,
  provider_type TEXT,
  status TEXT,
  country TEXT NULL,
  symbol TEXT NULL,
  category TEXT NULL,
  query TEXT NULL,
  url TEXT NULL,
  item_count INTEGER DEFAULT 0,
  error TEXT NULL,
  warning TEXT NULL,
  retrieved_at TEXT,
  duration_ms INTEGER NULL,
  raw_payload_json TEXT NULL
);

CREATE TABLE IF NOT EXISTS enrichment_runs (
  id INTEGER PRIMARY KEY,
  run_id TEXT UNIQUE,
  started_at TEXT,
  finished_at TEXT NULL,
  status TEXT,
  trigger TEXT,
  events_checked INTEGER DEFAULT 0,
  db_hits INTEGER DEFAULT 0,
  db_misses INTEGER DEFAULT 0,
  provider_hits INTEGER DEFAULT 0,
  provider_misses INTEGER DEFAULT 0,
  ai_research_requests INTEGER DEFAULT 0,
  facts_written INTEGER DEFAULT 0,
  news_written INTEGER DEFAULT 0,
  errors_json TEXT NULL,
  warnings_json TEXT NULL
);
"""

FACT_COLUMNS = [
    "fact_key", "fact_type", "country", "symbol", "category", "event_name", "period", "value", "unit",
    "forecast", "previous", "consensus", "actual", "source", "source_url", "provider_type", "reliability",
    "confidence", "retrieved_at", "release_at", "valid_from", "valid_until", "next_refresh_at", "status",
    "raw_payload_json", "notes", "warnings_json", "errors_json", "created_at", "updated_at",
]


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


def connect_market_db(settings: Settings) -> sqlite3.Connection:
    settings.market_db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.market_db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_market_db(settings: Settings) -> None:
    with connect_market_db(settings) as conn:
        conn.executescript(SCHEMA)
        conn.commit()


class MarketFactRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        init_market_db(settings)

    def upsert_fact(self, fact: dict[str, Any]) -> dict[str, Any]:
        payload = dict(fact)
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
                [payload[column] for column in columns],
            )
            conn.commit()
        return self.get_fact(payload["fact_key"]) or payload

    def get_fact(self, fact_key: str) -> dict[str, Any] | None:
        with connect_market_db(self.settings) as conn:
            row = conn.execute("SELECT * FROM market_facts WHERE fact_key = ?", (fact_key,)).fetchone()
        return self._row(row)

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
                "SELECT COUNT(*) c FROM market_facts WHERE country = ? AND status = 'active'",
                (country.upper(),),
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
        clause = "WHERE fact_type = ?" if fact_type else ""
        params = (fact_type,) if fact_type else ()
        with connect_market_db(self.settings) as conn:
            return int(conn.execute(f"SELECT COUNT(*) c FROM market_facts {clause}", params).fetchone()["c"])

    def active_count(self) -> int:
        with connect_market_db(self.settings) as conn:
            return int(conn.execute("SELECT COUNT(*) c FROM market_facts WHERE status = 'active'").fetchone()["c"])

    def get_valid_facts_by_type(self, fact_type: str, *, allow_stale: bool = False) -> list[dict[str, Any]]:
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                "SELECT * FROM market_facts WHERE fact_type = ? AND status = 'active' ORDER BY updated_at DESC",
                (fact_type,),
            ).fetchall()
        facts = [self._row(row) for row in rows if row]
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
        payload = event.model_dump(mode="json") if hasattr(event, "model_dump") else dict(event)
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

    def db_summary(self) -> dict[str, Any]:
        with connect_market_db(self.settings) as conn:
            facts_total = conn.execute("SELECT COUNT(*) c FROM market_facts").fetchone()["c"]
            facts_active = conn.execute("SELECT COUNT(*) c FROM market_facts WHERE status = 'active'").fetchone()["c"]
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
        return {
            "market_facts": {"total": facts_total, "active": facts_active, "stale": facts_stale},
            "economic_events_history": {"total": events_total},
            "market_news": {"total": news_total},
            "provider_observations": {"total": observations_total},
            "enrichment_runs": {"total": runs_total},
            "facts_by_type": {row["fact_type"] or "unknown": row["c"] for row in by_type},
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

    def _row(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        data = dict(row)
        data["warnings"] = decode(data.pop("warnings_json", None), [])
        data["errors"] = decode(data.pop("errors_json", None), [])
        data["raw_payload"] = decode(data.pop("raw_payload_json", None), None)
        return data
