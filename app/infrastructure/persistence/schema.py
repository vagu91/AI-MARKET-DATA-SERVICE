from __future__ import annotations


CANONICAL_SCHEMA = """
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


PROVIDER_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS provider_cache_entries (
  cache_key TEXT PRIMARY KEY,
  provider_name TEXT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  valid_until TEXT NULL,
  stale_until TEXT NULL,
  status TEXT NOT NULL DEFAULT 'valid_cache',
  checksum TEXT NULL,
  last_error TEXT NULL,
  source_url TEXT NULL,
  metadata_json TEXT NULL
);

CREATE TABLE IF NOT EXISTS provider_state (
  state_key TEXT PRIMARY KEY,
  provider_name TEXT NOT NULL,
  state_type TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT NULL,
  retryable INTEGER DEFAULT 0,
  next_retry_at TEXT NULL,
  payload_json TEXT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""


FED_EXPECTATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS fed_expectation_snapshots (
  id INTEGER PRIMARY KEY,
  snapshot_key TEXT UNIQUE NOT NULL,
  data_as_of TEXT NULL,
  retrieved_at TEXT NOT NULL,
  valid_until TEXT NULL,
  source TEXT NULL,
  source_type TEXT NULL,
  quality_score REAL DEFAULT 0,
  payload_json TEXT NOT NULL,
  checksum TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fed_expectations_retrieved_at
  ON fed_expectation_snapshots(retrieved_at DESC);
"""


RISK_CONTEXT_SCHEMA = """
CREATE TABLE IF NOT EXISTS risk_context_snapshots (
  id INTEGER PRIMARY KEY,
  snapshot_key TEXT UNIQUE NOT NULL,
  data_as_of TEXT NULL,
  retrieved_at TEXT NOT NULL,
  valid_until TEXT NULL,
  status TEXT NOT NULL,
  quality_score REAL DEFAULT 0,
  payload_json TEXT NOT NULL,
  checksum TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_risk_context_retrieved_at
  ON risk_context_snapshots(retrieved_at DESC);
"""


MIGRATIONS: tuple[tuple[str, str], ...] = (
    ("001_initial_canonical_store", CANONICAL_SCHEMA),
    ("002_provider_cache_and_state", PROVIDER_CACHE_SCHEMA),
    ("003_fed_expectation_history", FED_EXPECTATIONS_SCHEMA),
    ("004_risk_context_history", RISK_CONTEXT_SCHEMA),
)
