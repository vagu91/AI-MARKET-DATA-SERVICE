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


CANONICAL_MACRO_FACTS_SCHEMA = """
DELETE FROM market_facts
WHERE id IN (
  SELECT id
  FROM (
    SELECT
      id,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(country, ''), UPPER(COALESCE(category, ''))
        ORDER BY
          CASE WHEN LOWER(COALESCE(source, '')) LIKE '% via fred%' THEN 0 ELSE 1 END DESC,
          COALESCE(retrieved_at, updated_at, created_at, '') DESC,
          id DESC
      ) AS duplicate_rank
    FROM market_facts
    WHERE fact_type = 'official_macro_latest'
  ) ranked
  WHERE duplicate_rank > 1
);

UPDATE market_facts
SET fact_key = UPPER(COALESCE(country, 'US')) || ':' || UPPER(category) || ':latest:official_macro_latest'
WHERE fact_type = 'official_macro_latest'
  AND category IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_facts_official_macro_series
  ON market_facts(country, category)
  WHERE fact_type = 'official_macro_latest';
"""


MIXED_EVENT_LINEAGE_SCHEMA = """
UPDATE market_facts
SET provider_type = 'MIXED'
WHERE fact_type = 'macro_event_enrichment'
  AND provider_type = 'API'
  AND raw_payload_json LIKE '%AI_RESEARCHER_CODEX_CLI%'
  AND raw_payload_json LIKE '%consensus_verified%';
"""


PERSISTENT_AI_JOBS_SCHEMA = """
CREATE TABLE IF NOT EXISTS ai_research_jobs (
  job_id TEXT PRIMARY KEY,
  idempotency_key TEXT UNIQUE NOT NULL,
  job_type TEXT NOT NULL,
  symbol TEXT NOT NULL,
  event_key TEXT NULL,
  correlation_id TEXT NOT NULL,
  status TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 100,
  request_payload_json TEXT NOT NULL,
  result_payload_json TEXT NULL,
  policy_version TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  created_at TEXT NOT NULL,
  started_at TEXT NULL,
  heartbeat_at TEXT NULL,
  lease_expires_at TEXT NULL,
  completed_at TEXT NULL,
  next_retry_at TEXT NULL,
  last_error TEXT NULL,
  workspace_path TEXT NULL,
  output_checksum TEXT NULL,
  worker_id TEXT NULL,
  accepted_fields_json TEXT NULL,
  rejected_fields_json TEXT NULL,
  pending_fields_json TEXT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ai_research_jobs_dispatch
  ON ai_research_jobs(status, next_retry_at, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_ai_research_jobs_event
  ON ai_research_jobs(event_key, job_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_research_jobs_correlation
  ON ai_research_jobs(correlation_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ai_research_job_attempts (
  id INTEGER PRIMARY KEY,
  job_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  worker_id TEXT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  completed_at TEXT NULL,
  error TEXT NULL,
  output_checksum TEXT NULL,
  FOREIGN KEY(job_id) REFERENCES ai_research_jobs(job_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_research_job_attempt_number
  ON ai_research_job_attempts(job_id, attempt_number);

CREATE TABLE IF NOT EXISTS market_context_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  revision INTEGER NOT NULL,
  generated_at TEXT NOT NULL,
  data_as_of TEXT NULL,
  refresh_mode TEXT NOT NULL,
  debug_payload_json TEXT NOT NULL,
  consumer_payload_json TEXT NOT NULL,
  ai_status TEXT NOT NULL,
  source_job_id TEXT NULL,
  checksum TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(symbol, revision)
);

CREATE INDEX IF NOT EXISTS idx_market_context_snapshots_latest
  ON market_context_snapshots(symbol, revision DESC);

CREATE TABLE IF NOT EXISTS event_value_candidates (
  id INTEGER PRIMARY KEY,
  canonical_event_key TEXT NOT NULL,
  field_name TEXT NOT NULL,
  value TEXT NULL,
  metric_id TEXT NULL,
  period TEXT NULL,
  frequency TEXT NULL,
  unit TEXT NULL,
  source TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  source_tier INTEGER NOT NULL,
  source_classification TEXT NOT NULL,
  evidence_text TEXT NULL,
  reliability REAL NOT NULL DEFAULT 0,
  confidence REAL NOT NULL DEFAULT 0,
  validation_status TEXT NOT NULL,
  warnings_json TEXT NULL,
  policy_version TEXT NOT NULL,
  retrieved_at TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  UNIQUE(canonical_event_key, field_name, source_url, value, period)
);

ALTER TABLE economic_events_history ADD COLUMN canonical_event_key TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN event_kind TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN temporal_status TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN outcome_json TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN field_lineage_json TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN actual_retrieved_at TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN policy_version TEXT NULL;

ALTER TABLE market_facts ADD COLUMN field_lineage_json TEXT NULL;
ALTER TABLE market_facts ADD COLUMN policy_version TEXT NULL;
ALTER TABLE market_facts ADD COLUMN source_tier INTEGER NULL;
ALTER TABLE market_facts ADD COLUMN source_classification TEXT NULL;
ALTER TABLE market_facts ADD COLUMN canonical_url TEXT NULL;
ALTER TABLE market_facts ADD COLUMN canonical_event_key TEXT NULL;

ALTER TABLE market_news ADD COLUMN canonical_url TEXT NULL;
ALTER TABLE market_news ADD COLUMN aggregator_url TEXT NULL;
ALTER TABLE market_news ADD COLUMN original_publisher TEXT NULL;
ALTER TABLE market_news ADD COLUMN source_tier INTEGER NULL;
ALTER TABLE market_news ADD COLUMN source_classification TEXT NULL;
ALTER TABLE market_news ADD COLUMN lifecycle_status TEXT NULL;
"""


AI_JOB_SCOPING_SCHEMA = """
ALTER TABLE ai_research_jobs ADD COLUMN snapshot_id TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN generation TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN run_window TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN scope_key TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_ai_research_jobs_scope
  ON ai_research_jobs(scope_key, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_research_jobs_snapshot
  ON ai_research_jobs(snapshot_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS market_context_snapshot_jobs (
  snapshot_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  event_key TEXT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(snapshot_id, job_id),
  FOREIGN KEY(snapshot_id) REFERENCES market_context_snapshots(snapshot_id),
  FOREIGN KEY(job_id) REFERENCES ai_research_jobs(job_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_jobs_job
  ON market_context_snapshot_jobs(job_id, snapshot_id);
"""


AGENTIC_RESEARCH_RUNTIME_SCHEMA = """
ALTER TABLE ai_research_jobs ADD COLUMN profile_id TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN input_fingerprint TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN capability_status TEXT NULL;

ALTER TABLE event_value_candidates ADD COLUMN event_metric_id TEXT NULL;
ALTER TABLE event_value_candidates ADD COLUMN source_series_id TEXT NULL;
ALTER TABLE event_value_candidates ADD COLUMN transformation TEXT NULL;
ALTER TABLE event_value_candidates ADD COLUMN seasonal_adjustment TEXT NULL;
ALTER TABLE event_value_candidates ADD COLUMN reference_period TEXT NULL;
ALTER TABLE event_value_candidates ADD COLUMN release_timestamp TEXT NULL;
ALTER TABLE event_value_candidates ADD COLUMN release_vintage TEXT NULL;
ALTER TABLE event_value_candidates ADD COLUMN calculation_lineage_json TEXT NULL;

ALTER TABLE economic_events_history ADD COLUMN actual_metric_id TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN actual_unit TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN actual_frequency TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN actual_seasonal_adjustment TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN actual_reference_period TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN actual_transformation TEXT NULL;
ALTER TABLE economic_events_history ADD COLUMN actual_semantic_compatible INTEGER NULL;
ALTER TABLE economic_events_history ADD COLUMN semantic_warnings_json TEXT NULL;

CREATE TABLE IF NOT EXISTS ai_research_capability_reports (
  report_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  backend TEXT NULL,
  executable_path TEXT NULL,
  executable_version TEXT NULL,
  report_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ai_capability_latest
  ON ai_research_capability_reports(created_at DESC);

CREATE TABLE IF NOT EXISTS research_runs (
  run_id TEXT PRIMARY KEY,
  job_id TEXT UNIQUE NOT NULL,
  symbol TEXT NOT NULL,
  event_key TEXT NULL,
  profile_id TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  status TEXT NOT NULL,
  input_fingerprint TEXT NOT NULL,
  request_json TEXT NOT NULL,
  result_json TEXT NULL,
  coverage_score REAL NOT NULL DEFAULT 0,
  required_topics_json TEXT NOT NULL DEFAULT '[]',
  completed_topics_json TEXT NOT NULL DEFAULT '[]',
  missing_topics_json TEXT NOT NULL DEFAULT '[]',
  blocking_gaps_json TEXT NOT NULL DEFAULT '[]',
  non_blocking_gaps_json TEXT NOT NULL DEFAULT '[]',
  source_domains_json TEXT NOT NULL DEFAULT '[]',
  warnings_json TEXT NOT NULL DEFAULT '[]',
  started_at TEXT NULL,
  completed_at TEXT NULL,
  data_as_of TEXT NULL,
  fresh_until TEXT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(job_id) REFERENCES ai_research_jobs(job_id)
);

CREATE INDEX IF NOT EXISTS idx_research_runs_latest
  ON research_runs(symbol, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_research_runs_fingerprint
  ON research_runs(input_fingerprint, status, created_at DESC);

CREATE TABLE IF NOT EXISTS research_run_steps (
  step_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  input_checksum TEXT NULL,
  output_checksum TEXT NULL,
  input_json TEXT NULL,
  output_json TEXT NULL,
  backend TEXT NULL,
  tool TEXT NULL,
  source_domains_json TEXT NOT NULL DEFAULT '[]',
  started_at TEXT NULL,
  completed_at TEXT NULL,
  duration_ms INTEGER NULL,
  error TEXT NULL,
  UNIQUE(run_id, step_name),
  FOREIGN KEY(run_id) REFERENCES research_runs(run_id)
);

CREATE TABLE IF NOT EXISTS research_claims (
  claim_id TEXT PRIMARY KEY,
  research_run_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  field_semantics TEXT NOT NULL,
  value_json TEXT NULL,
  metric_id TEXT NULL,
  period TEXT NULL,
  frequency TEXT NULL,
  unit TEXT NULL,
  event_key TEXT NULL,
  symbol TEXT NULL,
  valid_from TEXT NULL,
  valid_until TEXT NULL,
  published_at TEXT NULL,
  retrieved_at TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  validation_status TEXT NOT NULL,
  warnings_json TEXT NOT NULL DEFAULT '[]',
  policy_version TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  checksum TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(research_run_id) REFERENCES research_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_research_claims_run
  ON research_claims(research_run_id, validation_status, topic);

CREATE TABLE IF NOT EXISTS research_evidence (
  evidence_id TEXT PRIMARY KEY,
  claim_id TEXT NOT NULL,
  query_text TEXT NULL,
  source_url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  publisher TEXT NULL,
  source_domain TEXT NOT NULL,
  source_tier INTEGER NOT NULL,
  evidence_text TEXT NOT NULL,
  published_at TEXT NULL,
  retrieved_at TEXT NOT NULL,
  redirect_url TEXT NULL,
  source_status TEXT NULL,
  independent_source_group TEXT NOT NULL,
  content_checksum TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(claim_id, canonical_url, content_checksum),
  FOREIGN KEY(claim_id) REFERENCES research_claims(claim_id)
);

CREATE INDEX IF NOT EXISTS idx_research_evidence_claim
  ON research_evidence(claim_id, source_domain);

CREATE TABLE IF NOT EXISTS research_scheduler_decisions (
  decision_id TEXT PRIMARY KEY,
  trigger_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  input_fingerprint TEXT NOT NULL,
  decision TEXT NOT NULL,
  reason TEXT NOT NULL,
  job_id TEXT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_research_scheduler_fingerprint
  ON research_scheduler_decisions(trigger_name, symbol, input_fingerprint, created_at DESC);
"""


VERIFIED_RESEARCH_RUNTIME_SCHEMA = """
ALTER TABLE ai_research_jobs ADD COLUMN retry_class TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN retry_deadline_at TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN last_retry_reason TEXT NULL;

ALTER TABLE research_runs ADD COLUMN valid_not_applicable_topics_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE research_runs ADD COLUMN search_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN opened_source_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN usage_json TEXT NULL;
ALTER TABLE research_runs ADD COLUMN cost_json TEXT NULL;

ALTER TABLE research_run_steps ADD COLUMN telemetry_json TEXT NULL;
ALTER TABLE research_evidence ADD COLUMN source_content_hash TEXT NULL;

CREATE TABLE IF NOT EXISTS research_tool_events (
  event_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  step_id TEXT NULL,
  event_type TEXT NOT NULL,
  source_url TEXT NULL,
  canonical_url TEXT NULL,
  redirect_url TEXT NULL,
  observed_at TEXT NOT NULL,
  content_hash TEXT NULL,
  http_status INTEGER NULL,
  usage_json TEXT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES research_runs(run_id),
  FOREIGN KEY(step_id) REFERENCES research_run_steps(step_id)
);

CREATE INDEX IF NOT EXISTS idx_research_tool_events_run
  ON research_tool_events(run_id, event_type, observed_at);
CREATE INDEX IF NOT EXISTS idx_research_tool_events_url
  ON research_tool_events(run_id, canonical_url, source_url);

CREATE TABLE IF NOT EXISTS ai_research_live_verifications (
  verification_id TEXT PRIMARY KEY,
  backend TEXT NOT NULL,
  executable_version TEXT NULL,
  verified_at TEXT NOT NULL,
  expires_at TEXT NULL,
  evidence_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ai_live_verifications_latest
  ON ai_research_live_verifications(backend, verified_at DESC);
"""

AGENTIC_RUNTIME_DIAGNOSTICS_SCHEMA = """
ALTER TABLE ai_research_jobs ADD COLUMN last_diagnostic_json TEXT NULL;

ALTER TABLE ai_research_job_attempts ADD COLUMN error_category TEXT NULL;
ALTER TABLE ai_research_job_attempts ADD COLUMN exit_code INTEGER NULL;
ALTER TABLE ai_research_job_attempts ADD COLUMN retry_classification TEXT NULL;
ALTER TABLE ai_research_job_attempts ADD COLUMN diagnostic_json TEXT NULL;

ALTER TABLE research_run_steps ADD COLUMN diagnostic_json TEXT NULL;

CREATE TABLE IF NOT EXISTS research_step_attempts (
  step_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  run_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  completed_at TEXT NULL,
  error TEXT NULL,
  diagnostic_json TEXT NULL,
  PRIMARY KEY(step_id, attempt),
  FOREIGN KEY(step_id) REFERENCES research_run_steps(step_id),
  FOREIGN KEY(run_id) REFERENCES research_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_research_step_attempts_run
  ON research_step_attempts(run_id, step_name, attempt);
"""

OBSERVABLE_TOOL_TELEMETRY_SCHEMA = """
ALTER TABLE research_tool_events ADD COLUMN raw_event_type TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN lifecycle TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN item_id TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN item_type TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN phase TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN job_id TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN provider_tool_type TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN semantic_action TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN tool_action_fingerprint TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN status TEXT NULL;
ALTER TABLE research_tool_events ADD COLUMN counts_usage INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_evidence ADD COLUMN tool_event_id TEXT NULL;

ALTER TABLE research_runs ADD COLUMN metrics_json TEXT NULL;
ALTER TABLE research_runs ADD COLUMN checkpoint_json TEXT NULL;
ALTER TABLE research_runs ADD COLUMN continuation_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN threshold_warnings_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE research_runs ADD COLUMN loop_detection_count INTEGER NOT NULL DEFAULT 0;

UPDATE research_tool_events
SET raw_event_type=COALESCE(raw_event_type,'legacy.completed'),
    lifecycle=COALESCE(lifecycle,'completed'),
    phase=COALESCE(
      phase,
      (SELECT step_name FROM research_run_steps
       WHERE research_run_steps.step_id=research_tool_events.step_id),
      'UNKNOWN'
    ),
    job_id=COALESCE(
      job_id,
      (SELECT job_id FROM research_runs
       WHERE research_runs.run_id=research_tool_events.run_id)
    ),
    provider_tool_type=COALESCE(provider_tool_type,event_type),
    semantic_action=COALESCE(
      semantic_action,
      CASE
        WHEN event_type='search' THEN 'search'
        WHEN event_type IN ('open_source','server_source_verified')
          THEN 'open_source'
        ELSE 'non_operational'
      END
    ),
    tool_action_fingerprint=COALESCE(tool_action_fingerprint,event_id),
    status=COALESCE(status,'completed'),
    counts_usage=CASE
      WHEN event_type IN ('search','open_source','server_source_verified')
        THEN 1 ELSE counts_usage END;

CREATE UNIQUE INDEX IF NOT EXISTS idx_research_tool_event_lifecycle
  ON research_tool_events(run_id, phase, tool_action_fingerprint, lifecycle)
  WHERE tool_action_fingerprint IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_research_tool_event_semantic_usage
  ON research_tool_events(run_id, semantic_action, lifecycle, counts_usage);
CREATE INDEX IF NOT EXISTS idx_research_evidence_tool_event
  ON research_evidence(tool_event_id);
"""

RESEARCH_SOURCE_GATEWAY_SCHEMA = """
ALTER TABLE research_evidence ADD COLUMN source_id TEXT NULL;
ALTER TABLE research_evidence ADD COLUMN verification_id TEXT NULL;
ALTER TABLE research_evidence ADD COLUMN verification_method TEXT NULL;
ALTER TABLE research_evidence ADD COLUMN verification_reason TEXT NULL;
ALTER TABLE research_evidence ADD COLUMN verification_score REAL NULL;

CREATE TABLE IF NOT EXISTS research_sources (
  source_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  requested_url TEXT NOT NULL,
  final_url TEXT NULL,
  canonical_url TEXT NULL,
  source_domain TEXT NOT NULL,
  source_tier INTEGER NULL,
  publisher TEXT NULL,
  title TEXT NULL,
  fetch_status TEXT NOT NULL,
  verification_status TEXT NOT NULL DEFAULT 'UNVERIFIED',
  rejection_reason TEXT NULL,
  http_status INTEGER NULL,
  content_type TEXT NULL,
  retrieved_at TEXT NOT NULL,
  content_sha256 TEXT NULL,
  content_bytes INTEGER NOT NULL DEFAULT 0,
  content_text TEXT NULL,
  redirect_chain_json TEXT NOT NULL DEFAULT '[]',
  duplicate_of_source_id TEXT NULL,
  acquisition_backend TEXT NOT NULL,
  fetch_duration_ms INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(run_id, requested_url),
  FOREIGN KEY(run_id) REFERENCES research_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_research_sources_run_status
  ON research_sources(run_id, fetch_status, verification_status);
CREATE INDEX IF NOT EXISTS idx_research_sources_run_url
  ON research_sources(run_id, canonical_url, final_url, requested_url);
CREATE INDEX IF NOT EXISTS idx_research_sources_content
  ON research_sources(run_id, content_sha256);

CREATE TABLE IF NOT EXISTS research_evidence_verifications (
  verification_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  claim_ref TEXT NOT NULL,
  source_id TEXT NULL,
  evidence_url TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT NOT NULL,
  match_method TEXT NULL,
  match_score REAL NOT NULL DEFAULT 0,
  evidence_anchor TEXT NOT NULL,
  evidence_token_count INTEGER NOT NULL DEFAULT 0,
  matched_token_count INTEGER NOT NULL DEFAULT 0,
  verification_duration_ms INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  UNIQUE(run_id, claim_ref, evidence_url, evidence_anchor),
  FOREIGN KEY(run_id) REFERENCES research_runs(run_id),
  FOREIGN KEY(source_id) REFERENCES research_sources(source_id)
);

CREATE INDEX IF NOT EXISTS idx_research_evidence_verifications_run
  ON research_evidence_verifications(run_id, status, reason);

CREATE TABLE IF NOT EXISTS research_backend_invocations (
  invocation_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  backend TEXT NOT NULL,
  purpose TEXT NOT NULL,
  model TEXT NULL,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cached_tokens INTEGER NOT NULL DEFAULT 0,
  reasoning_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  cost_json TEXT NULL,
  duration_ms INTEGER NOT NULL DEFAULT 0,
  output_checksum TEXT NOT NULL,
  output_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES research_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_research_backend_invocations_run
  ON research_backend_invocations(run_id, created_at);
"""

RESEARCH_SEMANTIC_LIFECYCLE_SCHEMA = """
ALTER TABLE research_claims ADD COLUMN event_at TEXT NULL;
ALTER TABLE research_claims ADD COLUMN release_at TEXT NULL;
ALTER TABLE research_claims ADD COLUMN issuer TEXT NULL;
ALTER TABLE research_claims ADD COLUMN next_refresh_at TEXT NULL;
ALTER TABLE research_claims ADD COLUMN lifecycle_status TEXT NULL;
ALTER TABLE research_claims ADD COLUMN post_event_semantics TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_research_claims_lifecycle
  ON research_claims(validation_status, lifecycle_status, next_refresh_at);
"""

ATOMIC_RESEARCH_PERSISTENCE_SCHEMA = """
ALTER TABLE research_claims
  ADD COLUMN materialization_status TEXT NOT NULL DEFAULT 'ELIGIBLE';
ALTER TABLE research_evidence
  ADD COLUMN audit_status TEXT NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE market_context_snapshots
  ADD COLUMN audit_status TEXT NOT NULL DEFAULT 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_research_claims_materialization
  ON research_claims(research_run_id, materialization_status, validation_status);
CREATE INDEX IF NOT EXISTS idx_research_evidence_audit
  ON research_evidence(claim_id, audit_status);
CREATE INDEX IF NOT EXISTS idx_market_context_snapshots_audit
  ON market_context_snapshots(symbol, audit_status, revision DESC);
"""

GAP_AWARE_PARALLEL_RESEARCH_SCHEMA = """
ALTER TABLE ai_research_jobs ADD COLUMN parent_job_id TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN parent_run_id TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN specialized_topic TEXT NULL;
ALTER TABLE ai_research_jobs ADD COLUMN child_ordinal INTEGER NULL;

ALTER TABLE research_runs ADD COLUMN parent_run_id TEXT NULL;
ALTER TABLE research_runs ADD COLUMN is_parent INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN planned_query_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN deduplicated_search_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN discovered_url_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN acquisition_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN fetched_source_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_runs ADD COLUMN verified_source_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE market_context_snapshots ADD COLUMN research_run_id TEXT NULL;
ALTER TABLE market_context_snapshots ADD COLUMN parent_run_id TEXT NULL;
ALTER TABLE market_context_snapshots
  ADD COLUMN research_link_status TEXT NOT NULL DEFAULT 'NOT_REQUIRED';

ALTER TABLE research_sources ADD COLUMN stage_status TEXT NULL;
ALTER TABLE research_sources ADD COLUMN stage_error TEXT NULL;
ALTER TABLE research_sources ADD COLUMN http_fetched_at TEXT NULL;
ALTER TABLE research_sources ADD COLUMN content_extracted_at TEXT NULL;

ALTER TABLE research_claims ADD COLUMN event_type TEXT NULL;
ALTER TABLE research_claims ADD COLUMN event_start_at TEXT NULL;
ALTER TABLE research_claims ADD COLUMN event_end_at TEXT NULL;
ALTER TABLE research_claims ADD COLUMN decision_at TEXT NULL;
ALTER TABLE research_claims ADD COLUMN confirmation_status TEXT NULL;

ALTER TABLE economic_events_history
  ADD COLUMN temporal_audit_status TEXT NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE economic_events_history ADD COLUMN temporal_invalid_reason TEXT NULL;
ALTER TABLE market_facts
  ADD COLUMN temporal_audit_status TEXT NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE market_facts ADD COLUMN temporal_invalid_reason TEXT NULL;

CREATE TABLE IF NOT EXISTS research_parent_runs (
  parent_run_id TEXT PRIMARY KEY,
  parent_job_id TEXT NULL,
  symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  snapshot_id TEXT NULL,
  manifest_id TEXT NOT NULL,
  requested_backend TEXT NOT NULL,
  concurrency_limit INTEGER NOT NULL,
  expected_child_count INTEGER NOT NULL DEFAULT 0,
  terminal_child_count INTEGER NOT NULL DEFAULT 0,
  checkpoint_json TEXT NOT NULL DEFAULT '{}',
  telemetry_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  started_at TEXT NULL,
  completed_at TEXT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS research_gap_manifests (
  manifest_id TEXT PRIMARY KEY,
  parent_run_id TEXT NULL,
  symbol TEXT NOT NULL,
  source_snapshot_id TEXT NULL,
  generated_at TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  checksum TEXT NOT NULL,
  manifest_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS research_gap_items (
  manifest_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  applicability TEXT NOT NULL,
  deterministic_status TEXT NOT NULL,
  freshness TEXT NOT NULL,
  data_as_of TEXT NULL,
  valid_until TEXT NULL,
  completeness REAL NOT NULL,
  missing_fields_json TEXT NOT NULL DEFAULT '[]',
  source_lineage_json TEXT NOT NULL DEFAULT '[]',
  required_action TEXT NOT NULL,
  reason TEXT NOT NULL,
  PRIMARY KEY(manifest_id,topic),
  FOREIGN KEY(manifest_id) REFERENCES research_gap_manifests(manifest_id)
);

CREATE TABLE IF NOT EXISTS research_parent_children (
  parent_run_id TEXT NOT NULL,
  child_job_id TEXT NOT NULL,
  child_run_id TEXT NULL,
  topic TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  status TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  result_checksum TEXT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(parent_run_id,child_job_id),
  FOREIGN KEY(parent_run_id) REFERENCES research_parent_runs(parent_run_id),
  FOREIGN KEY(child_job_id) REFERENCES ai_research_jobs(job_id)
);

CREATE TABLE IF NOT EXISTS temporal_quarantine (
  quarantine_id TEXT PRIMARY KEY,
  entity_table TEXT NOT NULL,
  entity_key TEXT NOT NULL,
  domain TEXT NOT NULL,
  timestamp_field TEXT NOT NULL,
  timestamp_value TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  detected_at TEXT NOT NULL,
  details_json TEXT NOT NULL DEFAULT '{}',
  UNIQUE(entity_table,entity_key,timestamp_field,reason_code)
);

CREATE TABLE IF NOT EXISTS news_research_candidate_decisions (
  candidate_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  canonical_url TEXT NULL,
  source_domain TEXT NULL,
  article_status TEXT NOT NULL,
  claim_verification_status TEXT NOT NULL,
  confirmation_status TEXT NOT NULL,
  rejection_reason TEXT NULL,
  decision_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES research_runs(run_id)
);

CREATE TABLE IF NOT EXISTS market_context_components (
  symbol TEXT NOT NULL,
  component_name TEXT NOT NULL,
  source_snapshot_id TEXT NOT NULL,
  source_revision INTEGER NOT NULL,
  data_as_of TEXT NULL,
  valid_until TEXT NULL,
  component_checksum TEXT NOT NULL,
  component_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(symbol,component_name,source_snapshot_id),
  FOREIGN KEY(source_snapshot_id) REFERENCES market_context_snapshots(snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_research_parent_children_status
  ON research_parent_children(parent_run_id,status,ordinal);
CREATE INDEX IF NOT EXISTS idx_gap_items_action
  ON research_gap_items(manifest_id,required_action,topic);
CREATE INDEX IF NOT EXISTS idx_temporal_quarantine_entity
  ON temporal_quarantine(entity_table,entity_key);
CREATE INDEX IF NOT EXISTS idx_news_candidate_decisions_run
  ON news_research_candidate_decisions(run_id,article_status,rejection_reason);
CREATE INDEX IF NOT EXISTS idx_market_context_components_latest
  ON market_context_components(symbol,component_name,source_revision DESC);
CREATE INDEX IF NOT EXISTS idx_snapshot_exact_research
  ON market_context_snapshots(research_run_id,parent_run_id,revision DESC);
"""

TEMPORAL_QUARANTINE_RUNTIME_SCHEMA = """
CREATE TABLE IF NOT EXISTS temporal_reconciliation_runs (
  reconciliation_id TEXT PRIMARY KEY,
  source_schema_version INTEGER NOT NULL,
  scanned_count INTEGER NOT NULL DEFAULT 0,
  quarantined_count INTEGER NOT NULL DEFAULT 0,
  errors_json TEXT NOT NULL DEFAULT '[]',
  started_at TEXT NOT NULL,
  completed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_temporal_quarantine_domain_reason
  ON temporal_quarantine(domain,reason_code,detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_economic_events_temporal_audit
  ON economic_events_history(temporal_audit_status,temporal_status,release_at);
"""


MIGRATIONS: tuple[tuple[str, str], ...] = (
    ("001_initial_canonical_store", CANONICAL_SCHEMA),
    ("002_provider_cache_and_state", PROVIDER_CACHE_SCHEMA),
    ("003_fed_expectation_history", FED_EXPECTATIONS_SCHEMA),
    ("004_risk_context_history", RISK_CONTEXT_SCHEMA),
    ("005_canonical_macro_facts", CANONICAL_MACRO_FACTS_SCHEMA),
    ("006_mixed_event_lineage", MIXED_EVENT_LINEAGE_SCHEMA),
    ("007_persistent_ai_jobs_and_temporal_domains", PERSISTENT_AI_JOBS_SCHEMA),
    ("008_ai_job_scoping_and_snapshot_links", AI_JOB_SCOPING_SCHEMA),
    ("009_semantic_actuals_and_agentic_research_runtime", AGENTIC_RESEARCH_RUNTIME_SCHEMA),
    ("010_verified_evidence_deadlines_and_completeness", VERIFIED_RESEARCH_RUNTIME_SCHEMA),
    ("011_agentic_runtime_diagnostics_and_step_history", AGENTIC_RUNTIME_DIAGNOSTICS_SCHEMA),
    ("012_observable_tool_telemetry_and_checkpoints", OBSERVABLE_TOOL_TELEMETRY_SCHEMA),
    ("013_research_source_gateway_and_backend_invocations", RESEARCH_SOURCE_GATEWAY_SCHEMA),
    ("014_research_semantic_lifecycle", RESEARCH_SEMANTIC_LIFECYCLE_SCHEMA),
    ("015_atomic_research_persistence_and_quarantine", ATOMIC_RESEARCH_PERSISTENCE_SCHEMA),
    ("016_gap_aware_parallel_research_and_temporal_audit", GAP_AWARE_PARALLEL_RESEARCH_SCHEMA),
    ("017_temporal_quarantine_runtime_reconciliation", TEMPORAL_QUARANTINE_RUNTIME_SCHEMA),
)
