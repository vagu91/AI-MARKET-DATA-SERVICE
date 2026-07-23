from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AI_MARKET_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    service_name: str = "AI-MARKET-DATA-SERVICE"
    environment: str = "local"
    log_level: str = "INFO"
    database_path: Path = Path("./data/market_data_service.sqlite")
    diagnostics_dir: Path = Field(
        default=Path("./data/diagnostics"), validation_alias="AI_MARKET_DIAGNOSTICS_DIR"
    )
    backups_dir: Path = Field(
        default=Path("./data/backups"), validation_alias="AI_MARKET_BACKUPS_DIR"
    )
    logs_dir: Path = Field(default=Path("./logs"), validation_alias="AI_MARKET_LOGS_DIR")
    temp_dir: Path = Field(default=Path("./data/temp"), validation_alias="AI_MARKET_TEMP_DIR")
    diagnostics_retention_days: int = Field(
        default=7, validation_alias="AI_MARKET_DIAGNOSTICS_RETENTION_DAYS"
    )
    diagnostics_max_total_mb: int = Field(
        default=250, validation_alias="AI_MARKET_DIAGNOSTICS_MAX_TOTAL_MB"
    )
    diagnostics_max_runs: int = Field(default=10, validation_alias="AI_MARKET_DIAGNOSTICS_MAX_RUNS")
    backups_retention_days: int = Field(
        default=14, validation_alias="AI_MARKET_BACKUPS_RETENTION_DAYS"
    )
    backups_max_total_mb: int = Field(
        default=500, validation_alias="AI_MARKET_BACKUPS_MAX_TOTAL_MB"
    )
    backups_max_files: int = Field(default=5, validation_alias="AI_MARKET_BACKUPS_MAX_FILES")
    log_max_file_mb: int = Field(default=10, validation_alias="AI_MARKET_LOG_MAX_FILE_MB")
    log_backup_count: int = Field(default=5, validation_alias="AI_MARKET_LOG_BACKUP_COUNT")
    temp_retention_hours: int = Field(default=24, validation_alias="AI_MARKET_TEMP_RETENTION_HOURS")
    disk_warning_free_mb: int = Field(
        default=1024, validation_alias="AI_MARKET_DISK_WARNING_FREE_MB"
    )
    disk_critical_free_mb: int = Field(
        default=512, validation_alias="AI_MARKET_DISK_CRITICAL_FREE_MB"
    )
    provider_observations_retention_days: int = Field(
        default=30, validation_alias="AI_MARKET_PROVIDER_OBSERVATIONS_RETENTION_DAYS"
    )
    enrichment_runs_retention_days: int = Field(
        default=30, validation_alias="AI_MARKET_ENRICHMENT_RUNS_RETENTION_DAYS"
    )
    expired_cache_retention_days: int = Field(
        default=7, validation_alias="AI_MARKET_EXPIRED_CACHE_RETENTION_DAYS"
    )
    market_news_retention_days: int = Field(
        default=30, validation_alias="AI_MARKET_MARKET_NEWS_RETENTION_DAYS"
    )
    market_facts_retention_days: int = Field(
        default=30, validation_alias="AI_MARKET_MARKET_FACTS_RETENTION_DAYS"
    )
    economic_events_history_retention_days: int = Field(
        default=730, validation_alias="AI_MARKET_ECONOMIC_EVENTS_HISTORY_RETENTION_DAYS"
    )
    snapshot_history_retention_days: int = Field(
        default=90, validation_alias="AI_MARKET_SNAPSHOT_HISTORY_RETENTION_DAYS"
    )
    db_vacuum_min_size_mb: int = Field(
        default=250, validation_alias="AI_MARKET_DB_VACUUM_MIN_SIZE_MB"
    )
    db_vacuum_min_reclaimable_mb: int = Field(
        default=50, validation_alias="AI_MARKET_DB_VACUUM_MIN_RECLAIMABLE_MB"
    )
    storage_cleanup_interval_hours: int = Field(
        default=24, validation_alias="AI_MARKET_STORAGE_CLEANUP_INTERVAL_HOURS"
    )
    timezone: str = "Europe/Rome"
    http_timeout_seconds: float = 15.0
    timeout_macro_seconds: float = Field(
        default=30.0, validation_alias="AI_MARKET_TIMEOUT_MACRO_SECONDS"
    )
    timeout_events_seconds: float = Field(
        default=30.0, validation_alias="AI_MARKET_TIMEOUT_EVENTS_SECONDS"
    )
    timeout_news_seconds: float = Field(
        default=12.0, validation_alias="AI_MARKET_TIMEOUT_NEWS_SECONDS"
    )
    timeout_official_news_seconds: float = Field(
        default=4.0, validation_alias="AI_MARKET_TIMEOUT_OFFICIAL_NEWS_SECONDS"
    )
    timeout_nasdaq_seconds: float = Field(
        default=45.0, validation_alias="AI_MARKET_TIMEOUT_NASDAQ_SECONDS"
    )
    timeout_earnings_seconds: float = Field(
        default=12.0, validation_alias="AI_MARKET_TIMEOUT_EARNINGS_SECONDS"
    )
    timeout_cot_seconds: float = Field(
        default=8.0, validation_alias="AI_MARKET_TIMEOUT_COT_SECONDS"
    )
    timeout_sentiment_seconds: float = Field(
        default=8.0, validation_alias="AI_MARKET_TIMEOUT_SENTIMENT_SECONDS"
    )
    timeout_canonical_seconds: float = Field(
        default=4.0, validation_alias="AI_MARKET_TIMEOUT_CANONICAL_SECONDS"
    )
    timeout_article_fetch_seconds: float = Field(
        default=4.0, validation_alias="AI_MARKET_TIMEOUT_ARTICLE_FETCH_SECONDS"
    )
    timeout_ai_research_seconds: float = Field(
        default=180.0, validation_alias="AI_MARKET_TIMEOUT_AI_RESEARCH_SECONDS"
    )

    fred_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("AI_MARKET_FRED_API_KEY", "FRED_API_KEY"),
    )
    bea_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("AI_MARKET_BEA_API_KEY", "BEA_API_KEY"),
    )
    bls_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("AI_MARKET_BLS_API_KEY", "BLS_API_KEY"),
    )
    alpha_vantage_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "AI_MARKET_ALPHA_VANTAGE_API_KEY",
            "ALPHA_VANTAGE_API_KEY",
        ),
    )
    openai_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("AI_MARKET_OPENAI_API_KEY", "OPENAI_API_KEY"),
    )

    enable_scraper_fallbacks: bool = False
    enable_event_enrichment_scrapers: bool = True
    enable_openai_event_enrichment: bool = False
    enable_openai_fallback: bool = False
    enable_scheduler: bool = False
    enable_browser_scraping: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "AI_MARKET_ENABLE_BROWSER_SCRAPING", "ENABLE_BROWSER_SCRAPING"
        ),
    )
    browser_scraping_headless: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "AI_MARKET_BROWSER_SCRAPING_HEADLESS", "BROWSER_SCRAPING_HEADLESS"
        ),
    )
    browser_scraping_timeout_seconds: float = Field(
        default=15.0,
        validation_alias=AliasChoices(
            "AI_MARKET_BROWSER_SCRAPING_TIMEOUT_SECONDS",
            "BROWSER_SCRAPING_TIMEOUT_SECONDS",
        ),
    )
    browser_scraping_max_pages: int = Field(
        default=3,
        validation_alias=AliasChoices(
            "AI_MARKET_BROWSER_SCRAPING_MAX_PAGES", "BROWSER_SCRAPING_MAX_PAGES"
        ),
    )
    enable_aggressive_scraping: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "AI_MARKET_ENABLE_AGGRESSIVE_SCRAPING", "ENABLE_AGGRESSIVE_SCRAPING"
        ),
    )
    enrich_only_high_impact: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "AI_MARKET_ENRICH_ONLY_HIGH_IMPACT", "ENRICH_ONLY_HIGH_IMPACT"
        ),
    )
    enrichment_max_events: int = Field(
        default=10,
        validation_alias=AliasChoices("AI_MARKET_ENRICHMENT_MAX_EVENTS", "ENRICHMENT_MAX_EVENTS"),
    )
    event_enrichment_cache_ttl_hours: int = Field(
        default=24,
        validation_alias=AliasChoices(
            "AI_MARKET_EVENT_ENRICHMENT_CACHE_TTL_HOURS",
            "EVENT_ENRICHMENT_CACHE_TTL_HOURS",
        ),
    )
    enable_targeted_search_enrichment: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "AI_MARKET_ENABLE_TARGETED_SEARCH_ENRICHMENT",
            "ENABLE_TARGETED_SEARCH_ENRICHMENT",
        ),
    )
    targeted_search_max_events: int = Field(
        default=10,
        validation_alias=AliasChoices(
            "AI_MARKET_TARGETED_SEARCH_MAX_EVENTS", "TARGETED_SEARCH_MAX_EVENTS"
        ),
    )
    targeted_search_timeout_seconds: float = Field(
        default=10.0,
        validation_alias=AliasChoices(
            "AI_MARKET_TARGETED_SEARCH_TIMEOUT_SECONDS",
            "TARGETED_SEARCH_TIMEOUT_SECONDS",
        ),
    )
    targeted_search_recency_days: int = Field(
        default=30,
        validation_alias=AliasChoices(
            "AI_MARKET_TARGETED_SEARCH_RECENCY_DAYS",
            "TARGETED_SEARCH_RECENCY_DAYS",
        ),
    )
    targeted_search_require_source_url: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "AI_MARKET_TARGETED_SEARCH_REQUIRE_SOURCE_URL",
            "TARGETED_SEARCH_REQUIRE_SOURCE_URL",
        ),
    )
    openai_event_enrichment_model: str = "gpt-5-mini"
    openai_event_enrichment_max_events: int = 5
    manual_event_enrichment_path: Path = Path("./data/manual_event_enrichment.json")
    allow_stale_facts: bool = Field(
        default=False,
        validation_alias=AliasChoices("AI_MARKET_ALLOW_STALE_FACTS", "ALLOW_STALE_FACTS"),
    )
    default_news_ttl_hours: int = Field(
        default=24, validation_alias="AI_MARKET_DEFAULT_NEWS_TTL_HOURS"
    )
    default_fact_ttl_hours: int = Field(
        default=24, validation_alias="AI_MARKET_DEFAULT_FACT_TTL_HOURS"
    )
    readiness_require_news: bool = Field(
        default=False,
        validation_alias=AliasChoices("AI_MARKET_READINESS_REQUIRE_NEWS", "READINESS_REQUIRE_NEWS"),
    )
    fmp_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("AI_MARKET_FMP_API_KEY", "FMP_API_KEY"),
    )
    readiness_require_rates: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "AI_MARKET_READINESS_REQUIRE_RATES", "READINESS_REQUIRE_RATES"
        ),
    )
    readiness_require_positioning: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "AI_MARKET_READINESS_REQUIRE_POSITIONING", "READINESS_REQUIRE_POSITIONING"
        ),
    )
    readiness_require_sentiment: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "AI_MARKET_READINESS_REQUIRE_SENTIMENT", "READINESS_REQUIRE_SENTIMENT"
        ),
    )
    readiness_require_prediction_markets: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "AI_MARKET_READINESS_REQUIRE_PREDICTION_MARKETS",
            "READINESS_REQUIRE_PREDICTION_MARKETS",
        ),
    )
    news_weekend_lookback_hours: int = Field(
        default=72,
        validation_alias=AliasChoices(
            "AI_MARKET_NEWS_WEEKEND_LOOKBACK_HOURS", "NEWS_WEEKEND_LOOKBACK_HOURS"
        ),
    )
    news_market_open_lookback_hours: int = Field(
        default=24,
        validation_alias=AliasChoices(
            "AI_MARKET_NEWS_MARKET_OPEN_LOOKBACK_HOURS",
            "NEWS_MARKET_OPEN_LOOKBACK_HOURS",
        ),
    )
    news_holiday_lookback_hours: int = Field(
        default=72,
        validation_alias=AliasChoices(
            "AI_MARKET_NEWS_HOLIDAY_LOOKBACK_HOURS", "NEWS_HOLIDAY_LOOKBACK_HOURS"
        ),
    )
    qqq_holdings_ttl_hours: int = Field(
        default=24, validation_alias="AI_MARKET_QQQ_HOLDINGS_TTL_HOURS"
    )
    qqq_holdings_stale_tolerance_hours: int = Field(
        default=72, validation_alias="AI_MARKET_QQQ_HOLDINGS_STALE_TOLERANCE_HOURS"
    )
    qqq_reconstructed_weight_ttl_hours: int = Field(
        default=12, validation_alias="AI_MARKET_QQQ_RECONSTRUCTED_WEIGHT_TTL_HOURS"
    )
    qqq_weight_total_tolerance_pct: float = Field(
        default=1.0, validation_alias="AI_MARKET_QQQ_WEIGHT_TOTAL_TOLERANCE_PCT"
    )
    qqq_weight_min_coverage_pct: float = Field(
        default=95.0, validation_alias="AI_MARKET_QQQ_WEIGHT_MIN_COVERAGE_PCT"
    )
    qqq_weight_max_constituent_pct: float = Field(
        default=25.0, validation_alias="AI_MARKET_QQQ_WEIGHT_MAX_CONSTITUENT_PCT"
    )
    sec_class_shares_ttl_hours: int = Field(
        default=24, validation_alias="AI_MARKET_SEC_CLASS_SHARES_TTL_HOURS"
    )
    sec_request_timeout_seconds: float = Field(
        default=15.0, validation_alias="AI_MARKET_SEC_REQUEST_TIMEOUT_SECONDS"
    )
    sec_user_agent: str = Field(
        default="AI-MARKET-DATA-SERVICE contact@example.com",
        validation_alias="AI_MARKET_SEC_USER_AGENT",
    )
    earnings_ttl_hours: int = Field(default=24, validation_alias="AI_MARKET_EARNINGS_TTL_HOURS")
    enable_ai_researcher: bool = Field(
        default=False, validation_alias="AI_MARKET_ENABLE_AI_RESEARCHER"
    )
    ai_researcher_mode: str = Field(
        default="codex_cli", validation_alias="AI_MARKET_AI_RESEARCHER_MODE"
    )
    research_backend: str = Field(
        default="codex_cli",
        validation_alias="AI_MARKET_RESEARCH_BACKEND",
    )
    research_parallelism: int = Field(
        default=4,
        ge=1,
        le=16,
        validation_alias="AI_MARKET_RESEARCH_PARALLELISM",
    )
    research_clock_skew_seconds: int = Field(
        default=300,
        ge=0,
        validation_alias="AI_MARKET_RESEARCH_CLOCK_SKEW_SECONDS",
    )
    research_macro_horizon_days: int = Field(
        default=550,
        ge=1,
        validation_alias="AI_MARKET_RESEARCH_MACRO_HORIZON_DAYS",
    )
    research_earnings_horizon_days: int = Field(
        default=400,
        ge=1,
        validation_alias="AI_MARKET_RESEARCH_EARNINGS_HORIZON_DAYS",
    )
    ai_researcher_max_events: int = Field(
        default=5, validation_alias="AI_MARKET_AI_RESEARCHER_MAX_EVENTS"
    )
    ai_researcher_max_macro_events: int = Field(
        default=5, validation_alias="AI_MARKET_AI_RESEARCH_MAX_MACRO_EVENTS"
    )
    ai_researcher_max_news: int = Field(
        default=10, validation_alias="AI_MARKET_AI_RESEARCH_MAX_NEWS"
    )
    ai_researcher_max_earnings_symbols: int = Field(
        default=13, validation_alias="AI_MARKET_AI_RESEARCH_MAX_EARNINGS_SYMBOLS"
    )
    ai_researcher_max_cot_requests: int = Field(
        default=1, validation_alias="AI_MARKET_AI_RESEARCH_MAX_COT_REQUESTS"
    )
    ai_researcher_max_sentiment_requests: int = Field(
        default=1, validation_alias="AI_MARKET_AI_RESEARCH_MAX_SENTIMENT_REQUESTS"
    )
    ai_researcher_min_confidence: float = Field(
        default=0.5, validation_alias="AI_MARKET_AI_RESEARCH_MIN_CONFIDENCE"
    )
    ai_researcher_require_evidence: bool = Field(
        default=True, validation_alias="AI_MARKET_AI_RESEARCH_REQUIRE_EVIDENCE"
    )
    ai_researcher_only_high_impact: bool = Field(
        default=True,
        validation_alias="AI_MARKET_AI_RESEARCHER_ONLY_HIGH_IMPACT",
    )
    ai_researcher_require_source_url: bool = Field(
        default=True,
        validation_alias="AI_MARKET_AI_RESEARCHER_REQUIRE_SOURCE_URL",
    )
    ai_diagnostics: bool = Field(default=False, validation_alias="AI_MARKET_AI_DIAGNOSTICS")
    ai_diagnostics_dir: Path | None = Field(
        default=None, validation_alias="AI_MARKET_AI_DIAGNOSTICS_DIR"
    )
    save_failed_research: bool = Field(
        default=True, validation_alias="AI_MARKET_SAVE_FAILED_RESEARCH"
    )
    codex_cli_command: str = Field(default="codex", validation_alias="AI_MARKET_CODEX_CLI_COMMAND")
    codex_workspace_dir: Path = Field(
        default=Path("./data/ai_research_workspace"),
        validation_alias="AI_MARKET_CODEX_WORKSPACE_DIR",
    )
    codex_research_timeout_seconds: int = Field(
        default=300,
        validation_alias="AI_MARKET_CODEX_RESEARCH_TIMEOUT_SECONDS",
    )
    openai_research_model: str | None = Field(
        default=None,
        validation_alias="AI_MARKET_OPENAI_RESEARCH_MODEL",
    )
    openai_research_timeout_seconds: int = Field(
        default=60,
        validation_alias="AI_MARKET_OPENAI_RESEARCH_TIMEOUT_SECONDS",
    )
    openai_research_temperature: float = Field(
        default=0,
        validation_alias="AI_MARKET_OPENAI_RESEARCH_TEMPERATURE",
    )
    release_refresh_retry_seconds: str = Field(
        default="30,120,300,900,1800,3600",
        validation_alias="AI_MARKET_RELEASE_REFRESH_RETRY_SECONDS",
    )
    max_release_refresh_attempts: int = Field(
        default=6,
        validation_alias="AI_MARKET_MAX_RELEASE_REFRESH_ATTEMPTS",
    )
    official_feed_delay_hours: int = Field(
        default=24,
        ge=1,
        validation_alias="AI_MARKET_OFFICIAL_FEED_DELAY_HOURS",
    )
    official_actual_retry_seconds: str = Field(
        default="30,120,300,900,1800,3600,3600,3600,3600,3600,3600,3600",
        validation_alias="AI_MARKET_OFFICIAL_ACTUAL_RETRY_SECONDS",
    )
    official_actual_max_attempts: int = Field(
        default=48,
        ge=2,
        validation_alias="AI_MARKET_OFFICIAL_ACTUAL_MAX_ATTEMPTS",
    )
    source_policy_path: Path = Field(
        default=Path("./config/source_policy.json"),
        validation_alias="AI_MARKET_SOURCE_POLICY_PATH",
    )
    ai_worker_enabled: bool = Field(
        default=False,
        validation_alias="AI_MARKET_AI_WORKER_ENABLED",
    )
    ai_worker_poll_seconds: float = Field(
        default=1.0,
        ge=0.05,
        validation_alias="AI_MARKET_AI_WORKER_POLL_SECONDS",
    )
    ai_job_lease_seconds: int = Field(
        default=60,
        ge=5,
        validation_alias="AI_MARKET_AI_JOB_LEASE_SECONDS",
    )
    ai_job_max_runtime_seconds: int = Field(
        default=600,
        ge=1,
        validation_alias="AI_MARKET_AI_JOB_MAX_RUNTIME_SECONDS",
    )
    ai_job_max_attempts: int = Field(
        default=3,
        ge=1,
        validation_alias="AI_MARKET_AI_JOB_MAX_ATTEMPTS",
    )
    ai_research_web_access_enabled: bool = Field(
        default=False,
        validation_alias="AI_MARKET_AI_RESEARCH_WEB_ACCESS_ENABLED",
    )
    ai_job_workspace_root: Path = Field(
        default=Path("./data/ai_research_jobs"),
        validation_alias="AI_MARKET_AI_JOB_WORKSPACE_ROOT",
    )
    ai_worker_shutdown_timeout_seconds: float = Field(
        default=10.0,
        ge=1.0,
        validation_alias="AI_MARKET_AI_WORKER_SHUTDOWN_TIMEOUT_SECONDS",
    )
    ai_run_window_news_minutes: int = Field(
        default=15, ge=1, validation_alias="AI_MARKET_AI_RUN_WINDOW_NEWS_MINUTES"
    )
    ai_run_window_missing_event_minutes: int = Field(
        default=60, ge=1, validation_alias="AI_MARKET_AI_RUN_WINDOW_MISSING_EVENT_MINUTES"
    )
    ai_run_window_speech_minutes: int = Field(
        default=30, ge=1, validation_alias="AI_MARKET_AI_RUN_WINDOW_SPEECH_MINUTES"
    )
    ai_run_window_earnings_minutes: int = Field(
        default=60, ge=1, validation_alias="AI_MARKET_AI_RUN_WINDOW_EARNINGS_MINUTES"
    )
    ai_run_window_general_market_minutes: int = Field(
        default=30, ge=1, validation_alias="AI_MARKET_AI_RUN_WINDOW_GENERAL_MARKET_MINUTES"
    )
    ai_run_window_actual_refresh_minutes: int = Field(
        default=15, ge=1, validation_alias="AI_MARKET_AI_RUN_WINDOW_ACTUAL_REFRESH_MINUTES"
    )
    research_scheduler_enabled: bool = Field(
        default=False, validation_alias="AI_MARKET_RESEARCH_SCHEDULER_ENABLED"
    )
    research_premarket_enabled: bool = Field(
        default=True, validation_alias="AI_MARKET_RESEARCH_PREMARKET_ENABLED"
    )
    research_session_enabled: bool = Field(
        default=True, validation_alias="AI_MARKET_RESEARCH_SESSION_ENABLED"
    )
    research_postmarket_enabled: bool = Field(
        default=True, validation_alias="AI_MARKET_RESEARCH_POSTMARKET_ENABLED"
    )
    research_event_triggers_enabled: bool = Field(
        default=True, validation_alias="AI_MARKET_RESEARCH_EVENT_TRIGGERS_ENABLED"
    )
    research_news_enabled: bool = Field(
        default=True, validation_alias="AI_MARKET_RESEARCH_NEWS_ENABLED"
    )
    research_premarket_time: str = Field(
        default="08:00", validation_alias="AI_MARKET_RESEARCH_PREMARKET_TIME"
    )
    research_postmarket_time: str = Field(
        default="17:00", validation_alias="AI_MARKET_RESEARCH_POSTMARKET_TIME"
    )
    research_session_interval_minutes: int = Field(
        default=30, ge=1, validation_alias="AI_MARKET_RESEARCH_SESSION_INTERVAL_MINUTES"
    )
    research_max_concurrent_jobs: int = Field(
        default=1, ge=1, validation_alias="AI_MARKET_RESEARCH_MAX_CONCURRENT_JOBS"
    )
    research_max_searches: int = Field(
        default=8, ge=1, validation_alias="AI_MARKET_RESEARCH_MAX_SEARCHES"
    )
    research_max_opened_sources: int = Field(
        default=12, ge=1, validation_alias="AI_MARKET_RESEARCH_MAX_OPENED_SOURCES"
    )
    research_budget_mode: str = Field(
        default="observe",
        pattern="^(observe|enforce)$",
        validation_alias="AI_MARKET_RESEARCH_BUDGET_MODE",
    )
    research_loop_repeat_action_threshold: int = Field(
        default=3,
        ge=2,
        validation_alias="AI_MARKET_RESEARCH_LOOP_REPEAT_ACTION_THRESHOLD",
    )
    research_loop_no_progress_action_threshold: int = Field(
        default=12,
        ge=3,
        validation_alias="AI_MARKET_RESEARCH_LOOP_NO_PROGRESS_ACTION_THRESHOLD",
    )
    research_loop_cycle_window: int = Field(
        default=4,
        ge=2,
        validation_alias="AI_MARKET_RESEARCH_LOOP_CYCLE_WINDOW",
    )
    research_loop_cycle_repetitions: int = Field(
        default=3,
        ge=2,
        validation_alias="AI_MARKET_RESEARCH_LOOP_CYCLE_REPETITIONS",
    )
    research_emergency_max_tool_actions: int = Field(
        default=200,
        ge=20,
        validation_alias="AI_MARKET_RESEARCH_EMERGENCY_MAX_TOOL_ACTIONS",
    )
    research_checkpoint_on_deadline: bool = Field(
        default=True,
        validation_alias="AI_MARKET_RESEARCH_CHECKPOINT_ON_DEADLINE",
    )
    research_single_invocation_enabled: bool = Field(
        default=True,
        validation_alias="AI_MARKET_RESEARCH_SINGLE_INVOCATION_ENABLED",
    )
    research_gateway_timeout_seconds: float = Field(
        default=12.0,
        ge=1.0,
        le=30.0,
        validation_alias="AI_MARKET_RESEARCH_GATEWAY_TIMEOUT_SECONDS",
    )
    research_gateway_max_redirects: int = Field(
        default=4,
        ge=0,
        le=10,
        validation_alias="AI_MARKET_RESEARCH_GATEWAY_MAX_REDIRECTS",
    )
    research_gateway_respect_robots: bool = Field(
        default=True,
        validation_alias="AI_MARKET_RESEARCH_GATEWAY_RESPECT_ROBOTS",
    )
    research_gateway_max_content_bytes: int = Field(
        default=5_000_000,
        ge=65_536,
        le=25_000_000,
        validation_alias="AI_MARKET_RESEARCH_GATEWAY_MAX_CONTENT_BYTES",
    )
    research_gateway_max_text_chars: int = Field(
        default=200_000,
        ge=5_000,
        le=1_000_000,
        validation_alias="AI_MARKET_RESEARCH_GATEWAY_MAX_TEXT_CHARS",
    )
    research_gateway_min_text_chars: int = Field(
        default=80,
        ge=1,
        le=5_000,
        validation_alias="AI_MARKET_RESEARCH_GATEWAY_MIN_TEXT_CHARS",
    )
    research_gateway_max_sources_per_run: int = Field(
        default=32,
        ge=1,
        le=200,
        validation_alias="AI_MARKET_RESEARCH_GATEWAY_MAX_SOURCES_PER_RUN",
    )
    research_evidence_match_threshold: float = Field(
        default=0.88,
        ge=0.7,
        le=1.0,
        validation_alias="AI_MARKET_RESEARCH_EVIDENCE_MATCH_THRESHOLD",
    )
    research_evidence_min_tokens: int = Field(
        default=5,
        ge=3,
        le=50,
        validation_alias="AI_MARKET_RESEARCH_EVIDENCE_MIN_TOKENS",
    )
    research_daily_budget_runs: int = Field(
        default=8, ge=0, validation_alias="AI_MARKET_RESEARCH_DAILY_BUDGET_RUNS"
    )
    research_minimum_freshness_minutes: int = Field(
        default=15, ge=1, validation_alias="AI_MARKET_RESEARCH_MINIMUM_FRESHNESS_MINUTES"
    )
    research_pre_event_window_minutes: int = Field(
        default=120, ge=1, validation_alias="AI_MARKET_RESEARCH_PRE_EVENT_WINDOW_MINUTES"
    )
    research_daily_budget_searches: int = Field(
        default=64, ge=0, validation_alias="AI_MARKET_RESEARCH_DAILY_BUDGET_SEARCHES"
    )
    research_daily_budget_opened_sources: int = Field(
        default=96, ge=0, validation_alias="AI_MARKET_RESEARCH_DAILY_BUDGET_OPENED_SOURCES"
    )
    enable_investing_calendar: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_INVESTING_CALENDAR"
    )
    enable_investing_holidays: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_INVESTING_HOLIDAYS"
    )
    enable_marketbeat_holidays: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_MARKETBEAT_HOLIDAYS"
    )
    enable_investing_fed_rate_monitor: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_INVESTING_FED_RATE_MONITOR"
    )
    enable_cboe_risk_indices: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_CBOE_RISK_INDICES"
    )
    enable_cboe_vix_futures: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_CBOE_VIX_FUTURES"
    )
    enable_cboe_put_call: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_CBOE_PUT_CALL"
    )
    risk_context_ttl_minutes: int = Field(
        default=30, validation_alias="AI_MARKET_RISK_CONTEXT_TTL_MINUTES"
    )
    risk_context_history_min_points: int = Field(
        default=60, validation_alias="AI_MARKET_RISK_CONTEXT_HISTORY_MIN_POINTS"
    )
    risk_curve_flat_tolerance_pct: float = Field(
        default=0.25, validation_alias="AI_MARKET_RISK_CURVE_FLAT_TOLERANCE_PCT"
    )
    risk_alignment_max_gap_minutes: int = Field(
        default=1440, validation_alias="AI_MARKET_RISK_ALIGNMENT_MAX_GAP_MINUTES"
    )
    enable_nasdaq_earnings: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_NASDAQ_EARNINGS"
    )
    enable_fmp_earnings: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_FMP_EARNINGS"
    )
    enable_xtb_calendar: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_XTB_CALENDAR"
    )
    xtb_calendar_timeout_seconds: float = Field(
        default=10.0, validation_alias="AI_MARKET_XTB_CALENDAR_TIMEOUT_SECONDS"
    )
    xtb_calendar_min_impact: int = Field(
        default=2, ge=0, le=3, validation_alias="AI_MARKET_XTB_CALENDAR_MIN_IMPACT"
    )
    xtb_calendar_lookahead_days: int = Field(
        default=7, ge=1, le=14, validation_alias="AI_MARKET_XTB_CALENDAR_LOOKAHEAD_DAYS"
    )
    xtb_calendar_ttl_minutes: int = Field(
        default=30, ge=1, validation_alias="AI_MARKET_XTB_CALENDAR_TTL_MINUTES"
    )
    enable_nasdaq_100: bool = Field(default=True, validation_alias="AI_MARKET_ENABLE_NASDAQ_100")
    enable_nasdaq_market_info: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_NASDAQ_MARKET_INFO"
    )
    enable_cme_market_schedule: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_CME_MARKET_SCHEDULE"
    )
    cme_market_schedule_timeout_seconds: float = Field(
        default=10.0, validation_alias="AI_MARKET_CME_MARKET_SCHEDULE_TIMEOUT_SECONDS"
    )
    cme_market_schedule_ttl_hours: int = Field(
        default=24, validation_alias="AI_MARKET_CME_MARKET_SCHEDULE_TTL_HOURS"
    )
    enable_nasdaq_qqq_options: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_NASDAQ_QQQ_OPTIONS"
    )
    enable_aaii_sentiment: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_AAII_SENTIMENT"
    )
    enable_social_sentiment: bool = Field(
        default=True, validation_alias="AI_MARKET_ENABLE_SOCIAL_SENTIMENT"
    )
    social_sentiment_ttl_minutes: int = Field(
        default=30, validation_alias="AI_MARKET_SOCIAL_SENTIMENT_TTL_MINUTES"
    )
    social_sentiment_timeout_seconds: float = Field(
        default=6.0, validation_alias="AI_MARKET_SOCIAL_SENTIMENT_TIMEOUT_SECONDS"
    )
    social_sentiment_max_items: int = Field(
        default=40, validation_alias="AI_MARKET_SOCIAL_SENTIMENT_MAX_ITEMS"
    )
    enable_macromicro_aaii_crosscheck: bool = Field(
        default=False, validation_alias="AI_MARKET_ENABLE_MACROMICRO_AAII_CROSSCHECK"
    )
    enable_polymarket: bool = Field(default=False, validation_alias="AI_MARKET_ENABLE_POLYMARKET")
    provider_failure_cache_minutes: int = Field(
        default=30, validation_alias="AI_MARKET_PROVIDER_FAILURE_CACHE_MINUTES"
    )
    provider_negative_cache_minutes: int = Field(
        default=60, validation_alias="AI_MARKET_PROVIDER_NEGATIVE_CACHE_MINUTES"
    )
    provider_max_retries: int = Field(default=1, validation_alias="AI_MARKET_PROVIDER_MAX_RETRIES")
    provider_circuit_breaker_failures: int = Field(
        default=3, validation_alias="AI_MARKET_PROVIDER_CIRCUIT_BREAKER_FAILURES"
    )
    provider_circuit_breaker_minutes: int = Field(
        default=15, validation_alias="AI_MARKET_PROVIDER_CIRCUIT_BREAKER_MINUTES"
    )
    investing_domain_id: int = Field(default=1, validation_alias="AI_MARKET_INVESTING_DOMAIN_ID")
    investing_country_ids: str = Field(
        default="5", validation_alias="AI_MARKET_INVESTING_COUNTRY_IDS"
    )
    investing_calendar_lookahead_days: int = Field(
        default=35, validation_alias="AI_MARKET_INVESTING_CALENDAR_LOOKAHEAD_DAYS"
    )
    investing_calendar_page_limit: int = Field(
        default=100, validation_alias="AI_MARKET_INVESTING_CALENDAR_PAGE_LIMIT"
    )
    marketbeat_timeout_seconds: float = Field(
        default=10.0, validation_alias="AI_MARKET_MARKETBEAT_TIMEOUT_SECONDS"
    )
    marketbeat_holidays_ttl_hours: int = Field(
        default=24, validation_alias="AI_MARKET_MARKETBEAT_HOLIDAYS_TTL_HOURS"
    )
    investing_fed_rate_monitor_timeout_seconds: float = Field(
        default=10.0, validation_alias="AI_MARKET_INVESTING_FED_RATE_MONITOR_TIMEOUT_SECONDS"
    )
    investing_fed_rate_monitor_ttl_minutes: int = Field(
        default=30, validation_alias="AI_MARKET_INVESTING_FED_RATE_MONITOR_TTL_MINUTES"
    )
    investing_fed_rate_monitor_max_meetings: int = Field(
        default=8, validation_alias="AI_MARKET_INVESTING_FED_RATE_MONITOR_MAX_MEETINGS"
    )
    nasdaq_options_symbol: str = Field(
        default="QQQ", validation_alias="AI_MARKET_NASDAQ_OPTIONS_SYMBOL"
    )
    nasdaq_options_lookahead_days: int = Field(
        default=30, validation_alias="AI_MARKET_NASDAQ_OPTIONS_LOOKAHEAD_DAYS"
    )
    nasdaq_options_default_money: str = Field(
        default="all", validation_alias="AI_MARKET_NASDAQ_OPTIONS_DEFAULT_MONEY"
    )
    nasdaq_options_default_type: str = Field(
        default="all", validation_alias="AI_MARKET_NASDAQ_OPTIONS_DEFAULT_TYPE"
    )
    nasdaq_options_page_size: int = Field(
        default=60, validation_alias="AI_MARKET_NASDAQ_OPTIONS_PAGE_SIZE"
    )
    nasdaq_options_max_pages: int = Field(
        default=3, validation_alias="AI_MARKET_NASDAQ_OPTIONS_MAX_PAGES"
    )
    nasdaq_options_cache_minutes: int = Field(
        default=30, validation_alias="AI_MARKET_NASDAQ_OPTIONS_CACHE_MINUTES"
    )
    polymarket_gamma_base_url: str = Field(
        default="https://gamma-api.polymarket.com",
        validation_alias="AI_MARKET_POLYMARKET_GAMMA_BASE_URL",
    )
    polymarket_data_base_url: str = Field(
        default="https://data-api.polymarket.com",
        validation_alias="AI_MARKET_POLYMARKET_DATA_BASE_URL",
    )
    polymarket_clob_base_url: str = Field(
        default="https://clob.polymarket.com", validation_alias="AI_MARKET_POLYMARKET_CLOB_BASE_URL"
    )
    polymarket_timeout_seconds: float = Field(
        default=10.0, validation_alias="AI_MARKET_POLYMARKET_TIMEOUT_SECONDS"
    )
    polymarket_min_liquidity_usd: float = Field(
        default=10000.0, validation_alias="AI_MARKET_POLYMARKET_MIN_LIQUIDITY_USD"
    )
    polymarket_min_volume_usd: float = Field(
        default=25000.0, validation_alias="AI_MARKET_POLYMARKET_MIN_VOLUME_USD"
    )
    polymarket_max_spread: float = Field(
        default=0.25, validation_alias="AI_MARKET_POLYMARKET_MAX_SPREAD"
    )
    polymarket_max_markets: int = Field(
        default=20, validation_alias="AI_MARKET_POLYMARKET_MAX_MARKETS"
    )
    polymarket_lookahead_days: int = Field(
        default=180, validation_alias="AI_MARKET_POLYMARKET_LOOKAHEAD_DAYS"
    )
    polymarket_history_days: int = Field(
        default=14, validation_alias="AI_MARKET_POLYMARKET_HISTORY_DAYS"
    )
    polymarket_cache_minutes: int = Field(
        default=15, validation_alias="AI_MARKET_POLYMARKET_CACHE_MINUTES"
    )

    fred_base_url: str = "https://api.stlouisfed.org/fred"
    bls_base_url: str = "https://api.bls.gov/publicAPI/v2/timeseries/data"
    bea_base_url: str = "https://apps.bea.gov/api/data"
    federal_reserve_calendar_base_url: str = "https://www.federalreserve.gov/newsevents"
    bls_schedule_base_url: str = "https://www.bls.gov/schedule"
    bea_release_schedule_url: str = "https://www.bea.gov/news/schedule"
    invesco_qqq_holdings_url: str = (
        "https://www.invesco.com/us/financial-products/etfs/holdings/"
        "main/holdings/0?audienceType=Investor&action=download&ticker=QQQ"
    )
    nasdaq_100_constituents_url: str = (
        "https://api.nasdaq.com/api/quote/list-type/nasdaq100?assetclass=stocks"
    )
    sec_submissions_base_url: str = "https://data.sec.gov/submissions"
    sec_archives_base_url: str = "https://www.sec.gov/Archives/edgar/data"
    yahoo_quote_url: str = "https://query1.finance.yahoo.com/v7/finance/quote"
    yahoo_chart_url: str = "https://query1.finance.yahoo.com/v8/finance/chart"
    yahoo_quote_summary_url: str = "https://query2.finance.yahoo.com/v10/finance/quoteSummary"
    gdelt_doc_api_url: str = "https://api.gdeltproject.org/api/v2/doc/doc"
    alpha_vantage_base_url: str = "https://www.alphavantage.co/query"
    google_news_rss_url: str = "https://news.google.com/rss/search"
    yahoo_finance_rss_url: str = "https://finance.yahoo.com/rss/topstories"
    marketwatch_rss_url: str = "https://feeds.content.dowjones.io/public/rss/mw_topstories"
    federal_reserve_rss_url: str = Field(
        default="https://www.federalreserve.gov/feeds/press_all.xml"
    )
    bls_rss_url: str = Field(default="https://www.bls.gov/feed/news_release.rss")
    bea_rss_url: str = Field(default="https://www.bea.gov/news/rss.xml")
    dailyfx_calendar_url: str = "https://www.dailyfx.com/economic-calendar"
    forex_factory_calendar_url: str = "https://www.forexfactory.com/calendar"
    investing_calendar_url: str = "https://www.investing.com/economic-calendar/"
    fxstreet_calendar_url: str = "https://www.fxstreet.com/economic-calendar"
    marketwatch_calendar_url: str = "https://www.marketwatch.com/economy-politics/calendar"
    yahoo_economic_calendar_url: str = "https://finance.yahoo.com/calendar/economic"
    generic_search_calendar_url: str | None = None
    investing_economic_calendar_api_url: str = (
        "https://endpoints.investing.com/pd-instruments/v1/calendars/economic/events/occurrences"
    )
    investing_holiday_calendar_api_url: str = (
        "https://endpoints.investing.com/pd-instruments/v1/calendars/holidays"
    )
    marketbeat_holidays_url: str = "https://www.marketbeat.com/stock-market-holidays/"
    investing_fed_rate_monitor_url: str = "https://www.investing.com/central-banks/fed-rate-monitor"
    cboe_vvix_url: str = "https://cdn.cboe.com/api/global/delayed_quotes/quotes/_VVIX.json"
    cboe_skew_url: str = "https://cdn.cboe.com/api/global/delayed_quotes/quotes/_SKEW.json"
    cboe_vvix_history_url: str = (
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv"
    )
    cboe_skew_history_url: str = (
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv"
    )
    cboe_vix_history_url: str = (
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    )
    cboe_vix_futures_settlement_url: str = (
        "https://www-api.cboe.com/us/futures/market_statistics/settlement/csv/"
    )
    cboe_vix_futures_delayed_url: str = (
        "https://cdn.cboe.com/api/global/delayed_quotes/futures/_VIX.json"
    )
    cboe_put_call_url: str = "https://www.cboe.com/markets/us/options/market-statistics/daily"
    nasdaq_earnings_calendar_url: str = "https://api.nasdaq.com/api/calendar/earnings"
    fmp_earnings_calendar_url: str = "https://financialmodelingprep.com/stable/earnings-calendar"
    xtb_economic_calendar_url: str = (
        "https://www.xtb.com/web-api/v3/languages/it/market-calendars?showForWeek=true&cache=false"
    )
    nasdaq_market_info_url: str = "https://api.nasdaq.com/api/market-info"
    cme_market_schedule_url: str = "https://www.cmegroup.com/trading-hours.html"
    nasdaq_qqq_option_chain_url: str = "https://api.nasdaq.com/api/quote/QQQ/option-chain"
    macromicro_aaii_chart_url: str = "https://en.macromicro.me/charts/20828/us-aaii-sentimentsurvey"
    macromicro_aaii_api_url: str = "https://en.macromicro.me/api/view/chart/20828"
    hacker_news_algolia_url: str = "https://hn.algolia.com/api/v1/search_by_date"
    hacker_news_rss_url: str = "https://news.ycombinator.com/rss"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return init_settings, dotenv_settings, env_settings, file_secret_settings


@lru_cache
def get_settings() -> Settings:
    return Settings()
