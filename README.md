# AI-MARKET-DATA-SERVICE

Python FastAPI service that collects official macro data, economic events, and Federal Reserve news, normalizes responses, caches the latest valid payloads in SQLite, and exposes REST endpoints for AI-TRADER.

This service does **not** implement trading logic, choose trades, or place orders.
It provides data only. Trading decisions are delegated to AI-TRADER.

## Quick Start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
Copy-Item .env.example .env
uvicorn app.main:app --reload
```

API docs: <http://127.0.0.1:8000/docs>

## Endpoints

- `GET /health`
- `GET /macro/latest`
- `GET /events/today?country=US`
- `GET /events/upcoming?country=US&days=7`
- `GET /events/active-windows?symbol=MNQ`
- `GET /market-context/mnq`
- `GET /market-context/mnq/consumer?refresh=false|auto|force`
- `GET /market-context/mnq/debug?refresh=false|auto|force`
- `GET /ai-research/jobs/latest`
- `GET /ai-research/jobs/{job_id}`
- `GET /ai-research/status`
- `POST /ai-research/jobs` (idempotent data-research enqueue; no trading)
- `GET /db/health`
- `GET /storage/health`
- `GET /storage/retention-policy`
- `GET /facts/lookup?fact_key=...`
- `GET /facts/search?country=US&category=CPI`
- `GET /facts/stale`
- `GET /facts/coverage?country=US&days=30`
- `GET /enrichment/run/status`
- `POST /enrichment/run?country=US&days=30`
- `GET /news/stored?symbols=NVDA,QQQ&days=7`
- `GET /nasdaq/qqq/holdings`
- `GET /nasdaq/mega-cap/snapshot`
- `GET /nasdaq/mega-cap/breadth`
- `GET /nasdaq/earnings/upcoming?days=14`
- `GET /news/latest?symbols=NVDA,AAPL,MSFT,QQQ&limit=20&recency_days=14`
- `GET /nasdaq/context`
- `GET /providers/investing/economic-calendar?refresh=false|auto|force`
- `GET /providers/xtb/economic-calendar?refresh=false|auto|force`
- `GET /providers/investing/holidays?refresh=false|auto|force`
- `GET /providers/marketbeat/holidays?refresh=false|auto|force`
- `GET /providers/investing/fed-rate-monitor?refresh=false|auto|force`
- `GET /providers/cboe/risk-indices?refresh=false|auto|force`
- `GET /providers/nasdaq/earnings-calendar?refresh=false|auto|force`
- `GET /providers/nasdaq/nasdaq-100?refresh=false|auto|force`
- `GET /providers/nasdaq/market-info?refresh=false|auto|force`
- `GET /providers/nasdaq/qqq-options?refresh=false|auto|force`
- `GET /providers/sentiment/aaii?refresh=false|auto|force`
- `GET /providers/sentiment/macromicro-aaii?refresh=false|auto|force`
- `GET /providers/polymarket/markets?refresh=false|auto|force`
- `GET /diagnostics/data-quality`

## Persistent Data Store

The central persistent SQLite DB defaults to `./data/market_data_service.sqlite`. New deployments should configure one path only:

```env
AI_MARKET_DATABASE_PATH=./data/market_data_service.sqlite
```

The DB stores reusable facts, official event history, deduplicated news, provider observations, enrichment run metrics, provider cache entries, provider state, versioned context snapshots, source candidates with lineage, persistent AI jobs/attempts, observed research tool events, verified evidence and schema migrations. Schemas 7-11 are additive and upgrade existing schema-6/8/9/10 databases without rebuilding tables. Migration 11 adds redacted Codex diagnostics and immutable per-step attempt history.

The required enrichment order is:

1. valid DB fact
2. existing provider/API/scraper chain
3. persistent AI Researcher job when explicitly enabled (the HTTP request never waits for it)
4. null values with clear warnings

DB hits do not call providers or AI. Facts carry source, source URL, `retrieved_at`, `valid_until`, `next_refresh_at`, reliability, confidence, warnings, and errors. If `valid_until` is missing, TTL defaults are used; stale facts are blocked unless `AI_MARKET_ALLOW_STALE_FACTS=true`.

## DB-first snapshots and refresh modes

- `refresh=false` returns the latest materialized SQLite snapshot byte-for-byte when one exists: no provider, browser or AI call, no job and no write. If the database has no snapshot yet, it performs one DB/cache-only cold materialization and persists revision 1; it still performs no network, browser or AI call and creates no job.
- `refresh=auto` reads valid DB data first, refreshes only missing or expired deterministic data, persists and reads it back, then queues only missing AI fields. The response contains `ai_enrichment.status=PENDING|RUNNING|NOT_REQUIRED` immediately.
- `refresh=force` refreshes deterministic providers and idempotently queues missing research, but still does not wait for AI.

The consumer remains `ai_trader_market_context_consumer` schema `2.1` and includes snapshot identity/revision, lifecycle, section availability, readiness/confidence and structured AI status. `ready_for_full_analysis` is fail-closed while critical temporal data or AI enrichment is pending.

## Temporal domains and source policy

Scheduled numeric releases move `PRE_RELEASE -> AWAITING_ACTUAL -> RELEASED` (or `ACTUAL_UNAVAILABLE` after bounded retries). Speeches move `PRE_RELEASE -> AWAITING_OUTCOME -> COMPLETED`; no numeric actual is synthesized. Earnings that have passed move out of `upcoming`. Expired news stays in retention history but is excluded from current drivers.

The executable policy is [config/source_policy.json](config/source_policy.json). Official actuals require Tier-1 sources. Calendar aggregators may contribute forecast/consensus; discordant candidates are preserved separately and never overwrite an official actual.

## Asynchronous AI worker

The worker is controlled separately from `AI_MARKET_ENABLE_SCHEDULER`:

```env
AI_MARKET_ENABLE_AI_RESEARCHER=false
AI_MARKET_AI_RESEARCH_WEB_ACCESS_ENABLED=false
AI_MARKET_AI_WORKER_ENABLED=false
```

Enabling the worker without both research and web-access switches does not run Codex. Even with static prerequisites present, ordinary AI jobs remain gated at `READY_TO_SMOKE` until a separately authorized live smoke is persisted as `LIVE_VERIFIED`; CLI `--search` support alone is insufficient. `READY_TO_SMOKE` is offline readiness, not live-web verification. Keep `AI_MARKET_RESEARCH_SCHEDULER_ENABLED=false` until that smoke succeeds. Each acquired job has an atomic lease, heartbeat, persistent attempts/retries, unique workspace and one overall monotonic watchdog deadline. Release actual jobs use persisted deterministic official candidates first, have a separate configurable official-feed-delay retry horizon, and do not use AI as their primary resolver.

Non-destructive rollback: stop the service, set `AI_MARKET_AI_WORKER_ENABLED=false`, and deploy the prior application version. Do not delete the SQLite database; schema-7 through schema-11 tables/columns are additive and older code can ignore them.

Useful persistent-data checks:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/db/health"
Invoke-RestMethod "http://127.0.0.1:8000/db/health/details"
Invoke-RestMethod "http://127.0.0.1:8000/db/schema-version"
Invoke-RestMethod "http://127.0.0.1:8000/db/cache/stats"
Invoke-RestMethod "http://127.0.0.1:8000/storage/health"
Invoke-RestMethod "http://127.0.0.1:8000/storage/retention-policy"
Invoke-RestMethod "http://127.0.0.1:8000/facts/coverage?country=US&days=30"
Invoke-RestMethod -Method Post "http://127.0.0.1:8000/enrichment/run?country=US&days=30"
```

Operational scripts:

```powershell
.\.venv\Scripts\python.exe .\scripts\migrate_legacy_database.py --source .\data\old.sqlite --dry-run
.\.venv\Scripts\python.exe .\scripts\backup_database.py
.\.venv\Scripts\python.exe .\scripts\validate_database.py
.\.venv\Scripts\python.exe .\scripts\reset_database.py --cache-only --dry-run
.\.venv\Scripts\python.exe .\scripts\cleanup_storage.py --dry-run
.\.venv\Scripts\python.exe .\scripts\cleanup_storage.py --category diagnostics --apply
```

Storage retention covers diagnostics, backups, logs, service temp files, and DB maintenance tables. Manual cleanup defaults to dry-run; startup performs only lightweight non-blocking temp cleanup, and scheduled full cleanup runs at most once per configured interval when the scheduler is enabled.

## Data Sources

Initial providers:

- FRED API: VIX, yields, Fed Funds, financial conditions
- BLS API: CPI, PPI, payrolls, unemployment
- BEA API: GDP, Real GDP, PCE, Core PCE, personal income, personal spending
- Federal Reserve official RSS/pages: FOMC, speeches, minutes, press releases
- Invesco/Nasdaq public data: QQQ holdings and Nasdaq-100 constituents
- Alpha Vantage API: QQQ ETF holdings, earnings calendar, news metadata, controlled quote fallback
- Financial Modeling Prep stable earnings calendar: primary upcoming mega-cap earnings source when `FMP_API_KEY` is configured
- XTB JSON economic calendar: secondary US macro calendar, consensus/previous/actual cross-check, impact 2-3
- Yahoo public chart endpoint: primary mega-cap quote snapshot source
- Stooq public CSV: quote fallback only when primary quote sources do not produce data
- GDELT public API and RSS feeds: macro and mega-cap news fallback
- RSS fallback feeds: Google News RSS search, Yahoo Finance RSS, MarketWatch RSS, Federal Reserve RSS
- Event enrichment fallbacks: DailyFX, ForexFactory, Investing public economic calendars, FXStreet, MarketWatch, Yahoo economic calendar, optional browser scraping, targeted Search/RSS snippets, optional manual file, optional OpenAI web scaffold, then cached enrichment
- Economic calendar scraper placeholders: DailyFX, ForexFactory, Investing, disabled by config

Each provider is isolated. If a live source fails, the service logs the error, tries configured fallbacks, and returns the last valid cached payload when available.

## Structured Multi-Source Enrichment

The service also supports read-only structured enrichment sources that are persisted through `market_facts` with provider observations and DB read-back diagnostics:

- Investing Economic Calendar: secondary consensus/previous/actual context. `forecast` maps to canonical macro `consensus`; secondary actuals never overwrite official BLS/BEA/Fed values.
- Investing Holiday Calendar: exchange and market-holiday context, exposed under `market_schedule.holidays`.
- CBOE delayed quote endpoints for VVIX and SKEW: enrich `risk_context`; SKEW zero open/high/low fields are treated as unreliable.
- Nasdaq Earnings Calendar: corporate EPS consensus, kept distinct from macro consensus.
- Nasdaq-100 constituents endpoint: exposed as an official snapshot with anomaly diagnostics; it does not replace QQQ holdings automatically.
- Nasdaq Market Info: Nasdaq cash-session status and raw session timestamps in `America/New_York`.
- Nasdaq QQQ option chain: a Nasdaq-100 proxy only, with descriptive open-interest and volume aggregates. It does not invent Greeks, dealer positioning, levels, or signals.
- AAII Sentiment Survey: weekly public sentiment parser with DOM/script support and validation that percentages sum to roughly 100.
- MacroMicro AAII: optional anonymous cross-check only; restricted responses are reported as non-blocking.
- Polymarket Gamma/Data/CLOB public data: read-only market-implied probabilities filtered for macro/Nasdaq relevance, volume, liquidity, end date, and rules. No authentication or user-account capability is used.

Feature flags:

```env
AI_MARKET_ENABLE_INVESTING_CALENDAR=true
AI_MARKET_ENABLE_INVESTING_HOLIDAYS=true
AI_MARKET_ENABLE_CBOE_RISK_INDICES=true
AI_MARKET_ENABLE_NASDAQ_EARNINGS=true
AI_MARKET_ENABLE_NASDAQ_100=true
AI_MARKET_ENABLE_NASDAQ_MARKET_INFO=true
AI_MARKET_ENABLE_NASDAQ_QQQ_OPTIONS=true
AI_MARKET_ENABLE_AAII_SENTIMENT=true
AI_MARKET_ENABLE_MACROMICRO_AAII_CROSSCHECK=false
AI_MARKET_ENABLE_POLYMARKET=true
AI_MARKET_NASDAQ_OPTIONS_SYMBOL=QQQ
AI_MARKET_NASDAQ_OPTIONS_LOOKAHEAD_DAYS=30
AI_MARKET_NASDAQ_OPTIONS_PAGE_SIZE=60
AI_MARKET_NASDAQ_OPTIONS_MAX_PAGES=3
AI_MARKET_POLYMARKET_MIN_LIQUIDITY_USD=10000
AI_MARKET_POLYMARKET_MIN_VOLUME_USD=25000
AI_MARKET_POLYMARKET_MAX_SPREAD=0.25
```

Refresh modes:

- `refresh=false`: DB/cache only for structured enrichment; no network, browser, or AI calls.
- `refresh=auto`: provider endpoints use valid DB first and call a provider only when needed.
- `refresh=force`: provider endpoints and `/market-context/mnq?refresh=force` force live provider refreshes and then persist/read back facts.

CME QuikStrike was reviewed and excluded as an operational provider because the useful view is session-bound and login/session dependent. The service records the review as `source_reviews.quikstrike` with `session_bound=true` and `operational_integration=false`.

## Configuration

The service reads `.env` from the project root. Deployment-safe names use the `AI_MARKET_` prefix:

```env
AI_MARKET_FRED_API_KEY=...
AI_MARKET_BEA_API_KEY=...
AI_MARKET_BLS_API_KEY=...
AI_MARKET_ALPHA_VANTAGE_API_KEY=...
AI_MARKET_FMP_API_KEY=...
```

For local compatibility, unprefixed names are also accepted:

```env
FRED_API_KEY=...
BEA_API_KEY=...
BLS_API_KEY=...
ALPHA_VANTAGE_API_KEY=...
FMP_API_KEY=...
```

API keys are never written to structured logs. If FMP is not configured, its request is skipped and the existing earnings fallback/cache remains available. XTB requires no key and can be disabled with `AI_MARKET_ENABLE_XTB_CALENDAR=false`.

Official event schedules currently used by `/events/today` and `/events/upcoming`:

- Federal Reserve calendar pages for FOMC events and speeches
- BLS monthly release schedule
- BEA release schedule

Events with only a date and no release time are returned with `incomplete_time=true` and zero default risk-window minutes.

Economic events include a neutral `enrichment` object. It may contain `forecast`, `previous`, `consensus`, and `actual` values from public calendar enrichment sources. Enrichment never replaces the official event `source` or official event time; if source times differ inside the matching tolerance, the response includes a warning.

DailyFX, ForexFactory, and Investing can block automated requests with HTTP `403` or `429`. When that happens, the service reports `provider_unavailable` and a warning such as `Structured providers unavailable: DailyFX 403, ForexFactory 403, Investing 429` instead of labeling the event as a simple match miss.

Optional local enrichment can be supplied in `data/manual_event_enrichment.json`:

```json
{
  "events": [
    {
      "country": "US",
      "date": "2026-07-14",
      "category": "CPI",
      "forecast": null,
      "previous": null,
      "consensus": null,
      "actual": null,
      "source": "manual",
      "source_url": "https://example.com/source",
      "reliability": 0.6
    }
  ]
}
```

OpenAI event enrichment is scaffolded and disabled by default. It requires both an API key and `AI_MARKET_ENABLE_OPENAI_EVENT_ENRICHMENT=true`; without both, no OpenAI request is made and no noisy provider error is emitted. The scaffold is data-only and must return only sourced values.

AI Researcher is a separate final fallback for missing persistent enrichment facts. It is disabled by default:

```env
AI_MARKET_ENABLE_AI_RESEARCHER=false
AI_MARKET_AI_RESEARCHER_MODE=codex_cli
AI_MARKET_AI_RESEARCHER_MAX_EVENTS=5
AI_MARKET_AI_RESEARCHER_ONLY_HIGH_IMPACT=true
AI_MARKET_AI_RESEARCHER_REQUIRE_SOURCE_URL=true
```

`codex_cli` mode is for personal development with a locally authenticated Codex CLI and file exchange under `AI_MARKET_CODEX_WORKSPACE_DIR`. It does not automate `chatgpt.com` and does not require an OpenAI API key. If the CLI is unavailable, the provider reports `provider_unavailable` and endpoints continue.

`openai_api` mode is scaffolded for production use with `AI_MARKET_OPENAI_API_KEY` or `OPENAI_API_KEY`, `AI_MARKET_OPENAI_RESEARCH_MODEL`, timeout, and temperature settings. It is skipped when no key is configured. Every accepted value must have a `source_url`; otherwise it is rejected rather than invented.

Browser scraping is also disabled by default:

```env
AI_MARKET_ENABLE_BROWSER_SCRAPING=false
AI_MARKET_BROWSER_SCRAPING_HEADLESS=true
AI_MARKET_BROWSER_SCRAPING_TIMEOUT_SECONDS=15
AI_MARKET_BROWSER_SCRAPING_MAX_PAGES=3
AI_MARKET_ENABLE_AGGRESSIVE_SCRAPING=false
```

When enabled, Playwright-based DailyFX, ForexFactory, and Investing providers try to read rendered calendar pages. If Playwright or a browser is unavailable, or a page shows captcha/access-denied/challenge content, the provider reports `provider_unavailable` and the endpoint continues. The service does not bypass captcha or use aggressive techniques.

Aggressive scraping is not a primary strategy. The service prefers official schedules, APIs, cache/DB facts, RSS/search snippets, and explicit source URLs because several calendar sites block automated reads or return unreliable values.

Event enrichment is limited by default to US high-impact macro categories and at most 10 events per request:

```env
AI_MARKET_ENRICH_ONLY_HIGH_IMPACT=true
AI_MARKET_ENRICHMENT_MAX_EVENTS=10
AI_MARKET_EVENT_ENRICHMENT_CACHE_TTL_HOURS=24
```

Targeted Search/RSS enrichment is enabled by default. It generates event-specific Google News RSS queries for high-impact US events and extracts only explicit patterns such as forecast, expected, consensus, previous, prior, and actual values. Every accepted value must have a source URL when `AI_MARKET_TARGETED_SEARCH_REQUIRE_SOURCE_URL=true`.

```env
AI_MARKET_ENABLE_TARGETED_SEARCH_ENRICHMENT=true
AI_MARKET_TARGETED_SEARCH_MAX_EVENTS=10
AI_MARKET_TARGETED_SEARCH_TIMEOUT_SECONDS=10
AI_MARKET_TARGETED_SEARCH_RECENCY_DAYS=30
AI_MARKET_TARGETED_SEARCH_REQUIRE_SOURCE_URL=true
```

Targeted search cache uses:

- `macro_event_enrichment:v3:targeted_search:{country}:{date}:{category}`

Event metadata uses neutral fields:

- `impact`
- `event_risk_level`
- `default_risk_window_before_minutes`
- `default_risk_window_after_minutes`
- `source`
- `reliability`
- `freshness`

AI-TRADER consumes these fields independently.

Nasdaq context endpoints provide external data only:

- QQQ holdings and Nasdaq-100 constituents
- Mega-cap quote snapshots for `NVDA, AAPL, MSFT, AMZN, META, GOOGL, GOOG, AVGO, TSLA, AMD, NFLX, COST`
- Numeric breadth aggregates based on snapshot changes and QQQ weights
- Mega-cap earnings calendar metadata
- Keyword-tagged macro and mega-cap news

`/market-context/mnq` includes event enrichment metadata with `provider_statuses`, provider counts, enriched/missing counts, `browser_scraping_enabled`, `browser_scraping_used`, `structured_sources_blocked`, `targeted_search_enabled`, `targeted_search_used`, `targeted_search_queries`, `targeted_search_matches`, `targeted_search_no_match_count`, and cache/manual fallback notes when enrichment was attempted.

`/market-context/mnq` also includes persistent enrichment metadata under `metadata.persistent_enrichment` and a flattened `metadata.data_quality` block with DB hits/misses, provider hits/failures, AI usage, missing critical fields, stale fields, warnings, and errors. AI-TRADER should consume this as sourced market data only and perform any analysis or decision in its own layer.

Useful smoke checks after starting the service on port `8010`:

```powershell
Invoke-RestMethod "http://127.0.0.1:8010/events/upcoming?country=US&days=30" | ConvertTo-Json -Depth 40
Invoke-RestMethod "http://127.0.0.1:8010/market-context/mnq" | ConvertTo-Json -Depth 40
```

When `FMP_API_KEY` or `AI_MARKET_FMP_API_KEY` is configured, Financial Modeling Prep stable earnings calendar is the primary source for the 14-day mega-cap earnings block; Alpha Vantage remains the controlled earnings fallback. Alpha Vantage can also supply QQQ holdings and news metadata. Mega-cap quote snapshots use Yahoo Finance Chart first to avoid consuming Alpha Vantage free-tier quote limits; Alpha Vantage `GLOBAL_QUOTE` is used only as a controlled fallback.

These endpoints do not compute chart levels, generate signals, or decide actions.

## Persistent semantic research runtime

The optional AI Researcher is asynchronous and fail-closed. It first probes the configured Codex CLI capability, then records the bounded phases `PLAN`, `SEARCH`, `OPEN_SOURCE`, `EXTRACT`, `CROSS_CHECK`, `VALIDATE`, `PERSIST`, `READ_BACK`, `MATERIALIZE`, and `COMPLETE`. Claims and short evidence are stored atomically; source tiers and independent confirmations are recalculated by the service. HTTP requests never wait for the researcher.

Every Codex invocation uses stdin for the prompt and an isolated command shape: `--search`, read-only sandbox, unique non-Git workspace, `--skip-git-repo-check`, `--ephemeral`, `--ignore-user-config`, `--ignore-rules`, JSONL events, a closed phase-specific output schema, deterministic `--output-last-message`, and `--color never`. Persisted login remains available, while personal config, MCP/plugin configuration, rules and repository instructions are not part of the research contract. The final-message file is authoritative; JSONL is used only for events, observed search/source activity, usage and runtime errors.

Failure handling is fail-closed:

- schema, CLI argument/config/auth, executable, output-contract and deterministic policy failures are non-retryable;
- only rate limits, watchdog timeouts, temporary network failures, backend 5xx responses and documented transient interruptions receive bounded retry;
- a non-retryable failure creates one failed attempt and atomically closes the job, run and active step;
- a scheduled retry sets both job and run to `RETRY_SCHEDULED`; acquisition returns both to `RUNNING`;
- startup migration/reconciliation repairs historical active runs whose linked job is already terminal.

`last_error` remains compact. `GET /ai-research/jobs/{job_id}` exposes redacted attempt diagnostics, and run endpoints expose redacted step diagnostics and per-step attempt history. Diagnostics include category, exit code, bounded stderr/JSONL tails, structured error events, command shape, CLI version, step, duration, workspace and timestamp. Prompts, environment contents, credentials, cookies, `auth.json` and user config are never persisted.

Official release actuals use explicit event semantics and transformations instead of publishing raw BLS/BEA levels. Unsupported mappings return visible `NO_DATA`. Automatic jobs are idempotent inside configurable run windows, while `force_requeue=true` creates an explicit post-terminal generation.

Operational endpoints:

- `GET /ai-research/capabilities`
- `POST /market-research/mnq/runs`
- `GET /market-research/mnq/runs/{run_id}`
- `GET /market-research/mnq/latest`
- `GET /market-research/mnq/status`
- `GET /market-research/mnq/evidence/{claim_id}`

The authorized smoke script polls both run and job. It fails immediately on an incompatible terminal state or an orphaned non-terminal record with an empty queue, and writes a compact `failure-report.json` even when PowerShell throws. A live smoke is never run automatically and requires explicit authorization.

See `docs/persistent-ai-and-temporal-architecture.md` for mappings, lifecycle, source policy, scheduler controls, readiness behavior, smoke and recovery procedures. No research endpoint supports trading or order submission.
News responses accept `recency_days` and filter articles by `published_at` when the upstream source provides a timestamp. If Alpha Vantage and GDELT are rate-limited, `/news/latest` falls back to RSS feeds and deduplicates articles by URL/title.

`/nasdaq/context` separates data-quality metadata into:

- `critical_errors`: failures that affect the final returned section
- `warnings`: non-critical data caveats
- `fallback_notes`: failed upstream attempts when a fallback produced final data

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest
```
