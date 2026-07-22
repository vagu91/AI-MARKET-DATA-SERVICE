# Persistent AI research and temporal market-context architecture

## Request flow

```text
HTTP refresh=false -> existing latest snapshot -> exact consumer/debug response, zero writes
                   -> no snapshot yet -> one DB/cache-only cold materialization, then persist revision 1
HTTP auto/force   -> DB read -> deterministic providers as required -> validate
                  -> persist + read-back -> canonical event reconciliation
                  -> enqueue idempotent missing-field jobs -> materialize snapshot revision
                  -> immediate consumer/debug response (never waits for AI)

independent worker -> capability gate -> atomic job lease -> heartbeat -> unique job workspace
                   -> semantic official-actual pipeline OR persistent agentic runtime
                   -> PLAN -> SEARCH -> OPEN_SOURCE -> EXTRACT -> CROSS_CHECK -> VALIDATE
                   -> PERSIST -> READ_BACK -> MATERIALIZE -> COMPLETE
                   -> terminal job state -> DB-only snapshot revision
```

The worker is independent from the APScheduler switch. Expired `RUNNING` leases close their active attempt as `ABANDONED`; jobs with attempts remaining become `RETRY_SCHEDULED`, while exhausted jobs become `FAILED`. The process watchdog is separate from HTTP timeouts and kills the entire process group at the worker runtime limit or bounded application shutdown.

## Storage

Migration 7 adds `ai_research_jobs`, `ai_research_job_attempts`, `market_context_snapshots`, and `event_value_candidates`, plus lineage/lifecycle columns on existing event, fact, and news tables. Migration 8 adds job generation/scope/snapshot linkage and the snapshot-job join table. Migration 9 adds semantic-actual columns plus capability reports, research runs/steps, atomic claims/evidence and scheduler decisions. Migration 10 adds persistent official-feed retry deadlines, observed tool events, evidence content hashes, server-computed topic completeness and usage/cost fields. All migrations are additive and preserve schema-6/schema-8/schema-9 data.

Candidate actuals retain `event_metric_id`, `source_series_id`, transformation, SA/NSA variant, frequency, unit, reference period, release vintage, observation lineage and official canonical URL. Raw macro levels are never promoted directly to event actuals. Surprise is calculated only when metric, period, frequency, unit and seasonal adjustment match the forecast/consensus baseline; incompatibility is persisted as a structured warning and remains fail-closed for full analysis.

Supported semantic mappings are headline/core CPI MoM/YoY, headline PPI MoM/YoY, NFP monthly delta, unemployment level, average-hourly-earnings MoM/YoY, GDP annualized QoQ and YoY, headline/core PCE price-index MoM/YoY, and personal-income/spending MoM. Core PPI and initial claims remain explicit `NO_DATA` because no demonstrated official adapter/series mapping exists in this service.

Research claims and evidence are persisted separately. Evidence is verified only against an observed `OPEN_SOURCE` event (or deterministic server-side HTTP verification with final URL, timestamp, status, content hash and normalized text match). Source domain/tier/classification and independent confirmations are recalculated from canonical URLs and stored evidence; model-declared counts or tiers are ignored. Syndicated identical content is one independent source group. Required-topic coverage and missing topics are computed server-side. Accepted, persisted, projected and read-back counts must agree before `SUCCEEDED`; mixed outcomes are `PARTIAL`.

Snapshot revisions are allocated under the same `BEGIN IMMEDIATE` transaction that inserts the immutable snapshot. Worker revisions reconstruct the event calendar from canonical database rows and never mutate a previous payload recursively. AI status is computed from the current event set or explicit snapshot-job links, not from unrelated historical jobs.

Idempotency deduplicates active work by stable scope and allows only one automatic generation per configurable domain run window. A terminal job is not automatically requeued inside the same window; the next window creates a new immutable generation. Explicit `force_requeue=true` creates a unique generation after terminal completion but still does not duplicate an already-active scope.

## Capability and scheduler

`GET /ai-research/capabilities` checks configuration, executable/version, login status, `exec`, `--search`, structured output, workspace write access, process-group watchdog, source policy and worker state. `CONFIGURED` means the static prerequisites are present, `READY_TO_SMOKE` means a separately authorized live proof is still required, and only a persisted successful proof can produce `LIVE_VERIFIED` and `web_search_available=true`. The worker leaves ordinary AI jobs pending until that proof exists; deterministic official-actual jobs remain eligible. The boolean web setting or CLI flag support alone is never treated as proof of availability.

Optional pre-market, in-session, post-market, pre-event, post-release, speech, earnings, news and temporary-source retry triggers are disabled unless the general and research schedulers are enabled. Each trigger persists its input fingerprint and returns `NOT_REQUIRED` when inputs have not changed, the run window already ran, concurrency is full or the daily run budget is exhausted. Pre-event and post-release triggers enqueue event-scoped missing-field/official-actual work from the persisted snapshot rather than a generic timer-only job.

## Lifecycle

| Domain | Before occurrence | After occurrence, missing result | Complete |
| --- | --- | --- | --- |
| Numeric release | `PRE_RELEASE` (actual forced null) | `AWAITING_ACTUAL`, persistent retries 30/120/300/900/1800/3600s | `RELEASED`; forecast/consensus/previous preserved; surprise calculated |
| Speech/testimony | `PRE_RELEASE` | `AWAITING_OUTCOME` | `COMPLETED` with sourced outcome/transcript |
| Earnings | upcoming | removed from upcoming | released EPS/revenue and surprises where available |
| News | current until `valid_until` | expired/historical, excluded from current drivers | retained until retention cleanup |

## Operational API

- `GET /ai-research/jobs/latest`
- `GET /ai-research/jobs/{job_id}`
- `GET /ai-research/status`
- `GET /ai-research/capabilities`
- `POST /ai-research/jobs` for explicit idempotent data-only research (`force_requeue=true` for a new terminal generation)
- `POST /market-research/mnq/runs` (always asynchronous, HTTP 202)
- `GET /market-research/mnq/runs/{run_id}`
- `GET /market-research/mnq/latest`
- `GET /market-research/mnq/status`
- `GET /market-research/mnq/evidence/{claim_id}`

No endpoint supports trade decisions or order submission. Job payloads returned by status endpoints contain structured requests/results but no environment secrets. Keep `AI_MARKET_AI_RESEARCH_WEB_ACCESS_ENABLED=false` unless web availability has been explicitly verified.

The compact schema-2.1 AI-TRADER consumer exposes research status, coverage, topic/gap counts, verified driver references, evidence IDs and freshness only. Prompts, raw Codex output, complete documents, detailed source attempts and reasoning remain in audit storage. Existing `refresh=false` snapshots are returned without calls, enqueue or writes.

## Authorized future smoke test

Do not run this during development. After explicit live authorization and service startup:

```powershell
.\scripts\smoke_test_market_research.ps1 -BaseUrl http://127.0.0.1:8000 -TimeoutSeconds 600
```

The script probes capability, queues exactly one MNQ research run, polls with a deadline, records snapshot revision and SHA-256 artifact hashes, and never calls trading or AI-TRADER endpoints.
