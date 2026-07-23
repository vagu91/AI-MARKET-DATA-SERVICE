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

The worker is independent from the APScheduler switch. HTTP always returns without waiting for Codex. Expired `RUNNING` leases close their active attempt as `ABANDONED`; jobs with attempts remaining become `RETRY_SCHEDULED`, while exhausted jobs become `FAILED`. The process watchdog is separate from HTTP timeouts and kills the entire process group at the worker runtime limit or bounded application shutdown.

## Storage

Migration 7 adds `ai_research_jobs`, `ai_research_job_attempts`, `market_context_snapshots`, and `event_value_candidates`, plus lineage/lifecycle columns on existing event, fact, and news tables. Migration 8 adds job generation/scope/snapshot linkage and the snapshot-job join table. Migration 9 adds semantic-actual columns plus capability reports, research runs/steps, atomic claims/evidence and scheduler decisions. Migration 10 adds persistent official-feed retry deadlines, observed tool events, evidence content hashes, server-computed topic completeness and usage/cost fields. Migration 11 adds redacted job/attempt/step diagnostics and `research_step_attempts`, preserving every retry of a failed step. All migrations are additive and preserve schema-6/schema-8/schema-9/schema-10 data.

Candidate actuals retain `event_metric_id`, `source_series_id`, transformation, SA/NSA variant, frequency, unit, reference period, release vintage, observation lineage and official canonical URL. Raw macro levels are never promoted directly to event actuals. Surprise is calculated only when metric, period, frequency, unit and seasonal adjustment match the forecast/consensus baseline; incompatibility is persisted as a structured warning and remains fail-closed for full analysis.

Supported semantic mappings are headline/core CPI MoM/YoY, headline PPI MoM/YoY, NFP monthly delta, unemployment level, average-hourly-earnings MoM/YoY, GDP annualized QoQ and YoY, headline/core PCE price-index MoM/YoY, and personal-income/spending MoM. Core PPI and initial claims remain explicit `NO_DATA` because no demonstrated official adapter/series mapping exists in this service.

Research claims and evidence are persisted separately. Evidence is verified only against an observed `OPEN_SOURCE` event (or deterministic server-side HTTP verification with final URL, timestamp, status, content hash and normalized text match). Source domain/tier/classification and independent confirmations are recalculated from canonical URLs and stored evidence; model-declared counts or tiers are ignored. Syndicated identical content is one independent source group. Required-topic coverage and missing topics are computed server-side. Accepted, persisted, projected and read-back counts must agree before `SUCCEEDED`; mixed outcomes are `PARTIAL`.

Snapshot revisions are allocated under the same `BEGIN IMMEDIATE` transaction that inserts the immutable snapshot. Worker revisions reconstruct the event calendar from canonical database rows and never mutate a previous payload recursively. AI status is computed from the current event set or explicit snapshot-job links, not from unrelated historical jobs.

Idempotency deduplicates active work by stable scope and allows only one automatic generation per configurable domain run window. A terminal job is not automatically requeued inside the same window; the next window creates a new immutable generation. Explicit `force_requeue=true` creates a unique generation after terminal completion but still does not duplicate an already-active scope.

## Capability and scheduler

`GET /ai-research/capabilities` is an offline probe. It checks command resolution, restricted environment compatibility, version/login, required global and `exec` options, all local output schemas, isolated command construction, workspace, source policy and worker state. It never calls a model. Results distinguish `NOT_CONFIGURED`, `AUTH_UNAVAILABLE`, `SCHEMA_INVALID`, `EXECUTOR_UNAVAILABLE`, `WEB_UNAVAILABLE`, `DEGRADED`, `READY_TO_SMOKE` and `LIVE_VERIFIED`. `READY_TO_SMOKE` means only that an explicitly authorized live proof may be attempted; it is not live-web verification. Only a persisted successful smoke can produce `LIVE_VERIFIED` and `web_search_available=true`. The worker leaves ordinary AI jobs pending until that proof exists; deterministic official-actual jobs remain eligible. Keep the research scheduler disabled until the smoke succeeds.

## Codex execution and retry contract

The prompt is supplied only on stdin. The command always requests web search, a read-only sandbox, an isolated non-Git working directory, ephemeral session state, ignored user config/rules, JSONL events, a locally validated closed schema, a deterministic final-message file and no color. Persisted authentication remains available. Personal MCP/plugin configuration and rules are outside the runtime contract. Capability and executor preflight reject a workspace with an `AGENTS.md` anywhere in its ancestor chain, so repository/user instructions cannot silently enter the research session.

The final-message file is the primary structured payload. JSONL contributes only event telemetry, observed searches/source opens, usage and `error`/`turn.failed` diagnostics. Every object schema is closed, every array has defined bounded items, nullable fields are explicit and the AI contract cannot produce numeric `actual` values. Source tiers, confirmation counts and verification remain server-computed.

Each run persists one effective budget as the source of truth for prompts, dynamic
schemas, execution, recovery, capability checks, telemetry and failure reports.
It combines per-run search/open limits, remaining daily capacity and the runtime
deadline. The MNQ profile retains the default of eight searches for ten required
topics by grouping compatible topics into no more than eight planned queries;
increasing an environment value is not required for structural compatibility.
Every phase receives numeric limits, plus already completed queries and opened
URLs on recovery.

Observed JSONL tool events are authoritative usage. Each bounded, redacted
envelope preserves raw event type, lifecycle, item identity/type, phase, provider
tool type, semantic action, query or URL, fingerprint, status and available
usage. `started`, `completed` and replayed events for one action share a
fingerprint; only one terminal completed action consumes usage. Empty lifecycle
events remain observable but do not increment counters. A URL-only `web_search`
is an `open_source` action during `OPEN_SOURCE` and an open/verify action during
`CROSS_CHECK`, rather than an economic search.

`AI_MARKET_RESEARCH_BUDGET_MODE=observe` is the initial default. Per-run and daily
search/open values are telemetry thresholds: overshoot records a compact warning
and execution continues while it produces new sources, claims or phase progress.
`enforce` retains non-retryable `BUDGET_EXCEEDED`, but applies it only to
deduplicated terminal actions. The independent emergency tool-action ceiling,
per-call watchdog, cancellation, heartbeat and process-group termination remain
active in both modes.

The progress guard detects repeated normalized queries without new URLs, reopened
URLs without new evidence, cyclic fingerprint sequences, configurable
no-progress windows and the emergency ceiling. It emits non-retryable
`LOOP_DETECTED`; many searches that continue discovering new sources are not a
loop. All thresholds are configurable in `.env.example`.

At the end of a productive operational window, the run stores a checkpoint and
the worker schedules a technical continuation instead of reporting a timeout.
Completed phases and tool fingerprints are reused on resume, the same logical
job/run is retained, and the continuation does not consume a new daily run.
Single-call watchdogs and an unproductive overall deadline still fail closed.

Model-declared `OPENED`, evidence availability, HTTP status and content hashes are
retained only as declarations. Runtime reconciliation records separate observed
and verified states; only terminal tool observations, deterministic server
verification, or valid cached lineage can establish them. Claims referencing
unobserved or unverified evidence are rejected. Source domains are derived from
policy-valid observed canonical URLs.

Per-run metrics include raw/normalized/deduplicated action counts, token fields,
searches/opens/new sources, extracted/accepted/rejected claims, phase durations,
tokens and cost per accepted claim, searches per new source, warnings, loops and
continuations. Monetary cost is `cost_unavailable` unless the runtime actually
provides it.

| Failure category | Retry |
| --- | --- |
| invalid schema, unsupported argument, invalid config, missing auth/executable, incompatible output contract, deterministic policy rejection | never |
| `BUDGET_EXCEEDED` contract violation | never |
| `LOOP_DETECTED` with bounded fingerprint evidence | never |
| rate limit, watchdog timeout, temporary network failure, backend 5xx, documented transient interruption | bounded backoff |
| unknown/opaque CLI exit | never (fail closed) |

The compact job error contains only a stable error code. Redacted diagnostics retain category, exit code, bounded stderr/stdout tails, structured error events, safe command shape, CLI version, step, duration, workspace and timestamp. Prompts, secrets, environment contents, cookies, auth files and personal config are not stored.

Optional pre-market, in-session, post-market, pre-event, post-release, speech, earnings, news and temporary-source retry triggers are disabled unless the general and research schedulers are enabled. Each trigger persists its input fingerprint and returns `NOT_REQUIRED` when inputs have not changed, the run window already ran, concurrency is full or the daily run budget is exhausted. Pre-event and post-release triggers enqueue event-scoped missing-field/official-actual work from the persisted snapshot rather than a generic timer-only job.

## Lifecycle

| Domain | Before occurrence | After occurrence, missing result | Complete |
| --- | --- | --- | --- |
| Numeric release | `PRE_RELEASE` (actual forced null) | `AWAITING_ACTUAL`, persistent retries 30/120/300/900/1800/3600s | `RELEASED`; forecast/consensus/previous preserved; surprise calculated |
| Speech/testimony | `PRE_RELEASE` | `AWAITING_OUTCOME` | `COMPLETED` with sourced outcome/transcript |
| Earnings | upcoming | removed from upcoming | released EPS/revenue and surprises where available |
| News | current until `valid_until` | expired/historical, excluded from current drivers | retained until retention cleanup |

Job and run lifecycle is synchronized transactionally:

| Job transition | Run transition | Step behavior |
| --- | --- | --- |
| acquired | `RUNNING` | failed step can start a new numbered attempt |
| productive window checkpoint | `RETRY_SCHEDULED` | completed steps and deduplicated tool events are retained |
| transient failure | `RETRY_SCHEDULED` | current attempt remains terminal and queryable |
| `SUCCEEDED`, `PARTIAL`, `NO_DATA` | same terminal status, `completed_at` set | completed history retained |
| `LOOP_DETECTED` | same terminal status, `completed_at` set | bounded loop evidence retained |
| `FAILED`, `TIMED_OUT`, `CANCELLED`, `REJECTED` | same terminal status, `completed_at` set | diagnostic and blocking gaps retained |

Migration/startup reconciliation is idempotent. A terminal job linked to a `PENDING`, `RUNNING` or `RETRY_SCHEDULED` run closes that run without deleting history. An active run linked to a retry-scheduled job becomes `RETRY_SCHEDULED`.

## Operational API

- `GET /ai-research/jobs/latest?view=full|compact` returns an array ordered
  newest-first (`created_at DESC`, then insertion order DESC). The backward-compatible
  `full` view contains request/result payloads; use `compact` for operational
  diagnostics without those payloads.
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

The compact schema-2.1 AI-TRADER consumer exposes research status, coverage, topic/gap counts, verified driver references, evidence IDs and freshness only. Prompts, raw Codex output, complete documents and reasoning are not exposed. Redacted bounded diagnostics remain on job/run audit APIs. Existing `refresh=false` snapshots are returned without calls, enqueue or writes.

## Authorized future smoke test

Do not run this during development. After explicit live authorization and service startup:

```powershell
.\scripts\smoke_test_market_research.ps1 -BaseUrl http://127.0.0.1:8000 -TimeoutSeconds 600
```

The script probes capability, queues exactly one MNQ research run, and polls both the run and linked job. It fails immediately if either becomes incompatibly terminal or if the queue is empty with no owner and the latest attempt is terminal. `failure-report.json` is written even when the script throws and contains only compact status/diagnostic fields. On success it records snapshot revision and SHA-256 artifact hashes. It never calls trading or AI-TRADER endpoints.

Recovery is non-destructive: disable the AI worker and research scheduler, inspect the job and run APIs, then restart the service so migration reconciliation can repair historical lifecycle drift. Do not delete or recreate SQLite. A subsequent single live smoke requires fresh explicit authorization; local tests and `READY_TO_SMOKE` do not prove the live issue resolved.
