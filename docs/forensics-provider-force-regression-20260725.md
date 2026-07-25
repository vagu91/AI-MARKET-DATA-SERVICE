# Provider-force regression forensic report — 2026-07-25

## Scope and safety

This repair was developed and validated exclusively with fixtures, mocks,
temporary SQLite databases, and offline replay. No provider, browser, Codex
CLI, OpenAI, Uvicorn, operational database, account/order/execution endpoint,
or trading system was called. No order was placed. The local `.env` and the
untracked `ai-trader-consumer-payload.json` were not modified or printed.

No schema migration was added; schema 20 remains current.

## Root causes

The `500` originated in `build_ai_trader_consumer_v2` after the live provider
projection serialized above 90,000 UTF-8 bytes. The snapshot repository began
its transaction before consumer construction, so final projection failure
occurred too late in the materialization lifecycle. Provider cache,
observation, event-fact, and enrichment-run writes had already committed
separately; no snapshot/component/outbox insert had committed.

The five AI calls originated in `EnrichmentOrchestrator.enrich_events`:
`DiagnosticsService.full_model` translated `refresh=force` to `force=True`,
passed it to `enqueue_missing_events`, and the queue service generated a new
`force-<uuid>` generation for every candidate. This bypassed run-window
idempotency and created five `MISSING_EVENT_RESEARCH` jobs. The on-demand
diagnostics route did not use the research scheduler, so
`AI_MARKET_RESEARCH_SCHEDULER_ENABLED=false` could not prevent it.

AI authority was previously represented only by feature/agent switches.
Neither the request’s authority nor its origin was persisted in the job, and
the worker checked agent enablement but not request authorization immediately
before backend execution.

Every terminal job was passed to
`DBOnlyMarketContextMaterializer.materialize_for_job` even when its result was
`NO_DATA` with zero accepted/persisted claims. Consequently each child rebuilt
an equivalent snapshot.

## Incident facts preserved

Five real invocations consumed 293,603 input, 6,076 output, 299,679 total, and
136,448 cached tokens. All ended `NO_DATA`.

| Revision | Snapshot |
|---:|---|
| 86 | `mcs-e8969da5-93d1-47eb-8980-db2045cd27d9` |
| 87 | `mcs-5d20bb45-83a9-4fe2-81a5-854bd7bd6eb1` |
| 88 | `mcs-d8d62f83-1c2f-4640-9e94-e90a92ee44a7` |
| 89 | `mcs-bec58c91-8405-46a5-9668-29fde9edc4c8` |
| 90 | `mcs-faa3aff2-17e5-4578-b0c0-7066a1e0aecc` |

They have `refresh_mode=worker_db_only_materialization`,
`ai_status=NO_DATA`, and initially `audit_status=ACTIVE`.

## Before/after offline replay

The redacted structural fixture is
`tests/fixtures/provider_force_failure_20260725_redacted.json`. The replay
reconstructs the pre-compaction projection by disabling only the final
enforcer inside the offline process, then runs the corrected projection.

| Measure | Reconstructed before | After |
|---|---:|---:|
| canonical consumer bytes | 179,344 | 31,775 |
| AI jobs | forensic 5 | 0 |
| research runs | forensic 5 | 0 |
| backend invocations | forensic 5 | 0 |
| snapshots | forensic 5 `NO_DATA` | 1 coherent provider snapshot |
| market session | weekend | weekend |

Largest reconstructed top-level sections were
`options_positioning` 123,961 bytes, `lifecycle` 19,917,
`market_schedule` 5,108, `rates_context` 4,289, `macro` 4,176,
`risk` 3,423, `sentiment` 3,174, `event_risk` 2,758,
`news` 1,638, `rates` 1,492, and `nasdaq` 1,452. All section sizes
are emitted by the replay and the consumer `compaction.sections` telemetry.
The corrected option section is 363 bytes and contains aggregates, freshness,
and compact lineage, with no individual contracts.

Run:

```powershell
python -m scripts.replay_provider_force_regression_offline
```

## Corrected invariants

- Provider refresh routes construct an immutable provider-only execution
  context. `force` cannot change `allow_ai`.
- `RELEASE_ACTUAL_REFRESH` is a deterministic resolver/provider job, not an AI
  job. A structurally valid context with `allow_live_providers=true` is
  sufficient; AI agent flags and `allow_ai` are irrelevant to this path.
  Acquisition, recovery, and the worker enforce the same split.
- Actual refresh emits `resolver_evaluation` and `provider_request_*`
  telemetry only. It creates no research backend invocation, emits no
  `ai_invocation_*` event, and records zero AI tokens. A provider failure uses
  the configured deterministic official-actual backoff and cannot implicitly
  fall through to Codex/OpenAI. Any later residual AI research must be a new,
  explicitly authorized job.
- Only trusted API, configured scheduler, and configured recovery entrypoints
  construct AI authority. Queue services, coordinators, due scanners, startup
  catch-up, and scheduler evaluation receive it explicitly and never infer it
  from `force` or an `ai_enqueue` callback.
- The synthetic `test` request origin can authorize AI only when
  `settings.environment == "test"`. A persisted `allow_ai=true`,
  `request_origin=test` payload is rejected before acquisition in every other
  environment.
- Execution-context payloads require all four fields, strict booleans, a
  non-empty correlation ID, and a whitelisted origin. Missing, incomplete,
  `allow_ai=false`, or unknown-origin contexts emit `AI_SUPPRESSED` and create
  no job, run, invocation, or token usage.
- Worker acquisition terminalizes unauthorized legacy or malformed jobs as
  idempotent `REJECTED` / `AI_NOT_AUTHORIZED`, with a terminal timestamp and
  structured non-retryable diagnostic. They are not merely hidden from the
  acquisition query and cannot create attempts, snapshots, or outbox events.
- Explicitly authorized residual research remains idempotent and bounded.
- Consumer sanitization is recursive and unconditional before section budgets:
  forbidden raw structures, headers, authorization fields, API keys, tokens,
  and bearer values are removed or redacted even in small under-budget
  sections. Safe provider lineage, freshness, `as_of`, quality, and `NO_DATA`
  reasons remain.
- Projection/schema/size/source/temporal validation completes before
  `BEGIN IMMEDIATE`; snapshot, components, links, lifecycle, and outbox then
  commit or roll back together.
- `NO_DATA` with no accepted/persisted claims produces no snapshot. Parent
  batches materialize at most once and only for a meaningful non-`NO_DATA`
  outcome.

## Validation results

- complete suite: `1539 passed`;
- focused PR-review blocker suite: `59 passed`;
- selected provider/actual/recovery/worker/telemetry suites: `138 passed`;
- migration matrix `1 -> 20` and schema-20 reopen: passed;
- Ruff: passed;
- `py_compile` and `compileall`: passed;
- `git diff --check`: passed;
- Windows PowerShell parser: all four repository `.ps1` scripts valid;
- offline replay: 179,344 bytes before, 31,775 after, zero live calls.

## Historical reconciliation

`scripts/reconcile_unauthorized_no_data_snapshots.py` is read-only by default.
It recognizes exactly revisions 86–90 plus their `MISSING_EVENT_RESEARCH`
jobs and `NO_DATA` runs. Apply requires an exclusive lock (closed operational
database), a byte-identical backup, and an audit JSON path:
Apply also probes the configured service host/port and aborts if a listener is
present, before opening the database for writes.

```powershell
python -m scripts.reconcile_unauthorized_no_data_snapshots `
  --database C:\path\market.sqlite

python -m scripts.reconcile_unauthorized_no_data_snapshots `
  --database C:\path\market.sqlite `
  --apply `
  --backup C:\path\market.pre-reconcile.sqlite `
  --audit-output C:\path\reconcile-audit.json `
  --service-host 127.0.0.1 `
  --service-port 8053
```

Apply changes only the five snapshot `audit_status` values from `ACTIVE` to
the existing semantic state `ORPHANED`. It deletes nothing and does not alter
jobs, runs, token usage, or telemetry. Repeated apply is idempotent. The
expected-state guard aborts on any identity, revision, status, job, or run
mismatch. The canonical default port is `8053`;
`AI_MARKET_SERVICE_HOST`/`AI_MARKET_SERVICE_PORT` and explicit CLI arguments
remain supported overrides. A listener belonging to AI-TRADER on `8000` does
not affect this guard. Dry-run opens SQLite read-only and does not require the
service to be stopped.

## Future single live-smoke checklist

This task did not run a live smoke. A separately authorized future smoke should:

1. close/backup the operational DB and run reconciliation dry-run first;
2. confirm provider and AI switches plus the intended provider-only context;
3. issue one `refresh=force` request;
4. verify zero new AI jobs/runs/invocations/token usage;
5. verify one snapshot, canonical consumer below 90,000 bytes, and section
   telemetry;
6. verify weekend/holiday semantics and last-known-good timestamps;
7. stop immediately on any unexpected enqueue, network target, or revision.
