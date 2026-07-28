# PR28 force-generation forensic closure and R4 LIVE acceptance

Status: **R4 LIVE PRODUCER PATH ACCEPTED**

This correction is limited to the two R3 blockers:
`STALE_NO_DATA_NOT_RECLAIMED` and
`COVERAGE_WRITE_ESCAPES_FAILED_FORCE_TRANSACTION`. The R3 sandbox and HTTP
artifact were inspected read-only. No operational database, live provider, AI
backend, browser, delivery, trading, order, migration, Uvicorn process or
`.env` file was used while implementing or verifying the correction offline.
The later controlled R4 acceptance artifacts record the authorized live
provider validation described below.

## Read-only R3 evidence

The captured route returned HTTP 500 after
`deterministic_provider_result_not_fresh`. Snapshot revision 97, outbox 3, AI
jobs 48 and backend invocations 41 did not change, but canonical coverage grew
from 8 to 71 rows. The lifecycle for
`xtb:146392:2026-07-24` existed: it was an actual-missing,
`AWAITING_ACTUAL`/`SUPERSEDED` row whose validity had expired. It was not a
missing lifecycle.

The redacted R3 evidence used by the route tests is
`tests/fixtures/pr28_r3_force_generation_redacted.json`.

## Root causes

The provider-force preparation path only recognized an active
`NO_DATA_BACKOFF`. Every other actual-missing lifecycle was passed directly to
the generic due resolver. A newly returned historical official actual then
failed the generic expiring-datum TTL check, and the resulting `NO_DATA`
envelope had no lifecycle. The caller incorrectly raised
`provider_force_actual_lifecycle_missing`.

Schedule catch-up used `materialize_snapshot=False`, but still wrote coverage,
canonical occurrences and projected lifecycle rows directly. A later actual
or snapshot failure therefore left those earlier writes visible.

## Corrected lifecycle state machine

Provider-force eligibility now distinguishes:

- truly missing lifecycle: initialize through the same provider-first plan;
- fresh `NO_DATA`: skip without provider or canonical write;
- future negative-cache/backoff: skip and preserve `next_retry_at`;
- stale actual-missing occurrence: reclaim as due;
- terminal `EXHAUSTED_NO_DATA`: stop without a retry loop;
- future/unpublished occurrence: never reclaim early.

The deterministic resolver now returns a typed lifecycle for admitted
`NO_DATA`, temporary failures and exhausted results. A newly resolved official
macro actual is treated as a completed historical occurrence rather than an
expiring quote, so its old release timestamp cannot fail the generic TTL gate.
The live-equivalent New Home Sales path reaches FRED `HSN1F` and preserves the
XTB occurrence identity.

A process-local plus OS file lease serializes the complete force generation
across threads and application processes. Waiters resume after the claimant
releases the lease and observe the committed fixed point; they do not invoke
the official actual resolver again. The lease is technical state outside the
canonical database and is automatically released by the OS on process exit.

## Atomic force generation

Before network discovery, SQLite backup creates a private generation database.
All schedule-provider work, coverage proof and discovery writes occur there.
No SQLite write transaction remains open during provider I/O. The staging
database is removed after a whitelisted delta has been extracted.

Only these staged canonical tables can be published:

- `event_calendar_coverage`;
- `economic_events_history`;
- non-actual `datum_lifecycle_items`.

Official `macro_actual` lifecycle remains owned by the actual reconciliation
plan and cannot be overwritten by generic schedule projection. After discovery,
actual resolution, reconciliation and payload validation complete, the staged
delta, exact-occurrence reconciliation, lifecycle, snapshot, sync sections and
outbox are committed by the existing short `BEGIN IMMEDIATE` snapshot
transaction. Any exception before or during that transaction leaves no
canonical generation visible.

Technical telemetry records reclaimed, skipped, exhausted, provider-success,
provider-unavailable, committed, no-op and aborted decisions. Telemetry failure
cannot change the HTTP or canonical result.

## Route-level proof

The tests use the real `app.main` lifespan, production
`build_application_state`, dependency graph and route:

`GET /market-context/mnq?refresh=force&view=debug`

Only provider HTTP transports are controlled. The proof covers:

- stale `NO_DATA` reclaimed, one provider resolver invocation, `HSN1F`
  selected, one lifecycle and one finalization;
- fresh `NO_DATA`, future backoff and `EXHAUSTED_NO_DATA` as write-free fixed
  points;
- two concurrent force requests with one claimant and waiter fan-out;
- successful publication of 21 distinct coverage dates with zero duplicate
  logical keys;
- injected failure after coverage preparation and before reconciliation;
- injected failure after actual reconciliation and before finalization;
- exact before/after equality for coverage, canonical occurrences, lifecycle,
  snapshot, outbox, revision and AI jobs at both rollback points;
- S&P HTTP 403 returned as HTTP 200 degraded, with PMI actual null,
  `2026-07`, 51.5 forecast, 51.2 previous and an explicit reason code;
- controlled FRED result delivering New Home Sales 628.0 / 610.0 / 618.0 /
  `2026-06`, unchanged XTB ID, FRED `HSN1F` lineage and `RELEASED`.

Focused provider-force, route, lifecycle, coverage and observability
regression: **110 passed**. Schema-22 and migration-focused verification:
**44 passed**. The final complete repository suite passes with **1,828 passed**.
Ruff, `py_compile`, `compileall` and `git diff --check` pass.

## Offline replay

Two independent offline replay executions are byte-identical:

- summary: 3,609 bytes, SHA-256
  `AABC0E264C3571EFAC74975A602C479CE0588E8B8EA9B78DAE55EBCAD0E1A1D9`;
- 17-section full-sync: 465,269 bytes, SHA-256
  `EFBC4A28E61ADC5A955D414E9E00A88D9855A1FB63BF4A828AD4A783068E4A67`.

The replay reports zero provider-live, AI/backend, browser, delivery,
operational-database and trading side effects, plus a byte-identical,
write-free fixed point.

## R4 controlled LIVE acceptance

The R4 sandbox and the final acceptance artifacts were inspected read-only:

- `data/pr28-r4-live-sandbox-20260728-151525`;
- `data/pr28-r4-live-sandbox-20260728-151525/r4-live-rerun-20260728T135518Z`;
- `data/pr28-r4-live-sandbox-20260728-151525/final-readonly-acceptance-20260728T140151Z`.

The acceptance result is `PASS`, with zero recorded errors.

### Provider-force reconciliation

The stale `NO_DATA` lifecycle for
`xtb:146392:2026-07-24` was classified `RECLAIMABLE` with reason
`STALE_NO_DATA_RETRY_DUE`. FRED was invoked exactly once and returned
successfully through official series `HSN1F`. The published occurrence
preserves its XTB identity and contains:

- actual 628;
- forecast 610;
- previous 618;
- reference period `2026-06`;
- lifecycle/release status `PUBLISHED`;
- official actual publisher `FRED` and adapter `FRED_OFFICIAL_API`.

For `xtb:146945:2026-07-24`, S&P Global returned HTTP 403. The producer
returned HTTP 200 with typed degraded state rather than inventing data:

- actual remains null;
- forecast remains 51.5;
- previous remains 51.2;
- reference period remains `2026-07`;
- resolution status is `PROVIDER_UNAVAILABLE`;
- reason code is `sp_global_public_release_access_restricted`;
- the result is retryable and fail-closed.

### Atomic publication and accounting

Exactly one canonical finalization was observed:

- snapshot revision/count: 97 to 98;
- outbox: 3 to 4;
- coverage rows: 8 to 71, representing 21 distinct dates;
- AI jobs: unchanged at 48;
- backend invocations: unchanged at 41;
- SQLite integrity: `ok`;
- persistence outcome: `ATOMIC_COMMIT_WITH_SNAPSHOT`.

The operational database remained unchanged. The failed-force atomicity guard
passed, the queue was empty and no partial canonical generation was exposed.

### Read-only full-sync acceptance

Two independent reads of snapshot revision 98 produced byte-identical,
checksum-verified full-sync payloads:

- 17 sections;
- 3,633,738 bytes;
- SHA-256
  `CAFDAA4E60E86988F1E8BB4DAC90AE3E87680791CECFB241B22C489C14272549`;
- no 90 KB ceiling, truncation or lossful payload reduction;
- zero database writes caused by either read.

Readiness remains explicit and must not be presented as complete:

- trading context: `PARTIAL`;
- macro analysis: `PARTIAL`;
- event-risk analysis: `PARTIAL`;
- news analysis: `UNAVAILABLE`;
- market schedule: `PARTIAL`.

### Final safety gate and residual limitations

At final verification, port 8053 was free, process 4892 was not running, the
operational database SHA-256 remained
`69514FAC4DC680BF5C0AC7FE278C76FDBB656625374A40201A3C10CA4669FA83`,
and the intentional untracked consumer artifact remained
`BCED28DECDF98D65AF9843C3CF3FF23DAB0A164C721B8BEF9B3E0D7697699DD4`.
The PR28 merge gate is therefore open.

Both R3 software blockers are closed and the PR28 producer path is validated
LIVE. This does not make the whole system production-ready. S&P Global public
release access remains externally restricted by HTTP 403, and news analysis
is unavailable in the accepted snapshot; both limitations remain visible as
degradation. Consumer integration with AI Trader and a real shadow
end-to-end run remain the next step.
