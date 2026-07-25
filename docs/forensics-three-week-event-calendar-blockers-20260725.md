# PR 22 forensic blocker closure

## Scope and invariants

This report covers the review of PR #22 at initial HEAD
`67e278b3f2a5c7567c1a868415eac27750242466`, based on
`b5be6ea63ad82337b3186bb8bf414318983c1a0b`.

The work remained offline. It did not call providers, OpenAI, Codex CLI,
browsers, delivery endpoints, trading systems, or an operational database.
AI-TRADER, `.env`, and `ai-trader-consumer-payload.json` were not modified.

## Findings and closure

### 1. Occurrence lifecycle typing

Cause: `_persist_projected_lifecycle` passed every visible occurrence to
`compute_datum_lifecycle` as `macro_actual`.

Before: earnings, FOMC, regulatory, geopolitical, and unknown occurrences could
become macro-actual work and reach the macro provider adapter.

After: `event_occurrence_lifecycle_service` is the single fail-closed
classifier. It maps numeric macro, earnings, FOMC decision, and FOMC
communication contracts explicitly. Regulatory, geopolitical, unknown, and
unverifiable contracts are `schedule_only`, persist `IDLE`, attempt no outcome
fields, and are excluded from due claims. Production adapter wiring keeps
earnings on `earnings_provider`; only `macro_actual` owns the macro actual
adapter.

### 2. Persistent catch-up

Cause: the original startup function ran one `min(batch_size, max_per_tick)`
scan and returned a cursor that was not persisted.

Before: the default batch drained at most 20 rows per process start.

After: one tick processes repeated batches up to `max_per_tick`; lifecycle
leases and an existing schema-20 `provider_state` checkpoint persist cursor,
backlog, counts, tick count, and completion. A dedicated provider-only
application task runs subsequent ticks independently from the AI lifecycle
scanner. Completed/no-new-work ticks are read-only.

Evidence: an offline backlog of 45 occurrences drains as `40 + 5`, reaches
backlog zero on tick 2, and returns `ALREADY_COMPLETE` with zero writes on tick
3. Simulated boundaries of 7, 30, 365, and 730 days are covered. Provider
failure tests remain in deterministic backoff with zero AI.

### 3. CME/MNQ provenance

Cause: discovery of a CME page and document links set
`data_origin_is_official=true`, although no daily equity-index override had
been parsed.

Before: static Globex rules could be presented as an officially verified daily
MNQ state.

After: `official_document_discovered`, `official_schedule_parsed`, and
`session_state_verified` are separate. The structured
`cme_equity_index_schedule_v1` parser normalizes official equity-index
overrides. Static fallback is never marked official. On holiday-sensitive dates
without parsed coverage, MNQ is `UNKNOWN/UNVERIFIED`, not invented as open.

Fixture evidence:

- 2026-01-01: cash closed and verified MNQ holiday closure;
- 2026-07-03 observed holiday: cash closed while verified MNQ is open before
  its 13:15 ET early close;
- 2026-11-27 after 13:15 ET: verified MNQ early-close state;
- unavailable/malformed CME schedule: unverified fail-closed state.

Weekend, Sunday pre/post Globex open, and daily maintenance-break cases remain
covered independently.

### 4. Bucket-aware Consumer retention

Cause: a global impact ranking could consume the entire event budget with one
week.

Before: a nonempty previous/current/next bucket could disappear while coverage
reported only global counts.

After: each nonempty bucket receives a deterministic minimum quota. Quota
priority preserves published/revised previous-week actuals, current-day and
`AWAITING_ACTUAL` items, and next-week HIGH items. Each bucket publishes
candidate, retained, and omitted counts. Any omission prevents `COMPLETE`. An
impossible minimum under the byte budget is explicitly `DEGRADED` with
`byte_budget_insufficient_for_nonempty_bucket_minimum`.

### 5. Removed calendar occurrences

Cause: comparison iterated only IDs present in the new window.

Before: disappearance was invisible to the change classifier.

After: a missing prior ID becomes `UNCONFIRMED_REMOVAL`, carries comparison
lineage across snapshots, and never triggers by absence alone. An admitted,
explicit cancellation or postponement tombstone upgrades it to
`REMOVED_FROM_CALENDAR` and produces the corresponding semantic trigger.

### 6. Catch-up loop resilience and state semantics follow-up

Follow-up review scope: PR HEAD
`4584641d9ca9a36ab2fac576578042b12e56c311`.

Cause: an exception escaping one application-loop tick terminated the
long-running task. Separately, `_event_calendar_catchup_tick` persisted the
right completion checkpoint but always exposed `status=COMPLETED`. A tick
before a deferred item's `next_retry_at` also rewrote the same
`WAITING_BACKOFF` checkpoint and emitted duplicate telemetry.

After:

- ordinary tick exceptions are logged and emitted as redacted
  `startup_catch_up` error telemetry with correlation ID, error type, current
  catch-up state, and bounded retry delay;
- retry delay grows exponentially from one second and is capped at 30 seconds;
- a telemetry persistence failure is itself contained so the application loop
  still advances to a later tick;
- `asyncio.CancelledError` is explicitly re-raised both from the tick and error
  reporting path so shutdown is never swallowed;
- every loop tick still creates a provider-only execution context, and
  `allow_ai_residual` remains false;
- runtime/API `status`, checkpoint `completion_status`, tick telemetry
  `status`, and logs now distinguish `IN_PROGRESS`, `WAITING_BACKOFF`, and
  `COMPLETED`; a read-only subsequent tick exposes `ALREADY_COMPLETE` while
  retaining the final `COMPLETED` checkpoint;
- the checkpoint stores the earliest pending `next_retry_at` in the existing
  schema-20 `provider_state` row;
- while already `WAITING_BACKOFF` and before that time, the tick performs only
  due/backoff state reads. It makes zero provider or resolver calls, creates
  zero AI jobs/invocations, snapshots, or outbox rows, and does not rewrite the
  checkpoint or duplicate telemetry. A write is allowed only for a real state
  transition.

Deterministic evidence:

- transient tick failure followed by a successful second tick in the same
  task, without restart;
- `CancelledError` propagation with no error telemetry or AI work;
- 45-item drain states `IN_PROGRESS -> COMPLETED -> ALREADY_COMPLETE` for
  `40 + 5 + 0` claims;
- deferred provider result transitions to `WAITING_BACKOFF`; an early repeated
  tick is byte-for-byte checkpoint-idempotent with unchanged telemetry,
  snapshot, and outbox counts;
- `research_backend_invocations` and AI jobs remain zero in every case.

## Atomicity and zero-AI evidence

Snapshot preflight, projected lifecycle rows, resolved lifecycle rows, and
outbox emission continue inside the same SQLite transaction. Catch-up
rematerialization is coalesced per bounded batch. A repeated completed tick
creates neither snapshot nor outbox.

The provider-only execution context has `allow_ai=false`. The 45-item replay
records:

- AI invocations: 0;
- AI jobs: 0;
- `research_backend_invocations`: 0;
- live provider calls: 0.

## Validation

- Original focused calendar/lifecycle/CME/Consumer suites: 95 passed.
- Original extended provider/actual/recovery/worker/telemetry/snapshot suites:
  507 passed.
- Follow-up catch-up blocker tests: 14 passed.
- Follow-up pertinent lifecycle/provider/actual/outbox/telemetry suites:
  252 passed.
- Follow-up full suite: 1,655 passed in 259.27 seconds.
- Migration matrix schema 1 through 20: 20 passed.
- Ruff: passed.
- `py_compile`: passed.
- `compileall`: passed.
- `git diff --check`: passed.
- Offline replay: deterministic, 51,604 UTF-8 bytes, SHA-256
  `4ace5e2ee88d1fbc8edfaedaa82c1a73a11cc753c13929659959aa9c089618e3`.

## Residual limits

- CME PDFs or pages without the supported structured schedule payload remain
  discovered but unparsed and unverified.
- FOMC and earnings recovery needs a configured dedicated deterministic
  adapter; missing adapters cannot fall through to macro providers or AI.
- An event without an admitted explicit removal tombstone remains unconfirmed.
- Automatic catch-up ticks require a running service process; the checkpoint
  persists across downtime and resumes on the next start.
