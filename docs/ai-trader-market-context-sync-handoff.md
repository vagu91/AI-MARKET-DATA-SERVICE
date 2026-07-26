# Handoff to AI Trader

This document specifies the future consumer task. Producer support is complete;
integration with AI Trader is not.

## Ownership boundary

AI Trader must use only the versioned HTTP contract in
`market-context-sync-contract.md`. It must not open the producer SQLite file,
copy producer tables, execute producer migrations or infer state from internal
job records.

Senior Analyst remains analysis-only. A deterministic AI Trader coordinator
owns inventory comparison, expiration policy, synchronization, atomic
persistence, ACK, snapshot pinning and analysis invalidation.

## Required persistent consumer model

Persist at least:

```text
consumer_id
symbol
snapshot_id
snapshot_revision
context_fingerprint
generated_at
data_as_of
section_name
section_revision
section_fingerprint
record_count
freshness
valid_until
status
section_payload
received_at
delivery_id
checksum
```

Maintain immutable snapshot rows and a single atomic pointer to the current
usable snapshot. Never update the current pointer until all requested sections,
the manifest and checksums have committed successfully. Retain the prior pointer
for rollback and analysis pinning.

Maintain a separate delivery inbox keyed by `delivery_id`, and a request ledger
keyed by `request_id`. Both must be idempotent across restart.

## Analysis profiles

Define required sections in deterministic configuration. Suggested
`SENIOR_MARKET_ANALYSIS` minimum:

```text
macro
macro_actuals
event_calendar
fed
rates
risk
vix
positioning
nasdaq
news
market_schedule
options_positioning
market_internals
cross_asset_context
earnings
earnings_intelligence
geopolitical_regulatory_risk
```

A narrower follow-up profile may request a strict subset. The model must not
choose synchronization requirements dynamically. The coordinator may add a
section for an explicitly configured follow-up type.

## Notification intake

1. Validate contract version, event type, symbol, delivery id and revisions.
2. Insert the notification idempotently before doing network work.
3. If the same delivery is already ACKed, do nothing.
4. Fetch the manifest URL; do not trust notification metadata as the data
   source.
5. Coalesce wakeups, but retain every delivery id until its target is persisted
   or superseded by a reconstructible later revision.

Notification receipt alone must never update the consumer inventory.

## Planning and pull flow

1. Load the local atomic current-snapshot inventory.
2. POST the inventory and profile requirements to `/sync/plan`.
3. For `NONE`, verify the target manifest still matches the local inventory.
4. For `SELECTIVE`, POST exactly `sections_to_fetch` to `/sync/sections` with
   the returned target revision.
5. For `FULL`, GET `/sync/full`.
6. For `REVISION_GAP` or `RESYNC_REQUIRED`, discard only the uncommitted staging
   transaction and perform full sync. Do not delete the last committed context
   first.
7. Validate contract/schema, symbol, snapshot id/revision, section revisions,
   fingerprints, global checksum and context fingerprint.
8. Reject a response if any section belongs to another revision.

Do not interpret `UNAVAILABLE_AT_PRODUCER` as a missing download. Persist its
status and reason in the target snapshot so analysis can distinguish unavailable
producer data from a consumer persistence gap.

## Refresh work

If planning shows current producer data is insufficient for a market trigger or
explicit deepening request:

1. POST `/sync/refresh` with a stable request id.
2. On 200 `READY`, plan again.
3. On 202, persist `work_id` and poll the supplied status URL with bounded
   backoff.
4. On `WAITING_BACKOFF`, schedule the next poll at `next_retry_at`; never add
   force flags.
5. After committed completion, fetch the manifest and plan again. Work
   completion is not itself proof that the consumer persisted the new revision.
6. Resume polling after restart from the request ledger.

Multiple local analyses may wait on the same work id. Fan-out is a coordinator
concern, not a set of in-memory HTTP requests.

## Atomic consumer save

Use a staging transaction:

1. Insert immutable snapshot metadata.
2. Copy unchanged sections from the pinned local base only when fingerprint and
   section revision exactly match the target manifest.
3. Insert delivered sections.
4. Verify every required target section.
5. Recompute record counts, section fingerprints and context fingerprint.
6. Verify global checksum where supplied.
7. Commit all section rows and the current pointer atomically.
8. Only after commit, POST ACK.

If any verification fails, roll back staging, retain the previous current
snapshot, record the error and request a full resync. Never ACK partial or
uncommitted state.

Large responses are normal. Remove client limits based on 90 KB and configure
HTTP, deserialization and storage for multi-megabyte payloads. Do not summarize,
deduplicate across sources or truncate records while saving.

## ACK

Send `status=PERSISTED`, the exact delivery id, target snapshot revision and
exactly the changed-section revision inventory named by that delivery. Use the
coordinator timestamp, never a time before notification creation or more than
five minutes in the future.
Retry the identical ACK on timeout. Treat HTTP 409 as a contract conflict that
requires diagnosis; do not manufacture another ACK payload.

ACK does not mean analysis complete.

Do not call the deprecated global outbox ACK endpoint or the deprecated
`/market-context/mnq/consumer` projection. The former returns HTTP 410; the
latter is not complete synchronization input.

## Snapshot pinning and Senior Analyst start

Create an analysis record containing:

```text
analysis_request_id
snapshot_id
snapshot_revision
section_revisions
context_fingerprint
analysis_profile
created_at
status
```

Start Senior Analyst only after:

- target persistence committed;
- all profile-required available sections match the manifest;
- producer-unavailable sections are explicitly represented;
- the analysis record is pinned.

Pass the pinned immutable context. Do not let Senior Analyst query producer
state, compare revisions, decide expiration or mutate the pinned context.

## New updates during analysis

When a notification targets a later revision:

- persist it independently;
- compare changed sections with the analysis dependency set;
- mark a materially affected running/completed analysis `SUPERSEDED` and queue
  a new analysis according to policy;
- leave the original pinned input immutable for audit;
- use non-material or unrelated changes in the next analysis only.

Never splice a new section into a running analysis.

## Follow-up and deepening requests

Senior Analyst may request a named deterministic follow-up profile. The
coordinator maps it to whitelisted sections, performs refresh/plan/pull/save,
creates a new pinned analysis request and returns that context. It must reject
arbitrary table, SQL, field-path or force requests.

## Full resync conditions

Perform full resync when:

- local inventory is absent or corrupt;
- producer returns `REVISION_GAP` or `RESYNC_REQUIRED`;
- checksum, fingerprint, count or revision verification fails;
- a selective response omits a requested available section;
- the local base snapshot is not immutable/reconstructible.

Keep the previous committed context usable until the replacement commits.

## Consumer acceptance tests

The future AI Trader task must cover notification duplication, process crash at
every persistence boundary, full/selective checksum failure, >1 MB payloads,
missing producer data, revision gaps, refresh backoff, identical concurrent
requests, analysis pinning, supersession and idempotent ACK after restart.
