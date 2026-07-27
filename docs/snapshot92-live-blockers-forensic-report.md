# Snapshot 92 live-blocker forensic closure and PR #25 adversarial review

Date of source validation: 2026-07-26
Final independent adversarial review: 2026-07-27
Mode of this closure: redacted fixture and offline replay only
Database schema: 21, unchanged
Review branch: `codex/close-snapshot92-live-blockers-20260726`
Initial review HEAD: `2ebf25df852ffc48ce969370fb779bdb7263c3d5`
Base: `main@94c8044a03be477d7217a290571aca5a48fec68f`

## Evidence integrity

The live evidence was inspected read-only and was not copied into tracked
artifacts. The two source artifacts had:

- provider-first catch-up: 12,111 bytes,
  SHA-256 `182E1AF75B80D13184848EAED58DE275079537E188B2ECEE20C9339857ACEB73`;
- exact full sync: 926,516 bytes,
  SHA-256 `77428654F1D227BAC5D8D721947586A846B7CFAA2AD4C2C000750E121053EDBD`.

The tracked reproduction is
`tests/fixtures/snapshot_92_live_blockers_redacted.json`. It contains no
secrets, personal data, credentials, local paths, operational database content,
or AI Trader payload. Its final SHA-256 is
`4B8CB1C37AD0AD4D573D81030AB46DF217796818580F99BFEDD6A0850CDED4AB`.

## Root causes

1. Discovery persisted lifecycle rows but did not update the canonical
   calendar component used by snapshot construction. The due backlog was then
   reported after processing without proving delivery-stage convergence.
2. A successful provider request with a `NO_DATA` envelope was transitioned to
   a short-lived idle state and reported as `COMPLETED`; provider success was
   conflated with actual recovery, persistence and rematerialization.
3. A forward-looking provider response could omit older occurrences.
   Comparison recorded them as unconfirmed removals but the next builder did
   not rehydrate their previous occurrence, so their actual lifecycle vanished
   from `actual_missing_ids`.
4. News source validation applied a generic allowlist and two-confirmation rule
   to editorial news. IBD and Reuters-through-Yahoo were withheld even though
   their publisher/distributor provenance was recoverable. Counts and digest
   were not recalculated after withholding.
5. Failure to verify the CME override source erased a deterministic weekly
   closure, producing `UNKNOWN` for both market-session projections.
6. Sync selected an old nested `valid_until` before considering newer delivered
   records, causing current rates retrieved on July 26 to inherit a July 13
   expiry.

The adversarial review found six additional contract defects in the first
version of PR #25:

1. `catch_up_backlog_after` counted only immediately due rows and omitted
   durable `BACKOFF` rows. A 17-row residual therefore appeared as backlog zero.
2. `writes` counted only snapshot rematerializations, not lifecycle
   transitions. One snapshot was reported for 34 lifecycle writes plus one
   snapshot write.
3. The real schedule-discovery merge retained omitted occurrences but did not
   label them `UNCONFIRMED_REMOVAL` or ensure their missing-actual lifecycle
   remained operational. The earlier projection test injected a pre-corrected
   comparison and therefore did not exercise this path.
4. A post-quarantine empty news section could retain an inherited `AVAILABLE`
   status even though accepted and delivered counts were zero.
5. An ordinary Globex-open interval became `is_open=null` whenever holiday
   overrides were unavailable, even though the base weekly rule was
   deterministic.
6. Temporal correction was applied to section-manifest metadata after the
   fingerprint had been computed. Nested delivered records could still violate
   the invariant and the fingerprint described the uncorrected payload.

The review also found that partial provider resolutions were omitted from
`actuals_recovered` and `revisions_reconciled`; those counters now include both
partial and coalesced final resolutions.

The final independent review of HEAD
`3c401b41b9007016a7a7f30014467986ae614b7e` found four further lifecycle
blockers:

1. A tick one second into a durable lifecycle backoff still reacquired the
   schedule-discovery lease and rewrote `provider_state`. The prior idempotency
   test froze the clock and checked only snapshot count, so it did not prove
   byte identity.
2. An expired `BACKOFF` row was counted both as immediately due and as pending
   backoff. Conversely, a provider residual finalized as retryable `IDLE` was
   omitted from the real backlog.
3. Leaving a lease for `IDLE` or `COMPLETED` preserved an obsolete
   `next_retry_at`. The same occurrence could therefore be reclaimed repeatedly
   until `max_per_tick`, inflating residual, write and provider-call counts.
4. Tick completion considered only due/backoff counts plus the current
   exhausted list. Persisted `DISABLED`/`NO_DATA` gaps could disappear from the
   completion decision, allowing false `COMPLETED` and overlapping terminal
   semantics.

The closure adds a read-only early-backoff fast path, separates due work from
future retry/lease work, clears obsolete retry timestamps on finalization, and
derives terminal status from persisted gap rows. Six independent executions
cover an advancing-clock byte comparison, expired-backoff non-overlap,
retryable `IDLE`, pure exhausted no-data, disabled terminal gaps, and a mixed
resolved/terminal `PARTIAL` outcome.

## Before and after

| Stage | Live snapshot 92 before | Offline closure after |
|---|---:|---:|
| Previous-week discovery | 14 verified candidates | 14 canonical deliveries |
| Current-week discovery | 3 verified candidates | 3 canonical deliveries |
| Previous-week snapshot | 0, `UNVERIFIED_EMPTY` | 14 |
| Current-week snapshot | 0, `UNVERIFIED_EMPTY` | 5, including 2 retained unconfirmed removals |
| Next-week snapshot | 25 | 25 |
| Catch-up status | false `COMPLETED` | `WAITING_BACKOFF` |
| Catch-up claimed/resolved/residual | 18 / 0 / 18 | 17 redacted / 0 / 17 |
| Catch-up materialization | 0 writes | 34 lifecycle writes + 1 atomic snapshot write |
| News candidate/accepted/delivered/rejected | 100 / 4 digest / 0 / inconsistent | 3 / 2 / 2 / 1 redacted |
| News usability | `true` with quarantined empty payload | `true` only with 2 delivered articles |
| Cash/MNQ weekend | `UNKNOWN` / `UNKNOWN` | closed `WEEKEND` / closed `WEEKEND` |
| Rates validity | July 24 datum, July 13 expiry | expiry floored to July 26 retrieval |

The replay intentionally retains only the redacted 14+3 discovery subset. The
independent adversarial test starts with 18 lifecycle rows, discovers all 17
additional rows, and uses `max_per_tick=20`. Tick one reports 35 total backlog,
claims 20, leaves 15 due plus 20 in backoff, and remains `IN_PROGRESS`. Tick two
claims the remaining 15 and reports 35 in backoff as `WAITING_BACKOFF`. An early
third tick performs zero writes and creates no revision.

The advancing-clock variant moves time forward by one second inside the same
backoff, asserts that schedule acquisition is not called again, and compares the
SQLite file byte-for-byte before and after the tick. The expired-retry variant
proves one occurrence yields `backlog_before=1`, not two. Future retry and
terminal-gap counts are now separately exposed while legacy
`pending_backoff` remains available.

## Discovery, resolution, persistence, materialization and delivery

These stages now have separate evidence:

- discovery reports provider result/success counts and discovered occurrence
  IDs;
- resolution reports resolver evaluations, provider requests, request
  completions, failures, actuals recovered and retry exhaustion;
- persistence reports new lifecycle gaps and durable backoff;
- materialization reports snapshot IDs and writes independently from resolver
  success;
- delivery proves the cross-stage equation and full/selective fingerprint
  equality.

A `NO_DATA` provider response retains `AWAITING_ACTUAL`, occurrence payload,
reference period, provider lineage and retry state. It cannot reach AI enqueue.
At retry exhaustion it becomes the explicit acquisition outcome
`EXHAUSTED_NO_DATA`, not a fabricated actual or confirmed calendar removal.

## Two named actuals

The following IDs remain visible and retryable:

- `xtb:146392:2026-07-24`;
- `xtb:146945:2026-07-24`.

Both are delivered in the current-week canonical bucket with
`removal_status=UNCONFIRMED_REMOVAL`, their complete previous occurrence,
comparison lineage, `release_status=AWAITING_ACTUAL`, `actual=null`, and an
entry in `actual_missing_ids`. Exact-occurrence resolution continues to require
the semantic metric and exact reference period. No value is invented and no
AI numeric resolution is permitted.

The adversarial suite also resolves both IDs through the real lifecycle scan.
It asserts exact reference periods `2026-06` and `2026-Q2`, atomically projects
the official offline-fixture values, changes release status to `PUBLISHED`,
removes both IDs from `actual_missing_ids`, rematerializes one coalesced
resolution snapshot, and proves full/selective section equality. The NO_DATA
variant retains both rows in current week with complete lineage and durable
retry timestamps.

## News provenance and losslessness

`source-policy-v5` admits Investor's Business Daily directly and admits
`finance.yahoo.com` only as an explicitly distribution-only path for an allowed
original publisher. The replay preserves Reuters as original publisher and
Yahoo Finance as distributor. An unknown publisher on a syntactically valid but
unlisted domain remains quarantined. A single admitted IBD record is delivered
with confirmation false and explicit reliability.

Post-withholding reconciliation derives every count, status, digest and
usability flag from the same raw delivered set. Technical retry deduplication
does not remove distinct providers or syndicated records. The replay also
round-trips 13 distinct raw Unicode articles in a 7,023,223-byte canonical
payload without count or byte caps. A deliberately contradictory empty section
is normalized to `NO_DATA`/`NO_DATA_AVAILABLE`, zero accepted/delivered records,
four rejected candidates and `usable_for_analysis=false`.

## Session and temporal invariants

Weekly cash and Globex rules are separate from holiday/special-session
overrides. On the fixture weekend both sessions expose `is_open=false`,
`closed_reason=WEEKEND`, `verification_scope=BASE_WEEKLY_RULE`, and
`holiday_override_status=UNVERIFIED`. Holiday name and early-close flags remain
empty, so the timeout does not create official claims.

The base-rule matrix separately checks Saturday, Sunday before Globex open,
Sunday after open, an ordinary weeknight and the maintenance break. Verified
holiday, observed-holiday, early-close, provider-timeout, valid LKG and missing
or expired LKG cases remain covered by the CME and content-closure suites.
Ordinary open intervals now report `GLOBEX_OPEN`; holiday-sensitive dates still
report an unverified state instead of inventing an override.

For every sync section:

```text
effective valid_until >= max(data_as_of, observed_at, retrieved_at)
```

When correction is required, the inherited and effective values plus their
record paths are disclosed under `producer_disclosures.temporal_reconciliation`.
The corrected record is persisted before record counting, fingerprinting and
serialization. Daily, weekly and already-valid mixed records are checked
through both full and selective delivery.

## Existing coverage independently rechecked

The review did not duplicate cases that already had strong assertions. It
reran and mapped the following existing coverage:

- cancellation propagation, schedule/lifecycle lease expiry and recovery:
  `test_event_calendar_forensic_blockers.py`;
- atomic snapshot/section/outbox rollback and revision consistency:
  `test_market_context_sync_protocol.py`;
- payloads above one and five megabytes, Unicode equality, selective/delta
  losslessness and revision pinning: `test_market_context_sync_protocol.py`;
- consumer-specific ACK, wrong or partial ACK rejection, retry/dead-letter,
  single-flight, waiter fan-out and heartbeat:
  `test_market_context_sync_protocol.py`;
- official actual provider matching and no-AI fallback:
  `test_macro_actual_lifecycle_wiring.py`;
- Reuters/Yahoo distribution policy, IBD, malformed and unknown sources,
  single-source reliability, syndicated views, historical news and
  non-destructive clustering: `test_news_intelligence_service.py` plus the
  snapshot-92 adversarial tests;
- stale, partial, quarantined and producer-truth readiness:
  `test_ai_trader_readiness_contract.py`;
- schema versions 1 through 20 upgraded to 21 and 21 reopened idempotently:
  `test_event_calendar_catchup_batch.py`.

## Offline proof

`scripts/replay_snapshot92_live_blockers_offline.py` was executed twice. Both
outputs were byte-identical with SHA-256:

`4E472D93F849C19470EAEEA1EEFED22ED62144FA5F5CAD0BB1FE5E2F254F8ED1`.

Each canonical output is 3,110 bytes.

The canonical result also embeds the encoding-independent replay digest
`E16D622A23379890A75A337C61ACF2E0F4E038F4D3DA4190FE3E16A41C2A2277`.

The replay reports:

- 14 previous, 5 current (3 discovered plus 2 retained), and 25 next events;
- cross-stage `unexplained_loss=0`;
- both named actual IDs retained;
- catch-up `WAITING_BACKOFF`, 17 pending, one atomic rematerialization;
- 2 admitted news records and 1 quarantined record;
- cash and MNQ deterministic weekend closure;
- current monotonic rates validity;
- a lossless 7,023,223-byte Unicode payload;
- zero live provider calls, AI jobs, AI backend invocations, AI enqueue,
  browser, delivery, trading, and operational database writes.

## Verification ledger

The focused adversarial file contains 19 executions: 13 from the first PR
review plus six new independent executions from the final review. The extended
offline groups completed as follows:

```text
snapshot-92 adversarial                         19 passed
catch-up, cancellation, leases, actuals        110 passed
sync protocol and readiness                    127 passed
news, sessions and three-week projection       197 passed
migration matrix (included above)               22 passed
complete pytest suite                          1737 passed
Ruff                                             passed
py_compile                                     246 files passed
compileall                                       passed
git diff --check                                 passed
offline replay x2                               byte-identical
```
