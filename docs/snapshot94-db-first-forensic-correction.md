# Snapshot 94 DB-first forensic correction

## Scope and safety

The investigation used the redacted live evidence captured in
`monday-provider-live-validation-20260727T093918Z`. The backup SQLite
database was inspected only with URI `mode=ro` and `PRAGMA query_only=ON`.
No operational database, `.env`, live provider, AI backend, browser,
Uvicorn, scheduler process, migration process, AI-TRADER delivery path, or
trading path was executed.

The immutable consumer artifact remained untracked with SHA-256
`BCED28DECDF98D65AF9843C3CF3FF23DAB0A164C721B8BEF9B3E0D7697699DD4`.

## Root causes

1. Schedule catch-up rebuilt the snapshot from the latest provider envelope.
   On the Sunday-to-Monday rollover, valid historical occurrences therefore
   disappeared from the projection even though canonical rows still existed.
2. There was no durable, uniquely scoped daily coverage proof. An empty
   response could not be distinguished from an uncalled, failed, partial, or
   successfully verified-empty scope.
3. XTB normalization retained localized period labels, discarded the
   `forecast` field into `consensus`, and restricted history to one day. The
   exact Friday releases were consequently outside Monday catch-up.
4. `actual_missing_ids` was derived only from delivered rows. Lifecycle,
   coverage, quarantine, and absent-projection gaps could be hidden.
5. A missing CME holiday cross-check was able to dominate the section-level
   interpretation even when deterministic regular-session rules remained
   valid.

## Corrected model

Schema 22 is additive. It introduces `event_calendar_coverage`, keyed by
calendar date, domain, entity type, provider, scope, and exact window. Each
row records one of `VERIFIED_COMPLETE`, `VERIFIED_EMPTY`, `PARTIAL`,
`UNKNOWN`, `PROVIDER_UNAVAILABLE`, or `QUARANTINED`, together with call
evidence, validity, revision/retry deadlines, lineage, and a semantic
fingerprint. `VERIFIED_EMPTY` is structurally impossible unless a scoped
provider call completed successfully.

Canonical events remain in `economic_events_history`. Additive columns record
item completeness, outcome contract, publication grace, revision due time,
and removal lineage. Exact no-op writes preserve `updated_at`.

The catch-up flow now:

1. computes dynamic America/New_York daily buckets;
2. reads coverage before acquisition;
3. calls only contiguous due gaps;
4. persists accepted canonical events and daily coverage;
5. rereads the complete three-week window from the database;
6. rematerializes a snapshot/outbox only for a material change.

Valid `VERIFIED_COMPLETE` and `VERIFIED_EMPTY` days produce zero provider
calls. Restart gaps of one day, one month, and one year are date-driven rather
than tied to the last in-memory snapshot.

## Actuals, news, and schedule

XTB monthly Italian period names are normalized to `YYYY-MM`, the provider
lookback follows the configured persistent catch-up horizon, and
actual/forecast/previous retain separate lineage. Exact-occurrence selection
requires matching identity, release minute, frequency, reference period,
accepted source, and an `actual`/`current` source field. This recovers:

- `xtb:146392:2026-07-24`: actual 628.0, period 2026-06, forecast 610.0,
  previous 618.0.
- `xtb:146945:2026-07-24`: actual 53.6, period 2026-07, forecast 52.0,
  previous 51.2.

The negative cases reject forecast/previous swaps, release mismatches, and a
quarterly PMI period.

The existing productive news path remains lossless: IBD and two temporally
distinct Reuters updates preserve URL, publisher, distributor, content, and
lineage; an unknown publisher distributed by Yahoo remains quarantined.
Deduplication is limited to exact technical identity.

At `2026-07-27T09:40:00Z` (05:40 ET), the normalized schedule exposes Nasdaq
cash `CLOSED`, phase `PREMARKET`, and MNQ `GLOBEX_OPEN`. An unavailable CME
holiday cross-check is explicit and retryable; the section is `PARTIAL`, not
quarantined.

## Before/after evidence

The snapshot-94 evidence showed calendar `0/25/7`, two hidden overdue XTB
actual gaps, 100 news candidates with zero delivered, and a 17-section
1,007,063-byte full sync. The redacted correction fixture records the required
calendar target `14/3/25`, actual gaps `2 → 0`, one retained
`UNCONFIRMED_REMOVAL`, and the required news/schedule invariants.

The materialized offline full-sync replay contains all 17 sections and applies
no count, byte, top-N, destructive deduplication, or lossy-summary cap. Two
independent runs were byte-identical:

- full sync: 383,680 bytes,
  SHA-256 `917FE949ECFAA07E2F689984F9C99FE84F556CFA31B3554FDABA56AF7882F890`;
- separate summary: 13,110 bytes,
  SHA-256 `17988FA6775E1DB1D0EDCEC2B79AE0ED76B66BE2AAB940E74F9DF27395B19A99`.

## Validation

- focused XTB/coverage/provider tests: 50 passed;
- affected regression rerun: 7 passed;
- full suite initial adversarial run: 1,750 passed, 7 failures;
- all seven failures were corrected (three expectations now assert the new
  zero-call fixed point; three migration expectations include schema 22; one
  canonical persistence defect now derives `date`/`time_utc` from
  `release_at`);
- pre-review full suite: 1,757 passed, one third-party deprecation warning;
- Ruff: passed;
- `py_compile` and `compileall`: passed;
- migration replay: 1→22, 20→22, 21→22, 22→22 passed and idempotent;
- offline replay twice: byte-identical summary and full sync.

## Adversarial PR review

The post-implementation adversarial review found and corrected five blockers:

1. Including `window_start/window_end` in the primary key allowed multiple
   logical rows for the same day as a current-day window advanced.
2. A successful function return without affirmative scope, pagination,
   parsing, expected-source, record-validity, and authentic-empty evidence
   could create false terminal coverage.
3. Coverage did not include contract and source-policy versions, so a policy
   change could leave obsolete verification active.
4. Two concurrent migrators could both observe a stale migration set and
   execute the same `ALTER TABLE`.
5. Exact actual promotion did not reject stale validation, incompatible
   frequency/unit, or invalid forecast/previous field lineage.

The logical coverage identity now includes date, domain, entity type,
provider, query scope, symbol, contract version, and policy version. Window
bounds are evidence, not identity. Terminal coverage requires all positive
proof flags and a finite `valid_until`; an empty result additionally requires
`authentic_empty`. Unproven empty, partial pagination, wrong scope, parsing
loss, quarantine, missing source, provider failure, and policy/contract
changes remain due.

Migration locking now uses `BEGIN IMMEDIATE` plus an in-transaction version
recheck. Concurrent writers produce one ledger row, and a forced failure
after the schema-22 DDL rolls the entire migration back to schema 21.

The migration was also executed on a temporary copy of the 215,019,520-byte
snapshot-94 forensic database. It completed in 0.060 seconds, preserved an
empty conservative ledger, passed `integrity_check`, and the principal lookup
used `idx_event_calendar_coverage_due` rather than a table scan (1,000-query
average: 0.0040 ms).

The redacted live-news fixture now records all 100 candidates with exact
exclusion totals: 79 stale/expired, 13 missing content, 3 low relevance,
2 ambiguous topic, 2 personal finance, and 1 mortgage.

The replay acquisition counters were: 0 avoided calls on the initial empty
ledger, 1 due call, 1 executed call, 16 canonical writes, 14 coverage metadata
writes, 1 schedule snapshot write, and 1 corresponding outbox write.

Final adversarial validation: 1,776 tests passed, including proof-failure,
logical-key, concurrent-writer, concurrent-migrator, crash rollback,
schema-21 populated/empty-ledger, policy/contract invalidation, calendar,
actual, news, schedule, sync, lifecycle, ACK, and consumer coverage.

No merge is part of this correction.
