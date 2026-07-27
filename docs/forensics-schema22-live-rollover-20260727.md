# Schema 22 live rollover forensic correction

## Scope and safety

This correction analyzes only sanitized JSON evidence from snapshots 95 and
96 and runs redacted fixtures against temporary databases. It does not read
`.env`, modify the operational database, call a live provider, invoke AI or a
search backend, start Uvicorn, use a browser, deliver externally, touch AI
Trader, trade or place orders.

## Observed evidence

Snapshot 95 delivered 17 sections and a valid 1,251,177-byte full sync, but its
calendar equation left 11 source rows unexplained. The calendar was 21/53/9,
the two July 24 XTB occurrences had no actual, all 100 news candidates were
withheld, and schedule was quarantined.

The provider-force run persisted two events and 16 news rows without AI.
Snapshot 96 remained transport-valid at 1,195,442 bytes, but its provider
envelope covered only current/future dates. The projection fell to 0/25/7 and
treated prior-week absence as removal. News still delivered zero records.
Debug exposed schedule as `PARTIAL`, while Consumer Sync withheld it as
`QUARANTINED`.

## Root causes

1. Provider-force built its snapshot from the current provider envelope instead
   of the canonical three-week database union.
2. Coverage acquisition ended at “today”, leaving next-week dates absent from
   the ledger, and removal comparison did not require date-level authoritative
   scope.
3. Localized Italian month labels were not normalized with a release-date
   anchor. Actual promotion also preferred stale snapshot forecast/previous
   over exact observation lineage and did not reject ambiguous or stale proof.
4. Provider-force read only 100 active news rows. Publisher fallback ignored a
   separately supplied publisher, quarantined rows were unavailable for
   record-level diagnostics, and rejected source policy did not itself exclude
   an otherwise relevant article.
5. The validated weekly MNQ rule lacked durable source URL/lineage, so sync
   source validation could withhold the entire schedule when CME holiday
   cross-checking failed.
6. Readiness equated every partial/stale producer classification with total
   unavailability even when a lossless nonempty payload was delivered.

## Corrected behavior

Provider-force now loads the canonical prior/current/next database window and
unions it with the provider response before reconciliation. The coverage ledger
materializes all 21 dates; nonterminal future evidence remains retryable.
Removal comparison requires the release date to be explicitly authoritative,
so out-of-scope absence cannot remove prior-week events.

Monthly localized reference periods use the release date to derive `YYYY-MM`.
Exact observations preserve separate actual/forecast/previous fields and
lineage. Incompatible frequency/unit, stale retrieval, invalid lineage,
forecast/previous swaps and conflicting exact observations fail closed.

News DB projection has no row cap. Reliable publisher metadata is distinct from
Yahoo distribution, and later verified acquisition may promote a previously
quarantined row. Unknown publishers remain quarantined. Rejected candidates
are losslessly disclosed per record with their policy outcome and reason.

The weekly CME Globex rule carries a stable CME source URL and explicit
versioned-schedule lineage. An unavailable official holiday cross-check yields
`PARTIAL`; Nasdaq `PREMARKET/CLOSED` and MNQ `GLOBEX_OPEN` can coexist.
Readiness now distinguishes usable degraded sections from unavailable ones
without changing the conservative overall `PARTIAL` result.

## Adversarial PR #27 follow-up

The follow-up review found that the original replay did not traverse the
productive `DiagnosticsService.full_model(refresh="force")` branch. That
branch still used a 100-row active-news query and reconciled only the
current/future provider envelope. It now invokes the same persistent
schedule-catch-up seed used at startup, reads the canonical three-week window,
unions it before reconciliation, and requests the complete active plus
quarantined news interval. A provider-to-canonical-to-snapshot-to-full-sync
test exercises this exact path.

The review also closed these independent blockers:

- two force refreshes could overlap schedule acquisition, and the second
  catch-up mutated `provider_state` even with no due gaps; both paths now share
  the persistent single-flight lease and the no-work preflight is read-only;
- future absence inherited a segment-wide `authentic_empty` bit; future dates
  now require an explicit date-level empty proof and otherwise remain
  `PARTIAL`;
- a globally partial 21-day window suppressed legitimate date-authoritative
  removal evidence; removal decisions now use only the individually proven
  date inside the requested bounds;
- cross-stage accounting assigned arbitrary unexplained IDs to a revision
  count; revisions with differing provider IDs now require an explicit
  semantic alias, while every other ID remains unexplained and blocks closure;
- English month labels, semantically equivalent distributor IDs, ordered
  actual revisions, numeric zero and decimal precision were not covered;
- equal-timestamp news rows lacked a stable tie-breaker;
- delivered-payload readiness did not distinguish trading, macro, news and
  event-risk analysis.

Crash recovery is tested between canonical and coverage writes. No snapshot or
outbox is emitted from the incomplete attempt; the retry completes the ledger
and rematerializes only from coherent persisted state.

## Offline before/after

`python -B -m scripts.replay_schema22_live_rollover_offline` uses only minimal
redacted fixtures and temporary SQLite databases. The final replay proves:

- calendar 0/25/7 becomes 14/3/25;
- actual gaps 2 become 0;
- New Home Sales is 628.0 / 610.0 / 618.0 for 2026-06;
- Flash Services PMI is 53.6 / 52.0 / 51.2 for 2026-07 monthly;
- three reliable articles are delivered, including two distinct Reuters
  updates, while the unknown Yahoo-distributed publisher stays quarantined;
- schedule becomes `PARTIAL`, not quarantined;
- the occurrence candidate equation closes with `unexplained_loss=0`;
- all 17 sections are present and no byte/count/top-N/lossy-summary cap applies;
- independent replays and consecutive full reads are byte-identical;
- the second catch-up has zero provider calls, resolver evaluations, canonical,
  lifecycle, coverage, snapshot, outbox and provider-state writes, and creates
  no retry or revision;
- live provider, AI, browser, delivery, operational-DB and trading counters are
  all zero.

The final adversarial replay contains 17 sections, is 398,964 bytes, and has
SHA-256
`3B631DC203ECA30327EACBD74395CB9C4E24AF9DBBB1AA9CB1CE3E8597726885`.
The migration matrix covers 1→22, 20→22, 21→22 and 22→22, plus rollback and
concurrent migrators. Ruff, `py_compile`, `compileall`, and
`git diff --check` pass. The complete suite passes 1,805 tests; its only
warning is the pre-existing Starlette/httpx TestClient deprecation.

## Residual risk

The replay proves deterministic behavior for the captured redacted shapes, not
future provider correctness. In production, partial provider evidence remains
partial and scheduled for retry. Official CME holiday/early-close overrides
remain unavailable until their cross-check succeeds; the weekly rule is not
promoted to holiday authority. Unknown publishers remain withheld until a
later acquisition supplies admissible originator provenance.
