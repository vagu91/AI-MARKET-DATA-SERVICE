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
- live provider, AI, browser, delivery, operational-DB and trading counters are
  all zero.

## Residual risk

The replay proves deterministic behavior for the captured redacted shapes, not
future provider correctness. In production, partial provider evidence remains
partial and scheduled for retry. Official CME holiday/early-close overrides
remain unavailable until their cross-check succeeds; the weekly rule is not
promoted to holiday authority. Unknown publishers remain withheld until a
later acquisition supplies admissible originator provenance.
