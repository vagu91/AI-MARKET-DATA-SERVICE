# Snapshot 92 live-blocker forensic closure

Date of source validation: 2026-07-26
Mode of this closure: redacted fixture and offline replay only
Database schema: 21, unchanged

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
or AI Trader payload.

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
| Catch-up materialization | 0 writes | 1 atomic snapshot write |
| News candidate/accepted/delivered/rejected | 100 / 4 digest / 0 / inconsistent | 3 / 2 / 2 / 1 redacted |
| News usability | `true` with quarantined empty payload | `true` only with 2 delivered articles |
| Cash/MNQ weekend | `UNKNOWN` / `UNKNOWN` | closed `WEEKEND` / closed `WEEKEND` |
| Rates validity | July 24 datum, July 13 expiry | expiry floored to July 26 retrieval |

The redacted replay intentionally has 17 discovered actual gaps rather than the
18-row live lifecycle backlog because it reproduces the exact 14+3 schedule
discovery subset. It proves the fixed-point property without disclosing the
remaining live row.

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
round-trips 13 distinct raw articles in a canonical payload larger than two
megabytes without count or byte caps.

## Session and temporal invariants

Weekly cash and Globex rules are separate from holiday/special-session
overrides. On the fixture weekend both sessions expose `is_open=false`,
`closed_reason=WEEKEND`, `verification_scope=BASE_WEEKLY_RULE`, and
`holiday_override_status=UNVERIFIED`. Holiday name and early-close flags remain
empty, so the timeout does not create official claims.

For every sync section:

```text
effective valid_until >= max(data_as_of, observed_at, retrieved_at)
```

When correction is required, inherited expiry is retained in internal metadata
for audit, the effective value is clamped to the delivered temporal floor, and
freshness is derived from delivered records.

## Offline proof

`scripts/replay_snapshot92_live_blockers_offline.py` was executed twice. Both
outputs were byte-identical with SHA-256:

`411B3EF9E48761DD794B66BE8B21AAA28A65CFCD01D9F5C7F3FD9431E867EB60`.

The canonical result also embeds the encoding-independent replay digest
`D94107069B4E37C17B7DCF3CF741AE0245508E5295D4C49FA17235CF97305A25`.

The replay reports:

- 14 previous, 5 current (3 discovered plus 2 retained), and 25 next events;
- cross-stage `unexplained_loss=0`;
- both named actual IDs retained;
- catch-up `WAITING_BACKOFF`, 17 pending, one atomic rematerialization;
- 2 admitted news records and 1 quarantined record;
- cash and MNQ deterministic weekend closure;
- current monotonic rates validity;
- a lossless multi-megabyte payload;
- zero live provider calls, AI jobs, AI backend invocations, AI enqueue,
  browser, delivery, trading, and operational database writes.
