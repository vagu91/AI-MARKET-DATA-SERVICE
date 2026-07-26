# Snapshot 91 producer-sync forensic review

Scope: read-only artifacts under
`data/controlled-provider-test-20260725T200541Z`. The operational database was
not opened or modified by the replay. A minimized redacted fixture is stored at
`tests/fixtures/snapshot_91_sync_forensic_redacted.json`.

## Source identity

```text
snapshot_id       mcs-0dccc313-75a5-44c6-aeba-9c3ab37d99e4
snapshot_revision 91
debug bytes       1,593,830
consumer bytes    46,841
consumer SHA-256  78F2487B99D88DDB9690EFB09F26F89A3D88A6095BDFF560AB3D54DAAB8430D1
```

Force and read-back debug artifacts are byte-identical. The original protected
root artifact remained at
`BCED28DECDF98D65AF9843C3CF3FF23DAB0A164C721B8BEF9B3E0D7697699DD4`.

## Root causes

1. The consumer projection imposed a 90,000-byte hard ceiling.
2. Per-section budgets recursively shortened lists, bounded strings and
   replaced remaining information with `compacted_item_count`.
3. News intelligence selected a bounded representative set and treated same
   event/source similarities as removable content.
4. Nasdaq and event projections exposed top-N subsets even though the producer
   had the complete source payload.
5. Calendar projection applied both a maximum event count and a 17,000-byte
   event budget.
6. Readiness was calculated on the hardened source object before subsequent
   consumer reductions.
7. Lifecycle `data_present` inspected the outer positioning object while COT
   values were nested below `cot.nasdaq_100`.
8. Market schedule unconditionally returned `AVAILABLE`; a rejected secondary
   holiday source could therefore appear authoritative.
9. Nasdaq cash and MNQ futures session states were conflated at the schedule
   level even when official CME coverage was absent.
10. Cache/live/last-known-good and provider telemetry were projected in several
    shapes, allowing acquisition counters to describe the source pipeline
    rather than the actual request path.

These were systemic projection and state-model defects, not isolated missing
fields.

## Snapshot 91 calendar before

```text
candidate_count     30
retained_count      20
omitted_for_size    10
previous_week        0
current_week         2
next_week           18
AWAITING_ACTUAL      2
```

The previous-week zero had no affirmative source-coverage proof. Two occurrences
from July 24 remained `AWAITING_ACTUAL`. The artifacts do not contain a valid
actual for them, so no actual has been inferred.

## Offline replay after

Command:

```powershell
.\.venv\Scripts\python.exe scripts\replay_snapshot91_sync_offline.py
```

Observed result:

```text
sync full bytes              1,466,342
section count                       17
calendar candidate_count            30
calendar retained_count             30
calendar omitted_count               0
sum bucket.event_count              30
sum len(bucket.events)              30
previous_week_count                  0
source_coverage_status         PARTIAL
missing coverage buckets PREVIOUS_WEEK
AWAITING_ACTUAL                       2
provider calls                        0
AI calls                              0
deliveries                            0
operational DB writes                 0
```

The full payload is about 31 times the old consumer artifact and is merely
measured. All 30 calendar candidates are retained. The empty previous week is
now explicitly `UNVERIFIED_EMPTY` under partial source coverage. The two missing
actuals remain missing.

## General corrections

- Immutable complete section payloads are persisted at snapshot commit.
- Stable material fingerprints exclude telemetry and keep order-independent
  record sets stable.
- Full and selective deliveries have no byte/count compaction.
- Calendar count invariants are enforced by construction.
- News records remain individually identifiable; clusters are supplemental and
  do not replace records.
- Nested COT data participates in lifecycle `data_present`.
- Rejected schedule sources are quarantined from operational holiday logic.
- Schedule status is only `AVAILABLE` when both Nasdaq cash and MNQ futures
  session coverage are verified; mixed coverage is `PARTIAL`.
- Sync readiness is calculated on the delivered payload.
- Delta, outbox and ACK state reference immutable snapshot and section
  revisions.
- The scheduler leases and executes durable refresh work outside the HTTP
  request, with restart recovery and persistent backoff.

## Residual evidence gaps

The forensic artifacts cannot reconstruct previous-week events that were never
acquired, and they cannot supply the two missing actuals. Production catch-up
must retrieve those provider-first after deployment and publish a new atomic
revision only when validated. AI fallback remains prohibited unless separately
and explicitly authorized for a residual field.

## Verification

- complete suite: 1,670 passed;
- sync and migration matrix: passed;
- Ruff, `py_compile`, `compileall` and `git diff --check`: passed;
- Windows PowerShell 5.1 parser: four scripts, zero errors;
- offline replay: zero provider calls, AI calls, deliveries and operational DB
  writes.
