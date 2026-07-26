# Snapshot 91 market-context content closure

Date: 2026-07-26

Base: `4858e76168118b7f173bc76fe77657fefb53c07f`
Snapshot: `mcs-0dccc313-75a5-44c6-aeba-9c3ab37d99e4`, revision 91

## Evidence integrity

The offline replay verifies before parsing:

- full exact SHA-256:
  `6521F9B03004DB1D7B52398FABCF46306C26AE604D263580A7475B8DBFE3407F`;
- validation-summary SHA-256:
  `3F130BF7DA0448FA283E2A185EB041ED28CA26A1A5E4F6E8ABC61BCF95D39605`;
- original full body: 881,788 bytes;
- 17 sync sections and readiness `PARTIAL`.

The source artifacts remain unchanged under
`data/market-context-sync-live-validation-20260726T083217Z`.

## Root causes

1. The canonical event window applied a 17,000-byte retention budget after
   acquisition. It retained 20 of 30 distinct occurrences; ten still existed
   in complete pre-consumer lists.
2. Source completeness was inferred from visible counts. The empty 13–19 July
   bucket had no affirmative provider coverage.
3. Catch-up reconciled only lifecycle rows already persisted; a missing
   schedule could not create the lifecycle work needed to recover an actual.
4. BLS month-grid flattening treated adjacent-month day cells as days of the
   requested month. That produced implausible Saturday/Sunday rows and
   incompatible period mappings.
5. News content-filter acceptance was reported before source-policy
   quarantine. The post-quarantine arrays were empty while historical counts
   and flags retained their pre-withholding values.
6. CME timeout left deterministic session calculations looking authoritative.
7. The hardening readiness calculation treated any non-empty market-schedule
   object as available.

## Closure

- The three-week builder has no count or byte limit and supplements canonical
  calendar rows from all complete pre-consumer event lists.
- Source record, occurrence, quarantine and exact-duplicate counts are
  separated. Different sources remain as source evidence.
- Temporal anomalies are auditably quarantined with specific reason codes.
- Startup catch-up performs a provider-first previous/current-week acquisition
  under a persistent single-flight lease and persistent provider backoff.
- Exact-actual reconciliation remains provider-only and exact-occurrence. A
  missing exact match stays `AWAITING_ACTUAL`; forecast/previous values are
  never promoted.
- Accepted current and historical news records are delivered raw with original
  content and lineage. Post-quarantine counts are recalculated from delivered
  arrays.
- Nasdaq cash and CME/MNQ remain separate. Unverified CME state sets
  `is_open=null`; a valid official last-known-good calendar may restore
  verified truth.
- Producer readiness continues to be calculated from delivered sync sections.
  Consumer hardening now blocks an unverified schedule.

Schema 21 already contains the required lifecycle and `provider_state`
structures. No migration 22 is added.

## Offline replay result

Run:

```powershell
python -B -m scripts.replay_snapshot91_sync_offline
```

Observed closure:

| Measure | Before | Offline replay |
|---|---:|---:|
| Distinct delivered occurrences | 20 | 24 |
| Previously excluded distinct IDs | 10 | 8 delivered, 2 quarantined |
| Source candidate records | not explicit | 32 |
| Delivered valid source records | not explicit | 24 |
| Temporal quarantine | not explicit | 2 |
| Exact technical duplicates | not explicit | 6 |
| Omitted for size | 10 | 0 |
| Omitted for count | 0 | 0 |
| Unexplained loss | not explicit | 0 |
| Previous-week coverage | implicit empty | `UNVERIFIED_EMPTY` |
| Replayed exact full-sync size | 881,788 bytes | 926,826 bytes |

The equation is exact:

```text
32 candidate = 24 delivered-valid + 2 quarantined-invalid
             + 6 exact-duplicate-technical
```

The recovered delivered IDs are:

```text
event:07838cf2e2def33653f29af4
event:3eeb14a2cf0fc46fb57b21a1
xtb:145333:2026-07-30
xtb:145706:2026-07-31
xtb:149744:2026-07-30
xtb:151135:2026-07-29
xtb:151212:2026-07-30
xtb:151720:2026-07-31
```

The two remaining IDs are temporal quarantines, not artificial count fillers.
The prior-week bucket remains honestly `UNVERIFIED_EMPTY` because an immutable
sync artifact cannot retroactively prove provider coverage.

The exact missing actual IDs remain:

```text
xtb:146392:2026-07-24
xtb:146945:2026-07-24
```

No value is inferred. Existing deterministic tests cover both exact-match
resolution and the no-match/backoff path.

The forensic sync contains no operational raw news records because its two
content-accepted records were source-policy quarantined. The replay therefore
reports zero raw records, resets historical count/availability to zero/false,
retains safe quarantine diagnostics, and does not promote quarantined content.
Separate controlled tests prove that admitted current and historical raw
records are delivered.

Market schedule remains `QUARANTINED` in the immutable forensic input. The
replay does not turn a historical timeout into verified session truth.

All replay side-effect counters are zero: provider calls, AI jobs, AI runs, AI
backend invocations and operational-database writes.

## Live post-merge verification

Run only in the authorized deployment environment after merge. Do not copy the
producer database to AI Trader.

1. Restart one producer instance and wait for the provider-only catch-up tick.
2. Confirm logs contain a schedule coverage state for previous/current week and
   no implicit AI invocation.
3. Save exact HTTP bytes, not terminal-formatted JSON:

```powershell
$baseUrl = 'http://127.0.0.1:8000'
Invoke-WebRequest `
  -Uri "$baseUrl/market-context/mnq/sync/manifest" `
  -OutFile 'manifest-exact.json'
Invoke-WebRequest `
  -Uri "$baseUrl/market-context/mnq/sync/full" `
  -OutFile 'ai-trader-full-sync-exact.json'
```

4. Analyze the definitive AI Trader JSON:

```powershell
@'
import hashlib, json
from pathlib import Path
p = Path("ai-trader-full-sync-exact.json")
raw = p.read_bytes()
d = json.loads(raw)
w = d["sections"]["event_calendar"]["window"]
c = w["coverage"]
n = d["sections"]["news"]["context"]
s = d["sections"]["market_schedule"]["sync"]
print("sha256", hashlib.sha256(raw).hexdigest().upper())
print("bytes", len(raw), "reported", d["payload_size_bytes"])
print("snapshot", d["snapshot_id"], d["snapshot_revision"])
print("calendar", {
    "candidate": c["source_candidate_count"],
    "delivered": c["delivered_occurrence_count"],
    "valid_source_records": c["delivered_valid_source_record_count"],
    "quarantined": c["quarantined_occurrence_count"],
    "duplicates": c["exact_duplicate_count"],
    "size_omissions": c["omitted_for_size_count"],
    "count_omissions": c["omitted_for_count_count"],
    "unexplained_loss": c["unexplained_loss"],
    "buckets": c["by_bucket"],
})
print("news", {
    "current": len(n.get("articles", [])),
    "historical": len(n.get("historical_articles", [])),
    "declared_historical": n.get("historical_article_count"),
    "historical_available": n.get("historical_context_available"),
    "historical_coverage": n.get("historical_coverage_status"),
})
print("schedule", s["status"], s["freshness"], s.get("reason"))
print("readiness", d["readiness"])
'@ | python -B -
```

5. Require exact payload-size equality, zero omission/loss counters, exact
   candidate equation, array/count agreement for news, and explicit coverage
   states for all three buckets. `PARTIAL`, `UNVERIFIED_EMPTY`,
   `PROVIDER_UNAVAILABLE`, `QUARANTINED`, stale or expired sections must remain
   unavailable in readiness.
6. Repeat the full request and compare SHA-256 at the same pinned revision.
   Then execute the existing selective/delta/ACK acceptance flow from
   `market-context-sync-contract.md`.

Do not treat a live count of 30 as an acceptance criterion. The accepted count
is whatever remains after deterministic validation and quarantine.
