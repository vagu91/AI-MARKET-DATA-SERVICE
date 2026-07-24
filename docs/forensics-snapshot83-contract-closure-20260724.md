# Snapshot 83 contract closure — offline forensic report

Date: 2026-07-24
Scope: AI-MARKET-DATA-SERVICE only
Live activity: none

## Evidence baseline

The redacted fixture `tests/fixtures/snapshot_83_forensic_redacted.json` records:

- parent `prun-b67a84fc-29bb-4db0-90c0-42648e087ea1`;
- snapshot `mcs-7064e0af-faf1-479d-8855-cdb80ef110fb`, revision 83;
- generated at `2026-07-24T15:01:42+00:00`;
- 6 terminal children: 1 `PARTIAL`, 5 `NO_DATA`, 0 failed;
- 808,644 tokens;
- 19 candidate claims: 2 accepted, 17 rejected.

The fixture contains only the minimum redacted rows needed to reproduce the temporal,
deduplication, issuer, CFTC, Cboe, Nasdaq and disabled-agent findings. The replay
`python -m scripts.replay_snapshot83_forensics` performs no network, provider or AI
backend operation.

## Root causes and corrections

| Defect | Root cause | Correction |
|---|---|---|
| Past events remained `PRE_RELEASE` | persisted temporal labels were trusted during DB read-back and consumer materialization | temporal status is recalculated at read-back, hardening and consumer projection; an explicit quarantine still wins |
| Missed actuals after downtime | no bounded startup lifecycle reconciliation existed | startup catch-up scans only the configured window, only when scheduler and due scanner are enabled, using provider-first resolution and persistent idempotency |
| Duplicate Service PMI / New Home Sales | identity included mutable labels/categories and did not prioritize provider occurrence IDs | canonical identity uses provider, provider event ID, country, release minute and normalized provider type; aliases are conservatively merged and retained for audit |
| Past event exposed as “next” | event projection did not separate temporal buckets | consumer now separates upcoming, awaiting actual, recently released and historical events; “next” is future-only |
| Published AMD announcement rejected | issuer announcements with an event timestamp were treated as elapsed schedules | published issuer content is the content itself, uses published validity and is not sent through scheduled-event actual refresh |
| COT consumed AI despite official data | CFTC adapter selected a generic Nasdaq row and used incorrect TFF column offsets | exact MNQ code 209747 selection, official TFF structured parsing, arithmetic checks, Tier-1 lineage and weekly publication validity |
| Cboe numbers rejected by fuzzy matching | numeric exchange tables were routed through text matching | structured put/call, VX and risk-index normalization with interval and future-time validation |
| Invalid Nasdaq source fed calculations | presence of holdings/count could override invalid-source evidence | schema 2.1 requires an operational source or a fully qualified last-known-good record; invalid inputs yield no holdings or derived calculations |
| Lifecycle contradictions | generic non-empty payload checks treated diagnostics/null shells as data | domain-specific operational-value checks now drive `data_present`, freshness and `currently_valid`; lineage is synthesized only from a real source |
| Disabled domains were verbose `NO_DATA` | consumer projection did not consistently apply enablement | disabled domains are compact `DISABLED` records and the four optional topics are listed explicitly |
| Consumer exceeded its warning limit | warning/trimming stages did not enforce the encoded byte ceiling | duplicated lifecycle/debug material, nulls and long lists are removed in bounded stages; final UTF-8 size is enforced |
| Provider availability could still lead to AI | lifecycle adapters were not wired for CFTC/Cboe and partial resolution was not field-aware | existing deterministic providers are registered; complete results skip AI, partial results expose only missing fields, temporary errors persist backoff/negative cache |
| Retry could continue indefinitely | provider exhaustion had no elapsed-event terminal deadline | configurable bounded retry deadline ends with `NO_DATA` and no AI eligibility |
| Impossible combinations were hard to query | deterministic telemetry lacked domain anomaly rules | structured anomaly categories cover temporal, lifecycle, source, payload, enablement and provider-first invariants |
| Legacy readiness regressed | strict 2.1 Nasdaq rules were also reflected into schema 1.0 count-only readiness | schema 1.0 retains its historical count-only marker only when no explicit invalid-source signal exists; schema 2.1 remains fail-closed |

## Offline replay: before / after

| Invariant | Snapshot 83 evidence | Offline replay after correction |
|---|---:|---:|
| past `PRE_RELEASE` events | 3 | 0 |
| unique awaiting-actual occurrences | duplicated aliases | 3 |
| duplicate occurrence count | Service PMI duplicated | 0 |
| past `next_critical_event` | possible | none |
| AMD official announcement | rejected as elapsed | `CURRENT`, `PUBLISHED` |
| CFTC MNQ contract | metadata-only AI result | code 209747, open interest 278,558 and all TFF groups validated |
| Cboe put/call | fuzzy rejection | structured ratio 0.82 |
| VX curve | fuzzy rejection | 2 structured monthly contracts |
| invalid Nasdaq holdings | 103 holdings and derived calculations | `NOT_AVAILABLE`, 0 holdings, no concentration |
| disabled optional agents | noisy/no-data projection | 4 compact `DISABLED` projections |
| consumer encoded size | about 103.4 KB | 54,170 UTF-8 bytes for the forensic fixture |
| live calls | out of scope | 0 |

## Test coverage

`tests/test_snapshot83_contract_closure.py` adds 30 named contract scenarios
(36 collected cases with parametrization), covering:

1. startup before an event;
2. one-hour-late startup with deterministic actual, atomic snapshot/outbox and repeated-start idempotency;
3. provider temporary failure, negative cache and backoff;
4. disabled scheduler/catch-up zero-work and zero-write;
5. complete and partial provider-first resolution;
6. retry deadline;
7. today/tomorrow occurrence separation;
8. published issuer content and earnings schedule/result semantics;
9. CFTC and Cboe structured parsers;
10. duplicate merge, future-only next event and recently released projection;
11. invalid Nasdaq and qualified last-known-good behavior;
12. compact disabled domains and real UTF-8 payload limit;
13. lifecycle operational-value invariants;
14. CLI/OpenAI normalized backend parity;
15. deterministic anomaly categories and the snapshot 83 replay.

Existing integration suites additionally cover disabled-agent recovery, trigger
envelopes, non-triggering refresh behavior, outbox idempotency, read-only
`refresh=false`, migration from every supported schema version and DB reopen
idempotency.

No migration was added: the required fields and tables already exist. The complete
supported migration matrix is exercised from every migration version through the
current schema, followed by a second no-op migration and SQLite integrity check.

## Verification commands

- `python -m pytest tests/test_snapshot83_contract_closure.py -q`
- pertinent integration suite (provider-first, temporal, outbox, enablement,
  consumer, CFTC/Cboe, semantics and migration matrix)
- `python -m pytest -q`
- `python -m ruff check app scripts/replay_snapshot83_forensics.py tests/test_snapshot83_contract_closure.py tests/test_runtime_acquisition.py`
- `python -m compileall -q app scripts tests/test_snapshot83_contract_closure.py`
- `git diff --check`
- Windows PowerShell 5.1 AST parser over all four `.ps1` scripts
- `python -m scripts.replay_snapshot83_forensics`

The final exact test counts and commit/PR identifiers are reported in the pull
request description and delivery summary.

## Explicit exclusions

No AI/Codex/OpenAI request, provider request, web/browser request, live smoke,
service restart, trading action, execution/order action, AI-TRADER change or
operational database reset was performed. This report is an offline verification;
it does not claim live runtime validation.
