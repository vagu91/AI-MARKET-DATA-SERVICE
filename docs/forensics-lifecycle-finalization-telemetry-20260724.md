# Lifecycle finalization and telemetry forensic closure

Date: 2026-07-24

Base commit: `337d6376df52f6d1611b4fc7dd1dbbf767bddd5f`

Observed batch: `manual-provider-only-20260724T170650Z`

## Protected evidence

The operational database was never opened for writing, copied, reset, restored or
passed to a test. The offline investigation used the supplied backup:

- `data/market_data_service-pre-provider-only-20260724T170650Z.sqlite`
- SHA-256:
  `E1017E5C567711FF59370CB27B34A576544A1A453F2339FC99BD0828AA5EFF71`

The local `.env` and `ai-trader-consumer-payload.json` were not modified. The
consumer artifact remained untracked.

## Exact root causes

1. `LifecycleRepository.upsert()` and
   `persist_lifecycle_in_transaction()` changed `work_status` on conflict but
   did not clear `lease_owner`, `lease_expires_at` and `heartbeat_at`. The
   snapshot transaction therefore atomically committed `COMPLETED/FRESH` while
   retaining the lease that had selected the item.
2. `MarketContextSnapshotRepository._persist_projected_lifecycle()` replays all
   projected earnings lifecycle records whenever a snapshot is saved. Its
   shared UPSERT always executed the conflict update and always replaced
   `updated_at`, even when lifecycle and payload were materially identical.
3. `ResearchSchedulerService.scan_due_items()` incremented `provider_calls` for
   every resolver invocation and emitted `event_name=provider_call` before it
   knew whether an adapter had performed I/O. A committed-payload short circuit
   was therefore reported as a provider request.
4. Committed-payload revalidation delegated freshness entirely to
   `compute_datum_lifecycle()`. A `NO_DATA` envelope contains diagnostic strings
   such as `reason`, `fields_attempted` and `sources_attempted`; the generic
   material-data check treated those strings as data. With a future
   `valid_until`, the envelope was incorrectly classified `FRESH` despite
   `value=null`, empty lineage and no operational datum.

The two supplied backup rows confirm root cause 4:

- `MNQ:nasdaq_100`: `status=NO_DATA`, `value=null`,
  `reason=no_fresh_verified_source`, empty `source_lineage_json`;
- `MNQ:geopolitical_regulatory_risk`: the same non-operational envelope shape.

## Closure

- Both lifecycle persistence paths compare all lifecycle fields plus a
  deterministic, volatile-field-stripped payload fingerprint before updating.
  A semantic no-op preserves both `created_at` and `updated_at`.
- Every conflict write performed by those finalization paths clears all three
  lease columns in the same SQL statement whenever the resulting status is not
  `LEASED`.
- Snapshot, outbox, resolved datum and final lifecycle remain in one SQLite
  transaction. Partial provider data is first finalized as `PARTIAL` with the
  lease released, then may advance from that unleased state to `QUEUED` or its
  bounded terminal status.
- Resolver and provider telemetry now use distinct events:
  `resolver_evaluation`, `committed_payload_hit`,
  `provider_request_attempted`, `provider_request_completed`,
  `provider_request_failed`, `provider_cache_hit` and
  `provider_negative_cache_hit`.
- Scheduler counters now distinguish `resolver_evaluations`,
  `committed_payload_hits`, `actual_provider_requests`,
  `successful_provider_requests`, `failed_provider_requests`,
  `ai_invocations` and `ai_jobs_created`. The compatibility field
  `provider_calls` now equals actual provider requests, not resolver
  evaluations.
- A committed payload is accepted without provider I/O only when it has an
  operational requested value, non-rejected source evidence, sufficient
  lineage and current temporal validity. Deterministic reason codes are:
  `committed_payload_operationally_fresh`,
  `committed_payload_temporally_valid_but_not_operational`,
  `committed_payload_no_data_envelope` and
  `committed_payload_source_unverified`.

`committed_payload_hits` counts persisted payloads that were found and
evaluated. Acceptance or rejection is reported separately by the deterministic
reason code. This preserves the forensic count of two committed payload hits
without misclassifying the two `NO_DATA` envelopes as operational data.

## Offline before/after

| Invariant | Observed before | Redacted offline replay after |
|---|---:|---:|
| claimed lifecycle items | 2 | 2 |
| lifecycle rows changed | 9 | 2 target rows only |
| unrelated rows touched only in `updated_at` | 7 | 0 |
| terminal rows retaining leases | 2 | 0 |
| `resolver_evaluations` | not distinguished | 2 |
| `committed_payload_hits` | not distinguished | 2 |
| `actual_provider_requests` | reported as 2 | 0 |
| legacy `provider_call` events | 2 | 0 |
| AI invocations | 0 | 0 |
| AI jobs created | 0 | 0 |
| snapshot delta | 1 | 0 (bounded maximum: 1) |
| NON_TRIGGERING outbox delta | 0 | 0 |
| consumer artifact | unchanged | unchanged |

Standalone replay:

```powershell
python -m scripts.replay_provider_only_lifecycle_forensics `
  --workspace "$env:TEMP\provider-only-lifecycle-replay"
```

The replay uses only
`tests/fixtures/provider_only_lifecycle_forensic_redacted.json` and a temporary
SQLite database.

## Post-merge reconciliation command

The reconciliation was prepared but **not executed** against the operational
database. It is dry-run by default:

```powershell
python -m scripts.reconcile_terminal_lifecycle_leases `
  --database data/market_data_service.sqlite
```

After separate authorization, the idempotent apply form is:

```powershell
python -m scripts.reconcile_terminal_lifecycle_leases `
  --database data/market_data_service.sqlite `
  --apply `
  --audit-output data/diagnostics/lifecycle-terminal-lease-reconciliation.json
```

The apply statement is restricted to rows where `work_status <> 'LEASED'` and
changes only `lease_owner`, `lease_expires_at` and `heartbeat_at`. It does not
change `updated_at`, payloads, snapshots, outbox rows or history.

## Verification

- focused lifecycle finalization tests: 22 passed;
- lifecycle/telemetry suite: 81 passed;
- pertinent integration suite: 375 passed;
- complete repository suite: 1,436 passed;
- standalone redacted replay: passed;
- Ruff: passed;
- compileall: passed;
- `git diff --check`: passed.

No live provider, AI/Codex/OpenAI, browser/web, smoke, restart, migration,
AI-TRADER, trading, execution or order operation was performed.
