# PR 23 adversarial forensic review — 2026-07-26

## Scope and immutable baseline

Review range:

```text
40c73884f17c8166be7c9cca3fddb68405342d5e
..
ac6e5d9e2f947ece5b1536d72882e31cdbdd0dca
```

The initial local HEAD matched the remote head of open, non-draft, unmerged PR
#23 on `codex/market-context-producer-sync`; the base matched the requested
commit. The review used only repository fixtures, temporary SQLite databases
and offline replay artifacts. It did not read `.env`, open or mutate the
operational database, call providers, browsers or AI, deliver notifications,
start schedulers, restart services, trade, place orders or modify AI Trader.

The untracked protected artifact remained outside Git with SHA-256:

```text
BCED28DECDF98D65AF9843C3CF3FF23DAB0A164C721B8BEF9B3E0D7697699DD4
```

## Blocking findings and root causes

| Severity | Finding | Root cause | Closure |
| --- | --- | --- | --- |
| blocker | Full sync omitted material event and sentiment roots | section extraction covered only the older projections | all material calendar, event, sentiment and risk roots are mapped |
| blocker | Rejected/quarantined provider records were exposed inside otherwise valid sections | source sanitation was not a contract-level withholding boundary | recursive withholding plus aggregate disclosure; rejected material never enters a usable section |
| blocker | `PARTIAL` and `UNVERIFIED_EMPTY` could produce `READY` | readiness treated several degraded states as available | readiness now derives strict producer classifications from the delivered payload |
| blocker | Partial or wrong-consumer ACK could be accepted and global outbox state could be closed | ACK was global and did not bind a notified consumer to the exact changed-section set | consumer-specific delivery targets, exact-set validation and deprecated HTTP 410 global ACK |
| blocker | A completed refresh replay returned an in-progress shape | waiter replay did not branch on terminal state | terminal replay returns HTTP 200 `COMPLETED` with the committed revision |
| high | Same request id with a different body was accepted | idempotency was keyed only by consumer/request id | canonical request fingerprint conflict is HTTP 409 |
| blocker | Fingerprints changed for equivalent numbers/timezones and could miss A→B→A history | JSON representation and endpoint comparison were insufficiently canonical | decimal and UTC canonicalization; changes compare section revision as well as current fingerprint |
| blocker | Section backfill could replace immutable historical content | `INSERT OR REPLACE` allowed mutation | insert-once plus exact immutable read-back conflict |
| blocker | Full construction could read a later manifest than its selected rows | full issued separate latest reads | selected revision is pinned and the manifest is requested explicitly for it |
| blocker | Snapshot/section/outbox failure boundaries lacked deterministic rollback evidence | no fault injected at the outbox boundary | abort trigger proves the entire write transaction rolls back |
| blocker | Single-flight and lease ownership were process-ambiguous | caller-provided lease owner could collide and long work had no heartbeat | unique lease owner per run, persistent heartbeat, expired-lease recovery and cross-instance contention tests |
| high | Coalescing preserved only one trigger cause | worker/outbox reduced trigger metadata to one type | every persisted reason is retained and emitted in deterministic trigger envelopes |
| blocker | Delivery retry had no consumer-specific terminal state | attempt counters were global and unbounded | per-consumer attempts, exponential backoff and explicit dead-letter at attempt eight |
| high | Work status exposed waiter identifiers and internal lease state | internal row shapes were returned directly | public status returns counts and safe fields only |
| high | Debug-derived paths and credentialed URLs could be reflected | generic redaction did not cover filesystem paths or URL userinfo | local paths, secret query values, URL credentials and sensitive fields are redacted |
| high | The schema-1 compatibility projection retained destructive top-N slices | old “compact” helpers silently discarded arrays | legacy arrays and source fields are preserved; diagnostics remain sanitized |
| blocker | The legacy global outbox ACK remained wired in production | old route mutated global delivery status | route is explicitly deprecated and returns HTTP 410 |

## Snapshot 91 preservation evidence

`scripts/replay_snapshot91_sync_offline.py` reads the controlled redacted
snapshot artifacts and writes only a temporary database. It compares validated
source records with delivered full-sync records using provider, source, record
and provider record ids, occurrence, event/article/issuer/claim ids, version,
event/published timestamps and material fingerprint.

Observed:

```text
source validated identities        118
delivered identities               118
missing / extra identities         0 / 0
material-content mismatches        0
quarantined source fingerprints    207
quarantined material exposed       0
full UTF-8 bytes                    916011
calendar candidates / retained     30 / 30
calendar omitted                   0
AWAITING_ACTUAL                    2
provider / AI / delivery calls     0 / 0 / 0
operational database writes        0
```

The two July 24 actuals remain absent rather than being inferred. Previous week
remains explicitly `UNVERIFIED_EMPTY` through partial source coverage.

## Atomicity, concurrency and recovery evidence

- Full and selective rows are selected from one immutable snapshot revision.
  A deterministic interleaving commits revision N+1 between full row selection
  and manifest construction; the response remains entirely revision N.
- A SQLite abort injected before outbox insert leaves no revision N+1 snapshot,
  no revision N+1 section row and no outbox event.
- Ten simultaneous requests through ten service instances create one work row
  and ten durable waiters. Ten simultaneous lease claims yield one owner.
- Pending overlap is unioned once. Running input is immutable and residual or
  new-trigger work receives the next generation with `parent_work_id`.
- Work in backoff absorbs overlapping waiters without clearing its retry time.
  Expired leases and due backoff are reclaimable after restart.
- A heartbeat renews the persistent lease; lease loss prevents unauthorized
  completion or backoff mutation.
- Concurrent identical ACKs produce one insert and idempotent replays. An abort
  injected before ACK insert leaves the target `NOTIFIED`. Wrong consumer,
  partial persistence, wrong revision, pre-delivery time and future time fail.
- Superseded ACK state is retained without rolling the consumer inventory back.
  Notification failure reaches consumer-specific `DEAD_LETTER` after eight
  attempts and is no longer falsely counted as pending.

## Payload and contract evidence

Repository-backed tests preserve exact multi-megabyte Unicode arrays and verify
UTF-8 byte size, deterministic checksum after service restart, whitelist
enforcement and zero semantic compaction. The maximum exercised full response
is greater than 5 MB. Future AI Trader and reverse proxies must permit
multi-megabyte JSON responses; gzip may be negotiated at the HTTP layer but is
not required and must remain lossless.

Control bodies are separately limited to 256 KiB and reject unknown fields.
That safety limit applies only to plan/refresh/ACK metadata, never to full or
selective market data.

## Migration 21 review

Migration 21 is additive and registers immutable section state, durable
generation/waiter state, consumer-specific delivery targets, ACKs, attempts and
outbox metadata. Checks constrain generation, statuses, attempts and consumer
identifier lengths; foreign keys bind snapshots, deliveries and parent work.
The active-work, section-manifest, section-revision, request-fingerprint and
delivery-due paths are indexed.

The migration matrix constructs and upgrades every schema version 1 through 20
to 21, then reopens schema 21 idempotently. All work uses test databases.

## Wiring and transition

The versioned routes traverse FastAPI route → sync service → snapshot
repository/SQLite. Migration 21 is registered. Refresh remains asynchronous and
the worker runs only under existing scheduler configuration; this review did
not enable it.

`/market-context/mnq/consumer` is now explicitly deprecated and emits a
successor link to `/sync/full`. It remains a compatibility analysis projection,
not a complete synchronization source. The future AI Trader must use only the
versioned sync contract. The former global outbox ACK returns HTTP 410.

## Verification results

Final command results are recorded after the complete verification pass:

```text
targeted protocol/adversarial tests   43 passed
offline snapshot 91 replay            passed
maximum payload                       >5 MB Unicode
migration matrix                      22 passed (1..20→21, 21→21)
complete pytest suite                 1698 passed
Ruff                                  passed
py_compile / compileall               passed
git diff --check                      passed
PowerShell 5.1 parser                 4 files, 0 errors
```

## Residual risks

The snapshot artifacts cannot create source records that were never acquired:
previous-week coverage and the two missing actuals require a future
provider-first refresh after deployment. That is an evidence gap, not permission
to synthesize data. Live delivery remains disabled, so integration with the
future AI Trader, client-side atomic persistence and reverse-proxy sizing remain
future acceptance work. These are deployment/integration risks rather than open
producer blockers; the reviewed producer invariants and final checks are green.
