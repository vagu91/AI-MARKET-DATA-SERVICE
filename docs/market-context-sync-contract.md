# AI Trader market-context sync contract

Status: producer contract implemented by AI Market Data Service
Contract: `ai_trader_market_context_sync`
Schema version: `1.0`
Symbol in scope: `MNQ`

AI Market Data Service is the only owner of acquisition, validation,
quarantine, lifecycle, freshness, snapshot publication, revisioning, outbox
and ACK tracking. AI Trader must never open the producer SQLite database or
depend on its tables or migrations.

## Immutable publication model

Every committed producer state has:

```text
snapshot_id
snapshot_revision
generated_at
data_as_of
context_fingerprint
```

The global revision is monotonic. All sections returned by one full or
selective delivery come from the requested immutable snapshot revision.
Silent mixing of revisions is forbidden.

Each section has:

```text
section_revision
fingerprint
record_count
data_as_of
valid_until
freshness
status
reason
```

For every delivered section, effective `valid_until` is monotonic with the
materialized records: it cannot precede the maximum `data_as_of`,
`observed_at`, or `retrieved_at`. An inherited older expiry is clamped to that
floor and does not override a current freshness state carried by newer
delivered records. Section freshness is calculated after quarantine
withholding, from the records actually delivered.

Material fingerprints exclude telemetry and volatile acquisition fields.
Record-like lists are canonicalized independently of order. A telemetry-only
change preserves both fingerprint and section revision. A material addition,
correction, revision, invalidation or removal increments the section revision.
An identical refresh may publish a new global snapshot but does not create a
false section change.

The producer persists the exact section payload for every snapshot. Payload
size is measured, never limited. Records are not summarized, title-deduplicated,
merged across providers, truncated by count or replaced by
`compacted_item_count`.

## Section whitelist

The only selectable sections are:

```text
macro
macro_actuals
event_calendar
fed
rates
risk
vix
positioning
nasdaq
news
market_schedule
options_positioning
market_internals
cross_asset_context
earnings
earnings_intelligence
geopolitical_regulatory_risk
```

Unknown names fail with HTTP 422. No endpoint accepts SQL, table names,
arbitrary field paths or query expressions.

## Manifest

```http
GET /market-context/mnq/sync/manifest
```

The response identifies the current immutable snapshot, independently reports
Nasdaq cash and MNQ/Globex session state, and exposes all section metadata.
`AVAILABLE`, `DEGRADED`, `QUARANTINED`, `UNAVAILABLE`, `NO_DATA`, `PARTIAL`
and `BACKOFF` are producer truth; they are never converted into
consumer-missing state. For news specifically, `DEGRADED` means valid records
were delivered with explicit source/content/classification uncertainty;
`NO_DATA` requires affirmative `authentic_empty` evidence.

A normal weekend or maintenance closure remains reportable when the official
holiday-override source times out. Cash and MNQ remain separate and expose
`is_open=false`, the deterministic closure reason,
`verification_scope=BASE_WEEKLY_RULE`, and
`holiday_override_status=UNVERIFIED`. An unverified override never creates a
holiday or early-close claim, and the manifest therefore avoids `UNKNOWN` for
an unambiguous base-weekly closure.

## Synchronization plan

```http
POST /market-context/mnq/sync/plan
Content-Type: application/json
```

Request:

```json
{
  "consumer_id": "ai-trader",
  "request_id": "analysis-request-id",
  "analysis_profile": "SENIOR_MARKET_ANALYSIS",
  "required_sections": ["macro", "news", "vix"],
  "known_snapshot_revision": 91,
  "known_sections": {
    "news": {
      "section_revision": 28,
      "fingerprint": "sha256"
    }
  }
}
```

Deterministic results:

- `FULL / NO_CONSUMER_CONTEXT`: no persisted consumer inventory exists.
- `FULL / REVISION_GAP`: the base revision is not reconstructible.
- `SELECTIVE`: one or more available producer sections are missing, older or
  fingerprint-incompatible at the consumer.
- `NONE`: all requested available sections match.

`MISSING_AT_CONSUMER` means the producer has a usable section that AI Trader
does not have. Producer truth is classified exactly as
`UNAVAILABLE_AT_PRODUCER`, `STALE_AT_PRODUCER`,
`PARTIAL_AT_PRODUCER` (including delivered `DEGRADED` news) or
`QUARANTINED_AT_PRODUCER`; the response includes the real status, freshness
and reason. None of these classifications is a request to fetch an empty
replacement.

## Full snapshot

```http
GET /market-context/mnq/sync/full
```

The response contains the manifest, every whitelisted section, lineage,
per-section fingerprints, `context_fingerprint`, exact UTF-8
`payload_size_bytes`, and a global SHA-256 checksum. Readiness is recalculated
from the sections actually delivered. Empty sections remain present with
status and reason.

Only records with a concrete, deterministic reason are withheld: corrupt or
unparseable material, unrecoverable timestamps, dangerous URLs/content,
certain source-policy contradictions, insufficient collision-safe identity,
or records outside the requested temporal scope. Quarantine disclosures carry
safe fingerprints, reason codes and lineage without exposing unsafe material.
Secrets, credentialed URLs and local filesystem paths are redacted before
section persistence.

Market-news admission uses versioned `source-policy-v5`, independently from
official macro/actual policy. Trusted editorial publishers may be delivered
from one source with explicit reliability and
`confirmation.confirmed=false`. `investors.com` is an admitted editorial
publisher. `finance.yahoo.com` is distribution-only and never promotes Yahoo,
the acquisition provider, or an unknown label into a verified editorial
publisher. A safe, temporally valid Yahoo-distributed record whose original
publisher is unknown is still delivered with
`source_verification_status=UNKNOWN`, `analysis_usability=DEGRADED` and
explicit warning codes. UNKNOWN is not INVALID.

Every canonical news record keeps acquisition provider, distribution source,
original publisher and its status, source verification, headline, summary,
real full content when present, content availability, canonical URL, publish/
update/first-seen/last-seen timestamps, lifecycle, category, topics,
relevance, validation, lineage, raw source identity and reason/warning codes.
Missing publisher, headline-only or summary-only content, LOW relevance,
UNCLASSIFIED category, ambiguous topic and an in-window historical lifecycle
are quality metadata, never destructive filters.

`articles` is the complete canonical in-scope inventory. `latest`,
`historical_articles`, `directly_relevant`, `supporting`, clusters and digest
are derived views and cannot replace or truncate it. Similar stories and
temporal updates remain distinct. Only the exact same editorial occurrence
(publisher, timestamp, title and content) can be consolidated; all acquisition
and distribution occurrences remain in `source_occurrences` and
`distribution_lineage`.

After withholding, `accepted_article_count`,
`delivered_raw_article_count`, `historical_article_count`, rejected count,
digest status, context status and `usable_for_analysis` are recomputed from the
same delivered set. A digest cannot be `AVAILABLE` when zero articles are
delivered. Clusters are supplemental views and never replace admitted raw
records.

News diagnostics must balance all three equations and list the exact record id,
disposition and reason code for every non-delivered record:

```text
raw_acquired
= persisted_valid + technically_rejected

persisted_valid_in_scope
= delivered + quarantined_with_concrete_reason
  + withheld_with_concrete_reason

delivered
= current_delivered + historical_delivered
```

News readiness is `AVAILABLE` for verified usable delivery, `DEGRADED` for
usable delivery with explicit uncertainty, `NO_DATA` only with authentic-empty
proof, `UNAVAILABLE` for acquisition/materialization failure, and
`QUARANTINED` only when every candidate has a concrete blocking reason.

`checksum_scope=CANONICAL_DELIVERY_WITHOUT_MEASUREMENT_FIELDS` means the
checksum is computed from canonical JSON after omitting only `checksum` and
`payload_size_bytes`. The size is the exact canonical UTF-8 byte length of the
final response, including both measurement fields.

There is no 90 KB ceiling or any other payload ceiling. HTTP infrastructure
in front of this API must be configured for multi-megabyte responses.

## Selective delivery

```http
POST /market-context/mnq/sync/sections
```

```json
{
  "consumer_id": "ai-trader",
  "target_snapshot_revision": 92,
  "sections": ["news", "vix", "market_internals"],
  "include_lineage": true
}
```

All returned sections belong to revision 92. If revision 92 is unavailable,
the producer returns `status=RESYNC_REQUIRED` and
`requires_full_resync=true`; it never substitutes the latest revision.

For news, a record delta is allowed only when the consumer's persisted ACK can
be joined to the exact delivery, snapshot revision and payload checksum. The
delta carries consumer id, acknowledged base delivery/checksum and base/target
section revisions. It contains only new records, material content/publisher/
timestamp corrections and lifecycle changes. Absence alone is disclosed as
unconfirmed and never deletes a record. Without an auditable ACK binding the
producer sends a full section resync.

## Delta manifest

```http
GET /market-context/mnq/sync/changes?since_revision=91
```

The response compares the reconstructible base manifest with the current
manifest and returns changed section revisions only. It carries no full
context. A missing base yields `REVISION_GAP` and full resync.

## Asynchronous refresh

```http
POST /market-context/mnq/sync/refresh
GET  /market-context/mnq/sync/requests/{work_id}
```

Refresh never keeps the HTTP request open for provider or AI work. If committed
data already satisfies a non-trigger request, the producer returns 200
`READY`. Otherwise it persists work and returns 202 `IN_PROGRESS`, or
`WAITING_BACKOFF` without creating a bypass job.

When the service scheduler is enabled, its sync refresh worker leases one
durable generation at a time, executes the deterministic provider-first
runtime with `refresh=auto`, commits a new atomic snapshot, and only then marks
the work complete. Failures enter persistent exponential backoff. Expired
leases and due backoff rows are reclaimed after restart. This worker never
authorizes or invokes AI by itself.

`force=true` has no meaning in this contract and cannot bypass lifecycle,
authorization, backoff, negative cache, idempotency or single-flight.

### Single-flight

The persistent key space is symbol, generation, reason and normalized section
set. Equivalent requests attach a durable waiter to the same `work_id`.
Ten equivalent requests therefore create one work row and ten waiters.

If pending work covers `news + vix` and a request asks for
`news + vix + market_internals`, the pending work is atomically expanded only
with the residual section. If a generation is already running, its inputs are
immutable; uncovered residual work is assigned to the next generation and
linked with `parent_work_id`. Restart recovery reads work and waiters from
SQLite; it does not depend on in-memory futures.

Completion must commit one coherent snapshot, its section rows and at most one
coalesced outbox event in the same transaction. A rollback creates no visible
notification. Material work arriving during generation N is never inserted
retroactively into N and must remain queued for N+1.

Provider-first schedule discovery uses the same transaction boundary: canonical
calendar components, snapshot, section revisions, lifecycle rows and any
outbox row become visible together. Full and selective sync therefore read the
same revision and fingerprint immediately after a material catch-up discovery.

## Trigger policy

Notification-producing triggers include macro actuals and revisions, FOMC
decisions, material Fed communication, material news, geopolitical or
regulatory developments, earnings actuals or revisions, data invalidation and
material schedule changes.

VIX, VVIX, options positioning, market internals, cross-asset, breadth and fast
quotes are refresh-on-trigger domains. They do not independently notify every
move. A material trigger refreshes the trigger domain plus configured fast
domains, commits one snapshot and emits one coalesced notification containing
all changed sections.

## Outbox notification

Notifications are lightweight and created transactionally with the committed
snapshot:

```json
{
  "event_type": "MARKET_CONTEXT_UPDATED",
  "contract_version": "1.0",
  "delivery_id": "outbox-id",
  "symbol": "MNQ",
  "base_revision": 91,
  "target_revision": 92,
  "checksum": "outbox-payload-sha256",
  "checksum_scope": "OUTBOX_MATERIAL_PAYLOAD",
  "changed_sections": ["news", "vix"],
  "triggers": [
    {
      "type": "NEW_MATERIAL_NEWS",
      "entity_id": "record-id",
      "occurred_at": "2026-07-25T20:06:20Z"
    }
  ],
  "manifest_url": "/market-context/mnq/sync/manifest",
  "changes_url": "/market-context/mnq/sync/changes?since_revision=91",
  "created_at": "2026-07-25T20:06:20Z"
}
```

The notification never contains the full context. Retry records delivery
attempts; it does not resend a full snapshot. The same outbox idempotency key
does not create duplicate deliveries. Attempts use bounded exponential backoff;
attempt eight transitions that consumer target to `DEAD_LETTER` instead of
silently deleting it. Multiple material causes remain in the `triggers` array.

Notification inspection is available at:

```http
GET /market-context/mnq/sync/deliveries/{delivery_id}
```

## ACK and consumer state

```http
POST /market-context/mnq/sync/ack
GET  /market-context/mnq/sync/consumers/{consumer_id}
```

ACK request:

```json
{
  "consumer_id": "ai-trader",
  "delivery_id": "outbox-id",
  "snapshot_revision": 92,
  "status": "PERSISTED",
  "checksum": "outbox-payload-sha256",
  "section_revisions": {
    "news": 29,
    "vix": 44
  },
  "acknowledged_at": "2026-07-25T20:07:00Z"
}
```

The producer accepts an ACK only for the consumer-specific target that was
actually notified. It validates delivery existence, consumer id, snapshot
revision, exact 64-hex payload checksum, clock ordering and the exact
changed-section revision set: wrong-consumer, wrong-revision, wrong-checksum,
partial `PERSISTED` claims and impossible section revisions are rejected.
Identical replay is idempotent; conflicting replay is HTTP 409. A late ACK for
a superseded delivery is retained without regressing the consumer's latest
acknowledged snapshot or section inventory. ACK and consumer-specific delivery
state survive restart.
`PERSISTED` means only that AI Trader declares an atomic local save. It does not
mean Senior Analyst has run, approved or interpreted the data.

Without ACK, the delivery remains pending and eligible for notification retry.
The producer must not infer consumer possession from notification send alone.

The former global endpoint
`POST /market-context/outbox/events/{event_id}/ack` is explicitly deprecated
and returns HTTP 410. It cannot close delivery state for all consumers. New and
future consumers must use `/market-context/mnq/sync/ack`.

## Legacy consumer transition

`GET /market-context/mnq/consumer` remains temporarily available as the
analysis-oriented schema-2.1 projection and emits `Deprecation: true` plus a
successor link. It is not a synchronization or persistence source and the
future AI Trader must not use it. Its remaining record arrays are not silently
top-N truncated, but only the sync endpoints guarantee the complete immutable
producer projection, per-section revisions and checksums.

## Calendar and recovery

The canonical calendar is previous/current/next Monday-Sunday in
`America/New_York`. No event is removed by count, impact or byte budget.

```text
counts.total
= coverage.retained_count
= sum(bucket.event_count)
= sum(len(bucket.events))
```

An empty bucket without affirmative provider coverage is
`UNVERIFIED_EMPTY`; the projection exposes partial source coverage rather than
claiming that no events exist. Past missing actuals remain
`AWAITING_ACTUAL`; the producer never invents an actual. Restart catch-up must
walk missing occurrences provider-first, preserve occurrence identity and
publish only after one atomic realignment.

## Snapshot pinning

The consumer must persist and pass these identifiers to its deterministic
analysis coordinator:

```text
analysis_request_id
snapshot_id
snapshot_revision
section_revisions
context_fingerprint
```

Senior Analyst starts only after the requested sync is durably complete. Its
context is immutable for the analysis lifetime. New material data causes
consumer-side invalidation or a new analysis; it is never injected into an
analysis already running.

## Degraded but usable sections

Readiness is derived only from the delivered section payload and its persisted
`sync` metadata. A `PARTIAL` or stale section with a nonzero delivered record
count is usable with disclosure: it appears in both `sections_available` and
`sections_degraded`. It is not placed in `sections_unavailable`. A quarantined,
provider-unavailable or zero-record no-data section remains unavailable.
`READY` still requires no degraded or unavailable sections, so this distinction
does not inflate the overall decision.

`market_schedule` preserves Nasdaq cash and MNQ/Globex as separate sessions.
When the CME holiday cross-check is unavailable, an accepted, versioned weekly
Globex rule remains deliverable with its CME trading-hours URL and lineage.
The section is `PARTIAL`, with the missing cross-check disclosed; it is not
converted to `QUARANTINED` merely because official holiday verification is
temporarily unavailable.

News projection reads the complete selected DB interval without SQL `LIMIT`.
Publisher and distributor are independent fields. Quarantined rows may be
included as diagnostic candidates, but a rejected source-policy outcome can
never enter accepted news. Each rejection retains title, available content,
URL, timestamps, publisher, distributor, lineage, policy outcome and exact
reason. Technical deduplication requires an identical record identity and
content; temporally distinct Reuters updates remain distinct.
