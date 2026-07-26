# Three-week event calendar contract

AI-TRADER Consumer 2.1 exposes scheduled information in
`event_calendar_window`. Unscheduled news remains in the recent news/event-risk
views and is never projected as a future calendar occurrence.

## Window semantics

The semantic timezone is `America/New_York`. Every generation contains exactly
three complete Monday-Sunday buckets:

- `PREVIOUS_WEEK`
- `CURRENT_WEEK`
- `NEXT_WEEK`

The boundaries are timezone-aware, so a week may contain a daylight-saving
transition. Date-only events remain date-only: `scheduled_at` is the ISO date,
`scheduled_at_utc` is `null`, and `scheduled_time_precision` is `DATE`. The
service does not invent a release time. Ambiguous, nonexistent, invalid, or
timezone-less local timestamps that cannot be localized safely are rejected
from the operational projection and recorded under the debug audit.

Past events in the current week remain in `CURRENT_WEEK`. Future events never
receive a fabricated actual and do not create AI work merely because the actual
is absent.

## Canonical occurrence and statuses

An occurrence has a stable `occurrence_id`, a separate `event_id`, local and UTC
schedule fields, one week bucket, temporal flags, values, source/freshness
metadata, trigger class, `lifecycle_entity_type`, `outcome_contract`, and full
debug lineage. Consumer values use JSON
`null`; empty-string and placeholder numeric values are normalized to `null`.

Release states are:

`SCHEDULED`, `AWAITING_RELEASE`, `AWAITING_ACTUAL`, `PUBLISHED`, `REVISED`,
`POSTPONED`, `CANCELLED`, and `UNAVAILABLE`.

`NO_DATA` remains an acquisition outcome and is not an event-existence state.

### Fail-closed lifecycle classification

The central occurrence classifier has no default-to-macro branch:

- numeric macro indicators use `macro_actual`;
- issuer result occurrences use `earnings_actual`;
- FOMC decisions use `fomc_decision`;
- FOMC statements, minutes, speeches, and equivalent scheduled publications
  use `fomc_communication`;
- scheduled regulatory and geopolitical events use `schedule_only`;
- an unknown or unverifiable outcome contract also uses `schedule_only`.

`schedule_only` rows are persisted as `IDLE`, have no attempted outcome fields,
and are excluded from due claims. They cannot reach a provider or residual AI
path. The macro actual adapter is therefore unreachable for earnings,
regulatory, geopolitical, and unknown scheduled occurrences.

## Persistence and catch-up

Schema 21 is sufficient; this closure adds no migration.
`economic_events_history` remains the historical
occurrence store, while `datum_lifecycle_items` provides the persistent
checkpoint, due time, retry/backoff, lease, heartbeat, and payload. Snapshot
materialization atomically seeds visible scheduled occurrences into the
lifecycle table. A future occurrence is idle until its exact release; a past
occurrence without an actual is `AWAITING_ACTUAL` and due.

At startup and on every provider-only catch-up tick, the service first computes
the canonical previous/current-week interval and asks the deterministic
calendar service for that exact interval. Validated schedule gaps are persisted,
including `schedule_only` occurrences; existing rows are unchanged. The
acquisition itself uses a five-minute persistent single-flight lease in
`provider_state`. All-provider failure records `PROVIDER_UNAVAILABLE` and a
persistent retry time. A second tick inside that backoff performs no provider
call. Successful empty results are `VERIFIED_COMPLETE` only when at least one
configured provider completed without errors.

Event catch-up is separately opt-in and disabled by default. It:

1. revalidates committed payload/cache;
2. uses deterministic and official provider adapters;
3. resolves the exact occurrence;
4. processes batches bounded by `batch_size`, up to `max_per_tick`;
5. commits the batch in one snapshot and at most one outbox envelope;
6. never queues residual AI work implicitly.

Catch-up state is stored in the existing schema-20 `provider_state` table. The
checkpoint contains `backlog_before`, `claimed`, `resolved`, `backoff`,
`backlog_after`, `pending_backoff`, `cursor`, `tick_count`, and
`completion_status`. A dedicated provider-only application task continues
bounded ticks independently of the AI-authorized lifecycle scanner. A completed
checkpoint with no newly due work produces a read-only `ALREADY_COMPLETE`
result: no snapshot, outbox, telemetry, or checkpoint rewrite.

The default lookback is 730 days. Visible/recent and high-impact items are
prioritized. Reconciled history outside the notification horizon is persisted
without an outbox notification. Temporary provider failures enter deterministic
backoff/negative-cache state and remain in `WAITING_BACKOFF`; they never enable
AI.

## Trigger and session semantics

Semantic triggers include first actual publication, material actual revision,
cancellation, postponement, material schedule changes, newly observed
high-impact future events, and optionally material consensus changes. VIX,
quotes, candles, option chains, market internals, cross-asset ticks, TTL refresh,
and idempotent repeats do not trigger on their own. A batch envelope carries
sorted `changed_event_ids`, causes, and a deterministic fingerprint.

`market_schedule` exposes distinct `nasdaq_cash_session` and
`mnq_futures_session` objects, including open state, closure reason, holiday,
early-close/maintenance classification, and next open. A closed trading session
does not block macro occurrence persistence or outbox creation.

Calendar disappearance alone is not cancellation. A previously visible
occurrence missing from a later calendar is recorded as
`UNCONFIRMED_REMOVAL`, with its previous occurrence and comparison lineage, and
does not trigger. It becomes `REMOVED_FROM_CALENDAR` and may trigger
`EVENT_CANCELLED` or `EVENT_POSTPONED` only when an admitted source supplies an
explicit cancellation/postponement confirmation. Unconfirmed tombstones remain
available across subsequent snapshots so a later confirmation can reconcile
them.

### CME session provenance

CME provenance has three independent flags:

- `official_document_discovered`;
- `official_schedule_parsed`;
- `session_state_verified`.

Finding an official page or linked document does not verify a daily MNQ state.
The deterministic `cme_equity_index_schedule_v1` parser normalizes equity-index
futures closures, late opens, early closes, and maintenance overrides from a
structured official payload. `data_origin_is_official=true` is emitted only
when the requested date is inside the parsed coverage. If the schedule is
missing or unparseable, static Globex rules remain explicitly unverified; on a
holiday-sensitive weekday MNQ becomes `UNKNOWN` with
`UNVERIFIED_HOLIDAY_SCHEDULE` instead of being invented as open.

## Lossless consumer projection and source coverage

The canonical projection is lossless and deterministically ordered by scheduled
time, descending impact, then occurrence ID. Configuration fields retained for
backward compatibility do not cap this projection. There is no event-count
limit, byte budget, top-N, impact-floor reduction, overflow removal, mandatory
summary, or destructive compaction. The builder reads the complete calendar
lists plus the pre-consumer `events_today`, `next_24h_events`,
`next_7d_critical_events`, `recently_released_events`, and
`upcoming_high_impact_events` views; the legacy projected window is never its
only source.

Coverage exposes, globally and per bucket:

- `source_candidate_count`;
- `validated_occurrence_count`;
- `delivered_occurrence_count`;
- `delivered_valid_source_record_count`;
- `quarantined_occurrence_count`;
- `invalid_temporal_count`;
- `exact_duplicate_count`;
- `omitted_for_size_count=0`;
- `omitted_for_count_count=0`;
- `unexplained_loss=0`;
- `source_coverage_status`.

Permitted source coverage states are `VERIFIED_COMPLETE`, `PARTIAL`,
`UNVERIFIED_EMPTY`, `PROVIDER_UNAVAILABLE`, and `QUARANTINED`. An empty bucket
without affirmative provider evidence is `UNVERIFIED_EMPTY`; non-empty data
alone does not prove complete acquisition.

Occurrence identity never uses array position or normalized-title similarity.
Different timestamps remain different occurrences. Multiple source records for
one occurrence remain in `source_evidence` with provider IDs, URLs, retrieval
time, timezone, validation and field lineage. Only a content-identical record
with the same deterministic occurrence/source identity is a technical
duplicate.

`scripts/replay_snapshot91_sync_offline.py` reconstructs the snapshot-91 window
from the complete pre-consumer lists, refreshes the sync section in memory, and
verifies the candidate equation, exact payload size, historical-news
consistency, and zero provider, AI, or operational-database side effects.

## Temporal admission

Operational macro-calendar admission validates the timestamp, weekday,
reference period, timezone, precision, source validation and occurrence
ambiguity. Invalid rows remain under `audit.quarantined_occurrences` with a
specific code and are excluded from delivered events. Current codes include
`RELEASE_ON_IMPLAUSIBLE_WEEKEND`,
`REFERENCE_PERIOD_AFTER_RELEASE_DATE`,
`PERIOD_RELEASE_DATE_INCONSISTENT`, `SCHEDULE_DATE_UNVERIFIED`, and
`SOURCE_OCCURRENCE_AMBIGUOUS`. A weekend occurrence is admitted only when its
semantics explicitly permit a weekend release.

The BLS production parser consumes the official list view, whose rows contain a
full date, time and reference period. It requires the full date to match the
requested year/month, preventing the leading/trailing adjacent-month cells of
the month grid from being remapped to day 1 or 2 of the wrong month.

## Forensic blocker closure

Root causes before this revision:

1. snapshot projection hard-coded every calendar row to `macro_actual`;
2. startup catch-up returned after one bounded call and did not persist its
   reporting cursor;
3. CME document discovery was treated as daily-session verification;
4. global retention could empty an entire week;
5. comparison visited only occurrences still present in the new window.

After this revision, classification and resolver routing are typed and
fail-closed, catch-up resumes from persistent lifecycle/checkpoint state on
automatic provider-only ticks, CME daily provenance is explicit, retention
preserves week coverage when feasible, and removed IDs retain auditable
confirmation state.

Residual fail-closed behavior:

- a CME document without the supported structured equity-index payload is
  discovered but not parsed or session-verified;
- FOMC/earnings recovery requires its dedicated configured deterministic
  adapter; lack of an adapter cannot fall through to BLS/BEA or AI;
- an absent event without an admitted explicit tombstone remains indefinitely
  unconfirmed rather than being guessed as cancelled;
- catch-up makes progress only while the service process is running, but schema
  21 state survives restarts and resumes without manual cursor reconstruction.
