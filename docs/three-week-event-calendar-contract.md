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

Schema 20 is sufficient. `economic_events_history` remains the historical
occurrence store, while `datum_lifecycle_items` provides the persistent
checkpoint, due time, retry/backoff, lease, heartbeat, and payload. Snapshot
materialization atomically seeds visible scheduled occurrences into the
lifecycle table. A future occurrence is idle until its exact release; a past
occurrence without an actual is `AWAITING_ACTUAL` and due.

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

## Consumer limit and offline verification

The Consumer 2.1 projection keeps compact source/domain lineage and deterministic
ordering by scheduled time, descending impact, then occurrence ID. Configurable
impact and count limits produce explicit coverage and overflow metrics.
Retention is bucket-aware: every nonempty week first receives a minimum quota,
then HIGH-impact and bucket-specific priorities are applied (published/revised
previous-week actuals, current-day/awaiting-actual occurrences, and next-week
HIGH events). Each bucket exposes `candidate_count`, `retained_count`, and
`omitted_count`. Any omission prevents `COMPLETE`; if even one item per nonempty
bucket cannot fit, coverage is `DEGRADED` with
`byte_budget_insufficient_for_nonempty_bucket_minimum`. The complete consumer
remains below 90,000 UTF-8 bytes.

`scripts/replay_three_week_event_calendar_offline.py` verifies the three buckets,
session split, deterministic replay, byte size, and zero provider, AI, browser,
delivery, and trading calls.

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

Residual limits are intentional and fail closed:

- a CME document without the supported structured equity-index payload is
  discovered but not parsed or session-verified;
- FOMC/earnings recovery requires its dedicated configured deterministic
  adapter; lack of an adapter cannot fall through to BLS/BEA or AI;
- an absent event without an admitted explicit tombstone remains indefinitely
  unconfirmed rather than being guessed as cancelled;
- catch-up makes progress only while the service process is running, but schema
  20 state survives restarts and resumes without manual cursor reconstruction.
