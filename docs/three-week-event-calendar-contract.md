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
metadata, trigger class, and full debug lineage. Consumer values use JSON
`null`; empty-string and placeholder numeric values are normalized to `null`.

Release states are:

`SCHEDULED`, `AWAITING_RELEASE`, `AWAITING_ACTUAL`, `PUBLISHED`, `REVISED`,
`POSTPONED`, `CANCELLED`, and `UNAVAILABLE`.

`NO_DATA` remains an acquisition outcome and is not an event-existence state.

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
4. processes bounded batches with persistent leases/checkpoints;
5. commits the batch in one snapshot and at most one outbox envelope;
6. never queues residual AI work implicitly.

The default lookback is 730 days. Visible/recent and high-impact items are
prioritized. Reconciled history outside the notification horizon is persisted
without an outbox notification.

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

## Consumer limit and offline verification

The Consumer 2.1 projection keeps compact source/domain lineage and deterministic
ordering by scheduled time, descending impact, then occurrence ID. Configurable
impact and count limits produce explicit coverage and overflow metrics. The
complete consumer remains below 90,000 UTF-8 bytes.

`scripts/replay_three_week_event_calendar_offline.py` verifies the three buckets,
session split, deterministic replay, byte size, and zero provider, AI, browser,
delivery, and trading calls.
