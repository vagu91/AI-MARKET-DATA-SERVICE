# Event-driven lifecycle and outbox contract

## Scope and safety

This contract is owned by AI-MARKET-DATA-SERVICE. It does not deliver events,
trade, place orders, or modify AI-TRADER. The due scanner is disabled by
default and does no work unless explicitly enabled. A missing deterministic
resolver is not treated as resolver exhaustion and can never fall through to
AI.

## Unified datum lifecycle

`compute_datum_lifecycle` is the only lifecycle calculator. Ingestion,
research persistence, the field-level gap manifest, scheduler, snapshot
projection, consumer projection, and outbox use its persisted output.

Trigger classes:

- `TRIGGER`: official macro actual/correction, FOMC decision or communication,
  earnings actual/guidance, official calendar change, new COT publication, and
  verified material breaking news.
- `REFRESH_ON_TRIGGER`: VIX/VVIX/SKEW, VIX futures, put/call, options,
  internals, cross-asset, price, and breadth.
- `CACHE_UNTIL_DUE`: current COT, schedules, upcoming earnings, published
  macro data, and structural facts.
- `NON_TRIGGERING`: anything without an explicit triggering policy.

Lifecycle timestamps are stored independently: observed, data-as-of,
published, event, validity, next refresh, and next retry. `NO_DATA` stores a
typed reason, attempted sources and fields, session/trigger, bounded retry
policy, and a deterministic negative-cache key.

Earnings are keyed by canonical issuer/ticker plus event date. A past event
without actuals is `AWAITING_ACTUAL` and gets its own bounded retry; it is not
deferred to the next issuer event. Unknown event time uses the configured
window and is never presented as an exact source time.

COT remains valid until the next configured CFTC publication in
`America/New_York`, including configured holidays/delays. Report date and
contract identity alone are partial coverage. A complete projection requires
open interest and at least one requested group with long and short positions.

## Status compatibility

The additive canonical fields are:

- `execution_status`, `execution_complete`
- `data_outcome`
- `coverage_complete`, `coverage_score`
- `missing_topics`, `blocking_gaps`, `policy_no_data_topics`
- `ready_for_trading_context`

Legacy `research_complete` and `research_partial` remain compatibility aliases
and are explicitly marked deprecated in the consumer. `research_complete` is
true only when execution and coverage are both complete.

## Persistent outbox

Only a `TRIGGER` plus a material section diff may insert
`market_context.updated`. Volatile timestamps, identifiers, telemetry, and
audit metadata do not constitute a material change. The deterministic
idempotency key prevents duplicate events for identical rematerialization.

The event stores:

`event_id`, `event_type`, trace/correlation IDs, trigger type/entity, current
and previous snapshot IDs, revision, changed sections, material fingerprints,
data-as-of, creation time, delivery status, attempt count, next attempt, and
idempotency key.

API support is intentionally limited to listing/reading and controlled
acknowledgement with consumer identity plus the exact idempotency key. There is
no delivery worker in this change.

## Central research-agent enablement

`AI_MARKET_RESEARCH_AGENTS_ENABLED` is the master authority. Thirteen typed
topic/profile/job mappings then use
`AI_MARKET_RESEARCH_AGENT_<TOPIC>_ENABLED`. The established nine agents default
to enabled; options positioning, market internals, cross-asset context, and
earnings intelligence default to disabled. The registry validates that every
specialized profile is mapped exactly once.

The manifest, coordinator, job service, repository, and worker all enforce the
same decision. A disabled topic has `required_action=NONE`,
`ai_eligible=false`, `execution_status=NOT_REQUESTED`, and
`data_outcome=DISABLED`. It creates no child/job/retry/recovery/backend/token or
web work and is not a coverage denominator or blocking gap. A queued job that
becomes disabled is terminally rejected immediately before backend execution
with `AGENT_DISABLED` and a non-retryable classification. A running job is not
interrupted. Disabling an agent never deletes its previously committed data.

The general scheduler, research scheduler, due scanner, master agent switch,
per-agent switches, and worker are separate controls. The scanner defaults to
false. Changes to a local `.env` require a later service restart; this change
does not restart the service.

## Telemetry and incidents

The shared JSON schema is
`config/service_telemetry_event.schema.json`. Payloads are bounded, structured,
key-aware redacted, control-character sanitized, and detailed only when
per-run TRACE detail is enabled. No hidden reasoning is recorded.

Backend usage is deduplicated by `invocation_id`; cached tokens remain
separate. AI duration is recorded once, service phases are exclusive, and
wall-clock is separate. CLI billing remains explicitly unavailable with its
token basis. API estimation uses only the versioned configurable pricing file.

The deterministic incident detector fingerprints and persists job/lease,
loop, early NO_DATA retry, usage, accounting, source, pending actual,
projection, readiness, future timestamp, reserved host, read-only write,
outbox lag, and CLI/API divergence signals. It never invokes AI.
An attempted disabled-agent path is also a deterministic anomaly. Telemetry
contains structured identifiers, profiles, durations, usage/cost status and
stop reasons only; it never records prompts, hidden reasoning, secrets, or
chain-of-thought.

## Required-test traceability

The 42 dedicated tests in
`tests/test_event_driven_lifecycle_outbox_observability.py`, together with the
existing domain suites, cover the required matrix:

| Requirement | Test coverage |
|---|---|
| 1–7 provider-first, NO_DATA, coalescing | dedicated lifecycle/scheduler tests; `test_gap_aware_parallel_research_finalization.py` |
| 8–11 trigger materiality and outbox | dedicated VIX/material-diff/outbox tests |
| 12–15 earnings states and bounded retry | dedicated earnings lifecycle tests; `test_semantic_actuals_agentic_runtime.py` |
| 16–20 COT cadence, coverage, projection | dedicated COT tests; `test_mnq_agentic_domains.py` |
| 21–23 AMD reconciliation, evidence reuse, field gaps | snapshot reconciliation; `test_gap_aware_parallel_research_finalization.py` |
| 24 status separation | dedicated consumer status test |
| 25–29 read-only stability, restart/lease, parity | existing refresh/read tests plus dedicated lease tests |
| 30–34 telemetry, duration, costs, redaction | dedicated telemetry/pricing/redaction tests; telemetry suites |
| 35–37 future timestamp, reserved domains, mojibake | dedicated hardening/text tests; quarantine suites |
| 38–40 migration, outbox, anomaly fingerprint | dedicated migration/outbox/anomaly tests |
| 41–42 immutable artifact and no trading surface | dedicated hash and route-surface tests |

The offline replay entry point is
`python scripts/replay_event_driven_forensics.py`; it only reads the immutable
fixture directory and records `live_calls_executed=0`.
