# Event-driven lifecycle forensic report — 2026-07-24

## Scope and evidence

This report records the offline, pre-code-change reproduction of the agentic-domain
smoke run. The local JSON artifacts under
`data/market-research-smoke-agentic-domains-20260724` are the only runtime
evidence used. No service, provider, browser, AI backend, or web call was made.

- Parent run: `prun-68a91ef9-dcd0-4025-a848-6b396a96bd10`
- Snapshot: `mcs-e75788ce-09fe-47b0-b70e-870bc1c624f3`
- Requested snapshot revision: `82`
- Baseline commit: `099f7a4e3008ad52474a784c0d96739f1da1927b`
- Consumer artifact SHA-256:
  `BCED28DECDF98D65AF9843C3CF3FF23DAB0A164C721B8BEF9B3E0D7697699DD4`
- Database backup SHA-256:
  `98F3DF7D08B15980C5EEBA029466FA6AEAF3ED4F4B44CA60E75F698E44F42059`

## Reproduced observations

| Observation | Offline evidence |
|---|---|
| Ten AI backend invocations produced only three accepted claims | `summary.json`: `backend_invocations=10`, `accepted_claims=3` |
| Aggregate usage was 1,084,879 tokens | `summary.json`: `research_metrics.usage.total_tokens=1084879` |
| The four new domains consumed 374,408 tokens with no accepted claim | Child totals: options 42,986; internals 80,945; cross-asset 125,424; earnings intelligence 125,053 |
| GOOGL and TSLA remained `AWAITING_ACTUAL` on 2026-07-24 after 2026-07-22 events | `market-context.json`: two released earnings, both with missing actuals and no item-level retry |
| Earnings refresh was deferred to 2026-07-29 | Aggregate earnings lifecycle uses the earliest upcoming event rather than pending-actual items |
| COT completed at coverage 1 with only report date and contract identity | COT child accepted `cot_report_date` and `cot_contract`; the numeric positioning claim was rejected |
| Consumer positioning was still `not_configured` | `market-context.json`: empty dealer/asset-manager/leveraged-fund positions and null open interest |
| COT expired at 10:00 UTC on the same day | Consumer lifecycle: `valid_until=2026-07-24T10:00:00+00:00` |
| Official AMD evidence did not reconcile the existing earnings item | The accepted AMD claim has verified issuer IR evidence and a 21:00 UTC event, while the consumer AMD item retains provider-only lineage, reliability 0, and no time |
| Execution success was presented as research completion despite 20% coverage | Parent status `SUCCEEDED`; consumer `research_complete=true`; coverage 0.2 with eight blocking gaps |
| `NO_DATA` had no usable negative-cache expiry | Domain results contain reason, searched time, and attempted sources, but no `next_retry_at`, retry class, cache key, or fields attempted |
| Child warning counts were always zero | Every child reports `warning_count=0`, although persisted run warnings are non-empty for multiple children |
| Rejection accounting was incomplete | Nineteen rejected claims versus eleven counted rejection reasons |
| Parent budget and continuation values were null | `summary.json`: both values are null although children report `budget_mode=observe` and continuation zero |
| Child durations double-counted the AI wall time | PLAN and SEARCH commonly contain the same backend invocation duration; the parent sums both phase values |
| Earnings verified-source counts diverged | The earnings run records two verified research sources while child aggregation reports one |
| A 2099 timestamp escaped the audit-only boundary | `debug_context.event_windows.legacy.checked_at_utc`; the quarantined timestamps are correctly present separately under audit |
| Debug data contained mojibake | News titles contain sequences such as `ā\\x80\\x99`; the compact consumer happened not to expose those excluded titles |

Repeated GETs of the parent kept `completed_at` and `updated_at` stable. No test
domain was found in the captured debug payload, and no trading/order endpoint was
called by the smoke.

## Verified root causes in the baseline code

1. `ResearchGapManifestBuilder` declares provider precedence but does not execute
   a persisted provider/resolver stage before producing agent work. It has no
   negative-cache eligibility check, no cross-child claim reuse, and no
   field-level retry deadline.
2. `ParallelResearchCoordinator.create_parent` creates a child for each
   `AGENT_RESEARCH` item. There is no durable due-item gate between the manifest
   and child creation.
3. `data_lifecycle_service._explicit_valid_until` computes one earnings expiry
   from upcoming events. It does not create lifecycle state per issuer event, so
   a pending actual is hidden by the next scheduled issuer.
4. `data_lifecycle_service._default_ttl` gives COT a generic six-hour TTL. It
   does not derive the next configured CFTC publication in America/New_York.
5. `research_gap_manifest._completeness` considers COT complete when
   `report_date` alone is present. It does not enforce numeric group coverage.
6. Research claims are projected into the research section, but no reconciliation
   step enriches an equivalent canonical earnings event. Consequently the AMD
   issuer evidence cannot upgrade lineage, event time, reliability, or validity.
7. `ai_trader_consumer_v2_service._research` derives `research_complete` only
   from execution status `SUCCEEDED`, independent of coverage and blocking gaps.
8. `research_domain_contracts.no_data_contract` does not persist backoff fields.
9. Parent child warning counts use `threshold_warnings` only, ignoring the
   persisted run warning list.
10. Parent rejection reasons aggregate gateway verification rejections only,
    whereas `rejected_claims` includes every rejected claim.
11. Parent telemetry omits aggregated `budget_mode` and `continuation_count`.
12. Child duration is the sum of phase durations. PLAN and SEARCH may both carry
    the same AI invocation wall time, so one invocation is counted twice.
13. Run-level verified-source counters use `research_sources`; metrics combine
    that state with accepted-evidence URLs. A later failed evidence verification
    can downgrade a source and make the two projections disagree.
14. Temporal quarantine preserves the invalid timestamp for audit, but the
    legacy event-window read projection still carries its `checked_at_utc`.
15. Text normalization detects control characters in `ā\\x80\\x99` but its
    byte-decoding candidates cannot repair a string containing U+0101. Projection
    normalization therefore needs an explicit safe repair for this encoding form.

## Required invariants for the implementation

- A valid datum causes zero provider and zero AI calls.
- A due datum is offered to deterministic providers/resolvers before AI.
- AI receives only residual, material, field-level gaps that are not in backoff.
- `NO_DATA` creates a typed, durable negative cache and cannot immediately recur.
- Execution completion and coverage completion are independent.
- Fast data is `REFRESH_ON_TRIGGER`; it never creates a notification by itself.
- COT metadata alone cannot complete positioning.
- Pending earnings actuals are individual due items with bounded retries.
- Materially identical snapshots do not emit outbox events.
- Telemetry accounting reconciles warnings, rejections, sources, invocation
  durations, tokens, budget mode, continuation count, and cost status.
- Invalid future timestamps remain available only in explicit audit projections.
- Historical immutable payloads are not rewritten; read normalization and
  reconciliation records are used instead.

## Residual risk before implementation

The fixture proves behavior for one captured run. Provider-specific holiday and
delayed-publication behavior must be deterministic and configurable because no
live provider validation is allowed in this change. Live delivery of outbox
events and the AI-TRADER consumer remain explicitly out of scope.

## Implementation result

Migration 020 adds, without replacing or deleting history:

- `datum_lifecycle_items` with negative cache, retry, lease, heartbeat, and
  materiality state;
- `market_context_outbox` with deterministic idempotency;
- `service_telemetry_events` with bounded retention metadata;
- `anomaly_incidents` with deterministic fingerprint/upsert;
- `model_pricing_versions`;
- additive execution/coverage fields on parent runs and field-level retry state
  on gap items.

The implementation is split across the central lifecycle, scheduler, gap
manifest, research persistence/metrics/coordinator, snapshot reconciliation,
consumer projection, outbox, hardening, and observability services. Supporting
configuration and contracts are in `config/model_pricing.json`,
`config/service_telemetry_event.schema.json`, and
`docs/event-driven-lifecycle-contract.md`.

Key post-change invariants verified:

- no resolver means no AI fallback; an exhausted resolver may enqueue one
  coalesced residual batch;
- active negative cache produces no new invocation;
- GOOGL and TSLA pending actuals are distinct durable issuer-event items;
- COT metadata remains partial; complete group positions project to the
  consumer and use the next configured CFTC publication;
- the verified AMD issuer-calendar claim enriches the existing event lineage,
  source tier, policy reliability, event time, validity, refresh, and
  confirmation status;
- parent execution completion does not upgrade partial coverage;
- VIX and other fast data cannot emit an outbox event by themselves;
- material trigger rematerialization is idempotent;
- future legacy timestamps leave the operational projection and remain in an
  explicit audit entry;
- the observed U+0101/control-sequence mojibake is repaired without altering
  valid Unicode.

## Validation evidence

- Dedicated lifecycle/outbox/observability suite: 44 passed.
- Central enablement/end-to-end closure suite: 13 passed.
- Pertinent closure suite: 69 passed.
- Complete repository suite: 1,355 passed in 204.87 seconds.
- Ruff: passed.
- `compileall`: passed.
- Windows PowerShell 5.1 parser: four scripts parsed, zero errors.
- `git diff --check`: passed.
- Migration matrix: source versions 1 through 20 upgraded/reopened
  idempotently to version 20; every temporary database returned
  `integrity_check=ok`.
- Offline replay: passed with 10 backend invocations, 1,084,879 total tokens,
  three accepted claims, 374,408 tokens and zero accepted claims for the four
  new domains, coverage 0.2, eight blocking gaps, two pending-actual issuers,
  and `live_calls_executed=0`.
- Consumer artifact: 84,137 bytes and SHA-256
  `BCED28DECDF98D65AF9843C3CF3FF23DAB0A164C721B8BEF9B3E0D7697699DD4`.
- Database backup: 175,865,856 bytes and SHA-256
  `98F3DF7D08B15980C5EEBA029466FA6AEAF3ED4F4B44CA60E75F698E44F42059`.

No test used or migrated the operational database. No Uvicorn process was
stopped or restarted. No service AI/OpenAI/Codex, provider, browser, web,
trading, execution, or order call was made.

## End-to-end closure addendum

The follow-up reproduction added red tests before implementation. They exposed
five concrete production gaps: no centralized per-agent authority, an
APScheduler due-scan call without resolver/enqueue dependencies, pre-event
earnings actual retries scheduled before occurrence, a partial atomic lifecycle
UPSERT, and numeric zero treated as missing. The fixes use one typed 13-agent
registry, defense in depth through worker pre-backend, a real application
resolver/enqueue wiring, event-relative earnings retry, authoritative atomic
field replacement, and type-aware material-data checks.

Migration 021 was not required. All state fits the existing migration 020 JSON
and additive columns. The local `.env` was changed only for the requested
master and 13 per-agent keys, remains untracked, and requires a future operator
restart; no restart occurred. The protected consumer artifact and pre-change
database backup were not modified.

## Residual risk and AI-TRADER follow-up

Configured CFTC holiday/delay data must be maintained operationally; this
change deliberately does not fetch a live holiday calendar. API cost estimation
remains unavailable until an explicitly reviewed model/version price is added
to the empty pricing table. Outbox delivery, retry transport, consumer-side
deduplication, and AI-TRADER readiness handling remain for a separate
AI-TRADER change. The acknowledgement endpoint is present only to define and
test the future consumer contract; no delivery worker or trading surface was
added.
