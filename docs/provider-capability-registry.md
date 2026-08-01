# Provider capability registry

`app.services.provider_capability_registry` is the single, versioned source of
truth for provider identity, runtime adapters, Senior Analyst source policy and
auditable capabilities. Runtime projection, request accounting, capability
audit, generated source matrices and CI must consume this registry rather than
maintaining parallel provider lists.

The registry contains four frozen records:

- `ProviderRegistration`: provider identity, adapter paths, type, roles,
  credentials, bounded execution policy, rate-limit knowledge and probe.
- `CapabilityRegistration`: dataset, metric, supported fields, frequency,
  transformation, probe, field validator, AI eligibility and an optional
  explicit acquisition request group. `canonical_metric_ids` records an
  explicit raw-series or AI-target relationship; `probe_query_id` is present
  only when it is an actual adapter query argument.
  `delivery_capability_id` and `delivery_capability_ids` are explicit
  relations between differently named raw series and one or more delivered
  capabilities. `delivery_field_map` binds each emitted source field to the
  exact delivered field it can satisfy. Neither relation is inferred from a
  shared dataset, a similar field name or a non-null payload value.
- `OfficialMetricRegistration`: one canonical metric, its owning dataset,
  exact raw provider/series, transformation, seasonal adjustment, frequency,
  unit, comparison lag, precision and canonical URL.
- `DatasetSourcePolicy`: the 25 Senior Analyst datasets, their canonical
  repository, DB-first primary/fallback chain, optional capability-scoped AI
  candidates and dataset-specific SLA.
- `FIELD_VALIDATOR_SCHEMAS`: the field namespace each declared validator can
  actually assess. A capability cannot advertise a field outside that schema.

## Registration invariant

Every concrete `*Provider` adapter under `app/providers` must occur exactly once
in the runtime adapter paths exposed by the registry. Repositories,
reconciliation services, transformations and AI providers used by the route or
audit are also registered, even when they are not `BaseProvider` subclasses.

Adding a source requires one atomic change set:

1. Add its provider registration and importable adapter path.
2. Declare each capability at dataset + metric + field granularity.
3. Assign a real isolated `probe_id` and field validator.
4. Add it to a dataset source policy only after registration.
5. Mark AI eligibility only for capabilities that the AI audit can certify.
6. Add adapter/probe tests and regenerate the matrices.

`request_group` is not a provider-wide cache key. It may be populated only when
one real acquisition returns evidence for multiple declared capabilities. The
audit can then deduplicate that exact acquisition while still emitting a result
for every capability. Missing groups remain `null`; they must never be inferred
from a shared provider name. The current `bls.series.batch` group is explicit on
the ten raw BLS series capabilities because the BLS adapter submits those
series in one real batch request. The separate BLS release-calendar probe is
not grouped.

`audit_leaf_request_count` is declared only when one isolated adapter probe
must execute a bounded, known fan-out of distinct HTTP leaves. It limits the
acquisition call budget separately from `max_attempts`, which remains the retry
bound for one exact request fingerprint. Exceeding either bound fails closed.

The capability `frequency` is the cadence of the acquired source series, not
the refresh lifecycle of the consumer dataset. For example, the FRED target
range bounds are daily observations while the `target_range` dataset changes
on an event lifecycle; `FEDFUNDS` is a monthly series even though the
`fed_funds` dataset is checked daily. Provider no-argument defaults are derived
from the registered leaf capabilities handled by that adapter. CI rejects a
runtime default series that is absent from the registry.

## Official raw-to-canonical boundary

BLS, BEA, Census, FRED and S&P Global capabilities describe only raw atomic
series and fields actually emitted by their adapters. They do not claim
canonical `actual`, `released_at` or transformed frequency semantics. Every
official canonical metric occurs exactly once in
`OFFICIAL_METRIC_REGISTRATIONS`, exactly once in a source-series relationship,
and exactly once in `OFFICIAL_ACTUAL_TRANSFORMATION`.

`official_actual_semantics.OFFICIAL_METRICS` is derived from those immutable
registrations. Derivation fails closed on a source-series, provider, frequency,
seasonal-adjustment or reference-period mismatch and emits an `actual` alias
plus field-specific calculation lineage.

AI certification uses the same exact identity:
provider + dataset + canonical metric + field. `AI_RESEARCHER` has bounded
metric-specific targets; certification of one field does not authorize another
field or metric. Both AI registrations are audit-only in the current runtime
policy, and no dataset chain treats a generic model/backend status as delivery
authorization. `OPENAI_EVENT_ENRICHMENT` remains registered but non-certifiable
because its current adapter is a no-call scaffold.

## Fail-closed validation

`validate_registry()` blocks startup, audit and CI when it finds duplicate or
invalid IDs, a missing/disabled probe, a missing field validator, an
unimportable adapter, an unregistered policy source, a non-repository canonical
store, an AI fallback without capability-specific eligibility, a field outside
its validator schema, or a request group that does not contain at least two
capabilities handled by the same provider and probe. A `FALLBACK` policy is
also rejected when a declared fallback has no explicit delivered-capability
and delivered-field overlap with its primary.

`DatasetSourcePolicy.provider_strategy` is explicit:

- `FALLBACK` means ordered substitution for the same delivered capability.
- `CASCADE` means the registered providers are sequential acquisition
  dependencies and are not interchangeable.
- `FAN_IN` means independent capability groups contribute to one dataset.

Audit accounting assigns every policy step a stable
`phase_index + source_kind + source_id` identity. The canonical repository is a
separate DB phase even when its provider ID is also the primary provider ID.
`FALLBACK` selects the first eligible exact metric+field equivalent; `FAN_IN`
retains all eligible contributors; `CASCADE` retains all ordered dependencies.

The market schedule is `FAN_IN`: Nasdaq cash-session information, CME futures
hours and holiday-calendar observations are independent groups. MarketBeat is
only a true fallback for the Investing holiday capability. A success in one
independent group cannot suppress acquisition of another.

The registry records `UNKNOWN` when a rate limit or health state has not been
demonstrated. A configured adapter, importable class or HTTP 200 is not a
capability certification. The LIVE capability audit must validate schema,
fields, semantics, occurrence, lineage, lifecycle and freshness.

## DB-first policy

The dataset chain is evaluated in this order:

1. Read the canonical repository.
2. Evaluate `data_as_of`, `content_valid_until`, `refresh_due_at`, UTC now,
   dataset SLA and release lifecycle.
3. Deliver a valid canonical record without calling a provider.
4. For a missing/expired/due record, call the registered primary.
5. Try registered deterministic fallbacks in order.
6. Use only an AI fallback certified for that exact capability.
7. Return `null` with a reason code if no eligible source succeeds.

`retrieved_at` never makes an old observation current. An expired
last-known-good value may be retained in technical audit evidence but must not
be delivered as current.

## Generated matrices

The committed JSON and Markdown matrices are deterministic projections of the
registry:

- `docs/baselines/senior-analyst-data-source-matrix.json`
- `docs/senior-analyst-data-source-matrix.md`

Regenerate them with:

```powershell
python scripts/generate_provider_capability_matrix.py
```

CI performs a byte-for-byte check equivalent to:

```powershell
python scripts/generate_provider_capability_matrix.py --check
```

Manual edits to either generated file are rejected. LIVE audit results may be
published into the verified compact baseline
`docs/baselines/provider-capability-last-live.json`. When that baseline exists,
the generated JSON and Markdown expose the exact last-LIVE health and
recommended role for each provider + dataset + metric + field. Aggregate
capability health is never copied across fields. A missing baseline renders
deterministically as `NO_VERIFIED_LIVE_BASELINE`; it does not silently alter
source policy or provider priority. Baseline schema `1.2` binds the compact
content to the accepted audit-report identity with a canonical SHA-256
attestation. Loading fails closed if the provenance, row counts, field set,
health/score relationship, derived system health, eligibility, recommendation,
timestamp, or attestation is altered. The exact baseline file bytes must also
match the separately reviewed SHA-256 pin in the registry source; recomputing
hashes inside a rewritten JSON file cannot make it authoritative.
