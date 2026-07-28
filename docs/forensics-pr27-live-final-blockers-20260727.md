# PR27 live final blockers — forensic correction

Date: 2026-07-27

Base: `f32fecb365d91501cb863497a6b2c15d3b128ff7`
Evidence: `data/pr27-live-acceptance-20260727T161552Z`

## PR28 isolated live acceptance addendum

Evidence:
`data/pr28-live-sandbox-20260727-194238/acceptance-20260727T174803Z`.
The five JSON artifacts were parsed in full, including duplicate-key checks.
The exact full-sync is 3,935,493 bytes and has file SHA-256
`DA41C09AF03B72B72A6C853E14B7C9EE9756B3B2FC9D7D79C0677D43FB10D164`.
Its pretty rendering is semantically identical. The sandbox database, WAL,
and SHM were copied to a temporary directory before read-only SQLite
inspection. The operational database was not opened or modified.

The PR28 live run confirms that the coverage, news, and sync corrections below
remain valid:

- 71 provider/scope/date ledger rows across all 21 dates, with 53
  `VERIFIED_EMPTY`, 8 `PARTIAL`, 10 `VERIFIED_COMPLETE`, no logical duplicate,
  and no terminal-proof violation;
- five admitted news records with explicit provenance and content
  availability, while two quarantined records remain withheld;
- 17-section `FULL_SNAPSHOT`, byte-identical repeated reads, checksum
  verification, and candidate equation
  67 = 53 delivered + 0 quarantined + 14 exact duplicates.

The same run exposed three additional blockers.

### PR28 blocker 1 - provider actual reconciliation

The correct actual candidates 628.0 and 53.6 exist in
`event_value_candidates` inside the isolated sandbox. They came from complete
post-release XTB records and retain stable occurrence IDs, retrieval time,
reference-period evidence, and field lineage:

- `current` maps to actual;
- `forecast` maps to consensus/forecast;
- `previous` maps to previous.

They were rejected only for use as *official* actuals, which is correct:
XTB is a distributor, not the originating statistical publisher. They were
therefore never read by `accepted_official_actual`. No accepted canonical or
lifecycle row contained the complete records. The expected Flash Services PMI
forecast 52.0 is also absent from the captured live database and JSON
artifacts; the captured incomplete provider candidate contains 51.5. The 52.0
value is present only in the redacted canonical acceptance fixture and must be
revalidated against a fresh provider response in the next isolated live run.

The synchronous force path called `reconcile_calendar_events`, whose prior
contract deliberately set every aggregator actual to null. It merged only
forecast/consensus/previous, preserved non-empty stale values on conflict, and
did not normalize a localized reference period already present in history.
The deterministic official resolver was not called in this HTTP path. Instead,
`EnrichmentOrchestrator` unconditionally called
`enqueue_temporal_refreshes`; `RELEASE_ACTUAL_REFRESH` treated a provider-only
context as sufficient authority and deferred the missing actual to the
persistent AI job table.

Correction:

- a complete provider row is validated synchronously before snapshot
  materialization using stable occurrence identity, post-release retrieval,
  numeric field checks, verified forecast/consensus semantics, explicit field
  lineage, provenance, and canonical monthly/quarterly reference period;
- actual, forecast, consensus, previous, occurrence ID, and reference period
  are promoted as one atomic record, never field-swapped or partially merged;
- the lineage and audit explicitly say `official_actual=false`;
- identical duplicate observations collapse by deterministic fingerprint;
- discordant complete records do not promote an actual and remain auditably
  `CONFLICT`;
- production code contains no event-specific numeric constants. The values
  exist only in redacted fixtures/tests.

Offline fixture
`tests/fixtures/pr28_live_actual_reconciliation_redacted.json` starts with the
two exact incomplete occurrences and supplies complete post-release provider
records. It proves:

- 628.0 / 610.0 / 618.0 / 2026-06;
- 53.6 / 52.0 / 51.2 / 2026-07;
- `RELEASED`, with no residual `AWAITING_ACTUAL`;
- exact read-back under the original occurrence IDs.

### PR28 blocker 2 - unauthorized persistent jobs

The live route created 15 `RELEASE_ACTUAL_REFRESH` rows (48 to 63) while the
worker, researcher, fallback, and enrichment AI flags were disabled.
`research_backend_invocations` correctly stayed at 41, but execution-time
suppression was too late: queue state had already been mutated.

Root cause:

- temporal release refresh used `ai_required=false`;
- the repository exempted `RELEASE_ACTUAL_REFRESH` from centralized agent
  enablement;
- repository enqueue did not validate the execution context before `INSERT`;
- acquisition/recovery checks could reject a row later, but could not undo the
  unauthorized queue mutation.

Correction:

- every persistent research job, including release refresh, requires explicit
  persisted AI authority before enqueue;
- release refresh additionally requires live-provider authority;
- centralized research-agent enablement now applies to release refresh too;
- repository enqueue fails closed before any row is written;
- acquisition, retry, recovery, and startup retain the same authorization and
  enablement gates;
- the safe provider-only context is retained in redacted authorization
  telemetry for verification.

The integrated route regression passes the provider-only context through the
force orchestration and verifies zero `ai_research_jobs`, zero
`research_runs`, zero active/running jobs, and zero backend invocations.

### PR28 blocker 3 - coalesced materialization

The one live request produced revisions 98 and 99 because
`DiagnosticsService._force_schedule_catch_up` invoked scheduler discovery,
which immediately called `_rematerialize_schedule_discovery` for revision 98
and its outbox event. The route then unconditionally called
`_materialize_market_context`, creating revision 99. This was scheduler
preflight plus route finalization, not a necessary two-generation contract.

Correction:

- schedule discovery accepts an explicit `materialize_snapshot` policy;
- ordinary background scheduler calls retain the existing default;
- diagnostics force preflight passes `materialize_snapshot=false`;
- canonical coverage and lifecycle state are committed during preflight, but
  no intermediate snapshot or outbox row is exposed;
- the consolidated route result is materialized once, allowing the final
  snapshot comparison to emit at most one corresponding outbox event.

The fixed-point regression retains 21 dates and proves zero due provider calls,
resolver evaluations, AI jobs, canonical writes, lifecycle writes, coverage
writes, snapshot writes, outbox writes, provider-state mutations, and retry
writes on the subsequent pass.

### PR28 verification boundary

Proved offline:

- complete provider-record promotion and exact database read-back;
- fail-closed conflict audit;
- provider-only pre-enqueue rejection in both service and repository;
- route-to-orchestrator context propagation;
- deferred scheduler preflight and one route finalization;
- 21-date fixed point;
- coverage, news, market-schedule, readiness, and full-sync regressions;
- two independent 17-section full-sync replays are byte-identical at 464,424
  bytes with SHA-256
  `54C04FA33E3888708BD38D1B0C5DA01E436985AA5CED813A1086CC5611FA391D`.

Still to be proved live:

- a new isolated provider-force returns the complete current provider records,
  including the canonical 52.0 Flash Services PMI forecast;
- one request advances the snapshot revision by at most one and emits at most
  one corresponding outbox event;
- queue, active/running counts, and backend invocation count remain unchanged.

Final PR28 offline verification:

- focused blocker regression: 89 passed;
- provider/actual/lifecycle/queue/sync regression: 509 passed;
- schema migration matrix 1 to 22, 20 to 22, 21 to 22, and 22 to 22:
  4 passed;
- complete suite: 1,814 passed;
- Ruff, `py_compile`, `compileall`, and `git diff --check`: passed.

## Safety and evidence integrity

The operational service was stopped and port 8053 was free. The operational
database was not modified. Analysis used the committed pre-force backup and a
temporary copy of the stopped post-force SQLite database. `.env` was neither
read nor modified. No live provider, AI/backend, browser, Uvicorn, migration,
delivery, AI Trader, trading, or order path was executed.

The pre-force backup is 237,178,880 bytes with SHA-256
`D42977E2AB9075FA652449E7E4156D76C5EF9C92F924BBF9ED5DD53CA11F45F8`.
Both forensic databases are schema 22 and pass `PRAGMA integrity_check`.

## Observed pre-force / post-force state

| Measurement | Pre-force | Post-force |
|---|---:|---:|
| Snapshot revision | 96 | 97 |
| Economic event rows | 110 | 129 |
| Market-news rows | 241 | 257 |
| Coverage rows | 8 | 8 |
| AI research jobs | 48 | 48 |
| Backend invocations | 41 | 41 |

The live full-sync is a valid 17-section `FULL_SNAPSHOT`, 3,219,296 bytes,
with internal checksum
`3ac94814d0d825ad6550671f808e8a6ef5ff8e7ecdd149338c3b2d7c83daac47`.
Its file SHA-256 is
`0007CF871B12F1FE67D051796C0548876F75A456721E897A24E4014D450592D1`;
the two reads are byte-identical. Candidate accounting is
101 = 79 delivered + 0 quarantined + 22 technical duplicates, with zero
unexplained loss. The live 19/51/9 bucket counts have no temporal
out-of-range records and are not treated as failures.

## Root cause 1 — XTB actual occurrences

The two numeric XTB observations exist in the pre-force backup in
`event_value_candidates`, not as accepted canonical actual columns:

- `xtb:146392:2026-07-24`: candidate value 628.0;
- `xtb:146945:2026-07-24`: candidate value 53.6.

Both candidate rows are rejected because the source/semantic proof required
for an official actual is absent. This correction does not promote them and
does not hardcode their values in production.

The canonical history contains several rows for each provider occurrence.
Changes in provider metadata, localized reference period, and the presence or
absence of `occurrence_id` produced different semantic hashes. The
provider-force then projected the new hashes as discoveries while the stable
XTB IDs disappeared from the current comparison set. Snapshot comparison
therefore emitted non-triggering `UNCONFIRMED_REMOVAL` rows. The same identity
split also allowed an additional lifecycle to be seeded.

Correction:

- exact provider occurrence identity is selected before semantic fallback for
  persistence;
- history lookup also matches the provider `event_id`;
- the persisted canonical alias remains stable when a richer or poorer
  projection updates the same row;
- DB-first merging preserves non-null occurrence identity, actual, forecast,
  previous, and reference period;
- history projection collapses only rows with the same exact occurrence and
  preserves non-null canonical fields;
- lifecycle seeding recognizes existing payload aliases, preventing a second
  logical lifecycle;
- a no-gap fixed point cannot rematerialize a snapshot or outbox row.

## Root cause 2 — coverage ledger

All eight live rows cover only 2026-07-20 through 2026-07-27 under
`provider_name=economic_calendar_composite` and `query_scope=country=US`.
Every row is `PARTIAL`: the provider was marked called and scope-verified, but
request, pagination, parsing, expected-source, and authentic-empty proof flags
were false. The remaining canonical dates had no ledger row.

`refresh=force` did not traverse the rollover because explicit force was
incorrectly gated by the background catch-up enable flag. When invoked, the
proof was all-or-nothing across calendar and non-calendar providers, so an
unavailable/disabled source could invalidate every date.

Correction:

- explicit force traverses schedule coverage independently of the background
  scheduling flag;
- authoritative BLS, BEA, and Federal Reserve calendar adapters expose their
  actual covered date ranges;
- acquisition targets and ledger rows are keyed by real provider and scope;
- only missing or expired provider/scope/date segments are called;
- complete and empty states still require all positive proof flags;
- `VERIFIED_EMPTY` additionally requires day-specific `authentic_empty`;
- dates outside a provider's proven range remain `PARTIAL`;
- aggregate authority retains the contributing providers per date;
- removal comparison requires that the prior record's provider is among the
  authoritative sources for that date (the legacy composite fallback remains
  conservative for older callers);
- existing contract-version and source-policy-version keys continue to
  invalidate incompatible coverage;
- SQLite `BEGIN IMMEDIATE` plus the existing logical unique key protects
  concurrent migrators/writers.

No coverage is fabricated to reach 21 rows: 21 is the NY calendar date window;
the number of ledger rows is provider × scope × covered date.

## Root cause 3 — news provenance and content

The live IBD Tesla row contains provider summary metadata but no raw body. The
IBD Apple provider payload contains no `content`, `body`, `summary`,
`description`, or `content_snippet`. Those fields are also absent in
`market_news` and snapshot 97; this is source absence, not a full-sync
projection loss. The provider did not supply Yahoo as distributor for either
row. A Yahoo token or hostname is not sufficient provenance.

Correction:

- every normalized article exposes `article_id`, `headline`, publisher,
  explicit distributor, publication/retrieval timestamps, source/canonical
  URLs, provider/source, validation, structured `provenance` and `lineage`;
- raw text is preserved in `content` when supplied;
- missing source text is represented as
  `content_availability=SOURCE_NOT_PROVIDED` without invented prose;
- distributor is accepted only from explicit provider metadata, never inferred
  solely from an aggregator URL or tracking parameter;
- temporal editorial identity still includes publication time, so distinct
  updates remain distinct; only equivalent technical retries collapse;
- repository read-back projects the new provenance and content fields.

## Non-regression scope

The market-schedule semantics are unchanged: CME cross-check failure remains
`PARTIAL`, cash and futures remain separate, and the section is neither
promoted to complete nor quarantined. The independent readiness dimensions
for trading context, macro, news, and event risk remain payload-derived.
There is no top-N, payload-size truncation, lossy summarization, or destructive
editorial deduplication in this correction.

## Verification boundary

Code-correct:

- stable exact-occurrence persistence and conservative removal authority;
- provider/scope/date coverage proof;
- explicit news provenance/content-availability contract;
- fixed-point write suppression.

Demonstrated offline:

- exact actual/forecast/previous/reference-period preservation on temporary
  schema-22 databases using redacted fixtures;
- provider-scoped 21-date rollover with unproven future dates left partial;
- distinct editorial updates, explicit missing content, and non-inferred
  distributor;
- existing schema-22 replay, full-sync accounting, market schedule,
  readiness, zero-AI, and byte-identical fixed point.

Still pending:

- a new controlled live provider-force acceptance run after deployment. The
  historical live artifacts remain evidence of the defect and must not be
  relabelled as proof of the correction.

## Final offline verification

- targeted provider/lifecycle/calendar/news/sync/readiness suite:
  313 passed;
- migration/schema-22 focused matrix: 63 passed;
- complete suite: 1,809 passed;
- Ruff, `py_compile`, `compileall`, and `git diff --check`: passed;
- two independent final offline replay executions:
  - summary: 3,608 bytes,
    SHA-256 `4342C2488D0CC272F87B93D90E32CEC7C275B1F0A2D54599D0F96979B02B2D84`;
  - full-sync: 464,391 bytes,
    SHA-256 `C11AC14D39FED1BF3FAAA17EDD61133C3131E705AC614656FBADD8A6BD0C114F`;
  - both summary and full-sync are byte-identical across the two executions.

## PR 28 follow-up - LIVE_ACTUAL_RECONCILIATION (2026-07-28)

### Evidence and root cause

The isolated live acceptance database and its final artifacts were inspected
read-only. The forced refresh called the Investing and XTB calendar paths.
Investing returned HTTP 403. XTB returned 37 current calendar rows, but neither
24 July occurrence was still present. Historical XTB candidates did contain
the two actual values, but they were correctly rejected by source policy
because XTB is not a tier-1 official actual source.

The lifecycle adapter then required the historical occurrence to reappear in a
one-minute query of the current authoritative calendar before invoking the
official actual resolver. That requirement made the resolver unreachable for
an aged-out XTB occurrence. The calendar record was therefore left at
`AWAITING_ACTUAL`.

### Corrected deterministic path

- `xtb:146392:2026-07-24` is mapped by documented stable event identity to
  FRED series `HSN1F`. FRED is a tier-1 admitted official redistributor of the
  Census/HUD New Residential Sales series. The adapter requests observations,
  validates monthly frequency, SAAR units and exact `2026-06`, and preserves
  the prior observation separately.
- `xtb:146945:2026-07-24` is mapped by documented stable event identity to an
  S&P Global public PMI release adapter. It only accepts HTTPS pages under
  `pmi.spglobal.com`, parses the named Flash US Services PMI Business Activity
  Index row, validates exact month/frequency/unit, and records a redacted
  content SHA-256 plus release URL.
- `Giugno` and `Luglio` are normalized from a fixed internal month table using
  the occurrence release date, independently of host locale.
- An occurrence that has aged out of the current calendar can now retain its
  persisted XTB occurrence identity while receiving an actual only from the
  admitted official resolver.
- Calendar-provider actuals without `actual_is_official=true` now remain
  fail-closed. The old redacted XTB test payload is explicitly tested as
  rejected and is not a production source.

No target numeric value is stored in runtime code. Numeric values in tests are
provider-response fixtures used to verify parsing and semantics only.

### Before/after and unresolved operational proof

| Occurrence | Live before | Deterministic source path | Offline adapter result |
| --- | --- | --- | --- |
| `xtb:146392:2026-07-24` | actual null; previous 580; `Giugno`; awaiting | FRED `HSN1F` / Census-HUD lineage | actual 628; previous 618; `2026-06`; official candidate accepted |
| `xtb:146945:2026-07-24` | actual null; forecast 51.5; previous 51.2; `Luglio`; awaiting | S&P Global public PMI release | parser proves actual 53.6; previous 51.2; `2026-07` when the release page is accessible |

The PMI path remains operationally blocked in the observed environment:
S&P Global's public PMI endpoint returned HTTP 403, and no credentialed or
licensed endpoint is configured. The official release publishes actual and
previous, not the calendar consensus. The requested forecast `52.0` is not
present in the captured live candidate (`51.5`) and has not been proven by two
independent admitted consensus sources. The runtime therefore preserves the
scheduled forecast and does not invent or overwrite it.

Consequently this change is code-correct and fail-closed, but the producer
blocker must not be declared closed until a controlled live run has:

1. access to the exact S&P Global release through an authorized endpoint;
2. independent admitted lineage proving the required PMI forecast;
3. demonstrated atomic canonical/lifecycle/snapshot/outbox materialization and
   a completely immutable second fixed-point run.

### Verification boundary

Offline targeted tests cover both real occurrence IDs, official parsing,
stable identity after calendar age-out, Italian month normalization, exact
period/frequency/unit semantics, access restriction, preservation of
forecast/previous, and rejection of non-official calendar actuals. No live
provider, AI backend, operational database, scheduler daemon, delivery,
trading, or order path was invoked during this correction.

Final verification for this follow-up:

- new focused tests: 7 passed;
- provider/actual/lifecycle/calendar focused regression: 63 passed;
- schema-22/migration/semantic focused regression: 106 passed;
- complete suite: 1,821 passed;
- Ruff, targeted `py_compile`, `compileall`, and `git diff --check`: passed;
- two independent schema-22 offline replays were byte-identical:
  - summary: 3,608 bytes,
    SHA-256 `D40459950CACCFB03E16D23F499073F32910272D19393CEEA04E2A5982A5BC00`;
  - full-sync: 464,424 bytes,
    SHA-256 `54C04FA33E3888708BD38D1B0C5DA01E436985AA5CED813A1086CC5611FA391D`.

## PR 28 R2 follow-up - production route wiring (2026-07-28)

Initial PR head:
`1e85e47e8b04871f41c7293d0bc6dca67f311407`.
Base:
`f32fecb365d91501cb863497a6b2c15d3b128ff7`.
The final implementation head is the commit containing this addendum on
`codex/fix-pr27-live-forensic-blockers`; its exact SHA is recorded in PR #28
and in the final handoff.

### Read-only R2 evidence

The isolated R2 database and three captured artifacts under
`data/pr28-r2-live-sandbox-20260728-103300` were inspected read-only. SQLite
was opened with immutable/query-only semantics and passed
`PRAGMA integrity_check`. The operational database was not opened.

- `actual-diagnostic-summary.json`: 19,937 bytes,
  SHA-256
  `7C3B2CE20AB11DC5582737F9322B0B5FC3FB6AB3D2E98FB593431A41DF933088`;
- `provider-force-debug.json`: 4,815,230 bytes,
  SHA-256
  `39502A778EC4BA402CBF20D839A291AF8CE271F7D32BD502C6A109023B0AC966`;
- `full-sync-exact.json`: 3,832,706 bytes,
  SHA-256
  `DBDF8A8FCA00435956F23391218EE09B32F0C1E3D12BE6321D41DCC487BA92E2`.

The full-sync had 17 sections, snapshot 98, and contract checksum
`e2cae8e6939b60db3905b5625ca163718d818c29634689caf9864fbed95a7b80`.
The calendar contained 56 delivered plus 15 exact duplicates across the 71
provider/scope/date candidates, with zero omission. Snapshot 97 advanced to
98 and outbox 3 advanced to 4, while AI jobs stayed at 48, backend
invocations stayed at 41, and the queue stayed empty. Both target
occurrences remained `actual_missing`; localized `Giugno`/`Luglio` and the
stale scheduled values were still visible.

### Exact root cause and call graph

The official actual resolver was instantiated by
`build_application_state`, and the lifecycle resolver owned it. It was not,
however, a dependency of the force route.

Before:

```text
GET /market-context/mnq?refresh=force
  -> construct DiagnosticsService inside the route
  -> DiagnosticsService.full_model
     -> calendar/provider reconciliation
     -> scheduler coverage preflight (snapshot materialization disabled)
  -> DeterministicProviderRuntimeService.enrich_market_context
  -> harden_market_context
  -> MarketContextSnapshotRepository.save_next
     -> snapshot/outbox
```

The `MacroActualLifecycleProviderAdapter` and
`DeterministicActualResolver` existed only behind the lifecycle resolver used
by startup/background catch-up. The route did not pass that resolver to its
orchestrator, did not give it the two `actual_missing_ids`, and therefore
never selected FRED `HSN1F` or attempted S&P Global. Normalization and the
PMI reason code lived in the unreachable branch and could not reach the
canonical row or full-sync.

After:

```text
GET /market-context/mnq?refresh=force
  -> FastAPI dependency graph supplies the production lifecycle resolver
  -> DiagnosticsService.full_model
  -> DeterministicProviderRuntimeService.enrich_market_context
  -> ProviderForceActualReconciliationService.prepare
     -> DB-first exact-occurrence gap detection
     -> official mapping
     -> production lifecycle resolver
        -> MacroActualLifecycleProviderAdapter
           -> DeterministicActualResolver
              -> FRED HSN1F / S&P Global adapter
     -> validate and prepare canonical + lifecycle mutations
     -> project losslessly into event calendar and macro actuals
  -> harden_market_context
  -> MarketContextSnapshotRepository.save_next
     -> one BEGIN IMMEDIATE transaction
        -> canonical exact-occurrence reconciliation
        -> lifecycle outcome
        -> snapshot
        -> outbox
```

The scheduler now checks the actual provider/scope coverage targets before
acquiring its lease, so an already-covered fixed point cannot mutate
`provider_state`. The stable provider occurrence ID has precedence over a
derived canonical alias, preventing a replay or richer projection from
renaming `xtb:146392:2026-07-24` or `xtb:146945:2026-07-24`.

Earlier tests passed because they manually constructed an adapter/resolver
pair or replaced `DiagnosticsService`; neither path exercised the dependency
graph built by `app.main` and used by the HTTP route.

### Provider results and provenance

For `xtb:146392:2026-07-24`, the route-level controlled HTTP response contains
FRED observations 628 and 618 for `HSN1F`. Runtime code contains the mapping
and semantic constraints, not those numeric values. The resolver validates
monthly frequency, SAAR/thousands unit, observation month `2026-06`, and
source policy. The materialized occurrence retains the XTB identity and
distributor, while the official actual lineage identifies FRED/Census-HUD,
`HSN1F`, source URL, retrieval/release/reference timestamps, unit, frequency,
validation status, and occurrence mapping. Forecast retains separate XTB
lineage. Previous 618 has separate official lineage with
`derivation=previous_official_series_observation` and
`source_field=previous`; it is not relabelled as forecast or as the stale XTB
previous.

For `xtb:146945:2026-07-24`, the route really reaches the S&P Global HTTP
boundary. A controlled HTTP 403 produces no candidate and no invented actual.
The occurrence remains `AWAITING_ACTUAL`, normalizes to `2026-07`, preserves
forecast 51.5 and previous 51.2 with XTB lineage, and carries structured
`actual_resolution` telemetry in debug and full-sync:

- resolver invoked and mapping selected;
- provider `SPGLOBAL`, HTTP outcome `HTTP_403`;
- stable reason
  `sp_global_public_release_access_restricted`;
- retryable, actual still missing, attempted timestamp;
- provider/candidate/reconciliation/persistence and write accounting.

No alternative deterministic source admitted by the current source policy was
found for this PMI release. The unavailable provider therefore remains
fail-closed.

### Production-graph and atomicity proof

`tests/test_pr28_route_provider_force_wiring.py` starts the real `app.main`
lifespan and calls the production `build_application_state`. It replaces only
the FRED and S&P HTTP transports and rejects all other network at the boundary.
It calls the actual route and then `/market-context/mnq/sync/full`.

The test proves FRED invocation and `HSN1F` selection, 628/610/618,
`2026-06`, unchanged XTB occurrence identity, separate field lineage, one real
S&P 403, fail-closed PMI, `2026-07`, reason propagation, 17 full-sync sections,
zero AI jobs, and zero backend invocations.

Test-only SQLite triggers audit `INSERT`, `UPDATE`, and `DELETE` on snapshots,
outbox, canonical history, lifecycle, candidates, coverage, provider state, AI
jobs, and backend invocations. The first force creates exactly one snapshot
and at most one outbox. On the second force, the audit table is empty: zero
canonical, lifecycle, coverage, provider-state, retry, snapshot, outbox, AI,
and backend writes. The S&P negative cache prevents a second HTTP call.

The canonical event repository now performs lossless deep lineage merge and
suppresses semantically identical numeric/time rewrites. A poorer provider
projection cannot delete richer verified lineage, and a derived temporal
projection cannot create database churn.

### Final offline verification

- route/actual/live-blocker/sync focused suite: 56 passed;
- extended provider/actual/lifecycle/scheduler/coverage/snapshot/sync suite:
  217 passed;
- schema migration matrix 1 to 22, 20 to 22, 21 to 22, and 22 to 22:
  4 passed;
- complete suite: 1,822 passed;
- Ruff, `py_compile`, `compileall`, and `git diff --check`: passed;
- two independent schema-22 replays are byte-identical:
  - summary: 3,609 bytes,
    SHA-256
    `AABC0E264C3571EFAC74975A602C479CE0588E8B8EA9B78DAE55EBCAD0E1A1D9`;
  - full-sync: 465,269 bytes, 17 sections,
    SHA-256
    `EFBC4A28E61ADC5A955D414E9E00A88D9855A1FB63BF4A828AD4A783068E4A67`;
  - `actual_missing` is zero in the offline replay;
  - provider calls, resolver evaluations, canonical/lifecycle/coverage/
    provider-state/retry/snapshot/outbox writes are all zero at its fixed
    point;
  - live provider calls, AI jobs, and backend invocations are zero.

### Remaining live validation and safety boundary

The historical R2 artifacts prove the defect, not this correction. PR #28
must remain non-mergeable operationally until a new isolated live
provider-force run demonstrates:

1. FRED `HSN1F` is reached by the real deployed route and the 628/610/618
   record is committed under the unchanged XTB occurrence;
2. the deployed S&P endpoint outcome is delivered with the structured reason
   and no invented value;
3. one force creates at most one snapshot/outbox and the second fixed-point
   force performs no unintended write.

During this correction `.env` was neither read nor modified. The operational
database, Uvicorn, live providers, AI/OpenAI/Codex backends, browser research,
AI Trader, delivery, trading, and order paths were not invoked.
