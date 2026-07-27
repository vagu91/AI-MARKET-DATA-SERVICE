# PR27 live final blockers — forensic correction

Date: 2026-07-27

Base: `f32fecb365d91501cb863497a6b2c15d3b128ff7`
Evidence: `data/pr27-live-acceptance-20260727T161552Z`

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
