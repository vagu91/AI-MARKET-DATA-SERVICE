# Lossless news pipeline forensic correction — 2026-07-28

## Scope and safety

This correction was developed from `main` at
`a11673e20c7d12cce09a1707ce1560e2fc96ccf9`. The PR28 R4 SQLite sandbox and
captured full-sync were opened only through SQLite `mode=ro` plus
`PRAGMA query_only=ON`. The replay made zero provider, AI/backend, browser,
delivery, trading, or order calls. It did not read `.env`, did not start
Uvicorn, and did not open the operational database through a write-capable
connection.

## Root causes

The observed loss was produced by several independent gates:

1. `SourcePolicyService.validate()` treated a Yahoo/MSN distribution rule as
   an editorial publisher allowlist. Its one-name list accepted Reuters only
   and emitted `distribution_source_original_publisher_not_allowed` for every
   other record, even when the original publisher had a separately configured
   direct source rule. Publisher, distributor, and acquisition provider were
   therefore conflated.
2. `news_intelligence_service._exclusion_reason()` converted descriptive
   properties into destructive filters. LOW relevance, ambiguous topics,
   missing symbols, analyst/listicle classification, and EXPIRED lifecycle
   removed otherwise usable canonical records.
3. `_deduplicate_articles()` selected one representative and marked exact
   acquisition retries as rejected. `reconcile_delivered_section()` then
   applied a second identity-based collapse and replaced `articles` with the
   current-only `latest` view.
4. `MarketNewsRepository.stored()` had a default SQL limit of 200. The raw
   acquisition payload was also allowed to overwrite canonical projected
   fields when materializing news.
5. Legacy rows had no category at acquisition time. A NULL lifecycle remained
   visible when no later write repaired it, while the reader treated any
   missing/expired `valid_until` as EXPIRED. The resulting states were neither
   explicit nor auditable.
6. Generic section readiness treated any historical EXPIRED news record as
   degraded lifecycle data. With all usable articles filtered or collapsed,
   the final news section became QUARANTINED/UNAVAILABLE despite canonical
   records being present.

## Corrected model

- A distribution host is validated separately from the original publisher.
  Any original publisher already represented by a configured, news-capable
  direct source rule is verified through Yahoo/MSN without broadening the
  registry to arbitrary names. Unknown or contradictory originators remain
  quarantined.
- Every delivered record exposes `original_publisher`, `distributor`,
  `acquisition_provider`, publisher/distributor/lineage statuses, provenance,
  validation, content status, topic status, category status, and lifecycle.
- `articles` is the canonical, lossless in-scope inventory. `latest`,
  `historical_articles`, `directly_relevant`, and `supporting` are derived
  views only.
- LOW and ambiguous classifications are metadata. EXPIRED means historical,
  not deleted.
- Technical duplicates are classified but retained. Similar stories, separate
  publishers, and later updates are never semantically collapsed.
- Quarantined records are represented by SHA-256 material fingerprints,
  reason code, timestamps, policy version, and safe lineage only. Headline,
  summary, content, and unsafe URL material are not exposed in the quarantine
  projection.
- Full sync has no article count, byte-size, or top-N cap. Post-ACK section
  sync computes record-level new, material-update, lifecycle-change, and
  explicitly confirmed-removal sets. Absence alone never becomes a removal.
- News readiness is calculated from the final delivered `articles`: usable
  records with complete policy coverage are AVAILABLE; usable records plus
  quarantined/invalid coverage are PARTIAL; no usable records are UNAVAILABLE
  (or QUARANTINED when policy withholding is the sole result).

## Schema decision

No migration was added. Schema 22 already stores canonical lifecycle, category,
publisher, aggregator URL, source-policy state, and the complete redacted raw
payload. Distributor, acquisition provider, content/topic/category status, and
safe lineage remain faithfully persisted in `raw_payload_json`. The canonical
reader deterministically projects legacy NULL lifecycle/category values, while
all future upserts persist the repaired lifecycle and category. This avoids an
optimistic or destructive backfill and leaves migration 22 unchanged.

## Read-only forensic replay

The captured pre-correction state contained 273 database records and delivered
zero news records in the full-sync:

| Measure | Captured before | Read-only replay after |
|---|---:|---:|
| Raw canonical rows | 273 | 273 |
| Delivered `articles` | 0 | 79 |
| Current delivered | 0 | 7 |
| Historical delivered in scope | 0 | 72 |
| Lifecycle UNCLASSIFIED | 158 observed in DB | 0 in projection |
| Policy quarantine | 271 excluded at top level | 194 |
| Technically invalid | not separately accounted | 0 |
| Verified publishers | not available | 79 |
| Unknown/unverified publishers | not available | 194 |
| Explicit category UNCLASSIFIED | 273 NULL in DB | 64 |
| Explicit topic AMBIGUOUS | 2 destructively excluded | 56 delivered metadata |
| `news_analysis` | UNAVAILABLE | PARTIAL |

The replay delivers the cited Reuters record, the cited historical IBD record,
23 Reuters records, 18 IBD records, two Associated Press records, and ten
Federal Reserve records. It intentionally retains 194 records in quarantine
because their declared originator is not represented by the conservative
configured publisher registry.

The accounting is exact:

```text
in_scope_raw 273 = delivered 79 + quarantined 194 + technically_invalid 0
raw_fetched 273 = in_scope_raw 273 + outside_scope 0
```

Two independent 17-section replays were byte-identical:

- exact size: 4,779,513 bytes;
- SHA-256:
  `1726A6EF690475243D784ABD8BFBCE981BB1D9648767E1E23F98B51DA16E6747`;
- contract checksum:
  `7bf8c3a2e645cec4c879fc63b7ceaf5f0ee813ce6bf313602d68c31722778ed3`;
- sandbox DB SHA-256 before and after:
  `FC51B5EE0F1E0B34B0DDE3449ACC7CC55B8DBF680C44EF0E5D694C419C998185`.

The replay reported zero database writes, new snapshot revisions, new outbox
rows, provider calls, AI jobs/backend invocations, browser calls, deliveries,
trading actions, and orders.

## Tests and validation

The dedicated redacted fixture contains only synthetic/redacted headlines,
short summaries, timestamps, source URLs, and lineage fields required to
reproduce the forensic shapes. The adversarial suite covers all 22 requested
cases, including multi-megabyte Unicode content, no top-N behavior,
nondestructive duplicates, record delta after ACK, invalid ACK rejection,
readiness states, accounting, and deterministic fixed point.

Validation commands:

```text
python -m pytest tests/test_lossless_news_pipeline_forensics.py -q
python -m pytest tests/test_news_intelligence_service.py -q
python -m pytest tests/test_market_context_sync_protocol.py -q
python -m pytest -q
python -m ruff check .
python -m py_compile <changed Python files>
python -m compileall -q app scripts tests
git diff --check
python -B scripts/replay_lossless_news_pipeline_offline.py ...
```

Final offline results:

- dedicated adversarial lossless-news suite: 28 passed;
- focused news/sync/schema-22/snapshot-92/snapshot-94 regression: 236 passed;
- complete repository suite: 1,856 passed in 473.30 seconds;
- Ruff: passed;
- `py_compile`: passed;
- `compileall`: passed;
- `git diff --check`: passed.

## Residual risks and one controlled LIVE validation

The conservative registry deliberately leaves 194 declared publishers
unverified. Expanding that registry requires independent source-domain
verification and must not be inferred from Yahoo distribution alone. This
offline replay proves deterministic behavior for the captured records; it does
not prove that a provider will return identical metadata later.

One subsequent controlled LIVE validation should:

1. clone the operational database and artifact inputs into a new isolated
   sandbox, record hashes, and prohibit AI/backend, browser, delivery, trading,
   and orders;
2. perform exactly one bounded provider acquisition outside all SQLite
   transactions, preserving provider/publisher/distributor lineage;
3. persist only into the isolated clone, materialize one snapshot/outbox
   transaction, then close network access;
4. execute full sync twice and verify byte identity, accounting, readiness,
   cited Reuters/IBD/AP/Federal Reserve records, safe unknown-publisher
   quarantine, and no byte/count caps;
5. ACK that full sync with a test consumer, add one controlled new/update/
   lifecycle fixture, and verify a record-only delta;
6. repeat the materialization at fixed point and require zero writes,
   revisions, and outbox rows;
7. compare the original operational DB and external artifact hashes with their
   pre-test values before accepting the LIVE result.
