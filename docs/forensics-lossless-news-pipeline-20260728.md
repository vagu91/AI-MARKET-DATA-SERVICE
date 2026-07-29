# Lossless news pipeline forensic correction — 2026-07-28

## Scope and safety

The adversarial correction was performed on draft PR #29, branch
`codex/fix-lossless-news-pipeline`, from initial HEAD
`30525e9cda4228ffbc749bb67dbc0ed12688ce8b` and base
`a11673e20c7d12cce09a1707ce1560e2fc96ccf9`.

The PR28 R4 SQLite sandbox and captured full sync were used only as offline
forensic inputs. The replay copied the SQLite file into two independent
temporary sandboxes, opened each with SQLite `mode=ro` and
`PRAGMA query_only=ON`, and verified source and copy hashes before/after. It
made zero provider, AI/backend, browser, delivery, trading or order calls. It
did not read `.env`, start Uvicorn or modify an operational database.

## Adversarial blockers found

1. Unknown or unverified editorial publishers were treated as invalid. The
   captured 273-row replay delivered 79 records and quarantined 194 solely
   because positive publisher verification was unavailable.
2. Missing publisher and safe unknown public hosts could be rejected even
   when timestamp, content and stable source identity were usable. A missing
   URL was always rejected despite a collision-safe provider record id.
3. A downstream hardening pass reconstructed news from current-day views,
   removed valid in-window history, and deduplicated by a semantic identity
   that could collapse distinct content.
4. Publisher, distributor and acquisition provider were not consistently
   distinct. A direct publisher could be repeated as its own distributor, and
   an acquisition provider could appear to support an editorial attribution.
5. Lifecycle, content availability, category/topic quality, raw identity,
   first/last-seen timestamps, warnings and source occurrences were not all
   retained through repository, sync and legacy-consumer projections.
6. The SQLite persistence `updated_at` could mask the original editorial
   `updated_at` on read-back.
7. Accounting did not implement the three required equations or identify each
   non-delivered record and reason. Legacy unexplained loss could be relabelled
   as technical invalidity.
8. News readiness lacked a stable `DEGRADED` state and could report valid
   uncertain delivery as unavailable. Empty legacy projections could be
   optimistically reported as authentic `NO_DATA`.
9. ACK validation bound consumer, snapshot and section revisions, but not the
   notified payload checksum. Record delta generation did not prove its base
   ACK/outbox checksum binding.

## Corrected contract

- UNKNOWN is quality metadata, not invalidity. A valid record is delivered
  with `source_verification_status=UNKNOWN`,
  `analysis_usability=DEGRADED` and explicit warning codes.
- Concrete quarantine is limited to corrupt/unparseable material,
  unrecoverable timestamps, dangerous URLs/content, certain policy
  contradictions or insufficient collision-safe identity. Outside-scope
  records have their own disposition.
- `articles` is the complete canonical in-scope inventory. `latest`,
  `historical_articles`, relevance views, clusters and digest are derived
  views and never replace it.
- LOW relevance, UNCLASSIFIED category, ambiguous topic, headline-only or
  summary-only content, unknown attribution and in-window history do not
  remove a record.
- Similar stories and temporal updates remain distinct. Only an exact
  editorial occurrence with the same publisher, timestamp, normalized title
  and content can be consolidated. All acquisition/distribution occurrences
  remain in `source_occurrences` and `distribution_lineage`.
- Acquisition provider, distribution source, original publisher and all
  verification states remain separate. A direct publisher is not represented
  as its own distributor.
- URL-less records require a stable source identity. No title-only fallback is
  used for persistence identity.
- Legacy lifecycle without `valid_until` is derived deterministically from
  `published_at` with a 24-hour current window and a warning. EXPIRED records
  inside the requested scope remain delivered as historical.
- Editorial `updated_at`, `first_seen_at` and `last_seen_at` survive
  persistence. The SQLite row timestamp is retained separately as
  `persistence_updated_at`.
- Full sync has no article-count, top-N or byte cap and performs no lossy
  summarization.
- News readiness is `AVAILABLE`, `DEGRADED`, `NO_DATA`, `UNAVAILABLE` or
  `QUARANTINED`. `NO_DATA` requires affirmative authentic-empty evidence.
- ACK requires the exact consumer, delivery, snapshot revision, changed
  section revisions and 64-hex outbox checksum. Wrong consumer/revision/
  checksum/section inventory is rejected. Delta generation falls back to a
  full section when the persisted ACK cannot be joined to the exact outbox
  checksum.
- Absence alone never confirms removal. Publisher/content/timestamp
  corrections produce material updates; retry and replay remain idempotent.

No schema migration was added. Schema 22 already stores the redacted complete
source payload and all required canonical persistence columns. The correction
is additive at projection/contract level and avoids an optimistic backfill.

## Required accounting

Captured pre-correction state:

| Measure | Before |
|---|---:|
| Raw acquired | 273 |
| Delivered | 79 |
| Quarantined only for unknown/unverified publisher | 194 |
| Technically invalid | 0 |
| Outside scope | 0 |

Final independent read-only replay:

| Measure | After |
|---|---:|
| Raw acquired | 273 |
| Persisted valid | 273 |
| Persisted valid in scope | 273 |
| Delivered source records | 273 |
| Delivered logical articles | 273 |
| Current delivered | 27 |
| Historical delivered | 246 |
| Quarantined with concrete reason | 0 |
| Withheld with concrete reason | 0 |
| Technically rejected | 0 |
| Outside scope | 0 |
| Non-delivered record IDs | none |
| Publisher VERIFIED | 79 |
| Publisher UNKNOWN | 194 |
| News readiness | DEGRADED |

The equations are exact:

```text
273 raw_acquired
= 273 persisted_valid + 0 technically_rejected

273 persisted_valid_in_scope
= 273 delivered + 0 quarantined_with_concrete_reason
   + 0 withheld_with_concrete_reason

273 delivered
= 27 current_delivered + 246 historical_delivered
```

Content availability is 3 full-text, 16 summary-only and 254 headline-only
records. Acquisition is 272 RSS plus one API record. Distribution is 241 via
Yahoo Finance and 32 direct/unknown. The delivered set includes 23 Reuters,
18 Investor's Business Daily, two Associated Press and ten Federal Reserve RSS
records. All 194 unverified publishers remain explicit and degraded; none is
optimistically promoted.

## Replay integrity

Two independently copied and read-only SQLite sandboxes produced byte-identical
17-section full syncs:

- exact canonical UTF-8 size: **16,299,930 bytes**;
- full-sync SHA-256:
  `CCE456ECBD51E27F6A485F41869D0BEE6B2BBA9E8F1BFDA14834A0A809E3AAC5`;
- contract checksum:
  `ebcabd43a74b3bd7d43deff3b4ecfbfcbd44ad6bce77be93c1ec792cb2364f1a`;
- source sandbox DB SHA-256 before/after:
  `FC51B5EE0F1E0B34B0DDE3449ACC7CC55B8DBF680C44EF0E5D694C419C998185`;
- source DB unchanged: true;
- both independent sandbox-copy hashes unchanged: true;
- new DB writes, snapshot revisions and outbox rows: zero;
- provider/AI/backend/browser/delivery/trading/order activity: zero.

No record was lost due to payload size, count, relevance, category, topic,
unknown attribution or historical lifecycle.

## Tests and static validation

- Dedicated lossless-news adversarial suite: **37 passed**.
- Focused news/sync/readiness/ACK/schema-22/snapshot-94 suite:
  **245 passed**.
- Explicit schema-22 upgrade/empty-proof/logical-key/policy-invalidation/
  concurrent-writer/rollback matrix: **10 passed**.
- Complete repository suite: **1,865 passed in 500.41 seconds**.
- Ruff: passed.
- `py_compile` for every changed Python file: passed.
- `compileall -q app scripts tests`: passed.
- `git diff --check`: passed.

The redacted fixtures contain synthetic/redacted source shapes only. Tests
cover null/unknown publishers, known distributor, acquisition-provider
separation, headline/summary/full content, URL-less stable identity, LOW and
UNCLASSIFIED metadata, ambiguous topics, historical delivery, temporal
updates, exact syndication lineage, exact technical duplicates, same-time
different content, stable order, multi-megabyte Unicode, no caps, exact
accounting, full/delta behavior, checksum-bound ACK, concurrency, retry,
rollback and fixed point.

## Residual risks and controlled LIVE plan

Real residual risks:

- 194 records remain attribution-uncertain and must stay visibly DEGRADED
  until independent publisher evidence exists.
- The deterministic 24-hour lifecycle derivation is conservative metadata for
  legacy rows, not publisher-supplied validity.
- The 16.3 MB payload requires consumer HTTP/deserialization/storage limits
  that do not truncate or summarize.
- AI Trader must adopt checksum-bearing ACKs before record delta is available;
  otherwise the safe behavior is full-section resync.
- Offline replay proves deterministic behavior for the captured sandbox, not
  future provider metadata. No LIVE validation was performed in this task.

One later controlled LIVE validation should:

1. clone operational DB and artifact inputs into a new isolated sandbox and
   record hashes;
2. allow one bounded acquisition only, outside SQLite transactions, with
   provider/publisher/distributor lineage preserved;
3. persist only to the isolated clone, then close network access;
4. materialize/full-sync twice and verify 17 sections, byte identity, exact
   accounting and no caps;
5. verify Reuters temporal updates, IBD, safe UNKNOWN delivery and any concrete
   quarantine reasons individually;
6. ACK with a test consumer using the exact checksum, then exercise one
   new/material-update/lifecycle-change delta and invalid ACK variants;
7. repeat at fixed point and require zero writes/revisions/outbox rows;
8. verify original operational DB and external artifact hashes are unchanged
   before accepting the result.

## 2026-07-29 adversarial acquisition follow-up

An independent review of HEAD
`45ff1ab489da392360b20515603862065dd22b0e` found that the downstream v3
projection was lossless for the captured database but the production
acquisition path could still lose records before they reached canonical
persistence:

1. `NewsProvider.fetch_for_symbols()` returned the first non-empty Alpha
   Vantage or GDELT result and therefore skipped every later RSS provider.
2. RSS fan-in deduplicated on `url or title` and stopped at an aggregate
   count, which could collapse temporal updates and discard later feeds.
3. GDELT, Alpha Vantage and RSS parsers rejected every URL-less record before
   testing provider-native or stable derived identity.
4. Persistence exceptions were caught with a silent `continue`; failed rows
   remained indistinguishable from successfully delivered rows.
5. `NasdaqDataService.latest_news()` applied another aggregate slice after
   acquisition.

The v4 correction replaces fallback precedence with independent provider
fan-in. Alpha Vantage (when configured), GDELT, Federal Reserve, BLS, BEA,
Yahoo Finance, MarketWatch and Google News RSS each have their own call,
coverage, raw/parsed/persisted IDs, rejection IDs, errors and configurable
per-provider limit. One provider failure cannot cancel another provider. No
aggregate cap is applied after fan-in.

Acquisition completes before persistence starts. Every persistence attempt
now records the raw ID, technical acquisition ID, outcome, typed error,
concrete reason code and retryability. Failed persistence is omitted from
delivered `articles` and produces explicit `PARTIAL` or `UNAVAILABLE`
readiness. A missing repository is `NOT_CONFIGURED`, not a false successful
delivery.

Identity is split into two non-lossy layers:

- `technical_acquisition_id` includes provider-native identity, acquisition
  provider, distributor, source URL and exact editorial occurrence. It is
  used only for exact acquisition retry detection and idempotent persistence.
- `editorial_occurrence_id` includes original publisher, canonical URL,
  published time, editorial update time, normalized title and content hash.
  It permits exact syndication consolidation while retaining every
  `source_occurrence` and `distribution_lineage`.

Consequently, same URL/different timestamp, same title/different content and
same URL/title/different editorial update remain distinct. Different direct
canonical URLs are no longer inferred to be duplicates from title similarity.
Exact syndicated copy through two distributors can share one logical article
only when the editorial identity is exact and both lineages are retained.

URL-less records are accepted when a provider-native ID/GUID exists or a
stable identity can be derived from provider/feed, timestamp, useful
title/content and content hash. Missing title is accepted when useful content
and stable source identity remain. Every record declares
`content_availability_status`, `canonical_url_status` and
`source_identity_status`.

### Provider accounting proofs

The fully mocked fan-in test exercised all eight enabled providers with a
per-provider limit of one:

```text
for each provider:
1 raw = 1 persisted + 0 technically rejected + 0 explicit out-of-scope

union:
8 valid source occurrences
= 8 canonical deliverable fixtures + 0 exact duplicates
```

The controlled persistence-fault test proves explicit partial delivery:

```text
GDELT:
2 raw = 1 persisted + 1 PERSISTENCE_RUNTIMEERROR + 0 explicit out-of-scope
```

The rejected record is absent from delivered `articles`, retains its exact raw
ID and is marked non-retryable. A separate provider-isolation test returns six
persisted records while one RSS provider reports independent `FAILED`
coverage.

The occurrence tests additionally prove:

```text
2 exact syndicated source occurrences
= 1 canonical logical article + 1 exact duplicate with complete lineage

2 exact technical retry occurrences
= 1 canonical logical article + 1 TECHNICAL_ACQUISITION_RETRY
```

### Updated offline replay and verification

The independent two-sandbox replay remains fully balanced:

```text
273 raw_acquired = 273 persisted_valid + 0 technically_rejected
273 persisted_valid_in_scope = 273 delivered + 0 quarantined + 0 withheld
273 delivered = 27 current_delivered + 246 historical_delivered
```

- sections: **17**;
- exact canonical UTF-8 size: **19,999,141 bytes**;
- SHA-256:
  `6458881A2063795F8F5EBED896D98981C36C7E5C35DE8F5C5BB472BBCD2060F5`;
- contract checksum:
  `5d82f8845ff0e592a9f0c3c4f63007256afc0faf0634369d33f077a2d875ba45`;
- two independent replay outputs byte-identical: true;
- replay database SHA-256 before/after:
  `FC51B5EE0F1E0B34B0DDE3449ACC7CC55B8DBF680C44EF0E5D694C419C998185`;
- replay database and both temporary copies unchanged: true;
- new writes, revisions and outbox rows: zero;
- provider LIVE, AI/backend, browser, delivery, trading and order calls: zero.

Offline validation after the v4 correction:

- new fan-in/identity/persistence adversarial suite: **18 passed**;
- focused news/sync/readiness/schema-22/snapshot suite: **275 passed**;
- complete repository suite: **1,883 passed in 561.03 seconds**;
- Ruff: passed;
- `py_compile` for every changed Python file: passed;
- `compileall -q app scripts tests`: passed;
- `git diff --check`: passed.
- secret scan across all 16 patch files: zero findings.

No controlled LIVE acquisition was run. The obsolete stage-2 script and its
old sandbox/preflight evidence were not reused or regenerated. The PR must
remain `OPEN`, `DRAFT` and `DO NOT MERGE`.

## 2026-07-29 stage2-v4 network-surface false positive

The first stage2-v4 execution stopped before service startup and before the
single-acquisition guard was created. The fail-closed static detector reported
this import:

```text
library: urllib
module: urllib.parse
callables: parse_qsl, urlencode, urlparse, urlunparse
production module: app.services.news_intelligence_service
production functions: canonicalize_url, classify_news_source
```

This was a detector false positive, not an unobserved transport.
`urllib.parse` only parses and serializes URL values. It exposes no HTTP client,
socket connection or network transport. Runtime regression coverage replaces
`socket.create_connection`, `socket.socket.connect` and
`urllib.request.urlopen` with fail-fast sentinels while exercising
`canonicalize_url`; all sentinels remain untouched.

The corrected callsite-aware detector now proves the complete reachable news
network surface:

```text
Alpha Vantage       _fetch_alpha_vantage                  httpx.AsyncClient.get
GDELT               _fetch_gdelt                          httpx.AsyncClient.get
RSS                  _fetch_one_rss_feed                   httpx.AsyncClient.get
metadata enrichment _enrich_missing_metadata.enrich       httpx.AsyncClient.get
```

Every reachable network call routes through both
`httpx.AsyncClient.send` and
`httpx.AsyncHTTPTransport.handle_async_request`, where stage 2 applies the exact
allowlist, redacted audit, timing, HTTP status and per-provider accounting.
Imports and callsites are distinguished: `urllib.parse` is explicitly
`NO_NETWORK_CAPABILITY`, while `urllib.request`, `requests`, `aiohttp`,
`urllib3`, raw sockets and other network-capable modules fail with library,
module, callable, file, enclosing function, line, reason code and reason.

The post-failure invariant check proved:

- acquisition guard absent;
- zero listeners on port 8053;
- zero residual PR29 Python/Uvicorn processes;
- operational main/WAL/SHM byte size, timestamp and SHA-256 unchanged against
  the evidence captured before the failed stage;
- `.env` still ignored and untracked, with a last-write timestamp preceding
  the failed run; its contents were not read;
- intentional consumer artifact SHA-256 unchanged:
  `BCED28DECDF98D65AF9843C3CF3FF23DAB0A164C721B8BEF9B3E0D7697699DD4`.

Offline verification after the detector correction:

- new network-surface regression suite: **4 passed**;
- focused news/provider/identity suite: **167 passed**;
- complete repository suite: **1,887 passed in 633.76 seconds**;
- no LIVE provider, Uvicorn, AI, browser, delivery, trading, order or
  operational migration activity.
