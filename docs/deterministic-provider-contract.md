# Deterministic provider contract

## Purpose

The service resolves committed fresh data first, then deterministic providers, then an
explicit stale grace when policy permits it. AI is eligible only for authorized
qualitative residual fields. Numeric gaps remain `NO_DATA`; the service never asks AI
to reconstruct prices, actuals, estimates, EPS, revenue, volume, open interest,
Greeks, IV, yields, strikes, release dates, or timestamps.

Provider/domain enablement is independent from specialist AI-agent enablement. In
particular, options positioning, market internals, cross-asset context, and earnings
intelligence remain deterministic domains when their corresponding AI flags are
false.

## Provider and AI authority

Every refresh carries one immutable execution context with
`allow_live_providers`, `allow_ai`, `request_origin`, and `correlation_id`.
`refresh=false|auto|force` may affect deterministic-provider authority only;
none of those values can set `allow_ai=true`. Provider refresh routes always
construct a provider-only context.

The final persistent queue method validates the context, known job type,
research master switch, and per-agent switch. Scheduler, recovery, lifecycle
resolver, retry, worker acquisition, and the agentic runtime preserve and
re-check the same context. An explicit queue API is an authority boundary;
provider/on-demand services must pass their provider-only context. Decisions
are recorded as `AI_ALLOWED`, `AI_SUPPRESSED`, or `AI_NOT_REQUIRED` without
prompts, credentials, headers, or source content.

Resolution order is cache/provider, normalization and validation, residual-gap
calculation, optional explicitly authorized AI, coalescing/idempotency, then
persistence. A legitimately empty weekend/holiday calendar is not a gap.

## Consumer and materialization budgets

Consumer 2.1 uses canonical compact JSON (`UTF-8`, separators `,` and `:`) and
deterministic per-section byte budgets. Oversized sections are semantically
deduplicated, relevance/recency sorted, item-bounded, stripped of raw provider
and diagnostic records, and summarized if still above their budget. Option
chains expose verified aggregates only; individual contracts remain in
debug/audit storage. The `compaction` section records total bytes, section
bytes, item counts before/after, removed/deduplicated counts, and reason.

Snapshot preflight builds and validates debug and consumer projections,
contract/schema, canonical byte size, source policy, and temporal policy before
the snapshot/component/link/outbox transaction begins. Provider cache and
observation writes are reusable fetch evidence and remain separate.

## Common envelope

Every adapter can project a result through `BaseProvider.provider_contract`. New
adapters additionally use `ProviderEnvelope` and `NormalizedObservation`.

The envelope contains:

- provider ID, kind, authority tier, and requested domain;
- retrieval and provider timestamps;
- redacted source URL and a secret-free request fingerprint;
- cache/freshness state and exact occurrence identity;
- normalized observations, warnings, and rejection reasons;
- rate-limit and retry classification;
- a SHA-256 of the redacted canonical raw payload;
- lifecycle, snapshot, and outbox lineage;
- request telemetry that distinguishes actual provider requests from cache reads and
  resolver evaluations.

Values are validated without truthiness shortcuts, so numeric zero is preserved.
Single objects and arrays normalize to the same internal shape. Empty, malformed,
non-finite, future-dated, or semantically mismatched observations are rejected or
reported as explicit `NO_DATA`.

## Provider matrix

| Provider | Authority | Allowed endpoints/data | Primary use | Trigger policy |
|---|---|---|---|---|
| FRED | Tier 1 official economic redistributor | Series observations for DGS2, DGS10, DGS30, FEDFUNDS, SOFR, T10Y2Y, T10Y3M, VIXCLS, NFCI, WALCL | Rates, curve, liquidity, financial conditions, volatility history | `NON_TRIGGERING` |
| BLS | Tier 1 official | Public Data API v2, explicit CPI/core CPI/PPI/NFP/unemployment/wages/ECI registry | Exact macro actual and revisions | New exact actual/revision is `TRIGGER` |
| BEA | Tier 1 official | NIPA tables selected by table and line identity | GDP, GDP price index, PCE, core PCE, income, spending | New exact actual/revision is `TRIGGER` |
| Census | Tier 1 official | EITS MARTS, ADVM3, RESCONST, FTD | Retail sales, durable goods, housing starts, permits, trade | New exact actual/revision is `TRIGGER` |
| Tradier | Tier 2 licensed deterministic market-data vendor | GET `/markets/quotes`, `/markets/options/expirations`, `/markets/options/chains` only | QQQ options, Nasdaq-100 breadth proxy, ETF cross-asset proxies | `REFRESH_ON_TRIGGER` |
| Finnhub | Tier 3 structured vendor/discovery | `/stock/profile2`, `/calendar/earnings`, `/company-news` | Symbol validation, structured earnings, news candidate discovery | Earnings actual/material accepted news can trigger |

Finnhub is not issuer official. A Finnhub news record is a candidate only. Source
independence is based on the original article domain, never `finnhub.io`. Candidate
articles still pass through Source Gateway fetch, classification, verification,
deduplication, freshness, and materiality checks before becoming current news.

## Explicit mappings

### BLS

The service owns an explicit mapping for:

- `CUSR0000SA0` and `CUUR0000SA0`: headline CPI;
- `CUSR0000SA0L1E` and `CUUR0000SA0L1E`: core CPI;
- `WPSFD4` and `WPUFD4`: final-demand PPI;
- `CES0000000001`: total nonfarm payrolls;
- `LNS14000000`: unemployment rate;
- `CES0500000003`: average hourly earnings;
- `CIU1010000000000A`: Employment Cost Index.

Each observation carries unit, frequency, seasonal adjustment, reference period, and
release vintage. The exact event occurrence supplies the expected reference period;
the latest available value is never attached to a different occurrence merely
because its series ID matches.

### BEA

NIPA rows are selected by stable table and line identity, never array position:

- GDP: T10101 line 1;
- optional GDP price index: T10101 line 4;
- real GDP: T10106 line 1;
- PCE level: T20805 line 1;
- headline/core PCE index: T20804 lines 1/25;
- personal income/spending: T20600 lines 1/28.

Frequency, units, seasonal adjustment, annualization, period, and revision vintage
remain in lineage. Estimate vintage is taken from official response metadata when
available; it is not inferred from array order.

### Census

Production requests require an exact lifecycle period (`YYYY-MM` or `YYYY-Qn`).
There is no historical `time=2012` default.

- MARTS: program `MARTS`, category `44X72`, data type `SM`, adjusted, monthly,
  millions of dollars: advance retail and food-services sales;
- ADVM3: program `M3ADV`, category `MDM`, data type `NO`, adjusted, monthly,
  millions of dollars: advance durable-goods new orders;
- RESCONST housing starts: program `RESCONST`, category `ASTARTS`, data type
  `TOTAL`, adjusted, monthly SAAR, thousands of units;
- RESCONST building permits: program `RESCONST`, category `APERMITS`, data type
  `TOTAL`, adjusted, monthly SAAR, thousands of units;
- FTD: program `FTD`, category `BOPGS`, data type `BAL`, adjusted, monthly,
  millions of dollars: goods-and-services balance.

`time`, `for`, `in`, and `ucgid` are predicate-only and cannot enter `get`.
The adapter requests only the value and exact discriminants, including
`program_code`, `data_type_code`, `seasonally_adj`, `error_data`, and
`time_slot_id`. Period, program, category, data type, seasonal variant, and the
period-derived time slot must all match. Zero matches are deterministic `NO_DATA`;
multiple exact matches are `ambiguous_census_mapping`. Neither case is materialized.
Raw multi-row data is represented by hashes in audit lineage and is never copied to
the consumer.

The mapping is sourced from the official EITS variable definitions and program data
dictionaries:

- [MARTS variables](https://api.census.gov/data/timeseries/eits/marts/variables.html)
  and [MARTS dictionary](https://www.census.gov/econ_getzippedfile/?programCode=MARTS);
- [ADVM3 variables](https://api.census.gov/data/timeseries/eits/advm3/variables.html)
  and [M3ADV dictionary](https://www.census.gov/econ_getzippedfile/?programCode=M3ADV);
- [RESCONST variables](https://api.census.gov/data/timeseries/eits/resconst/variables.html)
  and [RESCONST dictionary](https://www.census.gov/econ_getzippedfile/?programCode=RESCONST);
- [FTD variables](https://api.census.gov/data/timeseries/eits/ftd/variables.html)
  and [FTD dictionary](https://www.census.gov/econ_getzippedfile/?programCode=FTD).

Historical observations refresh relative to retrieval/cache policy. They never
schedule a permanent loop by deriving `next_refresh_at` from an old reference period,
and no release timestamp is invented when Census does not supply one.

## Tradier read-only boundary

The adapter exposes no account, balance, position, history, order, preview, or
streaming method. It accepts only GET and the three exact allowlisted paths. Redirects
are disabled and rejected. Only `api.tradier.com` and `sandbox.tradier.com` are
transport hosts. `stream.tradier.com` is configuration-only, and streaming must remain
disabled.

QQQ is always labeled as a Nasdaq-100 liquid ETF proxy for MNQ. It is never represented
as MNQ. Option-expiration selection is deduplicated and capped at three: first
available, next available weekly candidate, and nearest to 30 days.

The conventional gamma proxy is:

`gamma × open_interest × contract_size × spot² × 0.01`

Calls are positive and puts negative only in the signed conventional view. Outputs
state `is_model_derived=true`, `sign_assumption=conventional`, and
`dealer_positioning_confirmed=false`. A missing contract size is excluded; 100 is
never silently assumed. Division by zero yields null plus `DENOMINATOR_ZERO`.

## Deterministic derived domains

- Options positioning: put/call volume and OI, top strikes, expiry concentration, ATM
  IV, deterministic skew, liquidity spreads, Greeks/OI coverage, and gamma proxies.
- Market internals: Nasdaq-100 constituent breadth, QQQ-weighted breadth, up/down
  volume proxy, dispersion, mega-cap concentration, missing/stale/rejected coverage.
  It is not official Nasdaq TICK/TRIN.
- Cross asset: QQQ, SPY, IWM, DIA, TLT, HYG, LQD, GLD, USO, and UUP ETF returns plus
  FRED rates. ETF observations remain explicitly labeled as proxies.
- Earnings intelligence: issuer/SEC evidence has priority for official announcements
  and actuals; Finnhub supplies structured schedules, estimates, candidate actuals,
  and candidate news.

Fast components are non-triggering and refresh on an external material trigger. One
material trigger produces one coherent snapshot and at most one idempotent outbox
envelope.

## Freshness and lifecycle

- Tradier quotes: short intraday TTL; delayed/stale is explicit.
- Tradier chains: refresh on trigger with a minutes-scale cache.
- Expirations: daily cache.
- Open interest: daily unless provider evidence indicates otherwise.
- Market internals and cross asset: refresh on trigger.
- FRED: validity follows daily/weekly/monthly series frequency and market calendars.
- BLS/BEA/Census: occurrence and reference-period lifecycle.
- Earnings schedule: valid until occurrence or schedule change.
- Earnings actual: separate post-release lifecycle.
- Company news: publication-time/materiality TTL.

Where applicable, records carry `observed_at`, `retrieved_at`, `release_at`,
`event_at`, `valid_until`, `next_refresh_at`, freshness, lifecycle state, and source
lineage.

## Security

FRED, BEA, and Census query secrets are redacted before errors, logs, lineage,
telemetry, snapshots, and outbox persistence. BLS registration keys are body-only and
redacted. Finnhub uses `X-Finnhub-Token`; Tradier uses `Authorization: Bearer`.
Authentication headers are always redacted.

Provider transports require HTTPS, reject redirects and userinfo, and use exact host
allowlists. The central source policy rejects localhost, non-public IPs, reserved test
domains, and DNS impersonations such as `api.bls.gov.evil.com`,
`finnhub.io.evil.com`, and `tradier.com.evil.com`.

## Offline validation and post-merge smoke

All implementation validation uses redacted fixtures or mocked transports. It is not
a live verification.

After merge, an operator may run one separately authorized read-only smoke:

1. verify only that required environment-variable names are present;
2. call one benign endpoint per enabled provider;
3. for Tradier, call only a QQQ quote, expirations, and at most one chain;
4. confirm zero account/order paths, zero writes, and redacted telemetry;
5. inspect provider request counts, cache behavior, lifecycle linkage, snapshot size,
   and a locally produced outbox envelope;
6. stop without delivering the envelope to AI-TRADER.
