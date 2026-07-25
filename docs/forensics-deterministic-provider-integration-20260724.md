# Forensics: deterministic provider integration — 2026-07-24

## Scope and constraints

This integration consolidates FRED, BLS, BEA, Census, Finnhub, and Tradier into one
provider-first change set. Work started from commit
`873a3b10ed52d4d4e211aad986aa97bc9cd78c4b`.

No live provider, browser, AI, Codex CLI, OpenAI, service restart, operational
database migration, trading, account, or order call was performed. Validation is
offline only.

The existing schema 020 is sufficient:

- `provider_observations` stores redacted generic provider audit;
- provider cache entries support valid, stale, last-known-good, and negative states;
- datum lifecycle items hold exact occurrence, retry, freshness, and lineage;
- component and snapshot tables hold compact normalized projections;
- snapshot and outbox writes already share one SQLite transaction.

Migration 021 was therefore not created.

## Before and after

Before this change, FRED/BLS/BEA existed as independent macro adapters, while Census,
Finnhub, and a technically restricted Tradier client were absent. Specialist domain
flags could also be read as if a disabled AI agent disabled its entire data domain.

After this change:

- all providers expose a common secret-free deterministic contract;
- provider enablement and domain enablement are separate from AI-agent enablement;
- FRED includes the required curve/liquidity series and treats `.` as missing;
- BLS adds explicit ECI coverage and preserves zero/revision/reference-period data;
- BEA includes a stable GDP price-index mapping without array-position selection;
- Census requires exact periods and compacts MARTS, ADVM3, RESCONST, and FTD;
- Finnhub handles profiles, earnings sessions/actuals/estimates, and candidate news;
- Tradier is constrained to three market-data GET endpoints;
- pure deterministic services compute QQQ options, Nasdaq-100 internals, and
  cross-asset proxies;
- the provider-first planner excludes numeric gaps from AI and limits residual AI to
  authorized qualitative fields;
- consumer 2.1 exposes compact deterministic sections and explicit domain/provider/AI
  status while retaining the 90 KB guard.

## Safety findings

Authentication values are marked `repr=False` in settings. Query, body, and header
redaction covers Census/FRED/BEA keys, the BLS registration key, Finnhub token, and
Tradier bearer token. Request fingerprints are computed from redacted canonical
inputs. Raw-payload hashes operate on redacted canonical payloads.

The transport boundary requires HTTPS, exact provider hosts, no redirects, bounded
retry with exponential backoff, and a circuit breaker. Tradier additionally validates
the exact method and path before transport construction. There are no methods for
orders, previews, accounts, balances, positions, history, or streaming.

Source policy v4 now protects both official and vendor DNS boundaries.
Finnhub news independence uses the original article domain.

## Lifecycle and trigger behavior

Existing exact-occurrence lifecycle wiring already covers delayed startup:

1. a bounded lookback finds the occurrence at its original release minute;
2. the official provider resolves the exact expected reference period;
3. a newly published actual transitions the same lifecycle item;
4. snapshot and outbox persist atomically;
5. idempotency prevents a second restart/tick from duplicating the outbox event.

Census was added to the same official actual resolver. Earnings records expose stable
occurrence IDs using issuer, scheduled date, and BMO/AMC/DMH session.

Rates, options positioning, market internals, and cross asset are non-triggering or
refresh-on-trigger. They cannot autonomously notify AI-TRADER.

## Derived-data limits

QQQ is a proxy for the Nasdaq-100/MNQ context, not MNQ itself. ETF cross-asset rows
are not futures or spot. Constituent breadth is not official Nasdaq TICK/TRIN.

Gamma output is a conventional model-derived proxy, not confirmed dealer
positioning. Contracts missing contract size are excluded. No numeric field is
imputed, and divide-by-zero produces null with a reason code.

Finnhub company news remains discovery-only until Source Gateway verification,
deduplication, freshness, and materiality acceptance. The 250-item cap is observable
and candidates are compacted before consumer projection.

## Offline evidence

The dedicated fixture suite covers:

- configuration separation and secret-free settings representation;
- source-policy DNS boundary failures;
- query/body/header redaction and request fingerprints;
- retry and malicious redirect rejection;
- all four Census programs, exact-period enforcement, zero and precision;
- Finnhub BMO/AMC records, null/zero actuals and estimates, surprises, Unicode,
  deduplication, original-domain independence, and candidate-only news;
- Tradier object/list normalization, zero values, quote/expiry/chain parsing,
  disallowed methods and paths, and absence of an order surface;
- maximum-three expiry selection;
- option ratios, missing contract size, gamma-proxy labeling;
- constituent breadth, missing coverage, weighted contributions, and cross-asset
  divergence/stale-rate warnings;
- AI cost avoidance for numeric gaps;
- NaN/Infinity, crossed quote, and negative-strike anomaly detection.

The existing BLS, BEA, configuration, macro exact-occurrence, lifecycle, snapshot,
consumer, outbox, security/quarantine, and full regression suites remain the
authoritative regression evidence.

Final offline validation:

- focused provider/security/lifecycle/snapshot/outbox/consumer suites: passed;
- complete suite: **1,470 passed** (baseline supplied in the task: 1,440);
- Ruff: passed;
- `py_compile` and `compileall`: passed;
- `git diff --check`: passed;
- aggregate fixture replay: passed with zero network and zero AI calls;
- modified PowerShell scripts: none, so no PowerShell 5.1 parse target;
- migration matrix: existing schema 020 tests passed; no schema 021 exists.

The suite emits one dependency-level `StarletteDeprecationWarning` because the
installed Starlette test client falls back to `httpx` when optional `httpx2` is not
installed. Warning-as-error isolates it during test collection; it is unrelated to
this change and the normal complete suite is green.

## Residual gaps

- Provider responses are intentionally validated offline; live behavior was not
  re-tested in this change.
- Census program codes are service-owned mappings and must be updated deliberately
  if the official contract changes.
- Finnhub actuals remain vendor data until confirmed by issuer/SEC evidence where the
  field requires official authority.
- Guidance remains `NO_DATA` unless an official source provides it; AI may summarize
  verified qualitative guidance but cannot manufacture it.
- Live outbox delivery is out of scope. Only envelope production is validated.

## Post-merge operator procedure

Run exactly one separately authorized read-only smoke as documented in
`docs/deterministic-provider-contract.md`. Never include secret values in command
output or reports. Do not enable Tradier account access, trading, or streaming, and
do not deliver the generated outbox envelope to AI-TRADER as part of that smoke.
