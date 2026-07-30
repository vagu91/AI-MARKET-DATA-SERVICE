# Senior Analyst data-source matrix

This is the human-readable companion to
`docs/baselines/senior-analyst-data-source-matrix.json`. The JSON file is
authoritative and contains timeout, retry, repository, SLA, expiry and last
observed LIVE result for every row. An undocumented rate limit is recorded as
`UNKNOWN`; no probing was performed.

| Dataset | Metrics | Primary | Ordered fallbacks | Freshness / fail-closed rule | Snapshot 98 |
|---|---|---|---|---|---|
| Nasdaq-100 | membership | Nasdaq | none | 12h; null if invalid | available |
| Mega-cap quotes | AAPL, NVDA, AMZN, META, TSLA, AMD price/change/weight | Nasdaq | none | one canonical quote occurrence | available; legacy drivers contradicted quotes |
| Market internals | A/D, breadth, advancers | Tradier | none | 2h; reject derivation with stale constituents | rejected |
| VIX | VIX | FRED | CBOE | last official close within 2 days | expired |
| VVIX | VVIX | CBOE | none | 2h | expired |
| Risk | sentiment, score | CBOE-derived | none | excluded inputs never contribute | rejected |
| Treasury | DGS2, DGS10, T10Y2Y, SOFR | FRED | none | per-series daily SLA | partial |
| Fed Funds | DFF, FEDFUNDS, SOFR | FRED | none | daily/monthly cadence | partial |
| Target range | DFEDTARL/U, official outcome | Federal Reserve | FRED | after release require official outcome | pre-release |
| FOMC expectations | pre-meeting probabilities | Investing Fed monitor | none | historical at release time | no probabilities |
| CPI | headline/core index | BLS | none | latest official monthly release | headline missing; core available |
| PPI | final-demand index | BLS | none | latest official monthly release | available |
| PCE | nominal spending, headline/core price indexes | BEA | none | distinct semantics; monthly cadence | headline price index missing |
| GDP | annualized growth, real level | BEA | none | distinct transformations; quarterly cadence | available |
| Employment | unemployment rate | BLS | none | latest official monthly release | available |
| Wages | average hourly earnings | BLS | none | changes require valid history | level available |
| NFP | payroll level and monthly delta | BLS | none | never reinterpret level as delta | level available; delta null |
| Claims | ICSA | FRED | none | latest official weekly release | 187000 claims |
| Macro calendar | actual, consensus, previous, revisions | canonical event repository | Investing, XTB | exact occurrence identity | legacy duplicates/past awaiting |
| Flash Services PMI | actual, forecast, previous | S&P Global | Investing event 1062 | exact occurrence/time/period | primary 403; fallback fixture passes offline |
| Earnings | issuer event | Nasdaq | FMP | canonical issuer/date, -1d/+14d | no current canonical event |
| Options | IV, OI, volume, skew | Tradier | none | 2h; future timestamps rejected | rejected future |
| Positioning | CFTC MNQ COT | CFTC | none | latest official weekly report | available |
| News | current articles | Finnhub | RSS, official sources | 24h; history excluded | no current news |
| Schedule | Nasdaq cash, MNQ futures | Nasdaq Market Info | CME, Investing holidays, MarketBeat | partial if unverified | partial |

## DB-first and fallback invariant

For every row, the production request first reads the canonical repository. A
valid record is returned without a provider call. An expired record causes the
primary and then the listed fallbacks to be attempted in order. If no valid
observation is selected, the analytical value is `null`; the expired value may
remain only in technical storage and never contributes to readiness or derived
sentiment.

The only provider added by this change is `INVESTING_EVENT_1062`, and it is
restricted to Flash Services PMI.
