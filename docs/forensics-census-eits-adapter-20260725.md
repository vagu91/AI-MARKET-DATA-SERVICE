# Census EITS adapter forensic correction

## Scope and operating boundary

This correction is limited to the AI-MARKET-DATA-SERVICE Census EITS adapter,
its offline regression evidence, and its contract documentation. Validation uses
redacted fixtures and `httpx.MockTransport`. It does not call Census, Tradier, FRED,
BLS, BEA, Finnhub, Codex, OpenAI, or any other live service. It does not write the
operational database or AI-TRADER.

## Root cause

The adapter put the predicate-only `time` variable in `get`, which Census rejects
with HTTP 400. Removing that field alone would still leave a semantic defect: the
normalizer filtered chiefly by period and `category_code`, then chose
`matches[-1]`. EITS responses contain multiple data types, seasonal variants,
measures, and time slots for the same period and category, so the selected value
depended on provider row order.

The service-owned mappings also contained three wrong identities:

- ADVM3 used category `00` without the `NO` new-orders data type;
- RESCONST mapped housing starts to `APERMITS`;
- RESCONST mapped permits to `PERMITS`.

The first PR revision then introduced a residual temporal blocker: it treated
`time_slot_id` as an absolute month index derived from hardcoded epochs. Manual
read-only validation supplied for `time=2026-05` showed that MARTS, ADVM3,
RESCONST, and FTD all return `time_slot_id="0"` when the response window contains
one month. The field is a response-window offset, not a calendar identity. The
calculated values 413/809 therefore rejected valid exact economic tuples.

## Official mapping decision

The official Census EITS variable definitions establish that `time` is a predicate
and that category, value, data type, error marker, program, seasonal variant, and
time-slot fields are output variables. The official program dictionaries establish
these exact tuples:

| Semantic field | Dataset / program | Exact category | Exact data type | Adjustment | Frequency / unit |
|---|---|---|---|---|---|
| Advance retail sales | MARTS / MARTS | `44X72` | `SM` | adjusted | monthly / millions USD |
| Durable goods new orders | ADVM3 / M3ADV | `MDM` | `NO` | adjusted | monthly / millions USD |
| Housing starts | RESCONST / RESCONST | `ASTARTS` | `TOTAL` | adjusted | monthly SAAR / thousands |
| Building permits | RESCONST / RESCONST | `APERMITS` | `TOTAL` | adjusted | monthly SAAR / thousands |
| Goods and services balance | FTD / FTD | `BOPGS` | `BAL` | adjusted | monthly / millions USD |

Official sources:

- [Economic Indicators Time Series overview](https://www.census.gov/data/developers/data-sets/economic-indicators.html)
- [MARTS variables](https://api.census.gov/data/timeseries/eits/marts/variables.html)
  and [official dictionary](https://www.census.gov/econ_getzippedfile/?programCode=MARTS)
- [ADVM3 variables](https://api.census.gov/data/timeseries/eits/advm3/variables.html)
  and [official M3ADV dictionary](https://www.census.gov/econ_getzippedfile/?programCode=M3ADV)
- [RESCONST variables](https://api.census.gov/data/timeseries/eits/resconst/variables.html)
  and [official dictionary](https://www.census.gov/econ_getzippedfile/?programCode=RESCONST)
- [FTD variables](https://api.census.gov/data/timeseries/eits/ftd/variables.html)
  and [official dictionary](https://www.census.gov/econ_getzippedfile/?programCode=FTD)
- [EITS API User Guide](https://www2.census.gov/data/api-documentation/EITS_API_User_Guide_Dec2020.pdf)

The manual live evidence supplied to this correction reported the same temporal
shape for every program: `time="2026-05"`, `time_slot_id="0"`,
`time_slot_date="2026-05-01 00:00:00.0"`, and
`time_slot_name="May2026"`. Implementation and validation in this PR remain
strictly offline; that evidence is represented only by a redacted structural
fixture with synthetic economic values.

## Before and after

Before:

```text
get=category_code,cell_value,data_type_code,seasonally_adj,time
time=<period>
selection=(category_code, period), then matches[-1]
```

First PR revision (still incorrect):

```text
get=category_code,cell_value,data_type_code,error_data,program_code,seasonally_adj,time_slot_id
time=<period>
selection=(dataset, program_code, category_code, data_type_code,
           seasonal variant, exact period, exact time_slot_id, non-error row)
```

Final:

```text
get=category_code,cell_value,data_type_code,error_data,program_code,
    seasonally_adj,time_slot_date,time_slot_id,time_slot_name
time=<period>
selection=(dataset, program_code, category_code, data_type_code,
           seasonal variant, exact returned time, parsed month-start date,
           non-error row, valid cell_value)
```

ADVM3 keeps its official national `for=us:*` predicate outside `get`. All query
credentials are redacted from URLs, telemetry, exceptions, fixtures, snapshots,
and reports.

`time_slot_id` is preserved exactly as returned but never calculated or matched.
`time_slot_name` is descriptive lineage only. The parsed `time_slot_date` is a
secondary consistency check; the requested and returned `time` value is the
primary temporal identity.

Zero exact matches produce audited `NO_DATA`. Multiple exact matches produce
`ambiguous_census_mapping`; only redacted SHA-256 row hashes remain in lineage.
No arbitrary value reaches the consumer.

## Lineage and lifecycle

Accepted observations preserve the provider, dataset, program, exact period,
category, data type, raw and canonical seasonal adjustment, returned `time`,
`time_slot_id`, `time_slot_date`, `time_slot_name`, semantic field, frequency,
unit, original decimal text and precision, retrieval time, raw-payload hash,
request fingerprint, and redacted source URL. Occurrence IDs include series and
period, so the two RESCONST measures cannot collapse into one occurrence and a
relative slot offset cannot redefine the month.

Census does not provide a release timestamp in this response contract, so none is
invented. `valid_until` and `next_refresh_at` are computed from retrieval time and
the configured cache TTL. A historical period therefore cannot create a permanent
refresh loop by scheduling its next refresh in the past.

## Offline regression evidence

The redacted fixture
`tests/fixtures/census_eits_adapter_regression_redacted.json` records the broken and
corrected query shapes, semantic distractors, two ambiguous exact rows, and the
expected exact tuple. Parametrized tests cover all five indicators, slot zero,
query predicates, credential redaction, order independence, wrong categories,
wrong data types, SA/NSA, provider error flags, wrong returned periods, wrong
parsed month starts, invalid values, zero/ambiguous matches, 400/401/403 terminal
behavior, historical lifecycle scheduling, and zero AI job/backend persistence.

Final offline validation:

- dedicated Census regression file: 25 passed;
- Census plus deterministic-provider targets: 47 passed;
- provider/actual/recovery/lifecycle/freshness/consumer/snapshot/atomicity
  selection: 569 passed;
- complete suite: 1,564 passed in 259.01 seconds;
- explicit schema-20 compatibility and preservation matrix: 5 passed;
- Ruff (whole repository), `py_compile`, `compileall`, and `git diff --check`:
  passed;
- provider-force offline replay: 31,775-byte consumer, zero AI jobs, zero
  research runs/backend invocations, deterministic output, weekend behavior
  preserved, secrets absent, zero live calls;
- snapshot-83 offline replay: passed, 36,651-byte consumer, zero live calls.

No Census, deterministic provider, AI backend, Codex CLI, OpenAI, browser,
Uvicorn, trading, account, or order endpoint was invoked during implementation
or validation.
