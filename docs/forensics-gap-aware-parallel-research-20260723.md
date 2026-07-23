# Offline forensics: MNQ gap-aware parallel research

## Evidence examined

The analysis used only the supplied ZIP and its already-present local extraction under
`data/market-research-smoke-atomicfix-20260723`. No provider, browser, Codex, OpenAI,
or market-data request was made.

The terminal run was
`rrun-f6c7c83d-9fa4-4638-99e2-9cd2749df5d0`, linked to job
`airj-17b9f74e-adef-46c4-82aa-65e230e02392`. It completed all ten declared phases
and ended `PARTIAL`. Its persisted counters are internally coherent:
11 candidates, 6 accepted, 6 persisted, and 6 read back. Telemetry records one
Codex invocation, 3 executed searches, 9 opened sources, 13 discovered sources,
6 verified sources, 156,883 tokens, and 145,633 ms of AI duration.

The compact immutable replay is
`tests/fixtures/atomicfix_run_20260723_offline_replay.json`.

## Demonstrated root causes

### Snapshot projected an unrelated historical run

Snapshot `mcs-fc1be2c3-8fe8-4d09-8ca3-81d937e3076c`, revision 67, was generated
for the new job but its consumer projected run
`rrun-f88713ec-3e4e-4655-a117-e3274fa53cb7` with status `FAILED`.

The code cause was `DBOnlyMarketContextMaterializer`: it copied the prior
snapshot `debug_payload`, removed only a short exclusion list, and therefore
retained the old `research`, readiness, schedule, summary, lifecycle, and other
clock-sensitive values. No immutable `research_run_id` existed on the snapshot,
and projection was not validated against `source_job_id`.

### Session state was copied rather than recalculated

At `2026-07-23T15:25:40Z` the forensic consumer reported MNQ `open`, Nasdaq cash
`market_closed`, global `market_closed`, and a same-day Nasdaq close at
`20:00:00Z`. The deterministic session rules correctly classify 11:25:40 ET on
that Thursday as open. The copied hardening metadata also caused
`harden_market_context` to return early for the same context date.

### Impossible 2099 event remained selectable

The forensic consumer selected `evt-cpi`, release
`2099-07-14T12:30:00Z`, as a valid pre-release CPI event, including consensus
`0.3%` and previous `0.2%`. There was no service-owned future-horizon audit;
lifecycle logic therefore treated a syntactically valid timestamp as valid
domain data.

### Research was not DB/API-first

The request passed a large prior snapshot to one broad MNQ profile. It did not
persist a server-owned topic decision before invoking the backend. The forensic
consumer already contained available earnings, Nasdaq context/holdings, a macro
snapshot, a market schedule, and stale-but-present rates expectations. Despite
that, the broad query mixed BLS/BEA, VIX, and CFTC terms. Telemetry proves only
three searches were executed, even where a declared plan described more.

### Source and reliability semantics were conflated

The source gateway stored only `FETCHED` or `REJECTED`, so HTTP success followed
by PDF extraction failure could later surface as `source_not_fetched`.
Agent-derived market facts were also projected with hard-coded
`reliability=0.0`, discarding the service-owned source tier.

## Final architecture

Schema 16 adds immutable snapshot links, persisted gap manifests/items,
persistent parent/child coordination, query/source counters, source stage
audit, component snapshots, event semantics, and non-destructive temporal
quarantine.

The request flow is now:

1. read committed deterministic components;
2. persist `ResearchGapManifest`;
3. create only specialized children whose `required_action` is
   `AGENT_RESEARCH`;
4. execute independent children with bounded concurrency (default 4), existing
   leases, heartbeats, retry/checkpoint state, telemetry, and isolated
   workspaces;
5. acquire and verify sources through the service-owned gateway;
6. aggregate only terminal, counter-consistent children;
7. rebuild an immutable snapshot from committed components and records;
8. recalculate schedule, event/news lifecycle, readiness, quality, and consumer
   summary at the snapshot clock;
9. project research exclusively through the validated snapshot link.

`AI_MARKET_RESEARCH_BACKEND=codex_cli|openai_api` selects one adapter explicitly.
Both adapters implement `ResearchBackend`, receive the same compact normalized
input, and return the same output contract. There is no automatic fallback.

All ten MNQ topics are declared applicable. A bounded search with no current
result is `NO_CURRENT_ITEM`, never `NOT_APPLICABLE`.
