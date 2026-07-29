# PR 29 controlled-live acceptance reconciliation

## Scope and immutable evidence

This report re-evaluates the already-consumed controlled-live evidence at
`data/news-pipeline-verification/20260729T133210Z/controlled-live/`.
No provider call or new controlled-live guard was used during the
reconciliation.

The primary evidence file is
`controlled-live-validation.json` (7,008,272 bytes,
SHA-256
`AA415C6F6AA6C29D1088129D3C8A17ABDDB0FAD72F7A5C5D0EF44F5D98421589`).
Its network instrumentation and provider identity accounting both pass.

## Provider reconciliation

| Provider | Raw | Persisted | Outside scope | Technical rejection | Calls | Evidence state |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| GDELT Doc API | 0 | 0 | 0 | 0 | 3 | `ConnectTimeout`, two bounded retries |
| Federal Reserve RSS | 20 | 10 | 10 | 0 | 1 | complete |
| BLS RSS | 1 | 1 | 0 | 0 | 1 | complete |
| BEA RSS | 46 | 3 | 43 | 0 | 1 | complete |
| Yahoo Finance RSS | 49 | 49 | 0 | 0 | 9 | main RSS complete; eight optional metadata attempts returned 307 |
| MarketWatch RSS | 10 | 10 | 0 | 0 | 1 | complete |
| Google News RSS | 100 | 90 | 10 | 0 | 1 | complete |
| **Total** | **226** | **163** | **63** | **0** | **17** | exact |

The identity equation is:

```text
226 raw
= 163 persisted
 + 0 technically rejected
 + 63 explicitly outside scope
 + 0 exact technical duplicates
 + 0 persistence failures
```

All 163 response article identities are present in the persisted identity
set. The eight Yahoo records whose optional metadata request returned 307
are in both sets. The redirect therefore caused no article loss, exclusion
or persistence mutation. The old `PARTIAL` main-provider state was caused
only by coupling optional enrichment status to RSS coverage.

GDELT is an accessory source. The old verifier failed every run containing
any provider account with `coverage_status == FAILED`; it had no provider
criticality or minimum-primary-group model. The three GDELT calls were one
attempt plus two bounded retries, while all six primary RSS feeds completed.
The corrected runtime records retry exhaustion as
`TEMPORARILY_UNAVAILABLE`, source coverage remains non-complete, and the
aggregate result is `PASS_DEGRADED` when the primary group and payload are
usable.

## Operational database classification

MAIN and WAL were byte- and metadata-identical before and after. SHM kept
the same size and SHA-256 and changed only its filesystem timestamp:

| Role | Bytes | SHA-256 | Timestamp result |
| --- | ---: | --- | --- |
| MAIN | 251,183,104 | `69514FAC4DC680BF5C0AC7FE278C76FDBB656625374A40201A3C10CA4669FA83` | unchanged |
| WAL | 14,634,272 | `1352209F2D9430EDE37920BAD44C97CE10BAF65E8333CE21B872896482AA4B72` | unchanged |
| SHM | 32,768 | `0D8D2615462920ABCE02FFB9B6B574B3FEC0FBC4280CCA1403885316148AD5FF` | timestamp only |

Targeted offline tracing on copied WAL-mode fixtures separated four
operations. Fingerprinting did not change content or metadata. Opening and
querying a copied SQLite bundle, and the former read-only SQLite backup,
could update SHM coordination state. The production verifier performed that
backup directly from the operational database using `mode=ro`, so it was
the only verifier operation capable of explaining the observed SHM-only
timestamp change.

The permanent verifier now copies the stable MAIN/WAL/SHM bundle without
opening the operational database, verifies source stability and copied
hashes, and opens only the sandbox later. It reports three independent
invariants:

- `content_unchanged`: roles, sizes and hashes match;
- `metadata_unchanged`: timestamps match;
- `semantic_database_unchanged`: database bytes match.

Any size or hash change is `FAIL`. An otherwise identical SHM timestamp-only
change is explicitly
`METADATA_ONLY_SHM_TIMESTAMP_CHANGE`; it is not reported as a logical
database mutation. A metadata-only change to MAIN or WAL remains fail
closed.

## Acceptance semantics

- `PASS`: network, accounting, response identity, required provider group
  and database invariants pass with no degraded coverage.
- `PASS_DEGRADED`: all hard invariants pass and the payload is usable, but
  an optional source is temporarily unavailable, coverage/enrichment is
  partial, or the proven SHM-only metadata condition is present.
- `FAIL`: network/accounting/content integrity fails, a non-complete source
  claims complete coverage, an unavailable source lacks a reason, or no
  required primary provider group remains usable.

Offline replay of the preserved evidence produces `PASS_DEGRADED`, with
163 response records, zero response identities outside persistence, and no
hard failure.

## CME contract check

CME is unchanged by this patch. The existing market-session implementation
uses a deterministic local weekly Globex rule, treats the official CME
calendar as an optional authoritative cross-check, and emits `PARTIAL`
without quarantine when the cross-check is unavailable. The offline
contract case at 05:40 ET retains MNQ `GLOBEX_OPEN`, a timeout cross-check
status and an empty quarantine set.
