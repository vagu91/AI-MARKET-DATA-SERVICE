# PR 29 — Historical news-pipeline regression audit

## Method and evidence boundary

This audit was performed without network access using repository history:
`git log`, `git blame`, `git log -S`, `git log -G`, and historical file
content from `git show`. It distinguishes a defect present from the initial
implementation from a later behavioral regression.

The V7 evidence and the exact 172-record reconciliation remain documented in
`docs/forensics/pr29-v7-news-loss-reconciliation.md`. In particular, the 112
non-persisted records were 92 post-fetch limit exclusions, 10 legitimate
recency exclusions, and 10 false MarketWatch lineage rejections.

## Findings

| Problem | Origin commit and historical location | Classification | Historical proof | Missing regression gate |
| --- | --- | --- | --- | --- |
| BLS endpoint was `https://www.bls.gov/feed/news_release.rss` | `03f59830519d2e3c6216df05388ffe7e311ef107`, `app/core/config.py:259` in that commit | Latent defect | The wrong value entered in the initial implementation. `git log -S` finds no committed occurrence of the official `https://www.bls.gov/feed/bls_latest.rss` before this correction. The endpoint was therefore never correct in repository history. | A versioned official-endpoint baseline bound to `Settings.model_fields["bls_rss_url"].default`, plus a parseable BLS golden document. |
| BEA endpoint was `https://www.bea.gov/news/rss.xml` | `03f59830519d2e3c6216df05388ffe7e311ef107`, `app/core/config.py:260` in that commit | Latent defect | The wrong value entered in the initial implementation. `git log -S` finds no committed occurrence of the official `https://apps.bea.gov/rss/rss.xml` before this correction. The endpoint was therefore never correct in repository history. | A versioned official-endpoint baseline bound to `Settings.model_fields["bea_rss_url"].default`, plus a parseable BEA golden document. |
| Records already received were truncated by `limit` | `03f59830519d2e3c6216df05388ffe7e311ef107`, `app/providers/news_provider.py:144,219,258,296,344`; made an explicit accounted exclusion by `95175aba9433784326f5c37b6f6751c865e28309`, then `app/providers/news_provider.py:812-816,929-933,1058-1062` | Latent defect in the initial implementation; losslessness regression in PR29 commit `95175aba…` | Initial parsers sliced RSS, GDELT and Alpha payloads and stopped fallback accumulation at `limit`. Commit `95175aba…` retained the destructive behavior under reason `PER_PROVIDER_LIMIT`, so it accounted for loss but did not prevent it. Commit `608ce66cac6cd31ec6d5889dd58eb0ea328e41d6` removed the branches. | Feed/API fixtures with N+1 received records and configured N, specifically 11/10, 26/25 and 101/100, must prove every received identity survives. An AST/static gate must reject limit-dependent slicing and `PER_PROVIDER_LIMIT`. |
| The observed cap was 25 | V7 validation configuration, not a production default commit | Test-triggered manifestation of the generic cap | No production commit introduced a post-fetch default of 25. The V7 controlled request used `limit=25`, which activated the generic branches introduced initially and retained by `95175aba…`. It would be incorrect to attribute the number 25 to a separate production-code commit. | Tests must exercise multiple boundary values rather than only the configured default, so a generic post-fetch selector cannot hide behind a different number. |
| Acquisition label `MarketWatch RSS` was promoted to publisher | The label existed from `03f59830519d2e3c6216df05388ffe7e311ef107`, `app/providers/news_provider.py:184`. The harmful promotion was introduced by `95175aba9433784326f5c37b6f6751c865e28309`, then `app/providers/news_provider.py:1106,1129`. | Regression | There is no earlier commit in which this feed label was plain `MarketWatch`; `git log -S'MarketWatch RSS'` traces it to the initial commit. The initial fallback used the acquisition label when `<source>` was absent. Commit `a17f5f5bb6f0212654670b0a8ed22a515462396c` added author fallback but retained that label. Commit `95175aba…` assigned the fallback to `original_publisher`. Combined with the contradiction policy introduced by `30525e9cda4228ffbc749bb67dbc0ed12688ce8b` and refined by `45ff1ab489da392360b20515603862065dd22b0e`, this falsely rejected direct-host MarketWatch records. Commit `608ce66…` separated publisher fallback from acquisition provider. | A direct MarketWatch fixture with no `<source>` or author must assert `original_publisher=MarketWatch`, `acquisition_provider=MarketWatch RSS`, no distributor, and no contradictory lineage. |

## Why the historical tests did not stop the changes

The initial tests in `tests/test_nasdaq_providers.py` used one-item RSS
documents and override endpoints such as `https://news.test/rss`. They tested
fallback availability and individual parser fields, not the configured
official endpoints or documents larger than the requested limit.

The fan-in tests added by `95175aba…` explicitly asserted
`per_provider_limit`, but their documents remained at or below that limit.
Their RSS helper always emitted
`<source>{provider} Publisher</source>`, so the missing-source fallback that
turned `MarketWatch RSS` into a publisher was never executed.

Accounting tests at that point accepted `PER_PROVIDER_LIMIT` as a concrete
outside-scope reason. That made the accounting internally closed while still
allowing a provider-returned record to be discarded. The permanent baseline
now forbids that classification: request/page-size limits may bound
acquisition, but may not reclassify a record after it has been received.

## Permanent closure

The machine-readable contract is
`docs/baselines/news-pipeline-v1.json`. Its checksum is bound in
`tests/test_news_pipeline_baseline.py`, so a future endpoint or semantic
contract change requires an explicit baseline and test update.

The permanent entrypoint is `scripts/verify-news-pipeline.ps1`; it has only
`Offline` and `ControlledLive` modes. Files named as disposable Stage2
versions are prohibited under `scripts/` and `tests/`.
