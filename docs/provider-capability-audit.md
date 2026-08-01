# Provider Capability Audit

The Provider Capability Audit is a manual, isolated certification workflow. It is
not a scheduled health check and does not change the consumer route. Its unit of
certification is:

```text
provider + dataset + metric + field + frequency + transformation
```

The source of truth is `app.services.provider_capability_registry.PROVIDER_REGISTRY`.
Runtime construction, source policies, accounting, generated source matrices and
the audit must derive their provider identities from that registry. A configured
source without an isolated real probe is reported as `UNUSABLE` with
`ISOLATED_REAL_PROBE_NOT_IMPLEMENTED`; the audit never creates a synthetic
`HEALTHY` result.

## Running the audit

Run it only from the pushed PR branch:

```powershell
.\scripts\run-provider-capability-audit.ps1
```

The default includes every registered provider, capability, fallback and configured
AI provider. Optional filters are:

```powershell
.\scripts\run-provider-capability-audit.ps1 `
  -Provider FRED,BLS `
  -Dataset macro `
  -Metric headline_pce_yoy `
  -IncludeAI:$false
```

The root `run-provider-capability-audit.bat` forwards its arguments to the permanent
PowerShell runner. The runner is compatible with Windows PowerShell 5.1, enforces
the existing branch and pushed-HEAD invariants, bounds the worst-case full run at
14,400 seconds, captures stdout and stderr, and terminates every observed child
process. It performs no trading, delivery, order or manual SQL operation.

## Isolation and calls

Before importing an adapter, the Python entry point copies the configured SQLite
main/WAL/SHM bundle byte-for-byte into a temporary sandbox. It hashes the operational
bundle before the copy and again before audit completion. Adapters, caches,
repositories and AI workspaces receive only paths below the sandbox. A change to the
operational bundle fails the audit and leaves the previous latest pointer intact.
The audit never opens the operational database through a write-capable connection.

Each acquisition invokes only the selected registered adapter. Composite adapters
whose public `fetch` method automatically selects another source are fail-closed
until they expose a source-specific probe. A configured unsupported dispatch makes
the audit itself `FAILED`; it cannot be published as a completed full audit.
Capability-level `probe_adapter_path` values in the central registry select real
sub-adapters (for example earnings, release calendars and individual CBOE sources)
without an audit-side provider map. Repository and transformation hooks invoke the
registered real implementation against a private database snapshot or a documented
audit-only fixture. An explicit registry `request_group` may
deduplicate one identical acquisition that covers several capability rows. Without
that declaration, dataset/metric targets remain isolated. Each capability and field
still receives its own terminal result.

HTTPX calls made by a real adapter are captured inside the dedicated, single-purpose
audit process. The audit saves exact response bytes, response headers, status,
latency and attempt number. Attempts are bounded per request fingerprint; the audit
does not repeatedly probe a service to infer a rate limit. Request URLs and headers
are sanitized. If exact response bytes contain configured or recognizable secrets,
the persisted response is redacted and its metadata records both the in-memory
original hash and the persisted hash with `exact_bytes_saved=false`.
Adapters with a known multi-leaf acquisition declare
`audit_leaf_request_count` in the central registry. The resulting acquisition
budget is distinct from the per-fingerprint retry bound and replaces audit-side
provider allow-lists.

Each report acquisition is correlated to the run and its atomic capability rows by
`run_id`, unique `acquisition_id`, SHA-256 `request_key`, `provider_id` and the
explicit `target_ids`. It also records probe IDs and adapter paths, configured
state, bounded attempt count, dispatch status, real-adapter observation, network
exchange count, response hashes, reason codes and one `checked_at`. Every capability
result must join to exactly one acquisition with the same acquisition ID, request
key and observation time; the acquisition target IDs partition the complete result
set.

Runtime-adapter coverage is independently path-bound. If one logical provider
constructs multiple registered adapter classes, the audit creates a separate
coverage acquisition for every adapter path not already exercised by a capability
request. A provider ID alone cannot satisfy this gate. Each configured adapter must
show a real dispatch from that exact path; an unconfigured adapter must have its own
terminal `NOT_CONFIGURED` acquisition. Missing, substituted or unbound adapter
evidence makes `runtime_adapter_coverage.complete=false` and the audit `FAILED`.
Composite reconciliation adapters receive a local construction probe while their
atomic capability continues through the registered isolation hook; their external
fallback cascade is never executed as a substitute for an atomic source probe.

Lineage is field-specific and value-bound: a source label or a lineage row containing
only the field name is insufficient. A positive lineage gate requires the exact
value (or its canonical SHA-256), publisher, distributor and acquisition provider.
HTTPX lineage must bind its source URL and content hash to a captured exchange;
LOCAL_SANDBOX lineage must bind a concrete record locator; subprocess lineage must
bind an attested acquired source. The accepting validator recomputes those bindings
from the checksummed normalized response and acquisition artifacts.

Repository, deterministic provider and AI registrations use the same terminal-row
contract. AI knowledge or a model-declared URL is not proof: missing occurrence,
reference period, semantic mapping, freshness or field-level lineage remains
`UNKNOWN`/`UNUSABLE`, and that exact AI capability is ineligible.

## Quality score

The score is deterministic and totals 100. A component earns its full weight only
when the corresponding observed check is `true`; `false` or missing evidence earns
zero.

| Component | Weight |
|---|---:|
| Transport, HTTP and authentication | 10 |
| Schema | 15 |
| Required-field completeness | 10 |
| Freshness and lifecycle | 15 |
| Metric, unit, frequency and transformation semantics | 20 |
| Occurrence, reference period and release match | 15 |
| Field-level lineage | 15 |

`HEALTHY` requires a score of at least 85 and every correctness gate to be true.
`DEGRADED` requires a score of at least 60 and every correctness gate to be true.
An explicitly failed correctness gate is `UNUSABLE`. Missing mandatory evidence is
`UNKNOWN`, not inferred. Transport classification also exposes `DOWN`,
`AUTH_FAILED`, `RATE_LIMITED` and `NOT_CONFIGURED`.

A provider may be eligible as primary only when `HEALTHY`, its registration allows
that role, and all correctness gates pass. `DEGRADED` may be eligible only as a
fallback when the exact capability remains correct.

The persisted field observations are the inputs; scores and classifications are
not trusted assertions. Acceptance recomputes every score component, field health
and derived reason, aggregate checks, aggregate score/health, role eligibility and
recommendation with `validate_capability_result_derivations`. Empty checks paired
with a claimed `HEALTHY`/100 result, or any other inconsistent derivation, are
rejected.

For weekly, monthly, quarterly, annual and event releases, an age inside the SLA
and a `CURRENT` label do not prove freshness. A positive result requires explicit
lifecycle verification plus a match between the observed and expected occurrence
or reference period. A known mismatch is false; absent proof is unknown. Explicit
latest-release proof may keep an older monthly or quarterly reference period valid,
but it never overrides an expired `content_valid_until`, a due `refresh_due_at`, or
a stale/expired lifecycle observation. AI occurrence probes receive an immutable
request-key-derived occurrence ID and must return that exact correlation; a
same-period response from another occurrence cannot certify itself.

## Completion versus system health

`audit_status` describes whether the audit itself accounted for every selected
capability, verified the registry, respected database isolation and wrote verified
artifacts. `system_health` describes the sources found by a completed audit. It is
therefore valid to obtain:

```json
{
  "audit_status": "COMPLETED",
  "system_health": "DEGRADED"
}
```

`NOT_CONFIGURED` and observed provider failures are terminal capability results and
do not by themselves fail audit execution. A configured source for which the audit
cannot observe a real adapter dispatch is an audit implementation gap and therefore
makes the audit `FAILED`. Missing/duplicate rows, registry failure, incomplete
fallback-chain accounting, unexpected internal failures, artifact checksum failure
or an operational database change also make the audit `FAILED`.

The unfiltered audit accounts for every one of the 25 dataset policies in configured
DB-first order: canonical repository, primary, deterministic fallbacks, then
capability-certified AI. Every source in every chain must have at least one terminal
atomic capability result for the exact dataset; wildcard capability declarations
cannot satisfy coverage.
The report records the matching capability IDs and eligibility decision under
`fallback_chain_accounting`; it never generates artificial provider failures during
the LIVE audit. Accounting is recomputed at exact delivered metric + field
granularity from the central registry and the terminal probe rows. A result for a
different metric in the same dataset cannot fill a missing chain row, and every
declared field must have its own terminal result. Differently named raw series are
related only through explicit `delivery_capability_id`,
`delivery_capability_ids` and `delivery_field_map` declarations or a canonical
metric relation in the registry. Each DB, primary, fallback and AI step has a
unique phase identity, so a repository and primary with the same provider ID
cannot overwrite one another. The independent acceptance validator recomputes
the same structure and rejects a modified or coarser manifest.

Policies declare whether their provider list is a true ordered `FALLBACK`, an
acquisition `CASCADE`, or a complementary `FAN_IN`. In a `FAN_IN` dataset, one
successful capability group does not skip unrelated groups; only providers sharing
the same registered atomic capability form a fallback chain. Filtered re-probes mark
the full-chain gate as not applicable and cannot publish the canonical latest
pointer.

## Artifacts and publication

Every finalized run is stored under:

```text
data/provider-capability-audit/<RUN_ID>/
    audit-report.json
    audit-report.md
    capability-matrix.json
    provider-responses/
    ai-evidence/
    logs/
    checksums.json
    comparison-with-previous.json
```

Files are first written under a same-volume `.inprogress-*` directory. JSON is
UTF-8, canonicalized for hashes, redacted and atomically replaced. `checksums.json`
contains the relative path, byte size and SHA-256 for every other artifact; the
latest pointer records the checksum file's own identity.

Only a `COMPLETED` audit with exactly one terminal row per selected capability may
prepare a same-directory candidate pointer. Before publication, the independent
acceptance verifier re-reads the candidate and checks the complete checksum tree,
registry digest, capability and field derivations, acquisition bindings, capture
attestations, fallback accounting and summary counts. Only an accepted, unchanged
candidate may atomically replace:

```text
data/provider-capability-audit-latest.json
```

A completed Python audit never publishes this candidate by itself. The Windows
runner first observes the audit process exit, terminates and rechecks every
request-scoped child process, validates the completed report, and rechecks the
pushed branch from the repository path. It then invokes the isolated strong
publication mode as its final state-changing step. A cleanup or pre-publication
failure therefore leaves the previous latest pointer byte-for-byte unchanged.

The report, capability matrix and pointer carry the Git commit that produced the
run plus an exact SHA-256 over the runtime source tree under `app`, `config` and
`scripts`. Text line endings are normalized for clean-checkout reproducibility.
The only canonicalized value is the reviewed 64-hex LIVE-baseline pin itself;
changing provider code, prompts or source policy invalidates an older audit.
Before the pointer swap, identities for the candidate tree and any previous
accepted tree are rechecked to detect changes during acceptance.

`audit-report.md` must be the exact rendering of `audit-report.json`;
`comparison-with-previous.json` is recomputed from the current and previous
matrices; `logs/audit.jsonl` has a canonical run-bound sequence; and each
`ai-evidence` file is bound to its acquisition, targets, request key and field
evidence. Recalculating only the checksum manifest cannot make contradictory
semantic artifacts acceptable.

A run must also have full scope: no provider/dataset/metric filters, AI included,
and tested provider/capability counts equal to the registry counts. Filtered
re-probes retain their complete run directory but never create a publication
candidate. A failed or rejected run deletes its candidate and cannot overwrite the
previous pointer. The final publication operation performs the independent strong
verification. Before comparing with a previous run,
the writer verifies pointer containment, file size and SHA-256. Recommendations are non-mutating
and include `KEEP_PRIMARY`, `KEEP_FALLBACK`, `DEMOTE_TO_FALLBACK`,
`DISABLE_FOR_METRIC`, `REQUIRES_FIX` and `SOURCE_GAP`. One transient failure never
removes a provider automatically.

A verified full-audit pointer can be imported explicitly into the compact,
versioned matrix baseline:

```powershell
python scripts/generate_provider_capability_matrix.py `
  --import-live-pointer data/provider-capability-audit-latest.json
```

The compact baseline preserves distinct `field_results` for every
provider + dataset + metric + field, including health, eligibility and
recommended role. The generated matrices consume that file when present;
they never assign one aggregate capability status to all fields. Its exact UTF-8
bytes must also match the SHA-256 trust anchor committed in the registry; internal
hashes cannot self-attest a rewritten baseline.

## Offline verification

Tests inject controlled probe outcomes and transports. They cover every terminal
status, auditable scoring, explicit deduplication, fallback ordering, AI
certification failure, exact response/checksum publication, secret redaction,
database isolation, failed-run pointer protection and a real Windows PowerShell 5.1
parser/process smoke that performs no external request.
