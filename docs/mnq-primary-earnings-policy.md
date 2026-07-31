# MNQ primary earnings policy

`MNQ_PRIMARY_EARNINGS_V1` is the permanent selection contract for earnings
delivered to the Senior Analyst MNQ consumer. Its canonical executable
definition is `MNQ_EARNINGS_SELECTION_POLICY` in
`app/core/senior_analyst_policy.py`; provider adapters and runtime context
builders must derive from that object rather than maintain independent
watchlists.

## Universe and selection

| Property | Contract |
| --- | --- |
| Primary symbols | `AAPL`, `NVDA`, `AMZN`, `META`, `TSLA`, `AMD` |
| Nasdaq-100 expansion | Disabled; the full index is not an implicit earnings universe |
| Window | One calendar day before through 14 calendar days after the request date |
| Ordering | Ascending `event_date`, then ascending `symbol` |
| Maximum delivered events | 24 |
| Minimum fields | `symbol`, `event_date`, `temporal_precision`, `timing`, `data_as_of`, `content_valid_until`, `refresh_due_at`, `freshness`, `source` |

The maximum is a deterministic bound, not a silent truncation rule. The
consumer reports `total_available`, `relevant_count`, `delivered_count`, and
`excluded_count`; a bounded omission degrades the section and is disclosed by
its reason code. This earnings universe is intentionally independent from the
larger mega-cap quote universe, which remains unchanged.

## Time semantics

An occurrence for which only a date is evidenced is represented with
`temporal_precision=DATE_ONLY`, `event_at=null`, and `timing=UNKNOWN`. Midnight
UTC must not be synthesized as an event time. A session label may be retained
when the source supplies one, but it is not an exact timestamp. `EXACT` is
allowed only when occurrence-specific evidence contains a real event time.

## Coverage and readiness

A non-empty list is not sufficient for `AVAILABLE`. Readiness is derived from
the delivered events and evaluates all of the following:

- membership in the six-symbol primary universe and occurrence date validity;
- lifecycle freshness and source quality;
- timing coverage;
- EPS-estimate coverage;
- revenue-estimate coverage;
- the number of relevant and delivered occurrences.

A date-only collection with no timing or estimates is therefore `DEGRADED`,
not automatically `AVAILABLE`. Missing, expired, outside-window, or
outside-universe records cannot improve coverage or readiness.

## Request accounting and payload budget

The request-scoped accounting row for earnings refers to the delivered
collection instead of duplicating it:

```json
{
  "payload_path": "analytics.earnings.events",
  "item_count": 1,
  "content_sha256": null
}
```

At validation time `payload_path` is resolved against the exact response body,
then `item_count` and the canonical collection SHA-256 are recomputed and
compared. The consumer body must not embed the Provider Capability Audit or a
second copy of the events.

The hard limit is `MAX_SENIOR_ANALYST_PAYLOAD_BYTES = 250000` bytes, measured
on the exact HTTP body. Exceeding it is a gate failure, regardless of semantic
readiness.
