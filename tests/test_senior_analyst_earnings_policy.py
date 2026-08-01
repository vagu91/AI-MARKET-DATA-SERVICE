from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from app.core.senior_analyst_policy import (
    MAX_SENIOR_ANALYST_PAYLOAD_BYTES,
    MNQ_EARNINGS_SELECTION_POLICY,
)
from app.services.senior_analyst_projection_v1 import (
    build_senior_analyst_payload_v1,
    validate_senior_analyst_payload_v1,
)
from scripts.validate_senior_analyst_payload import (
    _content_length_matches_exact_body,
)


NOW = datetime(2026, 7, 31, 6, 2, 22, tzinfo=UTC)


def _source(
    events: list[dict],
    *,
    selection_counts: dict | None = None,
) -> dict:
    nasdaq_earnings = {"upcoming": events}
    if selection_counts is not None:
        nasdaq_earnings["selection_counts"] = selection_counts
    return {
        "symbol": "MNQ",
        "generated_at": NOW.isoformat(),
        "sections": {
            "earnings": {
                "nasdaq_earnings": nasdaq_earnings,
            }
        },
    }


def _event(
    symbol: str,
    *,
    day_offset: int = 1,
    exact: bool = False,
    eps_estimate: float | None = None,
    revenue_estimate: float | None = None,
) -> dict:
    event_date = (NOW.date() + timedelta(days=day_offset)).isoformat()
    value = {
        "symbol": symbol,
        "event_date": event_date,
        "data_as_of": NOW.isoformat(),
        "content_valid_until": (NOW + timedelta(days=2)).isoformat(),
        "refresh_due_at": (NOW + timedelta(days=2)).isoformat(),
        "source": "Nasdaq Earnings Calendar",
        "publisher": "NASDAQ",
        "distributor": "NASDAQ",
        "acquisition_provider": "NASDAQ",
        "source_url": (
            "https://www.nasdaq.com/market-activity/earnings"
        ),
        "eps_estimate": eps_estimate,
        "revenue_estimate": revenue_estimate,
    }
    if exact:
        value.update(
            {
                "event_at": f"{event_date}T20:05:00+00:00",
                "timing": "AFTER_CLOSE",
            }
        )
    lineage_fields = ["event_date"]
    if exact:
        lineage_fields.append("timing")
    if eps_estimate is not None:
        lineage_fields.append("eps_estimate")
    if revenue_estimate is not None:
        lineage_fields.append("revenue_estimate")
    value["lineage"] = [
        {
            "field": field,
            "source": "Nasdaq Earnings Calendar",
            "publisher": "NASDAQ",
            "distributor": "NASDAQ",
            "acquisition_provider": "NASDAQ",
            "source_url": (
                "https://www.nasdaq.com/market-activity/earnings"
            ),
        }
        for field in lineage_fields
    ]
    return value


def _earnings(
    events: list[dict],
    *,
    selection_counts: dict | None = None,
) -> tuple[dict, dict]:
    payload = build_senior_analyst_payload_v1(
        _source(events, selection_counts=selection_counts),
        now=NOW,
        request_id="earnings-policy-test",
        request_refresh_mode="replay",
    )
    return payload, payload["analytics"]["earnings"]


def test_large_provider_calendar_is_filtered_before_delivery() -> None:
    provider_events = [
        _event(f"OTHER{index:04d}")
        for index in range(3_000)
    ]
    provider_events.append(_event("AMD"))

    payload, earnings = _earnings(provider_events)

    assert [item["symbol"] for item in earnings["events"]] == ["AMD"]
    assert earnings["total_available"] == 3_001
    assert earnings["relevant_count"] == 1
    assert earnings["delivered_count"] == 1
    assert earnings["excluded_count"] == 3_000
    assert earnings["exclusion_counts"] == {
        "outside_window": 0,
        "outside_universe": 3_000,
        "bounded_limit": 0,
        "provider_prefiltered_or_invalid": 0,
    }
    assert earnings["excluded_count"] == (
        earnings["total_available"] - earnings["delivered_count"]
    )
    assert sum(earnings["exclusion_counts"].values()) == earnings[
        "excluded_count"
    ]
    assert payload["payload_size_bytes"] < MAX_SENIOR_ANALYST_PAYLOAD_BYTES


def test_prefiltered_provider_calendar_keeps_observed_counts_compact() -> None:
    payload, earnings = _earnings(
        [_event("AMD")],
        selection_counts={
            "total_available": 3_001,
            "relevant_count": 1,
            "delivered_count": 1,
            "excluded_count": 3_000,
        },
    )

    assert [item["symbol"] for item in earnings["events"]] == ["AMD"]
    assert earnings["total_available"] == 3_001
    assert earnings["relevant_count"] == 1
    assert earnings["delivered_count"] == 1
    assert earnings["excluded_count"] == 3_000
    assert earnings["exclusion_counts"] == {
        "outside_window": 0,
        "outside_universe": 0,
        "bounded_limit": 0,
        "provider_prefiltered_or_invalid": 3_000,
    }
    assert payload["payload_size_bytes"] < MAX_SENIOR_ANALYST_PAYLOAD_BYTES
    assert validate_senior_analyst_payload_v1(
        payload,
        now=NOW,
    )["status"] == "PASS_OFFLINE"


def test_date_only_event_is_not_promoted_to_exact_or_fabricated_time() -> None:
    payload, earnings = _earnings([_event("AMD")])

    event = earnings["events"][0]
    assert event["event_date"] == "2026-08-01"
    assert event["event_at"] is None
    assert event["temporal_precision"] == "DATE_ONLY"
    assert event["timing"] == "UNKNOWN"
    assert event["reason_code"] == "EARNINGS_RELEASE_TIME_NOT_AVAILABLE"
    assert earnings["status"] == "DEGRADED"
    assert earnings["coverage"]["timing"]["count"] == 0
    assert earnings["coverage"]["eps_estimate"]["count"] == 0
    assert earnings["coverage"]["revenue_estimate"]["count"] == 0
    assert payload["readiness"]["section_status"]["earnings"] == "DEGRADED"


def test_exact_well_sourced_event_can_be_available() -> None:
    payload, earnings = _earnings(
        [
            _event(
                "AMD",
                exact=True,
                eps_estimate=1.23,
                revenue_estimate=8_500_000_000,
            )
        ]
    )

    event = earnings["events"][0]
    assert event["event_at"] == "2026-08-01T20:05:00+00:00"
    assert event["temporal_precision"] == "EXACT"
    assert event["timing"] == "AFTER_CLOSE"
    assert event["source"]["publisher"] == "NASDAQ"
    assert event["source"]["distributor"] == "NASDAQ"
    assert event["source"]["source_url"] == (
        "https://www.nasdaq.com/market-activity/earnings"
    )
    assert earnings["status"] == "AVAILABLE"
    assert earnings["reason_code"] is None
    assert all(
        item["ratio"] == 1.0
        for item in earnings["coverage"].values()
    )
    assert payload["readiness"]["section_status"]["earnings"] == "AVAILABLE"


def test_unverified_exact_duplicate_cannot_displace_verified_date_only() -> None:
    verified = _event(
        "AMD",
        eps_estimate=1.23,
        revenue_estimate=8_500_000_000,
    )
    unverified_exact = deepcopy(verified)
    unverified_exact.update(
        {
            "event_at": "2026-08-01T20:05:00+00:00",
            "timing": "AFTER_CLOSE",
            "source": "Unverified Earnings Feed",
            "publisher": "Unverified",
            "distributor": "Unverified",
            "source_url": "https://evil.example/earnings",
            "lineage": [],
            "eps_estimate": None,
            "revenue_estimate": None,
        }
    )

    _, earnings = _earnings(
        [verified, unverified_exact]
    )

    delivered = earnings["events"][0]
    assert delivered["temporal_precision"] == "DATE_ONLY"
    assert delivered["event_at"] is None
    assert delivered["timing"] == "UNKNOWN"
    assert delivered["eps_estimate"] == 1.23
    assert delivered["revenue_estimate"] == 8_500_000_000
    assert delivered["source"]["publisher"] == "NASDAQ"
    assert earnings["candidate_diagnostics"][
        "duplicate_occurrence"
    ] == 1


def test_selection_is_deterministic_and_bounded_with_disclosed_omissions() -> None:
    policy = MNQ_EARNINGS_SELECTION_POLICY
    provider_events = [
        _event(
            symbol,
            day_offset=day_offset,
            exact=True,
            eps_estimate=float(day_offset),
            revenue_estimate=float(day_offset * 1_000_000),
        )
        for day_offset in range(0, 6)
        for symbol in reversed(policy.primary_symbols)
    ]

    _, earnings = _earnings(list(reversed(provider_events)))

    delivered_identities = [
        (item["event_date"], item["symbol"])
        for item in earnings["events"]
    ]
    assert delivered_identities == sorted(delivered_identities)
    assert len(delivered_identities) == policy.max_events
    assert earnings["total_available"] == 36
    assert earnings["relevant_count"] == 36
    assert earnings["delivered_count"] == policy.max_events
    assert earnings["excluded_count"] == 12
    assert earnings["exclusion_counts"]["bounded_limit"] == 12
    assert earnings["status"] == "DEGRADED"
    assert earnings["reason_code"] == "EARNINGS_SELECTION_BOUNDED"


def test_stale_event_is_excluded_even_when_retrieved_now() -> None:
    stale = _event("AMD")
    stale.update(
        {
            "retrieved_at": NOW.isoformat(),
            "content_valid_until": (NOW - timedelta(seconds=1)).isoformat(),
            "freshness": "CURRENT",
        }
    )

    _, earnings = _earnings([stale])

    assert earnings["events"] == []
    assert earnings["total_available"] == 0
    assert (
        earnings["candidate_diagnostics"]["expired_or_invalid_freshness"]
        == 1
    )
    assert earnings["candidate_diagnostics"][
        "freshness_rejection_reasons"
    ] == {"EARNINGS_CONTENT_EXPIRED": 1}


@pytest.mark.parametrize(
    ("missing_field", "expected_reason"),
    [
        (
            "data_as_of",
            "EARNINGS_OBSERVATION_TIME_NOT_AVAILABLE",
        ),
        (
            "content_valid_until",
            "EARNINGS_VALIDITY_DEADLINE_NOT_AVAILABLE",
        ),
        (
            "refresh_due_at",
            "EARNINGS_REFRESH_DUE_NOT_AVAILABLE",
        ),
    ],
)
def test_event_without_complete_freshness_evidence_is_excluded(
    missing_field: str,
    expected_reason: str,
) -> None:
    event = _event("AMD")
    event.pop(missing_field)

    _, earnings = _earnings([event])

    assert earnings["events"] == []
    assert earnings["total_available"] == 0
    assert earnings["candidate_diagnostics"][
        "freshness_rejection_reasons"
    ] == {expected_reason: 1}


def test_date_field_with_midnight_timestamp_remains_date_only() -> None:
    event = _event("AMD")
    event.pop("event_date")
    event["earnings_date"] = "2026-08-01T00:00:00Z"

    _, earnings = _earnings([event])

    delivered = earnings["events"][0]
    assert delivered["event_date"] == "2026-08-01"
    assert delivered["event_at"] is None
    assert delivered["temporal_precision"] == "DATE_ONLY"
    assert delivered["timing"] == "UNKNOWN"


def test_explicit_date_only_midnight_event_at_is_never_promoted() -> None:
    event = _event("AMD")
    event.pop("event_date")
    event["event_at"] = "2026-08-01T00:00:00+00:00"
    event["temporal_precision"] = "DATE_ONLY"

    _, earnings = _earnings([event])

    delivered = earnings["events"][0]
    assert delivered["event_date"] == "2026-08-01"
    assert delivered["event_at"] is None
    assert delivered["temporal_precision"] == "DATE_ONLY"
    assert delivered["timing"] == "UNKNOWN"
    assert (
        delivered["reason_code"]
        == "EARNINGS_RELEASE_TIME_NOT_AVAILABLE"
    )


def test_unqualified_midnight_event_at_is_not_exact_time_evidence() -> None:
    event = _event("AMD")
    event.pop("event_date")
    event["event_at"] = "2026-08-01T00:00:00+00:00"

    _, earnings = _earnings([event])

    delivered = earnings["events"][0]
    assert delivered["event_date"] == "2026-08-01"
    assert delivered["event_at"] is None
    assert delivered["temporal_precision"] == "DATE_ONLY"
    assert delivered["timing"] == "UNKNOWN"


def test_naive_event_at_is_not_promoted_to_exact_timestamp() -> None:
    event = _event("AMD")
    event["event_at"] = "2026-08-01T20:05:00"

    _, earnings = _earnings([event])

    delivered = earnings["events"][0]
    assert delivered["event_at"] is None
    assert delivered["temporal_precision"] == "DATE_ONLY"
    assert delivered["timing"] == "UNKNOWN"


def test_refresh_due_event_is_excluded_even_before_content_expiry() -> None:
    event = _event("AMD")
    event["content_valid_until"] = (
        NOW + timedelta(days=2)
    ).isoformat()
    event["refresh_due_at"] = (
        NOW - timedelta(seconds=1)
    ).isoformat()

    _, earnings = _earnings([event])

    assert earnings["events"] == []
    assert earnings["candidate_diagnostics"][
        "freshness_rejection_reasons"
    ] == {"EARNINGS_REFRESH_DUE": 1}


@pytest.mark.parametrize(
    ("lifecycle", "reason_code"),
    [
        (
            {
                "valid_until": (
                    NOW - timedelta(seconds=1)
                ).isoformat(),
            },
            "EARNINGS_CONTENT_EXPIRED",
        ),
        (
            {
                "next_refresh_at": (
                    NOW - timedelta(seconds=1)
                ).isoformat(),
            },
            "EARNINGS_REFRESH_DUE",
        ),
    ],
)
def test_contradictory_earnings_deadline_aliases_fail_closed(
    lifecycle: dict,
    reason_code: str,
) -> None:
    event = _event("AMD")
    event["lifecycle"] = lifecycle

    _, earnings = _earnings([event])

    assert earnings["events"] == []
    assert earnings["candidate_diagnostics"][
        "freshness_rejection_reasons"
    ] == {reason_code: 1}


@pytest.mark.parametrize(
    "lifecycle",
    [
        {"status": "EXPIRED"},
        {"freshness": "STALE"},
        {"lifecycle_status": "SUPERSEDED"},
        {"currently_valid": False},
        {"superseded_by": "newer-observation"},
    ],
)
def test_nested_invalid_earnings_lifecycle_is_excluded(
    lifecycle: dict,
) -> None:
    event = _event("AMD")
    event["lifecycle"] = lifecycle

    _, earnings = _earnings([event])

    assert earnings["events"] == []
    assert earnings["candidate_diagnostics"][
        "freshness_rejection_reasons"
    ] == {"EARNINGS_FRESHNESS_STATE_INVALID": 1}


def test_unverified_earnings_source_cannot_make_readiness_available() -> None:
    event = _event(
        "AMD",
        exact=True,
        eps_estimate=1.23,
        revenue_estimate=8_500_000_000,
    )
    event.update(
        {
            "source": "UNVERIFIED LABEL",
            "publisher": "UNVERIFIED LABEL",
            "source_url": "https://unverified.example.test/earnings",
        }
    )
    event.pop("acquisition_provider")
    event.pop("lineage")

    payload, earnings = _earnings([event])

    assert earnings["events"][0]["symbol"] == "AMD"
    assert earnings["coverage"]["source_quality"]["count"] == 0
    assert earnings["status"] == "DEGRADED"
    assert earnings["reason_code"] == "EARNINGS_COVERAGE_INCOMPLETE"
    assert payload["readiness"]["section_status"]["earnings"] == (
        "DEGRADED"
    )


def test_registered_provider_labels_cannot_launder_an_unbound_hostname() -> None:
    event = _event(
        "AMD",
        exact=True,
        eps_estimate=1.23,
        revenue_estimate=8_500_000_000,
    )
    event["source_url"] = "https://attacker.example/earnings"
    for evidence in event["lineage"]:
        evidence["source_url"] = "https://attacker.example/earnings"

    payload, earnings = _earnings([event])

    assert earnings["coverage"]["source_quality"]["count"] == 0
    assert earnings["status"] == "DEGRADED"
    assert earnings["reason_code"] == "EARNINGS_COVERAGE_INCOMPLETE"
    assert payload["readiness"]["section_status"]["earnings"] == (
        "DEGRADED"
    )


def test_validator_rejects_earnings_policy_and_temporal_tampering() -> None:
    payload, _ = _earnings(
        [
            _event(
                "AMD",
                exact=True,
                eps_estimate=1.23,
                revenue_estimate=8_500_000_000,
            )
        ]
    )
    baseline = validate_senior_analyst_payload_v1(payload, now=NOW)
    assert baseline["checks"]["semantic_mapping_errors"] == 0
    assert baseline["checks"]["invalid_temporal_mappings"] == 0

    outside_universe = deepcopy(payload)
    outside_universe["analytics"]["earnings"]["events"][0][
        "symbol"
    ] = "UNRELATED"
    assert (
        validate_senior_analyst_payload_v1(
            outside_universe,
            now=NOW,
        )["checks"]["semantic_mapping_errors"]
        > 0
    )

    fabricated_time = deepcopy(payload)
    event = fabricated_time["analytics"]["earnings"]["events"][0]
    event["temporal_precision"] = "DATE_ONLY"
    event["timing"] = "UNKNOWN"
    assert (
        validate_senior_analyst_payload_v1(
            fabricated_time,
            now=NOW,
        )["checks"]["invalid_temporal_mappings"]
        > 0
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda event: (
            event.pop("data_as_of"),
            event.update({"retrieved_at": NOW.isoformat()}),
        ),
        lambda event: event.pop("content_valid_until"),
        lambda event: event.pop("refresh_due_at"),
        lambda event: event.update(
            {
                "data_as_of": "1999-01-01T00:00:00+00:00",
                "content_valid_until": (
                    NOW + timedelta(days=2)
                ).isoformat(),
                "refresh_due_at": (
                    NOW + timedelta(days=2)
                ).isoformat(),
                "freshness": "CURRENT",
            }
        ),
        lambda event: event.update(
            {
                "refresh_due_at": (
                    NOW - timedelta(seconds=1)
                ).isoformat(),
                "content_valid_until": (
                    NOW + timedelta(days=2)
                ).isoformat(),
            }
        ),
        lambda event: (
            event["source"].update(
                {
                    "acquisition_provider": "UNREGISTERED",
                    "source_url": (
                        "https://unverified.example.test/earnings"
                    ),
                }
            ),
            event.update({"lineage": []}),
        ),
    ],
)
def test_validator_rejects_tampered_earnings_lifecycle_even_if_retrieval_is_recent(
    mutate,
) -> None:
    payload, _ = _earnings(
        [
            _event(
                "AMD",
                exact=True,
                eps_estimate=1.23,
                revenue_estimate=8_500_000_000,
            )
        ]
    )
    event = payload["analytics"]["earnings"]["events"][0]
    mutate(event)

    result = validate_senior_analyst_payload_v1(payload, now=NOW)

    assert result["checks"]["semantic_mapping_errors"] > 0
    assert result["status"] == "FAIL"


@pytest.mark.parametrize(
    ("exact_size", "expected"),
    [
        (MAX_SENIOR_ANALYST_PAYLOAD_BYTES, True),
        (MAX_SENIOR_ANALYST_PAYLOAD_BYTES + 1, False),
    ],
)
def test_validator_uses_exact_http_body_size_for_budget(
    exact_size: int,
    expected: bool,
) -> None:
    payload, _ = _earnings([])

    result = validate_senior_analyst_payload_v1(
        payload,
        now=NOW,
        exact_body_size_bytes=exact_size,
    )

    assert result["checks"]["payload_size_within_budget"] is expected
    assert result["measured_payload_size_bytes"] == exact_size
    if expected is False:
        assert result["status"] == "FAIL"


def test_validator_rejects_large_inline_accounting_collection() -> None:
    payload, _ = _earnings([])
    payload["provider_accounting"] = [
        {
            "dataset_id": "earnings",
            "delivered_value": [
                {"text": "x" * 17_000},
            ],
        }
    ]

    result = validate_senior_analyst_payload_v1(payload, now=NOW)

    assert result["checks"]["duplicate_large_collections"] == 1
    assert result["status"] == "FAIL"


@pytest.mark.parametrize(
    ("declared", "actual", "expected"),
    [
        ("250000", 250_000, True),
        (["250001"], 250_000, False),
        ("invalid", 250_000, False),
    ],
)
def test_content_length_must_match_the_exact_http_body(
    declared: object,
    actual: int,
    expected: bool,
) -> None:
    headers = {
        "content_headers": {
            "content-length": declared,
        }
    }

    assert (
        _content_length_matches_exact_body(
            headers,
            exact_body_size_bytes=actual,
        )
        is expected
    )
