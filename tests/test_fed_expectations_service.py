from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.services.fed_expectations_repository import FedExpectationsRepository
from app.services.fed_expectations_service import (
    FedExpectationsService,
    aggregate_outcomes,
    build_current_fed_state,
    calculate_repricing,
    canonicalize_investing_monitor,
    canonicalize_meeting,
    classify_change,
    official_fomc_dates,
    parse_probability,
    parse_target_range,
    reconstruct_monthly_futures_distribution,
    select_source,
    validate_distribution,
)


NOW = datetime(2026, 7, 11, 12, tzinfo=UTC)


def settings(tmp_path) -> Settings:
    return Settings(_env_file=None, database_path=tmp_path / "market.sqlite")


def macro_snapshot(*, complete: bool = True) -> dict:
    rates = {
        "DFF": {"value": 3.89, "data_as_of": "2026-07-10"},
        "SOFR": {"value": 3.91, "data_as_of": "2026-07-10"},
    }
    if complete:
        rates.update(
            {
                "DFEDTARL": {"value": 3.75, "data_as_of": "2026-07-10"},
                "DFEDTARU": {"value": 4.0, "data_as_of": "2026-07-10"},
            }
        )
    return {"rates_and_yields": rates}


def event_calendar(date_value: str = "2026-07-29") -> dict:
    return {
        "fed_communications": [
            {
                "event_id": "fed-fomc",
                "name": "Federal Open Market Committee Meeting",
                "category": "FOMC",
                "date": date_value,
                "time_utc": f"{date_value}T18:00:00Z",
                "source": "Federal Reserve Calendar",
            },
            {
                "event_id": "fed-presser",
                "name": "FOMC Press Conference",
                "category": "FOMC",
                "date": date_value,
            },
        ]
    }


def provider_payload(*, retrieved_at: str = "2026-07-11T12:00:00Z", total=(64.6, 35.4)) -> dict:
    return {
        "status": "found",
        "provider": "Investing.com Fed Rate Monitor",
        "source": "Investing Fed Rate Monitor",
        "source_url": "https://www.investing.com/central-banks/fed-rate-monitor",
        "source_type": "secondary_market_implied_probabilities",
        "official_fed_source": False,
        "retrieved_at": retrieved_at,
        "valid_until": "2026-07-11T13:00:00Z",
        "provider_calls": 1,
        "cache_used": False,
        "meetings": [
            {
                "meeting_date": "2026-07-29",
                "meeting_at": "2026-07-29T14:00:00-04:00",
                "updated_at": retrieved_at,
                "event_id": "516971",
                "future_price": 96.37,
                "target_rate_probabilities": [
                    {"target_rate": "3.50 - 3.75", "current_probability_pct": total[0]},
                    {"target_rate": "3.75 - 4.00", "current_probability_pct": total[1]},
                ],
            }
        ],
        "warnings": [],
        "errors": [],
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("3.50 - 3.75", (3.5, 3.75)),
        ("3.50%-3.75%", (3.5, 3.75)),
        ("0.00 - 0.25", (0.0, 0.25)),
        ("19.75 - 20.00", (19.75, 20.0)),
        ("4 - 4.25", (4.0, 4.25)),
        ("3,50 - 3,75", None),
        ("3.75", None),
        ("", None),
        (None, None),
        ("4.00 - 3.75", None),
        ("-0.25 - 0.00", None),
        ("25.00 - 25.25", None),
    ],
)
def test_parse_target_range_cases(value, expected) -> None:
    assert parse_target_range(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [(64.6, 0.646), ("64.6%", 0.646), (0.646, 0.646), (1, 1.0), (0, 0.0), (100, 1.0), ("0.25", 0.25), (None, None), ("", None), ("N/A", None)],
)
def test_probability_unit_normalization(value, expected) -> None:
    assert parse_probability(value) == expected


@pytest.mark.parametrize(
    ("change", "expected"),
    [(-75, "cut"), (-50, "cut"), (-25, "cut"), (-12.5, "hold"), (0, "hold"), (12.5, "hold"), (25, "hike"), (50, "hike"), (None, "hold")],
)
def test_change_classification(change, expected) -> None:
    assert classify_change(change) == expected


def test_current_fed_state_materializes_official_range_midpoint_effr_sofr_and_next_fomc() -> None:
    state = build_current_fed_state(macro_snapshot(), official_dates={"2026-07-29"}, now=NOW)

    assert state["current_target_lower_bound"] == 3.75
    assert state["current_target_upper_bound"] == 4.0
    assert state["current_target_midpoint"] == 3.875
    assert state["effective_fed_funds_rate"] == 3.89
    assert state["sofr"] == 3.91
    assert state["next_fomc_meeting_at"] == "2026-07-29T18:00:00Z"
    assert state["days_to_next_fomc"] == 18
    assert state["is_official_source"] is True


def test_current_range_missing_is_explicit() -> None:
    state = build_current_fed_state(macro_snapshot(complete=False), official_dates=set(), now=NOW)

    assert state["current_target_midpoint"] is None
    assert state["is_official_source"] is False


def test_calendar_mapping_excludes_press_conference_and_accepts_modified_date() -> None:
    assert official_fomc_dates(event_calendar("2026-07-30"), now=NOW) == {"2026-07-30"}


def test_calendar_mapping_keeps_combined_meeting_and_press_conference_title() -> None:
    calendar_payload = event_calendar()
    calendar_payload["fed_communications"][0]["name"] = "FOMC Meeting - Two-day meeting, July 28 - 29 - Press Conference"
    assert official_fomc_dates(calendar_payload, now=NOW) == {"2026-07-29"}


def test_monitor_canonicalization_builds_distribution_aggregates_and_provenance() -> None:
    result = canonicalize_investing_monitor(
        provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), history=[], now=NOW
    )
    meeting = result["meetings"][0]

    assert result["status"] == "available"
    assert meeting["meeting_time_utc"] == "2026-07-29T18:00:00Z"
    assert meeting["cut_probability"] == pytest.approx(0.646)
    assert meeting["hold_probability"] == pytest.approx(0.354)
    assert meeting["hike_probability"] == 0
    assert meeting["expected_target_midpoint"] == pytest.approx(3.7135)
    assert meeting["expected_change_bps"] == pytest.approx(-16.15)
    assert meeting["most_likely_target_range"] == "3.50-3.75"
    assert meeting["validation"]["meeting_date_match"] is True
    assert result["source_summary"]["selected_source_type"] == "secondary_monitor"
    assert result["source_summary"]["is_official_source"] is False
    assert result["quality"]["quality_score"] < 0.8


def test_meeting_beyond_loaded_official_calendar_horizon_is_not_a_false_mismatch() -> None:
    payload = provider_payload()
    future = dict(payload["meetings"][0])
    future["meeting_date"] = "2026-09-16"
    future["event_id"] = "future"
    payload["meetings"].append(future)
    calendar_payload = event_calendar()
    calendar_payload["other_economic_events"] = [{"date": "2026-08-31", "category": "OTHER"}]
    result = canonicalize_investing_monitor(
        payload, macro_snapshot=macro_snapshot(), event_calendar=calendar_payload, now=NOW
    )
    assert result["meetings"][1]["validation"]["meeting_date_match"] is None
    assert result["diagnostics"]["error_breakdown"]["calendar_mismatch"] == 0
    assert result["quality"]["mapping_valid_pct"] == 100


def test_investing_is_never_promoted_to_official() -> None:
    payload = provider_payload()
    payload["official_fed_source"] = True
    result = canonicalize_investing_monitor(payload, macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW)

    assert result["source_summary"]["is_official_source"] is False
    assert all(item["is_official_source"] is False for item in result["meetings"])


def test_safe_rounding_normalization_is_recorded() -> None:
    result = canonicalize_investing_monitor(
        provider_payload(total=(64.6, 35.3)), macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )

    assert result["status"] == "available"
    assert result["diagnostics"]["probability_normalization_count"] == 1
    assert sum(item["probability"] for item in result["meetings"][0]["outcomes"]) == pytest.approx(1.0)


def test_sub_one_percent_row_uses_distribution_level_percent_units() -> None:
    payload = provider_payload(total=(99.8, 0.2))
    result = canonicalize_investing_monitor(
        payload, macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )
    assert result["status"] == "available"
    assert result["meetings"][0]["outcomes"][1]["probability"] == pytest.approx(0.002)


@pytest.mark.parametrize("total", [(64.6, 20.0), (10.0, 10.0), (50.0, 120.0), (-1.0, 101.0)])
def test_grossly_invalid_distributions_are_rejected(total) -> None:
    result = canonicalize_investing_monitor(
        provider_payload(total=total), macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )

    assert result["status"] == "not_found"
    assert result["diagnostics"]["invalid_distribution_count"] == 1


def test_duplicate_target_ranges_are_rejected() -> None:
    payload = provider_payload()
    payload["meetings"][0]["target_rate_probabilities"][1]["target_rate"] = "3.50 - 3.75"
    result = canonicalize_investing_monitor(payload, macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW)

    assert result["status"] == "not_found"


def test_null_and_empty_provider_payloads_are_not_found() -> None:
    for payload in ({}, {"meetings": None}, {"meetings": []}):
        result = canonicalize_investing_monitor(payload, macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW)
        assert result["status"] == "not_found"
        assert result["quality"]["meeting_coverage_pct"] == 0


def test_schema_changed_is_diagnostic_not_fabricated_data() -> None:
    result = canonicalize_investing_monitor(
        {"status": "found", "unexpected": []}, macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )
    assert result["diagnostics"]["error_breakdown"]["schema_changed"] == 1
    assert result["meetings"] == []


def test_most_likely_and_expected_values_are_correct() -> None:
    outcomes = [
        {"probability": 0.2, "target_midpoint": 3.375, "change_bps": -50, "classification": "cut"},
        {"probability": 0.5, "target_midpoint": 3.625, "change_bps": -25, "classification": "cut"},
        {"probability": 0.3, "target_midpoint": 3.875, "change_bps": 0, "classification": "hold"},
    ]
    result = aggregate_outcomes(outcomes, current_midpoint=3.875)

    assert result["cut_probability"] == 0.7
    assert result["hold_probability"] == 0.3
    assert result["cut_25_probability"] == 0.5
    assert result["cut_50_or_more_probability"] == 0.2
    assert result["expected_target_midpoint"] == pytest.approx(3.65)
    assert result["expected_change_bps"] == pytest.approx(-22.5)


@pytest.mark.parametrize("meeting_day", [1, 2, 5, 10, 15, 20, 25, 28, 29, 30, 31])
def test_futures_reconstruction_uses_month_structure_and_bounded_probabilities(meeting_day) -> None:
    meeting_date = f"2026-07-{meeting_day:02d}"
    result = reconstruct_monthly_futures_distribution(
        futures_price=96.10,
        meeting_date=meeting_date,
        current_effective_rate=3.89,
        current_target_midpoint=3.875,
    )

    assert result["status"] == "available"
    assert result["pre_meeting_days"] == meeting_day - 1
    assert result["post_meeting_days"] == 31 - meeting_day + 1
    assert sum(item["probability"] for item in result["outcomes"]) == pytest.approx(1.0)
    assert all(0 <= item["probability"] <= 1 for item in result["outcomes"])


def test_futures_reconstruction_is_not_naive_price_subtraction() -> None:
    result = reconstruct_monthly_futures_distribution(
        futures_price=96.10,
        meeting_date="2026-07-15",
        current_effective_rate=3.89,
        current_target_midpoint=3.875,
    )

    assert result["monthly_implied_rate"] == pytest.approx(3.9)
    assert result["implied_post_meeting_rate"] != pytest.approx(3.9)


@pytest.mark.parametrize(
    ("kwargs", "status", "warning"),
    [
        ({"futures_price": 0}, "partial", "invalid_price"),
        ({"futures_price": 101}, "partial", "invalid_price"),
        ({"meeting_date": "bad"}, "partial", "meeting_mapping_failed"),
        ({"contract_month": "2026-08"}, "partial", "meeting_contract_month_mismatch"),
        ({"as_of": "2026-08-01"}, "excluded", "contract_expired"),
    ],
)
def test_futures_reconstruction_partial_and_expired_cases(kwargs, status, warning) -> None:
    params = {
        "futures_price": 96.1,
        "meeting_date": "2026-07-15",
        "current_effective_rate": 3.89,
        "current_target_midpoint": 3.875,
    }
    params.update(kwargs)
    result = reconstruct_monthly_futures_distribution(**params)
    assert result["status"] == status
    assert warning in result["warnings"]


@pytest.mark.parametrize(
    "probabilities",
    [
        [0.5, 0.5],
        [0.333, 0.667],
        [0.1, 0.2, 0.7],
        [0.999, 0.001],
        [1.0],
    ],
)
def test_probability_validation_accepts_complete_distributions(probabilities) -> None:
    outcomes = [
        {"target_lower_bound": index / 4, "target_upper_bound": index / 4 + 0.25, "probability": value}
        for index, value in enumerate(probabilities)
    ]
    assert validate_distribution(outcomes)["valid"] is True


@pytest.mark.parametrize("probabilities", [[0.2, 0.2], [0.9, 0.2], [-0.1, 1.1], [1.01]])
def test_probability_validation_rejects_incomplete_or_out_of_bounds(probabilities) -> None:
    outcomes = [
        {"target_lower_bound": index / 4, "target_upper_bound": index / 4 + 0.25, "probability": value}
        for index, value in enumerate(probabilities)
    ]
    assert validate_distribution(outcomes)["valid"] is False


@pytest.mark.parametrize(
    ("candidate_types", "expected"),
    [
        (("secondary_monitor", "verified_vendor_probability"), "lower"),
        (("official_futures_derived_partial", "verified_vendor_probability"), "official"),
        (("official_futures_derived_complete", "official_futures_derived_partial"), "complete"),
        (("last_known_good_official", "secondary_monitor"), "lower"),
        (("last_known_good_vendor", "not_found"), "lkg"),
    ],
)
def test_source_ranking_is_deterministic(candidate_types, expected) -> None:
    candidates = [
        {"status": "available", "ranking_class": candidate_types[0], "name": expected},
        {"status": "available", "ranking_class": candidate_types[1], "name": "lower"},
    ]
    assert select_source(candidates)["name"] == expected


def test_repricing_history_insufficient_is_null() -> None:
    current = canonicalize_investing_monitor(
        provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )["meetings"]
    result = calculate_repricing(current, [], now=NOW)

    assert result["history_status"] == "history_insufficient"
    assert result["probability_change_1h"] is None
    assert result["expected_rate_change_24h_bps"] is None


def test_repricing_calculates_1h_24h_7d_and_same_snapshot_zero() -> None:
    current_payload = canonicalize_investing_monitor(
        provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )
    current = current_payload["meetings"]
    history = []
    for delta in (timedelta(hours=1), timedelta(hours=24), timedelta(days=7)):
        snapshot = canonicalize_investing_monitor(
            provider_payload(retrieved_at=(NOW - delta).isoformat()),
            macro_snapshot=macro_snapshot(),
            event_calendar=event_calendar(),
            now=NOW - delta,
        )
        snapshot["retrieved_at"] = (NOW - delta).isoformat()
        history.append(snapshot)
    result = calculate_repricing(current, history, now=NOW)

    assert result["history_available"] is True
    for suffix in ("1h", "24h", "7d"):
        assert result[f"probability_change_{suffix}"]["cut_probability"] == 0
        assert result[f"expected_rate_change_{suffix}_bps"] == 0


def test_repricing_never_compares_a_different_meeting() -> None:
    current = canonicalize_investing_monitor(
        provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )["meetings"]
    history = [{"retrieved_at": (NOW - timedelta(days=1)).isoformat(), "meetings": [{"meeting_date": "2026-09-16"}]}]
    assert calculate_repricing(current, history, now=NOW)["history_available"] is False


def test_repository_append_readback_provenance_distribution_and_history_survive(tmp_path) -> None:
    repository = FedExpectationsRepository(settings(tmp_path))
    payload = canonicalize_investing_monitor(
        provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )
    repository.append(payload)
    read_back = FedExpectationsRepository(settings(tmp_path)).latest()

    assert repository.count() == 1
    assert read_back["source_summary"] == payload["source_summary"]
    assert read_back["meetings"][0]["outcomes"] == payload["meetings"][0]["outcomes"]
    assert read_back["diagnostics"]["valid_distribution_count"] == 1


def test_force_persists_reads_back_and_materializes(tmp_path) -> None:
    service = FedExpectationsService(settings(tmp_path))
    result = service.snapshot(
        refresh="force",
        provider_payload=provider_payload(),
        macro_snapshot=macro_snapshot(),
        event_calendar=event_calendar(),
        legacy_block={"fed_funds_futures": {"legacy": True}},
    )

    assert result["status"] == "available"
    assert result["diagnostics"]["persisted_count"] == 1
    assert result["diagnostics"]["read_back_count"] == 1
    assert result["diagnostics"]["materialized_count"] == 1
    assert result["cache_status"] == "DB_READ_BACK"
    assert result["fed_funds_futures"]["legacy"] is True


def test_restart_refresh_false_is_db_only_and_preserves_payload(tmp_path) -> None:
    cfg = settings(tmp_path)
    forced = FedExpectationsService(cfg).snapshot(
        refresh="force", provider_payload=provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar()
    )
    cached = FedExpectationsService(cfg).snapshot(
        refresh="false",
        provider_payload={"meetings": [{"must_not_be_used": True}]},
        macro_snapshot={},
        event_calendar={},
    )

    assert cached["meetings"][0]["outcomes"] == forced["meetings"][0]["outcomes"]
    assert cached["diagnostics"]["provider_calls"] == 0
    assert cached["diagnostics"]["browser_calls"] == 0
    assert cached["diagnostics"]["AI_called"] is False
    assert cached["diagnostics"]["cache_used"] is True


def test_auto_uses_fresh_cache_and_does_not_replace_it(tmp_path) -> None:
    cfg = settings(tmp_path)
    payload = provider_payload(retrieved_at=datetime.now(UTC).isoformat())
    payload["valid_until"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    service = FedExpectationsService(cfg)
    service.snapshot(refresh="force", provider_payload=payload, macro_snapshot=macro_snapshot(), event_calendar=event_calendar())
    cached = FedExpectationsService(cfg).snapshot(
        refresh="auto", provider_payload={"status": "failed"}, macro_snapshot={}, event_calendar={}
    )

    assert cached["status"] == "available"
    assert cached["diagnostics"]["cache_used"] is False
    assert FedExpectationsRepository(cfg).count() == 1


def test_failure_does_not_overwrite_last_known_good(tmp_path) -> None:
    cfg = settings(tmp_path)
    service = FedExpectationsService(cfg)
    service.snapshot(refresh="force", provider_payload=provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar())
    fallback = service.snapshot(
        refresh="force", provider_payload={"status": "provider_failed", "meetings": []}, macro_snapshot={}, event_calendar={}
    )

    assert fallback["source_summary"]["last_known_good_used"] is True
    assert fallback["diagnostics"]["last_known_good_used"] is True
    assert FedExpectationsRepository(cfg).count() == 1


def test_http_block_contains_required_sections_and_no_trading_logic(tmp_path) -> None:
    result = FedExpectationsService(settings(tmp_path)).snapshot(
        refresh="force", provider_payload=provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar()
    )
    for key in ("status", "current_fed_state", "next_meeting", "meetings", "repricing", "source_summary", "quality", "diagnostics"):
        assert key in result
    assert result["service_role"] == "data provider only"
    assert "signal" not in result
    assert "recommendation" not in result


def test_quality_penalties_for_missing_current_range_and_stale_source() -> None:
    complete = canonicalize_investing_monitor(
        provider_payload(), macro_snapshot=macro_snapshot(), event_calendar=event_calendar(), now=NOW
    )
    stale_payload = provider_payload()
    stale_payload["valid_until"] = "2026-07-10T00:00:00Z"
    degraded = canonicalize_investing_monitor(
        stale_payload, macro_snapshot=macro_snapshot(complete=False), event_calendar={}, now=NOW
    )

    assert degraded["quality"]["quality_score"] < complete["quality"]["quality_score"]
    assert degraded["quality"]["official_source_coverage_pct"] == 0
    assert degraded["quality"]["stale_snapshot_count"] == 1


def test_not_found_never_reports_high_completeness() -> None:
    result = canonicalize_investing_monitor({}, macro_snapshot={}, event_calendar={}, now=NOW)
    assert result["quality"]["quality_score"] == 0
    assert result["quality"]["probability_distribution_coverage_pct"] == 0


def test_canonical_meeting_accepts_decimal_probabilities_and_contract_month() -> None:
    raw = provider_payload()["meetings"][0]
    raw["target_rate_probabilities"][0]["current_probability_pct"] = 0.646
    raw["target_rate_probabilities"][1]["current_probability_pct"] = 0.354
    meeting, validation = canonicalize_meeting(
        raw,
        current_midpoint=3.875,
        official_dates={"2026-07-29"},
        source="Vendor",
        source_url="https://example.test",
        retrieved_at="2026-07-11T12:00:00Z",
        valid_until="2026-07-11T13:00:00Z",
        now=NOW,
    )
    assert meeting["contract_month"] == "2026-07"
    assert meeting["validation"]["probability_sum"] == 1
    assert validation["probabilities_normalized"] is False
