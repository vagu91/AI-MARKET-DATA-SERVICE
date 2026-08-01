from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.services.senior_analyst_projection_v1 import (
    _dataset_delivery_value,
    _delivery_evidence,
    _ensure_required_dataset_missing_data,
    build_senior_analyst_payload_v1,
    validate_senior_analyst_payload_v1,
)


NOW = datetime(2026, 7, 30, 15, 30, tzinfo=UTC)


def _source(*rate_rows: tuple[str, float]) -> dict:
    rates = {
        series_id: {
            "series_id": series_id,
            "value": value,
            "frequency": "daily",
            "data_as_of": (NOW - timedelta(days=1)).isoformat(),
            "content_valid_until": (NOW + timedelta(days=1)).isoformat(),
            "publisher": "FRED",
            "source_url": f"https://fred.stlouisfed.org/series/{series_id}",
            "field_lineage": {
                "value": {
                    "provider": "FRED",
                    "series_id": series_id,
                }
            },
        }
        for series_id, value in rate_rows
    }
    return {
        "contract": "ai_trader_market_context_sync",
        "schema_version": "1.0",
        "symbol": "MNQ",
        "snapshot_id": "second-live-rates-regression",
        "snapshot_revision": 100,
        "generated_at": NOW.isoformat(),
        "sections": {
            "macro": {
                "snapshot": {
                    "inflation": {},
                    "growth": {},
                    "labor": {},
                    "rates_and_yields": rates,
                }
            },
            "event_calendar": {},
            "fed": {},
            "nasdaq": {},
            "market_internals": {},
            "news": {},
            "vix": {},
            "risk": {},
            "rates": {},
            "positioning": {},
            "earnings": {},
            "options_positioning": {},
            "market_schedule": {},
        },
    }


def _build(*rate_rows: tuple[str, float]) -> dict:
    return build_senior_analyst_payload_v1(
        _source(*rate_rows),
        now=NOW,
    )


def _dataset_missing(payload: dict, dataset_id: str) -> dict:
    return next(
        item
        for item in payload["missing_data"]
        if item["field"] == f"datasets.{dataset_id}"
    )


def test_target_range_maps_complete_fred_bounds_with_lineage() -> None:
    payload = _build(
        ("DFEDTARL", 3.5),
        ("DFEDTARU", 3.75),
    )

    target = payload["analytics"]["rates"]["target_range"]

    assert target["status"] == "AVAILABLE"
    assert target["target_range_lower"] == 3.5
    assert target["target_range_upper"] == 3.75
    assert target["source"]["publisher"] == "FRED"
    assert target["data_as_of"] == "2026-07-29T15:30:00+00:00"
    assert target["content_valid_until"] == "2026-07-31T15:30:00+00:00"
    assert {
        (item["field"], item["series_id"])
        for item in target["lineage"]
    } == {
        ("target_range_lower", "DFEDTARL"),
        ("target_range_upper", "DFEDTARU"),
    }
    assert not any(
        item["field"] == "datasets.target_range"
        for item in payload["missing_data"]
    )


def test_target_range_with_only_lower_bound_is_partial_and_explained() -> None:
    payload = _build(("DFEDTARL", 3.5))

    rates = payload["analytics"]["rates"]
    target = rates["target_range"]
    readiness = payload["readiness"]

    assert rates["status"] == "PARTIAL"
    assert target["status"] == "PARTIAL"
    assert target["target_range_lower"] == 3.5
    assert target["target_range_upper"] is None
    assert target["reason_code"] == "TARGET_RANGE_UPPER_BOUND_NOT_AVAILABLE"
    assert _dataset_missing(payload, "target_range")["reason_code"] == (
        "TARGET_RANGE_UPPER_BOUND_NOT_AVAILABLE"
    )
    assert readiness["delivered_value_counts"]["rates"] == 1
    assert readiness["section_status"]["rates"] == "PARTIAL"
    assert "rates" in readiness["sections_degraded"]
    assert "rates" not in readiness["sections_available"]
    assert "rates" not in readiness["sections_unavailable"]
    report = validate_senior_analyst_payload_v1(payload, now=NOW)
    assert report["checks"]["required_dataset_omissions_without_reason"] == 0
    assert (
        report["checks"]["readiness_section_classification_mismatches"]
        == 0
    )


def test_partial_rates_target_is_never_replaced_by_fomc_bounds() -> None:
    payload = _build(("DFEDTARL", 3.5))
    payload["analytics"]["fomc"].update(
        {
            "target_range_lower": 4.0,
            "target_range_upper": 4.25,
        }
    )

    section, target = _dataset_delivery_value(
        "target_range",
        payload["analytics"],
    )
    delivery = _delivery_evidence(
        "target_range",
        analytics=payload["analytics"],
        missing_data=payload["missing_data"],
    )

    assert section == "rates"
    assert target["status"] == "PARTIAL"
    assert target["target_range_lower"] == 3.5
    assert target["target_range_upper"] is None
    assert delivery["selected_value_present"] is False
    assert delivery["delivered_value"] is None


def test_rates_available_without_any_complete_dataset_fails_gate() -> None:
    payload = _build(("DFEDTARL", 3.5))
    payload["analytics"]["rates"]["status"] = "AVAILABLE"

    report = validate_senior_analyst_payload_v1(payload, now=NOW)

    assert report["status"] == "FAIL"
    assert report["checks"]["available_without_substantive_value"] == 1


def test_required_dataset_without_corresponding_missing_data_fails_gate() -> None:
    payload = _build()
    payload["missing_data"] = [
        item
        for item in payload["missing_data"]
        if item["field"] != "datasets.treasury_rates"
    ]

    report = validate_senior_analyst_payload_v1(payload, now=NOW)

    assert report["status"] == "FAIL"
    assert report["checks"]["required_dataset_omissions_without_reason"] == 1
    assert report["checks"]["unexplained_omissions"] == 1


def test_generic_final_payload_reason_does_not_explain_required_dataset() -> None:
    payload = _build()
    _dataset_missing(payload, "treasury_rates")["reason_code"] = (
        "FINAL_PAYLOAD_VALUE_NOT_AVAILABLE"
    )

    report = validate_senior_analyst_payload_v1(payload, now=NOW)

    assert report["status"] == "FAIL"
    assert report["checks"]["required_dataset_omissions_without_reason"] == 1
    assert report["checks"]["unexplained_omissions"] == 1


def test_provider_success_without_required_series_has_auditable_reason() -> None:
    source = _source()
    source["request_scoped_provider_accounting"] = {
        "datasets": [
            {
                "dataset_id": "treasury_rates",
                "database_record_expired": True,
                "database_refresh_due_at": NOW.isoformat(),
                "primary_provider": {
                    "provider": "FRED",
                    "called": True,
                    "attempts": 1,
                    "result": "SUCCESS",
                },
                "fallbacks": [],
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=NOW)

    assert _dataset_missing(payload, "treasury_rates")["reason_code"] == (
        "PROVIDER_SUCCEEDED_REQUIRED_SERIES_ABSENT_AFTER_EXPIRED_DATABASE_RECORD"
    )


def test_observed_required_series_without_delivery_mapping_is_distinct() -> None:
    source = _source(("DGS10", 4.25))
    source["request_scoped_provider_accounting"] = {
        "datasets": [
            {
                "dataset_id": "treasury_rates",
                "database_record_expired": True,
                "database_refresh_due_at": NOW.isoformat(),
                "primary_provider": {
                    "provider": "FRED",
                    "called": True,
                    "attempts": 1,
                    "result": "SUCCESS",
                },
                "fallbacks": [],
            }
        ]
    }
    payload = build_senior_analyst_payload_v1(source, now=NOW)
    assert payload["analytics"]["rates"]["treasury_rates"]

    payload["analytics"]["rates"]["treasury_rates"] = []
    missing = _ensure_required_dataset_missing_data(
        payload["analytics"],
        missing_data=payload["missing_data"],
        source_payload=source,
    )
    treasury = next(
        item
        for item in missing
        if item["field"] == "datasets.treasury_rates"
    )

    assert treasury["reason_code"] == "DELIVERY_MAPPING_MISSING"
