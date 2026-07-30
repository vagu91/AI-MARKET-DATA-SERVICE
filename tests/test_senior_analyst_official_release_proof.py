from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

from app.services.senior_analyst_projection_v1 import (
    build_senior_analyst_payload_v1,
)


NOW = datetime(2026, 7, 29, 18, 17, 25, tzinfo=UTC)


def _source(item: dict) -> dict:
    return {
        "symbol": "MNQ",
        "generated_at": NOW.isoformat(),
        "sections": {
            "macro": {
                "snapshot": {
                    "inflation": {
                        "CUSR0000SA0": item,
                    }
                }
            }
        },
    }


def _official_cpi() -> dict:
    return {
        "series_id": "CUSR0000SA0",
        "name": "Consumer Price Index",
        "metric": "headline_cpi_index",
        "value": 325.4,
        "unit": "index",
        "frequency": "monthly",
        "reference_period": "2026-06",
        "data_as_of": "2026-06-01",
        "latest_release_at": "2026-07-15T12:30:00Z",
        "occurrence_id": "BLS:CPI:2026-06:2026-07-15T12:30:00Z",
        "retrieved_at": NOW.isoformat(),
        "content_valid_until": "2026-08-12T12:29:59Z",
        "freshness": "STALE",
        "is_official_source": True,
        "source": "BLS",
    }


def _proof() -> dict:
    return {
        "status": "VERIFIED",
        "is_latest_expected_release": True,
        "frequency": "monthly",
        "source_series_id": "CUSR0000SA0",
        "occurrence_id": "BLS:CPI:2026-06:2026-07-15T12:30:00Z",
        "expected_reference_period": "2026-06",
        "expected_release_at": "2026-07-15T12:30:00Z",
        "next_expected_release_at": "2026-08-12T12:30:00Z",
        "validated_at": NOW.isoformat(),
    }


def _headline_metric(item: dict) -> dict:
    payload = build_senior_analyst_payload_v1(_source(item), now=NOW)
    return next(
        metric
        for metric in payload["analytics"]["macro"]["metrics"]
        if metric["metric_id"] == "headline_cpi_index"
    )


def test_official_release_retrieved_today_is_not_latest_release_proof() -> None:
    metric = _headline_metric(_official_cpi())
    assert metric["value"] is None
    assert metric["freshness"] == "UNAVAILABLE"
    assert metric["reason_code"] == "LATEST_OFFICIAL_RELEASE_NOT_PROVEN"


def test_expired_content_is_rejected_even_with_complete_release_proof() -> None:
    item = _official_cpi()
    item["official_release_evidence"] = _proof()
    item["content_valid_until"] = "2026-07-28T23:59:59Z"
    metric = _headline_metric(item)
    assert metric["value"] is None
    assert metric["reason_code"] == "CONTENT_VALIDITY_EXPIRED"


def test_latest_official_release_requires_occurrence_period_and_lifecycle_proof() -> None:
    item = _official_cpi()
    item["official_release_evidence"] = _proof()
    metric = _headline_metric(item)
    assert metric["value"] == 325.4
    assert metric["freshness"] == "CURRENT_LATEST_OFFICIAL_RELEASE"

    mismatched = deepcopy(item)
    mismatched["official_release_evidence"]["expected_reference_period"] = "2026-05"
    excluded = _headline_metric(mismatched)
    assert excluded["value"] is None
    assert excluded["reason_code"] == "LATEST_OFFICIAL_RELEASE_NOT_PROVEN"
