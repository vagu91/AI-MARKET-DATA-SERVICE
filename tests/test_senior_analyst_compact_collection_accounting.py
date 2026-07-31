from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from app.core.senior_analyst_policy import (
    PROVIDER_ACCOUNTING_COLLECTION_PATHS,
)
from app.services.senior_analyst_projection_v1 import (
    FED_FUNDS_RATE_SERIES,
    MACRO_DATASET_SERIES,
    TREASURY_RATE_SERIES,
    _canonical_json,
    _dataset_delivery_value,
    _delivery_collection_reference,
    _delivery_evidence,
    _resolve_accounting_delivery,
    _selected_value_presence_mismatch_count,
    validate_senior_analyst_payload_v1,
)


NOW = datetime(2026, 7, 31, 15, 0, tzinfo=UTC)


def _paths(dataset_id: str) -> tuple[str, ...]:
    configured = PROVIDER_ACCOUNTING_COLLECTION_PATHS[dataset_id]
    return (configured,) if isinstance(configured, str) else configured


def _resolve_path(payload: dict, path: str):
    value = payload
    for part in path.split("."):
        value = value[part]
    return value


def _metric(series_id: str) -> dict:
    return {
        "series_id": series_id,
        "value": 1.0,
        "source": "CONTROLLED",
        "freshness": "CURRENT",
        "data_as_of": NOW.isoformat(),
        "content_valid_until": (NOW + timedelta(days=1)).isoformat(),
    }


def _all_collection_analytics() -> dict:
    macro_metrics = [
        _metric(sorted(series_ids)[0])
        for series_ids in MACRO_DATASET_SERIES.values()
    ]
    treasury = _metric(sorted(TREASURY_RATE_SERIES)[0])
    fed_funds = _metric(sorted(FED_FUNDS_RATE_SERIES)[0])
    pmi = {
        "occurrence_id": "controlled-flash-pmi",
        "event_id": "controlled-flash-pmi",
        "metric_id": "flash_services_pmi",
        "name": "Flash Services PMI",
        "actual": 53.6,
        "actual_source": "CONTROLLED",
        "freshness_state": "CURRENT",
        "release_at": NOW.isoformat(),
        "reference_period": "2026-07",
        "content_valid_until": (NOW + timedelta(days=1)).isoformat(),
    }
    return {
        "macro": {
            "source": "CONTROLLED",
            "freshness": "CURRENT",
            "metrics": macro_metrics,
        },
        "rates": {
            "source": "CONTROLLED",
            "freshness": "CURRENT",
            "treasury_rates": [treasury],
            "fed_funds": [fed_funds],
            "metrics": [treasury, fed_funds],
        },
        "nasdaq": {
            "source": "CONTROLLED",
            "freshness": "CURRENT",
            "components": [
                {
                    "symbol": "NVDA",
                    "weight_pct": 8.2,
                    "price": 170.0,
                    "change_pct": 1.5,
                    "source": "CONTROLLED",
                    "freshness": "CURRENT",
                    "data_as_of": NOW.isoformat(),
                    "content_valid_until": (
                        NOW + timedelta(hours=1)
                    ).isoformat(),
                }
            ],
        },
        "calendar": {
            "source": "CONTROLLED",
            "freshness": "CURRENT",
            "latest_released_events": [pmi],
            "active_event_windows": [
                {"occurrence_id": "active", "name": "Active event"}
            ],
            "next_24h_events": [
                {"occurrence_id": "next-24h", "name": "Next event"}
            ],
            "next_7d_high_impact_events": [
                {"occurrence_id": "next-7d", "name": "Weekly event"}
            ],
        },
        "earnings": {
            "source": "CONTROLLED",
            "freshness": "CURRENT",
            "events": [{"symbol": "AMD", "event_date": "2026-08-01"}],
        },
        "news": {
            "source": "CONTROLLED",
            "freshness": "CURRENT",
            "current_news": [{"headline": "Controlled current news"}],
        },
    }


def _reference_for(dataset_id: str, analytics: dict) -> dict:
    return _delivery_collection_reference(
        PROVIDER_ACCOUNTING_COLLECTION_PATHS[dataset_id],
        payload_root={"analytics": analytics},
    )


def _validator_payload(analytics: dict, row: dict) -> dict:
    return {
        "generated_at": NOW.isoformat(),
        "analytics": analytics,
        "missing_data": [],
        "provider_accounting": [row],
        "request": {},
    }


def test_every_delivered_collection_uses_an_exact_consumer_reference() -> None:
    analytics = _all_collection_analytics()
    payload_root = {"analytics": analytics}

    for dataset_id in PROVIDER_ACCOUNTING_COLLECTION_PATHS:
        _, semantic_delivery = _dataset_delivery_value(
            dataset_id,
            analytics,
        )
        assert isinstance(semantic_delivery, list)
        assert semantic_delivery

        evidence = _delivery_evidence(
            dataset_id,
            analytics=analytics,
            missing_data=[],
        )
        reference = evidence["delivered_value"]
        paths = _paths(dataset_id)
        exact_nodes = [
            item
            for path in paths
            for item in _resolve_path(payload_root, path)
        ]

        assert evidence["selected_value_present"] is True
        assert set(reference) == {
            "payload_path",
            "item_count",
            "content_sha256",
        }
        assert reference["payload_path"] == (
            paths[0] if len(paths) == 1 else list(paths)
        )
        assert reference["item_count"] == len(exact_nodes)
        assert reference["content_sha256"] == hashlib.sha256(
            _canonical_json(exact_nodes)
        ).hexdigest()
        assert _resolve_accounting_delivery(
            dataset_id,
            reference,
            payload_root=payload_root,
        ) == semantic_delivery


@pytest.mark.parametrize(
    ("dataset_id", "analytics"),
    [
        (
            "pce",
            {
                "macro": {
                    "metrics": [
                        _metric(sorted(MACRO_DATASET_SERIES["cpi"])[0])
                    ]
                }
            },
        ),
        (
            "mega_cap_quotes",
            {
                "nasdaq": {
                    "components": [
                        {"symbol": "NVDA", "weight_pct": 8.2}
                    ]
                }
            },
        ),
        (
            "flash_services_pmi",
            {
                "calendar": {
                    "latest_released_events": [],
                    "active_event_windows": [
                        {"occurrence_id": "cpi", "name": "CPI"}
                    ],
                    "next_24h_events": [],
                    "next_7d_high_impact_events": [],
                }
            },
        ),
    ],
)
def test_shared_collection_reference_does_not_invent_dataset_delivery(
    dataset_id: str,
    analytics: dict,
) -> None:
    reference = _reference_for(dataset_id, analytics)
    row = {
        "dataset_id": dataset_id,
        "selected_value_present": True,
        "delivered_value": reference,
    }

    assert _resolve_accounting_delivery(
        dataset_id,
        reference,
        payload_root={"analytics": analytics},
    ) == []
    assert _selected_value_presence_mismatch_count(
        [row],
        payload_root={"analytics": analytics},
    ) == 1


@pytest.mark.parametrize(
    "mutation",
    [
        "path",
        "path_order",
        "count",
        "hash",
    ],
)
def test_calendar_reference_tampering_fails_closed(mutation: str) -> None:
    analytics = _all_collection_analytics()
    reference = _reference_for("macro_calendar", analytics)
    tampered = deepcopy(reference)
    if mutation == "path":
        tampered["payload_path"][0] = (
            "analytics.calendar.latest_released_events"
        )
    elif mutation == "path_order":
        tampered["payload_path"].reverse()
    elif mutation == "count":
        tampered["item_count"] += 1
    else:
        tampered["content_sha256"] = "0" * 64
    row = {
        "dataset_id": "macro_calendar",
        "selected_value_present": True,
        "delivered_value": tampered,
    }

    result = validate_senior_analyst_payload_v1(
        _validator_payload(analytics, row),
        now=NOW,
    )

    assert result["status"] == "FAIL"
    assert result["checks"]["selected_value_presence_mismatches"] == 1


def test_even_a_small_inline_delivery_list_fails_closed() -> None:
    analytics = {
        "news": {
            "current_news": [{"headline": "Small but still duplicated"}]
        }
    }
    row = {
        "dataset_id": "current_news",
        "selected_value_present": True,
        "delivered_value": [{"headline": "Small but still duplicated"}],
    }

    result = validate_senior_analyst_payload_v1(
        _validator_payload(analytics, row),
        now=NOW,
    )

    assert result["status"] == "FAIL"
    assert result["checks"]["selected_value_presence_mismatches"] == 1
    assert result["checks"]["duplicate_large_collections"] == 1
