from __future__ import annotations

from copy import deepcopy

import pytest

import scripts.validate_senior_analyst_payload as acceptance
from app.services.senior_analyst_projection_v1 import _delivery_evidence


def _calendar_payload() -> dict:
    event = {
        "occurrence_id": "controlled-pce",
        "metric_id": "headline_pce_yoy",
        "consensus": 2.7,
        "field_lineage": {
            "consensus": {"provider_type": "AI_RESEARCHER_CODEX_CLI"}
        },
    }
    analytics = {
        "calendar": {
            "active_event_windows": [event],
            "next_24h_events": [],
            "next_7d_high_impact_events": [],
        }
    }
    reference = _delivery_evidence(
        "macro_calendar",
        analytics=analytics,
        missing_data=[],
    )["delivered_value"]
    return {
        "analytics": analytics,
        "provider_accounting": [
            {
                "dataset_id": "macro_calendar",
                "acquisition_selected_source": "AI_RESEARCHER",
                "selected_value_present": True,
                "delivered_value": reference,
            }
        ],
    }


def test_ai_acceptance_resolves_verified_ordered_collection_paths() -> None:
    payload = _calendar_payload()
    row = payload["provider_accounting"][0]

    delivered = acceptance._ai_delivery_value(
        row["delivered_value"],
        payload=payload,
        dataset_id="macro_calendar",
    )
    usages = acceptance._delivered_ai_field_usages(payload)

    assert delivered == payload["analytics"]["calendar"][
        "active_event_windows"
    ]
    assert (
        "AI_RESEARCHER",
        "macro_calendar",
        "headline_pce_yoy",
        "consensus",
    ) in usages


def test_ai_acceptance_resolves_verified_single_collection_path() -> None:
    analytics = {
        "news": {"current_news": [{"headline": "Controlled news"}]}
    }
    reference = _delivery_evidence(
        "current_news",
        analytics=analytics,
        missing_data=[],
    )["delivered_value"]
    payload = {"analytics": analytics}

    assert acceptance._ai_delivery_value(
        reference,
        payload=payload,
        dataset_id="current_news",
    ) == analytics["news"]["current_news"]


@pytest.mark.parametrize("mutation", ["path", "order", "count", "hash"])
def test_ai_acceptance_fails_closed_on_reference_tampering(
    mutation: str,
) -> None:
    payload = _calendar_payload()
    reference = payload["provider_accounting"][0]["delivered_value"]
    tampered = deepcopy(reference)
    if mutation == "path":
        tampered["payload_path"][0] = (
            "analytics.calendar.latest_released_events"
        )
    elif mutation == "order":
        tampered["payload_path"].reverse()
    elif mutation == "count":
        tampered["item_count"] += 1
    else:
        tampered["content_sha256"] = "0" * 64
    payload["provider_accounting"][0]["delivered_value"] = tampered

    delivered = acceptance._ai_delivery_value(
        tampered,
        payload=payload,
        dataset_id="macro_calendar",
    )
    usages = acceptance._delivered_ai_field_usages(payload)

    assert delivered is acceptance._INVALID_AI_DELIVERY_REFERENCE
    assert (
        "AI_RESEARCHER",
        "macro_calendar",
        "__INVALID_DELIVERY_REFERENCE__",
        "__INVALID_DELIVERY_REFERENCE__",
    ) in usages
    assert acceptance._ai_used_without_certification(
        payload,
        report={"results": []},
    ) > 0
