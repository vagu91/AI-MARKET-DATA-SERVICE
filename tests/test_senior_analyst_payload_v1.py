from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.main import app
from app.api.routes import _consumer_projection
from app.services.senior_analyst_projection_v1 import (
    INVALID_ANALYTIC_STATES,
    build_senior_analyst_payload_v1,
    validate_senior_analyst_payload_v1,
)


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_98 = (
    ROOT
    / "data"
    / "live-ai-trader-capture-20260729T181632Z"
    / "senior-analyst-consumer-full.json"
)
FIXED_NOW = datetime(2026, 7, 29, 18, 17, 25, tzinfo=UTC)
REQUIRED_METADATA = {
    "status",
    "freshness",
    "data_as_of",
    "content_valid_until",
    "snapshot_transport_valid_until",
    "refresh_due_at",
    "source",
    "reason_code",
    "lineage",
}


def _load_snapshot_98() -> dict:
    if not SNAPSHOT_98.exists():
        pytest.skip("authoritative local snapshot 98 is not present")
    payload = json.loads(SNAPSHOT_98.read_text(encoding="utf-8"))
    assert payload["snapshot_revision"] == 98
    assert payload["generated_at"].startswith("2026-07-29T18:17:25")
    return payload


def _synthetic_sync() -> dict:
    return {
        "contract": "ai_trader_market_context_sync",
        "schema_version": "1.0",
        "symbol": "MNQ",
        "snapshot_id": "snapshot-test",
        "snapshot_revision": 1,
        "generated_at": FIXED_NOW.isoformat(),
        "sections": {
            "macro": {
                "snapshot": {
                    "inflation": {},
                    "growth": {},
                    "labor": {},
                    "rates_and_yields": {},
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


def _walk_invalid_states(value) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"status", "freshness"} and str(item).upper() in INVALID_ANALYTIC_STATES:
                found.append(str(item).upper())
            found.extend(_walk_invalid_states(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(_walk_invalid_states(item))
    return found


def test_snapshot_98_reproduces_before_defects_and_passes_projection_gate() -> None:
    source = _load_snapshot_98()
    encoded_before = json.dumps(source)
    assert encoded_before.count('"freshness":"STALE"') + encoded_before.count(
        '"freshness": "STALE"'
    ) > 0
    assert '"invalid_period_mapping": true' in encoded_before
    assert '"release_status": "AWAITING_ACTUAL"' in encoded_before

    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
        request_id="snapshot-98-offline",
        request_refresh_mode="replay",
    )
    result = validate_senior_analyst_payload_v1(payload, now=FIXED_NOW)
    assert result["status"] == "PASS_OFFLINE"
    assert result["live_acceptance_evaluated"] is False
    assert not _walk_invalid_states(payload["analytics"])
    assert payload["quality_gate"]["live_acceptance"] == "PENDING"
    assert payload["readiness"]["status"] == "PARTIAL"


def test_snapshot_98_projection_is_byte_deterministic_and_fixed_point() -> None:
    source = _load_snapshot_98()
    kwargs = {
        "now": FIXED_NOW,
        "request_id": "deterministic-replay",
        "request_refresh_mode": "replay",
    }
    first = build_senior_analyst_payload_v1(source, **kwargs)
    second = build_senior_analyst_payload_v1(source, **kwargs)
    first_bytes = json.dumps(
        first,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    second_bytes = json.dumps(
        second,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert first_bytes == second_bytes
    assert first["snapshot_revision"] == second["snapshot_revision"] == 98


def test_required_structure_survives_all_null_values() -> None:
    payload = build_senior_analyst_payload_v1(_synthetic_sync(), now=FIXED_NOW)
    assert set(payload["analytics"]) == {
        "macro",
        "calendar",
        "fomc",
        "nasdaq",
        "market_internals",
        "news",
        "vix",
        "risk",
        "rates",
        "positioning",
        "earnings",
        "options_positioning",
        "market_schedule",
    }
    for section in payload["analytics"].values():
        assert REQUIRED_METADATA <= set(section)
    assert payload["analytics"]["news"]["current_news"] == []
    assert payload["readiness"]["calculated_from_delivered_payload"] is True
    assert payload["readiness"]["available_section_count"] == 0


def test_monthly_latest_official_release_is_not_rejected_for_old_reference_period() -> None:
    source = _synthetic_sync()
    source["sections"]["macro"]["snapshot"]["inflation"]["CUSR0000SA0"] = {
        "series_id": "CUSR0000SA0",
        "name": "Consumer Price Index",
        "metric": "headline_cpi_index",
        "value": 325.4,
        "unit": "index",
        "frequency": "monthly",
        "data_as_of": "2026-06-01",
        "retrieved_at": FIXED_NOW.isoformat(),
        "freshness": "STALE",
        "is_official_source": True,
        "source": "BLS",
    }
    metric = build_senior_analyst_payload_v1(source, now=FIXED_NOW)["analytics"][
        "macro"
    ]["metrics"][0]
    assert metric["value"] == 325.4
    assert metric["freshness"] == "CURRENT_LATEST_OFFICIAL_RELEASE"
    assert metric["metric_id"] == "headline_cpi_index"


def test_required_macro_semantics_do_not_reinterpret_levels_as_changes() -> None:
    payload = build_senior_analyst_payload_v1(_load_snapshot_98(), now=FIXED_NOW)
    by_metric = {
        item["metric_id"]: item
        for item in payload["analytics"]["macro"]["metrics"]
    }
    assert by_metric["initial_jobless_claims"]["unit"] == "claims"
    assert by_metric["real_gdp_annualized_qoq"]["transformation"] == (
        "official_annualized_qoq_rate"
    )
    assert by_metric["personal_consumption_expenditures_nominal_level"][
        "transformation"
    ] == "level"
    assert by_metric["total_nonfarm_payroll_level"]["value"] == 158984.0
    assert by_metric["nonfarm_payrolls_change"]["value"] is None
    assert by_metric["nonfarm_payrolls_change"]["reason_code"] == (
        "INSUFFICIENT_VALID_HISTORY"
    )
    assert by_metric["headline_cpi_index"]["series_id"] == "CUSR0000SA0"


def test_calendar_deduplicates_and_never_combines_occurrences_for_surprise() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW + timedelta(hours=2)
    base = {
        "occurrence_id": "one",
        "metric_id": "headline_cpi_mom",
        "name": "CPI",
        "release_at": release.isoformat(),
        "reference_period": "2026-06",
        "impact": "HIGH",
        "actual": 0.2,
        "forecast": 0.1,
        "previous": 0.0,
        "field_lineage": {
            "actual": {"occurrence_id": "one"},
            "forecast": {"occurrence_id": "two"},
        },
    }
    duplicate = {**base, "event_id": "duplicate"}
    source["sections"]["event_calendar"] = {
        "next_24h_events": [base, duplicate],
        "upcoming_high_impact_events": [base, duplicate],
    }
    calendar = build_senior_analyst_payload_v1(source, now=FIXED_NOW)[
        "analytics"
    ]["calendar"]
    assert len(calendar["next_24h_events"]) == 1
    event = calendar["next_24h_events"][0]
    assert event["actual"] is None
    assert event["consensus"] is None
    assert event["surprise_absolute"] is None
    assert event["reason_code"] == "OCCURRENCE_FIELD_LINEAGE_MISMATCH"


def test_past_awaiting_actual_and_invalid_period_mappings_are_excluded() -> None:
    source = _synthetic_sync()
    source["sections"]["event_calendar"]["next_24h_events"] = [
        {
            "occurrence_id": "past",
            "metric_id": "pmi",
            "release_at": (FIXED_NOW - timedelta(hours=1)).isoformat(),
            "release_status": "AWAITING_ACTUAL",
        },
        {
            "occurrence_id": "bad-period",
            "metric_id": "employment_situation",
            "release_at": (FIXED_NOW + timedelta(hours=1)).isoformat(),
            "invalid_period_mapping": True,
        },
    ]
    calendar = build_senior_analyst_payload_v1(source, now=FIXED_NOW)[
        "analytics"
    ]["calendar"]
    assert calendar["next_24h_events"] == []


def test_pre_meeting_probabilities_are_historical_and_excluded_after_release() -> None:
    source = _synthetic_sync()
    source["sections"]["fed"]["fomc_context"] = {
        "decision_time_utc": (FIXED_NOW - timedelta(minutes=1)).isoformat(),
        "status": "available",
        "probability_hold": 70.0,
        "probability_cut_25bps": 30.0,
        "expected_action": "hold",
        "source": "secondary monitor",
    }
    fomc = build_senior_analyst_payload_v1(source, now=FIXED_NOW)["analytics"][
        "fomc"
    ]
    assert fomc["status"] == "UNAVAILABLE_AFTER_RELEASE"
    assert fomc["pre_meeting_probabilities"] == []
    assert fomc["action"] is None
    assert fomc["reason_code"] == "OFFICIAL_OUTCOME_NOT_AVAILABLE"


def test_contradictory_nasdaq_drivers_are_excluded() -> None:
    source = _synthetic_sync()
    source["sections"]["nasdaq"] = {
        "qqq_holdings": {
            "holdings": [
                {
                    "symbol": "AAPL",
                    "price": 200,
                    "change_pct": 1.0,
                    "weight_pct": 10.0,
                    "freshness": "LIVE",
                    "retrieved_at": FIXED_NOW.isoformat(),
                    "valid_until": (FIXED_NOW + timedelta(hours=1)).isoformat(),
                    "source": "NASDAQ",
                }
            ]
        },
        "driver_context": [
            {
                "symbol": "AAPL",
                "change_pct": -9.0,
                "qqq_weight": 10.0,
                "weighted_contribution": -0.9,
            }
        ],
    }
    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    assert payload["analytics"]["nasdaq"]["drivers"] == []
    assert any(
        item["field"] == "nasdaq.drivers.AAPL"
        and item["reason_code"] == "CONTRADICTORY_CANONICAL_QUOTE"
        for item in payload["missing_data"]
    )


def test_expired_news_and_stale_risk_values_never_feed_current_context() -> None:
    source = _synthetic_sync()
    source["sections"]["news"] = {
        "latest": [
            {
                "article_id": "expired",
                "headline": "Old",
                "published_at": (FIXED_NOW - timedelta(days=2)).isoformat(),
                "freshness": "EXPIRED",
            }
        ]
    }
    source["sections"]["vix"] = {
        "vix": {
            "value": 19.0,
            "data_as_of": (FIXED_NOW - timedelta(days=5)).isoformat(),
            "freshness": "STALE",
        },
        "vvix": {
            "value": 105.0,
            "data_as_of": (FIXED_NOW - timedelta(hours=5)).isoformat(),
            "freshness": "STALE",
        },
    }
    source["sections"]["risk"] = {
        "risk_context": {
            "status": "AVAILABLE",
            "freshness": "STALE",
            "data_as_of": (FIXED_NOW - timedelta(hours=5)).isoformat(),
            "derived_context": {"risk_regime": "RISK_OFF"},
        }
    }
    analytics = build_senior_analyst_payload_v1(source, now=FIXED_NOW)[
        "analytics"
    ]
    assert analytics["news"]["current_news"] == []
    assert analytics["news"]["status"] == "NO_DATA"
    assert analytics["vix"]["VIX"]["value"] is None
    assert analytics["vix"]["VVIX"]["value"] is None
    assert analytics["risk"]["risk_sentiment"] is None
    assert analytics["risk"]["excluded_inputs_used"] is False


def test_readiness_counts_only_filtered_delivered_values() -> None:
    payload = build_senior_analyst_payload_v1(_load_snapshot_98(), now=FIXED_NOW)
    readiness = payload["readiness"]
    assert readiness["excluded_values_contribute"] is False
    assert readiness["delivered_value_counts"]["vix"] == 0
    assert "vix" in readiness["sections_unavailable"]
    assert readiness["coverage_ratio"] < 1.0


def test_every_omission_has_a_reason_and_provider_accounting_is_complete() -> None:
    payload = build_senior_analyst_payload_v1(_load_snapshot_98(), now=FIXED_NOW)
    assert payload["missing_data"]
    assert all(item["reason_code"] for item in payload["missing_data"])
    result = validate_senior_analyst_payload_v1(payload, now=FIXED_NOW)
    assert result["checks"]["unexplained_omissions"] == 0
    assert result["checks"]["provider_accounting_valid"] is True


def test_validator_rejects_temporally_expired_value_labeled_current() -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
        request_id="adversarial-expiry",
        request_refresh_mode="replay",
    )
    payload["analytics"]["macro"]["metrics"] = [
        {
            "series_id": "ICSA",
            "metric_id": "initial_jobless_claims",
            "value": 187000,
            "unit": "claims",
            "transformation": "level",
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "data_as_of": (FIXED_NOW - timedelta(days=1)).isoformat(),
            "content_valid_until": (FIXED_NOW - timedelta(seconds=1)).isoformat(),
            "snapshot_transport_valid_until": None,
            "refresh_due_at": None,
            "source": "FRED",
            "reason_code": None,
            "lineage": [],
        }
    ]
    result = validate_senior_analyst_payload_v1(payload, now=FIXED_NOW)
    assert result["status"] == "FAIL"
    assert result["checks"]["expired_values_delivered"] > 0


def test_live_gate_rejects_uncorrelated_provider_accounting() -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
        request_id="live-request",
        request_refresh_mode="force",
    )
    payload["provider_accounting"][0]["request_id"] = "different-request"
    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
        require_recent_response=True,
    )
    assert result["status"] == "FAIL"
    assert result["checks"]["provider_accounting_valid"] is False


def test_production_route_exposes_versioned_audience_on_force_capable_route() -> None:
    operation = app.openapi()["paths"]["/market-context/mnq"]["get"]
    parameters = {item["name"]: item for item in operation["parameters"]}
    assert "refresh" in parameters
    assert "audience" in parameters
    assert "senior_analyst_v1" in parameters["audience"]["schema"]["pattern"]


def test_route_projection_materializes_v1_without_replacing_legacy_storage() -> None:
    stored = {
        "debug_payload": _synthetic_sync(),
        "consumer_payload": {"contract": "ai_trader_market_context_consumer"},
    }
    legacy = _consumer_projection(
        stored,
        audience="legacy_v2",
        refresh="force",
        request_id="route-test",
    )
    senior = _consumer_projection(
        stored,
        audience="senior_analyst_v1",
        refresh="force",
        request_id="route-test",
    )
    assert legacy["contract"] == "ai_trader_market_context_consumer"
    assert senior["contract"] == "SeniorAnalystPayloadV1"
    assert senior["request"] == {
        "request_id": "route-test",
        "refresh_mode": "force",
        "same_request_provider_accounting": True,
    }


def test_second_projection_does_not_mutate_authoritative_input() -> None:
    source = _synthetic_sync()
    before = deepcopy(source)
    build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    assert source == before
