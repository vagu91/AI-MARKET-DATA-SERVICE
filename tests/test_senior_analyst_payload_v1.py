from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import app.services.provider_capability_registry as provider_registry
from app.main import app
from app.api.routes import _consumer_projection
from app.services.senior_analyst_projection_v1 import (
    INVALID_ANALYTIC_STATES,
    build_senior_analyst_payload_v1,
    validate_senior_analyst_payload_v1,
)


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_98_FIXTURE = (
    ROOT / "tests" / "fixtures" / "senior_analyst_snapshot98_compact.json"
)
DATA_SOURCE_MATRIX = (
    ROOT / "docs" / "baselines" / "senior-analyst-data-source-matrix.json"
)
SNAPSHOT_98_ORIGIN_SHA256 = (
    "3dc318ebdb7df82462f328ac9e623c46b2b3ea57b4238fde37544c0b859ee232"
)
SNAPSHOT_98_FIXTURE_PAYLOAD_SHA256 = (
    "6b9db5776477882109401abbba301014d3a3c15d44a741acdbfab020e9307d2e"
)
FIXED_NOW = datetime(2026, 7, 29, 18, 17, 25, tzinfo=UTC)
NEW_HOME_SALES_OCCURRENCE_ID = "xtb:146392:2026-07-24"
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
    fixture = json.loads(SNAPSHOT_98_FIXTURE.read_text(encoding="utf-8"))
    assert fixture["fixture_contract"] == (
        "SeniorAnalystSnapshot98RegressionFixture"
    )
    assert fixture["fixture_version"] == 1
    metadata = fixture["metadata"]
    assert metadata["origin"] == {
        "generated_at": "2026-07-29T18:17:25.927920+00:00",
        "path": (
            "data/live-ai-trader-capture-20260729T181632Z/"
            "senior-analyst-consumer-full.json"
        ),
        "sha256": SNAPSHOT_98_ORIGIN_SHA256,
        "size_bytes": 17_993_060,
        "snapshot_id": "mcs-d787d9a2-0e29-4636-9203-976d165008ab",
        "snapshot_revision": 98,
    }
    payload = fixture["payload"]
    canonical_payload = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    payload_sha256 = hashlib.sha256(canonical_payload).hexdigest()
    assert metadata["payload_sha256"] == SNAPSHOT_98_FIXTURE_PAYLOAD_SHA256
    assert payload_sha256 == SNAPSHOT_98_FIXTURE_PAYLOAD_SHA256
    assert payload["snapshot_revision"] == 98
    assert payload["generated_at"].startswith("2026-07-29T18:17:25")
    return payload


def test_snapshot_98_compact_fixture_is_versioned_and_bounded() -> None:
    source = _load_snapshot_98()
    assert SNAPSHOT_98_FIXTURE.stat().st_size < 50_000
    assert set(source["sections"]) == {
        "event_calendar",
        "macro",
        "market_internals",
        "nasdaq",
        "news",
        "options_positioning",
        "risk",
        "vix",
    }


def test_data_source_matrix_records_first_live_exact_body_digest() -> None:
    matrix = json.loads(DATA_SOURCE_MATRIX.read_text(encoding="utf-8"))
    capture = matrix["authoritative_capture"]

    assert capture == {
        "snapshot_revision": 99,
        "generated_at": "2026-07-30T11:57:24.569801+00:00",
        "run_id": "20260730T115633Z",
        "path": (
            "data/senior-analyst-live-validation/20260730T115633Z/"
            "response-body.json"
        ),
        "http_status": 200,
        "body_size_bytes": 116_469,
        "body_sha256": (
            "9a768dd051d5375177723d67ce3c60630b892505b12bb535e883100b7f184835"
        ),
        "body_sha256_scope": "EXACT_HTTP_RESPONSE_BODY_BYTES",
        "gate_status": "FAIL",
        "provider_accounting_valid": False,
    }
    assert len(matrix["datasets"]) == 25
    assert all(
        row["last_live_result"].startswith("RUN_20260730T115633Z:")
        for row in matrix["datasets"]
    )


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
    assert payload["readiness"]["status"] == "UNAVAILABLE"
    assert (
        payload["readiness"]["calculated_from_delivered_payload"]
        is True
    )
    assert payload["readiness"]["available_section_count"] == 0


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


def test_recent_official_read_does_not_prove_latest_monthly_release() -> None:
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
    assert metric["value"] is None
    assert metric["freshness"] == "UNAVAILABLE"
    assert metric["reason_code"] == "LATEST_OFFICIAL_RELEASE_NOT_PROVEN"
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
    assert by_metric["total_nonfarm_payroll_level"]["value"] is None
    assert by_metric["total_nonfarm_payroll_level"]["reason_code"] == (
        "CONTENT_VALIDITY_EXPIRED"
    )
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
        "actual_is_official": True,
        "actual_source": "BLS",
        "forecast": 0.1,
        "previous": 0.0,
        "field_lineage": {
            "actual": {
                "occurrence_id": "one",
                "metric_id": "headline_cpi_mom",
                "reference_period": "2026-06",
                "frequency": "MoM",
                "source": "BLS",
                "source_series_id": "CUSR0000SA0",
                "transformation": "pct_change_mom",
                "value": 0.2,
            },
            "forecast": {
                "occurrence_id": "two",
                "metric_id": "headline_cpi_mom",
                "reference_period": "2026-06",
                "frequency": "MoM",
                "source": "XTB Economic Calendar",
                "value": 0.1,
            },
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


def test_released_event_excludes_values_with_only_stale_field_lineage() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=5)
    occurrence_id = NEW_HOME_SALES_OCCURRENCE_ID
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": occurrence_id,
                "metric_id": "new_home_sales",
                "name": "New Home Sales",
                "release_at": release.isoformat(),
                "reference_period": "2026-06",
                "release_status": "RELEASED",
                "actual": 628.0,
                "actual_is_official": True,
                "actual_source": "FRED",
                "consensus": 610.0,
                "previous": 580.0,
                "freshness": "CURRENT_RELEASE",
                "content_valid_until": (
                    FIXED_NOW + timedelta(days=1)
                ).isoformat(),
                "refresh_due_at": (
                    FIXED_NOW + timedelta(hours=12)
                ).isoformat(),
                "source": "XTB Economic Calendar",
                "field_lineage": {
                    "actual": {
                        "source": "FRED",
                        "acquisition_provider": "FRED",
                        "source_url": (
                            "https://fred.stlouisfed.org/series/HSN1F"
                        ),
                        "source_domain": "fred.stlouisfed.org",
                        "occurrence_id": occurrence_id,
                        "metric_id": "new_home_sales",
                        "source_series_id": "HSN1F",
                        "transformation": "level",
                        "reference_period": "2026-06",
                        "frequency": "monthly",
                        "freshness": "CURRENT_RELEASE",
                        "value": 628.0,
                        "validation": {"status": "VERIFIED"},
                    },
                    "consensus": {
                        "source": "XTB Economic Calendar",
                        "retrieved_at": (
                            release - timedelta(days=2)
                        ).isoformat(),
                        "freshness": "STALE",
                        "value": 610.0,
                    },
                    "previous": {
                        "source": "XTB Economic Calendar",
                        "retrieved_at": (
                            release - timedelta(days=2)
                        ).isoformat(),
                        "freshness": "STALE",
                        "value": 580.0,
                    },
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] == 628.0
    assert event["actual_is_official"] is True
    assert event["actual_source"] == "FRED"
    assert event["consensus"] is None
    assert event["previous"] is None
    assert event["previous_revised"] is None
    assert event["surprise_absolute"] is None
    assert {
        item["field"]
        for item in event["lineage"]
    } == {"actual"}
    assert "_field_reason_codes" not in event
    assert not _walk_invalid_states(payload["analytics"])
    assert {
        (
            item["field"],
            item["reason_code"],
        )
        for item in payload["missing_data"]
        if occurrence_id in item["field"]
    } >= {
        (
            "calendar.latest_released_events."
            f"{occurrence_id}.consensus",
            "FIELD_LINEAGE_CONTENT_NOT_CURRENT",
        ),
        (
            "calendar.latest_released_events."
            f"{occurrence_id}.previous",
            "FIELD_LINEAGE_CONTENT_NOT_CURRENT",
        ),
    }
    assert validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )["checks"]["expired_values_delivered"] == 0
    assert validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )["checks"]["semantic_mapping_errors"] == 0


def _new_home_sales_payload(
    *,
    actual_source: str = "FRED",
    actual_is_official: bool = True,
    lineage_source: str = "FRED",
    source_series_id: str = "HSN1F",
    frequency: str = "monthly",
    transformation: str = "level",
    include_occurrence: bool = True,
    lineage_content_valid_until: str | None = None,
    include_validation: bool = True,
    include_freshness: bool = True,
    include_content_valid_until: bool = True,
    include_refresh_due_at: bool = True,
    acquisition_provider: str = "FRED",
    source_url: str = "https://fred.stlouisfed.org/series/HSN1F",
    source_domain: str = "fred.stlouisfed.org",
    include_acquisition_provider: bool = True,
    include_source_url: bool = True,
    include_source_domain: bool = True,
) -> dict:
    release = FIXED_NOW - timedelta(days=5)
    occurrence_id = NEW_HOME_SALES_OCCURRENCE_ID
    source = _synthetic_sync()
    event = {
                "occurrence_id": occurrence_id,
                "metric_id": "new_home_sales",
                "name": "New Home Sales",
                "release_at": release.isoformat(),
                "reference_period": "2026-06",
                "release_status": "RELEASED",
                "actual": 628.0,
                "actual_is_official": actual_is_official,
                "actual_source": actual_source,
                "source": "XTB Economic Calendar",
                "field_lineage": {
                    "actual": {
                        "field": "actual",
                        "source": lineage_source,
                        "occurrence_id": occurrence_id,
                        "metric_id": "new_home_sales",
                        "source_series_id": source_series_id,
                        "transformation": transformation,
                        "reference_period": "2026-06",
                        "frequency": frequency,
                        "value": 628.0,
                    }
                },
    }
    if include_freshness:
        event["freshness"] = "CURRENT_RELEASE"
        event["field_lineage"]["actual"]["freshness"] = (
            "CURRENT_RELEASE"
        )
    if include_content_valid_until:
        event["content_valid_until"] = (
            FIXED_NOW + timedelta(days=1)
        ).isoformat()
    if include_refresh_due_at:
        event["refresh_due_at"] = (
            FIXED_NOW + timedelta(hours=12)
        ).isoformat()
    if include_validation:
        event["field_lineage"]["actual"]["validation"] = {
            "status": "VERIFIED"
        }
    if include_acquisition_provider:
        event["field_lineage"]["actual"][
            "acquisition_provider"
        ] = acquisition_provider
    if include_source_url:
        event["field_lineage"]["actual"]["source_url"] = source_url
    if include_source_domain:
        event["field_lineage"]["actual"][
            "source_domain"
        ] = source_domain
    if not include_occurrence:
        event.pop("occurrence_id")
    if lineage_content_valid_until is not None:
        event["field_lineage"]["actual"][
            "content_valid_until"
        ] = lineage_content_valid_until
    source["sections"]["event_calendar"] = {
        "recently_released_events": [event]
    }
    return build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )


def _source_bound_calendar_event(provider_id: str) -> dict:
    cases = {
        "FRED": {
            "metric_id": "new_home_sales",
            "name": "New Home Sales",
            "reference_period": "2026-06",
            "actual": 628.0,
            "actual_source": "FRED",
            "actual_is_official": True,
            "lineage_source": "FRED",
            "source_series_id": "HSN1F",
            "transformation": "level",
            "source_url": (
                "https://fred.stlouisfed.org/series/HSN1F"
            ),
            "source_domain": "fred.stlouisfed.org",
        },
        "BEA": {
            "metric_id": "headline_pce_yoy",
            "name": "PCE A/A",
            "reference_period": "2026-06",
            "actual": 2.8,
            "actual_source": "BEA",
            "actual_is_official": True,
            "lineage_source": "BEA",
            "source_series_id": "BEA:PCE_PRICE_INDEX",
            "transformation": "pct_change_yoy",
            "source_url": (
                "https://www.bea.gov/data/consumer-spending/main"
            ),
            "source_domain": "bea.gov",
        },
        "SPGLOBAL": {
            "metric_id": "flash_services_pmi",
            "name": "Flash Services PMI",
            "reference_period": "2026-07",
            "actual": 53.6,
            "actual_source": "S&P Global",
            "actual_is_official": True,
            "lineage_source": "S&P Global",
            "source_series_id": (
                "SPGLOBAL:US:FLASH_SERVICES_PMI"
            ),
            "transformation": "level",
            "source_url": (
                "https://www.pmi.spglobal.com/"
                "Public/Home/PressRelease"
            ),
            "source_domain": "pmi.spglobal.com",
        },
        "INVESTING_EVENT_1062": {
            "metric_id": "flash_services_pmi",
            "name": "Flash Services PMI",
            "reference_period": "2026-07",
            "actual": 53.6,
            "actual_source": "INVESTING_EVENT_1062",
            "actual_is_official": False,
            "lineage_source": "S&P Global",
            "source_series_id": (
                "SPGLOBAL:US:FLASH_SERVICES_PMI"
            ),
            "transformation": "level",
            "source_url": (
                "https://endpoints.investing.com/"
                "economic-calendar-service/api/calendar/events/1062"
            ),
            "canonical_url": (
                "https://www.pmi.spglobal.com/"
                "Public/Home/PressRelease"
            ),
            "source_domain": "endpoints.investing.com",
        },
    }
    case = cases[provider_id]
    occurrence_id = f"source-binding:{provider_id.lower()}"
    return {
        "occurrence_id": occurrence_id,
        "metric_id": case["metric_id"],
        "name": case["name"],
        "release_at": (FIXED_NOW - timedelta(days=1)).isoformat(),
        "reference_period": case["reference_period"],
        "release_status": "RELEASED",
        "actual": case["actual"],
        "actual_source": case["actual_source"],
        "actual_is_official": case["actual_is_official"],
        "freshness": "CURRENT_RELEASE",
        "content_valid_until": (
            FIXED_NOW + timedelta(days=1)
        ).isoformat(),
        "refresh_due_at": (
            FIXED_NOW + timedelta(hours=12)
        ).isoformat(),
        "source": "Canonical Economic Calendar",
        "field_lineage": {
            "actual": {
                "field": "actual",
                "occurrence_id": occurrence_id,
                "metric_id": case["metric_id"],
                "reference_period": case["reference_period"],
                "frequency": "monthly",
                "source": case["lineage_source"],
                "acquisition_provider": provider_id,
                "source_series_id": case["source_series_id"],
                "transformation": case["transformation"],
                "source_url": case["source_url"],
                **(
                    {"canonical_url": case["canonical_url"]}
                    if case.get("canonical_url")
                    else {}
                ),
                "source_domain": case["source_domain"],
                "freshness": "CURRENT_RELEASE",
                "value": case["actual"],
                "validation": {"status": "VERIFIED"},
            }
        },
    }


def _project_calendar_event(raw_event: dict) -> dict:
    source = _synthetic_sync()
    source["sections"]["event_calendar"] = {
        "recently_released_events": [raw_event]
    }
    return build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )


@pytest.mark.parametrize(
    "provider_id",
    ("FRED", "BEA", "SPGLOBAL", "INVESTING_EVENT_1062"),
)
@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        ("missing_url", "FIELD_LINEAGE_SOURCE_URL_NOT_PROVEN"),
        ("http_url", "FIELD_LINEAGE_SOURCE_URL_MISMATCH"),
        ("foreign_host", "FIELD_LINEAGE_SOURCE_URL_MISMATCH"),
        (
            "foreign_domain",
            "FIELD_LINEAGE_SOURCE_DOMAIN_MISMATCH",
        ),
    ),
)
def test_calendar_actual_source_binding_is_fail_closed_in_projection_and_validator(
    provider_id: str,
    mutation: str,
    expected_reason: str,
) -> None:
    raw_event = _source_bound_calendar_event(provider_id)
    baseline = _project_calendar_event(deepcopy(raw_event))
    baseline_event = baseline["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert baseline_event["actual"] == raw_event["actual"]

    raw_lineage = raw_event["field_lineage"]["actual"]
    if mutation == "missing_url":
        raw_lineage.pop("source_url")
    elif mutation == "http_url":
        raw_lineage["source_url"] = raw_lineage[
            "source_url"
        ].replace("https://", "http://", 1)
    elif mutation == "foreign_host":
        raw_lineage["source_url"] = "https://evil.example/release"
    else:
        raw_lineage["source_domain"] = "evil.example"

    projected = _project_calendar_event(raw_event)
    projected_event = projected["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert projected_event["actual"] is None
    assert projected_event["reason_code"] == expected_reason

    validator_event = baseline_event
    validator_lineage = validator_event["lineage"][0]
    if mutation == "missing_url":
        validator_lineage.pop("source_url")
    elif mutation == "http_url":
        validator_lineage["source_url"] = validator_lineage[
            "source_url"
        ].replace("https://", "http://", 1)
    elif mutation == "foreign_host":
        validator_lineage["source_url"] = "https://evil.example/release"
    else:
        validator_lineage["source_domain"] = "evil.example"
    result = validate_senior_analyst_payload_v1(
        baseline,
        now=FIXED_NOW,
    )
    assert result["checks"]["semantic_mapping_errors"] == 1


@pytest.mark.parametrize(
    "provider_id",
    ("FRED", "BEA", "SPGLOBAL", "INVESTING_EVENT_1062"),
)
def test_calendar_actual_source_domain_is_derived_from_registered_url(
    provider_id: str,
) -> None:
    raw_event = _source_bound_calendar_event(provider_id)
    raw_event["field_lineage"]["actual"].pop("source_domain")

    projected = _project_calendar_event(raw_event)
    event = projected["analytics"]["calendar"][
        "latest_released_events"
    ][0]

    assert event["actual"] == raw_event["actual"]
    result = validate_senior_analyst_payload_v1(
        projected,
        now=FIXED_NOW,
    )
    assert result["checks"]["semantic_mapping_errors"] == 0


def test_investing_fallback_binds_originator_separately_from_acquisition() -> None:
    raw_event = _source_bound_calendar_event("INVESTING_EVENT_1062")
    baseline = _project_calendar_event(deepcopy(raw_event))
    assert baseline["analytics"]["calendar"][
        "latest_released_events"
    ][0]["actual"] == 53.6

    raw_event["field_lineage"]["actual"]["source"] = "Investing.com"
    projected = _project_calendar_event(raw_event)
    event = projected["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert event["actual"] is None
    assert event["reason_code"] == (
        "FIELD_LINEAGE_SOURCE_PROVIDER_MISMATCH"
    )

    validator_event = baseline["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    validator_event["lineage"][0]["source"] = "Investing.com"
    result = validate_senior_analyst_payload_v1(
        baseline,
        now=FIXED_NOW,
    )
    assert result["checks"]["semantic_mapping_errors"] == 1


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        (
            "missing_originator_url",
            "FIELD_LINEAGE_ORIGINATOR_URL_NOT_PROVEN",
        ),
        (
            "foreign_originator_url",
            "FIELD_LINEAGE_ORIGINATOR_URL_MISMATCH",
        ),
    ),
)
def test_investing_fallback_requires_registered_originator_url(
    mutation: str,
    expected_reason: str,
) -> None:
    raw_event = _source_bound_calendar_event("INVESTING_EVENT_1062")
    baseline = _project_calendar_event(deepcopy(raw_event))
    baseline_event = baseline["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert baseline_event["actual"] == 53.6

    raw_lineage = raw_event["field_lineage"]["actual"]
    if mutation == "missing_originator_url":
        raw_lineage.pop("canonical_url")
    else:
        raw_lineage["canonical_url"] = "https://evil.example/release"
    projected = _project_calendar_event(raw_event)
    projected_event = projected["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert projected_event["actual"] is None
    assert projected_event["reason_code"] == expected_reason

    validator_lineage = baseline_event["lineage"][0]
    if mutation == "missing_originator_url":
        validator_lineage.pop("canonical_url")
    else:
        validator_lineage["canonical_url"] = (
            "https://evil.example/release"
        )
    result = validate_senior_analyst_payload_v1(
        baseline,
        now=FIXED_NOW,
    )
    assert result["checks"]["semantic_mapping_errors"] == 1


def test_field_lineage_value_key_must_match_the_declared_field() -> None:
    raw_event = _source_bound_calendar_event("FRED")
    raw_lineage = raw_event["field_lineage"]["actual"]
    raw_lineage["previous"] = raw_lineage.pop("value")
    projected = _project_calendar_event(raw_event)
    event = projected["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert event["actual"] is None
    assert event["reason_code"] == "FIELD_LINEAGE_VALUE_NOT_PROVEN"

    payload = _project_calendar_event(
        _source_bound_calendar_event("FRED")
    )
    validator_lineage = payload["analytics"]["calendar"][
        "latest_released_events"
    ][0]["lineage"][0]
    validator_lineage["previous"] = validator_lineage.pop("value")
    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )
    assert result["checks"]["semantic_mapping_errors"] == 1


@pytest.mark.parametrize("target", ("event", "lineage"))
@pytest.mark.parametrize(
    "state_field",
    ("status", "lifecycle_status", "freshness", "freshness_state"),
)
@pytest.mark.parametrize("state", ("STALE", "EXPIRED"))
def test_nested_lifecycle_aliases_are_fail_closed_in_projection_and_validator(
    target: str,
    state_field: str,
    state: str,
) -> None:
    raw_event = _source_bound_calendar_event("FRED")
    raw_target = (
        raw_event
        if target == "event"
        else raw_event["field_lineage"]["actual"]
    )
    raw_target["lifecycle"] = {state_field: state}
    projected = _project_calendar_event(raw_event)
    event = projected["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert event["actual"] is None
    assert event["reason_code"] == "FIELD_LINEAGE_CONTENT_NOT_CURRENT"

    payload = _project_calendar_event(
        _source_bound_calendar_event("FRED")
    )
    validator_event = payload["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    validator_target = (
        validator_event
        if target == "event"
        else validator_event["lineage"][0]
    )
    validator_target["lifecycle"] = {state_field: state}
    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )
    assert result["checks"]["semantic_mapping_errors"] == 1


@pytest.mark.parametrize(
    ("provider_id", "contradictory_name"),
    (
        ("FRED", "PCE A/A"),
        ("BEA", "New Home Sales"),
        ("SPGLOBAL", "New Home Sales"),
        ("INVESTING_EVENT_1062", "PCE A/A"),
    ),
)
def test_calendar_metric_family_mismatch_is_fail_closed_in_projection_and_validator(
    provider_id: str,
    contradictory_name: str,
) -> None:
    raw_event = _source_bound_calendar_event(provider_id)
    baseline = _project_calendar_event(deepcopy(raw_event))
    raw_event["name"] = contradictory_name
    projected = _project_calendar_event(raw_event)
    event = projected["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert event["actual"] is None
    assert event["reason_code"] == "EVENT_METRIC_FAMILY_MISMATCH"
    assert validate_senior_analyst_payload_v1(
        projected,
        now=FIXED_NOW,
    )["checks"]["semantic_mapping_errors"] == 1

    validator_event = baseline["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    validator_event["name"] = contradictory_name
    result = validate_senior_analyst_payload_v1(
        baseline,
        now=FIXED_NOW,
    )
    assert result["checks"]["semantic_mapping_errors"] == 1


@pytest.mark.parametrize(
    ("provider_id", "localized_name"),
    (
        ("FRED", "Vendita case nuove"),
        ("SPGLOBAL", "Indice PMI dei servizi"),
        ("INVESTING_EVENT_1062", "Indice PMI dei servizi"),
    ),
)
def test_registered_localized_event_family_aliases_remain_valid(
    provider_id: str,
    localized_name: str,
) -> None:
    raw_event = _source_bound_calendar_event(provider_id)
    raw_event["name"] = localized_name
    payload = _project_calendar_event(raw_event)
    event = payload["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert event["actual"] == raw_event["actual"]
    assert event["reason_code"] is None
    assert validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )["checks"]["semantic_mapping_errors"] == 0


@pytest.mark.parametrize(
    ("values", "expected_reason"),
    [
        (
            {"source_series_id": "BOGUS_SERIES"},
            "FIELD_LINEAGE_SOURCE_SERIES_MISMATCH",
        ),
        (
            {
                "actual_source": "CENSUS",
                "lineage_source": "CENSUS",
            },
            "FIELD_LINEAGE_SOURCE_PROVIDER_MISMATCH",
        ),
        (
            {"frequency": "daily"},
            "FIELD_LINEAGE_FREQUENCY_MISMATCH",
        ),
        (
            {"transformation": "pct_change_mom"},
            "FIELD_LINEAGE_TRANSFORMATION_MISMATCH",
        ),
        (
            {"actual_is_official": False},
            "ACTUAL_OFFICIAL_STATUS_NOT_PROVEN",
        ),
        (
            {"include_occurrence": False},
            "EVENT_OCCURRENCE_NOT_PROVEN",
        ),
        (
            {
                "lineage_content_valid_until": (
                    FIXED_NOW.isoformat()
                )
            },
            "FIELD_LINEAGE_CONTENT_VALIDITY_EXPIRED",
        ),
        (
            {"include_validation": False},
            "FIELD_LINEAGE_VALIDATION_NOT_PROVEN",
        ),
        (
            {"include_freshness": False},
            "FIELD_LINEAGE_FRESHNESS_NOT_PROVEN",
        ),
        (
            {"include_content_valid_until": False},
            "FIELD_LINEAGE_CONTENT_VALIDITY_NOT_PROVEN",
        ),
        (
            {"include_refresh_due_at": False},
            "FIELD_LINEAGE_REFRESH_DUE_NOT_PROVEN",
        ),
        (
            {"include_source_url": False},
            "FIELD_LINEAGE_SOURCE_URL_NOT_PROVEN",
        ),
        (
            {"source_url": "http://fred.stlouisfed.org/series/HSN1F"},
            "FIELD_LINEAGE_SOURCE_URL_MISMATCH",
        ),
        (
            {"source_url": "https://evil.example/series/HSN1F"},
            "FIELD_LINEAGE_SOURCE_URL_MISMATCH",
        ),
        (
            {"source_domain": "evil.example"},
            "FIELD_LINEAGE_SOURCE_DOMAIN_MISMATCH",
        ),
    ],
)
def test_new_home_sales_projection_requires_exact_official_binding(
    values: dict,
    expected_reason: str,
) -> None:
    payload = _new_home_sales_payload(**values)
    event = payload["analytics"]["calendar"][
        "latest_released_events"
    ][0]

    assert event["actual"] is None
    assert event["actual_is_official"] is None
    assert event["actual_source"] is None
    assert event["lineage"] == []
    assert event["reason_code"] == expected_reason


@pytest.mark.parametrize(
    "mutate",
    [
        lambda event: event["lineage"][0].update(
            {"source_series_id": "BOGUS_SERIES"}
        ),
        lambda event: (
            event.update({"actual_source": "CENSUS"}),
            event["lineage"][0].update({"source": "CENSUS"}),
        ),
        lambda event: event["lineage"][0].update(
            {"frequency": "daily"}
        ),
        lambda event: event["lineage"][0].update(
            {"transformation": "pct_change_mom"}
        ),
        lambda event: event.update(
            {"actual_is_official": False}
        ),
        lambda event: event.pop("occurrence_id"),
        lambda event: event["lineage"][0].update(
            {"content_valid_until": FIXED_NOW.isoformat()}
        ),
        lambda event: event["lineage"][0].pop("validation"),
        lambda event: event["lineage"][0].pop("freshness"),
        lambda event: event["lineage"][0].pop(
            "content_valid_until"
        ),
        lambda event: event["lineage"][0].pop("refresh_due_at"),
        lambda event: event["lineage"][0].pop("source_url"),
        lambda event: event["lineage"][0].update(
            {"source_url": "http://fred.stlouisfed.org/series/HSN1F"}
        ),
        lambda event: event["lineage"][0].update(
            {"source_url": "https://evil.example/series/HSN1F"}
        ),
        lambda event: event["lineage"][0].update(
            {"source_domain": "evil.example"}
        ),
    ],
)
def test_validator_rejects_mutated_new_home_sales_official_binding(
    mutate,
) -> None:
    payload = _new_home_sales_payload()
    event = payload["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    assert event["actual"] == 628.0
    mutate(event)

    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )

    assert result["checks"]["semantic_mapping_errors"] == 1
    assert result["status"] == "FAIL"


def test_new_home_previous_with_wrong_frequency_is_fail_closed() -> None:
    baseline = _new_home_sales_payload()
    raw_event = deepcopy(
        baseline["analytics"]["calendar"][
            "latest_released_events"
        ][0]
    )
    raw_event["previous"] = 600.0
    raw_event["lineage"].append(
        {
            "field": "previous",
            "occurrence_id": raw_event["occurrence_id"],
            "metric_id": "new_home_sales",
            "reference_period": "2026-05",
            "frequency": "daily",
            "transformation": "pct_change_yoy",
            "source": "XTB Economic Calendar",
            "value": 600.0,
            "validation": {"status": "VERIFIED"},
        }
    )
    source = _synthetic_sync()
    source["sections"]["event_calendar"] = {
        "recently_released_events": [raw_event]
    }

    projected = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )
    event = projected["analytics"]["calendar"][
        "latest_released_events"
    ][0]

    assert event["actual"] == 628.0
    assert event["previous"] is None
    assert {
        item["field"] for item in event["lineage"]
    } == {"actual"}
    assert event["reason_code"] == (
        "FIELD_LINEAGE_FREQUENCY_MISMATCH"
    )


def test_validator_rejects_delivered_previous_with_wrong_frequency() -> None:
    payload = _new_home_sales_payload()
    event = payload["analytics"]["calendar"][
        "latest_released_events"
    ][0]
    event["previous"] = 600.0
    event["lineage"].append(
        {
            "field": "previous",
            "occurrence_id": event["occurrence_id"],
            "metric_id": "new_home_sales",
            "reference_period": "2026-05",
            "frequency": "daily",
            "transformation": "pct_change_yoy",
            "source": "XTB Economic Calendar",
            "value": 600.0,
            "validation": {"status": "VERIFIED"},
        }
    )

    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )

    assert result["checks"]["semantic_mapping_errors"] == 1
    assert result["status"] == "FAIL"


def test_live_pce_yoy_mom_mismatch_is_fail_closed() -> None:
    now = datetime(2026, 7, 30, 18, 20, tzinfo=UTC)
    occurrence_id = "xtb:145296:2026-07-30"
    source = _synthetic_sync()
    source["generated_at"] = now.isoformat()
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": occurrence_id,
                "metric_id": "headline_pce_mom",
                "name": "PCE A/A",
                "release_at": "2026-07-30T12:30:00+00:00",
                "reference_period": "2026-06",
                "release_status": "RELEASED",
                "actual": -0.1,
                "consensus": None,
                "previous": 4.1,
                "freshness": None,
                "actual_is_official": None,
                "actual_source": None,
                "lineage": [
                    {
                        "field": "value",
                        "source": {
                            "publisher": "XTB Economic Calendar",
                        },
                    }
                ],
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=now)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["occurrence_id"] == occurrence_id
    assert event["metric_id"] == "headline_pce_yoy"
    assert event["name"] == "PCE A/A"
    assert event["actual"] is None
    assert event["consensus"] is None
    assert event["previous"] is None
    assert event["previous_revised"] is None
    assert event["actual_is_official"] is None
    assert event["actual_source"] is None
    assert event["freshness"] is None
    assert event["lineage"] == []
    assert event["reason_code"] == "EVENT_METRIC_FREQUENCY_MISMATCH"
    assert {
        (item["field"], item["reason_code"])
        for item in payload["missing_data"]
        if occurrence_id in item["field"]
    } >= {
        (
            f"calendar.latest_released_events.{occurrence_id}.actual",
            "EVENT_METRIC_FREQUENCY_MISMATCH",
        ),
        (
            f"calendar.latest_released_events.{occurrence_id}.previous",
            "EVENT_METRIC_FREQUENCY_MISMATCH",
        ),
    }
    assert validate_senior_analyst_payload_v1(
        payload,
        now=now,
    )["checks"]["semantic_mapping_errors"] == 0


def test_pce_without_metric_id_cannot_bypass_official_lineage_gate() -> None:
    now = datetime(2026, 7, 30, 18, 20, tzinfo=UTC)
    occurrence_id = "xtb:pce-without-metric:2026-07-30"
    source = _synthetic_sync()
    source["generated_at"] = now.isoformat()
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": occurrence_id,
                "name": "PCE A/A",
                "release_at": "2026-07-30T12:30:00+00:00",
                "reference_period": "2026-06",
                "release_status": "RELEASED",
                "actual": 2.8,
                "previous": 2.7,
                "actual_is_official": None,
                "actual_source": None,
                "lineage": [],
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=now)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["metric_id"] == "headline_pce_yoy"
    assert event["actual"] is None
    assert event["previous"] is None
    assert event["lineage"] == []
    assert (
        event["reason_code"]
        == "FIELD_SPECIFIC_LINEAGE_NOT_AVAILABLE"
    )
    assert validate_senior_analyst_payload_v1(
        payload,
        now=now,
    )["checks"]["semantic_mapping_errors"] == 0


def test_validator_rejects_delivered_pce_values_without_metric_id() -> None:
    now = datetime(2026, 7, 30, 18, 20, tzinfo=UTC)
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=now,
    )
    observed = {
        "occurrence_id": "xtb:pce-without-metric:2026-07-30",
        "name": "PCE A/A",
        "release_at": "2026-07-30T12:30:00+00:00",
        "reference_period": "2026-06",
        "release_status": "RELEASED",
        "actual": 2.8,
        "consensus": None,
        "previous": 2.7,
        "previous_revised": None,
        "freshness": None,
        "actual_is_official": None,
        "actual_source": None,
        "lineage": [],
    }
    payload["analytics"]["calendar"]["latest_released_events"] = [
        observed
    ]
    payload["analytics"]["calendar"]["active_event_windows"] = [
        deepcopy(observed)
    ]

    result = validate_senior_analyst_payload_v1(payload, now=now)

    assert result["checks"]["semantic_mapping_errors"] == 1
    assert result["status"] == "FAIL"


def test_validator_counts_live_pce_semantic_error_once_per_occurrence() -> None:
    now = datetime(2026, 7, 30, 18, 20, tzinfo=UTC)
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=now,
    )
    observed = {
        "occurrence_id": "xtb:145296:2026-07-30",
        "metric_id": "headline_pce_mom",
        "name": "PCE A/A",
        "release_at": "2026-07-30T12:30:00+00:00",
        "reference_period": "2026-06",
        "release_status": "RELEASED",
        "actual": -0.1,
        "consensus": None,
        "previous": 4.1,
        "previous_revised": None,
        "freshness": None,
        "actual_is_official": None,
        "actual_source": None,
        "lineage": [],
    }
    payload["analytics"]["calendar"]["latest_released_events"] = [
        observed
    ]
    payload["analytics"]["calendar"]["active_event_windows"] = [
        deepcopy(observed)
    ]

    result = validate_senior_analyst_payload_v1(payload, now=now)

    assert result["checks"]["semantic_mapping_errors"] == 1
    assert result["status"] == "FAIL"


@pytest.mark.parametrize(
    ("name", "metric_id", "lineage", "expected_errors"),
    [
        (
            "PCE A/A",
            "headline_pce_yoy",
            [
                {
                    "field": "actual",
                    "occurrence_id": "pce-proof",
                    "metric_id": "headline_pce_yoy",
                    "reference_period": "2026-06",
                    "frequency": "monthly",
                    "source": "BEA",
                    "source_series_id": "BEA:PCE_PRICE_INDEX",
                    "transformation": "pct_change_yoy",
                    "value": 2.8,
                    "validation": {"status": "VERIFIED"},
                }
            ],
            0,
        ),
        (
            "PCE A/A",
            "headline_pce_mom",
            [
                {
                    "field": "actual",
                    "occurrence_id": "pce-proof",
                    "metric_id": "headline_pce_mom",
                    "reference_period": "2026-06",
                    "frequency": "monthly",
                    "source": "BEA",
                    "source_series_id": "BEA:PCE_PRICE_INDEX",
                    "transformation": "pct_change_mom",
                    "value": 0.2,
                    "validation": {"status": "VERIFIED"},
                }
            ],
            1,
        ),
        (
            "PCE M/M",
            "headline_pce_mom",
            [
                {
                    "field": "value",
                    "source": "XTB Economic Calendar",
                }
            ],
            1,
        ),
        (
            "Core PCE A/A",
            "headline_pce_yoy",
            [
                {
                    "field": "actual",
                    "occurrence_id": "pce-proof",
                    "metric_id": "headline_pce_yoy",
                    "reference_period": "2026-06",
                    "frequency": "YoY",
                    "source": "BEA",
                    "source_series_id": "BEA:PCE_PRICE_INDEX",
                    "transformation": "pct_change_yoy",
                    "value": 2.8,
                    "validation": {"status": "VERIFIED"},
                }
            ],
            1,
        ),
        (
            "PCE A/A and M/M",
            "headline_pce_mom",
            [
                {
                    "field": "actual",
                    "occurrence_id": "pce-proof",
                    "metric_id": "headline_pce_mom",
                    "reference_period": "2026-06",
                    "frequency": "MoM",
                    "source": "BEA",
                    "source_series_id": "BEA:PCE_PRICE_INDEX",
                    "transformation": "pct_change_mom",
                    "value": 0.2,
                    "validation": {"status": "VERIFIED"},
                }
            ],
            1,
        ),
    ],
)
def test_validator_requires_pce_metric_and_field_specific_proof(
    name: str,
    metric_id: str,
    lineage: list[dict],
    expected_errors: int,
) -> None:
    now = datetime(2026, 7, 30, 18, 20, tzinfo=UTC)
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=now,
    )
    actual = 2.8 if metric_id.endswith("_yoy") else 0.2
    for item in lineage:
        item.setdefault("freshness", "CURRENT_RELEASE")
        item.setdefault(
            "content_valid_until",
            (now + timedelta(days=1)).isoformat(),
        )
        item.setdefault(
            "refresh_due_at",
            (now + timedelta(hours=12)).isoformat(),
        )
        if item.get("field") == "actual":
            item.setdefault("acquisition_provider", "BEA")
            item.setdefault(
                "source_url",
                "https://www.bea.gov/data/consumer-spending/main",
            )
            item.setdefault("source_domain", "bea.gov")
    payload["analytics"]["calendar"]["latest_released_events"] = [
        {
            "occurrence_id": "pce-proof",
            "metric_id": metric_id,
            "name": name,
            "release_at": "2026-07-30T12:30:00+00:00",
            "reference_period": "2026-06",
            "release_status": "RELEASED",
            "actual": actual,
            "consensus": None,
            "previous": None,
            "previous_revised": None,
            "freshness": "CURRENT_RELEASE",
            "actual_is_official": True,
            "actual_source": "BEA",
            "lineage": lineage,
        }
    ]

    result = validate_senior_analyst_payload_v1(payload, now=now)

    assert (
        result["checks"]["semantic_mapping_errors"]
        == expected_errors
    )


def test_projection_uses_evaluation_method_even_with_monthly_frequency() -> None:
    now = datetime(2026, 7, 30, 18, 20, tzinfo=UTC)
    source = _synthetic_sync()
    source["generated_at"] = now.isoformat()
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "pce-evaluation-method",
                "metric_id": "headline_pce_mom",
                "name": "PCE",
                "frequency": "monthly",
                "evaluation_method": "A/A",
                "release_at": "2026-07-30T12:30:00+00:00",
                "reference_period": "2026-06",
                "release_status": "RELEASED",
                "actual": -0.1,
                "previous": 4.1,
                "lineage": [
                    {
                        "field": "value",
                        "source": "XTB Economic Calendar",
                    }
                ],
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=now)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["metric_id"] == "headline_pce_yoy"
    assert event["actual"] is None
    assert event["previous"] is None
    assert event["reason_code"] == "EVENT_METRIC_FREQUENCY_MISMATCH"


@pytest.mark.parametrize(
    ("lineage_updates", "expected_errors"),
    [
        ({}, 0),
        ({"reference_period": "2020-01"}, 1),
        ({"frequency": "monthly"}, 1),
        ({"provider_occurrence_id": "foreign-occurrence"}, 1),
    ],
)
def test_validator_requires_exact_previous_occurrence_period_and_basis(
    lineage_updates: dict,
    expected_errors: int,
) -> None:
    now = datetime(2026, 7, 30, 18, 20, tzinfo=UTC)
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=now,
    )
    previous_lineage = {
        "field": "previous",
        "occurrence_id": "pce-previous-proof",
        "metric_id": "headline_pce_yoy",
        "reference_period": "2026-05",
        "frequency": "YoY",
        "source": "XTB Economic Calendar",
        "value": 2.7,
        "validation": {"status": "VERIFIED"},
        "freshness": "CURRENT_RELEASE",
        "content_valid_until": (
            now + timedelta(days=1)
        ).isoformat(),
        "refresh_due_at": (
            now + timedelta(hours=12)
        ).isoformat(),
        **lineage_updates,
    }
    payload["analytics"]["calendar"]["latest_released_events"] = [
        {
            "occurrence_id": "pce-previous-proof",
            "metric_id": "headline_pce_yoy",
            "name": "PCE A/A",
            "release_at": "2026-07-30T12:30:00+00:00",
            "reference_period": "2026-06",
            "release_status": "RELEASED",
            "actual": None,
            "consensus": None,
            "previous": 2.7,
            "previous_revised": None,
            "freshness": "CURRENT_RELEASE",
            "actual_is_official": None,
            "actual_source": None,
            "lineage": [previous_lineage],
        }
    ]

    result = validate_senior_analyst_payload_v1(payload, now=now)

    assert (
        result["checks"]["semantic_mapping_errors"]
        == expected_errors
    )


def test_released_event_excludes_field_with_expired_lineage_deadline() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=2)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "calendar:released:expired-consensus",
                "metric_id": "consumer_confidence",
                "name": "Consumer Confidence",
                "release_at": release.isoformat(),
                "reference_period": "2026-07",
                "release_status": "RELEASED",
                "actual": 97.2,
                "consensus": 96.0,
                "previous_revised": 95.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "consensus": {
                        "source": "Market Calendar",
                        "freshness": "CURRENT",
                        "content_valid_until": (
                            FIXED_NOW + timedelta(hours=1)
                        ).isoformat(),
                        "valid_until": (
                            FIXED_NOW - timedelta(seconds=1)
                        ).isoformat(),
                        "value": 96.0,
                    },
                    "previous_revised": {
                        "source": "Market Calendar",
                        "freshness": "CURRENT",
                        "valid_until": (
                            FIXED_NOW - timedelta(seconds=1)
                        ).isoformat(),
                        "value": 95.0,
                    }
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] == 97.2
    assert event["consensus"] is None
    assert event["previous_revised"] is None
    assert event["surprise_absolute"] is None
    assert event["lineage"] == []
    assert any(
        item["field"].endswith(".consensus")
        and item["reason_code"]
        == "FIELD_LINEAGE_CONTENT_VALIDITY_EXPIRED"
        for item in payload["missing_data"]
    )
    assert any(
        item["field"].endswith(".previous_revised")
        and item["reason_code"]
        == "FIELD_LINEAGE_CONTENT_VALIDITY_EXPIRED"
        for item in payload["missing_data"]
    )
    assert validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )["checks"]["expired_values_delivered"] == 0


def test_event_lineage_aliases_cannot_validate_a_different_raw_field() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 628.0,
                "consensus": 610.0,
                "forecast": 620.0,
                "previous": 580.0,
                "previous_revised": 590.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "occurrence_id": "occurrence-a",
                        "freshness": "CURRENT_RELEASE",
                    },
                    "consensus": {
                        "occurrence_id": "occurrence-b",
                        "freshness": "STALE",
                    },
                    "forecast": {
                        "occurrence_id": "occurrence-a",
                        "freshness": "CURRENT_RELEASE",
                    },
                    "previous": {"freshness": "STALE"},
                    "previous_revised": {
                        "occurrence_id": "occurrence-a",
                        "freshness": "CURRENT_RELEASE",
                    },
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] == 628.0
    assert event["consensus"] is None
    assert event["previous"] is None
    assert event["previous_revised"] == 590.0
    assert event["surprise_absolute"] is None
    assert event["reason_code"] is None
    assert {
        item["field"]
        for item in event["lineage"]
    } == {"actual", "previous_revised"}
    assert not _walk_invalid_states(payload["analytics"])


def test_alias_lineage_requires_the_selected_raw_value_to_match() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 100.0,
                "consensus": 90.0,
                "forecast": 80.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "occurrence_id": "occurrence-a",
                        "freshness": "CURRENT_RELEASE",
                        "value": 100.0,
                    },
                    "forecast": {
                        "occurrence_id": "occurrence-a",
                        "freshness": "CURRENT_RELEASE",
                        "value": 80.0,
                    },
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] == 100.0
    assert event["consensus"] is None
    assert event["surprise_absolute"] is None
    assert [item["field"] for item in event["lineage"]] == ["actual"]
    assert any(
        item["field"].endswith(".consensus")
        and item["reason_code"]
        == "FIELD_LINEAGE_VALUE_NOT_RECONCILED"
        for item in payload["missing_data"]
    )


def test_mixed_lineage_cannot_launder_the_selected_stale_value() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 100.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": [
                    {
                        "field": "actual",
                        "occurrence_id": "occurrence-a",
                        "freshness": "CURRENT",
                        "value": 90.0,
                    },
                    {
                        "field": "actual",
                        "occurrence_id": "occurrence-a",
                        "freshness": "STALE",
                        "value": 100.0,
                    },
                ],
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] is None
    assert event["reason_code"] == "FIELD_LINEAGE_CONTENT_NOT_CURRENT"
    assert event["lineage"] == []
    assert any(
        item["field"].endswith(".actual")
        and item["reason_code"] == "FIELD_LINEAGE_CONTENT_NOT_CURRENT"
        for item in payload["missing_data"]
    )


def test_generic_stale_event_lineage_nulls_every_unproven_value() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW + timedelta(minutes=30)
    source["sections"]["event_calendar"] = {
        "next_24h_events": [
            {
                "occurrence_id": "calendar:generic-stale",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "SCHEDULED",
                "actual": 97.2,
                "consensus": 96.0,
                "previous": 95.5,
                "freshness": "CURRENT",
                "lineage": [
                    {
                        "field": "value",
                        "freshness": "STALE",
                        "source": "Market Calendar",
                    }
                ],
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["next_24h_events"][0]

    assert event["actual"] is None
    assert event["consensus"] is None
    assert event["previous"] is None
    assert event["surprise_absolute"] is None
    assert event["lineage"] == []
    assert not _walk_invalid_states(payload["analytics"])
    assert sum(
        item["reason_code"] == "FIELD_LINEAGE_CONTENT_NOT_CURRENT"
        for item in payload["missing_data"]
        if "calendar.next_24h_events.calendar:generic-stale"
        in item["field"]
    ) == 3


def test_mixed_generic_lineage_is_fail_closed_for_all_values() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW + timedelta(hours=1)
    source["sections"]["event_calendar"] = {
        "next_24h_events": [
            {
                "occurrence_id": "calendar:mixed-generic-lineage",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "SCHEDULED",
                "consensus": 100.0,
                "previous": 90.0,
                "freshness": "CURRENT",
                "lineage": [
                    {
                        "field": "value",
                        "freshness": "CURRENT",
                        "value": 100.0,
                    },
                    {
                        "field": "value",
                        "freshness": "STALE",
                        "value": 90.0,
                    },
                ],
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["next_24h_events"][0]

    assert event["consensus"] is None
    assert event["previous"] is None
    assert event["lineage"] == []
    assert not _walk_invalid_states(payload["analytics"])


def test_contradictory_lineage_markers_and_rejected_occurrence_are_ignored() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 97.2,
                "forecast": 96.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "occurrence_id": "occurrence-a",
                        "freshness": "CURRENT_RELEASE",
                    },
                    "forecast": {
                        "occurrence_id": "occurrence-b",
                        "freshness": "CURRENT",
                        "freshness_state": "STALE",
                    },
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] == 97.2
    assert event["consensus"] is None
    assert event["surprise_absolute"] is None
    assert event["reason_code"] is None
    assert [item["field"] for item in event["lineage"]] == ["actual"]
    assert not _walk_invalid_states(payload["analytics"])


def test_usable_lineage_occurrence_must_match_the_event_occurrence() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 100.0,
                "consensus": 90.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "occurrence_id": "occurrence-b",
                        "freshness": "CURRENT_RELEASE",
                        "value": 100.0,
                    },
                    "consensus": {
                        "occurrence_id": "occurrence-b",
                        "freshness": "CURRENT_RELEASE",
                        "value": 90.0,
                    },
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] is None
    assert event["consensus"] is None
    assert event["surprise_absolute"] is None
    assert event["reason_code"] == "OCCURRENCE_FIELD_LINEAGE_MISMATCH"


def test_event_id_cannot_replace_canonical_occurrence_proof() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "canonical-pce-occurrence",
                "event_id": "generic-pce-event",
                "metric_id": "headline_pce_yoy",
                "name": "PCE A/A",
                "release_at": release.isoformat(),
                "reference_period": "2026-06",
                "release_status": "RELEASED",
                "actual": 2.8,
                "actual_is_official": True,
                "actual_source": "BEA",
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "occurrence_id": "generic-pce-event",
                        "metric_id": "headline_pce_yoy",
                        "reference_period": "2026-06",
                        "frequency": "YoY",
                        "source": "BEA",
                        "source_series_id": "BEA:PCE_PRICE_INDEX",
                        "transformation": "pct_change_yoy",
                        "value": 2.8,
                        "validation": {"status": "VERIFIED"},
                    }
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"][
        "latest_released_events"
    ][0]

    assert event["actual"] is None
    assert event["lineage"] == []
    assert event["reason_code"] == "OCCURRENCE_FIELD_LINEAGE_MISMATCH"


def test_explicit_provider_occurrence_crosswalk_is_accepted() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "canonical-occurrence-a",
                "provider_event_id": 1062,
                "provider_occurrence_id": 552847,
                "metric_id": "flash_services_pmi",
                "name": "Flash Services PMI",
                "release_at": release.isoformat(),
                "reference_period": "2026-07",
                "release_status": "RELEASED",
                "actual": 53.6,
                "actual_is_official": True,
                "actual_source": "S&P Global",
                "forecast": 51.3,
                "freshness": "CURRENT_RELEASE",
                "content_valid_until": (
                    FIXED_NOW + timedelta(days=1)
                ).isoformat(),
                "refresh_due_at": (
                    FIXED_NOW + timedelta(hours=12)
                ).isoformat(),
                "field_lineage": {
                    "actual": {
                        "occurrence_id": 552847,
                        "metric_id": "flash_services_pmi",
                        "reference_period": "2026-07",
                        "frequency": "monthly",
                        "source": "S&P Global",
                        "acquisition_provider": "SPGLOBAL",
                        "source_url": (
                            "https://www.pmi.spglobal.com/"
                            "Public/Home/PressRelease"
                        ),
                        "source_domain": "pmi.spglobal.com",
                        "source_series_id": (
                            "SPGLOBAL:US:FLASH_SERVICES_PMI"
                        ),
                        "transformation": "level",
                            "freshness": "CURRENT_RELEASE",
                            "value": 53.6,
                            "validation": {
                                "status": "VERIFIED"
                            },
                        },
                    "forecast": {
                        "occurrence_id": "canonical-occurrence-a",
                        "metric_id": "flash_services_pmi",
                        "reference_period": "2026-07",
                        "frequency": "monthly",
                        "source": "S&P Global",
                            "freshness": "CURRENT_RELEASE",
                            "value": 51.3,
                            "validation": {
                                "status": "VERIFIED"
                            },
                        },
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["occurrence_id"] == "canonical-occurrence-a"
    assert event["provider_event_id"] == 1062
    assert event["provider_occurrence_id"] == 552847
    assert event["actual"] == 53.6
    assert event["consensus"] == 51.3
    assert event["surprise_absolute"] == pytest.approx(2.3)
    assert event["reason_code"] is None


def test_latest_release_deduplication_prefers_a_valid_actual() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    common = {
        "occurrence_id": "canonical-occurrence-a",
        "metric_id": "consumer_confidence",
        "release_at": release.isoformat(),
        "reference_period": "2026-07",
        "release_status": "RELEASED",
        "actual": 100.0,
        "freshness": "CURRENT_RELEASE",
    }
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                **common,
                "consensus": 90.0,
                "previous": 89.0,
                "source": "Stale Candidate",
                "field_lineage": {
                    "actual": {
                        "freshness": "STALE",
                        "value": 100.0,
                    },
                    "consensus": {
                        "freshness": "CURRENT",
                        "value": 90.0,
                    },
                    "previous": {
                        "freshness": "CURRENT",
                        "value": 89.0,
                    },
                },
            },
            {
                **common,
                "source": "Current Candidate",
                "field_lineage": {
                    "actual": {
                        "freshness": "CURRENT_RELEASE",
                        "value": 100.0,
                    }
                },
            },
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    events = payload["analytics"]["calendar"]["latest_released_events"]

    assert len(events) == 1
    assert events[0]["actual"] == 100.0
    assert events[0]["source"]["publisher"] == "Current Candidate"


@pytest.mark.parametrize(
    ("lineage_patch", "reason_code"),
    [
        (
            {"content_valid_until": "not-a-date"},
            "FIELD_LINEAGE_TIMESTAMP_INVALID",
        ),
        (
            {"data_as_of": "not-a-date"},
            "FIELD_LINEAGE_TIMESTAMP_INVALID",
        ),
        (
            {"validation": {"status": "rejected"}},
            "FIELD_LINEAGE_VALIDATION_NOT_ACCEPTED",
        ),
        (
            {"validation": {"status": "quarantined"}},
            "FIELD_LINEAGE_VALIDATION_NOT_ACCEPTED",
        ),
    ],
)
def test_malformed_or_rejected_lineage_is_fail_closed(
    lineage_patch: dict,
    reason_code: str,
) -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "canonical-occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 100.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "freshness": "CURRENT",
                        "value": 100.0,
                        **lineage_patch,
                    }
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] is None
    assert event["reason_code"] == reason_code
    assert event["lineage"] == []
    assert any(
        item["field"].endswith(".actual")
        and item["reason_code"] == reason_code
        for item in payload["missing_data"]
    )


@pytest.mark.parametrize(
    "raw_patch",
    [
        {"lifecycle_status": "STALE"},
        {
            "content_valid_until": (
                FIXED_NOW + timedelta(hours=1)
            ).isoformat(),
            "valid_until": (
                FIXED_NOW - timedelta(seconds=1)
            ).isoformat(),
        },
    ],
)
def test_raw_event_lifecycle_and_all_deadline_aliases_are_enforced(
    raw_patch: dict,
) -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "canonical-occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 100.0,
                "freshness": "CURRENT_RELEASE",
                "source": "Market Calendar",
                "field_lineage": {
                    "actual": {
                        "freshness": "CURRENT",
                        "value": 100.0,
                    }
                },
                **raw_patch,
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] is None
    assert event["lineage"] == []
    assert event["reason_code"] in {
        "FIELD_LINEAGE_CONTENT_NOT_CURRENT",
        "FIELD_LINEAGE_CONTENT_VALIDITY_EXPIRED",
    }


def test_due_field_lineage_cannot_remain_selected() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "canonical-occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 100.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "freshness": "CURRENT",
                        "value": 100.0,
                        "content_valid_until": (
                            FIXED_NOW + timedelta(hours=1)
                        ).isoformat(),
                        "refresh_due_at": (
                            FIXED_NOW - timedelta(seconds=1)
                        ).isoformat(),
                    }
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] is None
    assert event["reason_code"] == "FIELD_LINEAGE_REFRESH_DUE"


def test_actual_and_consensus_reference_periods_must_reconcile() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "canonical-occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "reference_period": "2026-07",
                "frequency": "monthly",
                "release_status": "RELEASED",
                "actual": 100.0,
                "consensus": 90.0,
                "previous": 89.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "occurrence_id": "canonical-occurrence-a",
                        "reference_period": "2026-07",
                        "freshness": "CURRENT",
                        "value": 100.0,
                    },
                    "consensus": {
                        "occurrence_id": "canonical-occurrence-a",
                        "reference_period": "2026-06",
                        "freshness": "CURRENT",
                        "value": 90.0,
                    },
                    "previous": {
                        "occurrence_id": "canonical-occurrence-a",
                        "reference_period": "2026-06",
                        "freshness": "CURRENT",
                        "value": 89.0,
                    },
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] is None
    assert event["consensus"] is None
    assert event["previous"] == 89.0
    assert event["surprise_absolute"] is None
    assert event["reason_code"] == (
        "REFERENCE_PERIOD_FIELD_LINEAGE_MISMATCH"
    )
    assert [item["field"] for item in event["lineage"]] == ["previous"]


@pytest.mark.parametrize(
    "lifecycle_state",
    [
        "HISTORICAL",
        "NOT_FOUND",
        "NOT_CONFIGURED",
        "DISABLED",
        "RESTRICTED",
        "NOT_CALLED",
        "EXHAUSTED_NO_DATA",
    ],
)
def test_canonical_invalid_lineage_states_are_fail_closed(
    lifecycle_state: str,
) -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "canonical-occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 100.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "freshness": lifecycle_state,
                        "value": 100.0,
                    }
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] is None
    assert event["reason_code"] == "FIELD_LINEAGE_CONTENT_NOT_CURRENT"


def test_verified_field_lineage_is_accepted() -> None:
    source = _synthetic_sync()
    release = FIXED_NOW - timedelta(days=1)
    source["sections"]["event_calendar"] = {
        "recently_released_events": [
            {
                "occurrence_id": "canonical-occurrence-a",
                "metric_id": "consumer_confidence",
                "release_at": release.isoformat(),
                "release_status": "RELEASED",
                "actual": 100.0,
                "freshness": "CURRENT_RELEASE",
                "field_lineage": {
                    "actual": {
                        "freshness": "CURRENT",
                        "value": 100.0,
                        "validation": {"status": "VERIFIED"},
                    }
                },
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    event = payload["analytics"]["calendar"]["latest_released_events"][0]

    assert event["actual"] == 100.0
    assert event["reason_code"] is None


def test_lineage_rejection_reason_uses_deterministic_worst_state() -> None:
    reasons: list[str | None] = []
    for states in (
        ("STALE", "REJECTED_FUTURE"),
        ("REJECTED_FUTURE", "STALE"),
    ):
        source = _synthetic_sync()
        release = FIXED_NOW - timedelta(days=1)
        source["sections"]["event_calendar"] = {
            "recently_released_events": [
                {
                    "occurrence_id": "canonical-occurrence-a",
                    "metric_id": "consumer_confidence",
                    "release_at": release.isoformat(),
                    "release_status": "RELEASED",
                    "actual": 100.0,
                    "freshness": "CURRENT_RELEASE",
                    "field_lineage": [
                        {
                            "field": "actual",
                            "freshness": state,
                            "value": 100.0,
                        }
                        for state in states
                    ],
                }
            ]
        }
        payload = build_senior_analyst_payload_v1(
            source,
            now=FIXED_NOW,
        )
        reasons.append(
            payload["analytics"]["calendar"][
                "latest_released_events"
            ][0]["reason_code"]
        )

    assert reasons == [
        "FIELD_LINEAGE_REJECTED_FUTURE",
        "FIELD_LINEAGE_REJECTED_FUTURE",
    ]


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
    assert analytics["vix"]["VIX"]["status"] == "UNAVAILABLE"
    assert analytics["vix"]["VVIX"]["status"] == "UNAVAILABLE"
    assert analytics["risk"]["risk_sentiment"] is None
    assert analytics["risk"]["excluded_inputs_used"] is False


def test_vix_and_vvix_null_values_are_not_delivered_from_metadata_only() -> None:
    source = _synthetic_sync()
    source["sections"]["vix"] = {
        "vix": {
            "value": None,
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "data_as_of": (FIXED_NOW - timedelta(days=1)).isoformat(),
            "content_valid_until": (FIXED_NOW + timedelta(days=1)).isoformat(),
            "source": "FRED",
        },
        "vvix": {
            "value": None,
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "data_as_of": (FIXED_NOW - timedelta(hours=1)).isoformat(),
            "content_valid_until": (FIXED_NOW + timedelta(hours=1)).isoformat(),
            "source": "CBOE",
        },
    }

    payload = build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    analytics = payload["analytics"]["vix"]

    for dataset_id, symbol in (("vix", "VIX"), ("vvix", "VVIX")):
        datum = analytics[symbol]
        assert datum["value"] is None
        assert datum["status"] == "UNAVAILABLE"
        assert datum["freshness"] == "UNAVAILABLE"
        assert datum["reason_code"] == f"{symbol}_VALUE_NOT_AVAILABLE"
        assert any(
            item["field"] == f"vix.{dataset_id}.value"
            and item["reason_code"] == f"{symbol}_VALUE_NOT_AVAILABLE"
            for item in payload["missing_data"]
        )


def test_validator_rejects_available_with_null_value() -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
    )
    payload["analytics"]["vix"]["VIX"].update(
        {
            "value": None,
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "reason_code": None,
        }
    )

    result = validate_senior_analyst_payload_v1(payload, now=FIXED_NOW)

    assert result["status"] == "FAIL"
    assert result["checks"]["available_without_substantive_value"] == 1


@pytest.mark.parametrize("dataset_id", ["vix", "risk"])
def test_validator_rejects_selected_value_presence_without_substance(
    dataset_id: str,
) -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
    )
    row = next(
        item
        for item in payload["provider_accounting"]
        if item["dataset_id"] == dataset_id
    )
    row.update(
        {
            "selected_value_present": True,
            "selected_source": {"publisher": "FRED"},
            "delivered_value": {
                "symbol": "VIX",
                "value": None,
                "status": "UNAVAILABLE",
                "freshness": "UNAVAILABLE",
            },
        }
    )

    result = validate_senior_analyst_payload_v1(payload, now=FIXED_NOW)

    assert result["status"] == "FAIL"
    assert result["checks"]["selected_value_presence_mismatches"] == 1


def test_validator_rejects_positioning_with_only_metadata() -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
    )
    metadata_only = {
        "report_date": "2026-07-28",
        "publication_date": "2026-07-30",
        "contract_code": "209742",
        "open_interest": None,
        "asset_managers": {},
        "leveraged_funds": {},
        "dealers": {},
    }
    payload["analytics"]["positioning"].update(
        {
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "reason_code": None,
            "cot": metadata_only,
        }
    )
    row = next(
        item
        for item in payload["provider_accounting"]
        if item["dataset_id"] == "positioning"
    )
    row.update(
        {
            "selected_value_present": True,
            "selected_source": "CFTC",
            "delivered_value": metadata_only,
        }
    )

    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )

    assert result["status"] == "FAIL"
    assert (
        result["checks"]["available_without_substantive_value"]
        == 1
    )
    assert result["checks"]["selected_value_presence_mismatches"] == 1


def test_projection_excludes_positioning_with_only_metadata() -> None:
    source = _synthetic_sync()
    source["sections"]["positioning"] = {
        "status": "AVAILABLE",
        "freshness": "CURRENT",
        "source": "CFTC",
        "data_as_of": (FIXED_NOW - timedelta(days=1)).isoformat(),
        "content_valid_until": (
            FIXED_NOW + timedelta(days=1)
        ).isoformat(),
        "refresh_due_at": (
            FIXED_NOW + timedelta(hours=12)
        ).isoformat(),
        "cot": {
            "nasdaq_100": {
                "report_date": "2026-07-28",
                "publication_date": "2026-07-29",
                "cftc_contract_market_code": "209742",
                "open_interest": None,
                "asset_managers": {},
                "leveraged_funds": {},
                "dealers": {},
            }
        },
    }

    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )

    positioning = payload["analytics"]["positioning"]
    assert positioning["status"] == "UNAVAILABLE"
    assert positioning["freshness"] == "UNAVAILABLE"
    assert positioning["reason_code"] == (
        "POSITIONING_VALUE_NOT_AVAILABLE"
    )
    assert positioning["cot"] == {}
    assert any(
        item["field"] == "positioning"
        and item["reason_code"]
        == "POSITIONING_VALUE_NOT_AVAILABLE"
        for item in payload["missing_data"]
    )


def test_projection_uses_dynamic_dataset_sla_and_not_retrieval_time(
    monkeypatch,
) -> None:
    policies = tuple(
        replace(policy, sla_seconds=5 * 24 * 60 * 60)
        if policy.dataset_id == "positioning"
        else policy
        for policy in provider_registry.DATASET_SOURCE_POLICIES
    )
    monkeypatch.setattr(
        provider_registry,
        "DATASET_SOURCE_POLICIES",
        policies,
    )
    source = _synthetic_sync()
    source["sections"]["positioning"] = {
        "status": "AVAILABLE",
        "freshness": "CURRENT",
        "source": "CFTC",
        "data_as_of": (
            FIXED_NOW - timedelta(days=6)
        ).isoformat(),
        "retrieved_at": FIXED_NOW.isoformat(),
        "content_valid_until": (
            FIXED_NOW + timedelta(days=1)
        ).isoformat(),
        "refresh_due_at": (
            FIXED_NOW + timedelta(hours=1)
        ).isoformat(),
        "cot": {
            "nasdaq_100": {
                "report_date": "2026-07-23",
                "open_interest": 100,
            }
        },
    }

    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )

    assert (
        payload["analytics"]["positioning"]["reason_code"]
        == "DATASET_SLA_EXCEEDED"
    )

    source["sections"]["positioning"].pop("data_as_of")
    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )

    assert (
        payload["analytics"]["positioning"]["reason_code"]
        == "DATA_AS_OF_NOT_AVAILABLE"
    )


@pytest.mark.parametrize(
    ("timestamp_patch", "expected_reason"),
    [
        (
            {
                "refresh_due_at": (
                    FIXED_NOW - timedelta(seconds=1)
                ).isoformat()
            },
            "REFRESH_DUE",
        ),
        ({"data_as_of": "not-a-date"}, "DATA_AS_OF_INVALID"),
        (
            {"content_valid_until": "not-a-date"},
            "CONTENT_VALID_UNTIL_INVALID",
        ),
        (
            {"refresh_due_at": "not-a-date"},
            "REFRESH_DUE_AT_INVALID",
        ),
    ],
)
def test_projection_rejects_due_or_invalid_explicit_timestamps(
    timestamp_patch: dict[str, str],
    expected_reason: str,
) -> None:
    source = _synthetic_sync()
    positioning = {
        "status": "AVAILABLE",
        "freshness": "CURRENT",
        "source": "CFTC",
        "data_as_of": (
            FIXED_NOW - timedelta(days=1)
        ).isoformat(),
        "content_valid_until": (
            FIXED_NOW + timedelta(days=1)
        ).isoformat(),
        "refresh_due_at": (
            FIXED_NOW + timedelta(hours=1)
        ).isoformat(),
        "cot": {
            "nasdaq_100": {
                "report_date": "2026-07-28",
                "open_interest": 100,
            }
        },
    }
    positioning.update(timestamp_patch)
    source["sections"]["positioning"] = positioning

    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )

    projected = payload["analytics"]["positioning"]
    assert projected["status"] == "UNAVAILABLE"
    assert projected["freshness"] == "UNAVAILABLE"
    assert projected["reason_code"] == expected_reason
    assert projected["cot"] == {}
    assert any(
        item["field"] == "positioning"
        and item["reason_code"] == expected_reason
        for item in payload["missing_data"]
    )


@pytest.mark.parametrize(
    ("lifecycle", "expected_reason"),
    [
        (
            {
                "valid_until": (
                    FIXED_NOW - timedelta(seconds=1)
                ).isoformat(),
            },
            "CONTENT_VALIDITY_EXPIRED",
        ),
        (
            {
                "next_refresh_at": (
                    FIXED_NOW - timedelta(seconds=1)
                ).isoformat(),
            },
            "REFRESH_DUE",
        ),
    ],
)
def test_generic_deadline_aliases_cannot_hide_an_expired_record(
    lifecycle: dict,
    expected_reason: str,
) -> None:
    source = _synthetic_sync()
    source["sections"]["positioning"] = {
        "status": "AVAILABLE",
        "freshness": "CURRENT",
        "source": "CFTC",
        "data_as_of": (
            FIXED_NOW - timedelta(days=1)
        ).isoformat(),
        "content_valid_until": (
            FIXED_NOW + timedelta(days=1)
        ).isoformat(),
        "refresh_due_at": (
            FIXED_NOW + timedelta(hours=1)
        ).isoformat(),
        "lifecycle": lifecycle,
        "cot": {
            "nasdaq_100": {
                "report_date": "2026-07-28",
                "open_interest": 100,
            }
        },
    }

    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )

    assert (
        payload["analytics"]["positioning"]["reason_code"]
        == expected_reason
    )


@pytest.mark.parametrize(
    ("section_name", "section", "expected_reason"),
    [
        (
            "options_positioning",
            {
                "status": "AVAILABLE",
                "freshness": "CURRENT",
                "source": "TRADIER",
                "open_interest": {
                    "symbol": "QQQ",
                    "expiration": "2026-08-01",
                    "data_as_of": "2026-07-29",
                },
                "skew": {
                    "calculation_version": 1,
                },
            },
            "OPTIONS_POSITIONING_VALUE_NOT_AVAILABLE",
        ),
        (
            "risk",
            {
                "risk_context": {
                    "status": "AVAILABLE",
                    "freshness": "CURRENT",
                    "source": "CBOE",
                    "derived_context": {
                        "risk_regime": {
                            "calculation_version": 1,
                        },
                        "risk_score": {
                            "calculation_version": 1,
                        },
                    },
                },
            },
            "RISK_VALUE_NOT_AVAILABLE",
        ),
        (
            "market_schedule",
            {
                "nasdaq_cash_session_verified": True,
                "context_date": "2026-07-29",
                "market_session_status": {
                    "calculation_version": 1,
                },
                "nasdaq_cash_session": {
                    "status": {"metadata": 1},
                    "next_open": {"metadata": 1},
                },
            },
            "MARKET_SCHEDULE_VALUE_NOT_AVAILABLE",
        ),
    ],
)
def test_projection_excludes_other_metadata_only_datasets(
    section_name: str,
    section: dict,
    expected_reason: str,
) -> None:
    source = _synthetic_sync()
    dated_section = deepcopy(section)
    target = (
        dated_section["risk_context"]
        if section_name == "risk"
        else dated_section
    )
    if section_name != "market_schedule":
        target.update(
            {
                "data_as_of": (
                    FIXED_NOW - timedelta(hours=1)
                ).isoformat(),
                "content_valid_until": (
                    FIXED_NOW + timedelta(hours=1)
                ).isoformat(),
                "refresh_due_at": (
                    FIXED_NOW + timedelta(minutes=30)
                ).isoformat(),
            }
        )
    source["sections"][section_name] = dated_section

    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )
    projected = payload["analytics"][section_name]

    assert projected["status"] == "UNAVAILABLE"
    assert projected["freshness"] == "UNAVAILABLE"
    assert projected["reason_code"] == expected_reason
    assert any(
        item["reason_code"] == expected_reason
        for item in payload["missing_data"]
    )


@pytest.mark.parametrize(
    "summary",
    [
        None,
        "N/A",
        "NULL",
        "META_TITLE_QUOTE",
        {"word_count": 10},
        ["token"],
        123,
    ],
)
def test_projection_excludes_metadata_only_current_news(
    summary: object,
) -> None:
    source = _synthetic_sync()
    source["sections"]["news"] = {
        "latest": [
            {
                "article_id": "metadata-only",
                "published_at": (
                    FIXED_NOW - timedelta(hours=1)
                ).isoformat(),
                "source": "Metadata Wire",
                "source_url": "https://example.test/metadata-only",
                "symbols": ["QQQ"],
                "summary": summary,
            }
        ]
    }

    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )

    news = payload["analytics"]["news"]
    assert news["status"] == "NO_DATA"
    assert news["current_news"] == []
    assert news["reason_code"] == "NO_CURRENT_NEWS"
    assert any(
        item["field"] == "news.current_news"
        and item["reason_code"] == "NO_CURRENT_NEWS"
        for item in payload["missing_data"]
    )


@pytest.mark.parametrize(
    "invalid_calls",
    [
        {"metadata": 1},
        [1],
        "NaN",
        "Infinity",
    ],
)
def test_projection_rejects_non_scalar_option_measures(
    invalid_calls: object,
) -> None:
    source = _synthetic_sync()
    source["sections"]["options_positioning"] = {
        "status": "AVAILABLE",
        "freshness": "CURRENT",
        "source": "TRADIER",
        "data_as_of": (FIXED_NOW - timedelta(hours=1)).isoformat(),
        "content_valid_until": (
            FIXED_NOW + timedelta(hours=1)
        ).isoformat(),
        "refresh_due_at": (
            FIXED_NOW + timedelta(minutes=30)
        ).isoformat(),
        "open_interest": {
            "calls": invalid_calls,
        },
    }

    payload = build_senior_analyst_payload_v1(
        source,
        now=FIXED_NOW,
    )

    projected = payload["analytics"]["options_positioning"]
    assert projected["status"] == "UNAVAILABLE"
    assert projected["reason_code"] == (
        "OPTIONS_POSITIONING_VALUE_NOT_AVAILABLE"
    )


@pytest.mark.parametrize(
    ("section_name", "projected"),
    [
        (
            "news",
            {
                "status": "AVAILABLE",
                "current_news": [
                    {
                        "article_id": "metadata-only",
                        "published_at": "2026-07-29T17:00:00+00:00",
                        "source": "Metadata Wire",
                    }
                ],
            },
        ),
        (
            "options_positioning",
            {
                "status": "AVAILABLE",
                "open_interest": {
                    "symbol": "QQQ",
                    "expiration": "2026-08-01",
                },
                "volume": {},
                "skew": {},
                "iv_atm": None,
            },
        ),
        (
            "risk",
            {
                "status": "AVAILABLE",
                "risk_sentiment": None,
                "risk_score": None,
                "excluded_inputs_used": False,
            },
        ),
        (
            "market_schedule",
            {
                "status": "AVAILABLE",
                "context_date": "2026-07-29",
                "market_session_status": None,
                "nasdaq_cash_session": {},
                "mnq_futures_session": {},
            },
        ),
    ],
)
def test_validator_rejects_metadata_only_available_dataset(
    section_name: str,
    projected: dict,
) -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
    )
    payload["analytics"][section_name].update(projected)

    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )

    assert result["status"] == "FAIL"
    assert (
        result["checks"]["available_without_substantive_value"]
        == 1
    )


@pytest.mark.parametrize(
    ("section_name", "projected"),
    [
        (
            "options_positioning",
            {
                "status": "AVAILABLE",
                "open_interest": {"calls": 0},
            },
        ),
        (
            "risk",
            {
                "status": "AVAILABLE",
                "risk_sentiment": None,
                "risk_score": 0,
            },
        ),
        (
            "market_schedule",
            {
                "status": "AVAILABLE",
                "market_session_status": None,
                "nasdaq_cash_session": {
                    "status": None,
                    "is_open": False,
                },
            },
        ),
    ],
)
def test_validator_treats_zero_and_false_as_observed_values(
    section_name: str,
    projected: dict,
) -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
    )
    payload["analytics"][section_name].update(projected)

    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )

    assert (
        result["checks"]["available_without_substantive_value"]
        == 0
    )


def test_validator_accepts_zero_as_substantive_positioning_value() -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
    )
    payload["analytics"]["positioning"].update(
        {
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "reason_code": None,
            "cot": {
                "report_date": "2026-07-28",
                "publication_date": "2026-07-30",
                "contract_code": "209742",
                "open_interest": 0,
                "asset_managers": {},
                "leveraged_funds": {},
                "dealers": {},
            },
        }
    )

    result = validate_senior_analyst_payload_v1(
        payload,
        now=FIXED_NOW,
    )

    assert (
        result["checks"]["available_without_substantive_value"]
        == 0
    )


def test_cli_gate_rejects_available_with_null_value(
    tmp_path: Path,
) -> None:
    payload = build_senior_analyst_payload_v1(
        _synthetic_sync(),
        now=FIXED_NOW,
    )
    payload["analytics"]["vix"]["VIX"].update(
        {
            "value": None,
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "reason_code": None,
        }
    )
    input_path = tmp_path / "payload.json"
    output_path = tmp_path / "report.json"
    input_path.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "validate_senior_analyst_payload.py"),
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--process-cleanup-ok",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    report = json.loads(output_path.read_text(encoding="utf-8"))

    assert completed.returncode == 1
    assert report["status"] == "FAIL"
    assert report["checks"]["available_without_substantive_value"] == 1


def test_readiness_counts_only_filtered_delivered_values() -> None:
    payload = build_senior_analyst_payload_v1(_load_snapshot_98(), now=FIXED_NOW)
    readiness = payload["readiness"]
    assert readiness["excluded_values_contribute"] is False
    assert readiness["delivered_value_counts"]["vix"] == 0
    assert "vix" in readiness["sections_unavailable"]
    assert readiness["coverage_ratio"] < 1.0


def test_every_omission_has_a_reason_and_inferred_accounting_is_incomplete() -> None:
    payload = build_senior_analyst_payload_v1(_load_snapshot_98(), now=FIXED_NOW)
    assert payload["missing_data"]
    assert all(item["reason_code"] for item in payload["missing_data"])
    result = validate_senior_analyst_payload_v1(payload, now=FIXED_NOW)
    assert result["checks"]["unexplained_omissions"] == 0
    assert result["checks"]["provider_accounting_valid"] is False
    assert result["status"] == "PASS_OFFLINE"


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
    assert senior["request"]["request_id"] == "route-test"
    assert senior["request"]["refresh_mode"] == "force"
    assert senior["request"]["same_request_provider_accounting"] is False
    assert senior["request"]["accounting_correlation_id"] is None


def test_second_projection_does_not_mutate_authoritative_input() -> None:
    source = _synthetic_sync()
    before = deepcopy(source)
    build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    build_senior_analyst_payload_v1(source, now=FIXED_NOW)
    assert source == before
