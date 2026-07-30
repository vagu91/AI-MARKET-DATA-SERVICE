from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

from app.services.senior_analyst_projection_v1 import (
    DATASET_POLICIES,
    build_senior_analyst_payload_v1,
    validate_senior_analyst_payload_v1,
)


NOW = datetime(2026, 7, 30, 10, 0, tzinfo=UTC)
REQUEST_ID = "sa-accounting-test"
STARTED_AT = (NOW - timedelta(seconds=30)).isoformat()
COMPLETED_AT = (NOW - timedelta(seconds=1)).isoformat()
OBSERVED_AT = (NOW - timedelta(seconds=10)).isoformat()


def _source() -> dict:
    return {
        "symbol": "MNQ",
        "generated_at": NOW.isoformat(),
        "sections": {
            "macro": {
                "provider_calls": 99,
                "actual_network_calls": 99,
                "cache_used": True,
                "source": "LOOKS_LIKE_DB_BUT_IS_NOT_EVIDENCE",
                "freshness": "CURRENT",
            }
        },
    }


def _complete_row(policy) -> dict:
    return {
        "dataset_id": policy.dataset_id,
        "request_id": REQUEST_ID,
        "correlation_id": REQUEST_ID,
        "evidence_origin": "NORMAL_APPLICATION_REQUEST",
        "evidence_status": "ACQUISITION_COMPLETE",
        "observed_at": OBSERVED_AT,
        "acquisition_id": f"observed:{policy.dataset_id}",
        "shared_acquisition_dataset_ids": [policy.dataset_id],
        "database_lookup_performed": True,
        "database_lookup_reason": "CONTROLLED_DATABASE_LOOKUP",
        "database_record_found": False,
        "database_data_as_of": None,
        "database_content_valid_until": None,
        "database_refresh_due_at": None,
        "database_lifecycle_status": None,
        "database_record_expired": False,
        "database_freshness_evaluation": "NOT_FOUND",
        "primary_provider": {
            "provider": policy.primary_provider,
            "called": True,
            "attempts": 1,
            "result": "SUCCESS",
            "not_called_reason": None,
            "execution_origin": "PROVIDER_CALL",
        },
        "fallbacks": [
            {
                "provider": provider,
                "called": False,
                "attempts": 0,
                "result": "NOT_CALLED",
                "not_called_reason": (
                    "PRIOR_PROVIDER_SUCCEEDED"
                    if policy.provider_strategy == "CASCADE"
                    else "PRIMARY_PROVIDER_SUCCEEDED"
                ),
                "execution_origin": "OBSERVED_SKIP",
            }
            for provider in policy.fallback_providers
        ],
        "acquisition_selected_source": policy.primary_provider,
        "acquisition_reason_code": "CONTROLLED_PROVIDER_VALUE_ACQUIRED",
    }


def _manifest() -> dict:
    return {
        "request_id": REQUEST_ID,
        "correlation_id": REQUEST_ID,
        "request_started_at": STARTED_AT,
        "request_completed_at": COMPLETED_AT,
        "evidence_origin": "NORMAL_APPLICATION_REQUEST",
        "evidence_status": "ACQUISITION_COMPLETE",
        "reason_code": "REQUEST_ACQUISITION_EVIDENCE_COMPLETE",
        "datasets": [_complete_row(policy) for policy in DATASET_POLICIES],
    }


def _build(source: dict) -> dict:
    return build_senior_analyst_payload_v1(
        source,
        now=NOW,
        request_id=REQUEST_ID,
        request_refresh_mode="force",
    )


def _validate_live(payload: dict) -> dict:
    return validate_senior_analyst_payload_v1(
        payload,
        now=NOW,
        require_recent_response=True,
    )


def test_heuristic_looking_payload_never_becomes_request_accounting() -> None:
    payload = _build(_source())
    assert payload["request"]["same_request_provider_accounting"] is False
    assert all(
        row["evidence_status"] == "INCOMPLETE"
        for row in payload["provider_accounting"]
    )
    encoded = str(payload["provider_accounting"])
    assert "DB_VALID_REUSED" not in encoded
    assert "SOURCE_SELECTED_FROM_SAME_REQUEST" not in encoded
    assert _validate_live(payload)["status"] == "FAIL"


def test_incomplete_request_manifest_fails_live_gate() -> None:
    source = _source()
    manifest = _manifest()
    missing_dataset_id = manifest["datasets"][-1]["dataset_id"]
    manifest["datasets"].pop()
    source["request_scoped_provider_accounting"] = manifest
    payload = _build(source)

    assert payload["request"]["same_request_provider_accounting"] is False
    missing_row = next(
        row
        for row in payload["provider_accounting"]
        if row["dataset_id"] == missing_dataset_id
    )
    assert missing_row["evidence_status"] == "INCOMPLETE"
    result = _validate_live(payload)
    assert result["status"] == "FAIL"
    assert result["checks"]["provider_accounting_valid"] is False


def test_uncorrelated_request_manifest_fails_live_gate() -> None:
    source = _source()
    manifest = _manifest()
    manifest["request_id"] = "different-request"
    source["request_scoped_provider_accounting"] = manifest
    payload = _build(source)
    assert payload["request"]["same_request_provider_accounting"] is False
    assert _validate_live(payload)["status"] == "FAIL"


def test_different_correlation_id_fails_live_gate() -> None:
    source = _source()
    manifest = _manifest()
    manifest["correlation_id"] = "different-correlation"
    for row in manifest["datasets"]:
        row["correlation_id"] = "different-correlation"
    source["request_scoped_provider_accounting"] = manifest
    payload = _build(source)
    assert payload["request"]["same_request_provider_accounting"] is False
    assert _validate_live(payload)["status"] == "FAIL"


def test_inferred_provider_attempt_fails_live_gate() -> None:
    source = _source()
    manifest = _manifest()
    manifest["datasets"][0]["primary_provider"][
        "execution_origin"
    ] = "INFERRED"
    source["request_scoped_provider_accounting"] = manifest
    payload = _build(source)
    assert payload["request"]["same_request_provider_accounting"] is False
    assert _validate_live(payload)["checks"]["provider_accounting_valid"] is False


def test_canonical_dataset_without_database_lookup_fails_live_gate() -> None:
    source = _source()
    manifest = _manifest()
    row = manifest["datasets"][0]
    dataset_id = row["dataset_id"]
    row.update(
        {
            "database_lookup_performed": False,
            "database_lookup_reason": "PROVIDER_ONLY_PATH",
            "database_record_found": None,
            "database_data_as_of": None,
            "database_content_valid_until": None,
            "database_refresh_due_at": None,
            "database_record_expired": None,
            "database_freshness_evaluation": "NOT_LOOKED_UP",
        }
    )
    source["request_scoped_provider_accounting"] = manifest

    payload = _build(source)

    assert payload["request"]["same_request_provider_accounting"] is False
    incomplete = [
        item
        for item in payload["provider_accounting"]
        if item["evidence_status"] == "INCOMPLETE"
    ]
    assert [item["dataset_id"] for item in incomplete] == [dataset_id]
    result = _validate_live(payload)
    assert result["status"] == "FAIL"
    assert result["checks"]["provider_accounting_valid"] is False


def test_expired_cache_hit_cannot_be_claimed_as_valid() -> None:
    source = _source()
    manifest = _manifest()
    row = manifest["datasets"][0]
    dataset_id = row["dataset_id"]
    row.update(
        {
            "database_record_found": True,
            "database_data_as_of": (
                NOW - timedelta(minutes=30)
            ).isoformat(),
            "database_content_valid_until": (
                NOW - timedelta(seconds=11)
            ).isoformat(),
            "database_refresh_due_at": (
                NOW + timedelta(hours=1)
            ).isoformat(),
            "database_record_expired": False,
            "database_freshness_evaluation": "VALID",
            "primary_provider": {
                "provider": row["primary_provider"]["provider"],
                "called": False,
                "attempts": 0,
                "result": "CACHE_HIT",
                "not_called_reason": "VALID_DATABASE_RECORD_SELECTED",
                "execution_origin": "CACHE_DECISION",
            },
            "fallbacks": [
                {
                    "provider": attempt["provider"],
                    "called": False,
                    "attempts": 0,
                    "result": "CACHE_HIT",
                    "not_called_reason": "VALID_DATABASE_RECORD_SELECTED",
                    "execution_origin": "CACHE_DECISION",
                }
                for attempt in row["fallbacks"]
            ],
        }
    )
    source["request_scoped_provider_accounting"] = manifest

    payload = _build(source)

    emitted = next(
        item
        for item in payload["provider_accounting"]
        if item["dataset_id"] == dataset_id
    )
    assert emitted["evidence_status"] == "INCOMPLETE"
    assert emitted["database_freshness_evaluation"] is None
    result = _validate_live(payload)
    assert result["status"] == "FAIL"
    assert result["checks"]["provider_accounting_valid"] is False


def test_expired_database_record_is_labeled_and_refreshes_provider() -> None:
    source = _source()
    manifest = _manifest()
    row = manifest["datasets"][0]
    dataset_id = row["dataset_id"]
    row.update(
        {
            "database_record_found": True,
            "database_data_as_of": (
                NOW - timedelta(minutes=30)
            ).isoformat(),
            "database_content_valid_until": (
                NOW - timedelta(seconds=11)
            ).isoformat(),
            "database_refresh_due_at": (
                NOW + timedelta(hours=1)
            ).isoformat(),
            "database_record_expired": True,
            "database_freshness_evaluation": (
                "EXPIRED_CONTENT_VALID_UNTIL"
            ),
        }
    )
    source["request_scoped_provider_accounting"] = manifest

    payload = _build(source)

    assert payload["request"]["same_request_provider_accounting"] is True
    emitted = next(
        item
        for item in payload["provider_accounting"]
        if item["dataset_id"] == dataset_id
    )
    assert emitted["database_record_expired"] is True
    assert (
        emitted["database_freshness_evaluation"]
        == "EXPIRED_CONTENT_VALID_UNTIL"
    )
    assert emitted["database_freshness_evaluation"] != "VALID"
    assert emitted["primary_provider"]["called"] is True
    assert _validate_live(payload)["checks"][
        "provider_accounting_valid"
    ] is True


def test_row_outside_request_window_fails_live_gate() -> None:
    source = _source()
    manifest = _manifest()
    manifest["datasets"][0]["observed_at"] = (
        NOW - timedelta(hours=1)
    ).isoformat()
    source["request_scoped_provider_accounting"] = manifest
    payload = _build(source)
    assert payload["request"]["same_request_provider_accounting"] is False
    assert _validate_live(payload)["checks"]["provider_accounting_valid"] is False


def test_complete_correlated_runtime_manifest_is_required_for_live_pass() -> None:
    source = _source()
    source["request_scoped_provider_accounting"] = _manifest()
    payload = _build(source)
    assert payload["request"]["same_request_provider_accounting"] is True
    result = _validate_live(payload)
    assert result["checks"]["provider_accounting_valid"] is True
    assert result["status"] == "PASS"


def test_fabricated_origin_cannot_pass_shape_validation() -> None:
    source = _source()
    manifest = deepcopy(_manifest())
    manifest["evidence_origin"] = "SYNTHESIZED_FROM_PAYLOAD"
    source["request_scoped_provider_accounting"] = manifest
    payload = _build(source)
    assert payload["request"]["same_request_provider_accounting"] is False
    assert _validate_live(payload)["status"] == "FAIL"


def test_flash_pmi_event_without_actual_has_null_delivery_evidence() -> None:
    source = _source()
    source["sections"]["event_calendar"] = {
        "next_24h_events": [
            {
                "occurrence_id": "flash-pmi-no-data",
                "metric_id": "flash_services_pmi",
                "name": "Flash Services PMI",
                "release_at": (
                    NOW + timedelta(minutes=30)
                ).isoformat(),
                "reference_period": "2026-07",
                "release_status": "PROVIDER_UNAVAILABLE",
                "actual": None,
                "source": "calendar-distributor",
            }
        ]
    }
    source["request_scoped_provider_accounting"] = _manifest()

    payload = _build(source)

    row = next(
        item
        for item in payload["provider_accounting"]
        if item["dataset_id"] == "flash_services_pmi"
    )
    assert row["selected_value_present"] is False
    assert row["delivered_value"] is None
    assert row["selected_source"] is None


def test_vix_and_vvix_null_delivery_is_not_selected_from_metadata() -> None:
    source = _source()
    source["sections"]["vix"] = {
        "vix": {
            "value": None,
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "data_as_of": (NOW - timedelta(days=1)).isoformat(),
            "content_valid_until": (NOW + timedelta(days=1)).isoformat(),
            "source": "FRED",
        },
        "vvix": {
            "value": None,
            "status": "AVAILABLE",
            "freshness": "CURRENT",
            "data_as_of": (NOW - timedelta(hours=1)).isoformat(),
            "content_valid_until": (NOW + timedelta(hours=1)).isoformat(),
            "source": "CBOE",
        },
    }
    source["request_scoped_provider_accounting"] = _manifest()

    payload = _build(source)
    rows = {
        row["dataset_id"]: row
        for row in payload["provider_accounting"]
        if row["dataset_id"] in {"vix", "vvix"}
    }

    for dataset_id in ("vix", "vvix"):
        row = rows[dataset_id]
        assert row["evidence_status"] == "COMPLETE"
        assert row["selected_value_present"] is False
        assert row["delivered_value"] is None
        assert row["selected_source"] is None
        assert row["payload_freshness"] == "UNAVAILABLE"
    assert _validate_live(payload)["checks"]["provider_accounting_valid"] is True
