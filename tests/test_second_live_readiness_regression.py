from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.services.senior_analyst_projection_v1 import (
    SECTION_NAMES,
    _readiness,
    validate_senior_analyst_payload_v1,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = (
    ROOT
    / "tests"
    / "fixtures"
    / "senior_analyst_second_live_readiness_compact.json"
)
ORIGINAL_BODY_SHA256 = (
    "68506c6d54ecd25429286d905c50aa1c49985aff33fd47c2f570f4ead6e6a6e0"
)
FIXTURE_PAYLOAD_SHA256 = (
    "bc44dd273251adf7f6f5c59a39b046e83f9cb3ac6c859b7ba71887aaa4abaf6f"
)
SECOND_LIVE_NOW = datetime(
    2026,
    7,
    30,
    15,
    12,
    41,
    116023,
    tzinfo=UTC,
)


def _load_fixture(path: Path = FIXTURE_PATH) -> dict:
    fixture = json.loads(path.read_text(encoding="utf-8"))
    assert fixture["fixture_contract"] == (
        "SeniorAnalystSecondLiveReadinessRegressionFixture"
    )
    assert fixture["fixture_version"] == 1
    origin = fixture["metadata"]["origin"]
    assert origin["run_id"] == "20260730T151138Z"
    assert origin["body_size_bytes"] == 157_162
    assert origin["body_sha256"] == ORIGINAL_BODY_SHA256
    assert origin["body_sha256_scope"] == (
        "EXACT_HTTP_RESPONSE_BODY_BYTES"
    )
    payload = fixture["payload"]
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()
    assert fixture["metadata"]["fixture_payload_sha256"] == (
        FIXTURE_PAYLOAD_SHA256
    )
    assert digest == FIXTURE_PAYLOAD_SHA256
    return payload


def _empty_analytics() -> dict:
    return {
        name: {"status": "UNAVAILABLE"}
        for name in SECTION_NAMES
    }


def _assert_mutually_exclusive(readiness: dict) -> None:
    available = set(readiness["sections_available"])
    degraded = set(readiness["sections_degraded"])
    unavailable = set(readiness["sections_unavailable"])
    assert available.isdisjoint(degraded)
    assert available.isdisjoint(unavailable)
    assert degraded.isdisjoint(unavailable)
    assert available | degraded | unavailable == set(SECTION_NAMES)
    assert readiness["available_section_count"] == len(available)
    assert readiness["degraded_section_count"] == len(degraded)
    assert readiness["unavailable_section_count"] == len(unavailable)


def test_second_live_compact_fixture_is_versioned_integral_and_bounded() -> None:
    payload = _load_fixture()
    assert FIXTURE_PATH.stat().st_size < 15_000
    assert payload["snapshot_revision"] == 100
    assert payload["generated_at"] == (
        "2026-07-30T15:12:41.116023+00:00"
    )


def test_second_live_compact_fixture_missing_is_a_hard_failure(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        _load_fixture(tmp_path / "missing-second-live-fixture.json")


def test_second_live_compact_fixture_mutation_is_detected(
    tmp_path: Path,
) -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    fixture["payload"]["analytics"]["news"]["current_news"][0][
        "headline"
    ] = "mutated"
    mutated = tmp_path / "mutated-second-live-fixture.json"
    mutated.write_text(
        json.dumps(fixture, ensure_ascii=False),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError):
        _load_fixture(mutated)


def test_second_live_fixture_reproduces_news_and_overlap_defects() -> None:
    payload = _load_fixture()
    captured = payload["readiness"]
    canonical = _readiness(payload["analytics"])

    assert captured["delivered_value_counts"]["news"] == 0
    assert canonical["delivered_value_counts"]["news"] == 1
    assert "news" in canonical["sections_available"]
    assert "news" not in canonical["sections_unavailable"]
    assert {"macro", "nasdaq"} <= (
        set(captured["sections_available"])
        & set(captured["sections_degraded"])
    )
    _assert_mutually_exclusive(canonical)

    result = validate_senior_analyst_payload_v1(
        payload,
        now=SECOND_LIVE_NOW,
    )
    assert result["status"] == "FAIL"
    assert (
        result["checks"]["readiness_section_classification_mismatches"]
        > 0
    )


def test_readiness_counts_substantive_news_but_not_news_metadata() -> None:
    analytics = _empty_analytics()
    analytics["news"] = {
        "status": "AVAILABLE",
        "current_news": [
            {
                "headline": "Substantive current headline",
                "published_at": "2026-07-30T15:10:00+00:00",
                "source": {"publisher": "Verified publisher"},
            }
        ],
    }
    readiness = _readiness(analytics)
    assert readiness["delivered_value_counts"]["news"] == 1
    assert readiness["section_status"]["news"] == "AVAILABLE"
    assert "news" in readiness["sections_available"]

    metadata_only = deepcopy(analytics)
    metadata_only["news"]["current_news"][0]["headline"] = None
    readiness = _readiness(metadata_only)
    assert readiness["delivered_value_counts"]["news"] == 0
    assert readiness["section_status"]["news"] == "UNAVAILABLE"
    assert "news" in readiness["sections_unavailable"]


def test_readiness_preserves_numeric_zero_and_false_as_substantive() -> None:
    analytics = _empty_analytics()
    analytics["market_internals"] = {
        "status": "AVAILABLE",
        "advancers": 0,
    }
    analytics["risk"] = {
        "status": "AVAILABLE",
        "risk_score": 0,
    }
    analytics["market_schedule"] = {
        "status": "AVAILABLE",
        "market_session_status": "",
        "nasdaq_cash_session": {
            "status": "",
            "is_open": False,
        },
    }

    readiness = _readiness(analytics)

    for section in ("market_internals", "risk", "market_schedule"):
        assert readiness["delivered_value_counts"][section] == 1
        assert readiness["section_status"][section] == "AVAILABLE"
        assert section in readiness["sections_available"]
    _assert_mutually_exclusive(readiness)


def test_readiness_ignores_source_time_and_lineage_without_content() -> None:
    analytics = _empty_analytics()
    metadata = {
        "source": {"publisher": "metadata-only"},
        "lineage": [{"field": "metadata-only"}],
    }
    analytics["calendar"] = {
        "status": "AVAILABLE",
        "next_24h_events": [
            {
                **metadata,
                "release_at": "2026-07-31T12:30:00+00:00",
            }
        ],
    }
    analytics["earnings"] = {
        "status": "AVAILABLE",
        "events": [
            {
                **metadata,
                "event_at": "2026-07-31T20:00:00+00:00",
            }
        ],
    }
    analytics["market_schedule"] = {
        "status": "AVAILABLE",
        "nasdaq_cash_session": {
            **metadata,
            "next_open": "2026-07-31T13:30:00+00:00",
        },
    }

    readiness = _readiness(analytics)

    for section in ("calendar", "earnings", "market_schedule"):
        assert readiness["delivered_value_counts"][section] == 0
        assert readiness["section_status"][section] == "UNAVAILABLE"
        assert section in readiness["sections_unavailable"]
    _assert_mutually_exclusive(readiness)


def test_partial_section_is_degraded_only_and_counts_match_lists() -> None:
    analytics = _empty_analytics()
    analytics["macro"] = {
        "status": "PARTIAL",
        "metrics": [
            {
                "series_id": "ICSA",
                "value": 0,
            }
        ],
    }

    readiness = _readiness(analytics)

    assert readiness["section_status"]["macro"] == "PARTIAL"
    assert "macro" in readiness["sections_degraded"]
    assert "macro" not in readiness["sections_available"]
    assert "macro" not in readiness["sections_unavailable"]
    assert readiness["coverage_ratio"] == round(
        1 / len(SECTION_NAMES),
        4,
    )
    _assert_mutually_exclusive(readiness)


def test_validator_rejects_readiness_classification_tampering() -> None:
    analytics = _empty_analytics()
    analytics["news"] = {
        "status": "AVAILABLE",
        "current_news": [{"headline": "A substantive current headline"}],
    }
    canonical = _readiness(analytics)
    clean_payload = {
        "generated_at": SECOND_LIVE_NOW.isoformat(),
        "analytics": analytics,
        "readiness": canonical,
        "missing_data": [],
        "provider_accounting": [],
    }
    clean = validate_senior_analyst_payload_v1(
        clean_payload,
        now=SECOND_LIVE_NOW,
    )
    assert (
        clean["checks"]["readiness_section_classification_mismatches"]
        == 0
    )

    tampered_payload = deepcopy(clean_payload)
    tampered_payload["readiness"]["sections_unavailable"].append("news")
    tampered = validate_senior_analyst_payload_v1(
        tampered_payload,
        now=SECOND_LIVE_NOW,
    )
    assert tampered["status"] == "FAIL"
    assert (
        tampered["checks"]["readiness_section_classification_mismatches"]
        > 0
    )
