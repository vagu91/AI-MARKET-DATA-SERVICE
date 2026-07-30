from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.services.data_freshness_service import DataFreshnessService
from app.services.diagnostics_service import (
    NEWS_ACCOUNTING_PROVIDER_NAMES,
    DiagnosticsService,
    _calendar_catch_up_succeeded,
    _calendar_primary_acquisition_succeeded,
    _select_current_news_database_candidate,
)
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
)
from app.services.senior_analyst_projection_v1 import DATASET_POLICIES


NOW = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)


def _complete_calendar_coverage() -> dict[str, object]:
    return {
        "status": "VERIFIED_COMPLETE",
        "provider_calls_executed": 2,
        "provider_success_count": 2,
        "unknown_coverage_days": [],
        "partial_coverage_days": [],
        "quarantined_occurrence_count": 0,
        "daily_matrix": {
            "status": "VERIFIED_COMPLETE",
        },
    }


def test_calendar_catch_up_completion_requires_full_coverage_proof() -> None:
    assert _calendar_catch_up_succeeded(
        _complete_calendar_coverage()
    )


def test_verified_empty_calendar_catch_up_skips_fallbacks() -> None:
    assert _calendar_primary_acquisition_succeeded(
        {
            "database_lookup_performed": True,
            "provider_called": True,
            "provider_result": "SCHEDULE_CATCH_UP_COMPLETED",
        },
        events=[],
    )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("status",), "PARTIAL"),
        (("provider_success_count",), 0),
        (("unknown_coverage_days",), ["2026-07-30"]),
        (("partial_coverage_days",), ["2026-07-30"]),
        (("quarantined_occurrence_count",), 1),
        (("daily_matrix", "status"), "PARTIAL"),
    ],
)
def test_calendar_catch_up_rejects_incomplete_or_unproved_coverage(
    path: tuple[str, ...],
    value: object,
) -> None:
    coverage = _complete_calendar_coverage()
    if len(path) == 1:
        coverage[path[0]] = value
    else:
        daily_matrix = coverage["daily_matrix"]
        assert isinstance(daily_matrix, dict)
        daily_matrix[path[-1]] = value

    assert not _calendar_catch_up_succeeded(coverage)


def _news_accounts() -> list[dict[str, object]]:
    accounts: list[dict[str, object]] = []
    for index, actual_name in enumerate(
        NEWS_ACCOUNTING_PROVIDER_NAMES.values()
    ):
        called = index == 1
        accounts.append(
            {
                "provider": actual_name,
                "calls": 1 if called else 0,
                "status": "COMPLETE" if called else "NOT_CALLED",
                "reason_code": (
                    None
                    if called
                    else "PROVIDER_DISABLED_BY_CONFIGURATION"
                ),
            }
        )
    return accounts


def _news_cache_accounts() -> list[dict[str, object]]:
    return [
        {
            "provider": actual_name,
            "calls": 0,
            "status": "CACHE_HIT",
            "reason_code": (
                "CURRENT_REQUEST_TECHNICAL_CACHE_SELECTED"
            ),
            "execution_origin": "CACHE_DECISION",
        }
        for actual_name in NEWS_ACCOUNTING_PROVIDER_NAMES.values()
    ]


def _news_collector() -> RequestProviderAccountingCollector:
    policy = next(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id == "current_news"
    )
    return RequestProviderAccountingCollector(
        request_id="request-news-live-regression",
        correlation_id="request-news-live-regression",
        request_started_at=NOW,
        policies=(policy,),
        clock=lambda: NOW + timedelta(seconds=1),
    )


def test_news_fan_in_no_current_articles_keeps_real_complete_telemetry() -> None:
    service = object.__new__(DiagnosticsService)
    service.freshness = DataFreshnessService(
        Settings(_env_file=None),
        clock=lambda: NOW,
    )
    historical = {
        "title": "Historical-only market item",
        "source": "Insider Monkey",
        "source_url": "https://example.test/historical",
        "published_at": (NOW - timedelta(days=3)).isoformat(),
        "data_as_of": (NOW - timedelta(days=3)).isoformat(),
        "retrieved_at": NOW.isoformat(),
        "valid_until": (NOW - timedelta(days=2)).isoformat(),
        "next_refresh_at": (NOW - timedelta(days=2)).isoformat(),
    }
    database_freshness = service.freshness.evaluate_canonical(
        historical,
        max_age=timedelta(hours=24),
    )
    collector = _news_collector()

    service._record_news_accounting(
        collector,
        news_items=[historical],
        database_item=historical,
        database_freshness=database_freshness,
        provider_quality={
            "provider_accounting_valid": False,
            "provider_accounting": _news_accounts(),
        },
    )

    manifest = collector.manifest(
        request_completed_at=NOW + timedelta(seconds=2)
    )
    row = manifest["datasets"][0]
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    assert row["evidence_status"] == "ACQUISITION_COMPLETE"
    assert row["acquisition_selected_source"] is None
    assert (
        row["acquisition_reason_code"]
        == "NEWS_FAN_IN_COMPLETED_NO_CURRENT_DATA"
    )
    assert sum(
        attempt["called"]
        for attempt in [row["primary_provider"], *row["fallbacks"]]
    ) == 1


def test_news_db_first_uses_one_eligible_canonical_observation() -> None:
    service = object.__new__(DiagnosticsService)
    service.freshness = DataFreshnessService(
        Settings(_env_file=None),
        clock=lambda: NOW,
    )
    quarantined = {
        "title": "Quarantined but temporally current",
        "source": "INVALID_A",
        "source_url": "https://invalid.test/a",
        "source_audit_status": "QUARANTINED",
        "published_at": (NOW - timedelta(minutes=30)).isoformat(),
        "data_as_of": (NOW - timedelta(minutes=30)).isoformat(),
        "valid_until": (NOW + timedelta(hours=2)).isoformat(),
        "next_refresh_at": (NOW + timedelta(hours=1)).isoformat(),
    }
    valid = {
        "title": "Eligible current market item",
        "source": "VALID_B",
        "source_url": "https://valid.test/b",
        "source_audit_status": "ACTIVE",
        "published_at": (NOW - timedelta(hours=1)).isoformat(),
        "data_as_of": (NOW - timedelta(hours=1)).isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        "next_refresh_at": (NOW + timedelta(minutes=30)).isoformat(),
    }
    placeholder = {
        "title": None,
        "summary": "N/A",
        "source": "PLACEHOLDER_SOURCE",
        "source_url": "https://placeholder.test/item",
        "source_audit_status": "ACTIVE",
        "published_at": (NOW - timedelta(minutes=45)).isoformat(),
        "data_as_of": (NOW - timedelta(minutes=45)).isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        "next_refresh_at": (NOW + timedelta(minutes=15)).isoformat(),
    }

    selected, freshness = _select_current_news_database_candidate(
        [quarantined, valid],
        freshness_service=service.freshness,
    )

    assert selected is valid
    assert freshness.usable is True
    assert freshness.data_as_of == valid["data_as_of"]

    missing, missing_freshness = (
        _select_current_news_database_candidate(
            [quarantined, placeholder],
            freshness_service=service.freshness,
        )
    )
    assert missing is None
    assert missing_freshness.usable is False


def test_news_db_first_preserves_expired_lookup_evidence() -> None:
    freshness_service = DataFreshnessService(
        Settings(_env_file=None),
        clock=lambda: NOW,
    )
    expired = {
        "title": "Eligible but expired market item",
        "source": "EXPIRED_SOURCE",
        "source_url": "https://expired.test/item",
        "source_audit_status": "ACTIVE",
        "published_at": (NOW - timedelta(days=2)).isoformat(),
        "data_as_of": (NOW - timedelta(days=2)).isoformat(),
        "valid_until": (NOW - timedelta(days=1)).isoformat(),
        "next_refresh_at": (NOW - timedelta(days=1)).isoformat(),
    }

    selected, freshness = _select_current_news_database_candidate(
        [expired],
        freshness_service=freshness_service,
    )

    assert selected is expired
    assert freshness.usable is False
    assert freshness.expired is True
    assert freshness.data_as_of == expired["data_as_of"]
    assert freshness.evaluation == "EXPIRED_CONTENT_VALID_UNTIL"


def test_news_fan_in_missing_one_source_is_incomplete() -> None:
    service = object.__new__(DiagnosticsService)
    service.freshness = DataFreshnessService(
        Settings(_env_file=None),
        clock=lambda: NOW,
    )
    collector = _news_collector()
    accounts = _news_accounts()

    service._record_news_accounting(
        collector,
        news_items=[],
        database_item=None,
        database_freshness=service.freshness.evaluate_canonical(
            None,
            max_age=timedelta(hours=24),
        ),
        provider_quality={
            "provider_accounting_valid": True,
            "provider_accounting": deepcopy(accounts[:-1]),
        },
    )

    manifest = collector.manifest(
        request_completed_at=NOW + timedelta(seconds=2)
    )
    assert manifest["evidence_status"] == "INCOMPLETE"
    assert manifest["datasets"][0]["evidence_status"] == "INCOMPLETE"


def test_news_technical_cache_is_observed_without_reusing_provider_calls() -> None:
    service = object.__new__(DiagnosticsService)
    service.freshness = DataFreshnessService(
        Settings(_env_file=None),
        clock=lambda: NOW,
    )
    collector = _news_collector()

    service._record_news_accounting(
        collector,
        news_items=[],
        database_item=None,
        database_freshness=service.freshness.evaluate_canonical(
            None,
            max_age=timedelta(hours=24),
        ),
        provider_quality={
            "provider_accounting_valid": True,
            "provider_accounting": _news_cache_accounts(),
        },
    )

    manifest = collector.manifest(
        request_completed_at=NOW + timedelta(seconds=2)
    )
    row = manifest["datasets"][0]
    attempts = [row["primary_provider"], *row["fallbacks"]]
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    assert (
        row["acquisition_reason_code"]
        == "NEWS_TECHNICAL_CACHE_NO_CURRENT_DATA"
    )
    assert all(
        attempt["called"] is False
        and attempt["attempts"] == 0
        and attempt["execution_origin"] == "CACHE_DECISION"
        and attempt["not_called_reason"]
        == "CURRENT_REQUEST_TECHNICAL_CACHE_SELECTED"
        for attempt in attempts
    )


def test_news_technical_cache_current_value_has_coherent_reason() -> None:
    service = object.__new__(DiagnosticsService)
    service.freshness = DataFreshnessService(
        Settings(_env_file=None),
        clock=lambda: NOW,
    )
    collector = _news_collector()
    current = {
        "title": "Current cache-selected market item",
        "source": "Federal Reserve",
        "source_url": "https://example.test/current",
        "published_at": (NOW - timedelta(hours=1)).isoformat(),
        "data_as_of": (NOW - timedelta(hours=1)).isoformat(),
        "retrieved_at": (NOW - timedelta(minutes=10)).isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        "next_refresh_at": (NOW + timedelta(minutes=30)).isoformat(),
    }

    service._record_news_accounting(
        collector,
        news_items=[current],
        database_item=None,
        database_freshness=service.freshness.evaluate_canonical(
            None,
            max_age=timedelta(hours=24),
        ),
        provider_quality={
            "provider_accounting_valid": True,
            "provider_accounting": _news_cache_accounts(),
        },
    )

    manifest = collector.manifest(
        request_completed_at=NOW + timedelta(seconds=2)
    )
    row = manifest["datasets"][0]
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    assert row["acquisition_selected_source"] == "Federal Reserve"
    assert (
        row["acquisition_reason_code"]
        == "NEWS_TECHNICAL_CACHE_VALUE_SELECTED"
    )
    assert all(
        attempt["execution_origin"] == "CACHE_DECISION"
        and attempt["called"] is False
        for attempt in [row["primary_provider"], *row["fallbacks"]]
    )
