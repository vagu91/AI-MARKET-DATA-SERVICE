from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services.request_provider_accounting import (
    _attempt_succeeded,
    _provider_flow_valid,
    provider_attempt,
)


def _calendar_attempt(result: str) -> dict:
    return provider_attempt(
        "CANONICAL_EVENT_REPOSITORY",
        called=True,
        attempts=1,
        result=result,
        execution_origin="PROVIDER_CALL",
    )


@pytest.mark.parametrize(
    "result",
    [
        "SCHEDULE_CATCH_UP_INCOMPLETE",
        "SCHEDULE_CATCH_UP_PARTIAL",
        "SCHEDULE_CATCH_UP_FAILED",
        "SCHEDULE_CATCH_UP_COMPLETED_WITHOUT_EVIDENCE",
    ],
)
def test_schedule_catch_up_non_terminal_results_are_not_successful(
    result: str,
) -> None:
    assert _attempt_succeeded(_calendar_attempt(result)) is False


def test_schedule_catch_up_completed_allows_observed_fallback_skips() -> None:
    policy = SimpleNamespace(
        provider_strategy="FALLBACK",
        fallback_providers=(
            "INVESTING_ECONOMIC_CALENDAR",
            "XTB",
        ),
    )
    row = {
        "database_record_found": True,
        "database_record_expired": True,
        "database_freshness_evaluation": "REFRESH_DUE",
        "primary_provider": _calendar_attempt(
            "SCHEDULE_CATCH_UP_COMPLETED"
        ),
        "fallbacks": [
            provider_attempt(
                provider,
                called=False,
                attempts=0,
                result="NOT_CALLED",
                not_called_reason="PRIOR_PROVIDER_SUCCEEDED",
                execution_origin="OBSERVED_SKIP",
            )
            for provider in policy.fallback_providers
        ],
    }

    assert _attempt_succeeded(row["primary_provider"]) is True
    assert _provider_flow_valid(row, policy=policy) is True


def test_incomplete_schedule_catch_up_cannot_justify_fallback_skips() -> None:
    policy = SimpleNamespace(
        provider_strategy="FALLBACK",
        fallback_providers=(
            "INVESTING_ECONOMIC_CALENDAR",
            "XTB",
        ),
    )
    row = {
        "database_record_found": True,
        "database_record_expired": True,
        "database_freshness_evaluation": "REFRESH_DUE",
        "primary_provider": _calendar_attempt(
            "SCHEDULE_CATCH_UP_INCOMPLETE"
        ),
        "fallbacks": [
            provider_attempt(
                provider,
                called=False,
                attempts=0,
                result="NOT_CALLED",
                not_called_reason="PRIOR_PROVIDER_SUCCEEDED",
                execution_origin="OBSERVED_SKIP",
            )
            for provider in policy.fallback_providers
        ],
    }

    assert _attempt_succeeded(row["primary_provider"]) is False
    assert _provider_flow_valid(row, policy=policy) is False
