from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.services.data_freshness_service import (
    CanonicalFreshnessPolicy,
    DataFreshnessService,
    evaluate_canonical_freshness,
)


NOW = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)


def _policy(
    *,
    mode: str = "official_release",
    max_age: timedelta = timedelta(hours=2),
) -> CanonicalFreshnessPolicy:
    return CanonicalFreshnessPolicy(
        max_age=max_age,
        data_reference_mode=mode,
    )


def _current_row(**overrides) -> dict:
    row = {
        "data_as_of": (NOW - timedelta(minutes=30)).isoformat(),
        "content_valid_until": (NOW + timedelta(hours=1)).isoformat(),
        "refresh_due_at": (NOW + timedelta(minutes=30)).isoformat(),
        "lifecycle": {
            "freshness_state": "FRESH",
            "currently_valid": True,
        },
    }
    row.update(overrides)
    return row


def _evaluate(
    row: dict | None,
    *,
    policy: CanonicalFreshnessPolicy | None = None,
):
    return evaluate_canonical_freshness(
        row,
        policy=policy or _policy(),
        observed_at=NOW,
    )


def test_earliest_content_deadline_across_layers_wins() -> None:
    row = _current_row(
        content_valid_until=(NOW + timedelta(hours=4)).isoformat(),
        raw_payload={
            "valid_until": (NOW + timedelta(hours=2)).isoformat(),
            "lifecycle": {
                "valid_until": (NOW - timedelta(seconds=1)).isoformat(),
            },
        },
    )

    result = _evaluate(row)

    assert result.complete is True
    assert result.usable is False
    assert result.expired is True
    assert result.evaluation == "EXPIRED_CONTENT_VALID_UNTIL"
    assert result.content_valid_until == (
        NOW - timedelta(seconds=1)
    ).isoformat()


def test_earliest_refresh_deadline_across_layers_wins() -> None:
    row = _current_row(
        refresh_due_at=(NOW + timedelta(hours=4)).isoformat(),
        raw_payload={
            "next_refresh_at": (NOW + timedelta(hours=2)).isoformat(),
            "lifecycle": {
                "next_refresh": NOW.isoformat(),
            },
        },
    )

    result = _evaluate(row)

    assert result.usable is False
    assert result.expired is True
    assert result.evaluation == "REFRESH_DUE"
    assert result.refresh_due_at == NOW.isoformat()


@pytest.mark.parametrize(
    "state",
    [
        "DUE",
        "OVERDUE",
        "EXPIRED",
        "STALE",
        "VERY_STALE",
        "HISTORICAL",
        "INVALID",
        "REJECTED",
        "SUPERSEDED",
        "NO_DATA_BACKOFF",
    ],
)
def test_invalid_lifecycle_state_excludes_record(state: str) -> None:
    row = _current_row(
        lifecycle={
            "freshness_state": state,
            "currently_valid": True,
        }
    )

    result = _evaluate(row)

    assert result.complete is True
    assert result.usable is False
    assert result.expired is True
    assert result.evaluation == "INVALID_LIFECYCLE"
    assert result.reason_code == f"CANONICAL_LIFECYCLE_{state}"


def test_false_currently_valid_in_any_layer_cannot_be_masked() -> None:
    row = _current_row(
        currently_valid=True,
        lifecycle={
            "freshness_state": "FRESH",
            "currently_valid": False,
        },
    )

    result = _evaluate(row)

    assert result.evaluation == "INVALID_LIFECYCLE"
    assert result.reason_code == (
        "CANONICAL_LIFECYCLE_NOT_CURRENTLY_VALID"
    )


def test_superseded_record_is_invalid_even_with_fresh_deadlines() -> None:
    row = _current_row(
        lifecycle={
            "freshness_state": "FRESH",
            "currently_valid": True,
            "superseded_by": "newer-occurrence",
        }
    )

    result = _evaluate(row)

    assert result.evaluation == "INVALID_LIFECYCLE"
    assert result.reason_code == "CANONICAL_LIFECYCLE_SUPERSEDED"


def test_retrieved_at_requires_explicit_point_in_time_policy() -> None:
    row = {
        "retrieved_at": (NOW - timedelta(minutes=5)).isoformat(),
        "valid_until": (NOW + timedelta(minutes=15)).isoformat(),
        "next_refresh_at": (NOW + timedelta(minutes=15)).isoformat(),
    }

    official = _evaluate(row)
    point_in_time = _evaluate(
        row,
        policy=_policy(mode="point_in_time"),
    )

    assert official.complete is False
    assert official.evaluation == "MISSING_DATA_AS_OF"
    assert point_in_time.complete is True
    assert point_in_time.usable is True
    assert point_in_time.data_as_of == row["retrieved_at"]


def test_recent_retrieval_does_not_refresh_old_official_release() -> None:
    row = {
        "data_as_of": "2026-01",
        "retrieved_at": NOW.isoformat(),
        "valid_until": (NOW + timedelta(days=10)).isoformat(),
        "next_refresh_at": (NOW + timedelta(days=10)).isoformat(),
    }

    result = _evaluate(
        row,
        policy=_policy(max_age=timedelta(days=45)),
    )

    assert result.complete is True
    assert result.usable is False
    assert result.expired is True
    assert result.evaluation == "SLA_EXPIRED"
    assert result.data_as_of == "2026-01"


def test_future_occurrence_is_allowed_only_for_event_policy() -> None:
    row = {
        "release_at": (NOW + timedelta(days=2)).isoformat(),
        "valid_until": (NOW + timedelta(days=3)).isoformat(),
        "next_refresh_at": (NOW + timedelta(days=1)).isoformat(),
    }

    official = _evaluate(
        row,
        policy=_policy(max_age=timedelta(days=7)),
    )
    event = _evaluate(
        row,
        policy=_policy(
            mode="event_occurrence",
            max_age=timedelta(days=7),
        ),
    )

    assert official.evaluation == "FUTURE_DATA_AS_OF"
    assert official.usable is False
    assert event.evaluation == "VALID"
    assert event.usable is True


@pytest.mark.parametrize(
    ("row", "evaluation"),
    [
        (
            {
                "data_as_of": NOW.isoformat(),
                "refresh_due_at": (NOW + timedelta(hours=1)).isoformat(),
            },
            "MISSING_CONTENT_VALID_UNTIL",
        ),
        (
            {
                "data_as_of": NOW.isoformat(),
                "content_valid_until": (
                    NOW + timedelta(hours=1)
                ).isoformat(),
            },
            "MISSING_REFRESH_DUE_AT",
        ),
    ],
)
def test_missing_boundaries_are_not_inferred(
    row: dict,
    evaluation: str,
) -> None:
    result = _evaluate(row)

    assert result.complete is False
    assert result.usable is False
    assert result.expired is False
    assert result.evaluation == evaluation


def test_invalid_deadline_in_any_layer_is_not_ignored() -> None:
    row = _current_row(
        raw_payload={
            "lifecycle": {
                "valid_until": "not-a-timestamp",
            }
        }
    )

    result = _evaluate(row)

    assert result.complete is False
    assert result.usable is False
    assert result.evaluation == "INVALID_CONTENT_VALID_UNTIL"


def test_service_wrapper_can_evaluate_at_request_observation_time() -> None:
    settings = Settings(_env_file=None)
    service = DataFreshnessService(
        settings,
        clock=lambda: NOW + timedelta(hours=2),
    )
    row = _current_row()

    result = service.evaluate_canonical(
        row,
        max_age=timedelta(hours=2),
        observed_at=NOW,
    )

    assert result.usable is True
    assert result.evaluated_at == NOW.isoformat()


def test_accounting_field_names_can_be_validated_directly() -> None:
    row = {
        "database_data_as_of": (
            NOW - timedelta(minutes=10)
        ).isoformat(),
        "database_content_valid_until": (
            NOW + timedelta(hours=1)
        ).isoformat(),
        "database_refresh_due_at": (
            NOW + timedelta(minutes=30)
        ).isoformat(),
    }

    result = _evaluate(row)

    assert result.complete is True
    assert result.usable is True
    assert result.evaluation == "VALID"


def test_accounting_lifecycle_status_cannot_be_masked_by_fresh_dates() -> None:
    row = {
        "database_data_as_of": (
            NOW - timedelta(minutes=10)
        ).isoformat(),
        "database_content_valid_until": (
            NOW + timedelta(hours=1)
        ).isoformat(),
        "database_refresh_due_at": (
            NOW + timedelta(minutes=30)
        ).isoformat(),
        "database_lifecycle_status": "STALE",
    }

    result = _evaluate(row)

    assert result.usable is False
    assert result.expired is True
    assert result.evaluation == "INVALID_LIFECYCLE"


def test_deadline_is_expired_at_exact_boundary() -> None:
    result = _evaluate(
        _current_row(content_valid_until=NOW.isoformat())
    )

    assert result.usable is False
    assert result.expired is True
    assert result.evaluation == "EXPIRED_CONTENT_VALID_UNTIL"
