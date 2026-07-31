from __future__ import annotations

from dataclasses import replace

import pytest

import app.services.provider_capability_registry as provider_registry
from app.services.diagnostics_service import (
    _calendar_fallback_provider_order,
    _ordered_dataset_runtime_blocks,
    _ordered_provider_attempts,
    _run_calendar_fallback_chain,
)
from app.services.request_provider_accounting import provider_attempt
from app.services.senior_analyst_projection_v1 import DATASET_POLICIES


class _CalendarRuntime:
    def __init__(self, successful_runtime: str | None = None) -> None:
        self.successful_runtime = successful_runtime
        self.calls: list[tuple[str, str]] = []

    async def provider(
        self,
        name: str,
        *,
        refresh: str,
    ) -> dict[str, object]:
        self.calls.append((name, refresh))
        return {
            "status": (
                "found"
                if name == self.successful_runtime
                else "not_found"
            ),
            "fetched_count": (
                1 if name == self.successful_runtime else 0
            ),
            "attempted": refresh != "false",
            "provider_calls": 1 if refresh != "false" else 0,
        }


@pytest.mark.asyncio
async def test_calendar_fallback_dispatch_tracks_central_policy_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = provider_registry.dataset_policy_by_id(
        "macro_calendar"
    )
    reordered = replace(
        original,
        fallback_providers=tuple(
            reversed(original.fallback_providers)
        ),
    )
    monkeypatch.setattr(
        provider_registry,
        "DATASET_SOURCE_POLICIES",
        tuple(
            reordered
            if policy.dataset_id == "macro_calendar"
            else policy
            for policy in provider_registry.DATASET_SOURCE_POLICIES
        ),
    )
    runtime = _CalendarRuntime(
        successful_runtime="xtb_economic_calendar"
    )

    order = _calendar_fallback_provider_order()
    output = await _run_calendar_fallback_chain(
        runtime,
        provider_order=order,
        refresh="force",
        primary_succeeded=False,
    )

    assert order == ("XTB", "INVESTING_ECONOMIC_CALENDAR")
    assert runtime.calls == [
        ("xtb_economic_calendar", "force"),
        ("investing_economic_calendar", "false"),
    ]
    assert list(output) == list(order)
    assert [
        output[provider_id]["provider_id"]
        for provider_id in order
    ] == list(order)


@pytest.mark.parametrize(
    "dataset_id",
    ("earnings", "market_schedule"),
)
def test_accounting_blocks_follow_policy_provider_identity(
    dataset_id: str,
) -> None:
    original = next(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id == dataset_id
    )
    order = (
        original.primary_provider,
        *original.fallback_providers,
    )
    reordered = replace(
        original,
        primary_provider=order[-1],
        fallback_providers=tuple(reversed(order[:-1])),
    )
    blocks = {
        f"runtime-{index}": {
            "dataset_id": dataset_id,
            "provider_id": provider_id,
            "marker": provider_id,
        }
        for index, provider_id in enumerate(order)
    }

    selected = _ordered_dataset_runtime_blocks(
        blocks,
        policy=reordered,
    )

    assert [block["marker"] for block in selected] == [
        reordered.primary_provider,
        *reordered.fallback_providers,
    ]


def test_vix_attempt_order_tracks_policy_and_short_circuits() -> None:
    fred = provider_attempt(
        "FRED",
        called=True,
        attempts=1,
        result="FAILED",
        execution_origin="PROVIDER_CALL",
    )
    cboe = provider_attempt(
        "CBOE",
        called=True,
        attempts=1,
        result="FOUND",
        execution_origin="PROVIDER_CALL",
    )

    attempts = _ordered_provider_attempts(
        ("CBOE", "FRED"),
        observed={"FRED": fred, "CBOE": cboe},
        database_valid=False,
    )

    assert attempts[0] == cboe
    assert attempts[1] == provider_attempt(
        "FRED",
        called=False,
        attempts=0,
        result="NOT_CALLED",
        not_called_reason="PRIOR_PROVIDER_SUCCEEDED",
        execution_origin="OBSERVED_SKIP",
    )


def test_unmapped_vix_policy_is_fail_closed() -> None:
    attempts = _ordered_provider_attempts(
        ("CBOE", "UNMAPPED"),
        observed={
            "CBOE": provider_attempt(
                "CBOE",
                called=True,
                attempts=1,
                result="FOUND",
                execution_origin="PROVIDER_CALL",
            )
        },
        database_valid=False,
    )

    assert all(
        attempt["called"] is False
        and attempt["not_called_reason"]
        == "RUNTIME_PROVIDER_MAPPING_UNAVAILABLE"
        for attempt in attempts
    )
    assert [attempt["provider"] for attempt in attempts] == [
        "CBOE",
        "UNMAPPED",
    ]
