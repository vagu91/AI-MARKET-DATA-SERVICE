from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from app.core.config import Settings
from app.services.deterministic_actual_resolver import PROVIDERS
from app.services.official_actual_semantics import OFFICIAL_METRICS
from app.services.provider_adapter_factory import create_registered_adapter
from app.services.provider_capability_registry import (
    MARKET_FACT_REPOSITORY_DATASET_QUERIES,
    dataset_policy_by_id,
    dataset_runtime_provider_order,
    provider_by_id,
)
from app.services.senior_analyst_projection_v1 import (
    FED_FUNDS_RATE_SERIES,
    MACRO_DATASET_SERIES,
    TARGET_RANGE_SERIES,
    TREASURY_RATE_SERIES,
)


def test_official_actual_runtime_adapters_are_registry_derived() -> None:
    required = {spec.provider for spec in OFFICIAL_METRICS.values()}

    assert required <= set(PROVIDERS)
    for provider_id, adapter_type in PROVIDERS.items():
        registration = provider_by_id(provider_id)
        assert (
            f"{adapter_type.__module__}:{adapter_type.__qualname__}"
            == registration.adapter_path
        )


def test_projection_repository_series_are_registry_derived() -> None:
    expected = {
        dataset_id: frozenset(query.series_ids)
        for dataset_id, query in (
            MARKET_FACT_REPOSITORY_DATASET_QUERIES.items()
        )
    }

    assert TREASURY_RATE_SERIES == expected["treasury_rates"]
    assert FED_FUNDS_RATE_SERIES == expected["fed_funds"]
    assert TARGET_RANGE_SERIES == expected["target_range"]
    assert MACRO_DATASET_SERIES == {
        dataset_id: expected[dataset_id]
        for dataset_id in (
            "cpi",
            "ppi",
            "pce",
            "gdp",
            "employment",
            "wages",
            "nfp",
            "jobless_claims",
        )
    }


@pytest.mark.asyncio
async def test_target_range_runtime_fetches_real_fred_adapter_fields(
    tmp_path: Path,
) -> None:
    requested_series: list[str] = []
    values = {"DFEDTARL": "4.25", "DFEDTARU": "4.50"}

    def fred_response(request: httpx.Request) -> httpx.Response:
        series_id = str(request.url.params["series_id"])
        requested_series.append(series_id)
        return httpx.Response(
            200,
            json={
                "observations": [
                    {
                        "date": "2026-07-30",
                        "value": values[series_id],
                        "realtime_start": "2026-07-30",
                    }
                ]
            },
        )

    settings = Settings(
        environment="test",
        database_path=tmp_path / "target-range-runtime.sqlite",
        fred_api_key="controlled-test-key",
    )
    provider = create_registered_adapter(
        "FRED",
        object(),
        settings,
        transport=httpx.MockTransport(fred_response),
    )
    policy = dataset_policy_by_id("target_range")
    provider_order = dataset_runtime_provider_order(
        "target_range",
        (provider.source,),
    )

    result = await provider.fetch(
        series_ids=("DFEDTARL", "DFEDTARU"),
    )

    assert provider_order == (policy.primary_provider,) == ("FRED",)
    assert policy.fallback_providers == ()
    assert requested_series == ["DFEDTARL", "DFEDTARU"]
    assert set(result.data) == {"DFEDTARL", "DFEDTARU"}
    for series_id, item in result.data.items():
        capability = next(
            capability
            for capability in provider_by_id("FRED").capabilities
            if capability.dataset_id == "target_range"
            and capability.metric_id == series_id
        )
        assert set(capability.supported_fields) <= set(item)
        assert item["series_id"] == series_id
        assert item["value"] == float(values[series_id])
        assert item["data_as_of"] == "2026-07-30"
        assert item["source"] == "FRED"
        assert item["source_url"].endswith(f"/{series_id}")
        assert not {
            "actual",
            "lower_bound",
            "upper_bound",
            "released_at",
        }.intersection(item)
