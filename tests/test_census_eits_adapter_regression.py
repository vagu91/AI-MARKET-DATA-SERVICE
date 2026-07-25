from __future__ import annotations

import inspect
import json
import sqlite3
from dataclasses import fields
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers import census as census_module
from app.providers.census import (
    EITS_OUTPUT_FIELDS,
    EITS_PREDICATE_ONLY_FIELDS,
    CensusProvider,
    CensusSeriesMapping,
)
from app.providers.deterministic import (
    DeterministicProviderError,
    RetryClassification,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "census_eits_adapter_regression_redacted.json"
SENTINEL = "census-regression-secret-sentinel"
PERIOD = "2026-05"
TIME_SLOT_DATE = "2026-05-01 00:00:00.0"
TIME_SLOT_NAME = "May2026"

INDICATOR_CASES = (
    {
        "dataset": "MARTS",
        "series_id": "CENSUS:MARTS:RETAIL_SALES",
        "program": "MARTS",
        "category": "44X72",
        "data_type": "SM",
        "value": "101.25",
    },
    {
        "dataset": "ADVM3",
        "series_id": "CENSUS:ADVM3:DURABLE_GOODS",
        "program": "M3ADV",
        "category": "MDM",
        "data_type": "NO",
        "value": "102.50",
    },
    {
        "dataset": "RESCONST",
        "series_id": "CENSUS:RESCONST:HOUSING_STARTS",
        "program": "RESCONST",
        "category": "ASTARTS",
        "data_type": "TOTAL",
        "value": "103",
    },
    {
        "dataset": "RESCONST",
        "series_id": "CENSUS:RESCONST:BUILDING_PERMITS",
        "program": "RESCONST",
        "category": "APERMITS",
        "data_type": "TOTAL",
        "value": "104",
    },
    {
        "dataset": "FTD",
        "series_id": "CENSUS:FTD:TRADE_BALANCE",
        "program": "FTD",
        "category": "BOPGS",
        "data_type": "BAL",
        "value": "-105.75",
    },
)


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "census-regression.sqlite",
        "environment": "test",
        "census_api_key": SENTINEL,
        "census_retry_attempts": 3,
        "census_cache_ttl_seconds": 1800,
        "source_policy_path": ROOT / "config" / "source_policy.json",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def row(
    case: dict[str, str],
    *,
    period: str = PERIOD,
    **overrides: str,
) -> dict[str, str]:
    output = {
        "time": period,
        "time_slot_id": "0",
        "time_slot_date": f"{period}-01 00:00:00.0",
        "time_slot_name": TIME_SLOT_NAME if period == PERIOD else "May2020",
        "program_code": case["program"],
        "category_code": case["category"],
        "data_type_code": case["data_type"],
        "seasonally_adj": "yes",
        "error_data": "no",
        "cell_value": case["value"],
    }
    if case["dataset"] == "RESCONST":
        output.update(
            {
                "geo_level_code": "US",
                "us": "1",
            }
        )
    output.update(overrides)
    return output


def competing_rows(case: dict[str, str]) -> list[dict[str, str]]:
    output = [
        row(case, program_code="WRONG_PROGRAM"),
        row(case, category_code="WRONG_CATEGORY"),
        row(case, data_type_code="WRONG_TYPE"),
        row(case, seasonally_adj="no"),
        row(case, error_data="yes"),
        row(case, error_data=""),
        row(case, time="2026-04"),
        row(case, time=""),
        row(case, time_slot_date="2026-04-01 00:00:00.0"),
        row(case, time_slot_date="2026-05-02 00:00:00.0"),
        row(case, cell_value="not-a-number"),
    ]
    if case["dataset"] == "RESCONST":
        output.extend(
            [
                row(case, geo_level_code="MW", us=""),
                row(case, geo_level_code="US", us="2"),
            ]
        )
    return output


def sibling_resconst_row(case: dict[str, str]) -> list[dict[str, str]]:
    if case["dataset"] != "RESCONST":
        return []
    sibling_id = (
        "CENSUS:RESCONST:BUILDING_PERMITS"
        if case["series_id"].endswith("HOUSING_STARTS")
        else "CENSUS:RESCONST:HOUSING_STARTS"
    )
    sibling = next(item for item in INDICATOR_CASES if item["series_id"] == sibling_id)
    return [row(sibling)]


async def fetch_dataset(
    tmp_path: Path,
    *,
    dataset: str,
    rows: list[dict[str, Any]],
    database_name: str,
):
    configured = settings(tmp_path, database_path=tmp_path / database_name)
    provider = CensusProvider(
        ProviderCacheRepository(configured.database_path),
        configured,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": rows})
        ),
    )
    return await provider.fetch_dataset(dataset, period=PERIOD)


def observation_for(envelope, series_id: str):
    return next(
        item for item in envelope.observations if item.observation_id == series_id
    )


def test_redacted_forensic_fixture_reproduces_live_temporal_shape() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    encoded = json.dumps(fixture, sort_keys=True)

    assert fixture["credential"] == "<redacted>"
    assert fixture["live_response_shape"] == {
        "time": PERIOD,
        "time_slot_id": "0",
        "time_slot_date": TIME_SLOT_DATE,
        "time_slot_name": TIME_SLOT_NAME,
    }
    assert len(fixture["official_tuples"]) == 5
    assert fixture["resconst_national_query"] == {
        "for": "us:*",
        "expected_geo_level_code": "US",
        "expected_us": "1",
    }
    assert {
        (
            item["dataset"],
            item["program_code"],
            item["category_code"],
            item["data_type_code"],
        )
        for item in fixture["official_tuples"]
    } == {
        (
            item["dataset"],
            item["program"],
            item["category"],
            item["data_type"],
        )
        for item in INDICATOR_CASES
    }
    fixture_rows = [
        *fixture["marts_competing_rows"],
        *fixture["ambiguous_exact_rows"],
    ]
    assert all(
        item["time_slot_id"] == "0" and item["time"] == PERIOD
        for item in fixture_rows
    )
    assert any(
        item["time_slot_date"] != TIME_SLOT_DATE
        for item in fixture["marts_competing_rows"]
    )
    assert all(
        item["time_slot_date"] == TIME_SLOT_DATE
        for item in fixture["ambiguous_exact_rows"]
    )
    for category in ("ASTARTS", "APERMITS"):
        geography_rows = [
            item
            for item in fixture["resconst_geography_rows"]
            if item["category_code"] == category
        ]
        assert {item["geo_level_code"] for item in geography_rows} == {
            "MW",
            "NO",
            "SO",
            "US",
            "WE",
        }
        national = [
            item for item in geography_rows if item["geo_level_code"] == "US"
        ]
        assert len(national) == 1
        assert national[0]["us"] == "1"
    assert SENTINEL not in encoded
    assert "api_key" not in encoded.lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    INDICATOR_CASES,
    ids=[item["series_id"] for item in INDICATOR_CASES],
)
async def test_all_indicators_select_exact_live_shape_order_independently(
    tmp_path: Path,
    case: dict[str, str],
) -> None:
    rows = [
        *competing_rows(case),
        row(case),
        *sibling_resconst_row(case),
    ]
    first = await fetch_dataset(
        tmp_path,
        dataset=case["dataset"],
        rows=rows,
        database_name=f"{case['series_id'].replace(':', '-')}-first.sqlite",
    )
    reversed_result = await fetch_dataset(
        tmp_path,
        dataset=case["dataset"],
        rows=list(reversed(rows)),
        database_name=f"{case['series_id'].replace(':', '-')}-reversed.sqlite",
    )

    for envelope in (first, reversed_result):
        observation = observation_for(envelope, case["series_id"])
        assert observation.value == float(case["value"])
        assert observation.occurrence_id == f"{case['series_id']}:{PERIOD}"
        assert ":slot-" not in observation.occurrence_id
        assert observation.metadata["program_code"] == case["program"]
        assert observation.metadata["category_code"] == case["category"]
        assert observation.metadata["data_type_code"] == case["data_type"]
        assert observation.metadata["seasonally_adjusted"] is True
        assert observation.metadata["time"] == PERIOD
        assert observation.metadata["time_slot_id"] == "0"
        assert observation.metadata["time_slot_date"] == TIME_SLOT_DATE
        assert observation.metadata["time_slot_name"] == TIME_SLOT_NAME
        assert observation.metadata["original_value"] == case["value"]
        assert envelope.lineage["temporal_identity"] == (
            "time+parsed_time_slot_date"
        )
        assert SENTINEL not in envelope.model_dump_json()
        if case["dataset"] == "RESCONST":
            assert observation.metadata["query_geography_predicate"] == "us:*"
            assert observation.metadata["geo_level_code"] == "US"
            assert observation.metadata["us"] == "1"

    first_observation = observation_for(first, case["series_id"])
    reversed_observation = observation_for(reversed_result, case["series_id"])
    assert first_observation.value == reversed_observation.value
    assert first_observation.occurrence_id == reversed_observation.occurrence_id
    assert first_observation.metadata["time"] == reversed_observation.metadata["time"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    INDICATOR_CASES,
    ids=[item["series_id"] for item in INDICATOR_CASES],
)
async def test_all_indicators_zero_match_are_audited_no_data(
    tmp_path: Path,
    case: dict[str, str],
) -> None:
    envelope = await fetch_dataset(
        tmp_path,
        dataset=case["dataset"],
        rows=competing_rows(case),
        database_name=f"{case['series_id'].replace(':', '-')}-no-data.sqlite",
    )

    assert envelope.observations == []
    assert envelope.freshness == "NO_DATA"
    assert f"no_exact_census_mapping:{case['series_id']}" in (
        envelope.rejection_reasons
    )
    assert envelope.lineage["rejected_row_hashes"]
    assert envelope.telemetry["accepted"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    INDICATOR_CASES,
    ids=[item["series_id"] for item in INDICATOR_CASES],
)
async def test_all_indicators_ambiguous_match_are_not_materialized(
    tmp_path: Path,
    case: dict[str, str],
) -> None:
    second = row(case, cell_value="999.99", time_slot_id="7")
    envelope = await fetch_dataset(
        tmp_path,
        dataset=case["dataset"],
        rows=[row(case), second],
        database_name=f"{case['series_id'].replace(':', '-')}-ambiguous.sqlite",
    )

    assert envelope.observations == []
    assert envelope.freshness == "NO_DATA"
    assert f"ambiguous_census_mapping:{case['series_id']}" in (
        envelope.rejection_reasons
    )
    assert "ambiguous_census_mapping" in envelope.telemetry["anomalies"]
    assert len(envelope.lineage["rejected_row_hashes"]) >= 2
    assert SENTINEL not in envelope.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", ("MARTS", "ADVM3", "RESCONST", "FTD"))
async def test_census_query_keeps_time_predicate_separate_and_requests_value(
    tmp_path: Path,
    dataset: str,
) -> None:
    requests: list[httpx.Request] = []
    dataset_cases = [item for item in INDICATOR_CASES if item["dataset"] == dataset]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"data": [row(item) for item in dataset_cases]})

    configured = settings(tmp_path)
    provider = CensusProvider(
        ProviderCacheRepository(configured.database_path),
        configured,
        transport=httpx.MockTransport(handler),
    )
    envelope = await provider.fetch_dataset(dataset, period=PERIOD)

    assert len(requests) == 1
    params = requests[0].url.params
    get_fields = tuple(params["get"].split(","))
    assert get_fields[: len(EITS_OUTPUT_FIELDS)] == EITS_OUTPUT_FIELDS
    assert "cell_value" in get_fields
    assert "time_slot_date" in get_fields
    assert "time_slot_name" in get_fields
    assert "time" not in get_fields
    assert not EITS_PREDICATE_ONLY_FIELDS.intersection(get_fields)
    assert params["time"] == PERIOD
    assert params["key"] == SENTINEL
    if dataset in {"ADVM3", "RESCONST"}:
        assert params["for"] == "us:*"
    else:
        assert "for" not in params
    if dataset == "RESCONST":
        assert "geo_level_code" in get_fields
    else:
        assert "geo_level_code" not in get_fields
    assert SENTINEL not in envelope.model_dump_json()


@pytest.mark.asyncio
async def test_resconst_mixed_geographies_select_only_national_rows(
    tmp_path: Path,
) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    rows = fixture["resconst_geography_rows"]
    first = await fetch_dataset(
        tmp_path,
        dataset="RESCONST",
        rows=rows,
        database_name="resconst-geography-first.sqlite",
    )
    reversed_result = await fetch_dataset(
        tmp_path,
        dataset="RESCONST",
        rows=list(reversed(rows)),
        database_name="resconst-geography-reversed.sqlite",
    )

    expected_ids = {
        "CENSUS:RESCONST:HOUSING_STARTS",
        "CENSUS:RESCONST:BUILDING_PERMITS",
    }
    regional_values = {
        float(item["cell_value"])
        for item in rows
        if item["geo_level_code"] != "US"
    }
    for envelope in (first, reversed_result):
        assert {item.observation_id for item in envelope.observations} == expected_ids
        assert len(envelope.observations) == 2
        assert all(item.value not in regional_values for item in envelope.observations)
        assert all(
            item.metadata["geo_level_code"] == "US"
            and item.metadata["us"] == "1"
            and item.metadata["query_geography_predicate"] == "us:*"
            for item in envelope.observations
        )
        assert envelope.lineage["predicate_fields"] == ["for", "time"]
        assert SENTINEL not in envelope.model_dump_json()

    assert {
        item.observation_id: item.value for item in first.observations
    } == {
        item.observation_id: item.value for item in reversed_result.observations
    }


@pytest.mark.asyncio
async def test_resconst_without_national_rows_is_no_data(tmp_path: Path) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    regional_rows = [
        item
        for item in fixture["resconst_geography_rows"]
        if item["geo_level_code"] != "US"
    ]
    envelope = await fetch_dataset(
        tmp_path,
        dataset="RESCONST",
        rows=regional_rows,
        database_name="resconst-geography-no-us.sqlite",
    )

    assert envelope.observations == []
    assert envelope.freshness == "NO_DATA"
    assert set(envelope.rejection_reasons) == {
        "no_exact_census_mapping:CENSUS:RESCONST:HOUSING_STARTS",
        "no_exact_census_mapping:CENSUS:RESCONST:BUILDING_PERMITS",
    }
    assert envelope.lineage["rejected_row_hashes"]


@pytest.mark.asyncio
async def test_four_dataset_simulation_materializes_five_observations(
    tmp_path: Path,
) -> None:
    rows_by_dataset = {
        dataset: [
            row(case)
            for case in INDICATOR_CASES
            if case["dataset"] == dataset
        ]
        for dataset in ("MARTS", "ADVM3", "RESCONST", "FTD")
    }

    def handler(request: httpx.Request) -> httpx.Response:
        dataset = request.url.path.rsplit("/", 1)[-1].upper()
        return httpx.Response(200, json={"data": rows_by_dataset[dataset]})

    configured = settings(tmp_path)
    provider = CensusProvider(
        ProviderCacheRepository(configured.database_path),
        configured,
        transport=httpx.MockTransport(handler),
    )
    envelopes = [
        await provider.fetch_dataset(dataset, period=PERIOD)
        for dataset in ("MARTS", "ADVM3", "RESCONST", "FTD")
    ]

    assert [len(item.observations) for item in envelopes] == [1, 1, 2, 1]
    assert sum(len(item.observations) for item in envelopes) == 5
    assert all(SENTINEL not in item.model_dump_json() for item in envelopes)


def test_absolute_time_slot_semantics_are_completely_removed() -> None:
    mapping_fields = {item.name for item in fields(CensusSeriesMapping)}
    source = inspect.getsource(census_module)

    assert "time_slot_epoch" not in mapping_fields
    assert "_expected_time_slot_id" not in source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "classification"),
    [
        (400, RetryClassification.TERMINAL),
        (401, RetryClassification.AUTHENTICATION),
        (403, RetryClassification.AUTHENTICATION),
    ],
)
async def test_census_4xx_is_terminal_redacted_and_not_retried(
    tmp_path: Path,
    status_code: int,
    classification: RetryClassification,
) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(status_code, json={"error": "redacted"})

    configured = settings(tmp_path)
    provider = CensusProvider(
        ProviderCacheRepository(configured.database_path),
        configured,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(DeterministicProviderError) as captured:
        await provider.fetch_dataset("MARTS", period=PERIOD)

    assert attempts == 1
    assert captured.value.status_code == status_code
    assert captured.value.classification == classification
    assert SENTINEL not in str(captured.value)


@pytest.mark.asyncio
async def test_historical_period_refreshes_from_retrieval_without_ai_state(
    tmp_path: Path,
) -> None:
    configured = settings(tmp_path)
    database_path = configured.database_path
    historical_row = row(
        INDICATOR_CASES[0],
        period="2020-05",
        time_slot_id="0",
        time_slot_date="2020-05-01 00:00:00.0",
        time_slot_name="May2020",
        cell_value="106.00",
    )
    provider = CensusProvider(
        ProviderCacheRepository(database_path),
        configured,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": [historical_row]})
        ),
    )
    envelope = await provider.fetch_dataset("MARTS", period="2020-05")

    assert len(envelope.observations) == 1
    observation = envelope.observations[0]
    assert observation.next_refresh_at > observation.retrieved_at
    assert observation.next_refresh_at == observation.valid_until
    assert observation.occurrence_id.endswith("2020-05")
    assert ":slot-" not in observation.occurrence_id
    assert observation.provider_timestamp is None
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM market_context_outbox"
        ).fetchone()[0] == 0

    created = {path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")}
    assert created
    assert all(item.startswith(database_path.name) for item in created)
