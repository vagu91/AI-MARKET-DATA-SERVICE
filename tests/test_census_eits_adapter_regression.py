from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.census import (
    EITS_OUTPUT_FIELDS,
    EITS_PREDICATE_ONLY_FIELDS,
    CensusProvider,
)
from app.providers.deterministic import (
    DeterministicProviderError,
    RetryClassification,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "census_eits_adapter_regression_redacted.json"
SENTINEL = "census-regression-secret-sentinel"


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
    *,
    period: str,
    slot: str,
    program: str,
    category: str,
    data_type: str,
    value: str,
    adjusted: str = "yes",
) -> dict[str, str]:
    return {
        "time": period,
        "time_slot_id": slot,
        "program_code": program,
        "category_code": category,
        "data_type_code": data_type,
        "seasonally_adj": adjusted,
        "error_data": "0.0",
        "cell_value": value,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dataset", "rows", "expected_ids", "expected_predicates"),
    [
        (
            "MARTS",
            [row(period="2026-05", slot="413", program="MARTS", category="44X72", data_type="SM", value="750123.40")],
            {"CENSUS:MARTS:RETAIL_SALES"},
            {"time"},
        ),
        (
            "ADVM3",
            [row(period="2026-05", slot="413", program="M3ADV", category="MDM", data_type="NO", value="301234.50")],
            {"CENSUS:ADVM3:DURABLE_GOODS"},
            {"time", "for"},
        ),
        (
            "RESCONST",
            [
                row(period="2026-05", slot="809", program="RESCONST", category="ASTARTS", data_type="TOTAL", value="1256"),
                row(period="2026-05", slot="809", program="RESCONST", category="APERMITS", data_type="TOTAL", value="1394"),
            ],
            {
                "CENSUS:RESCONST:HOUSING_STARTS",
                "CENSUS:RESCONST:BUILDING_PERMITS",
            },
            {"time"},
        ),
        (
            "FTD",
            [row(period="2026-05", slot="413", program="FTD", category="BOPGS", data_type="BAL", value="-71234.5")],
            {"CENSUS:FTD:TRADE_BALANCE"},
            {"time"},
        ),
    ],
)
async def test_census_query_and_exact_mapping_contract(
    tmp_path: Path,
    dataset: str,
    rows: list[dict[str, str]],
    expected_ids: set[str],
    expected_predicates: set[str],
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"data": rows})

    configured = settings(tmp_path)
    provider = CensusProvider(
        ProviderCacheRepository(configured.database_path),
        configured,
        transport=httpx.MockTransport(handler),
    )
    envelope = await provider.fetch_dataset(dataset, period="2026-05")

    assert len(requests) == 1
    params = requests[0].url.params
    get_fields = tuple(params["get"].split(","))
    assert get_fields == EITS_OUTPUT_FIELDS
    assert not EITS_PREDICATE_ONLY_FIELDS.intersection(get_fields)
    assert params["time"] == "2026-05"
    assert params["key"] == SENTINEL
    assert set(envelope.lineage["predicate_fields"]) == expected_predicates
    if dataset == "ADVM3":
        assert params["for"] == "us:*"
    else:
        assert "for" not in params

    assert {item.observation_id for item in envelope.observations} == expected_ids
    assert envelope.freshness == "CURRENT_RELEASE"
    assert envelope.rejection_reasons == []
    assert SENTINEL not in envelope.model_dump_json()
    assert len(envelope.request_fingerprint) == 64
    for observation in envelope.observations:
        assert observation.seasonal_adjustment in {"SA", "SAAR"}
        assert observation.next_refresh_at == observation.valid_until
        assert observation.next_refresh_at > observation.retrieved_at
        assert observation.provider_timestamp is None
        assert observation.metadata["reference_period"] == "2026-05"
        assert observation.metadata["raw_payload_hash"] == envelope.raw_payload_hash
        assert observation.metadata["request_fingerprint"] == envelope.request_fingerprint
        assert SENTINEL not in json.dumps(observation.metadata)


async def fetch_marts(
    tmp_path: Path,
    rows: list[dict[str, Any]],
    *,
    database_name: str,
):
    configured = settings(
        tmp_path,
        database_path=tmp_path / database_name,
    )
    provider = CensusProvider(
        ProviderCacheRepository(configured.database_path),
        configured,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": rows})
        ),
    )
    return await provider.fetch_dataset("MARTS", period="2026-05")


@pytest.mark.asyncio
async def test_multirow_selection_is_exact_and_order_independent(
    tmp_path: Path,
) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    rows = fixture["marts_rows"]
    first = await fetch_marts(tmp_path, rows, database_name="first.sqlite")
    reversed_result = await fetch_marts(
        tmp_path,
        list(reversed(rows)),
        database_name="reversed.sqlite",
    )

    assert len(first.observations) == len(reversed_result.observations) == 1
    expected = fixture["expected_exact_tuple"]
    for envelope in (first, reversed_result):
        observation = envelope.observations[0]
        assert observation.value == 750123.4
        assert observation.semantic_field == expected["semantic_field"]
        assert observation.metadata["category_code"] == expected["category_code"]
        assert observation.metadata["data_type_code"] == expected["data_type_code"]
        assert observation.metadata["seasonally_adjusted"] is True
        assert observation.metadata["time_slot_id"] == expected["time_slot_id"]
        assert observation.metadata["original_value"] == expected["value"]
        assert observation.metadata["precision"] == 2
        assert observation.occurrence_id == (
            "CENSUS:MARTS:RETAIL_SALES:2026-05:slot-413"
        )


@pytest.mark.asyncio
async def test_zero_exact_match_is_audited_no_data(tmp_path: Path) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    envelope = await fetch_marts(
        tmp_path,
        fixture["marts_rows"][:-1],
        database_name="no-data.sqlite",
    )

    assert envelope.observations == []
    assert envelope.freshness == "NO_DATA"
    assert envelope.warnings == ["no_exact_census_mapping"]
    assert envelope.rejection_reasons == [
        "no_exact_census_mapping:CENSUS:MARTS:RETAIL_SALES"
    ]
    assert envelope.lineage["rejected_row_hashes"]
    assert envelope.telemetry["accepted"] == 0
    assert envelope.telemetry["rejected"] == 1


@pytest.mark.asyncio
async def test_ambiguous_exact_match_is_not_materialized(tmp_path: Path) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    envelope = await fetch_marts(
        tmp_path,
        fixture["ambiguous_exact_rows"],
        database_name="ambiguous.sqlite",
    )

    assert envelope.observations == []
    assert envelope.freshness == "NO_DATA"
    assert envelope.rejection_reasons == [
        "ambiguous_census_mapping:CENSUS:MARTS:RETAIL_SALES"
    ]
    assert "ambiguous_census_mapping" in envelope.telemetry["anomalies"]
    assert len(envelope.lineage["rejected_row_hashes"]) == 2
    assert all(len(item) == 64 for item in envelope.lineage["rejected_row_hashes"])
    assert SENTINEL not in envelope.model_dump_json()


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
        await provider.fetch_dataset("MARTS", period="2026-05")

    assert attempts == 1
    assert captured.value.status_code == status_code
    assert captured.value.classification == classification
    assert SENTINEL not in str(captured.value)


@pytest.mark.asyncio
async def test_provider_only_census_path_creates_no_ai_state_or_external_files(
    tmp_path: Path,
) -> None:
    configured = settings(tmp_path)
    database_path = configured.database_path
    provider = CensusProvider(
        ProviderCacheRepository(database_path),
        configured,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "data": [
                        row(
                            period="2020-05",
                            slot="341",
                            program="MARTS",
                            category="44X72",
                            data_type="SM",
                            value="526441.00",
                        )
                    ]
                },
            )
        ),
    )
    envelope = await provider.fetch_dataset("MARTS", period="2020-05")

    assert len(envelope.observations) == 1
    observation = envelope.observations[0]
    assert observation.next_refresh_at > observation.retrieved_at
    assert observation.next_refresh_at == observation.valid_until
    assert observation.occurrence_id.endswith("2020-05:slot-341")
    assert observation.provider_timestamp is None
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0] == 0

    created = {path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")}
    assert created
    assert all(item.startswith(database_path.name) for item in created)
