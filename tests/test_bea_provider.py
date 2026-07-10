import httpx
import pytest
import respx

from app.core.cache import SQLiteCache
from app.core.config import Settings
from app.models.common import Freshness, ProviderType
from app.providers.bea import BeaProvider


def bea_payload(table: str) -> dict:
    rows_by_table = {
        "T10101": [
            {
                "LineNumber": "1",
                "LineDescription": "Gross domestic product",
                "TimePeriod": "2026Q1",
                "DataValue": "1.8",
                "CL_UNIT": "Percent change",
            }
        ],
        "T10106": [
            {
                "LineNumber": "1",
                "LineDescription": "Gross domestic product",
                "TimePeriod": "2026Q1",
                "DataValue": "23500.1",
                "CL_UNIT": "Billions of chained (2017) dollars",
            }
        ],
        "T20805": [
            {
                "LineNumber": "1",
                "LineDescription": "Personal consumption expenditures (PCE)",
                "TimePeriod": "2026M05",
                "DataValue": "22,059,839",
                "CL_UNIT": "Millions of dollars",
            }
        ],
        "T20804": [
            {
                "LineNumber": "25",
                "LineDescription": "PCE excluding food and energy",
                "TimePeriod": "2026M05",
                "DataValue": "126.456",
                "CL_UNIT": "Index",
            }
        ],
        "T20600": [
            {
                "LineNumber": "1",
                "LineDescription": "Personal income",
                "TimePeriod": "2026M05",
                "DataValue": "25,123,456",
                "CL_UNIT": "Millions of dollars",
            },
            {
                "LineNumber": "28",
                "LineDescription": "Less: Personal outlays",
                "TimePeriod": "2026M05",
                "DataValue": "22,947,517",
                "CL_UNIT": "Millions of dollars",
            },
        ],
    }
    return {"BEAAPI": {"Results": {"Data": rows_by_table[table]}}}


def settings(tmp_path) -> Settings:
    env_file = tmp_path / ".env"
    env_file.write_text("BEA_API_KEY=test-secret\n", encoding="utf-8")
    return Settings(_env_file=env_file, bea_base_url="https://bea.test/api/data")


@pytest.mark.asyncio
async def test_bea_provider_returns_requested_macro_series(tmp_path) -> None:
    provider = BeaProvider(SQLiteCache(tmp_path / "cache.sqlite3"), settings(tmp_path))

    def handler(request: httpx.Request) -> httpx.Response:
        table = request.url.params["TableName"]
        return httpx.Response(200, json=bea_payload(table))

    with respx.mock:
        respx.get("https://bea.test/api/data").mock(side_effect=handler)
        result = await provider.fetch()

    assert result.metadata.source == "BEA"
    assert result.metadata.provider_type == ProviderType.API
    assert result.metadata.freshness == Freshness.RECENT
    assert result.metadata.is_fallback is False
    assert result.metadata.errors == []

    assert set(result.data) == {
        "BEA:GDP",
        "BEA:REAL_GDP",
        "BEA:PCE",
        "BEA:CORE_PCE",
        "BEA:PERSONAL_INCOME",
        "BEA:PERSONAL_SPENDING",
    }
    assert result.data["BEA:REAL_GDP"]["name"] == "Real GDP"
    assert result.data["BEA:CORE_PCE"]["value"] == 126.456
    assert result.data["BEA:PERSONAL_SPENDING"]["value"] == 22947517.0


@pytest.mark.asyncio
async def test_bea_provider_reports_missing_series_without_breaking_response(tmp_path) -> None:
    provider = BeaProvider(SQLiteCache(tmp_path / "cache.sqlite3"), settings(tmp_path))

    def handler(request: httpx.Request) -> httpx.Response:
        table = request.url.params["TableName"]
        payload = bea_payload(table)
        if table == "T20804":
            payload = {"BEAAPI": {"Results": {"Data": []}}}
        return httpx.Response(200, json=payload)

    with respx.mock:
        respx.get("https://bea.test/api/data").mock(side_effect=handler)
        result = await provider.fetch()

    assert "BEA:CORE_PCE" not in result.data
    assert "BEA:PCE" in result.data
    assert result.metadata.is_fallback is False
    assert any("T20804 returned no data" in error for error in result.metadata.errors)

