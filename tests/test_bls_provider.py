from __future__ import annotations

import json
import httpx
import respx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.core.config import Settings
from app.providers.bls import BLS_FRED_FALLBACK_SERIES, BlsProvider
from app.services.official_actual_semantics import OFFICIAL_METRICS, derive_official_actual


async def test_bls_daily_threshold_uses_fred_mirror_with_bls_ids(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        bls_base_url="https://bls.test/publicAPI/v2/timeseries/data",
        fred_base_url="https://fred.test/fred",
        fred_api_key="fred-key",
    )
    provider = BlsProvider(ProviderCacheRepository(tmp_path / "cache.sqlite"), settings)

    with respx.mock(assert_all_called=True) as router:
        router.post("https://bls.test/publicAPI/v2/timeseries/data").mock(
            return_value=httpx.Response(
                200,
                json={
                    "status": "REQUEST_NOT_PROCESSED",
                    "message": ["Request could not be serviced, as the daily threshold has been reached."],
                    "Results": {},
                },
            )
        )
        for idx, fred_id in enumerate(BLS_FRED_FALLBACK_SERIES.values(), start=1):
            router.get("https://fred.test/fred/series/observations", params__contains={"series_id": fred_id}).mock(
                return_value=httpx.Response(
                    200,
                    json={"observations": [{"date": "2026-06-01", "value": str(idx)}]},
                )
            )
        result = await provider.fetch()
        fred_calls = [call for call in router.calls if "fred.test" in str(call.request.url)]

    assert result.metadata.source == "FRED fallback for unavailable BLS transport"
    assert result.metadata.is_fallback is True
    assert set(result.data) == set(BLS_FRED_FALLBACK_SERIES)
    assert all(item["source"] == "FRED" for item in result.data.values())
    assert all(item["official_adapter"] is False for item in result.data.values())
    assert all(len(item["observations"]) == 1 for item in result.data.values())
    assert fred_calls and all(call.request.url.params["limit"] == "25" for call in fred_calls)


async def test_bls_requests_multi_year_history_without_latest_and_derives_transformations(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        bls_base_url="https://bls.test/publicAPI/v2/timeseries/data",
    )
    provider = BlsProvider(ProviderCacheRepository(tmp_path / "cache.sqlite"), settings)
    periods = [(2025, month) for month in range(1, 13)] + [(2026, 1)]

    def rows(values: list[float]) -> list[dict[str, str]]:
        return [
            {"year": str(year), "period": f"M{month:02d}", "value": str(value)}
            for (year, month), value in reversed(list(zip(periods, values, strict=True)))
        ]

    payload = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {"series": [
            {"seriesID": "CUSR0000SA0", "data": rows([300 + index for index in range(13)])},
            {"seriesID": "CUUR0000SA0", "data": rows([300] + [301] * 11 + [306])},
            {"seriesID": "WPUFD4", "data": rows([250] + [251] * 11 + [255])},
            {"seriesID": "CES0500000003", "data": rows([30] + [30.1] * 11 + [31.2])},
            {"seriesID": "CES0000000001", "data": rows([150000 + index * 100 for index in range(13)])},
        ]},
    }
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=payload)

    with respx.mock(assert_all_called=True) as router:
        router.post(settings.bls_base_url).mock(side_effect=handler)
        result = await provider.fetch()

    assert "latest" not in captured
    assert int(captured["endyear"]) - int(captured["startyear"]) >= 2
    assert result.data["CUUR0000SA0"]["observations"][0]["period"] == "2025-01"
    assert result.data["CUUR0000SA0"]["observations"][-1]["period"] == "2026-01"
    assert len(result.data["CUUR0000SA0"]["observations"]) == 13
    assert result.data["CUUR0000SA0"]["official_adapter"] is True

    def derive(metric: str, series_id: str) -> str:
        return derive_official_actual(
            OFFICIAL_METRICS[metric], result.data[series_id], expected_period="2026-01",
            retrieved_at=result.metadata.retrieved_at.isoformat(), release_timestamp="2026-02-01T13:30:00Z",
        )["value"]

    assert derive("headline_cpi_mom", "CUSR0000SA0") == "0.3"
    assert derive("headline_cpi_yoy", "CUUR0000SA0") == "2.0"
    assert derive("headline_ppi_yoy", "WPUFD4") == "2.0"
    assert derive("average_hourly_earnings_yoy", "CES0500000003") == "4.0"
    assert derive("nonfarm_payrolls_change", "CES0000000001") == "100"
