from __future__ import annotations

import httpx
import respx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.core.config import Settings
from app.providers.bls import BLS_FRED_FALLBACK_SERIES, BlsProvider


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

    assert result.metadata.source == "BLS via FRED fallback"
    assert result.metadata.is_fallback is True
    assert set(result.data) == set(BLS_FRED_FALLBACK_SERIES)
    assert all(item["source"] == "BLS via FRED" for item in result.data.values())
