from datetime import UTC, datetime

import httpx

from app.core.cache import SQLiteCache
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, ProviderError, latest_observation, metadata


FRED_SERIES = {
    "VIXCLS": "CBOE Volatility Index: VIX",
    "DGS2": "2-Year Treasury Constant Maturity Rate",
    "DGS10": "10-Year Treasury Constant Maturity Rate",
    "FEDFUNDS": "Effective Federal Funds Rate",
    "NFCI": "Chicago Fed National Financial Conditions Index",
    "SOFR": "Secured Overnight Financing Rate",
    "T10Y2Y": "10-Year Treasury Minus 2-Year Treasury",
}


class FredProvider(BaseProvider):
    source = "FRED"
    provider_type = ProviderType.API
    reliability = 0.95
    cache_key = "provider:fred:macro_latest"

    def __init__(self, cache: SQLiteCache, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        if not self.settings.fred_api_key:
            raise ProviderError("FRED API key is not configured")

        data: dict[str, dict[str, object]] = {}
        latest_as_of: datetime | None = None
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            for series_id, name in FRED_SERIES.items():
                response = await client.get(
                    f"{self.settings.fred_base_url}/series/observations",
                    params={
                        "series_id": series_id,
                        "api_key": self.settings.fred_api_key,
                        "file_type": "json",
                        "sort_order": "desc",
                        "limit": 1,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                item = latest_observation(payload.get("observations", []))
                if not item:
                    continue
                value = float(item["value"])
                data[series_id] = {
                    "series_id": series_id,
                    "name": name,
                    "value": value,
                    "units": "index" if series_id in {"VIXCLS", "NFCI"} else "percent",
                    "data_as_of": item.get("date"),
                    "source": self.source,
                }
                observed_at = datetime.fromisoformat(item["date"]).replace(tzinfo=UTC)
                latest_as_of = max(latest_as_of, observed_at) if latest_as_of else observed_at

        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=latest_as_of,
                freshness=Freshness.RECENT,
            ),
            data=data,
        )
