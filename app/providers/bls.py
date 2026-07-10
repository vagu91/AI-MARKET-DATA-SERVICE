from datetime import UTC, datetime

import httpx

from app.core.cache import SQLiteCache
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, metadata


BLS_SERIES = {
    "CUSR0000SA0": "Consumer Price Index",
    "WPUFD4": "Producer Price Index: Final Demand",
    "CES0000000001": "Total Nonfarm Payrolls",
    "LNS14000000": "Unemployment Rate",
}


class BlsProvider(BaseProvider):
    source = "BLS"
    provider_type = ProviderType.API
    reliability = 0.93
    cache_key = "provider:bls:macro_latest"

    def __init__(self, cache: SQLiteCache, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        body: dict[str, object] = {"seriesid": list(BLS_SERIES.keys()), "latest": True}
        if self.settings.bls_api_key:
            body["registrationkey"] = self.settings.bls_api_key

        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            response = await client.post(self.settings.bls_base_url, json=body)
            response.raise_for_status()
            payload = response.json()

        series: dict[str, dict[str, object]] = {}
        latest_as_of: datetime | None = None
        for item in payload.get("Results", {}).get("series", []):
            series_id = item.get("seriesID")
            rows = item.get("data", [])
            if not series_id or not rows:
                continue
            latest = rows[0]
            year = int(latest["year"])
            period = latest["period"].replace("M", "")
            month = int(period) if period.isdigit() else 1
            data_as_of = datetime(year, month, 1, tzinfo=UTC)
            latest_as_of = max(latest_as_of, data_as_of) if latest_as_of else data_as_of
            series[series_id] = {
                "series_id": series_id,
                "name": BLS_SERIES.get(series_id, series_id),
                "value": float(latest["value"]),
                "units": None,
                "data_as_of": data_as_of.date().isoformat(),
                "source": self.source,
            }

        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=latest_as_of,
                freshness=Freshness.RECENT,
            ),
            data=series,
        )

