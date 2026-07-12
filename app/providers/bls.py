from datetime import UTC, datetime

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, metadata


BLS_SERIES = {
    "CUSR0000SA0": "Consumer Price Index",
    "WPUFD4": "Producer Price Index: Final Demand",
    "CES0000000001": "Total Nonfarm Payrolls",
    "LNS14000000": "Unemployment Rate",
    "CES0500000003": "Average Hourly Earnings of All Employees: Total Private",
}

BLS_FRED_FALLBACK_SERIES = {
    "CUSR0000SA0": "CPIAUCSL",
    "WPUFD4": "PPIFIS",
    "CES0000000001": "PAYEMS",
    "LNS14000000": "UNRATE",
    "CES0500000003": "CES0500000003",
}


class BlsProvider(BaseProvider):
    source = "BLS"
    provider_type = ProviderType.API
    reliability = 0.93
    cache_key = "provider:bls:macro_latest"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
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

        if payload.get("status") != "REQUEST_SUCCEEDED":
            message = "; ".join(str(item) for item in (payload.get("message") or [])) or "BLS request was not processed"
            if _is_bls_daily_threshold(message) and self.settings.fred_api_key:
                return await self._fetch_via_fred_fallback(message)
            raise RuntimeError(message)

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

        if not series:
            raise RuntimeError("BLS returned no series data")

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

    async def _fetch_via_fred_fallback(self, reason: str) -> ProviderResult:
        data: dict[str, dict[str, object]] = {}
        latest_as_of: datetime | None = None
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            for bls_series_id, fred_series_id in BLS_FRED_FALLBACK_SERIES.items():
                response = await client.get(
                    f"{self.settings.fred_base_url}/series/observations",
                    params={
                        "series_id": fred_series_id,
                        "api_key": self.settings.fred_api_key,
                        "file_type": "json",
                        "sort_order": "desc",
                        "limit": 1,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                observations = payload.get("observations") or []
                item = next((row for row in observations if row.get("value") not in (None, ".")), None)
                if not item:
                    continue
                data_as_of = datetime.fromisoformat(str(item["date"])).replace(tzinfo=UTC)
                latest_as_of = max(latest_as_of, data_as_of) if latest_as_of else data_as_of
                data[bls_series_id] = {
                    "series_id": bls_series_id,
                    "name": BLS_SERIES.get(bls_series_id, bls_series_id),
                    "value": float(item["value"]),
                    "units": None,
                    "data_as_of": item.get("date"),
                    "source": "BLS via FRED",
                    "source_url": f"{self.settings.fred_base_url}/series/observations?series_id={fred_series_id}",
                    "fallback_source": "FRED",
                    "fallback_reason": "bls_daily_threshold",
                }
        if not data:
            raise RuntimeError(f"BLS daily threshold reached and FRED fallback returned no data: {reason}")
        return ProviderResult(
            metadata=metadata(
                source="BLS via FRED fallback",
                provider_type=ProviderType.API,
                reliability=self.reliability,
                data_as_of=latest_as_of,
                freshness=Freshness.RECENT,
                is_fallback=True,
                errors=[f"BLS daily threshold reached; using FRED mirror: {reason}"],
            ),
            data=data,
        )


def _is_bls_daily_threshold(message: str) -> bool:
    lowered = message.lower()
    return "daily threshold" in lowered or "request could not be serviced" in lowered
