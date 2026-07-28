from datetime import UTC, datetime

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import (
    BaseProvider,
    ProviderDisabled,
    ProviderError,
    latest_observation,
    metadata,
)


FRED_SERIES = {
    "VIXCLS": "CBOE Volatility Index: VIX",
    "DGS2": "2-Year Treasury Constant Maturity Rate",
    "DGS10": "10-Year Treasury Constant Maturity Rate",
    "DGS30": "30-Year Treasury Constant Maturity Rate",
    "FEDFUNDS": "Effective Federal Funds Rate",
    "DFF": "Federal Funds Effective Rate",
    "DFEDTARL": "Federal Funds Target Range - Lower Limit",
    "DFEDTARU": "Federal Funds Target Range - Upper Limit",
    "NFCI": "Chicago Fed National Financial Conditions Index",
    "SOFR": "Secured Overnight Financing Rate",
    "T10Y2Y": "10-Year Treasury Minus 2-Year Treasury",
    "T10Y3M": "10-Year Treasury Minus 3-Month Treasury",
    "ICSA": "Initial Claims",
    "WALCL": "Federal Reserve Total Assets",
    "HSN1F": "New One Family Houses Sold: United States",
}
FRED_FREQUENCIES = {
    "FEDFUNDS": "monthly",
    "NFCI": "weekly",
    "ICSA": "weekly",
    "WALCL": "weekly",
    "HSN1F": "monthly",
}
FRED_UNITS = {
    "HSN1F": "thousands_annual_rate",
}


class FredProvider(BaseProvider):
    source = "FRED"
    provider_type = ProviderType.API
    reliability = 0.95
    cache_key = "provider:fred:macro_latest"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(
        self,
        *,
        series_ids: list[str] | tuple[str, ...] | None = None,
    ) -> ProviderResult:
        if not self.settings.fred_enabled:
            raise ProviderDisabled("FRED provider is disabled")
        if not self.settings.fred_api_key:
            raise ProviderError("FRED API key is not configured")

        data: dict[str, dict[str, object]] = {}
        latest_as_of: datetime | None = None
        async with httpx.AsyncClient(
            timeout=self.settings.fred_timeout_seconds,
            follow_redirects=False,
        ) as client:
            selected = tuple(series_ids or FRED_SERIES)
            for series_id in selected:
                name = FRED_SERIES.get(series_id)
                if name is None:
                    continue
                response = await client.get(
                    f"{self.settings.fred_base_url}/series/observations",
                    params={
                        "series_id": series_id,
                        "api_key": self.settings.fred_api_key,
                        "file_type": "json",
                        "sort_order": "desc",
                        "limit": 10,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                observations = [
                    row
                    for row in payload.get("observations", [])
                    if isinstance(row, dict) and row.get("value") not in {None, "", "."}
                ]
                item = latest_observation(observations)
                if not item:
                    continue
                value = float(item["value"])
                data[series_id] = {
                    "series_id": series_id,
                    "name": name,
                    "value": value,
                    "units": FRED_UNITS.get(
                        series_id,
                        "index" if series_id in {"VIXCLS", "NFCI"} else "thousands of claims" if series_id == "ICSA" else "percent",
                    ),
                    "data_as_of": item.get("date"),
                    "observation_date": item.get("date"),
                    "retrieved_at": datetime.now(UTC).isoformat(),
                    "frequency": FRED_FREQUENCIES.get(series_id, "daily"),
                    "source": self.source,
                    "source_url": (
                        f"https://fred.stlouisfed.org/series/{series_id}"
                    ),
                    "source_domain": "fred.stlouisfed.org",
                    "authority_tier": 1,
                    "trigger_class": "NON_TRIGGERING",
                    "provider_adapter": "FRED_OFFICIAL_API",
                    "official_adapter": True,
                    "seasonal_adjustment": "SAAR" if series_id == "HSN1F" else None,
                    "observations": [
                        {
                            "period": row.get("date"),
                            "value": row.get("value"),
                            "release_vintage": row.get("realtime_start"),
                        }
                        for row in observations
                    ],
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
