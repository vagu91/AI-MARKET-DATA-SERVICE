from datetime import UTC, datetime

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, metadata


BLS_SERIES = {
    "CUSR0000SA0": "Consumer Price Index",
    "CUUR0000SA0": "Consumer Price Index NSA",
    "CUSR0000SA0L1E": "Core Consumer Price Index",
    "CUUR0000SA0L1E": "Core Consumer Price Index NSA",
    "WPSFD4": "Producer Price Index: Final Demand SA",
    "WPUFD4": "Producer Price Index: Final Demand",
    "CES0000000001": "Total Nonfarm Payrolls",
    "LNS14000000": "Unemployment Rate",
    "CES0500000003": "Average Hourly Earnings of All Employees: Total Private",
}

BLS_SERIES_META = {
    "CUSR0000SA0": ("index", "SA"), "CUUR0000SA0": ("index", "NSA"),
    "CUSR0000SA0L1E": ("index", "SA"), "CUUR0000SA0L1E": ("index", "NSA"),
    "WPSFD4": ("index", "SA"), "WPUFD4": ("index", "NSA"),
    "CES0000000001": ("thousands of jobs", "SA"),
    "LNS14000000": ("percent", "SA"),
    "CES0500000003": ("dollars per hour", "SA"),
}

BLS_FRED_FALLBACK_SERIES = {
    "CUSR0000SA0": "CPIAUCSL",
    "WPUFD4": "PPIFIS",
    "CES0000000001": "PAYEMS",
    "LNS14000000": "UNRATE",
    "CES0500000003": "CES0500000003",
}

BLS_HISTORY_YEARS = 2
BLS_FRED_HISTORY_LIMIT = 25
BLS_API_CANONICAL_URL = "https://www.bls.gov/developers/api_signature_v2.htm"


class BlsProvider(BaseProvider):
    source = "BLS"
    provider_type = ProviderType.API
    reliability = 0.93
    cache_key = "provider:bls:macro_latest"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        end_year = datetime.now(UTC).year
        body: dict[str, object] = {
            "seriesid": list(BLS_SERIES.keys()),
            "startyear": str(end_year - BLS_HISTORY_YEARS),
            "endyear": str(end_year),
        }
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
            unit, seasonal_adjustment = BLS_SERIES_META.get(series_id, (None, None))
            observations = []
            for row in rows:
                raw_period = str(row.get("period") or "")
                if not raw_period.startswith("M") or not raw_period[1:].isdigit() or int(raw_period[1:]) not in range(1, 13):
                    continue
                observations.append({
                    "period": f"{row.get('year')}-{int(raw_period[1:]):02d}",
                    "value": row.get("value"),
                    "release_vintage": row.get("latest") or row.get("revision") or "initial",
                })
            observations.sort(key=lambda row: str(row["period"]))
            if not observations:
                continue
            latest_observation = observations[-1]
            data_as_of = datetime.fromisoformat(f"{latest_observation['period']}-01").replace(tzinfo=UTC)
            latest_as_of = max(latest_as_of, data_as_of) if latest_as_of else data_as_of
            series[series_id] = {
                "series_id": series_id,
                "name": BLS_SERIES.get(series_id, series_id),
                "value": float(latest_observation["value"]),
                "units": unit,
                "frequency": "monthly",
                "seasonal_adjustment": seasonal_adjustment,
                "data_as_of": data_as_of.date().isoformat(),
                "observations": observations,
                "source": self.source,
                "source_url": self.settings.bls_base_url,
                "canonical_url": BLS_API_CANONICAL_URL,
                "source_domain": "bls.gov",
                "provider_adapter": "BLS_OFFICIAL_API",
                "official_adapter": True,
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
                        "limit": BLS_FRED_HISTORY_LIMIT,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                observations = payload.get("observations") or []
                valid = [row for row in observations if row.get("value") not in (None, ".") and row.get("date")]
                valid.sort(key=lambda row: str(row["date"]))
                if not valid:
                    continue
                item = valid[-1]
                data_as_of = datetime.fromisoformat(str(item["date"])).replace(tzinfo=UTC)
                latest_as_of = max(latest_as_of, data_as_of) if latest_as_of else data_as_of
                data[bls_series_id] = {
                    "series_id": bls_series_id,
                    "name": BLS_SERIES.get(bls_series_id, bls_series_id),
                    "value": float(item["value"]),
                    "units": BLS_SERIES_META.get(bls_series_id, (None, None))[0],
                    "data_as_of": item.get("date"),
                    "frequency": "monthly",
                    "seasonal_adjustment": BLS_SERIES_META.get(bls_series_id, (None, None))[1],
                    "observations": [
                        {"period": str(row["date"])[:7], "value": row["value"], "release_vintage": "FRED"}
                        for row in valid
                    ],
                    "source": "FRED",
                    "source_url": f"{self.settings.fred_base_url}/series/observations?series_id={fred_series_id}",
                    "canonical_url": f"https://fred.stlouisfed.org/series/{fred_series_id}",
                    "source_domain": "fred.stlouisfed.org",
                    "provider_adapter": "FRED_FALLBACK_API",
                    "official_adapter": False,
                    "fallback_source": "FRED",
                    "fallback_reason": "bls_daily_threshold",
                }
        if not data:
            raise RuntimeError(f"BLS daily threshold reached and FRED fallback returned no data: {reason}")
        return ProviderResult(
            metadata=metadata(
                source="FRED fallback for unavailable BLS transport",
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
