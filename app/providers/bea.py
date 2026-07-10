from datetime import UTC, datetime
from itertools import groupby
from operator import itemgetter

import httpx

from app.core.cache import SQLiteCache
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, ProviderError, metadata, redact_sensitive


# BEA NIPA mappings are intentionally explicit.
# T10101 line 1 is the existing GDP growth series used by the service.
# T10106 line 1 adds Real GDP in chained dollars.
# T20805 line 1 adds monthly PCE current-dollar level.
# T20804 line 25 is BEA's monthly PCE price index excluding food and energy (Core PCE).
# T20600 line 1 adds monthly Personal Income.
# T20600 line 28 adds Personal Spending via BEA's "Personal outlays" line.
BEA_SERIES = [
    {
        "series_id": "BEA:GDP",
        "name": "Gross Domestic Product",
        "table": "T10101",
        "frequency": "Q",
        "line_number": "1",
    },
    {
        "series_id": "BEA:REAL_GDP",
        "name": "Real GDP",
        "table": "T10106",
        "frequency": "Q",
        "line_number": "1",
    },
    {
        "series_id": "BEA:PCE",
        "name": "Personal Consumption Expenditures",
        "table": "T20805",
        "frequency": "M",
        "line_number": "1",
    },
    {
        "series_id": "BEA:CORE_PCE",
        "name": "Core PCE Price Index",
        "table": "T20804",
        "frequency": "M",
        "line_number": "25",
    },
    {
        "series_id": "BEA:PERSONAL_INCOME",
        "name": "Personal Income",
        "table": "T20600",
        "frequency": "M",
        "line_number": "1",
    },
    {
        "series_id": "BEA:PERSONAL_SPENDING",
        "name": "Personal Spending",
        "table": "T20600",
        "frequency": "M",
        "line_number": "28",
    },
]


class BeaProvider(BaseProvider):
    source = "BEA"
    provider_type = ProviderType.API
    reliability = 0.94
    cache_key = "provider:bea:macro_latest:v2"

    def __init__(self, cache: SQLiteCache, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        if not self.settings.bea_api_key:
            raise ProviderError("BEA API key is not configured")

        series: dict[str, dict[str, object]] = {}
        errors: list[str] = []
        latest_as_of: datetime | None = None
        specs = sorted(BEA_SERIES, key=itemgetter("table", "frequency"))
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            for (table, frequency), table_specs_iter in groupby(
                specs,
                key=itemgetter("table", "frequency"),
            ):
                table_specs = list(table_specs_iter)
                try:
                    response = await client.get(
                        self.settings.bea_base_url,
                        params={
                            "UserID": self.settings.bea_api_key,
                            "method": "GetData",
                            "datasetname": "NIPA",
                            "TableName": table,
                            "Frequency": frequency,
                            "Year": "X",
                            "ResultFormat": "JSON",
                        },
                    )
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:
                    errors.append(f"BEA {table} request failed: {redact_sensitive(str(exc))}")
                    continue

                api_error = payload.get("BEAAPI", {}).get("Results", {}).get("Error")
                if api_error:
                    message = api_error.get("APIErrorDescription") or str(api_error)
                    errors.append(f"BEA {table} returned error: {message}")
                    continue

                rows = payload.get("BEAAPI", {}).get("Results", {}).get("Data", [])
                if not rows:
                    errors.append(f"BEA {table} returned no data")
                    continue

                for spec in table_specs:
                    item = self._latest_row_for_line(rows, str(spec["line_number"]))
                    if item is None:
                        errors.append(
                            f"BEA {table} line {spec['line_number']} unavailable for {spec['name']}"
                        )
                        continue
                    value = str(item.get("DataValue", "")).replace(",", "")
                    if not value or value == "---":
                        errors.append(
                            f"BEA {table} line {spec['line_number']} has no value for {spec['name']}"
                        )
                        continue
                    period = str(item.get("TimePeriod", ""))
                    data_as_of = self._period_to_datetime(period)
                    latest_as_of = max(latest_as_of, data_as_of) if latest_as_of else data_as_of
                    series[str(spec["series_id"])] = {
                        "series_id": spec["series_id"],
                        "name": spec["name"],
                        "value": float(value),
                        "units": item.get("CL_UNIT"),
                        "data_as_of": period,
                        "source": self.source,
                    }

        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability if series else 0.0,
                data_as_of=latest_as_of,
                freshness=Freshness.RECENT if series else Freshness.UNKNOWN,
                errors=errors,
            ),
            data=series,
        )

    @staticmethod
    def _latest_row_for_line(
        rows: list[dict[str, object]],
        line_number: str,
    ) -> dict[str, object] | None:
        for row in reversed(rows):
            if str(row.get("LineNumber")) == line_number:
                return row
        return None

    @staticmethod
    def _period_to_datetime(period: str) -> datetime:
        year = int(period[:4])
        if "Q" in period:
            quarter_month = {"Q1": 3, "Q2": 6, "Q3": 9, "Q4": 12}.get(period[-2:], 1)
            return datetime(year, quarter_month, 1, tzinfo=UTC)
        if "M" in period:
            return datetime(year, int(period[-2:]), 1, tzinfo=UTC)
        return datetime(year, 1, 1, tzinfo=UTC)
