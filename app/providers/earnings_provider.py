import json
from datetime import UTC, datetime, timedelta

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, Impact, ProviderResult, ProviderType
from app.models.nasdaq import EarningsTiming
from app.providers.alpha_vantage import csv_rows
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import REQUEST_HEADERS
from app.providers.fmp_earnings_calendar_provider import FmpEarningsCalendarProvider
from app.providers.mega_cap_snapshot_provider import MEGA_CAP_TICKERS


class EarningsProvider(BaseProvider):
    source = "Mega-cap Earnings Calendar"
    provider_type = ProviderType.API
    reliability = 0.72
    cache_key = "provider:mega_cap_earnings:v3"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        events = []
        errors = []
        now = datetime.now(UTC)
        fmp_result = await FmpEarningsCalendarProvider(self.cache, self.settings).fetch()
        fmp_data = fmp_result.data if isinstance(fmp_result.data, dict) else {}
        if fmp_data.get("events"):
            return fmp_result
        fmp_quality = fmp_data.get("data_quality") or {}
        fmp_messages = _dedupe_errors([*(fmp_quality.get("warnings") or []), *(fmp_quality.get("errors") or [])])
        fmp_attempted = int(fmp_quality.get("provider_calls") or 0) > 0
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            if self.settings.alpha_vantage_api_key:
                try:
                    response = await client.get(
                        self.settings.alpha_vantage_base_url,
                        params={
                            "function": "EARNINGS_CALENDAR",
                            "horizon": "3month",
                            "apikey": self.settings.alpha_vantage_api_key,
                        },
                        headers=REQUEST_HEADERS,
                    )
                    response.raise_for_status()
                    events = parse_alpha_vantage_earnings_calendar(response.text, now)
                    if events:
                        return ProviderResult(
                            metadata=metadata(
                                source="Alpha Vantage EARNINGS_CALENDAR",
                                provider_type=ProviderType.CSV,
                                reliability=0.78,
                                freshness=Freshness.RECENT,
                            ),
                            data={
                                "events": events,
                                "data_quality": {
                                    "errors": [],
                                    "warnings": fmp_messages,
                                    "fallback_used": fmp_attempted,
                                    "final_data_available": True,
                                    "no_data_found": False,
                                    "provider_failed": False,
                                    "rate_limited": False,
                                },
                            },
                        )
                    return ProviderResult(
                        metadata=metadata(
                            source="Alpha Vantage EARNINGS_CALENDAR",
                            provider_type=ProviderType.CSV,
                            reliability=0.78,
                            freshness=Freshness.RECENT,
                        ),
                        data={
                            "events": [],
                            "data_quality": {
                                "errors": ["No watchlist earnings found in requested window"],
                                    "warnings": fmp_messages,
                                    "fallback_used": fmp_attempted,
                                "final_data_available": True,
                                "no_data_found": True,
                                "provider_failed": False,
                                "rate_limited": False,
                            },
                        },
                    )
                except Exception as exc:
                    message = str(exc) or "Alpha Vantage EARNINGS_CALENDAR provider_failed"
                    errors.append(f"Alpha Vantage EARNINGS_CALENDAR {_category(message)}: {message}")

        if not self.settings.alpha_vantage_api_key:
            return fmp_result
        return ProviderResult(
            metadata=metadata(
                source="Alpha Vantage EARNINGS_CALENDAR" if self.settings.alpha_vantage_api_key else self.source,
                provider_type=self.provider_type,
                reliability=0.0,
                freshness=Freshness.UNKNOWN,
                errors=_dedupe_errors(errors),
            ),
            data={
                "events": [],
                "data_quality": {
                    "errors": _dedupe_errors(errors or ["Alpha Vantage API key is not configured"]),
                    "warnings": [],
                    "fallback_used": bool(errors),
                    "final_data_available": False,
                    "no_data_found": not bool(errors),
                    "provider_failed": any("provider_failed" in error for error in errors),
                    "rate_limited": any("rate_limited" in error for error in errors),
                },
            },
        )


def parse_alpha_vantage_earnings_calendar(text: str, now: datetime) -> list[dict[str, object]]:
    stripped = text.strip()
    if stripped.startswith("{"):
        payload = json.loads(stripped)
        for key in ("Note", "Information", "Error Message"):
            if payload.get(key):
                raise ValueError(f"Alpha Vantage {key}: {payload[key]}")
    rows = csv_rows(text)
    events = []
    watchlist = set(MEGA_CAP_TICKERS)
    for row in rows:
        symbol = (row.get("symbol") or row.get("Symbol") or "").upper()
        if symbol not in watchlist:
            continue
        date_value = row.get("reportDate") or row.get("fiscalDateEnding") or row.get("date")
        if not date_value:
            continue
        try:
            event_date = datetime.fromisoformat(date_value).date()
        except ValueError:
            continue
        if event_date < now.date():
            continue
        eps_estimate = _float(row.get("estimate") or row.get("epsEstimate"))
        events.append(
            {
                "symbol": symbol,
                "company": row.get("name") or row.get("companyName"),
                "date": event_date.isoformat(),
                "timing": EarningsTiming.UNKNOWN.value,
                "eps_estimate": eps_estimate,
                "eps_actual": None,
                "revenue_estimate": None,
                "revenue_actual": None,
                "source": "Alpha Vantage EARNINGS_CALENDAR",
                "source_url": "https://www.alphavantage.co/documentation/#earnings-calendar",
                "event_risk_level": (
                    Impact.HIGH.value
                    if symbol in {"NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG"}
                    else Impact.MEDIUM.value
                ),
                "reliability": 0.78,
            }
        )
    return events


def parse_yahoo_earnings(symbol: str, payload: dict, now: datetime) -> dict | None:
    results = payload.get("quoteSummary", {}).get("result") or []
    if not results:
        return None
    calendar = results[0].get("calendarEvents", {})
    earnings = calendar.get("earnings", {})
    dates = earnings.get("earningsDate") or []
    if not dates:
        return None
    raw_ts = dates[0].get("raw")
    if raw_ts is None:
        return None
    event_dt = datetime.fromtimestamp(raw_ts, tz=UTC)
    if event_dt < now - timedelta(days=1):
        return None
    return {
        "symbol": symbol.upper(),
        "company": None,
        "date": event_dt.date().isoformat(),
        "timing": EarningsTiming.UNKNOWN.value,
        "eps_estimate": _raw(earnings.get("earningsAverage")),
        "eps_actual": None,
        "revenue_estimate": _raw(earnings.get("revenueAverage")),
        "revenue_actual": None,
        "source": "Yahoo Finance Calendar Events",
        "source_url": f"https://finance.yahoo.com/quote/{symbol}/analysis",
        "event_risk_level": Impact.HIGH.value if symbol.upper() in {"NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG"} else Impact.MEDIUM.value,
        "reliability": 0.62,
    }


def _raw(value) -> float | None:
    if isinstance(value, dict):
        value = value.get("raw")
    return value if isinstance(value, int | float) else None


def _float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _dedupe_errors(errors: list[str]) -> list[str]:
    deduped = []
    for error in errors:
        if error and error not in deduped:
            deduped.append(error)
    return deduped


def _category(message: str) -> str:
    lowered = message.lower()
    if "rate" in lowered or "thank you for using alpha vantage" in lowered or "25 requests" in lowered:
        return "rate_limited"
    return "provider_failed"
