import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from app.core.senior_analyst_policy import (
    MNQ_EARNINGS_SELECTION_POLICY,
    MNQ_PRIMARY_SYMBOLS,
)
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Impact, ProviderResult, ProviderType
from app.models.nasdaq import EarningsTiming
from app.providers.alpha_vantage import csv_rows
from app.providers.base import BaseProvider
from app.providers.fmp_earnings_calendar_provider import FmpEarningsCalendarProvider


class EarningsProvider(BaseProvider):
    source = "Mega-cap Earnings Calendar"
    provider_type = ProviderType.API
    reliability = 0.72
    cache_key = "provider:mega_cap_earnings:v3"

    def __init__(
        self,
        cache: ProviderCacheProtocol,
        settings: Settings,
        *,
        fmp_provider_factory: Callable[
            [ProviderCacheProtocol, Settings],
            FmpEarningsCalendarProvider,
        ] = FmpEarningsCalendarProvider,
    ) -> None:
        super().__init__(cache)
        self.settings = settings
        self._fmp_provider_factory = fmp_provider_factory

    async def fetch(self) -> ProviderResult:
        # This legacy wrapper is retained for its response schema only.  The
        # formerly embedded Alpha Vantage fallback was an uncertified runtime
        # leaf and is deliberately unreachable. FMP is a separately registered
        # and atomically probed provider, so the wrapper delegates only to it.
        return await self._fmp_provider_factory(
            self.cache,
            self.settings,
        ).fetch()


def parse_alpha_vantage_earnings_calendar(text: str, now: datetime) -> list[dict[str, object]]:
    stripped = text.strip()
    if stripped.startswith("{"):
        payload = json.loads(stripped)
        for key in ("Note", "Information", "Error Message"):
            if payload.get(key):
                raise ValueError(f"Alpha Vantage {key}: {payload[key]}")
    rows = csv_rows(text)
    events = []
    watchlist = set(MNQ_PRIMARY_SYMBOLS)
    window_start = now.date() - timedelta(
        days=MNQ_EARNINGS_SELECTION_POLICY.lookback_days
    )
    window_end = now.date() + timedelta(
        days=MNQ_EARNINGS_SELECTION_POLICY.lookahead_days
    )
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
        if not window_start <= event_date <= window_end:
            continue
        eps_estimate = _float(row.get("estimate") or row.get("epsEstimate"))
        events.append(
            {
                "symbol": symbol,
                "company": row.get("name") or row.get("companyName"),
                "date": event_date.isoformat(),
                "event_date": event_date.isoformat(),
                "event_at": None,
                "temporal_precision": (
                    MNQ_EARNINGS_SELECTION_POLICY.date_only_temporal_precision
                ),
                "timing": MNQ_EARNINGS_SELECTION_POLICY.date_only_timing,
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
    events.sort(
        key=lambda item: tuple(
            str(item.get(field) or "")
            for field in MNQ_EARNINGS_SELECTION_POLICY.sort_fields
        )
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
