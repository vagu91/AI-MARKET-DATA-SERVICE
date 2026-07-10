from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS
from app.providers.mega_cap_snapshot_provider import MEGA_CAP_TICKERS
from app.services.economic_value_parser import parse_economic_value, parse_int_value


class NasdaqEarningsProvider:
    source = "Nasdaq Earnings Calendar"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self, *, days: int = 14) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_nasdaq_earnings:
            return _status("disabled", "nasdaq_earnings_disabled", started)
        events: list[dict[str, Any]] = []
        errors: list[str] = []
        today = datetime.now(UTC).date()
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout_earnings_seconds) as client:
                tasks = [
                    _fetch_day(
                        client,
                        url=self.settings.nasdaq_earnings_calendar_url,
                        event_date=today + timedelta(days=offset),
                        timeout_seconds=min(float(self.settings.timeout_earnings_seconds), 10.0),
                    )
                    for offset in range(days)
                ]
                for event_date, rows, error in await asyncio.gather(*tasks):
                    if error:
                        errors.append(f"{event_date.isoformat()}:{error}")
                        continue
                    for row in rows:
                        events.append(_normalize(row, event_date))
        except TimeoutError:
            return _status("provider_timeout", "Nasdaq earnings request timed out", started)
        relevant = [event for event in events if _is_relevant(event)]
        return {
            "status": "found" if events else "not_found",
            "provider": self.source,
            "source": self.source,
            "source_url": self.settings.nasdaq_earnings_calendar_url,
            "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "valid_until": (datetime.now(UTC) + timedelta(hours=12)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "events": events,
            "relevant_upcoming": relevant,
            "diagnostics": {"days_requested": days, "events": len(events), "relevant_upcoming": len(relevant), "errors": errors[:10]},
            "warnings": errors[:10],
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def _normalize(row: dict[str, Any], event_date: date) -> dict[str, Any]:
    eps = parse_economic_value(row.get("epsForecast"), default_unit="USD/share")
    market_cap = parse_economic_value(row.get("marketCap"), default_unit="USD")
    last_year_eps = parse_economic_value(row.get("lastYearEPS"), default_unit="USD/share")
    return {
        "symbol": str(row.get("symbol") or "").upper(),
        "company": row.get("name"),
        "earnings_date": event_date.isoformat(),
        "date": event_date.isoformat(),
        "release_session": _session(row.get("time")),
        "fiscal_quarter_ending": row.get("fiscalQuarterEnding"),
        "eps_consensus": eps["value"] if eps["parse_status"] == "parsed" else None,
        "consensus_type": "corporate_eps",
        "estimate_count": parse_int_value(row.get("noOfEsts")),
        "last_year_eps": last_year_eps["value"] if last_year_eps["parse_status"] == "parsed" else None,
        "last_year_report_date": None if str(row.get("lastYearRptDt") or "").upper() == "N/A" else row.get("lastYearRptDt"),
        "market_cap": market_cap["value"] if market_cap["parse_status"] == "parsed" else None,
        "source": "Nasdaq Earnings Calendar",
        "source_url": "https://www.nasdaq.com/market-activity/earnings",
        "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (datetime.now(UTC) + timedelta(hours=12)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }


def _session(value: Any) -> str:
    text = str(value or "").lower()
    if "pre" in text:
        return "time-pre-market"
    if "after" in text:
        return "time-after-hours"
    return "time-not-supplied"


def _is_relevant(event: dict[str, Any]) -> bool:
    symbol = str(event.get("symbol") or "").upper()
    return symbol in set(MEGA_CAP_TICKERS) or symbol in {"QQQ", "SMH", "SOXX"}


async def _fetch_day(
    client: httpx.AsyncClient,
    *,
    url: str,
    event_date: date,
    timeout_seconds: float,
) -> tuple[date, list[dict[str, Any]], str | None]:
    try:
        response = await asyncio.wait_for(
            client.get(url, params={"date": event_date.isoformat()}, headers=_json_headers()),
            timeout=max(timeout_seconds, 1.0),
        )
        response.raise_for_status()
        payload = response.json()
        rows = (((payload.get("data") or {}).get("rows")) or [])
        return event_date, [row for row in rows if isinstance(row, dict)], None
    except Exception as exc:
        return event_date, [], str(exc) or type(exc).__name__


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    return {
        "status": status,
        "provider": "Nasdaq Earnings Calendar",
        "source": "Nasdaq",
        "source_url": "https://api.nasdaq.com/api/calendar/earnings",
        "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (datetime.now(UTC) + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "events": [],
        "relevant_upcoming": [],
        "diagnostics": {"events": 0, "relevant_upcoming": 0},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _json_headers() -> dict[str, str]:
    return {
        **REQUEST_HEADERS,
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/market-activity/earnings",
    }
