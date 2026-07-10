from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS


class NasdaqMarketInfoProvider:
    source = "Nasdaq Market Info"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_nasdaq_market_info:
            return _status("disabled", "nasdaq_market_info_disabled", started)
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout_nasdaq_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(self.settings.nasdaq_market_info_url, headers=_json_headers()),
                    timeout=min(float(self.settings.timeout_nasdaq_seconds), 15.0),
                )
                response.raise_for_status()
                payload = response.json()
        except TimeoutError:
            return _status("provider_timeout", "Nasdaq market-info request timed out", started)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "Nasdaq market-info request failed", started)

        data = payload.get("data") or {}
        if isinstance(data, list):
            data = data[0] if data else {}
        now = datetime.now(UTC)
        status = data.get("marketIndicator") or data.get("mrktStatus") or data.get("marketStatus")
        return {
            "status": "found" if data else "not_found",
            "provider": self.source,
            "source": "Nasdaq",
            "source_url": self.settings.nasdaq_market_info_url,
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(minutes=15)),
            "market": "NASDAQ",
            "country": data.get("country") or "United States",
            "current_status": status,
            "market_countdown": data.get("marketCountDown"),
            "is_business_day": data.get("isBusinessDay"),
            "previous_trade_date": data.get("previousTradeDate"),
            "next_trade_date": data.get("nextTradeDate"),
            "timezone": "America/New_York",
            "sessions": {
                "premarket": {"open": data.get("preMarketOpeningTime"), "close": data.get("preMarketClosingTime")},
                "regular": {"open": data.get("marketOpeningTime"), "close": data.get("marketClosingTime")},
                "after_hours": {
                    "open": data.get("afterHoursMarketOpeningTime"),
                    "close": data.get("afterHoursMarketClosingTime"),
                },
            },
            "raw_timestamps": {
                key: data.get(key)
                for key in (
                    "preMarketOpeningTime",
                    "preMarketClosingTime",
                    "marketOpeningTime",
                    "marketClosingTime",
                    "afterHoursMarketOpeningTime",
                    "afterHoursMarketClosingTime",
                )
            },
            "diagnostics": {"data_present": bool(data), "keys": sorted(data.keys())[:50] if isinstance(data, dict) else []},
            "warnings": [] if data else ["nasdaq_market_info_empty_payload"],
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "Nasdaq Market Info",
        "source": "Nasdaq",
        "source_url": "https://api.nasdaq.com/api/market-info",
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(minutes=15)),
        "market": "NASDAQ",
        "country": "United States",
        "current_status": None,
        "is_business_day": None,
        "sessions": {},
        "diagnostics": {"data_present": False},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_headers() -> dict[str, str]:
    return {**REQUEST_HEADERS, "User-Agent": "Mozilla/5.0", "Accept": "application/json", "Origin": "https://www.nasdaq.com"}
