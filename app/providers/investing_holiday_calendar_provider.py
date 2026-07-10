from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS


class InvestingHolidayCalendarProvider:
    source = "Investing Holiday Calendar"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self, *, start: datetime | None = None, end: datetime | None = None) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_investing_holidays:
            return _status("disabled", "investing_holidays_disabled", started)
        start = start or started
        end = end or start + timedelta(days=45)
        params: dict[str, Any] = {
            "domain_id": self.settings.investing_domain_id,
            "limit": 100,
            "start_date": start.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "end_date": end.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "country_ids": self.settings.investing_country_ids,
        }
        pages: list[dict[str, Any]] = []
        cursor = None
        try:
            async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
                for _ in range(10):
                    request_params = dict(params)
                    if cursor:
                        request_params["cursor"] = cursor
                    response = await asyncio.wait_for(
                        client.get(self.settings.investing_holiday_calendar_api_url, params=request_params, headers=_json_headers()),
                        timeout=min(float(self.settings.http_timeout_seconds), 12.0),
                    )
                    response.raise_for_status()
                    payload = response.json()
                    pages.append(payload)
                    cursor = payload.get("next_page_cursor")
                    if not cursor:
                        break
        except TimeoutError:
            return _status("provider_timeout", "Investing holidays request timed out", started)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "Investing holidays request failed", started)
        holidays = [_normalize(item) for page in pages for item in (page.get("holidays") or [])]
        relevant = [item for item in holidays if _is_us_relevant(item)]
        return {
            "status": "found" if holidays else "not_found",
            "provider": self.source,
            "source": self.source,
            "source_url": self.settings.investing_holiday_calendar_api_url,
            "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "valid_until": (datetime.now(UTC) + timedelta(hours=12)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "holidays": holidays,
            "relevant_holidays": relevant,
            "diagnostics": {"pages_fetched": len(pages), "holidays": len(holidays), "relevant_us_market_holidays": len(relevant)},
            "warnings": [] if holidays else ["investing_holidays_no_rows"],
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def _normalize(item: dict[str, Any]) -> dict[str, Any]:
    exchange = item.get("exchange") or {}
    country = item.get("country") or {}
    return {
        "holiday_id": item.get("holiday_id") or item.get("id"),
        "holiday_name": item.get("holiday_name") or item.get("name"),
        "date": str(item.get("holiday_start") or item.get("date") or "")[:10] or None,
        "start_utc": item.get("holiday_start"),
        "end_utc": item.get("holiday_end"),
        "source_timezone": item.get("timezone"),
        "country": country.get("name") if isinstance(country, dict) else item.get("country"),
        "country_id": item.get("country_id") or (country.get("id") if isinstance(country, dict) else None),
        "exchange": exchange.get("name") if isinstance(exchange, dict) else item.get("exchange"),
        "exchange_id": item.get("exchange_id") or (exchange.get("id") if isinstance(exchange, dict) else None),
        "exchange_closed": bool(item.get("exchange_closed")),
        "holiday_type": item.get("holiday_type") or item.get("type") or ("exchange_closed" if item.get("exchange_closed") else "holiday"),
        "source": "Investing Holiday Calendar",
        "source_url": "https://www.investing.com/holiday-calendar/",
    }


def _is_us_relevant(item: dict[str, Any]) -> bool:
    text = f"{item.get('country')} {item.get('exchange')} {item.get('holiday_name')}".upper()
    return any(token in text for token in ("UNITED STATES", "US", "NASDAQ", "NYSE", "CME"))


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    return {
        "status": status,
        "provider": "Investing Holiday Calendar",
        "source": "Investing Holiday Calendar",
        "source_url": "https://endpoints.investing.com/pd-instruments/v1/calendars/holidays",
        "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (datetime.now(UTC) + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "holidays": [],
        "relevant_holidays": [],
        "diagnostics": {"pages_fetched": 0, "holidays": 0},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _json_headers() -> dict[str, str]:
    return {**REQUEST_HEADERS, "Accept": "application/json"}
