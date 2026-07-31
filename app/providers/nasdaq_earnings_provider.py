from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.core.senior_analyst_policy import (
    MNQ_EARNINGS_SELECTION_POLICY,
    MNQ_PRIMARY_SYMBOLS,
)
from app.providers.calendar_utils import REQUEST_HEADERS
from app.services.economic_value_parser import parse_economic_value, parse_int_value


class NasdaqEarningsProvider:
    source = "Nasdaq Earnings Calendar"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self, *, days: int | None = None) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_nasdaq_earnings:
            return _status("disabled", "nasdaq_earnings_disabled", started)
        requested_days = (
            MNQ_EARNINGS_SELECTION_POLICY.lookahead_days
            if days is None
            else max(int(days), 1)
        )
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
                    for offset in range(requested_days)
                ]
                responses = await asyncio.gather(*tasks)
                retrieved_at = datetime.now(UTC).replace(microsecond=0)
                content_valid_until = retrieved_at + timedelta(hours=12)
                for event_date, rows, error in responses:
                    if error:
                        errors.append(f"{event_date.isoformat()}:{error}")
                        continue
                    for row in rows:
                        events.append(
                            _normalize(
                                row,
                                event_date,
                                retrieved_at=retrieved_at,
                                content_valid_until=content_valid_until,
                            )
                        )
        except TimeoutError:
            return _status("provider_timeout", "Nasdaq earnings request timed out", started)
        relevant = _select_relevant(events)
        selection_counts = {
            "total_available": len(events),
            "relevant_count": len(relevant),
            "delivered_count": len(relevant),
            "excluded_count": len(events) - len(relevant),
        }
        return {
            "status": "found" if relevant else "not_found",
            "provider": self.source,
            "source": self.source,
            "source_url": self.settings.nasdaq_earnings_calendar_url,
            "retrieved_at": _z(retrieved_at),
            "data_as_of": _z(retrieved_at),
            "valid_until": _z(content_valid_until),
            "content_valid_until": _z(content_valid_until),
            "refresh_due_at": _z(content_valid_until),
            "events": relevant,
            "relevant_upcoming": relevant,
            "selection_counts": selection_counts,
            "diagnostics": {
                "days_requested": requested_days,
                "events_fetched": len(events),
                "events": len(relevant),
                "relevant_upcoming": len(relevant),
                "excluded_count": selection_counts["excluded_count"],
                "errors": errors[:10],
                "selection_policy": MNQ_EARNINGS_SELECTION_POLICY.policy_id,
            },
            "warnings": errors[:10],
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def _normalize(
    row: dict[str, Any],
    event_date: date,
    *,
    retrieved_at: datetime,
    content_valid_until: datetime,
) -> dict[str, Any]:
    eps = parse_economic_value(row.get("epsForecast"), default_unit="USD/share")
    market_cap = parse_economic_value(row.get("marketCap"), default_unit="USD")
    last_year_eps = parse_economic_value(row.get("lastYearEPS"), default_unit="USD/share")
    timing = _session(row.get("time"))
    source_url = "https://www.nasdaq.com/market-activity/earnings"
    return {
        "symbol": str(row.get("symbol") or "").upper(),
        "company": row.get("name"),
        "earnings_date": event_date.isoformat(),
        "date": event_date.isoformat(),
        "event_date": event_date.isoformat(),
        "event_at": None,
        "temporal_precision": (
            MNQ_EARNINGS_SELECTION_POLICY.date_only_temporal_precision
        ),
        "release_session": timing,
        "timing": timing,
        "fiscal_quarter_ending": row.get("fiscalQuarterEnding"),
        "eps_consensus": eps["value"] if eps["parse_status"] == "parsed" else None,
        "eps_estimate": eps["value"] if eps["parse_status"] == "parsed" else None,
        "consensus_type": "corporate_eps",
        "estimate_count": parse_int_value(row.get("noOfEsts")),
        "last_year_eps": last_year_eps["value"] if last_year_eps["parse_status"] == "parsed" else None,
        "last_year_report_date": None if str(row.get("lastYearRptDt") or "").upper() == "N/A" else row.get("lastYearRptDt"),
        "market_cap": market_cap["value"] if market_cap["parse_status"] == "parsed" else None,
        "source": "Nasdaq Earnings Calendar",
        "publisher": "Nasdaq",
        "distributor": "Nasdaq",
        "acquisition_provider": "NASDAQ",
        "source_url": source_url,
        "retrieved_at": _z(retrieved_at),
        "data_as_of": _z(retrieved_at),
        "valid_until": _z(content_valid_until),
        "content_valid_until": _z(content_valid_until),
        "refresh_due_at": _z(content_valid_until),
        "lineage": [
            {
                "field": field,
                "source": "Nasdaq Earnings Calendar",
                "source_field": source_field,
                "publisher": "Nasdaq",
                "distributor": "Nasdaq",
                "acquisition_provider": "NASDAQ",
                "source_url": source_url,
            }
            for field, source_field in {
                "symbol": "symbol",
                "event_date": "date",
                "timing": "time",
                "eps_estimate": "epsForecast",
            }.items()
        ],
    }


def _session(value: Any) -> str:
    text = str(value or "").lower()
    if "pre" in text:
        return "time-pre-market"
    if "after" in text:
        return "time-after-hours"
    return MNQ_EARNINGS_SELECTION_POLICY.date_only_timing


def _is_relevant(event: dict[str, Any]) -> bool:
    symbol = str(event.get("symbol") or "").upper()
    return symbol in MNQ_PRIMARY_SYMBOLS


def _select_relevant(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = [event for event in events if _is_relevant(event)]
    selected.sort(
        key=lambda event: tuple(
            str(event.get(field) or "")
            for field in MNQ_EARNINGS_SELECTION_POLICY.sort_fields
        )
    )
    return selected


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
    retrieved_at = datetime.now(UTC).replace(microsecond=0)
    content_valid_until = retrieved_at + timedelta(hours=6)
    return {
        "status": status,
        "provider": "Nasdaq Earnings Calendar",
        "source": "Nasdaq",
        "source_url": "https://api.nasdaq.com/api/calendar/earnings",
        "retrieved_at": _z(retrieved_at),
        "data_as_of": _z(retrieved_at),
        "valid_until": _z(content_valid_until),
        "content_valid_until": _z(content_valid_until),
        "refresh_due_at": _z(content_valid_until),
        "events": [],
        "relevant_upcoming": [],
        "selection_counts": {
            "total_available": 0,
            "relevant_count": 0,
            "delivered_count": 0,
            "excluded_count": 0,
        },
        "diagnostics": {
            "events_fetched": 0,
            "events": 0,
            "relevant_upcoming": 0,
            "excluded_count": 0,
        },
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


def _z(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace(
        "+00:00",
        "Z",
    )
