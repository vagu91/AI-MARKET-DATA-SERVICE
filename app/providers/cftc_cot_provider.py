from __future__ import annotations

import asyncio
import csv
from datetime import UTC, datetime, timedelta
from io import StringIO
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS

CFTC_FINANCIAL_FUTURES_URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"


class CftcCotProvider:
    source = "CFTC Commitments of Traders"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch_nasdaq(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout_cot_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(
                        CFTC_FINANCIAL_FUTURES_URL,
                        headers=REQUEST_HEADERS,
                        timeout=min(float(self.settings.timeout_cot_seconds), 8.0),
                    ),
                    timeout=min(float(self.settings.timeout_cot_seconds), 8.0),
                )
                response.raise_for_status()
        except TimeoutError:
            return _status("provider_timeout", "CFTC request timed out", started)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "CFTC request failed", started)

        row = find_nasdaq_row(response.text)
        if row is None:
            return _status("not_found", "No Nasdaq COT row found in official CFTC financial futures file.", started)
        parsed = parse_cftc_financial_row(row)
        if parsed["report_date"] and parsed["report_date"] > datetime.now(UTC).date().isoformat():
            return _status("rejected_invalid_cot_math", "CFTC report_date is in the future.", started)
        return {
            "status": "found",
            **parsed,
            "source": "CFTC",
            "source_url": CFTC_FINANCIAL_FUTURES_URL,
            "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "valid_until": (datetime.now(UTC) + timedelta(days=7)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "reliability": 0.95,
            "attempted_sources": [CFTC_FINANCIAL_FUTURES_URL],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
            "warnings": [],
            "errors": [],
        }


def find_nasdaq_row(text: str) -> list[str] | None:
    rows = csv.reader(StringIO(text))
    candidates = []
    for row in rows:
        if not row:
            continue
        name = row[0].upper()
        code = row[3] if len(row) > 3 else ""
        if "NASDAQ MINI" in name:
            return row
        if "NASDAQ" in name and str(code).startswith("209"):
            candidates.append(row)
    return candidates[0] if candidates else None


def parse_cftc_financial_row(row: list[str]) -> dict[str, Any]:
    market_name = _str(row, 0)
    report_date = _str(row, 2)
    code = _str(row, 3)
    report_type = _str(row, -1) or "FutOnly"
    asset_long = _int(row, 8)
    asset_short = _int(row, 9)
    asset_spread = _int(row, 10)
    leveraged_long = _int(row, 11)
    leveraged_short = _int(row, 12)
    leveraged_spread = _int(row, 13)
    dealer_long = _int(row, 14)
    dealer_short = _int(row, 15)
    asset_long_change = _int(row, 24)
    asset_short_change = _int(row, 25)
    leveraged_long_change = _int(row, 27)
    leveraged_short_change = _int(row, 28)
    return {
        "report_date": report_date,
        "publication_date": None,
        "market_name": market_name,
        "cftc_contract_market_code": code,
        "report_type": report_type,
        "asset_managers": _group(asset_long, asset_short, asset_spread, asset_long_change, asset_short_change),
        "leveraged_funds": _group(leveraged_long, leveraged_short, leveraged_spread, leveraged_long_change, leveraged_short_change),
        "dealers": {
            "long": dealer_long,
            "short": dealer_short,
            "net": _net(dealer_long, dealer_short),
        },
        "open_interest": _int(row, 7),
    }


def _group(long_value: int | None, short_value: int | None, spreading: int | None, long_change: int | None, short_change: int | None) -> dict[str, int | None]:
    return {
        "long": long_value,
        "short": short_value,
        "spreading": spreading,
        "net": _net(long_value, short_value),
        "net_change_week": _net(long_change, short_change),
    }


def _net(long_value: int | None, short_value: int | None) -> int | None:
    if long_value is None or short_value is None:
        return None
    return long_value - short_value


def _int(row: list[str], index: int) -> int | None:
    try:
        value = row[index].strip()
    except IndexError:
        return None
    if value in {"", "."}:
        return None
    try:
        return int(value.replace(",", ""))
    except ValueError:
        return None


def _str(row: list[str], index: int) -> str | None:
    try:
        value = row[index].strip()
    except IndexError:
        return None
    return value or None


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "report_date": None,
        "publication_date": None,
        "market_name": None,
        "cftc_contract_market_code": None,
        "report_type": None,
        "asset_managers": {"long": None, "short": None, "spreading": None, "net": None, "net_change_week": None},
        "leveraged_funds": {"long": None, "short": None, "spreading": None, "net": None, "net_change_week": None},
        "dealers": {"long": None, "short": None, "net": None},
        "open_interest": None,
        "source": "CFTC",
        "source_url": CFTC_FINANCIAL_FUTURES_URL,
        "retrieved_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (now + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "reliability": 0.0,
        "attempted_sources": [CFTC_FINANCIAL_FUTURES_URL],
        "reason": reason,
        "next_retry_at": (now + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "duration_ms": int((now - started).total_seconds() * 1000),
        "warnings": [reason],
        "errors": [] if status in {"not_found", "access_restricted"} else [reason],
    }
