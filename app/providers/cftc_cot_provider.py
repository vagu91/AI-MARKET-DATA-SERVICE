from __future__ import annotations

import asyncio
import csv
from datetime import UTC, datetime, timedelta
from io import StringIO
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS

CFTC_FINANCIAL_FUTURES_URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
MNQ_CFTC_CONTRACT_CODE = "209747"
NEW_YORK = ZoneInfo("America/New_York")


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

        row = find_mnq_row(response.text)
        if row is None:
            return _status(
                "not_found",
                "No MNQ/209747 row found in official CFTC financial futures file.",
                started,
            )
        parsed = parse_cftc_financial_row(row)
        if not parsed["validation"]["valid"]:
            return _status(
                "rejected_invalid_cot_math",
                ";".join(parsed["validation"]["errors"]),
                started,
            )
        if parsed["report_date"] and parsed["report_date"] > datetime.now(UTC).date().isoformat():
            return _status("rejected_invalid_cot_math", "CFTC report_date is in the future.", started)
        publication = _publication_at(
            str(parsed["report_date"]),
            settings=self.settings,
        )
        next_publication = _next_publication_after(
            publication,
            settings=self.settings,
        )
        return {
            "status": "found",
            **parsed,
            "target_contract": "MNQ",
            "publication_date": _iso(publication),
            "data_as_of": _iso(publication),
            "valid_from": _iso(publication),
            "source": "CFTC",
            "source_url": CFTC_FINANCIAL_FUTURES_URL,
            "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "valid_until": _iso(next_publication),
            "next_refresh_at": _iso(next_publication),
            "reliability": 0.99,
            "source_tier": 1,
            "source_classification": "OFFICIAL",
            "is_official_source": True,
            "acquisition_method": "api_provider",
            "source_lineage": [
                {
                    "source": "CFTC",
                    "source_url": CFTC_FINANCIAL_FUTURES_URL,
                    "source_tier": 1,
                    "report_date": parsed["report_date"],
                    "publication_date": _iso(publication),
                }
            ],
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


def find_mnq_row(text: str) -> list[str] | None:
    for row in csv.reader(StringIO(text)):
        if not row:
            continue
        name = str(row[0] or "").upper()
        code = str(row[3] if len(row) > 3 else "").strip()
        if (
            code == MNQ_CFTC_CONTRACT_CODE
            and "MICRO E-MINI NASDAQ-100" in name
        ):
            return row
    return None


def parse_cftc_financial_row(row: list[str]) -> dict[str, Any]:
    market_name = _str(row, 0)
    report_date = _str(row, 2)
    code = _str(row, 3)
    report_type = _str(row, -1) or "FutOnly"
    dealer_long = _int(row, 8)
    dealer_short = _int(row, 9)
    dealer_spread = _int(row, 10)
    asset_long = _int(row, 11)
    asset_short = _int(row, 12)
    asset_spread = _int(row, 13)
    leveraged_long = _int(row, 14)
    leveraged_short = _int(row, 15)
    leveraged_spread = _int(row, 16)
    dealer_long_change = _int(row, 25)
    dealer_short_change = _int(row, 26)
    asset_long_change = _int(row, 28)
    asset_short_change = _int(row, 29)
    leveraged_long_change = _int(row, 31)
    leveraged_short_change = _int(row, 32)
    open_interest = _int(row, 7)
    groups = {
        "dealers": _group(
            dealer_long,
            dealer_short,
            dealer_spread,
            dealer_long_change,
            dealer_short_change,
        ),
        "asset_managers": _group(
            asset_long,
            asset_short,
            asset_spread,
            asset_long_change,
            asset_short_change,
        ),
        "leveraged_funds": _group(
            leveraged_long,
            leveraged_short,
            leveraged_spread,
            leveraged_long_change,
            leveraged_short_change,
        ),
    }
    validation_errors = _cot_validation_errors(
        code=code,
        market_name=market_name,
        open_interest=open_interest,
        groups=groups,
    )
    return {
        "report_date": report_date,
        "publication_date": None,
        "market_name": market_name,
        "cftc_contract_market_code": code,
        "report_type": report_type,
        **groups,
        "open_interest": open_interest,
        "validation": {
            "valid": not validation_errors,
            "errors": validation_errors,
            "checks": [
                "mnq_contract_code",
                "nonnegative_open_interest",
                "group_positions_within_open_interest",
                "net_arithmetic",
            ],
        },
    }


def _cot_validation_errors(
    *,
    code: str | None,
    market_name: str | None,
    open_interest: int | None,
    groups: dict[str, dict[str, int | None]],
) -> list[str]:
    errors: list[str] = []
    if code != MNQ_CFTC_CONTRACT_CODE or "MICRO E-MINI NASDAQ-100" not in str(
        market_name or ""
    ).upper():
        errors.append("mnq_contract_identity_mismatch")
    if open_interest is None or open_interest <= 0:
        errors.append("open_interest_missing_or_nonpositive")
    for name, group in groups.items():
        for field in ("long", "short", "spreading"):
            value = group.get(field)
            if value is None or value < 0:
                errors.append(f"{name}_{field}_invalid")
            elif open_interest is not None and value > open_interest:
                errors.append(f"{name}_{field}_exceeds_open_interest")
        if (
            group.get("long") is not None
            and group.get("short") is not None
            and group.get("net")
            != group["long"] - group["short"]
        ):
            errors.append(f"{name}_net_mismatch")
    return sorted(set(errors))


def _publication_at(report_date: str, *, settings: Settings) -> datetime:
    report_day = datetime.fromisoformat(report_date).date()
    weekday = int(settings.cftc_release_weekday)
    days = (weekday - report_day.weekday()) % 7
    publication_day = report_day + timedelta(days=days)
    publication_day += timedelta(days=int(settings.cftc_release_delay_days))
    hour, minute = _release_time(settings)
    return datetime.combine(
        publication_day,
        datetime.min.time().replace(hour=hour, minute=minute),
        NEW_YORK,
    ).astimezone(UTC)


def _next_publication_after(
    publication: datetime,
    *,
    settings: Settings,
) -> datetime:
    candidate = publication + timedelta(days=7)
    holidays = {
        item.strip()
        for item in str(settings.cftc_release_holidays or "").split(",")
        if item.strip()
    }
    while candidate.astimezone(NEW_YORK).date().isoformat() in holidays:
        candidate += timedelta(days=1)
    return candidate


def _release_time(settings: Settings) -> tuple[int, int]:
    try:
        return tuple(
            int(item)
            for item in str(settings.cftc_release_time_new_york).split(":", 1)
        )
    except (TypeError, ValueError):
        return 15, 30


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace(
        "+00:00",
        "Z",
    )


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
