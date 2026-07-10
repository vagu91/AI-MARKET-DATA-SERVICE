from __future__ import annotations

import asyncio
import html
import json
import re
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS


MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


class MarketBeatHolidaysProvider:
    source = "MarketBeat Stock Market Holidays"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_marketbeat_holidays:
            return _status("disabled", "marketbeat_holidays_disabled", started, self.settings.marketbeat_holidays_url)
        try:
            async with httpx.AsyncClient(timeout=self.settings.marketbeat_timeout_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(self.settings.marketbeat_holidays_url, headers=_html_headers()),
                    timeout=min(float(self.settings.marketbeat_timeout_seconds), 15.0),
                )
                response.raise_for_status()
                text = response.text
        except TimeoutError:
            return _status("provider_timeout", "marketbeat_holidays_timeout", started, self.settings.marketbeat_holidays_url)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "marketbeat_holidays_failed", started, self.settings.marketbeat_holidays_url)

        parsed = parse_marketbeat_holidays_html(text)
        events = parsed["events"]
        warnings: list[str] = []
        fallback_used = False
        if not events:
            fallback_used = True
            events = parse_marketbeat_holidays_json_ld(text)
            if not events:
                warnings.append("marketbeat_holidays_no_rows")
        parser_strategy = "json_ld_fallback" if fallback_used and events else "html_table"

        now = datetime.now(UTC)
        deduped, duplicate_count, conflict_count = deduplicate_marketbeat_events(events)
        years = sorted({int(str(item["date"])[:4]) for item in deduped if item.get("date")})
        return {
            "status": "found" if deduped else "not_found",
            "provider": self.source,
            "source": "MarketBeat",
            "source_url": self.settings.marketbeat_holidays_url,
            "source_type": "secondary_calendar",
            "official_exchange_source": False,
            "is_official": False,
            "parser_strategy": parser_strategy,
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(hours=self.settings.marketbeat_holidays_ttl_hours)),
            "holidays": deduped,
            "relevant_holidays": deduped,
            "diagnostics": {
                "tables_seen": parsed["tables_seen"],
                "closed_rows": parsed["closed_rows"],
                "early_close_rows": parsed["early_close_rows"],
                "json_ld_fallback_used": fallback_used,
                "parser_strategy": parser_strategy,
                "years_covered": years,
                "duplicate_count": duplicate_count,
                "conflict_count": conflict_count,
                "closed_count": sum(1 for item in deduped if item.get("session_status") == "closed"),
                "early_close_count": sum(1 for item in deduped if item.get("session_status") == "early_close"),
            },
            "warnings": warnings,
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
            "service_role": "data provider only",
        }


def parse_marketbeat_holidays_html(text: str) -> dict[str, Any]:
    parser = _TableParser()
    parser.feed(text)
    events: list[dict[str, Any]] = []
    closed_rows = 0
    early_close_rows = 0
    for table in parser.tables:
        if not table:
            continue
        headers = [_clean_cell(value) for value in table[0]]
        header_text = " ".join(headers).lower()
        if "nasdaq" not in header_text or "nyse" not in header_text:
            continue
        is_partial = "partial" in header_text or "1:00" in header_text or "early" in header_text
        year_columns = [(idx, _int(headers[idx])) for idx in range(1, len(headers))]
        year_columns = [(idx, year) for idx, year in year_columns if year]
        for row in table[1:]:
            if not row:
                continue
            name = _clean_cell(row[0])
            if not name:
                continue
            if is_partial:
                early_close_rows += 1
            else:
                closed_rows += 1
            for idx, year in year_columns:
                cell = _clean_cell(row[idx] if idx < len(row) else "")
                if not cell:
                    continue
                status = "closed" if "fully closed" in cell.lower() else ("early_close" if is_partial else "closed")
                parsed_date = parse_marketbeat_date(cell, year)
                if not parsed_date:
                    continue
                events.append(_event(name=name, date=parsed_date, session_status=status))
    return {
        "events": events,
        "tables_seen": len(parser.tables),
        "closed_rows": closed_rows,
        "early_close_rows": early_close_rows,
    }


def parse_marketbeat_holidays_json_ld(text: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for match in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, flags=re.I | re.S):
        raw = html.unescape(match.group(1)).strip()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for item in _walk_json(payload):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("holiday_name") or "").strip()
            date_value = str(item.get("startDate") or item.get("date") or item.get("holiday_start") or "")[:10]
            if not name or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_value):
                continue
            text_blob = " ".join(str(value) for value in item.values()).lower()
            session_status = "early_close" if any(token in text_blob for token in ("early close", "1:00", "partial")) else "closed"
            events.append(_event(name=name, date=date_value, session_status=session_status))
    return events


def deduplicate_marketbeat_events(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    duplicates = 0
    conflicts = 0
    for item in events:
        key = (str(item.get("date")), str(item.get("market") or "NASDAQ_NYSE"))
        existing = by_key.get(key)
        if not existing:
            by_key[key] = item
            continue
        duplicates += 1
        if existing.get("session_status") != item.get("session_status"):
            conflicts += 1
            if item.get("session_status") == "closed":
                by_key[key] = item
    return sorted(by_key.values(), key=lambda item: (item.get("date") or "", item.get("name") or "")), duplicates, conflicts


def parse_marketbeat_date(value: str, year: int) -> str | None:
    text = _clean_cell(value)
    if not text or "fully closed" in text.lower():
        return None
    match = re.search(r"\b([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,\s*(\d{4}))?\b", text)
    if not match:
        return None
    month = MONTHS.get(match.group(1).lower())
    day = _int(match.group(2))
    parsed_year = _int(match.group(3)) or year
    if not month or not day or not parsed_year:
        return None
    try:
        return datetime(parsed_year, month, day, tzinfo=UTC).date().isoformat()
    except ValueError:
        return None


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []
        elif tag == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell is not None and self._row is not None:
            self._row.append("".join(self._cell))
            self._cell = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            if any(_clean_cell(cell) for cell in self._row):
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            self.tables.append(self._table)
            self._table = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def _event(*, name: str, date: str, session_status: str) -> dict[str, Any]:
    return {
        "name": _clean_cell(name),
        "holiday_name": _clean_cell(name),
        "date": date,
        "market": "NASDAQ_NYSE",
        "session_status": session_status,
        "exchange_closed": session_status == "closed",
        "early_close_time_local": "13:00:00" if session_status == "early_close" else None,
        "timezone": "America/New_York",
        "source": "MarketBeat",
        "source_url": "https://www.marketbeat.com/stock-market-holidays/",
        "source_type": "secondary_calendar",
        "official_exchange_source": False,
        "is_official": False,
    }


def _walk_json(value: Any) -> list[Any]:
    if isinstance(value, list):
        return [child for item in value for child in _walk_json(item)]
    if isinstance(value, dict):
        return [value, *[child for child_value in value.values() for child in _walk_json(child_value)]]
    return []


def _status(status: str, reason: str, started: datetime, source_url: str) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "MarketBeat Stock Market Holidays",
        "source": "MarketBeat",
        "source_url": source_url,
        "source_type": "secondary_calendar",
        "official_exchange_source": False,
        "is_official": False,
        "parser_strategy": None,
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(hours=6)),
        "holidays": [],
        "relevant_holidays": [],
        "diagnostics": {"tables_seen": 0, "closed_rows": 0, "early_close_rows": 0, "parser_strategy": None},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        "service_role": "data provider only",
    }


def _clean_cell(value: Any) -> str:
    return re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()


def _int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _html_headers() -> dict[str, str]:
    return {**REQUEST_HEADERS, "Accept": "text/html,application/xhtml+xml"}
