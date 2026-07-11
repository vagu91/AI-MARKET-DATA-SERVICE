from __future__ import annotations

import asyncio
import html
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from app.core.config import Settings
from app.core.text_normalization import normalize_text
from app.providers.calendar_utils import REQUEST_HEADERS


NY_TZ = ZoneInfo("America/New_York")


class InvestingFedRateMonitorProvider:
    source = "Investing.com Fed Rate Monitor"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_investing_fed_rate_monitor:
            return _status("disabled", "investing_fed_rate_monitor_disabled", started, self.settings.investing_fed_rate_monitor_url)
        try:
            async with httpx.AsyncClient(timeout=self.settings.investing_fed_rate_monitor_timeout_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(self.settings.investing_fed_rate_monitor_url, headers=_html_headers()),
                    timeout=min(float(self.settings.investing_fed_rate_monitor_timeout_seconds), 15.0),
                )
                response.raise_for_status()
                text = response.text
        except TimeoutError:
            return _status("provider_timeout", "investing_fed_rate_monitor_timeout", started, self.settings.investing_fed_rate_monitor_url)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "investing_fed_rate_monitor_failed", started, self.settings.investing_fed_rate_monitor_url)

        parsed = parse_investing_fed_rate_monitor_html(text, max_meetings=self.settings.investing_fed_rate_monitor_max_meetings)
        now = datetime.now(UTC)
        warnings = []
        if not parsed["meetings"]:
            warnings.append("investing_fed_rate_monitor_no_meetings")
        return {
            "status": "found" if parsed["meetings"] else "not_found",
            "provider": self.source,
            "source": "Investing Fed Rate Monitor",
            "source_url": self.settings.investing_fed_rate_monitor_url,
            "source_type": "secondary_market_implied_probabilities",
            "dataset_type": "market_implied_target_rate_distribution",
            "official_fed_data": False,
            "official_fed_source": False,
            "official_cme_data": False,
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(minutes=self.settings.investing_fed_rate_monitor_ttl_minutes)),
            "meetings": parsed["meetings"],
            "current_meeting": parsed["meetings"][0] if parsed["meetings"] else None,
            "history_endpoint": {
                "status": "not_integrated",
                "reason": "history chart endpoint remains excluded until payload contract is validated",
            },
            "history_endpoint_status": "not_integrated",
            "diagnostics": {
                "cards_seen": parsed["cards_seen"],
                "meetings_parsed": len(parsed["meetings"]),
                "rejected_cards": parsed["rejected_cards"],
                "probability_sum_outliers": parsed["probability_sum_outliers"],
                "null_previous_values": parsed["null_previous_values"],
                "history_endpoint_integrated": False,
            },
            "warnings": warnings,
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
            "service_role": "data provider only",
        }


def parse_investing_fed_rate_monitor_html(text: str, *, max_meetings: int = 8) -> dict[str, Any]:
    blocks = re.findall(r'<div class="cardWrapper">(.+?)(?=<div class="cardWrapper">|\Z)', text, flags=re.S)
    meetings: list[dict[str, Any]] = []
    rejected = 0
    outliers = 0
    null_previous_values = 0
    for block in blocks:
        meeting = _parse_card(block)
        if not meeting:
            rejected += 1
            continue
        null_previous_values += sum(
            1
            for item in meeting["target_rate_probabilities"]
            if item.get("previous_day_probability_pct") is None or item.get("previous_week_probability_pct") is None
        )
        total = meeting["probability_sum_pct"]
        if total is not None and not 99.0 <= total <= 101.0:
            outliers += 1
            meeting["diagnostics"]["probability_sum_outlier"] = True
        meetings.append(meeting)
        if len(meetings) >= max_meetings:
            break
    return {
        "cards_seen": len(blocks),
        "meetings": meetings,
        "rejected_cards": rejected,
        "probability_sum_outliers": outliers,
        "null_previous_values": null_previous_values,
    }


def _parse_card(block: str) -> dict[str, Any] | None:
    date_text = _first(r'<div class="fedRateDate"[^>]*>\s*(.*?)\s*</div>', block)
    if not date_text:
        return None
    meeting_date = parse_meeting_date(date_text)
    meeting_time = _first(r"<span>\s*Meeting Time:\s*</span>\s*<i>(.*?)</i>", block)
    future_price = _float(_first(r"<span>\s*Future Price:\s*</span>\s*<i>(.*?)</i>", block))
    updated_at = _clean(_first(r'<div class="fedUpdate">\s*Updated:\s*(.*?)\s*</div>', block))
    rows = re.findall(r"<tr>\s*<td[^>]*>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*</tr>", block, flags=re.S)
    probabilities: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    for rate_cell, current, previous_day, previous_week in rows:
        event_id = _first(r'eventId="([^"]+)"', rate_cell)
        if event_id:
            event_ids.add(event_id)
        target = _clean(re.sub(r"<.*?>", " ", rate_cell))
        if not target:
            continue
        probabilities.append(
            {
                "target_rate": target.replace("%", ""),
                "current_probability_pct": _pct(current),
                "previous_day_probability_pct": _pct(previous_day),
                "previous_week_probability_pct": _pct(previous_week),
                "event_id": event_id,
                "calc_key": _first(r'calcKey="([^"]+)"', rate_cell),
            }
        )
    if not meeting_date or not probabilities:
        return None
    meeting_time_text = _clean(meeting_time)
    updated_at_text = updated_at
    total = round(sum(float(item["current_probability_pct"] or 0) for item in probabilities), 4)
    return {
        "meeting_date": meeting_date,
        "meeting_at": parse_local_datetime(meeting_time_text, fallback_date=meeting_date),
        "meeting_time_local_text": meeting_time_text,
        "meeting_time_local": meeting_time_text,
        "timezone": "America/New_York",
        "future_price": future_price,
        "updated_at": parse_local_datetime(updated_at_text),
        "updated_at_text": updated_at_text,
        "event_ids": sorted(event_ids),
        "event_id": sorted(event_ids)[0] if event_ids else None,
        "target_rate_probabilities": probabilities,
        "probability_sum_pct": total,
        "max_probability": max(probabilities, key=lambda item: float(item["current_probability_pct"] or 0)),
        "probabilities_normalized": False,
        "source": "Investing.com Fed Rate Monitor",
        "source_url": "https://www.investing.com/central-banks/fed-rate-monitor",
        "source_type": "secondary_market_implied_probabilities",
        "diagnostics": {"probability_sum_outlier": False},
    }


def parse_meeting_date(value: str) -> str | None:
    text = _clean(value)
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_local_datetime(value: str | None, *, fallback_date: str | None = None) -> str | None:
    text = _clean(value)
    if not text:
        if fallback_date:
            return datetime.fromisoformat(f"{fallback_date}T14:00:00").replace(tzinfo=NY_TZ).isoformat()
        return None
    normalized = re.sub(r"\b(ET|EST|EDT)\b", "", text, flags=re.I).strip()
    formats = ("%b %d, %Y %I:%M%p", "%B %d, %Y %I:%M%p", "%b %d, %Y %I:%M %p", "%B %d, %Y %I:%M %p")
    for fmt in formats:
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=NY_TZ).isoformat()
        except ValueError:
            continue
    if fallback_date:
        time_match = re.search(r"(\d{1,2}):(\d{2})\s*([AP]M)", normalized, flags=re.I)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            if time_match.group(3).upper() == "PM" and hour != 12:
                hour += 12
            if time_match.group(3).upper() == "AM" and hour == 12:
                hour = 0
            try:
                return datetime.fromisoformat(f"{fallback_date}T{hour:02d}:{minute:02d}:00").replace(tzinfo=NY_TZ).isoformat()
            except ValueError:
                return None
        return datetime.fromisoformat(f"{fallback_date}T14:00:00").replace(tzinfo=NY_TZ).isoformat()
    return None


def _status(status: str, reason: str, started: datetime, source_url: str) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "Investing.com Fed Rate Monitor",
        "source": "Investing Fed Rate Monitor",
        "source_url": source_url,
        "source_type": "secondary_market_implied_probabilities",
        "dataset_type": "market_implied_target_rate_distribution",
        "official_fed_data": False,
        "official_fed_source": False,
        "official_cme_data": False,
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(minutes=30)),
        "meetings": [],
        "current_meeting": None,
        "history_endpoint": {"status": "not_integrated"},
        "history_endpoint_status": "not_integrated",
        "diagnostics": {"cards_seen": 0, "meetings_parsed": 0, "rejected_cards": 0},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        "service_role": "data provider only",
    }


def _first(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text, flags=re.S | re.I)
    return match.group(1) if match else None


def _pct(value: Any) -> float | None:
    text = _clean(value).replace("%", "")
    if text in {"", "-", "--", "—", "N/A"}:
        return None
    return _float(text)


def _float(value: Any) -> float | None:
    text = _clean(value).replace(",", "")
    if text in {"", "-", "--", "—", "N/A"}:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return normalize_text(re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip())


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _html_headers() -> dict[str, str]:
    return {**REQUEST_HEADERS, "Accept": "text/html,application/xhtml+xml", "Referer": "https://www.investing.com/"}
