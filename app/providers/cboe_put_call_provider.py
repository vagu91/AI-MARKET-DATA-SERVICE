from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS


SECTION_SCOPE = {
    "SUM OF ALL PRODUCTS": "total",
    "EQUITY OPTIONS": "equity",
    "INDEX OPTIONS": "index",
    "SPX + SPXW": "spx",
}

logger = logging.getLogger(__name__)


class CboePutCallProvider:
    source = "Cboe Daily Market Statistics"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_cboe_put_call:
            return _status("disabled", "cboe_put_call_disabled", started)
        try:
            async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
                response = await client.get(
                    self.settings.cboe_put_call_url,
                    headers={**REQUEST_HEADERS, "Accept": "text/html"},
                )
                response.raise_for_status()
        except Exception as exc:
            return _status("provider_failed", str(exc) or "cboe_put_call_failed", started)
        parsed = parse_cboe_daily_statistics_html(response.text)
        now = datetime.now(UTC)
        ratios, rejected = normalize_cboe_put_call(parsed, retrieved_at=_iso(now), valid_until=_iso(now + timedelta(hours=18)))
        if rejected:
            logger.warning("put_call_scope_rejected", extra={"source": self.source, "scope": None, "basis": None, "value": rejected, "fallback_reason": "invalid_or_zero_call_denominator"})
        return {
            "status": "found" if ratios else "not_found",
            "provider": self.source,
            "source": self.source,
            "source_url": self.settings.cboe_put_call_url,
            "source_type": "official_cboe_end_of_day_statistics",
            "is_official_source": True,
            "data_as_of": parsed.get("selectedDate"),
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(hours=18)),
            "ratios": ratios,
            "diagnostics": {
                "actual_network_calls": 1,
                "ratio_count": len(ratios),
                "rejected_ratio_count": rejected,
                "selected_date": parsed.get("selectedDate"),
            },
            "warnings": [] if ratios else ["cboe_put_call_empty_payload"],
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def parse_cboe_daily_statistics_html(text: str) -> dict[str, Any]:
    decoded_chunks: list[str] = []
    for match in re.finditer(r"self\.__next_f\.push\((\[.*?\])\)</script>", text, flags=re.S):
        try:
            chunk = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if len(chunk) > 1 and isinstance(chunk[1], str):
            decoded_chunks.append(chunk[1])
    decoded = "".join(decoded_chunks)
    marker = '"optionsData":'
    start = decoded.find(marker)
    if start < 0:
        return {}
    try:
        payload, end = json.JSONDecoder().raw_decode(decoded, start + len(marker))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    selected = re.search(r'"selectedDate":"(\d{4}-\d{2}-\d{2})"', decoded[end:end + 300])
    if selected:
        payload["selectedDate"] = selected.group(1)
    return payload


def normalize_cboe_put_call(
    payload: dict[str, Any], *, retrieved_at: str, valid_until: str
) -> tuple[list[dict[str, Any]], int]:
    ratios: list[dict[str, Any]] = []
    rejected = 0
    data_as_of = payload.get("selectedDate")
    for section, scope in SECTION_SCOPE.items():
        rows = payload.get(section) or []
        for row in rows:
            basis = "volume" if str(row.get("name") or "").upper() == "VOLUME" else "open_interest"
            if basis == "open_interest" and scope not in {"total", "spx"}:
                continue
            put_value = _nonnegative(row.get("put"))
            call_value = _nonnegative(row.get("call"))
            if put_value is None or call_value is None or call_value == 0:
                rejected += 1
                continue
            ratio = round(put_value / call_value, 6)
            ratios.append(
                {
                    "ratio_id": f"{scope}_{basis}_put_call",
                    "scope": scope,
                    "basis": basis,
                    "put_value": put_value,
                    "call_value": call_value,
                    "ratio": ratio,
                    "data_as_of": data_as_of,
                    "source": "Cboe Daily Market Statistics",
                    "source_url": "https://www.cboe.com/markets/us/options/market-statistics/daily",
                    "provider_type": "OFFICIAL_EXCHANGE_STATISTICS",
                    "retrieved_at": retrieved_at,
                    "valid_until": valid_until,
                    "freshness": "END_OF_DAY",
                    "reliability": 0.96,
                    "confidence": 0.95,
                    "is_official_source": True,
                    "cache_status": "provider",
                    "warnings": [],
                    "errors": [],
                }
            )
    return ratios, rejected


def _nonnegative(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "Cboe Daily Market Statistics",
        "source": "Cboe Daily Market Statistics",
        "source_url": "https://www.cboe.com/us/options/market_statistics/daily/",
        "is_official_source": True,
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(hours=18)),
        "ratios": [],
        "diagnostics": {"actual_network_calls": 0, "ratio_count": 0, "rejected_ratio_count": 0},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
