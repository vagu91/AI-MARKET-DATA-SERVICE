from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS


class MacroMicroAaiiCrosscheckProvider:
    source = "MacroMicro AAII Cross-check"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_macromicro_aaii_crosscheck:
            return _status("disabled", "macromicro_aaii_crosscheck_disabled", started)
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout_sentiment_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(self.settings.macromicro_aaii_api_url, headers=REQUEST_HEADERS),
                    timeout=min(float(self.settings.timeout_sentiment_seconds), 8.0),
                )
                if response.status_code in {401, 403}:
                    return _status("restricted", f"MacroMicro returned HTTP {response.status_code}", started)
                response.raise_for_status()
                payload = response.json()
        except TimeoutError:
            return _status("provider_timeout", "MacroMicro AAII request timed out", started)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "MacroMicro AAII request failed", started)

        parsed = parse_macromicro_aaii(payload)
        if not parsed:
            return _status("restricted", "MacroMicro AAII anonymous payload was not parseable.", started)
        now = datetime.now(UTC)
        return {
            "status": "found",
            "provider": self.source,
            "source": "MacroMicro",
            "source_url": self.settings.macromicro_aaii_chart_url,
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(days=7)),
            "crosscheck": parsed,
            "diagnostics": {"anonymous_api_access": True, "matched": True},
            "warnings": [],
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def parse_macromicro_aaii(payload: Any) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for item in _walk_dicts(payload):
        keys = {str(key).lower(): key for key in item}
        bullish_key = _first_key(keys, ("bullish", "bull"))
        neutral_key = _first_key(keys, ("neutral",))
        bearish_key = _first_key(keys, ("bearish", "bear"))
        date_key = _first_key(keys, ("date_", "date", "time"))
        if not (bullish_key and neutral_key and bearish_key):
            continue
        bullish = _float(item.get(bullish_key))
        neutral = _float(item.get(neutral_key))
        bearish = _float(item.get(bearish_key))
        if bullish is None or neutral is None or bearish is None:
            continue
        total = bullish + neutral + bearish
        if not 98.0 <= total <= 102.0:
            continue
        candidates.append(
            {
                "survey_date": _date_text(item.get(date_key)) if date_key else None,
                "bullish_pct": bullish,
                "neutral_pct": neutral,
                "bearish_pct": bearish,
                "bull_bear_spread": round(bullish - bearish, 2),
                "sum_pct": round(total, 2),
            }
        )
    if not candidates:
        return None
    candidates.sort(key=lambda row: row.get("survey_date") or "", reverse=True)
    latest = candidates[0]
    latest["history_count"] = len(candidates)
    return latest


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _first_key(keys: dict[str, str], needles: tuple[str, ...]) -> str | None:
    for lower, original in keys.items():
        if any(needle in lower for needle in needles):
            return original
    return None


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace("%", "").replace(",", ""))
    except ValueError:
        return None


def _date_text(value: Any) -> str | None:
    return None if value in (None, "") else str(value)


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "MacroMicro AAII Cross-check",
        "source": "MacroMicro",
        "source_url": "https://en.macromicro.me/charts/20828/us-aaii-sentimentsurvey",
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(hours=6)),
        "crosscheck": None,
        "diagnostics": {"anonymous_api_access": status not in {"restricted", "provider_failed"}},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
