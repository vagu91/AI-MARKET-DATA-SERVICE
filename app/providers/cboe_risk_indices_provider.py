from __future__ import annotations

import asyncio
import csv
import io
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS


class CboeRiskIndicesProvider:
    source = "CBOE Delayed Quotes"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_cboe_risk_indices:
            return _status("disabled", "cboe_risk_indices_disabled", started)
        results: dict[str, Any] = {}
        histories: dict[str, list[dict[str, Any]]] = {}
        errors: list[str] = []
        actual_network_calls = 0
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            for key, url in {"vvix": self.settings.cboe_vvix_url, "skew": self.settings.cboe_skew_url}.items():
                try:
                    actual_network_calls += 1
                    response = await asyncio.wait_for(
                        client.get(url, headers={**REQUEST_HEADERS, "Accept": "application/json"}),
                        timeout=min(float(self.settings.http_timeout_seconds), 10.0),
                    )
                    response.raise_for_status()
                    results[key] = _normalize(key, response.json(), url)
                except TimeoutError:
                    errors.append(f"{key}_timeout")
                except Exception as exc:
                    errors.append(f"{key}_failed:{exc or type(exc).__name__}")
            for key, url in {
                "vvix": self.settings.cboe_vvix_history_url,
                "skew": self.settings.cboe_skew_history_url,
                "vix": self.settings.cboe_vix_history_url,
            }.items():
                try:
                    actual_network_calls += 1
                    response = await asyncio.wait_for(
                        client.get(url, headers={**REQUEST_HEADERS, "Accept": "text/csv"}),
                        timeout=min(float(self.settings.http_timeout_seconds), 10.0),
                    )
                    response.raise_for_status()
                    histories[key] = parse_index_history_csv(response.text, key=key, limit=260)
                except TimeoutError:
                    errors.append(f"{key}_history_timeout")
                except Exception as exc:
                    errors.append(f"{key}_history_failed:{exc or type(exc).__name__}")
        return {
            "status": "found" if results else "provider_failed",
            "provider": self.source,
            "source": self.source,
            "source_url": "https://cdn.cboe.com/api/global/delayed_quotes/",
            "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "valid_until": (datetime.now(UTC) + timedelta(minutes=15)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "indices": results,
            "history": histories,
            "diagnostics": {
                "found": list(results),
                "missing": [key for key in ("vvix", "skew") if key not in results],
                "history_depth": {key: len(value) for key, value in histories.items()},
                "actual_network_calls": actual_network_calls,
            },
            "warnings": [error for error in errors if results],
            "errors": [] if results else errors,
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def _normalize(key: str, payload: dict[str, Any], url: str) -> dict[str, Any]:
    data = payload.get("data") or {}
    timestamp = _timestamp(payload.get("timestamp"))
    stale = bool(timestamp and datetime.now(UTC) - timestamp > timedelta(hours=2))
    unreliable_ohl = key == "skew" and any(float(data.get(field) or 0) == 0.0 for field in ("open", "high", "low"))
    return {
        "canonical_series_id": key.upper(),
        "provider_symbol": data.get("symbol") or payload.get("symbol"),
        "security_type": data.get("security_type"),
        "current_price": _float(data.get("current_price")),
        "open": None if unreliable_ohl else _float(data.get("open")),
        "high": None if unreliable_ohl else _float(data.get("high")),
        "low": None if unreliable_ohl else _float(data.get("low")),
        "close": _float(data.get("close")),
        "previous_close": _float(data.get("prev_day_close")),
        "change": _float(data.get("price_change")),
        "percentage_change": _float(data.get("price_change_percent")),
        "last_trade_time": data.get("last_trade_time"),
        "provider_timestamp": payload.get("timestamp"),
        "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "CBOE",
        "source_url": url,
        "delayed": True,
        "stale": stale,
        "reliability": 0.86 if not stale else 0.55,
        "is_official_source": True,
        "warnings": ["skew_open_high_low_zero_not_reliable"] if unreliable_ohl else [],
    }


def parse_index_history_csv(text: str, *, key: str, limit: int = 260) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    value_key = key.upper()
    for row in csv.DictReader(io.StringIO(text.lstrip("\ufeff"))):
        raw_date = str(row.get("DATE") or "").strip()
        raw_value = row.get(value_key)
        if key == "vix":
            raw_value = row.get("CLOSE")
        value = _float(raw_value)
        if not raw_date or value is None or value <= 0:
            continue
        try:
            data_as_of = datetime.strptime(raw_date, "%m/%d/%Y").date().isoformat()
        except ValueError:
            continue
        rows.append({"data_as_of": data_as_of, "value": value})
    rows.sort(key=lambda item: item["data_as_of"])
    return rows[-max(int(limit), 1):]


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    return {
        "status": status,
        "provider": "CBOE Delayed Quotes",
        "source": "CBOE",
        "source_url": "https://cdn.cboe.com/api/global/delayed_quotes/",
        "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (datetime.now(UTC) + timedelta(minutes=15)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "indices": {},
        "history": {},
        "diagnostics": {"found": [], "missing": ["vvix", "skew"]},
        "warnings": [reason],
        "errors": [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _float(value: Any) -> float | None:
    if value in (None, "", "--"):
        return None
    try:
        parsed = float(str(value).replace(",", ""))
        return parsed if math.isfinite(parsed) else None
    except ValueError:
        return None


def _timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace(" ", "T")).replace(tzinfo=UTC)
    except ValueError:
        return None
