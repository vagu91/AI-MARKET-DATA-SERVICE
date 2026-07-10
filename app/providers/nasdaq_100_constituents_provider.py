from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS
from app.providers.mega_cap_snapshot_provider import MEGA_CAP_TICKERS
from app.services.economic_value_parser import parse_economic_value


class Nasdaq100ConstituentsProvider:
    source = "Nasdaq-100 Constituents"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_nasdaq_100:
            return _status("disabled", "nasdaq_100_disabled", started)
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout_nasdaq_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(self.settings.nasdaq_100_constituents_url, headers=_json_headers()),
                    timeout=min(float(self.settings.timeout_nasdaq_seconds), 20.0),
                )
                response.raise_for_status()
                payload = response.json()
        except TimeoutError:
            return _status("provider_timeout", "Nasdaq-100 request timed out", started)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "Nasdaq-100 request failed", started)

        data = payload.get("data") or {}
        table = data.get("data") if isinstance(data.get("data"), dict) else data
        rows = table.get("rows") or data.get("rows") or []
        total_records = _int(table.get("totalrecords") or data.get("totalrecords") or data.get("totalRecords"))
        constituents = [_normalize(row) for row in rows if isinstance(row, dict)]
        symbols = [item["symbol"] for item in constituents if item.get("symbol")]
        duplicate_symbols = sorted({symbol for symbol in symbols if symbols.count(symbol) > 1})
        unique_symbols = sorted(set(symbols))
        sector_missing = sum(1 for item in constituents if not item.get("sector"))
        price_outliers = [item["symbol"] for item in constituents if item.get("last_sale_price") is not None and item["last_sale_price"] <= 0]
        market_cap_outliers = [item["symbol"] for item in constituents if item.get("market_cap") is not None and item["market_cap"] <= 0]
        overlap_mega_cap = sorted(set(unique_symbols) & set(MEGA_CAP_TICKERS))
        anomalies: list[str] = []
        if total_records is not None and abs(total_records - len(constituents)) > 3:
            anomalies.append("total_records_row_count_mismatch")
        if not 95 <= len(unique_symbols) <= 110:
            anomalies.append("unexpected_symbol_count")
        if duplicate_symbols:
            anomalies.append("duplicate_symbols")
        if price_outliers or market_cap_outliers:
            anomalies.append("numeric_outliers")
        status = "valid" if not anomalies else ("anomalous" if constituents else "rejected_snapshot")
        now = datetime.now(UTC)
        return {
            "status": status,
            "provider": self.source,
            "source": "Nasdaq",
            "source_url": self.settings.nasdaq_100_constituents_url,
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(hours=12)),
            "constituents": constituents,
            "diagnostics": {
                "totalrecords": total_records,
                "row_count": len(constituents),
                "total_symbols": len(unique_symbols),
                "overlap_mega_cap": len(overlap_mega_cap),
                "overlap_mega_cap_symbols": overlap_mega_cap,
                "duplicate_symbols": duplicate_symbols,
                "sector_missing_count": sector_missing,
                "market_cap_outliers": market_cap_outliers,
                "price_outliers": price_outliers,
                "source_anomalies": anomalies,
                "unexpected_additions": [],
                "unexpected_removals": [],
                "not_promoted_over_qqq_holdings": True,
            },
            "warnings": anomalies,
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    market_cap = parse_economic_value(row.get("marketCap"), default_unit="USD")
    price = parse_economic_value(row.get("lastSalePrice"), default_unit="USD")
    net_change = parse_economic_value(row.get("netChange"))
    pct_change = parse_economic_value(row.get("percentageChange"), default_unit="percent")
    return {
        "symbol": str(row.get("symbol") or "").upper(),
        "company_name": row.get("companyName") or row.get("company"),
        "market_cap": market_cap["value"] if market_cap["parse_status"] == "parsed" else None,
        "last_sale_price": price["value"] if price["parse_status"] == "parsed" else None,
        "net_change": net_change["value"] if net_change["parse_status"] == "parsed" else None,
        "percentage_change": pct_change["value"] if pct_change["parse_status"] == "parsed" else None,
        "delta_indicator": row.get("deltaIndicator"),
        "sector": row.get("sector") or None,
    }


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "Nasdaq-100 Constituents",
        "source": "Nasdaq",
        "source_url": "https://api.nasdaq.com/api/quote/list-type/nasdaq100",
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(hours=1)),
        "constituents": [],
        "diagnostics": {"row_count": 0, "source_anomalies": []},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _int(value: Any) -> int | None:
    try:
        return int(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_headers() -> dict[str, str]:
    return {**REQUEST_HEADERS, "User-Agent": "Mozilla/5.0", "Accept": "application/json", "Origin": "https://www.nasdaq.com"}
