import csv
import json
import re
from datetime import UTC, datetime
from html.parser import HTMLParser
from io import StringIO
from typing import Any

import httpx

from app.core.cache import SQLiteCache
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.nasdaq import QQQHolding
from app.providers.alpha_vantage import ensure_alpha_payload_ok, parse_percent
from app.providers.base import BaseProvider, ProviderError, metadata
from app.providers.calendar_utils import REQUEST_HEADERS


class QQQHoldingsProvider(BaseProvider):
    source = "QQQ Holdings"
    provider_type = ProviderType.API
    reliability = 0.9
    cache_key = "provider:qqq_holdings:v2"

    def __init__(self, cache: SQLiteCache, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        errors: list[str] = []
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            if self.settings.alpha_vantage_api_key:
                try:
                    response = await client.get(
                        self.settings.alpha_vantage_base_url,
                        params={
                            "function": "ETF_PROFILE",
                            "symbol": "QQQ",
                            "apikey": self.settings.alpha_vantage_api_key,
                        },
                        headers=REQUEST_HEADERS,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    ensure_alpha_payload_ok(payload)
                    holdings, parse_errors = parse_alpha_vantage_etf_profile(payload)
                    errors.extend(parse_errors)
                    if holdings:
                        return ProviderResult(
                            metadata=metadata(
                                source="Alpha Vantage ETF_PROFILE",
                                provider_type=ProviderType.API,
                                reliability=0.9,
                                freshness=Freshness.RECENT,
                                errors=errors,
                            ),
                            data={
                                "as_of": datetime.now(UTC).date().isoformat(),
                                "holdings": [item.model_dump(mode="json") for item in holdings],
                                "data_quality": _quality(holdings, errors),
                            },
                        )
                    errors.append("Alpha Vantage ETF_PROFILE returned no holdings")
                except Exception as exc:
                    errors.append(str(exc) or "Alpha Vantage ETF_PROFILE failed")

            try:
                response = await client.get(
                    self.settings.invesco_qqq_holdings_url,
                    headers=REQUEST_HEADERS,
                    timeout=min(float(self.settings.http_timeout_seconds), 8.0),
                )
                response.raise_for_status()
                holdings, as_of, parse_errors = parse_invesco_holdings_csv(response.text)
                errors.extend(parse_errors)
                if holdings:
                    return ProviderResult(
                        metadata=metadata(
                            source="Invesco QQQ Holdings",
                            provider_type=ProviderType.CSV,
                            reliability=0.88,
                            data_as_of=_parse_as_of(as_of),
                            freshness=Freshness.RECENT,
                            errors=errors,
                        ),
                        data={
                            "as_of": as_of,
                            "holdings": [item.model_dump(mode="json") for item in holdings],
                            "data_quality": _quality(holdings, errors),
                        },
                    )
                errors.append("Invesco holdings returned no normalized holdings")
            except Exception as exc:
                errors.append(f"Invesco holdings request failed: {exc or 'empty error detail'}")

            try:
                response = await client.get(
                    self.settings.nasdaq_100_constituents_url,
                    headers=REQUEST_HEADERS,
                    timeout=min(float(self.settings.http_timeout_seconds), 8.0),
                )
                response.raise_for_status()
                holdings = parse_nasdaq_constituents(response.text)
                if not holdings:
                    raise ProviderError("Nasdaq-100 fallback returned no holdings")
                fallback_errors = errors + ["Using Nasdaq-100 constituents fallback; weights may be missing"]
                return ProviderResult(
                    metadata=metadata(
                        source="Nasdaq-100 Constituents",
                        provider_type=ProviderType.API,
                        reliability=0.72,
                        freshness=Freshness.RECENT,
                        is_fallback=True,
                        errors=fallback_errors,
                    ),
                    data={
                        "as_of": datetime.now(UTC).date().isoformat(),
                        "holdings": [item.model_dump(mode="json") for item in holdings],
                        "data_quality": _quality(holdings, fallback_errors, fallback_used=True),
                    },
                )
            except Exception as exc:
                raise ProviderError("; ".join(errors + [f"Nasdaq-100 fallback request failed: {exc or 'empty error detail'}"]))


def parse_invesco_holdings_csv(text: str) -> tuple[list[QQQHolding], str | None, list[str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    as_of = None
    for line in lines[:10]:
        lower = line.lower()
        if "as of" in lower:
            as_of = line.split(",", maxsplit=1)[-1].strip().strip('"')
            break

    header_idx = 0
    for idx, line in enumerate(lines):
        lowered = line.lower()
        if ("ticker" in lowered or "symbol" in lowered) and "weight" in lowered:
            header_idx = idx
            break

    reader = csv.DictReader(StringIO("\n".join(lines[header_idx:])))
    holdings: list[QQQHolding] = []
    errors: list[str] = []
    for row in reader:
        symbol = _first(row, "Ticker", "Holding Ticker", "Symbol", "Identifier")
        if not symbol:
            continue
        weight_raw = _first(row, "Weight", "Weight (%)", "% Weight", "Weighting")
        weight = _parse_float(weight_raw)
        if weight_raw and weight is None:
            errors.append(f"Unable to parse weight for {symbol}")
        holdings.append(
            QQQHolding(
                symbol=normalize_symbol(symbol),
                name=_first(row, "Name", "Holding Name", "Security Name", "Company"),
                weight=weight,
                sector=_first(row, "Sector", "GICS Sector"),
            )
        )
    return holdings, as_of, errors


def parse_alpha_vantage_etf_profile(payload: dict[str, Any]) -> tuple[list[QQQHolding], list[str]]:
    ensure_alpha_payload_ok(payload)
    raw_holdings = payload.get("holdings") or payload.get("Holdings") or []
    holdings: list[QQQHolding] = []
    errors: list[str] = []
    missing_sector_count = 0
    for item in raw_holdings:
        symbol = item.get("symbol") or item.get("ticker") or item.get("Symbol")
        if not symbol:
            continue
        name = (
            item.get("description")
            or item.get("name")
            or item.get("holding")
            or item.get("Description")
        )
        weight_raw = item.get("weight") or item.get("Weight") or item.get("portfolio_percentage")
        weight = parse_percent(weight_raw)
        sector = item.get("sector") or item.get("Sector")
        if sector in (None, ""):
            missing_sector_count += 1
            sector = None
        if weight_raw not in (None, "") and weight is None:
            errors.append(f"Alpha Vantage ETF_PROFILE unable to parse weight for {symbol}")
        holdings.append(
            QQQHolding(
                symbol=normalize_symbol(str(symbol)),
                name=name,
                weight=weight,
                sector=sector,
            )
        )
    if missing_sector_count:
        errors.append(
            f"Alpha Vantage ETF_PROFILE missing sector for {missing_sector_count} holdings"
        )
    return holdings, _dedupe_errors(errors)


def parse_nasdaq_constituents(text: str) -> list[QQQHolding]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = {}
    rows: list[dict[str, Any]] = []
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        rows = data.get("data", {}).get("rows") or data.get("rows") or []
    holdings = []
    for row in rows:
        symbol = row.get("symbol") or row.get("ticker")
        if not symbol:
            continue
        holdings.append(
            QQQHolding(
                symbol=normalize_symbol(str(symbol)),
                name=row.get("companyName") or row.get("name"),
                weight=None,
                sector=row.get("sector"),
            )
        )
    if holdings:
        return holdings
    return parse_constituents_html(text)


class TableTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_cell = False
        self.current_cell: list[str] = []
        self.current_row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.lower() in {"td", "th"}:
            self.in_cell = True
            self.current_cell = []

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"td", "th"} and self.in_cell:
            value = " ".join("".join(self.current_cell).split())
            self.current_row.append(value)
            self.in_cell = False
        elif lowered == "tr" and self.current_row:
            self.rows.append(self.current_row)
            self.current_row = []

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.current_cell.append(data)


def parse_constituents_html(text: str) -> list[QQQHolding]:
    parser = TableTextParser()
    parser.feed(text)
    if len(parser.rows) < 2:
        return []
    header = [cell.lower() for cell in parser.rows[0]]
    symbol_idx = _index(header, "symbol", "ticker")
    name_idx = _index(header, "company", "name")
    sector_idx = _index(header, "sector")
    if symbol_idx is None:
        return []
    holdings = []
    for row in parser.rows[1:]:
        if symbol_idx >= len(row):
            continue
        symbol = re.sub(r"[^A-Za-z.\-]", "", row[symbol_idx])
        if not symbol:
            continue
        holdings.append(
            QQQHolding(
                symbol=normalize_symbol(symbol),
                name=row[name_idx] if name_idx is not None and name_idx < len(row) else None,
                weight=None,
                sector=row[sector_idx] if sector_idx is not None and sector_idx < len(row) else None,
            )
        )
    return holdings


def _index(header: list[str], *needles: str) -> int | None:
    for idx, value in enumerate(header):
        if any(needle in value for needle in needles):
            return idx
    return None


def normalize_symbol(symbol: str) -> str:
    value = symbol.strip().upper().replace(".", "-")
    return value


def _first(row: dict[str, str], *keys: str) -> str | None:
    lowered = {key.lower().strip(): value for key, value in row.items() if key}
    for key in keys:
        value = lowered.get(key.lower())
        if value not in (None, ""):
            return value.strip()
    return None


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = value.replace("%", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_as_of(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _quality(
    holdings: list[QQQHolding],
    errors: list[str],
    fallback_used: bool = False,
) -> dict[str, object]:
    return {
        "count": len(holdings),
        "missing_weights": any(item.weight is None for item in holdings),
        "stale": False,
        "fallback_used": fallback_used,
        "errors": errors,
        "warnings": [],
        "final_data_available": bool(holdings),
        "no_data_found": not bool(holdings),
        "provider_failed": False,
        "rate_limited": False,
    }


def _dedupe_errors(errors: list[str]) -> list[str]:
    deduped = []
    for error in errors:
        if error and error not in deduped:
            deduped.append(error)
    return deduped
