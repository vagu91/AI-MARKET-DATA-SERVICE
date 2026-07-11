import asyncio
from datetime import UTC, datetime
from urllib.parse import urlencode

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.nasdaq import MarketSession
from app.providers.alpha_vantage import ensure_alpha_payload_ok, parse_float, parse_int
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import REQUEST_HEADERS

MEGA_CAP_TICKERS = ["NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG", "AVGO", "TSLA", "AMD", "NFLX", "COST"]


class MegaCapSnapshotProvider(BaseProvider):
    source = "Mega-cap Snapshot"
    provider_type = ProviderType.API
    reliability = 0.78
    cache_key = "provider:mega_cap_snapshot:v3"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        errors: list[str] = []
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            stocks, chart_errors = await self._fetch_yahoo_chart(client)
            errors.extend(chart_errors)
            if stocks:
                return _snapshot_result(
                    source="Yahoo Finance Chart",
                    provider_type=ProviderType.API,
                    reliability=0.7,
                    stocks=stocks,
                    errors=errors,
                    fallback_used=False,
                )

            try:
                response = await client.get(_stooq_url(), headers=REQUEST_HEADERS)
                response.raise_for_status()
                stocks, stooq_errors = parse_stooq_quotes(response.text)
                errors.extend(stooq_errors)
                if stocks:
                    return _snapshot_result(
                        source="Stooq Quote CSV",
                        provider_type=ProviderType.CSV,
                        reliability=0.66,
                        stocks=stocks,
                        errors=errors,
                        fallback_used=True,
                    )
            except Exception as exc:
                errors.append(f"Stooq quote provider_failed: {exc or 'empty error detail'}")

            if self.settings.alpha_vantage_api_key:
                stocks, av_errors = await self._fetch_alpha_vantage_fallback(client)
                errors.extend(av_errors)
                if stocks:
                    return _snapshot_result(
                        source="Alpha Vantage GLOBAL_QUOTE",
                        provider_type=ProviderType.API,
                        reliability=0.76,
                        stocks=stocks,
                        errors=errors,
                        fallback_used=True,
                    )

            try:
                response = await client.get(
                    self.settings.yahoo_quote_url,
                    params={"symbols": ",".join(MEGA_CAP_TICKERS)},
                    headers=REQUEST_HEADERS,
                )
                response.raise_for_status()
                payload = response.json()
                stocks, yahoo_errors = parse_yahoo_quotes(payload)
                errors.extend(yahoo_errors)
                if stocks:
                    return _snapshot_result(
                        source="Yahoo Finance Quote",
                        provider_type=ProviderType.API,
                        reliability=0.72,
                        stocks=stocks,
                        errors=errors,
                        fallback_used=True,
                    )
            except Exception as exc:
                errors.append(f"Yahoo Finance quote provider_failed: {exc or 'empty error detail'}")

        return _snapshot_result(
            source=self.source,
            provider_type=ProviderType.API,
            reliability=0.0,
            stocks=[],
            errors=errors or ["No quote provider returned data"],
        )

    async def _fetch_yahoo_chart(
        self,
        client: httpx.AsyncClient,
    ) -> tuple[list[dict[str, object]], list[str]]:
        async def fetch_one(symbol: str) -> tuple[dict[str, object] | None, str | None]:
            try:
                response = await asyncio.wait_for(
                    client.get(
                        f"{self.settings.yahoo_chart_url}/{symbol}",
                        params={"range": "5d", "interval": "1d"},
                        headers=REQUEST_HEADERS,
                        timeout=min(float(self.settings.http_timeout_seconds), 4.0),
                    ),
                    timeout=min(float(self.settings.http_timeout_seconds), 4.5),
                )
                response.raise_for_status()
                stock = parse_yahoo_chart(symbol, response.json())
                if stock:
                    return stock, None
                return None, f"Yahoo Finance Chart no_data_found for {symbol}"
            except TimeoutError:
                return None, f"Yahoo Finance Chart provider_timeout for {symbol}"
            except Exception as exc:
                return None, f"Yahoo Finance Chart provider_failed for {symbol}: {exc or 'empty error detail'}"

        results = await asyncio.gather(*(fetch_one(symbol) for symbol in MEGA_CAP_TICKERS))
        stocks = [stock for stock, _ in results if stock]
        errors = [error for _, error in results if error]
        return stocks, _dedupe_errors(errors)

    async def _fetch_alpha_vantage_fallback(
        self,
        client: httpx.AsyncClient,
    ) -> tuple[list[dict[str, object]], list[str]]:
        # Free-tier Alpha Vantage allows very few daily quote calls. Use at most one
        # fallback call to avoid consuming the quota for the full watchlist.
        stocks = []
        errors = []
        symbol = MEGA_CAP_TICKERS[0]
        try:
            response = await client.get(
                self.settings.alpha_vantage_base_url,
                params={
                    "function": "GLOBAL_QUOTE",
                    "symbol": symbol,
                    "apikey": self.settings.alpha_vantage_api_key,
                },
                headers=REQUEST_HEADERS,
            )
            response.raise_for_status()
            payload = response.json()
            ensure_alpha_payload_ok(payload)
            stock = parse_alpha_vantage_global_quote(symbol, payload)
            if stock:
                stocks.append(stock)
            else:
                errors.append(f"Alpha Vantage GLOBAL_QUOTE no_data_found for {symbol}")
        except Exception as exc:
            message = str(exc) or f"Alpha Vantage GLOBAL_QUOTE provider_failed for {symbol}"
            category = "rate_limited" if _is_rate_limited(message) else "provider_failed"
            errors.append(f"Alpha Vantage GLOBAL_QUOTE {category}: {message}")
        return stocks, _dedupe_errors(errors)


def parse_alpha_vantage_global_quote(symbol: str, payload: dict) -> dict[str, object] | None:
    ensure_alpha_payload_ok(payload)
    quote = payload.get("Global Quote") or payload.get("globalQuote") or {}
    if not quote:
        return None
    price = parse_float(quote.get("05. price"))
    change = parse_float(quote.get("09. change"))
    change_pct = parse_float(quote.get("10. change percent"))
    return {
        "symbol": str(quote.get("01. symbol") or symbol).upper(),
        "name": None,
        "weight": None,
        "last_price": price,
        "change": change,
        "change_pct": change_pct,
        "volume": parse_int(quote.get("06. volume")),
        "market_session": MarketSession.UNKNOWN.value,
        "currency": "USD",
        "source": "Alpha Vantage GLOBAL_QUOTE",
        "retrieved_at": datetime.now(UTC).isoformat(),
    }


def parse_yahoo_quotes(payload: dict) -> tuple[list[dict[str, object]], list[str]]:
    retrieved_at = datetime.now(UTC)
    results = payload.get("quoteResponse", {}).get("result", [])
    stocks = []
    seen = set()
    for item in results:
        symbol = str(item.get("symbol", "")).upper()
        if not symbol:
            continue
        seen.add(symbol)
        stocks.append(
            {
                "symbol": symbol,
                "name": item.get("shortName") or item.get("longName"),
                "weight": None,
                "last_price": item.get("regularMarketPrice"),
                "change": item.get("regularMarketChange"),
                "change_pct": item.get("regularMarketChangePercent"),
                "volume": item.get("regularMarketVolume"),
                "market_session": _session(item),
                "currency": item.get("currency") or "USD",
                "source": "Yahoo Finance Quote",
                "retrieved_at": retrieved_at.isoformat(),
            }
        )
    missing = [symbol for symbol in MEGA_CAP_TICKERS if symbol not in seen]
    errors = [f"Missing quote data for: {', '.join(missing)}"] if missing else []
    return stocks, errors


def parse_yahoo_chart(symbol: str, payload: dict) -> dict[str, object] | None:
    results = payload.get("chart", {}).get("result") or []
    if not results:
        return None
    result = results[0]
    meta = result.get("meta", {})
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]
    closes = [value for value in quote.get("close", []) if value is not None]
    volumes = [value for value in quote.get("volume", []) if value is not None]
    price = parse_float(meta.get("regularMarketPrice"))
    previous_close = parse_float(meta.get("chartPreviousClose") or meta.get("previousClose"))
    if price is None and closes:
        price = parse_float(closes[-1])
    if previous_close is None and len(closes) >= 2:
        previous_close = parse_float(closes[-2])
    change = price - previous_close if price is not None and previous_close is not None else None
    change_pct = change / previous_close * 100.0 if change is not None and previous_close else None
    return {
        "symbol": symbol.upper(),
        "name": meta.get("shortName") or meta.get("longName"),
        "weight": None,
        "last_price": price,
        "change": change,
        "change_pct": change_pct,
        "volume": parse_int(meta.get("regularMarketVolume")) or (volumes[-1] if volumes else None),
        "market_session": MarketSession.UNKNOWN.value,
        "currency": meta.get("currency") or "USD",
        "source": "Yahoo Finance Chart",
        "retrieved_at": datetime.now(UTC).isoformat(),
    }


def parse_stooq_quotes(text: str) -> tuple[list[dict[str, object]], list[str]]:
    import csv
    from io import StringIO

    retrieved_at = datetime.now(UTC)
    reader = csv.DictReader(StringIO(text))
    stocks = []
    errors = []
    for row in reader:
        raw_symbol = row.get("Symbol") or ""
        symbol = raw_symbol.upper().replace(".US", "")
        if symbol not in MEGA_CAP_TICKERS:
            continue
        close = parse_float(row.get("Close") or row.get("Last"))
        open_price = parse_float(row.get("Open"))
        previous_close = parse_float(
            row.get("Previous Close")
            or row.get("PrevClose")
            or row.get("Previous")
            or row.get("Close(-1)")
        )
        baseline = previous_close if previous_close is not None else open_price
        change = close - baseline if close is not None and baseline is not None else None
        change_pct = change / baseline * 100.0 if change is not None and baseline else None
        stocks.append(
            {
                "symbol": symbol,
                "name": None,
                "weight": None,
                "last_price": close,
                "change": change,
                "change_pct": change_pct,
                "volume": parse_int(row.get("Volume")),
                "market_session": MarketSession.UNKNOWN.value,
                "currency": "USD",
                "source": "Stooq Quote CSV",
                "retrieved_at": retrieved_at.isoformat(),
            }
        )
    missing = [symbol for symbol in MEGA_CAP_TICKERS if symbol not in {item["symbol"] for item in stocks}]
    if missing:
        errors.append(f"Stooq missing quote data for: {', '.join(missing)}")
    return stocks, errors


def _snapshot_result(
    source: str,
    provider_type: ProviderType,
    reliability: float,
    stocks: list[dict[str, object]],
    errors: list[str],
    fallback_used: bool = False,
) -> ProviderResult:
    errors = _dedupe_errors([error for error in errors if error])
    missing_prices = [stock["symbol"] for stock in stocks if stock.get("last_price") is None]
    seen = {stock["symbol"] for stock in stocks}
    missing_symbols = [symbol for symbol in MEGA_CAP_TICKERS if symbol not in seen]
    missing_prices.extend(missing_symbols)
    quality_errors = errors if not stocks else [error for error in errors if "missing" in error.lower()]
    warnings = [] if not stocks else [error for error in errors if error not in quality_errors]
    return ProviderResult(
        metadata=metadata(
            source=source,
            provider_type=provider_type,
            reliability=reliability if stocks else 0.0,
            freshness=Freshness.LIVE if stocks else Freshness.UNKNOWN,
            is_fallback=fallback_used,
            errors=quality_errors,
        ),
        data={
            "stocks": stocks,
            "data_quality": {
                "tracked_count": len(MEGA_CAP_TICKERS),
                "resolved_count": len(stocks),
                "missing_prices": missing_prices,
                "fallback_used": fallback_used,
                "errors": quality_errors,
                "warnings": warnings,
                "final_data_available": bool(stocks),
                "no_data_found": not bool(stocks),
                "provider_failed": any("provider_failed" in error for error in quality_errors),
                "rate_limited": any("rate_limited" in error or "Note:" in error or "Information:" in error for error in quality_errors),
            },
        },
    )


def _stooq_url() -> str:
    symbols = ",".join(f"{symbol.lower()}.us" for symbol in MEGA_CAP_TICKERS)
    query = urlencode({"s": symbols, "f": "sd2t2ohlcv", "h": "", "e": "csv"})
    return f"https://stooq.com/q/l/?{query}"


def _dedupe_errors(errors: list[str]) -> list[str]:
    deduped = []
    for error in errors:
        if error and error not in deduped:
            deduped.append(error)
    return deduped


def _is_rate_limited(message: str) -> bool:
    lowered = message.lower()
    return "rate" in lowered or "thank you for using alpha vantage" in lowered or "25 requests" in lowered


def _session(item: dict) -> str:
    state = str(item.get("marketState") or "").upper()
    if state in {"PRE", "PREPRE"}:
        return MarketSession.PREMARKET.value
    if state in {"REGULAR", "POSTPOST"}:
        return MarketSession.REGULAR.value
    if state in {"POST", "POSTMARKET"}:
        return MarketSession.AFTER_HOURS.value
    return MarketSession.UNKNOWN.value
