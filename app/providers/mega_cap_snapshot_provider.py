import asyncio
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.nasdaq import MarketSession
from app.providers.alpha_vantage import ensure_alpha_payload_ok, parse_float, parse_int
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import REQUEST_HEADERS
from app.services.provider_capability_registry import (
    dataset_policy_by_id,
    dataset_runtime_provider_order,
)

MEGA_CAP_TICKERS = ["NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG", "AVGO", "TSLA", "AMD", "NFLX", "COST"]
MEGA_CAP_DATASET_ID = "mega_cap_quotes"
_MEGA_CAP_PROVIDER_DISPATCH_IDS = frozenset(
    {
        "YAHOO_FINANCE_CHART",
        "STOOQ",
        "ALPHA_VANTAGE",
        "YAHOO_FINANCE_QUOTE",
    }
)


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
        provider_accounting: list[dict[str, object]] = []
        declared_order: tuple[str, ...] = ()
        try:
            declared_order = _declared_mega_cap_provider_order()
            provider_order = _mega_cap_provider_order()
        except (KeyError, RuntimeError) as exc:
            reason = (
                "mega_cap_runtime_provider_policy_invalid:"
                f"{exc}"
            )
            return _snapshot_result(
                source=self.source,
                provider_type=ProviderType.API,
                reliability=0.0,
                stocks=[],
                errors=[reason],
                provider_accounting=[
                    _provider_observation(
                        provider,
                        calls=0,
                        status="NOT_CALLED",
                        reason_code=(
                            "RUNTIME_ADAPTER_MAPPING_UNAVAILABLE"
                        ),
                    )
                    for provider in declared_order
                ],
            )
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            for position, provider_id in enumerate(provider_order):
                fallback_used = position > 0
                if provider_id == "YAHOO_FINANCE_CHART":
                    stocks, chart_errors = (
                        await self._fetch_yahoo_chart(client)
                    )
                    stocks, observation_errors = (
                        _current_provider_stocks(stocks)
                    )
                    chart_errors.extend(observation_errors)
                    errors.extend(chart_errors)
                    provider_accounting.append(
                        _provider_observation(
                            provider_id,
                            calls=len(MEGA_CAP_TICKERS),
                            status=(
                                "SUCCESS"
                                if stocks and not chart_errors
                                else "PARTIAL"
                                if stocks
                                else "FAILED"
                            ),
                        )
                    )
                    if stocks:
                        return _snapshot_result(
                            source="Yahoo Finance Chart",
                            provider_type=ProviderType.API,
                            reliability=0.7,
                            stocks=stocks,
                            errors=errors,
                            fallback_used=fallback_used,
                            provider_accounting=(
                                _complete_provider_accounting(
                                    provider_accounting,
                                    provider_order=provider_order,
                                    alpha_configured=bool(
                                        self.settings.alpha_vantage_api_key
                                    ),
                                )
                            ),
                        )
                    continue

                if provider_id == "STOOQ":
                    try:
                        response = await client.get(
                            _stooq_url(),
                            headers=REQUEST_HEADERS,
                        )
                        response.raise_for_status()
                        stocks, stooq_errors = parse_stooq_quotes(
                            response.text
                        )
                        stocks, observation_errors = (
                            _current_provider_stocks(stocks)
                        )
                        stooq_errors.extend(observation_errors)
                        errors.extend(stooq_errors)
                        provider_accounting.append(
                            _provider_observation(
                                provider_id,
                                calls=1,
                                status=(
                                    "SUCCESS"
                                    if stocks and not stooq_errors
                                    else "PARTIAL"
                                    if stocks
                                    else "NO_DATA"
                                ),
                            )
                        )
                        if stocks:
                            return _snapshot_result(
                                source="Stooq Quote CSV",
                                provider_type=ProviderType.CSV,
                                reliability=0.66,
                                stocks=stocks,
                                errors=errors,
                                fallback_used=fallback_used,
                                provider_accounting=(
                                    _complete_provider_accounting(
                                        provider_accounting,
                                        provider_order=provider_order,
                                        alpha_configured=bool(
                                            self.settings.alpha_vantage_api_key
                                        ),
                                    )
                                ),
                            )
                    except Exception as exc:
                        errors.append(
                            "Stooq quote provider_failed: "
                            f"{exc or 'empty error detail'}"
                        )
                        provider_accounting.append(
                            _provider_observation(
                                provider_id,
                                calls=1,
                                status="FAILED",
                                reason_code=(
                                    str(exc) or type(exc).__name__
                                ),
                            )
                        )
                    continue

                if provider_id == "ALPHA_VANTAGE":
                    if not self.settings.alpha_vantage_api_key:
                        provider_accounting.append(
                            _provider_observation(
                                provider_id,
                                calls=0,
                                status="NOT_CALLED",
                                reason_code=(
                                    "ALPHA_VANTAGE_NOT_CONFIGURED"
                                ),
                            )
                        )
                        continue
                    stocks, av_errors = (
                        await self._fetch_alpha_vantage_fallback(
                            client
                        )
                    )
                    stocks, observation_errors = (
                        _current_provider_stocks(stocks)
                    )
                    av_errors.extend(observation_errors)
                    errors.extend(av_errors)
                    provider_accounting.append(
                        _provider_observation(
                            provider_id,
                            calls=1,
                            status=(
                                "SUCCESS"
                                if stocks
                                else "FAILED"
                                if av_errors
                                else "NO_DATA"
                            ),
                            reason_code=(
                                "; ".join(av_errors) or None
                            ),
                        )
                    )
                    if stocks:
                        return _snapshot_result(
                            source="Alpha Vantage GLOBAL_QUOTE",
                            provider_type=ProviderType.API,
                            reliability=0.76,
                            stocks=stocks,
                            errors=errors,
                            fallback_used=fallback_used,
                            provider_accounting=(
                                _complete_provider_accounting(
                                    provider_accounting,
                                    provider_order=provider_order,
                                    alpha_configured=True,
                                )
                            ),
                        )
                    continue

                if provider_id == "YAHOO_FINANCE_QUOTE":
                    try:
                        response = await client.get(
                            self.settings.yahoo_quote_url,
                            params={
                                "symbols": ",".join(
                                    MEGA_CAP_TICKERS
                                )
                            },
                            headers=REQUEST_HEADERS,
                        )
                        response.raise_for_status()
                        payload = response.json()
                        stocks, yahoo_errors = parse_yahoo_quotes(
                            payload
                        )
                        stocks, observation_errors = (
                            _current_provider_stocks(stocks)
                        )
                        yahoo_errors.extend(observation_errors)
                        errors.extend(yahoo_errors)
                        provider_accounting.append(
                            _provider_observation(
                                provider_id,
                                calls=1,
                                status=(
                                    "SUCCESS"
                                    if stocks and not yahoo_errors
                                    else "PARTIAL"
                                    if stocks
                                    else "NO_DATA"
                                ),
                            )
                        )
                        if stocks:
                            return _snapshot_result(
                                source="Yahoo Finance Quote",
                                provider_type=ProviderType.API,
                                reliability=0.72,
                                stocks=stocks,
                                errors=errors,
                                fallback_used=fallback_used,
                                provider_accounting=(
                                    _complete_provider_accounting(
                                        provider_accounting,
                                        provider_order=provider_order,
                                        alpha_configured=bool(
                                            self.settings.alpha_vantage_api_key
                                        ),
                                    )
                                ),
                            )
                    except Exception as exc:
                        errors.append(
                            "Yahoo Finance quote provider_failed: "
                            f"{exc or 'empty error detail'}"
                        )
                        provider_accounting.append(
                            _provider_observation(
                                provider_id,
                                calls=1,
                                status="FAILED",
                                reason_code=(
                                    str(exc) or type(exc).__name__
                                ),
                            )
                        )
                    continue

                reason = (
                    "MEGA_CAP_RUNTIME_ADAPTER_UNMAPPED:"
                    f"{provider_id}"
                )
                errors.append(reason)
                provider_accounting.append(
                    _provider_observation(
                        provider_id,
                        calls=0,
                        status="NOT_CALLED",
                        reason_code=(
                            "RUNTIME_ADAPTER_MAPPING_UNAVAILABLE"
                        ),
                    )
                )
                break

        return _snapshot_result(
            source=self.source,
            provider_type=ProviderType.API,
            reliability=0.0,
            stocks=[],
            errors=errors or ["No quote provider returned data"],
            provider_accounting=_complete_provider_accounting(
                provider_accounting,
                provider_order=provider_order,
                alpha_configured=bool(
                    self.settings.alpha_vantage_api_key
                ),
            ),
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
    data_as_of = _provider_observation_iso(
        quote.get("07. latest trading day")
    )
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
        "data_as_of": data_as_of,
        "observation_time_source": (
            "07. latest trading day" if data_as_of else None
        ),
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
        data_as_of = _provider_observation_iso(
            item.get("regularMarketTime")
        )
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
                "data_as_of": data_as_of,
                "observation_time_source": (
                    "regularMarketTime" if data_as_of else None
                ),
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
    raw_closes = list(quote.get("close", []) or [])
    close_indices = [
        index
        for index, value in enumerate(raw_closes)
        if value is not None
    ]
    closes = [raw_closes[index] for index in close_indices]
    volumes = [value for value in quote.get("volume", []) if value is not None]
    price = parse_float(meta.get("regularMarketPrice"))
    price_from_meta = price is not None
    previous_close = parse_float(meta.get("chartPreviousClose") or meta.get("previousClose"))
    if price is None and closes:
        price = parse_float(closes[-1])
    if previous_close is None and len(closes) >= 2:
        previous_close = parse_float(closes[-2])
    change = price - previous_close if price is not None and previous_close is not None else None
    change_pct = change / previous_close * 100.0 if change is not None and previous_close else None
    chart_timestamps = list(result.get("timestamp") or [])
    provider_time = meta.get("regularMarketTime")
    observation_source = "regularMarketTime"
    if (
        not price_from_meta
        and provider_time is None
        and close_indices
        and close_indices[-1] < len(chart_timestamps)
        and chart_timestamps[close_indices[-1]] is not None
    ):
        provider_time = chart_timestamps[close_indices[-1]]
        observation_source = "chart.timestamp[price_index]"
    data_as_of = _provider_observation_iso(provider_time)
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
        "data_as_of": data_as_of,
        "observation_time_source": (
            observation_source if data_as_of else None
        ),
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
        date_text = str(row.get("Date") or "").strip()
        time_text = str(row.get("Time") or "").strip()
        data_as_of = _provider_observation_iso(
            f"{date_text}T{time_text}"
            if date_text and time_text
            else date_text
        )
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
                "data_as_of": data_as_of,
                "observation_time_source": (
                    "Date+Time" if data_as_of else None
                ),
                "retrieved_at": retrieved_at.isoformat(),
            }
        )
    missing = [symbol for symbol in MEGA_CAP_TICKERS if symbol not in {item["symbol"] for item in stocks}]
    if missing:
        errors.append(f"Stooq missing quote data for: {', '.join(missing)}")
    return stocks, errors


def _provider_observation_iso(value: object) -> str | None:
    """Normalize a provider-issued observation time without using retrieval time."""

    if value in (None, "") or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), UTC).isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(float(text), UTC).isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _current_provider_stocks(
    stocks: list[dict[str, object]],
    *,
    observed_at: datetime | None = None,
) -> tuple[list[dict[str, object]], list[str]]:
    decision_at = (observed_at or datetime.now(UTC)).astimezone(UTC)
    max_age = timedelta(
        seconds=dataset_policy_by_id(
            MEGA_CAP_DATASET_ID
        ).sla_seconds
    )
    accepted: list[dict[str, object]] = []
    errors: list[str] = []
    for stock in stocks:
        symbol = str(stock.get("symbol") or "UNKNOWN")
        observation = _provider_observation_datetime(
            stock.get("data_as_of")
        )
        if observation is None:
            errors.append(
                f"{symbol} provider observation time missing"
            )
            continue
        if (
            observation > decision_at + timedelta(minutes=5)
            or decision_at >= observation + max_age
        ):
            errors.append(
                f"{symbol} provider observation outside dataset SLA"
            )
            continue
        accepted.append(stock)
    return accepted, errors


def _declared_mega_cap_provider_order() -> tuple[str, ...]:
    policy = dataset_policy_by_id(MEGA_CAP_DATASET_ID)
    return (
        policy.primary_provider,
        *policy.fallback_providers,
    )


def _mega_cap_provider_order() -> tuple[str, ...]:
    return dataset_runtime_provider_order(
        MEGA_CAP_DATASET_ID,
        _MEGA_CAP_PROVIDER_DISPATCH_IDS,
    )


def _provider_observation(
    provider: str,
    *,
    calls: int,
    status: str,
    reason_code: str | None = None,
) -> dict[str, object]:
    return {
        "provider": provider,
        "called": calls > 0,
        "calls": calls,
        "status": status,
        "reason_code": reason_code,
    }


def _complete_provider_accounting(
    observations: list[dict[str, object]],
    *,
    provider_order: tuple[str, ...] | None = None,
    alpha_configured: bool,
) -> list[dict[str, object]]:
    by_provider = {
        str(item.get("provider")): dict(item)
        for item in observations
    }
    prior_succeeded = False
    output: list[dict[str, object]] = []
    for provider in provider_order or _mega_cap_provider_order():
        observed = by_provider.get(provider)
        if observed is not None:
            output.append(observed)
            prior_succeeded = prior_succeeded or str(
                observed.get("status") or ""
            ).upper() in {"SUCCESS", "PARTIAL"}
            continue
        reason = (
            "PRIOR_PROVIDER_SUCCEEDED"
            if prior_succeeded
            else "ALPHA_VANTAGE_NOT_CONFIGURED"
            if provider == "ALPHA_VANTAGE"
            and not alpha_configured
            else "PROVIDER_EXECUTION_EVIDENCE_MISSING"
        )
        output.append(
            _provider_observation(
                provider,
                calls=0,
                status="NOT_CALLED",
                reason_code=reason,
            )
        )
    return output


def _snapshot_result(
    source: str,
    provider_type: ProviderType,
    reliability: float,
    stocks: list[dict[str, object]],
    errors: list[str],
    fallback_used: bool = False,
    provider_accounting: list[dict[str, object]] | None = None,
) -> ProviderResult:
    errors = _dedupe_errors([error for error in errors if error])
    data_as_of = _snapshot_data_as_of(stocks)
    now = datetime.now(UTC)
    max_age_seconds = dataset_policy_by_id(
        MEGA_CAP_DATASET_ID
    ).sla_seconds
    snapshot_freshness = (
        Freshness.RECENT
        if data_as_of is not None
        and data_as_of <= now
        and now - data_as_of
        <= timedelta(seconds=max_age_seconds)
        else Freshness.STALE
        if data_as_of is not None and data_as_of <= now
        else Freshness.UNKNOWN
    )
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
            data_as_of=data_as_of,
            freshness=(
                snapshot_freshness
                if stocks
                else Freshness.UNKNOWN
            ),
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
                "provider_accounting": list(
                    provider_accounting or []
                ),
                "final_data_available": bool(stocks),
                "no_data_found": not bool(stocks),
                "provider_failed": any("provider_failed" in error for error in quality_errors),
                "reason_code": (
                    "RUNTIME_PROVIDER_POLICY_INVALID"
                    if any(
                        "runtime_provider_policy_invalid" in error
                        for error in quality_errors
                    )
                    else None
                ),
                "rate_limited": any("rate_limited" in error or "Note:" in error or "Information:" in error for error in quality_errors),
            },
        },
    )


def _snapshot_data_as_of(
    stocks: list[dict[str, object]],
) -> datetime | None:
    observations = [
        _provider_observation_datetime(stock.get("data_as_of"))
        for stock in stocks
    ]
    if not stocks or any(value is None for value in observations):
        return None
    return min(
        value for value in observations if value is not None
    )


def _provider_observation_datetime(value: object) -> datetime | None:
    normalized = _provider_observation_iso(value)
    if normalized is None:
        return None
    return datetime.fromisoformat(normalized)


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
