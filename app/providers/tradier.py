from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any, Iterable
from urllib.parse import urlparse

import httpx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, ProviderDisabled, ProviderError, metadata
from app.providers.deterministic import (
    AsyncSlidingWindowRateLimiter,
    DeterministicHttpClient,
    DeterministicProviderError,
    as_list,
    safe_payload_hash,
)
from app.providers.parametric_cache import CacheResolution, ParametricProviderCache


TRADIER_ALLOWED_HOSTS = {"api.tradier.com", "sandbox.tradier.com"}
TRADIER_ALLOWED_PATHS = {
    "/v1/markets/quotes",
    "/v1/markets/options/expirations",
    "/v1/markets/options/chains",
}


class TradierProvider(BaseProvider):
    """Strictly read-only Tradier market-data adapter.

    There is deliberately no account, order, preview, positions, balances or
    streaming method on this class.
    """

    source = "TRADIER"
    provider_type = ProviderType.API
    reliability = 0.93
    cache_key = "provider:tradier:market_data"

    def __init__(
        self,
        cache: ProviderCacheProtocol,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock=lambda: datetime.now(UTC),
        sleeper=None,
    ) -> None:
        super().__init__(cache)
        self.settings = settings
        self.clock = clock
        self.environment, self.base_url, self.token = _configuration(settings)
        import asyncio

        actual_sleeper = sleeper or asyncio.sleep
        self.parametric_cache = ParametricProviderCache(cache, clock=clock)
        self.rate_limiter = AsyncSlidingWindowRateLimiter(
            settings.tradier_rate_limit_per_minute,
            clock=clock,
            sleeper=actual_sleeper,
        )
        self.last_telemetry: dict[str, Any] = {}
        self._uncached_telemetry: dict[str, dict[str, Any]] = {}
        self.http = DeterministicHttpClient(
            allowed_hosts=TRADIER_ALLOWED_HOSTS,
            timeout_seconds=settings.tradier_timeout_seconds,
            retry_attempts=settings.tradier_retry_attempts,
            allowed_methods={"GET"},
            transport=transport,
            clock=clock,
            sleeper=actual_sleeper,
        )

    async def fetch(self) -> ProviderResult:
        symbols = _csv_symbols(self.settings.tradier_cross_asset_symbols)
        quotes = await self.quotes(symbols)
        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=self.clock(),
                freshness=Freshness.LIVE,
            ),
            data={
                "quotes": quotes,
                "environment": self.environment,
                "read_only": True,
                "allowed_endpoints": sorted(TRADIER_ALLOWED_PATHS),
                "streaming_enabled": False,
            },
        )

    def _ready(self) -> None:
        if not self.settings.tradier_enabled:
            raise ProviderDisabled("Tradier provider is disabled")
        if not self.settings.tradier_market_data_enabled:
            raise ProviderDisabled("Tradier market data is disabled")
        if self.settings.tradier_account_access_enabled:
            raise ProviderError("Tradier account access must remain disabled")
        if self.settings.tradier_trading_enabled:
            raise ProviderError("Tradier trading must remain disabled")
        if self.settings.tradier_streaming_enabled:
            raise ProviderError("Tradier streaming is outside the integration contract")
        if not self.token:
            raise ProviderError(f"Tradier {self.environment} token is not configured")

    async def request(
        self,
        path: str,
        *,
        params: dict[str, Any],
        method: str = "GET",
        endpoint_category: str,
    ) -> tuple[Any, dict[str, Any], dict[str, str]]:
        self._ready()
        if str(method).upper() != "GET":
            raise DeterministicProviderError("Tradier only permits GET")
        normalized_path = "/" + str(path).lstrip("/")
        expected_prefix = urlparse(self.base_url).path.rstrip("/")
        full_path = (
            normalized_path
            if normalized_path.startswith(f"{expected_prefix}/")
            else f"{expected_prefix}{normalized_path}"
        )
        if full_path not in TRADIER_ALLOWED_PATHS:
            raise DeterministicProviderError("Tradier endpoint is not allowlisted")
        if any(segment in full_path.lower() for segment in ("/accounts", "/orders")):
            raise DeterministicProviderError("Tradier account/order endpoint rejected")
        payload, telemetry, request = await self.http.request(
            "GET",
            f"{urlparse(self.base_url).scheme}://{urlparse(self.base_url).netloc}{full_path}",
            endpoint_category=endpoint_category,
            provider=self.source,
            params=params,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            },
            before_attempt=self.rate_limiter.acquire,
        )
        return payload, telemetry.as_dict(), request

    async def quotes(self, symbols: Iterable[str]) -> list[dict[str, Any]]:
        requested = list(dict.fromkeys(_symbol(item) for item in symbols))
        if not requested:
            raise ValueError("at least one Tradier quote symbol is required")
        resolution = await self.parametric_cache.resolve(
            provider=self.source,
            endpoint="quotes",
            environment=self.environment,
            parameters={"symbols": sorted(requested), "greeks": False},
            ttl_seconds=self.settings.tradier_cache_ttl_seconds,
            loader=lambda: self._quotes_uncached(requested),
        )
        return self._with_cache_telemetry(resolution)

    async def _quotes_uncached(self, requested: list[str]) -> list[dict[str, Any]]:
        payload, telemetry, request = await self.request(
            "/markets/quotes",
            params={"symbols": ",".join(requested), "greeks": "false"},
            endpoint_category="quotes",
        )
        container = payload.get("quotes") if isinstance(payload, dict) else None
        rows = as_list(container, field_name="quote")
        if not rows and isinstance(container, dict) and "symbol" in container:
            rows = [container]
        output: list[dict[str, Any]] = []
        rejected = 0
        for row in rows:
            symbol = _symbol(row.get("symbol"))
            if symbol not in requested:
                rejected += 1
                continue
            bid = _number(row.get("bid"))
            ask = _number(row.get("ask"))
            if bid is not None and ask is not None and bid > ask:
                rejected += 1
                continue
            observed_at = _tradier_timestamp(
                row.get("trade_date") or row.get("bid_date") or row.get("ask_date")
            )
            output.append(
                {
                    "symbol": symbol,
                    "description": row.get("description"),
                    "last": _number(row.get("last")),
                    "bid": bid,
                    "ask": ask,
                    "open": _number(row.get("open")),
                    "high": _number(row.get("high")),
                    "low": _number(row.get("low")),
                    "close": _number(row.get("close") or row.get("prevclose")),
                    "change": _number(row.get("change")),
                    "change_percentage": _number(row.get("change_percentage")),
                    "volume": _number(row.get("volume")),
                    "average_volume": _number(row.get("average_volume")),
                    "observed_at": observed_at.isoformat() if observed_at else None,
                    "retrieved_at": self.clock().isoformat(),
                    "freshness_state": _quote_freshness(observed_at, self.clock()),
                    "source": self.source,
                    "environment": self.environment,
                    "source_url": request["source_url"],
                    "request_fingerprint": request["request_fingerprint"],
                    "raw_payload_hash": safe_payload_hash(row),
                    "proxy_instrument": True,
                    "instrument_type": "ETF",
                }
            )
        telemetry["accepted"] = len(output)
        telemetry["rejected"] = rejected
        for item in output:
            item["telemetry"] = telemetry
        return output

    async def expirations(self, symbol: str) -> list[date]:
        normalized = _symbol(symbol)
        resolution = await self.parametric_cache.resolve(
            provider=self.source,
            endpoint="option_expirations",
            environment=self.environment,
            parameters={"symbol": normalized},
            ttl_seconds=self.settings.tradier_cache_ttl_seconds,
            loader=lambda: self._expirations_uncached(normalized),
        )
        network_telemetry = (
            self._uncached_telemetry.get("option_expirations", {})
            if resolution.cache_status == "REFRESHED"
            else {}
        )
        self.last_telemetry[
            f"option_expirations:{resolution.cache_key}"
        ] = {
            **resolution.telemetry,
            **network_telemetry,
            "cache_status": resolution.cache_status,
            "cache_key": resolution.cache_key,
        }
        return [
            parsed
            for value in (resolution.value or [])
            if (parsed := _date_value(value)) is not None
        ]

    async def _expirations_uncached(self, normalized: str) -> list[str]:
        payload, telemetry, _ = await self.request(
            "/markets/options/expirations",
            params={"symbol": normalized, "includeAllRoots": "true", "strikes": "false"},
            endpoint_category="option_expirations",
        )
        self._uncached_telemetry["option_expirations"] = telemetry
        container = payload.get("expirations") if isinstance(payload, dict) else None
        values = container.get("date") if isinstance(container, dict) else container
        if isinstance(values, str):
            values = [values]
        output: list[date] = []
        for value in values or []:
            try:
                parsed = date.fromisoformat(str(value))
            except ValueError:
                continue
            if parsed >= self.clock().date():
                output.append(parsed)
        return [item.isoformat() for item in sorted(set(output))]

    async def option_chain(self, symbol: str, expiration: date) -> list[dict[str, Any]]:
        normalized = _symbol(symbol)
        if expiration < self.clock().date():
            raise DeterministicProviderError("past Tradier expiration rejected")
        resolution = await self.parametric_cache.resolve(
            provider=self.source,
            endpoint="option_chain",
            environment=self.environment,
            parameters={
                "symbol": normalized,
                "expiration": expiration.isoformat(),
                "greeks": True,
            },
            ttl_seconds=self.settings.tradier_cache_ttl_seconds,
            loader=lambda: self._option_chain_uncached(normalized, expiration),
        )
        return self._with_cache_telemetry(resolution)

    async def _option_chain_uncached(
        self,
        normalized: str,
        expiration: date,
    ) -> list[dict[str, Any]]:
        payload, telemetry, request = await self.request(
            "/markets/options/chains",
            params={
                "symbol": normalized,
                "expiration": expiration.isoformat(),
                "greeks": "true",
            },
            endpoint_category="option_chain",
        )
        container = payload.get("options") if isinstance(payload, dict) else None
        rows = as_list(container, field_name="option")
        output: list[dict[str, Any]] = []
        rejected = 0
        for row in rows:
            try:
                normalized_row = _normalize_contract(
                    row,
                    underlying=normalized,
                    expiration=expiration,
                    retrieved_at=self.clock(),
                    environment=self.environment,
                )
            except DeterministicProviderError:
                rejected += 1
                continue
            normalized_row.update(
                {
                    "source": self.source,
                    "source_url": request["source_url"],
                    "request_fingerprint": request["request_fingerprint"],
                    "raw_payload_hash": safe_payload_hash(row),
                }
            )
            output.append(normalized_row)
        telemetry["accepted"] = len(output)
        telemetry["rejected"] = rejected
        if rows and not output:
            telemetry.setdefault("anomalies", []).append("chain_empty_after_validation")
        for item in output:
            item["telemetry"] = telemetry
        return output

    def _with_cache_telemetry(self, resolution: CacheResolution) -> list[dict[str, Any]]:
        endpoint = str(resolution.telemetry["endpoint_category"])
        telemetry_key = (
            f"{endpoint}:{resolution.cache_key}"
            if endpoint == "option_chain"
            else endpoint
        )
        output = [dict(item) for item in (resolution.value or [])]
        network_telemetry = (
            dict((output[0].get("telemetry") or {}))
            if resolution.cache_status == "REFRESHED" and output
            else {}
        )
        self.last_telemetry[telemetry_key] = {
            **resolution.telemetry,
            **network_telemetry,
            "cache_status": resolution.cache_status,
            "cache_key": resolution.cache_key,
        }
        if resolution.cache_status != "REFRESHED":
            for item in output:
                item["telemetry"] = dict(self.last_telemetry[telemetry_key])
        return output

    async def relevant_option_chains(
        self,
        symbol: str = "QQQ",
        *,
        max_expirations: int = 3,
    ) -> dict[str, Any]:
        normalized = _symbol(symbol)
        available = await self.expirations(normalized)
        selected = select_relevant_expirations(
            available,
            as_of=self.clock().date(),
            maximum=min(max(1, int(max_expirations)), 3),
        )
        chains: dict[str, list[dict[str, Any]]] = {}
        for expiration in selected:
            chains[expiration.isoformat()] = await self.option_chain(normalized, expiration)
        return {
            "underlying": normalized,
            "target_context": "MNQ" if normalized == "QQQ" else normalized,
            "relationship": (
                "Nasdaq-100 liquid ETF proxy" if normalized == "QQQ" else "configured ETF proxy"
            ),
            "proxy_used": True,
            "provider": self.source,
            "environment": self.environment,
            "retrieved_at": self.clock().isoformat(),
            "selected_expirations": [item.isoformat() for item in selected],
            "chains": chains,
        }


def select_relevant_expirations(
    expirations: Iterable[date],
    *,
    as_of: date,
    maximum: int = 3,
) -> list[date]:
    values = sorted({item for item in expirations if item >= as_of})
    if not values:
        return []
    first = values[0]
    next_weekly = next((item for item in values if item > first), None)
    target = as_of + timedelta(days=30)
    nearest_30 = min(values, key=lambda item: (abs((item - target).days), item))
    selected = [first, next_weekly, nearest_30]
    return list(dict.fromkeys(item for item in selected if item is not None))[:maximum]


def _configuration(settings: Settings) -> tuple[str, str, str | None]:
    environment = str(settings.tradier_environment or "").strip().lower()
    if environment == "production":
        return (
            environment,
            settings.tradier_production_base_url.rstrip("/"),
            settings.tradier_production_token,
        )
    if environment == "sandbox":
        return (
            environment,
            settings.tradier_sandbox_base_url.rstrip("/"),
            settings.tradier_sandbox_token,
        )
    raise ProviderError("Tradier environment must be production or sandbox")


def _normalize_contract(
    row: dict[str, Any],
    *,
    underlying: str,
    expiration: date,
    retrieved_at: datetime,
    environment: str,
) -> dict[str, Any]:
    strike = _number(row.get("strike"))
    if strike is None or strike < 0:
        raise DeterministicProviderError("Tradier negative or missing strike")
    option_type = str(row.get("option_type") or "").lower()
    if option_type not in {"call", "put"}:
        raise DeterministicProviderError("Tradier invalid option type")
    bid = _number(row.get("bid"))
    ask = _number(row.get("ask"))
    if bid is not None and ask is not None and bid > ask:
        raise DeterministicProviderError("Tradier bid exceeds ask")
    greeks = row.get("greeks") if isinstance(row.get("greeks"), dict) else {}
    contract_size = _number(row.get("contract_size"))
    return {
        "contract_symbol": row.get("symbol"),
        "underlying": underlying,
        "expiration": expiration.isoformat(),
        "strike": strike,
        "option_type": option_type,
        "bid": bid,
        "ask": ask,
        "last": _number(row.get("last")),
        "volume": _number(row.get("volume")),
        "open_interest": _number(row.get("open_interest")),
        "contract_size": contract_size,
        "iv": _number(greeks.get("mid_iv") or greeks.get("smv_vol") or row.get("iv")),
        "delta": _number(greeks.get("delta")),
        "gamma": _number(greeks.get("gamma")),
        "theta": _number(greeks.get("theta")),
        "vega": _number(greeks.get("vega")),
        "rho": _number(greeks.get("rho")),
        "provider_timestamp": greeks.get("updated_at"),
        "retrieved_at": retrieved_at.isoformat(),
        "environment": environment,
    }


def _csv_symbols(value: str) -> list[str]:
    return [_symbol(item) for item in str(value).split(",") if item.strip()]


def _symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or not symbol.replace(".", "").replace("-", "").isalnum():
        raise ValueError("invalid Tradier symbol")
    return symbol


def _number(value: Any) -> int | float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DeterministicProviderError("Tradier impossible numeric value") from exc
    if number != number or number in {float("inf"), float("-inf")}:
        raise DeterministicProviderError("Tradier non-finite numeric value")
    return int(number) if number.is_integer() else number


def _tradier_timestamp(value: Any) -> datetime | None:
    if value in (None, "", 0):
        return None
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            raw = int(value)
            if raw > 10_000_000_000:
                raw //= 1000
            return datetime.fromtimestamp(raw, tz=UTC)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except (TypeError, ValueError, OSError):
        return None


def _quote_freshness(observed_at: datetime | None, now: datetime) -> str:
    if observed_at is None:
        return "UNKNOWN"
    age = now - observed_at
    if age < timedelta(0):
        return "REJECTED_FUTURE"
    if age <= timedelta(minutes=2):
        return "FRESH"
    if age <= timedelta(minutes=20):
        return "DELAYED"
    return "STALE"


def _date_value(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
