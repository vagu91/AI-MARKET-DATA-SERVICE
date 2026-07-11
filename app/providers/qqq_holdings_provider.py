import csv
import json
import re
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from io import StringIO
from typing import Any

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
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
    alpha_negative_cache_key = "provider:qqq_holdings:alpha_vantage:negative"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch_safe(self) -> ProviderResult:
        valid_cached = self._cached_result(max_age_hours=self.settings.qqq_holdings_ttl_hours)
        if valid_cached:
            return _with_quality_updates(
                valid_cached,
                cache_used=True,
                last_known_good_used=True,
                final_status="found",
                actual_network_calls=0,
                provider_attempts=["db_valid_cache"],
            )
        try:
            result = await self.fetch()
        except Exception as exc:
            stale = self._cached_result(
                max_age_hours=self.settings.qqq_holdings_ttl_hours + self.settings.qqq_holdings_stale_tolerance_hours
            )
            if stale:
                return _with_quality_updates(
                    stale,
                    cache_used=True,
                    last_known_good_used=True,
                    final_status="stale_acceptable",
                    actual_network_calls=0,
                    warnings=[_aggregate_warning(f"provider_exception:{exc or type(exc).__name__}")],
                )
            return _failure_result(str(exc) or "QQQ holdings provider failed")

        data = result.data if isinstance(result.data, dict) else {}
        quality = data.get("data_quality") if isinstance(data.get("data_quality"), dict) else {}
        if data.get("holdings") and not quality.get("provider_failed"):
            self.cache.set(self.cache_key, result.model_dump(mode="json"))
            return result
        stale = self._cached_result(
            max_age_hours=self.settings.qqq_holdings_ttl_hours + self.settings.qqq_holdings_stale_tolerance_hours
        )
        if stale:
            return _with_quality_updates(
                stale,
                cache_used=True,
                last_known_good_used=True,
                final_status="stale_acceptable",
                actual_network_calls=int(quality.get("actual_network_calls") or 0),
                provider_attempts=list(quality.get("provider_attempts") or []),
                warnings=list(quality.get("warnings") or []),
                alpha_vantage_status=quality.get("alpha_vantage_status"),
                alpha_vantage_rate_limited=bool(quality.get("alpha_vantage_rate_limited")),
                alpha_vantage_next_retry_at=quality.get("alpha_vantage_next_retry_at"),
                invesco_status=quality.get("invesco_status"),
                invesco_http_status=quality.get("invesco_http_status"),
            )
        return result

    async def fetch(self) -> ProviderResult:
        diagnostics = _diagnostics()
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            try:
                diagnostics["provider_attempts"].append("invesco")
                diagnostics["actual_network_calls"] += 1
                response = await client.get(
                    self.settings.invesco_qqq_holdings_url,
                    headers=REQUEST_HEADERS,
                    timeout=min(float(self.settings.http_timeout_seconds), 8.0),
                )
                diagnostics["invesco_http_status"] = response.status_code
                if response.status_code == 403:
                    diagnostics["invesco_status"] = "access_restricted"
                    diagnostics["invesco_retryable"] = False
                    raise ProviderError("Invesco holdings access_restricted http_status=403")
                response.raise_for_status()
                holdings, as_of, parse_errors = parse_invesco_holdings_csv(response.text)
                diagnostics["errors"].extend(parse_errors)
                if holdings:
                    diagnostics["invesco_status"] = "found"
                    return ProviderResult(
                        metadata=metadata(
                            source="Invesco QQQ Holdings",
                            provider_type=ProviderType.CSV,
                            reliability=0.88,
                            data_as_of=_parse_as_of(as_of),
                            freshness=Freshness.RECENT,
                            errors=diagnostics["errors"],
                        ),
                        data={
                            "as_of": as_of,
                            "holdings": [item.model_dump(mode="json") for item in holdings],
                            "data_quality": _quality(
                                holdings,
                                diagnostics,
                                final_source="Invesco QQQ Holdings",
                                final_status="found",
                                official_etf_holdings=True,
                            ),
                        },
                    )
                diagnostics["invesco_status"] = "not_found"
                diagnostics["errors"].append("Invesco holdings returned no normalized holdings")
            except Exception as exc:
                if diagnostics.get("invesco_status") is None:
                    diagnostics["invesco_status"] = "provider_failed"
                diagnostics["errors"].append(f"Invesco holdings request failed: {exc or 'empty error detail'}")

            if self.settings.alpha_vantage_api_key:
                negative = self._alpha_negative_cache()
                if negative:
                    diagnostics["provider_attempts"].append("alpha_vantage_negative_cache")
                    diagnostics["alpha_vantage_status"] = str(negative.get("status") or "rate_limited")
                    diagnostics["alpha_vantage_rate_limited"] = True
                    diagnostics["alpha_vantage_next_retry_at"] = negative.get("next_retry_at")
                else:
                    try:
                        diagnostics["provider_attempts"].append("alpha_vantage")
                        diagnostics["actual_network_calls"] += 1
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
                        if is_alpha_vantage_daily_rate_limited(payload):
                            next_retry_at = _next_utc_midnight()
                            diagnostics["alpha_vantage_status"] = "rate_limited"
                            diagnostics["alpha_vantage_rate_limited"] = True
                            diagnostics["alpha_vantage_next_retry_at"] = next_retry_at
                            self.cache.set(
                                self.alpha_negative_cache_key,
                                {
                                    "status": "rate_limited",
                                    "negative_cache_reason": "provider_daily_rate_limit",
                                    "retryable": "false_for_current_run",
                                    "next_retry_at": next_retry_at,
                                },
                            )
                        else:
                            ensure_alpha_payload_ok(payload)
                            holdings, parse_errors = parse_alpha_vantage_etf_profile(payload)
                            diagnostics["errors"].extend(parse_errors)
                            if holdings:
                                diagnostics["alpha_vantage_status"] = "found"
                                return ProviderResult(
                                    metadata=metadata(
                                        source="Alpha Vantage ETF_PROFILE",
                                        provider_type=ProviderType.API,
                                        reliability=0.9,
                                        freshness=Freshness.RECENT,
                                        errors=diagnostics["errors"],
                                    ),
                                    data={
                                        "as_of": datetime.now(UTC).date().isoformat(),
                                        "holdings": [item.model_dump(mode="json") for item in holdings],
                                        "data_quality": _quality(
                                            holdings,
                                            diagnostics,
                                            final_source="Alpha Vantage ETF_PROFILE",
                                            final_status="found",
                                            official_etf_holdings=True,
                                        ),
                                    },
                                )
                            diagnostics["alpha_vantage_status"] = "not_found"
                            diagnostics["errors"].append("Alpha Vantage ETF_PROFILE returned no holdings")
                    except Exception as exc:
                        if diagnostics.get("alpha_vantage_status") is None:
                            diagnostics["alpha_vantage_status"] = "provider_failed"
                        diagnostics["errors"].append(str(exc) or "Alpha Vantage ETF_PROFILE failed")
            else:
                diagnostics["alpha_vantage_status"] = "not_configured"

            try:
                diagnostics["provider_attempts"].append("nasdaq_100_proxy")
                diagnostics["actual_network_calls"] += 1
                response = await client.get(
                    self.settings.nasdaq_100_constituents_url,
                    headers=_nasdaq_headers(),
                    timeout=min(float(self.settings.http_timeout_seconds), 8.0),
                )
                response.raise_for_status()
                holdings = parse_nasdaq_constituents(response.text)
                if not holdings:
                    raise ProviderError("Nasdaq-100 fallback returned no holdings")
                diagnostics["nasdaq_proxy_used"] = True
                return ProviderResult(
                    metadata=metadata(
                        source="Nasdaq-100 Constituents",
                        provider_type=ProviderType.API,
                        reliability=0.72,
                        freshness=Freshness.RECENT,
                        is_fallback=True,
                        errors=[],
                    ),
                    data={
                        "status": "proxy",
                        "as_of": datetime.now(UTC).date().isoformat(),
                        "holdings": [item.model_dump(mode="json") for item in holdings],
                        "source": "Nasdaq-100 constituents",
                        "proxy_for": "QQQ holdings",
                        "is_proxy": True,
                        "holdings_count": len(holdings),
                        "weight_data_available": False,
                        "official_etf_holdings": False,
                        "data_quality": _quality(
                            holdings,
                            diagnostics,
                            fallback_used=True,
                            final_source="Nasdaq-100 constituents",
                            final_status="proxy",
                            is_proxy=True,
                            proxy_for="QQQ holdings",
                            official_etf_holdings=False,
                            weight_data_available=False,
                        ),
                    },
                )
            except Exception as exc:
                diagnostics["errors"].append(f"Nasdaq-100 fallback request failed: {exc or 'empty error detail'}")
                return ProviderResult(
                    metadata=metadata(
                        source="QQQ Holdings",
                        provider_type=ProviderType.API,
                        reliability=0.0,
                        freshness=Freshness.UNKNOWN,
                        is_fallback=True,
                        errors=[],
                    ),
                    data={
                        "as_of": None,
                        "holdings": [],
                        "data_quality": _quality(
                            [],
                            diagnostics,
                            final_source="none",
                            final_status="not_found",
                            provider_failed=True,
                            final_data_available=False,
                        ),
                    },
                )

    def _cached_result(self, *, max_age_hours: int | float) -> ProviderResult | None:
        entry = self.cache.get_entry(self.cache_key)
        if not entry:
            return None
        updated_at = _parse_dt(entry.get("updated_at"))
        if updated_at is None or datetime.now(UTC) - updated_at > timedelta(hours=float(max_age_hours)):
            return None
        try:
            result = ProviderResult.model_validate(entry["payload"])
        except Exception:
            return None
        data = result.data if isinstance(result.data, dict) else {}
        if not data.get("holdings"):
            return None
        result.metadata.provider_type = ProviderType.CACHE
        result.metadata.is_fallback = True
        result.metadata.retrieved_at = datetime.now(UTC)
        return result

    def _alpha_negative_cache(self) -> dict[str, Any] | None:
        payload = self.cache.get(self.alpha_negative_cache_key)
        if not isinstance(payload, dict):
            return None
        next_retry_at = _parse_dt(payload.get("next_retry_at"))
        if next_retry_at and next_retry_at > datetime.now(UTC):
            return payload
        return None


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


def is_alpha_vantage_daily_rate_limited(payload: dict[str, Any]) -> bool:
    message = str(payload.get("Information") or payload.get("Note") or "")
    lowered = message.lower()
    return "standard api rate limit" in lowered and "25 requests per day" in lowered


def parse_nasdaq_constituents(text: str) -> list[QQQHolding]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = {}
    rows: list[dict[str, Any]] = []
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        nested = data.get("data")
        if isinstance(nested, dict):
            rows = nested.get("rows") or []
        elif isinstance(nested, list):
            rows = nested
        rows = rows or data.get("rows") or []
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


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _next_utc_midnight() -> str:
    now = datetime.now(UTC)
    tomorrow = (now + timedelta(days=1)).date()
    return datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _nasdaq_headers() -> dict[str, str]:
    return {
        **REQUEST_HEADERS,
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    }


def _diagnostics() -> dict[str, Any]:
    return {
        "provider_attempts": [],
        "actual_network_calls": 0,
        "run_deduplicated_calls": 0,
        "run_cache_used": False,
        "alpha_vantage_status": None,
        "alpha_vantage_rate_limited": False,
        "alpha_vantage_next_retry_at": None,
        "invesco_status": None,
        "invesco_http_status": None,
        "invesco_retryable": None,
        "nasdaq_proxy_used": False,
        "last_known_good_used": False,
        "errors": [],
    }


def _quality(
    holdings: list[QQQHolding],
    diagnostics: dict[str, Any] | list[str],
    fallback_used: bool = False,
    final_source: str | None = None,
    final_status: str = "found",
    provider_failed: bool = False,
    final_data_available: bool | None = None,
    is_proxy: bool = False,
    proxy_for: str | None = None,
    official_etf_holdings: bool = True,
    weight_data_available: bool | None = None,
) -> dict[str, object]:
    if isinstance(diagnostics, list):
        diagnostics = {**_diagnostics(), "errors": diagnostics}
    weights_available = all(item.weight is not None for item in holdings) if holdings else False
    if weight_data_available is None:
        weight_data_available = weights_available
    warning = _aggregate_warning(
        f"invesco={diagnostics.get('invesco_status')}",
        f"alpha_vantage={diagnostics.get('alpha_vantage_status')}",
        f"nasdaq_proxy_used={diagnostics.get('nasdaq_proxy_used')}",
        f"final={final_status}",
    )
    return {
        "count": len(holdings),
        "holdings_count": len(holdings),
        "missing_weights": any(item.weight is None for item in holdings),
        "stale": False,
        "fallback_used": fallback_used,
        "errors": [],
        "warnings": [warning] if final_status not in {"found"} or diagnostics.get("errors") else [],
        "final_data_available": bool(holdings) if final_data_available is None else final_data_available,
        "no_data_found": not bool(holdings),
        "provider_failed": provider_failed,
        "rate_limited": bool(diagnostics.get("alpha_vantage_rate_limited")),
        "provider_attempts": list(diagnostics.get("provider_attempts") or []),
        "actual_network_calls": int(diagnostics.get("actual_network_calls") or 0),
        "run_deduplicated_calls": int(diagnostics.get("run_deduplicated_calls") or 0),
        "run_cache_used": bool(diagnostics.get("run_cache_used")),
        "alpha_vantage_status": diagnostics.get("alpha_vantage_status"),
        "alpha_vantage_rate_limited": bool(diagnostics.get("alpha_vantage_rate_limited")),
        "alpha_vantage_next_retry_at": diagnostics.get("alpha_vantage_next_retry_at"),
        "invesco_status": diagnostics.get("invesco_status"),
        "invesco_http_status": diagnostics.get("invesco_http_status"),
        "nasdaq_proxy_used": bool(diagnostics.get("nasdaq_proxy_used")),
        "last_known_good_used": bool(diagnostics.get("last_known_good_used")),
        "final_source": final_source,
        "final_status": final_status,
        "weights_available": weights_available,
        "is_proxy": is_proxy,
        "proxy_for": proxy_for,
        "official_etf_holdings": official_etf_holdings,
        "weight_data_available": bool(weight_data_available),
    }


def _with_quality_updates(
    result: ProviderResult,
    *,
    cache_used: bool,
    last_known_good_used: bool,
    final_status: str,
    actual_network_calls: int,
    provider_attempts: list[str] | None = None,
    warnings: list[str] | None = None,
    alpha_vantage_status: Any = None,
    alpha_vantage_rate_limited: bool = False,
    alpha_vantage_next_retry_at: Any = None,
    invesco_status: Any = None,
    invesco_http_status: Any = None,
) -> ProviderResult:
    data = result.data if isinstance(result.data, dict) else {}
    quality = data.setdefault("data_quality", {})
    holdings = data.get("holdings") or []
    quality.update(
        {
            "count": len(holdings),
            "holdings_count": len(holdings),
            "stale": final_status == "stale_acceptable",
            "fallback_used": True,
            "final_data_available": bool(holdings),
            "no_data_found": not bool(holdings),
            "provider_failed": False,
            "cache_used": cache_used,
            "last_known_good_used": last_known_good_used,
            "final_status": final_status,
            "final_source": data.get("source") or result.metadata.source,
            "actual_network_calls": actual_network_calls,
            "provider_attempts": provider_attempts or ["db_cache"],
            "alpha_vantage_status": alpha_vantage_status if alpha_vantage_status is not None else quality.get("alpha_vantage_status"),
            "alpha_vantage_rate_limited": alpha_vantage_rate_limited or bool(quality.get("alpha_vantage_rate_limited")),
            "alpha_vantage_next_retry_at": alpha_vantage_next_retry_at or quality.get("alpha_vantage_next_retry_at"),
            "invesco_status": invesco_status if invesco_status is not None else quality.get("invesco_status"),
            "invesco_http_status": invesco_http_status if invesco_http_status is not None else quality.get("invesco_http_status"),
            "weights_available": all(item.get("weight") is not None for item in holdings) if holdings else False,
            "weight_data_available": not any(item.get("weight") is None for item in holdings) if holdings else False,
            "warnings": warnings or quality.get("warnings") or [],
        }
    )
    result.data = data
    return result


def _failure_result(reason: str) -> ProviderResult:
    return ProviderResult(
        metadata=metadata(
            source="QQQ Holdings",
            provider_type=ProviderType.API,
            reliability=0.0,
            freshness=Freshness.UNKNOWN,
            is_fallback=True,
            errors=[],
        ),
        data={
            "as_of": None,
            "holdings": [],
            "data_quality": _quality(
                [],
                {**_diagnostics(), "errors": [reason]},
                final_source="none",
                final_status="not_found",
                provider_failed=True,
                final_data_available=False,
            ),
        },
    )


def _aggregate_warning(*parts: str) -> str:
    return "QQQ holdings fallback summary: " + "; ".join(part for part in parts if part and "None" not in part)


def _dedupe_errors(errors: list[str]) -> list[str]:
    deduped = []
    for error in errors:
        if error and error not in deduped:
            deduped.append(error)
    return deduped
