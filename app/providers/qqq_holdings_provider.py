import json
import re
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from typing import Any

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.nasdaq import QQQHolding
from app.providers.alpha_vantage import ensure_alpha_payload_ok, parse_percent
from app.providers.base import BaseProvider, ProviderError, metadata
from app.providers.calendar_utils import REQUEST_HEADERS
from app.providers.sec_class_shares_provider import SecClassSharesProvider
from app.services.nasdaq_multiclass_service import (
    apply_multi_class_adjustments,
    detect_multi_class_groups,
    log_multi_class_event,
)
from app.services.qqq_weight_intelligence_service import (
    EQUAL_WEIGHT_PROXY,
    OFFICIAL_QQQ_WEIGHT,
    RECONSTRUCTED_MARKET_CAP_WEIGHT,
    VENDOR_QQQ_WEIGHT,
    WEIGHT_METHOD_RANK,
    apply_weight_provenance,
    log_weight_event,
    parse_csv_holdings,
    reconstruct_market_cap_weights,
    validate_weight_set,
)

class QQQHoldingsProvider(BaseProvider):
    source = "QQQ Holdings"
    provider_type = ProviderType.API
    reliability = 0.9
    cache_key = "provider:qqq_holdings:v2"
    alpha_negative_cache_key = "provider:qqq_holdings:alpha_vantage:negative"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings
        self.sec_class_shares = SecClassSharesProvider(settings)

    async def fetch_safe(self, *, force: bool = False) -> ProviderResult:
        valid_cached = self._cached_result(max_age_hours=self.settings.qqq_holdings_ttl_hours)
        if valid_cached and not force:
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
        if valid_cached and _result_rank(valid_cached) > _result_rank(result):
            result = _preserve_better_cached_result(valid_cached, result)
            data = result.data if isinstance(result.data, dict) else {}
            quality = data.get("data_quality") if isinstance(data.get("data_quality"), dict) else {}
        if data.get("holdings") and not quality.get("provider_failed"):
            if data.get("weight_method") != EQUAL_WEIGHT_PROXY:
                self.cache.set(self.cache_key, result.model_dump(mode="json"))
                log_weight_event(
                    "qqq_weight_persisted",
                    source=data.get("source"),
                    method=data.get("weight_method"),
                    constituent_count=len(data.get("holdings") or []),
                    total_weight_pct=quality.get("total_weight_pct"),
                )
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
        log_weight_event("qqq_weight_lookup_started", method="ranked_source_cascade")
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            try:
                diagnostics["provider_attempts"].append("invesco")
                diagnostics["actual_network_calls"] += 1
                diagnostics["source_attempt_count"] += 1
                log_weight_event(
                    "qqq_weight_source_attempted",
                    source="Invesco QQQ Holdings",
                    source_url=self.settings.invesco_qqq_holdings_url,
                    method=OFFICIAL_QQQ_WEIGHT,
                )
                response = await client.get(
                    self.settings.invesco_qqq_holdings_url,
                    headers=REQUEST_HEADERS,
                    timeout=min(float(self.settings.http_timeout_seconds), 8.0),
                )
                diagnostics["invesco_http_status"] = response.status_code
                if response.status_code in {401, 403, 406}:
                    diagnostics["invesco_status"] = "access_restricted"
                    diagnostics["invesco_retryable"] = False
                    diagnostics["failure_breakdown"]["access_restricted"] += 1
                    raise ProviderError(
                        f"Invesco holdings access_restricted http_status={response.status_code}"
                    )
                response.raise_for_status()
                holdings, as_of, parse_errors = parse_invesco_holdings_csv(response.text)
                diagnostics["errors"].extend(parse_errors)
                if holdings:
                    candidate = _weighted_candidate(
                        holdings=[item.model_dump(mode="json") for item in holdings],
                        source="Invesco QQQ Holdings",
                        source_url=self.settings.invesco_qqq_holdings_url,
                        method=OFFICIAL_QQQ_WEIGHT,
                        as_of=as_of,
                        official=True,
                        reconstructed=False,
                        confidence=0.98,
                        ttl_hours=self.settings.qqq_holdings_ttl_hours,
                        total_tolerance_pct=self.settings.qqq_weight_total_tolerance_pct,
                        minimum_coverage_pct=self.settings.qqq_weight_min_coverage_pct,
                        maximum_constituent_pct=self.settings.qqq_weight_max_constituent_pct,
                        diagnostics=diagnostics,
                    )
                    if candidate["validation"]["valid"]:
                        diagnostics["invesco_status"] = "found"
                        diagnostics["source_success_count"] += 1
                        diagnostics["official_source_success"] = True
                        log_weight_event(
                            "qqq_weight_source_succeeded",
                            source="Invesco QQQ Holdings",
                            method=OFFICIAL_QQQ_WEIGHT,
                            constituent_count=len(candidate["holdings"]),
                            total_weight_pct=candidate["validation"].get("total_weight_pct"),
                        )
                        return _candidate_result(candidate, diagnostics, ProviderType.CSV, 0.98)
                    diagnostics["failure_breakdown"][_validation_failure(candidate)] += 1
                    log_weight_event(
                        "qqq_weight_set_rejected",
                        source="Invesco QQQ Holdings",
                        method=OFFICIAL_QQQ_WEIGHT,
                        fallback_reason=_validation_failure(candidate),
                    )
                diagnostics["invesco_status"] = "not_found"
                diagnostics["errors"].append("Invesco holdings returned no normalized holdings")
            except Exception as exc:
                diagnostics["source_failure_count"] += 1
                if diagnostics.get("invesco_status") is None:
                    diagnostics["invesco_status"] = "provider_failed"
                    diagnostics["failure_breakdown"]["official_unavailable"] += 1
                diagnostics["errors"].append(f"Invesco holdings request failed: {exc or 'empty error detail'}")
                log_weight_event(
                    "qqq_weight_source_failed",
                    source="Invesco QQQ Holdings",
                    method=OFFICIAL_QQQ_WEIGHT,
                    fallback_reason=str(exc),
                )

            if self.settings.alpha_vantage_api_key:
                negative = self._alpha_negative_cache()
                if negative:
                    diagnostics["provider_attempts"].append("alpha_vantage_negative_cache")
                    diagnostics["alpha_vantage_status"] = str(negative.get("status") or "rate_limited")
                    diagnostics["alpha_vantage_rate_limited"] = True
                    diagnostics["alpha_vantage_next_retry_at"] = negative.get("next_retry_at")
                    diagnostics["failure_breakdown"]["rate_limited"] += 1
                else:
                    try:
                        diagnostics["provider_attempts"].append("alpha_vantage")
                        diagnostics["actual_network_calls"] += 1
                        diagnostics["source_attempt_count"] += 1
                        log_weight_event(
                            "qqq_weight_source_attempted",
                            source="Alpha Vantage ETF_PROFILE",
                            source_url=self.settings.alpha_vantage_base_url,
                            method=VENDOR_QQQ_WEIGHT,
                        )
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
                            diagnostics["source_failure_count"] += 1
                            diagnostics["failure_breakdown"]["rate_limited"] += 1
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
                                candidate = _weighted_candidate(
                                    holdings=[item.model_dump(mode="json") for item in holdings],
                                    source="Alpha Vantage ETF_PROFILE",
                                    source_url=self.settings.alpha_vantage_base_url,
                                    method=VENDOR_QQQ_WEIGHT,
                                    as_of=datetime.now(UTC).date().isoformat(),
                                    official=False,
                                    reconstructed=False,
                                    confidence=0.88,
                                    ttl_hours=self.settings.qqq_holdings_ttl_hours,
                                    total_tolerance_pct=self.settings.qqq_weight_total_tolerance_pct,
                                    minimum_coverage_pct=self.settings.qqq_weight_min_coverage_pct,
                                    maximum_constituent_pct=self.settings.qqq_weight_max_constituent_pct,
                                    diagnostics=diagnostics,
                                )
                                if candidate["validation"]["valid"]:
                                    diagnostics["alpha_vantage_status"] = "found"
                                    diagnostics["source_success_count"] += 1
                                    diagnostics["vendor_source_success"] = True
                                    return _candidate_result(candidate, diagnostics, ProviderType.API, 0.88)
                                diagnostics["failure_breakdown"][_validation_failure(candidate)] += 1
                            diagnostics["alpha_vantage_status"] = "not_found"
                            diagnostics["errors"].append("Alpha Vantage ETF_PROFILE returned no holdings")
                    except Exception as exc:
                        diagnostics["source_failure_count"] += 1
                        if diagnostics.get("alpha_vantage_status") is None:
                            diagnostics["alpha_vantage_status"] = "provider_failed"
                        diagnostics["errors"].append(str(exc) or "Alpha Vantage ETF_PROFILE failed")
            else:
                diagnostics["alpha_vantage_status"] = "not_configured"

            try:
                diagnostics["provider_attempts"].append("nasdaq_100_market_cap")
                diagnostics["actual_network_calls"] += 1
                diagnostics["source_attempt_count"] += 1
                log_weight_event(
                    "qqq_weight_source_attempted",
                    source="Nasdaq-100 Constituents",
                    source_url=self.settings.nasdaq_100_constituents_url,
                    method=RECONSTRUCTED_MARKET_CAP_WEIGHT,
                )
                response = await client.get(
                    self.settings.nasdaq_100_constituents_url,
                    headers=_nasdaq_headers(),
                    timeout=min(float(self.settings.http_timeout_seconds), 8.0),
                )
                response.raise_for_status()
                payload = response.json()
                rows, as_of = _nasdaq_rows(payload)
                if not rows:
                    raise ProviderError("Nasdaq-100 fallback returned no holdings")
                shares_by_issuer: dict[str, dict[str, Any]] = {}
                for group in detect_multi_class_groups(rows):
                    issuer_group = str(group.get("issuer_group") or "")
                    if not group.get("cik"):
                        continue
                    diagnostics["provider_attempts"].append(f"sec_class_shares:{issuer_group}")
                    diagnostics["source_attempt_count"] += 1
                    log_multi_class_event(
                        "class_shares_lookup_started",
                        issuer=group.get("issuer_name"),
                        symbols=group.get("symbols"),
                        source="SEC submissions and inline XBRL",
                    )
                    shares = await self.sec_class_shares.fetch(
                        cik=str(group["cik"]),
                        listed_class_symbols={
                            class_code: symbol
                            for symbol, class_code in (group.get("class_by_symbol") or {}).items()
                        },
                    )
                    diagnostics["actual_network_calls"] += int(shares.get("network_calls") or 0)
                    shares_by_issuer[issuer_group] = shares
                    if shares.get("verified"):
                        diagnostics["source_success_count"] += 1
                        log_multi_class_event(
                            "class_shares_lookup_succeeded",
                            issuer=group.get("issuer_name"),
                            symbols=group.get("symbols"),
                            class_shares=list((shares.get("listed_shares") or {}).values()),
                            source=shares.get("source"),
                            confidence=0.99,
                        )
                    else:
                        diagnostics["source_failure_count"] += 1
                        diagnostics["failure_breakdown"]["partial_response"] += 1
                        log_multi_class_event(
                            "class_shares_lookup_failed",
                            issuer=group.get("issuer_name"),
                            symbols=group.get("symbols"),
                            source=shares.get("source"),
                            reason=";".join(shares.get("errors") or []) or "verified_class_shares_not_found",
                        )
                adjusted_rows, multi_class_quality = apply_multi_class_adjustments(
                    rows,
                    shares_by_issuer,
                )
                candidate = reconstruct_market_cap_weights(
                    adjusted_rows,
                    source="Nasdaq official constituent market-cap snapshot",
                    source_url=self.settings.nasdaq_100_constituents_url,
                    as_of=as_of,
                    ttl_hours=self.settings.qqq_reconstructed_weight_ttl_hours,
                    total_tolerance_pct=self.settings.qqq_weight_total_tolerance_pct,
                    minimum_coverage_pct=self.settings.qqq_weight_min_coverage_pct,
                    maximum_constituent_pct=self.settings.qqq_weight_max_constituent_pct,
                )
                candidate["proxy_for"] = "QQQ holdings / Nasdaq-100 modified weights"
                candidate["multi_class_quality"] = multi_class_quality
                if multi_class_quality.get("multi_class_unresolved_count"):
                    candidate["validation"]["valid"] = False
                    candidate["validation"]["partial"] = True
                    candidate["validation"].setdefault("rejection_reasons", []).append(
                        "multi_class_ambiguous"
                    )
                    log_multi_class_event(
                        "multi_class_weight_validation_failed",
                        symbols=[
                            symbol
                            for item in multi_class_quality.get("multi_class_diagnostics") or []
                            for symbol in item.get("symbols") or []
                        ],
                        classification="ambiguous",
                        confidence=multi_class_quality.get("issuer_semantics_quality_score"),
                        reason="verified_class_shares_unavailable",
                    )
                if candidate["validation"]["valid"]:
                    diagnostics["source_success_count"] += 1
                    diagnostics["reconstruction_used"] = True
                    diagnostics["nasdaq_proxy_used"] = True
                    log_weight_event(
                        "qqq_weight_fallback_selected",
                        source=candidate["source"],
                        method=RECONSTRUCTED_MARKET_CAP_WEIGHT,
                        constituent_count=len(candidate["holdings"]),
                        total_weight_pct=candidate["validation"].get("total_weight_pct"),
                        fallback_reason="official_and_vendor_weights_unavailable",
                    )
                    return _candidate_result(candidate, diagnostics, ProviderType.API, 0.76)
                diagnostics["failure_breakdown"][_validation_failure(candidate)] += 1
                equal_candidate = _equal_weight_candidate(
                    rows,
                    source="Nasdaq-100 constituents",
                    source_url=self.settings.nasdaq_100_constituents_url,
                    as_of=as_of,
                    diagnostics=diagnostics,
                )
                return _candidate_result(equal_candidate, diagnostics, ProviderType.API, 0.35)
            except Exception as exc:
                diagnostics["source_failure_count"] += 1
                diagnostics["failure_breakdown"]["official_unavailable"] += 1
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
                            weight_method=None,
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
        log_weight_event(
            "qqq_weight_read_back",
            source=data.get("source") or result.metadata.source,
            method=data.get("weight_method"),
            constituent_count=len(data.get("holdings") or []),
        )
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
    rows, as_of, errors = parse_csv_holdings(text)
    return [QQQHolding.model_validate(item) for item in rows], as_of, errors


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
                market_cap=_parse_float(row.get("marketCap")),
                price=_parse_float(row.get("lastSalePrice")),
                change_pct=_parse_float(row.get("percentageChange")),
                price_source="Nasdaq",
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
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y", "%b %d, %Y"):
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


def _nasdaq_rows(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    data = payload.get("data") if isinstance(payload, dict) else {}
    data = data if isinstance(data, dict) else {}
    table = data.get("data") if isinstance(data.get("data"), dict) else data
    rows = table.get("rows") or data.get("rows") or []
    as_of = table.get("date") or data.get("date") or table.get("asOf") or data.get("asOf")
    return [row for row in rows if isinstance(row, dict)], str(as_of) if as_of else None


def _weighted_candidate(
    *,
    holdings: list[dict[str, Any]],
    source: str,
    source_url: str,
    method: str,
    as_of: str | None,
    official: bool,
    reconstructed: bool,
    confidence: float,
    ttl_hours: float,
    total_tolerance_pct: float,
    minimum_coverage_pct: float,
    maximum_constituent_pct: float,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(UTC)
    apply_weight_provenance(
        holdings,
        method=method,
        source=source,
        source_url=source_url,
        as_of=as_of,
        retrieved_at=now,
        valid_until=now + timedelta(hours=float(ttl_hours)),
        official=official,
        reconstructed=reconstructed,
        confidence=confidence,
    )
    validation = validate_weight_set(
        holdings,
        total_tolerance_pct=total_tolerance_pct,
        minimum_coverage_pct=minimum_coverage_pct,
        maximum_constituent_pct=maximum_constituent_pct,
    )
    if not validation["valid"]:
        diagnostics["errors"].append(
            f"{source} weight set rejected: {','.join(validation['rejection_reasons']) or 'invalid'}"
        )
    return {
        "status": "found" if validation["valid"] else "partial",
        "as_of": as_of,
        "source": source,
        "source_url": source_url,
        "weight_method": method,
        "weight_is_official": official,
        "weight_is_reconstructed": reconstructed,
        "weight_verified": validation["valid"],
        "weight_confidence": confidence if validation["valid"] else min(confidence, 0.5),
        "weight_valid_until": (now + timedelta(hours=float(ttl_hours))).isoformat(),
        "holdings": sorted(holdings, key=lambda item: float(item.get("weight") or -1), reverse=True),
        "validation": validation,
    }


def _equal_weight_candidate(
    rows: list[dict[str, Any]],
    *,
    source: str,
    source_url: str,
    as_of: str | None,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(UTC)
    base = parse_nasdaq_constituents(json.dumps({"data": {"rows": rows}}))
    weight = 100.0 / len(base) if base else None
    holdings = [item.model_dump(mode="json") for item in base]
    apply_weight_provenance(
        holdings,
        method=EQUAL_WEIGHT_PROXY,
        source=source,
        source_url=source_url,
        as_of=as_of,
        retrieved_at=now,
        valid_until=now + timedelta(hours=1),
        official=False,
        reconstructed=False,
        confidence=0.35,
    )
    for holding in holdings:
        holding["weight"] = weight
        holding["weight_pct"] = weight
        holding["weight_verified"] = False
    diagnostics["equal_weight_used"] = True
    diagnostics["nasdaq_proxy_used"] = True
    return {
        "status": "proxy",
        "as_of": as_of,
        "source": source,
        "source_url": source_url,
        "weight_method": EQUAL_WEIGHT_PROXY,
        "weight_is_official": False,
        "weight_is_reconstructed": False,
        "weight_verified": False,
        "weight_confidence": 0.35,
        "weight_valid_until": (now + timedelta(hours=1)).isoformat(),
        "holdings": holdings,
        "validation": validate_weight_set(holdings),
        "proxy_for": "QQQ holdings / Nasdaq-100 weights",
    }


def _candidate_result(
    candidate: dict[str, Any],
    diagnostics: dict[str, Any],
    provider_type: ProviderType,
    reliability: float,
) -> ProviderResult:
    method = str(candidate.get("weight_method") or "")
    is_proxy = method in {RECONSTRUCTED_MARKET_CAP_WEIGHT, EQUAL_WEIGHT_PROXY}
    official_qqq = method == OFFICIAL_QQQ_WEIGHT
    holding_models = [QQQHolding.model_validate(item) for item in candidate.get("holdings") or []]
    validation = candidate.get("validation") or {}
    final_status = "proxy" if method == EQUAL_WEIGHT_PROXY else "found"
    quality = _quality(
        holding_models,
        diagnostics,
        fallback_used=is_proxy,
        final_source=candidate.get("source"),
        final_status=final_status,
        is_proxy=is_proxy,
        proxy_for=candidate.get("proxy_for"),
        official_etf_holdings=official_qqq,
        weight_data_available=bool(validation.get("non_null_weight_count")),
        weight_method=method,
        validation=validation,
    )
    quality["next_weight_refresh_at"] = candidate.get("weight_valid_until")
    weight_as_of = _parse_as_of(candidate.get("as_of"))
    quality["weight_age_hours"] = (
        round((datetime.now(UTC) - weight_as_of).total_seconds() / 3600.0, 3)
        if weight_as_of
        else None
    )
    quality["stale_weight_count"] = len(holding_models) if quality.get("stale") else 0
    quality["weight_quality_score"] = round(
        reliability * (1.0 if candidate.get("weight_verified") else 0.75), 3
    )
    quality["alternative_sources"] = quality.get("fallback_chain") or []
    multi_class_quality = candidate.get("multi_class_quality") or {}
    for field in (
        "multi_class_issuer_count",
        "multi_class_security_count",
        "verified_security_cap_count",
        "issuer_level_duplicate_count",
        "issuer_level_probable_count",
        "unknown_market_cap_semantics_count",
        "multi_class_adjustment_count",
        "multi_class_weight_coverage_pct",
        "issuer_semantics_quality_score",
        "multi_class_diagnostics",
    ):
        if field in multi_class_quality:
            quality[field] = multi_class_quality[field]
    log_weight_event(
        "qqq_weight_set_validated",
        source=candidate.get("source"),
        method=method,
        constituent_count=len(holding_models),
        total_weight_pct=validation.get("total_weight_pct"),
        coverage_pct=validation.get("coverage_pct"),
    )
    return ProviderResult(
        metadata=metadata(
            source=str(candidate.get("source") or "QQQ Holdings"),
            provider_type=provider_type,
            reliability=reliability,
            data_as_of=_parse_as_of(candidate.get("as_of")),
            freshness=Freshness.RECENT,
            is_fallback=is_proxy,
            errors=[],
        ),
        data={
            **candidate,
            "is_proxy": is_proxy,
            "proxy_for": candidate.get("proxy_for"),
            "holdings_count": len(holding_models),
            "weight_data_available": bool(validation.get("non_null_weight_count")),
            "official_etf_holdings": official_qqq,
            "data_quality": quality,
        },
    )


def _validation_failure(candidate: dict[str, Any]) -> str:
    reasons = list((candidate.get("validation") or {}).get("rejection_reasons") or [])
    return reasons[0] if reasons else "partial_response"


def _result_rank(result: ProviderResult) -> int:
    data = result.data if isinstance(result.data, dict) else {}
    quality = data.get("data_quality") or {}
    base = WEIGHT_METHOD_RANK.get(str(data.get("weight_method") or ""), 0) * 100
    semantics = float(quality.get("issuer_semantics_quality_score") or 0.0)
    verified = 10 if quality.get("multi_class_adjustment_count") else 0
    return base + int(semantics * 10) + verified


def _preserve_better_cached_result(cached: ProviderResult, attempted: ProviderResult) -> ProviderResult:
    attempted_data = attempted.data if isinstance(attempted.data, dict) else {}
    attempted_quality = attempted_data.get("data_quality") or {}
    cached_data = cached.data if isinstance(cached.data, dict) else {}
    quality = cached_data.setdefault("data_quality", {})
    quality.update(
        {
            "provider_attempts": attempted_quality.get("provider_attempts") or [],
            "actual_network_calls": int(attempted_quality.get("actual_network_calls") or 0),
            "source_attempt_count": int(attempted_quality.get("source_attempt_count") or 0),
            "source_success_count": int(attempted_quality.get("source_success_count") or 0),
            "source_failure_count": int(attempted_quality.get("source_failure_count") or 0),
            "failure_breakdown": attempted_quality.get("failure_breakdown") or {},
            "last_known_good_used": True,
            "fallback_used": True,
            "final_status": "found",
            "final_source": cached_data.get("source") or cached.metadata.source,
            "warnings": [
                _aggregate_warning(
                    "valid_last_known_good_preserved",
                    f"attempted_method={attempted_data.get('weight_method')}",
                )
            ],
        }
    )
    cached.data = cached_data
    log_weight_event(
        "qqq_weight_fallback_selected",
        source=cached_data.get("source"),
        method=cached_data.get("weight_method"),
        fallback_reason="new_candidate_rank_lower_than_valid_last_known_good",
    )
    return cached


def _attempt_status(attempt: str, diagnostics: dict[str, Any]) -> str:
    if attempt == "invesco":
        return str(diagnostics.get("invesco_status") or "attempted")
    if attempt.startswith("alpha_vantage"):
        return str(diagnostics.get("alpha_vantage_status") or "attempted")
    if attempt.startswith("nasdaq_100"):
        return "reconstructed" if diagnostics.get("reconstruction_used") else "proxy"
    return "cache_hit" if "cache" in attempt else "attempted"


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
        "source_attempt_count": 0,
        "source_success_count": 0,
        "source_failure_count": 0,
        "official_source_success": False,
        "vendor_source_success": False,
        "reconstruction_used": False,
        "equal_weight_used": False,
        "failure_breakdown": {
            "official_unavailable": 0,
            "access_restricted": 0,
            "rate_limited": 0,
            "schema_changed": 0,
            "partial_response": 0,
            "invalid_total_weight": 0,
            "missing_constituents": 0,
            "stale_source": 0,
            "market_cap_missing": 0,
            "price_missing": 0,
        },
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
    weight_method: str | None = None,
    validation: dict[str, Any] | None = None,
) -> dict[str, object]:
    if isinstance(diagnostics, list):
        diagnostics = {**_diagnostics(), "errors": diagnostics}
    weights_available = all(item.weight is not None for item in holdings) if holdings else False
    if weight_data_available is None:
        weight_data_available = weights_available
    validation = validation or validate_weight_set(
        [item.model_dump(mode="json") if hasattr(item, "model_dump") else dict(item) for item in holdings]
    )
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
        "source_attempt_count": int(diagnostics.get("source_attempt_count") or 0),
        "source_success_count": int(diagnostics.get("source_success_count") or 0),
        "source_failure_count": int(diagnostics.get("source_failure_count") or 0),
        "official_source_success": bool(diagnostics.get("official_source_success")),
        "vendor_source_success": bool(diagnostics.get("vendor_source_success")),
        "reconstruction_used": bool(diagnostics.get("reconstruction_used")),
        "equal_weight_used": bool(diagnostics.get("equal_weight_used")),
        "weighted_constituent_count": int(validation.get("weighted_constituent_count") or 0),
        "missing_weight_count": int(validation.get("missing_weight_count") or 0),
        "duplicate_symbol_count": int(validation.get("duplicate_symbol_count") or 0),
        "negative_weight_count": int(validation.get("negative_weight_count") or 0),
        "zero_weight_count": int(validation.get("zero_weight_count") or 0),
        "total_weight_pct": validation.get("total_weight_pct"),
        "top_10_weight_pct": validation.get("top_10_weight_pct"),
        "largest_weight_pct": validation.get("largest_weight_pct"),
        "weight_coverage_pct": float(validation.get("coverage_pct") or 0.0),
        "official_weight_coverage_pct": float(validation.get("coverage_pct") or 0.0) if official_etf_holdings else 0.0,
        "normalization_applied": bool(validation.get("normalization_applied")),
        "weight_method": weight_method,
        "weight_freshness": "FRESH" if final_status in {"found", "reconstructed"} else "STALE" if final_status == "stale_acceptable" else "UNKNOWN",
        "last_successful_weight_refresh_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z") if holdings else None,
        "failure_breakdown": dict(diagnostics.get("failure_breakdown") or {}),
        "fallback_chain": [
            {"source": item, "status": _attempt_status(item, diagnostics)}
            for item in diagnostics.get("provider_attempts") or []
        ],
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
