import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from typing import Any

from app.models.common import ProviderResult, ProviderType
from app.models.nasdaq import (
    BreadthContributor,
    EarningsQuality,
    EarningsResponse,
    MegaCapBreadthQuality,
    MegaCapBreadthResponse,
    MegaCapSnapshotQuality,
    MegaCapSnapshotResponse,
    MegaCapStock,
    NasdaqContextResponse,
    NewsQuality,
    NewsResponse,
    QQQHolding,
    QQQHoldingsQuality,
    QQQHoldingsResponse,
    QQQHoldingsSummary,
)
from app.providers.earnings_provider import EarningsProvider
from app.providers.mega_cap_snapshot_provider import MEGA_CAP_TICKERS, MegaCapSnapshotProvider
from app.providers.news_provider import NEWS_PROVIDER_SPECS, NewsProvider
from app.providers.qqq_holdings_provider import QQQHoldingsProvider
from app.services.data_freshness_service import parse_datetime
from app.services.qqq_weight_intelligence_service import (
    EQUAL_WEIGHT_PROXY,
    RECONSTRUCTED_MARKET_CAP_WEIGHT,
    log_weight_event,
    weighted_contributions,
)


class NasdaqDataService:
    def __init__(
        self,
        qqq_holdings_provider: QQQHoldingsProvider,
        mega_cap_snapshot_provider: MegaCapSnapshotProvider,
        earnings_provider: EarningsProvider,
        news_provider: NewsProvider,
    ) -> None:
        self.qqq_holdings_provider = qqq_holdings_provider
        self.mega_cap_snapshot_provider = mega_cap_snapshot_provider
        self.earnings_provider = earnings_provider
        self.news_provider = news_provider

    async def qqq_holdings(
        self,
        *,
        run_cache: dict[str, Any] | None = None,
        force: bool = False,
    ) -> QQQHoldingsResponse:
        cache_key = f"qqq_holdings:QQQ:force={force}"
        if run_cache is not None and cache_key in run_cache:
            cached = run_cache[cache_key]
            cached.data_quality.run_cache_used = True
            cached.data_quality.run_deduplicated_calls += 1
            return cached
        try:
            result = await self.qqq_holdings_provider.fetch_safe(force=force)
        except TypeError:
            result = await self.qqq_holdings_provider.fetch_safe()
        data = result.data if isinstance(result.data, dict) else {}
        holdings = [QQQHolding.model_validate(item) for item in data.get("holdings", [])]
        quality_data = data.get("data_quality", {})
        quality_data["fallback_used"] = bool(quality_data.get("fallback_used") or result.metadata.is_fallback)
        quality_data["errors"] = _merge_errors(quality_data.get("errors", []), result.metadata.errors)
        quality_data.setdefault("warnings", [])
        quality_data["count"] = len(holdings)
        quality_data["holdings_count"] = len(holdings)
        quality_data["missing_weights"] = any(item.weight is None for item in holdings)
        quality_data.setdefault("final_data_available", bool(holdings))
        quality_data.setdefault("weights_available", all(item.weight is not None for item in holdings) if holdings else False)
        quality_data.setdefault("weight_data_available", all(item.weight is not None for item in holdings) if holdings else False)
        quality_data.setdefault("provider_calls", int(quality_data.get("actual_network_calls") or 0))
        quality_data.setdefault("cache_used", result.metadata.provider_type in {ProviderType.CACHE, ProviderType.DB})
        quality = QQQHoldingsQuality.model_validate(quality_data)
        response = QQQHoldingsResponse(
            status=data.get("status") or quality.final_status or ("found" if holdings else "not_found"),
            as_of=data.get("as_of"),
            source=data.get("source") or result.metadata.source,
            provider_type=result.metadata.provider_type,
            retrieved_at=result.metadata.retrieved_at,
            reliability=result.metadata.reliability,
            is_fallback=result.metadata.is_fallback,
            is_proxy=bool(data.get("is_proxy") or quality.is_proxy),
            proxy_for=data.get("proxy_for") or quality.proxy_for,
            holdings_count=len(holdings),
            weight_data_available=bool(data.get("weight_data_available", quality.weight_data_available)),
            official_etf_holdings=bool(data.get("official_etf_holdings", quality.official_etf_holdings)),
            weight_method=data.get("weight_method") or quality.weight_method,
            weight_source=data.get("source") or result.metadata.source,
            weight_source_url=data.get("source_url"),
            weight_as_of=data.get("as_of"),
            weight_valid_until=data.get("weight_valid_until"),
            weight_verified=bool(data.get("weight_verified")),
            weight_is_official=bool(data.get("weight_is_official")),
            weight_is_reconstructed=bool(data.get("weight_is_reconstructed")),
            weight_confidence=float(data.get("weight_confidence") or 0.0),
            holdings=holdings,
            data_quality=quality,
        )
        if run_cache is not None:
            run_cache[cache_key] = response
        return response

    async def mega_cap_snapshot(
        self,
        *,
        run_cache: dict[str, Any] | None = None,
        force: bool = False,
    ) -> MegaCapSnapshotResponse:
        cache_key = f"mega_cap_snapshot:force={force}"
        if run_cache is not None and cache_key in run_cache:
            cached = run_cache[cache_key]
            cached.data_quality.run_cache_used = True
            cached.data_quality.run_deduplicated_calls += 1
            return cached
        holdings = await self.qqq_holdings(run_cache=run_cache, force=force)
        weights = {item.symbol: item for item in holdings.holdings}
        result = await _fetch_safe_with_force(
            self.mega_cap_snapshot_provider,
            force=force,
        )
        data = result.data if isinstance(result.data, dict) else {}
        stocks = []
        for item in data.get("stocks", []):
            item = dict(item)
            holding = weights.get(item.get("symbol"))
            item["weight"] = holding.weight if holding else None
            item["weight_method"] = holding.weight_method if holding else None
            item["weight_source"] = holding.weight_source if holding else None
            stocks.append(MegaCapStock.model_validate(item))
        quality_data = data.get("data_quality", {})
        quality_data["fallback_used"] = bool(quality_data.get("fallback_used") or result.metadata.is_fallback)
        quality_data["errors"] = _merge_errors(quality_data.get("errors", []), result.metadata.errors)
        quality_data.setdefault("warnings", [])
        quality_data.setdefault("final_data_available", bool(stocks))
        quality_data.setdefault("tracked_count", len(MEGA_CAP_TICKERS))
        quality_data["resolved_count"] = len(stocks)
        _set_provider_runtime(quality_data, result.metadata.provider_type)
        quality_data.setdefault(
            "missing_prices",
            [stock.symbol for stock in stocks if stock.last_price is None],
        )
        response = MegaCapSnapshotResponse(
            retrieved_at=result.metadata.retrieved_at,
            source=result.metadata.source,
            provider_type=result.metadata.provider_type,
            reliability=result.metadata.reliability,
            stocks=stocks,
            data_quality=MegaCapSnapshotQuality.model_validate(quality_data),
        )
        if run_cache is not None:
            run_cache[cache_key] = response
        return response

    async def mega_cap_breadth(
        self,
        *,
        run_cache: dict[str, Any] | None = None,
        force: bool = False,
    ) -> MegaCapBreadthResponse:
        holdings = await self.qqq_holdings(run_cache=run_cache, force=force)
        snapshot = await self.mega_cap_snapshot(run_cache=run_cache, force=force)
        stocks = snapshot.stocks
        missing_weights = [stock.symbol for stock in stocks if stock.weight is None]
        missing_prices = [stock.symbol for stock in stocks if stock.change_pct is None]
        usable = [stock for stock in stocks if stock.change_pct is not None]
        contribution_data = weighted_contributions(
            [item.model_dump(mode="json") for item in holdings.holdings],
            [item.model_dump(mode="json") for item in stocks],
        )
        mega_symbols = {stock.symbol for stock in stocks}
        contributor_rows = [
            item for item in contribution_data["contributors"] if item["symbol"] in mega_symbols
        ]
        contributors = [BreadthContributor.model_validate(item) for item in contributor_rows]
        positive_weight = sum(
            float(item.weight_pct or item.weight)
            for item in contributors
            if item.change_pct > 0
        )
        negative_weight = sum(
            float(item.weight_pct or item.weight)
            for item in contributors
            if item.change_pct < 0
        )
        neutral_weight = sum(
            float(item.weight_pct or item.weight)
            for item in contributors
            if item.change_pct == 0
        )
        covered_weight = positive_weight + negative_weight + neutral_weight
        weighted_positive = sum(item.weighted_contribution for item in contributors if item.weighted_contribution > 0)
        weighted_negative = sum(item.weighted_contribution for item in contributors if item.weighted_contribution < 0)
        weighted_net = weighted_positive + weighted_negative

        top_positive = sorted(
            [item for item in contributors if item.weighted_contribution > 0],
            key=lambda item: item.weighted_contribution,
            reverse=True,
        )[:5]
        top_negative = sorted(
            [item for item in contributors if item.weighted_contribution < 0],
            key=lambda item: item.weighted_contribution,
        )[:5]
        changes = [stock.change_pct for stock in usable if stock.change_pct is not None]
        method = holdings.weight_method
        calculation_method = (
            "official_weighted"
            if holdings.weight_is_official
            else "vendor_weighted"
            if method == "vendor_qqq_weight"
            else "reconstructed_weighted"
            if method == RECONSTRUCTED_MARKET_CAP_WEIGHT
            else "equal_weight_proxy"
            if method == EQUAL_WEIGHT_PROXY
            else "partial_weighted"
            if contributors
            else "unavailable"
        )
        confidence = min(snapshot.reliability, holdings.weight_confidence or holdings.reliability)
        log_weight_event(
            "qqq_weight_contribution_calculated",
            method=method,
            constituent_count=len(contributors),
            coverage_pct=covered_weight,
        )
        if covered_weight < 99.0:
            log_weight_event(
                "qqq_weight_coverage_degraded",
                method=method,
                coverage_pct=covered_weight,
                constituent_count=len(contributors),
            )
        return MegaCapBreadthResponse(
            retrieved_at=datetime.now(UTC),
            tracked_count=len(stocks),
            positive_count=sum(1 for stock in usable if (stock.change_pct or 0.0) > 0),
            negative_count=sum(1 for stock in usable if (stock.change_pct or 0.0) < 0),
            neutral_count=sum(1 for stock in usable if (stock.change_pct or 0.0) == 0),
            weighted_positive_pct=positive_weight,
            weighted_negative_pct=negative_weight,
            weighted_neutral_pct=neutral_weight,
            average_change_pct=sum(changes) / len(changes) if changes else 0.0,
            weighted_average_change_pct=weighted_net,
            coverage_adjusted_weighted_change_pct=(weighted_net / covered_weight * 100.0 if covered_weight else None),
            weighted_positive_contribution=weighted_positive,
            weighted_negative_contribution=weighted_negative,
            weighted_net_contribution=weighted_net,
            covered_weight_pct=covered_weight,
            uncovered_weight_pct=max(0.0, 100.0 - covered_weight),
            calculation_method=calculation_method,
            weight_method=method,
            covered_symbols=[item.symbol for item in contributors],
            missing_price_symbols=missing_prices,
            missing_weight_symbols=missing_weights,
            confidence=confidence,
            is_proxy=calculation_method == "equal_weight_proxy",
            top_positive_contributors=top_positive,
            top_negative_contributors=top_negative,
            reliability=snapshot.reliability,
            data_quality=MegaCapBreadthQuality(
                missing_weights=missing_weights,
                missing_prices=missing_prices,
                errors=snapshot.data_quality.errors,
                covered_weight_pct=covered_weight,
                uncovered_weight_pct=max(0.0, 100.0 - covered_weight),
                price_coverage_pct=len(usable) / max(len(stocks), 1) * 100.0,
                weight_coverage_pct=len(contributors) / max(len(stocks), 1) * 100.0,
            ),
        )

    async def earnings(
        self,
        days: int = 14,
        tickers: list[str] | None = None,
        *,
        force: bool = False,
    ) -> EarningsResponse:
        result = await _fetch_safe_with_force(
            self.earnings_provider,
            force=force,
        )
        data = result.data if isinstance(result.data, dict) else {}
        ticker_set = {symbol.upper() for symbol in tickers} if tickers else None
        events = data.get("events", [])
        if ticker_set:
            events = [event for event in events if str(event.get("symbol", "")).upper() in ticker_set]
        now_date = datetime.now(UTC).date()
        end_date = now_date + timedelta(days=days)
        events = [
            event
            for event in events
            if now_date <= datetime.fromisoformat(str(event["date"])).date() <= end_date
        ]
        quality_data = data.get("data_quality", {})
        quality_data["fallback_used"] = bool(quality_data.get("fallback_used") or result.metadata.is_fallback)
        quality_data["errors"] = _merge_errors(quality_data.get("errors", []), result.metadata.errors)
        quality_data.setdefault("warnings", [])
        quality_data.setdefault("final_data_available", True)
        _set_provider_runtime(quality_data, result.metadata.provider_type)
        return EarningsResponse(
            retrieved_at=result.metadata.retrieved_at,
            days=days,
            events=events,
            data_quality=EarningsQuality.model_validate(quality_data),
        )

    async def latest_news(
        self,
        symbols: list[str],
        limit: int = 20,
        recency_days: int = 14,
        *,
        force: bool = False,
    ) -> NewsResponse:
        fetch_for_symbols = getattr(
            self.news_provider,
            "fetch_for_symbols",
            None,
        )
        if not callable(fetch_for_symbols):
            result = await _fetch_safe_with_force(
                self.news_provider,
                force=force,
            )
        else:
            try:
                result = await fetch_for_symbols(
                    symbols=symbols,
                    limit=limit,
                    recency_days=recency_days,
                )
                cache_deadline = (
                    datetime.now(UTC) + timedelta(minutes=5)
                )
                self.news_provider.cache.set(
                    self.news_provider.cache_key,
                    result.model_dump(mode="json"),
                    provider_name=self.news_provider.source,
                    valid_until=cache_deadline.isoformat(),
                    stale_until=cache_deadline.isoformat(),
                    status="valid_cache",
                )
            except Exception as exc:
                result = (
                    None
                    if force
                    else _request_scoped_news_cache_result(
                        self.news_provider,
                        error=exc,
                    )
                )
                if result is None:
                    return _empty_news(
                        "latest_news_request_evidence_incomplete:"
                        f"{type(exc).__name__}"
                    )
        data = result.data if isinstance(result.data, dict) else {}
        quality_data = data.get("data_quality", {})
        quality_data["fallback_used"] = bool(quality_data.get("fallback_used") or result.metadata.is_fallback)
        quality_data["errors"] = _merge_errors(quality_data.get("errors", []), result.metadata.errors)
        quality_data.setdefault("warnings", [])
        quality_data.setdefault("final_data_available", bool(data.get("articles", [])))
        _set_provider_runtime(quality_data, result.metadata.provider_type)
        return NewsResponse(
            retrieved_at=result.metadata.retrieved_at,
            # ``limit`` is a per-provider acquisition bound. Applying it again
            # here would silently drop valid fan-in records from later sources.
            articles=data.get("articles", []),
            data_quality=NewsQuality.model_validate(quality_data),
        )

    async def context(
        self,
        *,
        force: bool = False,
        fetch_news: bool = True,
        fetch_holdings: bool = True,
        fetch_mega_cap: bool = True,
        fetch_earnings: bool = True,
        preloaded_holdings: dict[str, Any] | None = None,
    ) -> NasdaqContextResponse:
        critical_errors: list[str] = []
        warnings: list[str] = []
        fallback_notes: list[str] = []
        fallback_used = False
        run_cache: dict[str, Any] = {}
        holdings = (
            _canonical_holdings_response(preloaded_holdings)
            if preloaded_holdings
            else await _timed_section(
                "qqq_holdings",
                self.qqq_holdings(
                    run_cache=run_cache,
                    force=force,
                ),
                timeout=_provider_timeout(
                    self.qqq_holdings_provider,
                    "timeout_nasdaq_seconds",
                    45.0,
                ),
                fallback=lambda error: _empty_holdings(error),
                warnings=warnings,
            )
            if fetch_holdings
            else _empty_holdings(
                "canonical_holdings_not_required_for_this_batch"
            )
        )
        if preloaded_holdings:
            run_cache[
                f"qqq_holdings:QQQ:force={force}"
            ] = holdings
        snapshot = (
            await _timed_section(
                "mega_cap_snapshot",
                self.mega_cap_snapshot(
                    run_cache=run_cache,
                    force=force,
                ),
                timeout=_provider_timeout(
                    self.mega_cap_snapshot_provider,
                    "timeout_nasdaq_seconds",
                    45.0,
                ),
                fallback=lambda error: _empty_snapshot(error),
                warnings=warnings,
            )
            if fetch_mega_cap
            else _empty_snapshot(
                "canonical_mega_cap_snapshot_selected"
            )
        )
        breadth = (
            await _timed_section(
                "mega_cap_breadth",
                self.mega_cap_breadth(
                    run_cache=run_cache,
                    force=force,
                ),
                timeout=_provider_timeout(
                    self.mega_cap_snapshot_provider,
                    "timeout_nasdaq_seconds",
                    45.0,
                ),
                fallback=lambda error: _empty_breadth(error),
                warnings=warnings,
            )
            if fetch_mega_cap
            else _empty_breadth(
                "canonical_mega_cap_breadth_selected"
            )
        )
        earnings = (
            await _timed_section(
                "upcoming_earnings",
                self.earnings(days=14, force=force),
                timeout=_provider_timeout(
                    self.earnings_provider,
                    "timeout_earnings_seconds",
                    12.0,
                ),
                fallback=lambda error: _empty_earnings(
                    error,
                    days=14,
                ),
                warnings=warnings,
            )
            if fetch_earnings
            else _empty_earnings(
                "dedicated_earnings_runtime_owns_acquisition",
                days=14,
            )
        )
        news = (
            await _timed_section(
                "latest_news",
                self.latest_news(
                    symbols=["NVDA", "AAPL", "MSFT", "QQQ"],
                    limit=20,
                    recency_days=14,
                    force=force,
                ),
                timeout=_provider_timeout(
                    self.news_provider,
                    "timeout_news_seconds",
                    12.0,
                )
                + 1.0,
                fallback=lambda error: _empty_news(error),
                warnings=warnings,
            )
            if fetch_news
            else _empty_news(
                "valid_canonical_news_selected_before_nasdaq_batch"
            )
        )
        for label, quality in [
            ("qqq_holdings", holdings.data_quality),
            ("mega_cap_snapshot", snapshot.data_quality),
            ("mega_cap_breadth", breadth.data_quality),
            ("upcoming_earnings", earnings.data_quality),
            ("latest_news", news.data_quality),
        ]:
            fallback_used = fallback_used or bool(getattr(quality, "fallback_used", False))
            _classify_quality_messages(
                label=label,
                quality=quality,
                critical_errors=critical_errors,
                warnings=warnings,
                fallback_notes=fallback_notes,
            )
        runtime_qualities = (holdings.data_quality, snapshot.data_quality, earnings.data_quality, news.data_quality)
        provider_calls = sum(int(quality.provider_calls) for quality in runtime_qualities)
        actual_network_calls = sum(int(quality.actual_network_calls) for quality in runtime_qualities)
        run_deduplicated_calls = int(holdings.data_quality.run_deduplicated_calls) + int(snapshot.data_quality.run_deduplicated_calls)
        return NasdaqContextResponse(
            generated_at=datetime.now(UTC),
            qqq_holdings=holdings,
            qqq_holdings_summary=QQQHoldingsSummary(
                as_of=holdings.as_of,
                top_holdings=holdings.holdings[:10],
                source=holdings.source,
                reliability=holdings.reliability,
            ),
            mega_cap_snapshot=snapshot,
            mega_cap_breadth=breadth,
            upcoming_earnings=earnings,
            latest_news=news,
            metadata={
                "service_role": "data provider only",
                "trading_logic": "not implemented; data service only",
                "decisions_delegated_to": "AI-TRADER",
                "critical_errors": _merge_errors(critical_errors),
                "warnings": _merge_errors(warnings),
                "fallback_notes": _merge_errors(fallback_notes),
                "fallback_used": fallback_used,
                "provider_calls": provider_calls,
                "actual_network_calls": actual_network_calls,
                "run_deduplicated_calls": run_deduplicated_calls,
            },
        )


def _merge_errors(*groups) -> list[str]:
    merged: list[str] = []
    for group in groups:
        for error in group or []:
            if error and error not in merged:
                merged.append(error)
    return merged


def _set_provider_runtime(quality: dict[str, Any], provider_type: ProviderType) -> None:
    cache_used = provider_type in {ProviderType.CACHE, ProviderType.DB}
    quality.setdefault("provider_calls", 0 if cache_used else 1)
    quality.setdefault("actual_network_calls", 0 if cache_used else 1)
    quality.setdefault("cache_used", cache_used)


async def _fetch_safe_with_force(
    provider: Any,
    *,
    force: bool,
) -> Any:
    fetch_call = provider.fetch_safe
    parameters = inspect.signature(fetch_call).parameters
    supports_force = bool(
        "force" in parameters
        or any(
            parameter.kind
            == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
    )
    return await fetch_call(
        **({"force": force} if supports_force else {})
    )


def _request_scoped_news_cache_result(
    provider: NewsProvider,
    *,
    error: Exception,
) -> ProviderResult | None:
    get_entry = getattr(provider.cache, "get_entry", None)
    if not callable(get_entry):
        return None
    entry = get_entry(provider.cache_key)
    now = datetime.now(UTC)
    valid_until = parse_datetime(
        entry.get("valid_until") if entry else None
    )
    if (
        not entry
        or entry.get("status") != "valid_cache"
        or valid_until is None
        or valid_until <= now
    ):
        return None
    try:
        result = ProviderResult.model_validate(entry["payload"])
    except (KeyError, TypeError, ValueError):
        return None
    if not isinstance(result.data, dict):
        return None
    quality = (
        dict(result.data.get("data_quality") or {})
        if isinstance(result.data.get("data_quality"), dict)
        else {}
    )
    cached_accounts = [
        item
        for item in (
            quality.get("provider_accounting")
            or result.data.get("provider_accounting")
            or []
        )
        if isinstance(item, dict)
    ]
    by_provider = {
        str(item.get("provider") or ""): item
        for item in cached_accounts
        if item.get("provider")
    }
    expected = [str(spec[0]) for spec in NEWS_PROVIDER_SPECS]
    if (
        len(cached_accounts) != len(expected)
        or len(by_provider) != len(expected)
        or set(by_provider) != set(expected)
    ):
        return None
    cache_accounts = [
        {
            "provider": provider_name,
            "status": "CACHE_HIT",
            "calls": 0,
            "reason_code": "CURRENT_REQUEST_TECHNICAL_CACHE_SELECTED",
            "execution_origin": "CACHE_DECISION",
            "cache_key": provider.cache_key,
            "cached_provider_result": str(
                by_provider[provider_name].get("status") or "UNKNOWN"
            ),
        }
        for provider_name in expected
    ]
    articles = list(result.data.get("articles") or [])
    quality.update(
        {
            "provider_accounting": cache_accounts,
            "provider_accounting_valid": True,
            "provider_calls": 0,
            "actual_network_calls": 0,
            "cache_used": True,
            "fallback_used": True,
            "final_data_available": bool(articles),
            "no_data_found": not articles,
            "warnings": _merge_errors(
                quality.get("warnings") or [],
                [
                    "request_scoped_news_technical_cache_selected:"
                    f"{type(error).__name__}"
                ],
            ),
        }
    )
    result.data["provider_accounting"] = cache_accounts
    result.data["data_quality"] = quality
    result.metadata.provider_type = ProviderType.CACHE
    result.metadata.is_fallback = True
    return result


def _provider_timeout(provider, name: str, default: float) -> float:
    return float(getattr(getattr(provider, "settings", None), name, default))


def _classify_quality_messages(
    label: str,
    quality,
    critical_errors: list[str],
    warnings: list[str],
    fallback_notes: list[str],
) -> None:
    errors = list(getattr(quality, "errors", []) or [])
    quality_warnings = list(getattr(quality, "warnings", []) or [])
    final_data_available = bool(getattr(quality, "final_data_available", False))
    fallback_used = bool(getattr(quality, "fallback_used", False))
    no_data_found = bool(getattr(quality, "no_data_found", False))
    provider_failed = bool(getattr(quality, "provider_failed", False))
    rate_limited = bool(getattr(quality, "rate_limited", False))

    for warning in quality_warnings:
        target = fallback_notes if fallback_used else warnings
        target.append(f"{label}: {warning}")

    if final_data_available and not provider_failed and not rate_limited:
        for error in errors:
            if no_data_found:
                warnings.append(f"{label}: {error}")
            elif fallback_used:
                fallback_notes.append(f"{label}: {error}")
            else:
                warnings.append(f"{label}: {error}")
        return

    if final_data_available:
        for error in errors:
            fallback_notes.append(f"{label}: {error}")
        return

    for error in errors:
        critical_errors.append(f"{label}: {error}")


async def _timed_section(label: str, awaitable, *, timeout: float, fallback, warnings: list[str]):
    try:
        return await asyncio.wait_for(awaitable, timeout=max(float(timeout), 1.0))
    except TimeoutError:
        message = f"{label}: provider_timeout after {timeout}s"
        warnings.append(message)
        return fallback(message)
    except Exception as exc:
        message = f"{label}: provider_failed: {exc or type(exc).__name__}"
        warnings.append(message)
        return fallback(message)


def _canonical_holdings_response(
    payload: dict[str, Any],
) -> QQQHoldingsResponse:
    holdings = [
        QQQHolding.model_validate(item)
        for item in payload.get("holdings") or []
        if isinstance(item, dict)
    ]
    quality_payload = {
        **(
            payload.get("data_quality")
            if isinstance(payload.get("data_quality"), dict)
            else {}
        ),
        "count": len(holdings),
        "holdings_count": len(holdings),
        "final_data_available": bool(holdings),
        "no_data_found": not holdings,
        "provider_calls": 0,
        "actual_network_calls": 0,
        "cache_used": True,
        "provider_attempts": [],
        "provider_accounting": [],
    }
    source = str(
        payload.get("source")
        or payload.get("weight_source")
        or "CANONICAL_DATABASE"
    )
    retrieved_at = (
        payload.get("retrieved_at")
        or payload.get("data_as_of")
        or datetime.now(UTC)
    )
    return QQQHoldingsResponse(
        status=str(
            payload.get("status")
            or ("found" if holdings else "not_found")
        ),
        as_of=payload.get("as_of")
        or payload.get("weight_as_of"),
        source=source,
        provider_type=ProviderType.DB,
        retrieved_at=retrieved_at,
        reliability=float(payload.get("reliability") or 0.9),
        is_fallback=bool(payload.get("is_fallback")),
        is_proxy=bool(payload.get("is_proxy")),
        proxy_for=payload.get("proxy_for"),
        holdings_count=int(
            payload.get("holdings_count") or len(holdings)
        ),
        weight_data_available=bool(
            payload.get(
                "weight_data_available",
                all(item.weight is not None for item in holdings),
            )
        ),
        official_etf_holdings=bool(
            payload.get("official_etf_holdings", True)
        ),
        weight_method=payload.get("weight_method"),
        weight_source=payload.get("weight_source") or source,
        weight_source_url=payload.get("weight_source_url"),
        weight_as_of=payload.get("weight_as_of")
        or payload.get("as_of"),
        weight_valid_until=payload.get("weight_valid_until")
        or payload.get("content_valid_until"),
        weight_verified=bool(payload.get("weight_verified")),
        weight_is_official=bool(
            payload.get("weight_is_official")
        ),
        weight_is_reconstructed=bool(
            payload.get("weight_is_reconstructed")
        ),
        weight_confidence=float(
            payload.get("weight_confidence") or 0.0
        ),
        holdings=holdings,
        data_quality=QQQHoldingsQuality.model_validate(
            quality_payload
        ),
    )


def _empty_holdings(error: str) -> QQQHoldingsResponse:
    return QQQHoldingsResponse(
        status="not_found",
        as_of=None,
        source="provider_timeout",
        provider_type=ProviderType.API,
        retrieved_at=datetime.now(UTC),
        reliability=0.0,
        is_fallback=True,
        holdings_count=0,
        weight_data_available=False,
        official_etf_holdings=False,
        holdings=[],
        data_quality=QQQHoldingsQuality(
            count=0,
            holdings_count=0,
            errors=[],
            warnings=[error],
            fallback_used=True,
            final_data_available=False,
            no_data_found=True,
            provider_failed="provider_failed" in error,
            rate_limited=False,
        ),
    )


def _empty_snapshot(error: str) -> MegaCapSnapshotResponse:
    return MegaCapSnapshotResponse(
        retrieved_at=datetime.now(UTC),
        source="provider_timeout",
        provider_type=ProviderType.API,
        reliability=0.0,
        stocks=[],
        data_quality=MegaCapSnapshotQuality(
            tracked_count=len(MEGA_CAP_TICKERS),
            resolved_count=0,
            missing_prices=list(MEGA_CAP_TICKERS),
            errors=[],
            warnings=[error],
            fallback_used=True,
            final_data_available=False,
            no_data_found=True,
            provider_failed="provider_failed" in error,
            rate_limited=False,
        ),
    )


def _empty_breadth(error: str) -> MegaCapBreadthResponse:
    return MegaCapBreadthResponse(
        retrieved_at=datetime.now(UTC),
        tracked_count=0,
        positive_count=0,
        negative_count=0,
        neutral_count=0,
        weighted_positive_pct=0.0,
        weighted_negative_pct=0.0,
        weighted_neutral_pct=0.0,
        average_change_pct=0.0,
        weighted_average_change_pct=0.0,
        reliability=0.0,
        data_quality=MegaCapBreadthQuality(errors=[error]),
    )


def _empty_earnings(error: str, *, days: int) -> EarningsResponse:
    return EarningsResponse(
        retrieved_at=datetime.now(UTC),
        days=days,
        events=[],
        data_quality=EarningsQuality(
            errors=[],
            warnings=[error],
            fallback_used=True,
            final_data_available=True,
            no_data_found=True,
            provider_failed=False,
            rate_limited=False,
        ),
    )


def _empty_news(error: str) -> NewsResponse:
    return NewsResponse(
        retrieved_at=datetime.now(UTC),
        articles=[],
        data_quality=NewsQuality(
            errors=[],
            warnings=[error],
            fallback_used=True,
            final_data_available=False,
            no_data_found=True,
            provider_failed=False,
            rate_limited=False,
        ),
    )
