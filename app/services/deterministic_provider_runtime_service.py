from __future__ import annotations

import asyncio
import copy
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable, Mapping

from app.core.config import Settings
from app.models.common import ProviderType
from app.providers.parametric_cache import ParametricProviderCache
from app.services.deterministic_market_context_service import (
    compute_cross_asset_context,
    compute_market_internals,
    compute_options_positioning,
)
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
    provider_attempt,
)


class DeterministicProviderRuntimeService:
    """Materialize provider-first domains before snapshot/consumer projection."""

    def __init__(
        self,
        settings: Settings,
        *,
        providers: Mapping[str, Any],
        cache,
        clock=lambda: datetime.now(UTC),
    ) -> None:
        self.settings = settings
        self.providers = dict(providers)
        self.cache = ParametricProviderCache(cache, clock=clock)
        self.clock = clock
        self.last_run: dict[str, Any] = {}

    async def enrich_market_context(
        self,
        contract: dict[str, Any],
        *,
        refresh: str,
        trigger_type: str | None = None,
        accounting_collector: (
            RequestProviderAccountingCollector | None
        ) = None,
    ) -> dict[str, Any]:
        if refresh == "false":
            return contract
        output = dict(contract)
        telemetry = {
            "actual_provider_requests": 0,
            "cache_hits": 0,
            "negative_cache_hits": 0,
            "stale_grace_hits": 0,
            "ai_invocations": 0,
            "warnings": [],
        }

        official = await self._official_context(output, telemetry)
        fred_series = official["fred"]
        output["rates_context"] = _rates_context(fred_series, self.clock())
        output["macro_actuals"] = await self._macro_actuals(
            output,
            official=official,
            telemetry=telemetry,
        )

        tradier = self.providers.get("tradier")
        holdings = _holdings(output)
        constituent_symbols = [str(item.get("symbol") or "") for item in holdings]
        cross_symbols = _csv(self.settings.tradier_cross_asset_symbols)
        quote_symbols = list(
            dict.fromkeys([*cross_symbols, *constituent_symbols, "QQQ"])
        )
        quotes: list[dict[str, Any]] = []
        if (
            tradier is not None
            and self.settings.tradier_enabled
            and self.settings.tradier_market_data_enabled
        ):
            try:
                quotes = await tradier.quotes(quote_symbols)
            except Exception as exc:
                telemetry["warnings"].append(f"tradier_quotes_partial:{type(exc).__name__}")

        output["options_positioning"] = await self._options(
            tradier,
            quotes,
            telemetry,
        )
        if tradier is not None:
            _merge_tradier_telemetry(telemetry, tradier)
        output["market_internals"] = self._internals(
            holdings,
            quotes,
            telemetry,
        )
        self._record_tradier_accounting(
            accounting_collector,
            tradier=tradier,
            market_internals=output["market_internals"],
            options_positioning=output["options_positioning"],
        )
        output["cross_asset_context"] = self._cross_asset(
            quotes,
            fred_series,
            telemetry,
        )
        output["earnings_intelligence"] = await self._earnings(
            output,
            telemetry,
        )
        output["current_company_news"] = _verified_current_news(output)

        domains = {}
        for name in (
            "macro_actuals",
            "rates_context",
            "options_positioning",
            "market_internals",
            "cross_asset_context",
            "earnings_intelligence",
            "current_company_news",
        ):
            section = output.get(name) if isinstance(output.get(name), dict) else {}
            status = str(section.get("status") or "NO_DATA").upper()
            domains[name] = {
                "execution_status": section.get("execution_status") or "SUCCEEDED",
                "data_coverage_status": section.get("data_coverage_status")
                or ("COMPLETE" if status == "AVAILABLE" else status),
                "provider": section.get("provider"),
                "trigger_class": section.get("trigger_class"),
                "coverage": section.get("coverage")
                if section.get("coverage") is not None
                else (1.0 if status == "AVAILABLE" else 0.0),
                "warnings": list(section.get("warnings") or []),
            }
        output["deterministic_domains"] = {
            "status": (
                "AVAILABLE"
                if any(value["data_coverage_status"] == "COMPLETE" for value in domains.values())
                else "PARTIAL"
            ),
            "execution_status": "SUCCEEDED",
            "data_coverage_status": (
                "COMPLETE"
                if all(value["data_coverage_status"] == "COMPLETE" for value in domains.values())
                else "PARTIAL"
            ),
            "refresh_mode": refresh,
            "trigger_type": trigger_type,
            "domains": domains,
            "telemetry": telemetry,
            "provider_first_order": [
                "valid_cache",
                "deterministic_provider",
                "last_known_good_or_stale_grace",
                "qualitative_residual_ai_only",
                "NO_DATA",
            ],
            "numeric_gaps_sent_to_ai": 0,
        }
        self.last_run = telemetry
        return output

    def enrich_market_context_sync(
        self,
        contract: dict[str, Any],
        *,
        refresh: str,
        trigger_type: str | None = None,
        accounting_collector: (
            RequestProviderAccountingCollector | None
        ) = None,
    ) -> dict[str, Any]:
        return asyncio.run(
            self.enrich_market_context(
                contract,
                refresh=refresh,
                trigger_type=trigger_type,
                accounting_collector=accounting_collector,
            )
        )

    def _record_tradier_accounting(
        self,
        collector: RequestProviderAccountingCollector | None,
        *,
        tradier: Any,
        market_internals: dict[str, Any],
        options_positioning: dict[str, Any],
    ) -> None:
        if collector is None:
            return
        details = getattr(tradier, "last_telemetry", {})
        details = details if isinstance(details, dict) else {}
        quote_details = [
            value
            for key, value in details.items()
            if key == "quotes" and isinstance(value, dict)
        ]
        option_details = [
            value
            for key, value in details.items()
            if (
                key.startswith("option_")
                and isinstance(value, dict)
            )
        ]
        self._record_tradier_dataset(
            collector,
            dataset_id="market_internals",
            acquisition_id="tradier_quotes_for_market_internals",
            endpoint_details=quote_details,
            section=market_internals,
            enabled=bool(
                tradier is not None
                and self.settings.tradier_enabled
                and self.settings.tradier_market_data_enabled
                and self.settings.deterministic_market_internals_enabled
            ),
        )
        self._record_tradier_dataset(
            collector,
            dataset_id="options_positioning",
            acquisition_id="tradier_option_chain_for_positioning",
            endpoint_details=[*quote_details, *option_details],
            section=options_positioning,
            enabled=bool(
                tradier is not None
                and self.settings.tradier_enabled
                and self.settings.tradier_market_data_enabled
                and self.settings.deterministic_options_positioning_enabled
            ),
        )

    def _record_tradier_dataset(
        self,
        collector: RequestProviderAccountingCollector,
        *,
        dataset_id: str,
        acquisition_id: str,
        endpoint_details: list[dict[str, Any]],
        section: dict[str, Any],
        enabled: bool,
    ) -> None:
        calls = sum(
            int(item.get("actual_provider_requests") or 0)
            for item in endpoint_details
        )
        cache_hits = sum(
            int(item.get("cache_hit") or 0)
            for item in endpoint_details
        )
        if calls:
            attempt = provider_attempt(
                "TRADIER",
                called=True,
                attempts=calls,
                result=str(section.get("status") or "NO_DATA").upper(),
                execution_origin="PROVIDER_CALL",
            )
            complete = True
        elif cache_hits:
            attempt = provider_attempt(
                "TRADIER",
                called=False,
                attempts=0,
                result="CACHE_HIT",
                not_called_reason="PROVIDER_ADAPTER_CACHE_HIT",
                execution_origin="CACHE_DECISION",
            )
            complete = True
        else:
            warning = next(
                iter(section.get("warnings") or []),
                "PROVIDER_DISABLED_OR_PREREQUISITE_MISSING",
            )
            attempt = provider_attempt(
                "TRADIER",
                called=False,
                attempts=0,
                result="NOT_CALLED",
                not_called_reason=str(warning).upper(),
                execution_origin="OBSERVED_SKIP",
            )
            complete = not enabled or bool(section.get("warnings"))
        collector.record(
            dataset_id,
            acquisition_id=acquisition_id,
            shared_dataset_ids=(dataset_id,),
            database_lookup_performed=False,
            database_lookup_reason="DETERMINISTIC_PROVIDER_ONLY_STAGE",
            database_record_found=None,
            database_data_as_of=None,
            database_content_valid_until=None,
            database_record_expired=None,
            database_freshness_evaluation="NOT_LOOKED_UP",
            primary_provider=attempt,
            fallbacks=[],
            acquisition_selected_source=(
                section.get("provider")
                if section.get("status") == "AVAILABLE"
                else None
            ),
            acquisition_reason_code=(
                "TRADIER_VALUE_ACQUIRED"
                if section.get("status") == "AVAILABLE"
                else "TRADIER_VALUE_NOT_AVAILABLE"
            ),
            evidence_complete=complete,
        )

    async def _official_context(
        self,
        contract: dict[str, Any],
        telemetry: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        existing = _macro_series(contract)
        output: dict[str, dict[str, Any]] = {"fred": {}, "bls": {}, "bea": {}}
        for provider_name in output:
            selected = {
                key: value
                for key, value in existing.items()
                if str(value.get("source") or "").upper().startswith(provider_name.upper())
            }
            if selected:
                output[provider_name] = selected
                telemetry["cache_hits"] += 1
                continue
            provider = self.providers.get(provider_name)
            if provider is None:
                continue
            try:
                result = await provider.fetch_safe()
            except Exception as exc:
                telemetry["warnings"].append(
                    f"{provider_name}_provider_partial:{type(exc).__name__}"
                )
                continue
            output[provider_name] = dict(result.data or {})
            if result.metadata.provider_type == ProviderType.CACHE:
                telemetry["cache_hits"] += 1
            elif output[provider_name]:
                telemetry["actual_provider_requests"] += 1
        return output

    async def _macro_actuals(
        self,
        contract: dict[str, Any],
        *,
        official: dict[str, dict[str, Any]],
        telemetry: dict[str, Any],
    ) -> dict[str, Any]:
        items = _released_events(contract)
        for provider_name in ("bls", "bea"):
            for series_id, row in official[provider_name].items():
                if not isinstance(row, dict) or row.get("value") is None:
                    continue
                items.append(
                    {
                        "occurrence_id": f"{provider_name.upper()}:{series_id}:{row.get('data_as_of')}",
                        "series_id": series_id,
                        "actual": row.get("value"),
                        "unit": row.get("units"),
                        "reference_period": row.get("data_as_of"),
                        "provider": provider_name.upper(),
                        "freshness": row.get("freshness") or "CURRENT",
                        "lineage": {
                            "source_url": row.get("source_url"),
                            "official_adapter": row.get("official_adapter", True),
                        },
                    }
                )
        census_items = await self._census_due(contract, telemetry)
        items.extend(census_items)
        return _section(
            items,
            provider="BLS/BEA/CENSUS",
            trigger_class="TRIGGER",
            warnings=[] if items else ["no_published_macro_actuals"],
        )

    async def _census_due(
        self,
        contract: dict[str, Any],
        telemetry: dict[str, Any],
    ) -> list[dict[str, Any]]:
        provider = self.providers.get("census")
        if provider is None or not self.settings.census_enabled:
            return []
        requests: set[tuple[str, str]] = set()
        for event in _all_events(contract):
            provider_id = str(
                event.get("official_provider")
                or event.get("provider")
                or event.get("source")
                or ""
            ).upper()
            period = event.get("reference_period")
            dataset = event.get("dataset")
            if "CENSUS" in provider_id and period and dataset:
                requests.add((str(dataset).upper(), str(period)))
        output: list[dict[str, Any]] = []
        for dataset, period in sorted(requests):
            resolution = await self.cache.resolve(
                provider="CENSUS",
                endpoint="economic_indicators",
                environment=self.settings.environment,
                parameters={"dataset": dataset, "reference_period": period},
                ttl_seconds=self.settings.census_cache_ttl_seconds,
                loader=lambda d=dataset, p=period: self._fetch_census(
                    provider,
                    dataset=d,
                    period=p,
                ),
            )
            _merge_cache_telemetry(telemetry, resolution)
            data = dict(resolution.value or {})
            for series_id, row in data.items():
                output.append(
                    {
                        "occurrence_id": row.get("release_occurrence")
                        or f"CENSUS:{dataset}:{period}",
                        "series_id": series_id,
                        "actual": row.get("value"),
                        "unit": row.get("units"),
                        "reference_period": period,
                        "provider": "CENSUS",
                        "freshness": "CURRENT",
                        "lineage": row.get("lineage") or {},
                    }
                )
        return output

    @staticmethod
    async def _fetch_census(provider, *, dataset: str, period: str) -> dict[str, Any]:
        result = await provider.fetch(period=period, datasets=[dataset])
        return dict(result.data or {})

    async def _options(
        self,
        tradier,
        quotes: list[dict[str, Any]],
        telemetry: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.settings.deterministic_options_positioning_enabled:
            return _disabled("TRADIER", "REFRESH_ON_TRIGGER")
        quote = next((item for item in quotes if item.get("symbol") == "QQQ"), {})
        if tradier is None or not quote:
            return _no_data("TRADIER", "REFRESH_ON_TRIGGER", "qqq_quote_unavailable")
        try:
            chain_set = await tradier.relevant_option_chains("QQQ", max_expirations=3)
            result = compute_options_positioning(
                quote=quote,
                chains=chain_set.get("chains") or {},
                retrieved_at=self.clock(),
            )
            return _complete(result)
        except Exception as exc:
            telemetry["warnings"].append(f"tradier_options_partial:{type(exc).__name__}")
            return _no_data("TRADIER", "REFRESH_ON_TRIGGER", type(exc).__name__)

    def _internals(
        self,
        holdings: list[dict[str, Any]],
        quotes: list[dict[str, Any]],
        telemetry: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.settings.deterministic_market_internals_enabled:
            return _disabled("TRADIER", "REFRESH_ON_TRIGGER")
        if not holdings or not quotes:
            return _no_data(
                "TRADIER",
                "REFRESH_ON_TRIGGER",
                "constituents_or_quotes_unavailable",
            )
        return _complete(
            compute_market_internals(
                constituents=holdings,
                holdings=holdings,
                quotes=quotes,
            )
        )

    def _cross_asset(
        self,
        quotes: list[dict[str, Any]],
        fred_series: dict[str, Any],
        telemetry: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.settings.deterministic_cross_asset_context_enabled:
            return _disabled("TRADIER+FRED", "REFRESH_ON_TRIGGER")
        if not quotes:
            return _no_data(
                "TRADIER+FRED",
                "REFRESH_ON_TRIGGER",
                "cross_asset_quotes_unavailable",
            )
        configured = set(_csv(self.settings.tradier_cross_asset_symbols))
        cross_quotes = [
            item for item in quotes if str(item.get("symbol") or "") in configured
        ]
        return _complete(
            compute_cross_asset_context(
                quotes=cross_quotes,
                fred_series=fred_series,
            )
        )

    async def _earnings(
        self,
        contract: dict[str, Any],
        telemetry: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.settings.deterministic_earnings_intelligence_enabled:
            return _disabled("FINNHUB", "TRIGGER")
        provider = self.providers.get("finnhub")
        if (
            provider is None
            or not self.settings.finnhub_enabled
            or not self.settings.finnhub_api_key
        ):
            return _no_data("FINNHUB", "TRIGGER", "finnhub_not_configured")
        start = self.clock().date()
        end = start + timedelta(days=14)
        try:
            resolution = await self.cache.resolve(
                provider="FINNHUB",
                endpoint="earnings_calendar",
                environment=self.settings.environment,
                parameters={
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                },
                ttl_seconds=self.settings.finnhub_cache_ttl_seconds,
                loader=lambda: provider.earnings_calendar(start=start, end=end),
            )
            _merge_cache_telemetry(telemetry, resolution)
            earnings = list(resolution.value or [])
        except Exception as exc:
            telemetry["warnings"].append(f"finnhub_earnings_partial:{type(exc).__name__}")
            return _no_data("FINNHUB", "TRIGGER", type(exc).__name__)

        candidates: list[dict[str, Any]] = []
        symbols = list(
            dict.fromkeys(
                str(item.get("symbol") or "")
                for item in earnings
                if item.get("symbol")
            )
        )
        news_start = start - timedelta(days=2)
        for symbol in symbols:
            try:
                resolution = await self.cache.resolve(
                    provider="FINNHUB",
                    endpoint="company_news_discovery",
                    environment=self.settings.environment,
                    parameters={
                        "symbol": symbol,
                        "start": news_start.isoformat(),
                        "end": start.isoformat(),
                    },
                    ttl_seconds=self.settings.finnhub_cache_ttl_seconds,
                    loader=lambda s=symbol: provider.company_news(
                        s,
                        start=news_start,
                        end=start,
                    ),
                )
                _merge_cache_telemetry(telemetry, resolution)
                candidates.extend(list(resolution.value or []))
            except Exception as exc:
                telemetry["warnings"].append(
                    f"finnhub_news_candidate_partial:{symbol}:{type(exc).__name__}"
                )
        return {
            **_section(
                earnings,
                provider="FINNHUB",
                trigger_class="TRIGGER",
                warnings=[] if earnings else ["no_earnings_in_window"],
            ),
            "candidate_news_count": len(candidates),
            "company_news_candidates": candidates,
            "candidate_news_policy": (
                "DISCOVERY_ONLY; requires Source Gateway verification, "
                "deduplication, freshness and materiality before current news"
            ),
            "lineage": {
                "authority_tier": 3,
                "news_is_candidate_only": True,
            },
        }


def _complete(value: dict[str, Any]) -> dict[str, Any]:
    output = dict(value)
    output["execution_status"] = "SUCCEEDED"
    coverage = output.get("coverage")
    output["data_coverage_status"] = (
        "COMPLETE"
        if output.get("status") == "AVAILABLE"
        and (coverage is None or float(coverage) >= 0.8)
        else "PARTIAL"
        if output.get("status") == "AVAILABLE"
        else "NO_DATA"
    )
    return output


def _section(
    items: list[dict[str, Any]],
    *,
    provider: str,
    trigger_class: str,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "status": "AVAILABLE" if items else "NO_DATA",
        "execution_status": "SUCCEEDED",
        "data_coverage_status": "COMPLETE" if items else "NO_DATA",
        "items": items,
        "provider": provider,
        "data_as_of": _latest_time(items),
        "freshness": "CURRENT" if items else "UNKNOWN",
        "coverage": 1.0 if items else 0.0,
        "trigger_class": trigger_class,
        "warnings": warnings,
    }


def _disabled(provider: str, trigger_class: str) -> dict[str, Any]:
    return {
        "status": "DISABLED",
        "execution_status": "NOT_RUN",
        "data_coverage_status": "NOT_APPLICABLE",
        "provider": provider,
        "trigger_class": trigger_class,
        "warnings": ["domain_disabled"],
    }


def _no_data(
    provider: str,
    trigger_class: str,
    warning: str,
) -> dict[str, Any]:
    return {
        "status": "NO_DATA",
        "execution_status": "SUCCEEDED",
        "data_coverage_status": "NO_DATA",
        "provider": provider,
        "coverage": 0.0,
        "trigger_class": trigger_class,
        "warnings": [warning],
    }


def _rates_context(series: dict[str, Any], now: datetime) -> dict[str, Any]:
    selected = {
        key: value
        for key, value in series.items()
        if key in {"DGS2", "DGS10", "DGS30", "SOFR", "T10Y2Y", "T10Y3M", "NFCI"}
    }
    return {
        **_section(
            [
                {"series_id": key, **dict(selected[key])}
                for key in sorted(selected)
                if isinstance(selected[key], dict)
            ],
            provider="FRED",
            trigger_class="NON_TRIGGERING",
            warnings=[] if selected else ["fred_rate_series_unavailable"],
        ),
        "series": selected,
        "retrieved_at": now.isoformat(),
    }


def _verified_current_news(contract: dict[str, Any]) -> dict[str, Any]:
    context = contract.get("news_context") if isinstance(contract.get("news_context"), dict) else {}
    items = list(context.get("current_drivers") or context.get("articles") or context.get("latest") or [])
    verified = []
    seen: set[str] = set()
    for raw in items:
        if not isinstance(raw, dict) or raw.get("candidate_only"):
            continue
        verification = str(raw.get("verification_status") or "").upper()
        gateway_status = str(
            (raw.get("validation") or {}).get("status")
            if isinstance(raw.get("validation"), dict)
            else ""
        ).upper()
        materiality = str(raw.get("materiality_status") or "").upper()
        freshness = str(raw.get("freshness") or raw.get("freshness_state") or "").upper()
        if (
            verification not in {"VERIFIED", "CONFIRMED", "ACCEPTED"}
            and gateway_status != "ACCEPTED"
        ):
            continue
        if materiality not in {"MATERIAL", "HIGH", "RELEVANT"}:
            continue
        if freshness in {"STALE", "EXPIRED", "REJECTED_FUTURE"}:
            continue
        identity = str(
            raw.get("article_id")
            or raw.get("source_url")
            or raw.get("url")
            or raw.get("headline")
            or ""
        )
        if not identity or identity in seen:
            continue
        seen.add(identity)
        verified.append(raw)
    return {
        **_section(
            verified,
            provider="SOURCE_GATEWAY",
            trigger_class="TRIGGER",
            warnings=[] if verified else ["no_verified_material_company_news"],
        ),
        "candidate_discovery_provider": "FINNHUB",
        "verification_policy": (
            "source_gateway+deduplication+freshness+materiality_required"
        ),
    }


def _macro_series(contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    snapshot = contract.get("macro_snapshot") if isinstance(contract.get("macro_snapshot"), dict) else {}
    output: dict[str, dict[str, Any]] = {}
    for value in snapshot.values():
        if not isinstance(value, dict):
            continue
        for key, row in value.items():
            if isinstance(row, dict) and row.get("value") is not None:
                output[str(key)] = row
    return output


def _holdings(contract: dict[str, Any]) -> list[dict[str, Any]]:
    nasdaq = contract.get("nasdaq_context") if isinstance(contract.get("nasdaq_context"), dict) else {}
    qqq = nasdaq.get("qqq_holdings") if isinstance(nasdaq.get("qqq_holdings"), dict) else {}
    return [
        dict(item)
        for item in (qqq.get("holdings") or qqq.get("top_holdings") or [])
        if isinstance(item, dict) and item.get("symbol")
    ]


def _all_events(contract: dict[str, Any]) -> list[dict[str, Any]]:
    calendar = contract.get("event_calendar") if isinstance(contract.get("event_calendar"), dict) else {}
    output = []
    for value in calendar.values():
        if isinstance(value, list):
            output.extend(item for item in value if isinstance(item, dict))
    return output


def _released_events(contract: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in _all_events(contract)
        if item.get("actual") is not None
        or str(item.get("temporal_status") or item.get("status") or "").upper()
        in {"RELEASED", "PUBLISHED", "ACTUAL_AVAILABLE"}
    ]


def _csv(value: str) -> list[str]:
    return [item.strip().upper() for item in str(value).split(",") if item.strip()]


def _latest_time(items: Iterable[dict[str, Any]]) -> Any:
    values = [
        item.get("data_as_of")
        or item.get("retrieved_at")
        or item.get("published_at")
        or item.get("scheduled_date")
        for item in items
        if isinstance(item, dict)
    ]
    return max((value for value in values if value), default=None)


def _merge_cache_telemetry(telemetry: dict[str, Any], resolution) -> None:
    detail = resolution.telemetry
    telemetry["actual_provider_requests"] += int(detail.get("actual_provider_requests") or 0)
    telemetry["cache_hits"] += int(detail.get("cache_hit") or 0)
    telemetry["negative_cache_hits"] += int(detail.get("negative_cache_hit") or 0)
    telemetry["stale_grace_hits"] += int(resolution.cache_status == "STALE_GRACE")


def _merge_tradier_telemetry(telemetry: dict[str, Any], tradier) -> None:
    details = getattr(tradier, "last_telemetry", {})
    if not isinstance(details, dict):
        return
    # Each endpoint entry is overwritten by the most recent resolution, so count
    # only the snapshot visible at this point.
    telemetry["tradier"] = copy.deepcopy(details)
    telemetry["actual_provider_requests"] += sum(
        int(item.get("actual_provider_requests") or 0)
        for item in details.values()
    )
    telemetry["cache_hits"] += sum(int(item.get("cache_hit") or 0) for item in details.values())
    telemetry["negative_cache_hits"] += sum(
        int(item.get("negative_cache_hit") or 0) for item in details.values()
    )
