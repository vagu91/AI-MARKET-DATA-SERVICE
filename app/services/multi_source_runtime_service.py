from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.providers.cboe_risk_indices_provider import CboeRiskIndicesProvider
from app.providers.cme_market_schedule_provider import CmeMarketScheduleProvider
from app.providers.investing_economic_calendar_provider import InvestingEconomicCalendarProvider
from app.providers.investing_fed_rate_monitor_provider import InvestingFedRateMonitorProvider
from app.providers.investing_holiday_calendar_provider import InvestingHolidayCalendarProvider
from app.providers.macromicro_aaii_crosscheck_provider import MacroMicroAaiiCrosscheckProvider
from app.providers.marketbeat_holidays_provider import MarketBeatHolidaysProvider
from app.providers.nasdaq_100_constituents_provider import Nasdaq100ConstituentsProvider
from app.providers.nasdaq_earnings_provider import NasdaqEarningsProvider
from app.providers.nasdaq_market_info_provider import NasdaqMarketInfoProvider
from app.providers.nasdaq_qqq_option_chain_provider import NasdaqQQQOptionChainProvider
from app.providers.polymarket_prediction_provider import PolymarketPredictionProvider
from app.services.market_fact_repository import MarketFactRepository, now_iso
from app.services.positioning_runtime_service import PositioningRuntimeService
from app.services.provider_observation_repository import ProviderObservationRepository


FetchCallable = Callable[[], Awaitable[dict[str, Any]]]


FACT_TYPES = {
    "investing_economic_calendar": "investing_economic_calendar",
    "investing_holidays": "investing_holidays",
    "marketbeat_holidays": "marketbeat_holidays",
    "cme_market_schedule": "cme_market_schedule",
    "investing_fed_rate_monitor": "investing_fed_rate_monitor",
    "cboe_risk_indices": "cboe_risk_indices",
    "nasdaq_earnings": "nasdaq_earnings_calendar",
    "nasdaq_100": "nasdaq_100_constituents",
    "nasdaq_market_info": "nasdaq_market_info",
    "nasdaq_qqq_options": "nasdaq_qqq_options",
    "aaii_sentiment": "aaii_sentiment",
    "macromicro_aaii_crosscheck": "macromicro_aaii_crosscheck",
    "polymarket_prediction_markets": "polymarket_prediction_markets",
    "quikstrike_review": "quikstrike_review",
}


class MultiSourceRuntimeService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.facts = MarketFactRepository(settings)
        self.observations = ProviderObservationRepository(settings)
        self.investing_calendar = InvestingEconomicCalendarProvider(settings)
        self.investing_holidays = InvestingHolidayCalendarProvider(settings)
        self.marketbeat_holidays = MarketBeatHolidaysProvider(settings)
        self.cme_market_schedule = CmeMarketScheduleProvider(settings)
        self.investing_fed_rate_monitor = InvestingFedRateMonitorProvider(settings)
        self.cboe = CboeRiskIndicesProvider(settings)
        self.nasdaq_earnings = NasdaqEarningsProvider(settings)
        self.nasdaq_100 = Nasdaq100ConstituentsProvider(settings)
        self.nasdaq_market_info = NasdaqMarketInfoProvider(settings)
        self.nasdaq_options = NasdaqQQQOptionChainProvider(settings)
        self.positioning_runtime = PositioningRuntimeService(settings)
        self.macromicro = MacroMicroAaiiCrosscheckProvider(settings)
        self.polymarket = PolymarketPredictionProvider(settings)

    async def snapshot(
        self,
        *,
        refresh: str = "auto",
        preloaded_blocks: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        preloaded_blocks = preloaded_blocks or {}
        blocks = {
            "investing_economic_calendar": preloaded_blocks.get("investing_economic_calendar") or await self._run_provider(
                "investing_economic_calendar",
                FACT_TYPES["investing_economic_calendar"],
                self.investing_calendar.fetch,
                item_count=_count_investing_calendar,
                enabled=self.settings.enable_investing_calendar,
                source="Investing Economic Calendar",
                refresh=refresh,
            ),
            "investing_holidays": await self._run_provider(
                "investing_holidays",
                FACT_TYPES["investing_holidays"],
                self.investing_holidays.fetch,
                item_count=lambda payload: len(payload.get("holidays") or []),
                enabled=self.settings.enable_investing_holidays,
                source="Investing Holiday Calendar",
                refresh=refresh,
            ),
            "marketbeat_holidays": await self._run_provider(
                "marketbeat_holidays",
                FACT_TYPES["marketbeat_holidays"],
                self.marketbeat_holidays.fetch,
                item_count=lambda payload: len(payload.get("holidays") or []),
                enabled=self.settings.enable_marketbeat_holidays,
                source="MarketBeat Stock Market Holidays",
                refresh=refresh,
                persist_unmaterialized=False,
            ),
            "cme_market_schedule": await self._run_provider(
                "cme_market_schedule",
                FACT_TYPES["cme_market_schedule"],
                self.cme_market_schedule.fetch,
                item_count=lambda payload: 1 if payload.get("calendar_verified") else 0,
                enabled=self.settings.enable_cme_market_schedule,
                source="CME Group Trading Hours",
                refresh=refresh,
                persist_unmaterialized=False,
            ),
            "investing_fed_rate_monitor": await self._run_provider(
                "investing_fed_rate_monitor",
                FACT_TYPES["investing_fed_rate_monitor"],
                self.investing_fed_rate_monitor.fetch,
                item_count=lambda payload: len(payload.get("meetings") or []),
                enabled=self.settings.enable_investing_fed_rate_monitor,
                source="Investing.com Fed Rate Monitor",
                refresh=refresh,
                persist_unmaterialized=False,
            ),
            "cboe_risk_indices": await self._run_provider(
                "cboe_risk_indices",
                FACT_TYPES["cboe_risk_indices"],
                self.cboe.fetch,
                item_count=lambda payload: len(payload.get("indices") or {}),
                enabled=self.settings.enable_cboe_risk_indices,
                source="CBOE",
                refresh=refresh,
            ),
            "nasdaq_earnings": await self._run_provider(
                "nasdaq_earnings",
                FACT_TYPES["nasdaq_earnings"],
                self.nasdaq_earnings.fetch,
                item_count=lambda payload: len(payload.get("events") or []),
                enabled=self.settings.enable_nasdaq_earnings,
                source="Nasdaq Earnings Calendar",
                refresh=refresh,
            ),
            "nasdaq_100": await self._run_provider(
                "nasdaq_100",
                FACT_TYPES["nasdaq_100"],
                self.nasdaq_100.fetch,
                item_count=lambda payload: len(payload.get("constituents") or []),
                enabled=self.settings.enable_nasdaq_100,
                source="Nasdaq-100 Constituents",
                refresh=refresh,
            ),
            "nasdaq_market_info": await self._run_provider(
                "nasdaq_market_info",
                FACT_TYPES["nasdaq_market_info"],
                self.nasdaq_market_info.fetch,
                item_count=lambda payload: 1 if payload.get("status") == "found" else 0,
                enabled=self.settings.enable_nasdaq_market_info,
                source="Nasdaq Market Info",
                refresh=refresh,
            ),
            "nasdaq_qqq_options": await self._run_provider(
                "nasdaq_qqq_options",
                FACT_TYPES["nasdaq_qqq_options"],
                self.nasdaq_options.fetch,
                item_count=lambda payload: len(payload.get("contracts") or []),
                enabled=self.settings.enable_nasdaq_qqq_options,
                source="Nasdaq QQQ Option Chain",
                refresh=refresh,
            ),
            "aaii_sentiment": await self._run_aaii(refresh=refresh),
            "macromicro_aaii_crosscheck": await self._run_provider(
                "macromicro_aaii_crosscheck",
                FACT_TYPES["macromicro_aaii_crosscheck"],
                self.macromicro.fetch,
                item_count=lambda payload: 1 if payload.get("status") == "found" else 0,
                enabled=self.settings.enable_macromicro_aaii_crosscheck,
                source="MacroMicro AAII Cross-check",
                refresh=refresh,
            ),
            "polymarket_prediction_markets": await self._run_provider(
                "polymarket_prediction_markets",
                FACT_TYPES["polymarket_prediction_markets"],
                self.polymarket.fetch,
                item_count=lambda payload: len(payload.get("markets") or []),
                enabled=self.settings.enable_polymarket,
                source="Polymarket",
                refresh=refresh,
            ),
            "quikstrike_review": self._quikstrike_review(),
        }
        quality = _quality_summary(blocks)
        return {
            "status": "available",
            "refresh_mode": refresh,
            "blocks": blocks,
            "context_blocks": build_multi_source_context_blocks(blocks),
            "data_quality": quality,
            "service_role": "data provider only",
        }

    def persist_provider_result(self, name: str, result: dict[str, Any], *, source: str) -> int:
        return self._save_fact(name, FACT_TYPES[name], result, source=source)

    async def provider(self, name: str, *, refresh: str = "auto") -> dict[str, Any]:
        if name == "investing_economic_calendar":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.investing_calendar.fetch,
                item_count=_count_investing_calendar,
                enabled=self.settings.enable_investing_calendar,
                source="Investing Economic Calendar",
                refresh=refresh,
            )
        if name == "investing_holidays":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.investing_holidays.fetch,
                item_count=lambda payload: len(payload.get("holidays") or []),
                enabled=self.settings.enable_investing_holidays,
                source="Investing Holiday Calendar",
                refresh=refresh,
            )
        if name == "marketbeat_holidays":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.marketbeat_holidays.fetch,
                item_count=lambda payload: len(payload.get("holidays") or []),
                enabled=self.settings.enable_marketbeat_holidays,
                source="MarketBeat Stock Market Holidays",
                refresh=refresh,
                persist_unmaterialized=False,
            )
        if name == "cme_market_schedule":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.cme_market_schedule.fetch,
                item_count=lambda payload: 1 if payload.get("calendar_verified") else 0,
                enabled=self.settings.enable_cme_market_schedule,
                source="CME Group Trading Hours",
                refresh=refresh,
                persist_unmaterialized=False,
            )
        if name == "investing_fed_rate_monitor":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.investing_fed_rate_monitor.fetch,
                item_count=lambda payload: len(payload.get("meetings") or []),
                enabled=self.settings.enable_investing_fed_rate_monitor,
                source="Investing.com Fed Rate Monitor",
                refresh=refresh,
                persist_unmaterialized=False,
            )
        if name == "cboe_risk_indices":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.cboe.fetch,
                item_count=lambda payload: len(payload.get("indices") or {}),
                enabled=self.settings.enable_cboe_risk_indices,
                source="CBOE",
                refresh=refresh,
            )
        if name == "nasdaq_earnings":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.nasdaq_earnings.fetch,
                item_count=lambda payload: len(payload.get("events") or []),
                enabled=self.settings.enable_nasdaq_earnings,
                source="Nasdaq Earnings Calendar",
                refresh=refresh,
            )
        if name == "nasdaq_100":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.nasdaq_100.fetch,
                item_count=lambda payload: len(payload.get("constituents") or []),
                enabled=self.settings.enable_nasdaq_100,
                source="Nasdaq-100 Constituents",
                refresh=refresh,
            )
        if name == "nasdaq_market_info":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.nasdaq_market_info.fetch,
                item_count=lambda payload: 1 if payload.get("status") == "found" else 0,
                enabled=self.settings.enable_nasdaq_market_info,
                source="Nasdaq Market Info",
                refresh=refresh,
            )
        if name == "nasdaq_qqq_options":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.nasdaq_options.fetch,
                item_count=lambda payload: len(payload.get("contracts") or []),
                enabled=self.settings.enable_nasdaq_qqq_options,
                source="Nasdaq QQQ Option Chain",
                refresh=refresh,
            )
        if name == "aaii_sentiment":
            return await self._run_aaii(refresh=refresh)
        if name == "macromicro_aaii_crosscheck":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.macromicro.fetch,
                item_count=lambda payload: 1 if payload.get("status") == "found" else 0,
                enabled=self.settings.enable_macromicro_aaii_crosscheck,
                source="MacroMicro AAII Cross-check",
                refresh=refresh,
            )
        if name == "polymarket_prediction_markets":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.polymarket.fetch,
                item_count=lambda payload: len(payload.get("markets") or []),
                enabled=self.settings.enable_polymarket,
                source="Polymarket",
                refresh=refresh,
            )
        if name == "quikstrike_review":
            return self._quikstrike_review()
        raise KeyError(name)

    async def _run_provider(
        self,
        name: str,
        fact_type: str,
        fetcher: FetchCallable,
        *,
        item_count: Callable[[dict[str, Any]], int],
        enabled: bool,
        source: str,
        refresh: str | None = None,
        persist_unmaterialized: bool = True,
    ) -> dict[str, Any]:
        refresh_mode = refresh or "auto"
        cached = [] if refresh_mode == "force" else self.facts.get_valid_facts_by_type(fact_type)
        if cached:
            raw = cached[0].get("raw_payload") if isinstance(cached[0].get("raw_payload"), dict) else {}
            return _with_runtime_fields(raw, enabled=enabled, cache_used=True, provider_calls=0, attempted=False, persisted_count=1, read_back_count=1, materialized_count=1)
        if refresh_mode == "false":
            return _missing_payload(name, source, enabled, reason=f"{fact_type}_not_in_db_refresh_false")
        try:
            result = await fetcher()
        except Exception as exc:
            result = _provider_exception(name, source, exc)
        count = item_count(result)
        self._record(name, fact_type, result, count)
        materialized = _materialized(result, count)
        persisted_count = self._save_fact(name, fact_type, result, source=source) if _should_persist(result, count, persist_unmaterialized) else 0
        read_back_count = 1 if persisted_count and self.facts.get_fact(_fact_key(name, fact_type)) else 0
        materialized_count = 1 if read_back_count and materialized else 0
        return _with_runtime_fields(
            result,
            enabled=enabled,
            cache_used=False,
            provider_calls=1,
            attempted=True,
            persisted_count=persisted_count,
            read_back_count=read_back_count,
            materialized_count=materialized_count,
            item_count=count,
        )

    async def _run_aaii(self, *, refresh: str) -> dict[str, Any]:
        result = await self.positioning_runtime.aaii(refresh=refresh)
        cache_used = result.get("cache_status") == "hit"
        provider_calls = 0 if cache_used or refresh == "false" else 1
        found = 1 if result.get("status") == "found" and result.get("survey_date") else 0
        return _with_runtime_fields(
            result,
            enabled=self.settings.enable_aaii_sentiment,
            cache_used=cache_used,
            provider_calls=provider_calls,
            attempted=provider_calls > 0,
            persisted_count=found,
            read_back_count=found,
            materialized_count=found,
            item_count=found,
        )

    def _quikstrike_review(self) -> dict[str, Any]:
        payload = {
            "status": "reviewed_excluded",
            "provider": "CME QuikStrike",
            "source": "CME QuikStrike Open Interest Heatmap",
            "source_url": "https://www.cmegroup.com/tools-information/quikstrike/open-interest-heatmap.html",
            "retrieved_at": now_iso(),
            "valid_until": (datetime.now(UTC) + timedelta(days=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "session_bound": True,
            "operational_integration": False,
            "reason": "authentication/session/compliance",
            "warnings": ["quikstrike_excluded_session_bound_source"],
            "errors": [],
            "diagnostics": {"reviewed": True, "credentials_used": False},
            "service_role": "data provider only",
        }
        self._save_fact("quikstrike_review", FACT_TYPES["quikstrike_review"], payload, source="CME QuikStrike")
        return _with_runtime_fields(payload, enabled=True, cache_used=True, provider_calls=0, attempted=False, persisted_count=1, read_back_count=1, materialized_count=1, item_count=1)

    def _save_fact(self, name: str, fact_type: str, result: dict[str, Any], *, source: str) -> int:
        valid_until = result.get("valid_until") or (datetime.now(UTC) + timedelta(hours=self.settings.default_fact_ttl_hours)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.facts.upsert_fact(
            {
                "fact_key": _fact_key(name, fact_type),
                "fact_type": fact_type,
                "country": "US",
                "symbol": _symbol_for(name),
                "category": name,
                "event_name": source,
                "source": result.get("source") or source,
                "source_url": result.get("source_url"),
                "provider_type": "PUBLIC_HTTP",
                "reliability": result.get("reliability") or _reliability(result),
                "confidence": result.get("reliability") or _reliability(result),
                "retrieved_at": result.get("retrieved_at") or now_iso(),
                "valid_until": valid_until,
                "next_refresh_at": valid_until,
                "status": "active",
                "raw_payload_json": result,
                "warnings_json": result.get("warnings") or [],
                "errors_json": result.get("errors") or [],
            }
        )
        return 1

    def _record(self, name: str, fact_type: str, result: dict[str, Any], item_count: int) -> None:
        self.observations.record(
            provider_name=name,
            provider_type="PUBLIC_HTTP",
            status=result.get("status"),
            country="US",
            symbol=_symbol_for(name),
            category=fact_type,
            url=result.get("source_url"),
            item_count=item_count,
            error="; ".join(result.get("errors") or []) or None,
            warning="; ".join(result.get("warnings") or []) or None,
            duration_ms=result.get("duration_ms"),
            raw_payload_json={
                "status": result.get("status"),
                "diagnostics": result.get("diagnostics") or {},
                "warnings": result.get("warnings") or [],
                "errors": result.get("errors") or [],
            },
        )


def build_multi_source_context_blocks(blocks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    investing = blocks.get("investing_economic_calendar") or {}
    holidays = blocks.get("investing_holidays") or {}
    marketbeat_holidays = blocks.get("marketbeat_holidays") or {}
    cme_market_schedule = blocks.get("cme_market_schedule") or {}
    fed_rate_monitor = blocks.get("investing_fed_rate_monitor") or {}
    cboe = blocks.get("cboe_risk_indices") or {}
    earnings = blocks.get("nasdaq_earnings") or {}
    nasdaq_100 = blocks.get("nasdaq_100") or {}
    market_info = blocks.get("nasdaq_market_info") or {}
    options = blocks.get("nasdaq_qqq_options") or {}
    aaii = blocks.get("aaii_sentiment") or {}
    macromicro = blocks.get("macromicro_aaii_crosscheck") or {}
    polymarket = blocks.get("polymarket_prediction_markets") or {}
    quikstrike = blocks.get("quikstrike_review") or {}
    primary_holidays = holidays.get("relevant_holidays") or holidays.get("holidays") or []
    secondary_holidays = marketbeat_holidays.get("relevant_holidays") or marketbeat_holidays.get("holidays") or []
    merged_holidays = _merge_calendar_events(primary_holidays, secondary_holidays)
    return {
        "economic_calendar_enrichment": {
            "investing": {**investing, "events": investing.get("items") or []},
            "consensus_coverage": {
                "events_with_consensus": sum(1 for item in investing.get("items") or [] if item.get("consensus") is not None),
                "events_total": len(investing.get("items") or []),
            },
            "previous_coverage": {
                "events_with_previous": sum(1 for item in investing.get("items") or [] if item.get("previous") is not None),
                "events_total": len(investing.get("items") or []),
            },
            "secondary_actuals": {
                "count": sum(1 for item in investing.get("items") or [] if item.get("actual") is not None),
                "actual_is_official": False,
            },
        },
        "market_schedule": {
            "nasdaq_cash_session": market_info,
            "cme_calendar": cme_market_schedule,
            "holidays": primary_holidays or secondary_holidays,
            "holiday_source": holidays if primary_holidays else marketbeat_holidays,
            "holiday_fallback_source": marketbeat_holidays if secondary_holidays else {},
        },
        "market_calendar": {
            "cme_equity_futures": cme_market_schedule,
            "market_holidays": {
                "official_sources": {},
                "primary_sources": {
                    "investing": holidays,
                },
                "secondary_sources": {
                    "marketbeat": marketbeat_holidays,
                },
                "merged_relevant_holidays": merged_holidays,
            }
        },
        "rates_expectations": {
            "fed_funds_futures": {
                "investing_fed_rate_monitor": fed_rate_monitor,
                "primary_source": None,
                "secondary_source": "Investing.com Fed Rate Monitor" if fed_rate_monitor else None,
                "official_fed_source": False,
            }
        },
        "risk_context": {
            "vvix": _risk_index_block((cboe.get("indices") or {}).get("vvix")),
            "skew": _risk_index_block((cboe.get("indices") or {}).get("skew")),
            "status": cboe.get("status"),
            "source": cboe.get("source"),
            "warnings": cboe.get("warnings") or [],
        },
        "corporate_events": {
            "earnings": {
                "status": earnings.get("status"),
                "relevant_upcoming": earnings.get("relevant_upcoming") or [],
                "mega_cap": _filter_symbols(earnings.get("relevant_upcoming") or [], {"NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG", "AVGO", "TSLA"}),
                "semiconductors": _filter_symbols(earnings.get("relevant_upcoming") or [], {"NVDA", "AVGO", "AMD", "QCOM", "INTC", "MU", "AMAT", "ASML", "ARM"}),
                "coverage": earnings.get("diagnostics") or {},
            }
        },
        "nasdaq_context_additions": {
            "nasdaq_100_official_snapshot": nasdaq_100,
            "qqq_options": {
                "status": options.get("status"),
                "snapshot": options.get("snapshot") or {},
                "open_interest_matrix": options.get("open_interest_matrix") or {},
                "observed_aggregates": options.get("observed_aggregates") or options.get("aggregates") or {},
                "global_aggregates": options.get("global_aggregates"),
                "diagnostics": options.get("diagnostics") or {},
                "warnings": options.get("warnings") or [],
            },
        },
        "sentiment": {
            "aaii": aaii,
            "aaii_crosscheck": macromicro,
            "prediction_markets": polymarket,
        },
        "source_reviews": {
            "quikstrike": quikstrike,
        },
    }


def apply_multi_source_context(contract: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    context_blocks = snapshot.get("context_blocks") or {}
    contract["economic_calendar_enrichment"] = context_blocks.get("economic_calendar_enrichment") or {}
    contract["market_schedule"] = context_blocks.get("market_schedule") or {}
    contract["market_calendar"] = context_blocks.get("market_calendar") or {}
    contract["rates_expectations"] = context_blocks.get("rates_expectations") or {}
    contract["risk_context"] = context_blocks.get("risk_context") or {}
    contract["corporate_events"] = context_blocks.get("corporate_events") or {}
    additions = context_blocks.get("nasdaq_context_additions") or {}
    nasdaq = dict(contract.get("nasdaq_context") or {})
    nasdaq.update(additions)
    contract["nasdaq_context"] = nasdaq
    sentiment = context_blocks.get("sentiment") or {}
    contract["sentiment"] = sentiment
    sentiment_context = dict(contract.get("sentiment_context") or {})
    if sentiment.get("aaii_crosscheck"):
        sentiment_context["aaii_crosscheck"] = sentiment["aaii_crosscheck"]
    if sentiment.get("prediction_markets"):
        sentiment_context["prediction_markets"] = sentiment["prediction_markets"]
    contract["sentiment_context"] = sentiment_context
    contract["source_reviews"] = context_blocks.get("source_reviews") or {}
    quality = dict(contract.get("data_quality") or {})
    quality["multi_source_pipeline"] = snapshot.get("data_quality") or {}
    contract["data_quality"] = quality
    metadata = dict(contract.get("metadata") or {})
    metadata["multi_source_runtime"] = {
        "refresh_mode": snapshot.get("refresh_mode"),
        "provider_calls": (snapshot.get("data_quality") or {}).get("provider_calls"),
        "cache_used": (snapshot.get("data_quality") or {}).get("cache_used"),
    }
    contract["metadata"] = metadata
    return contract


def _quality_summary(blocks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "provider_calls": sum(int(block.get("provider_calls") or 0) for block in blocks.values()),
        "cache_used": all(bool(block.get("cache_used")) or int(block.get("provider_calls") or 0) == 0 for block in blocks.values()),
        "fetched_count": sum(int(block.get("fetched_count") or 0) for block in blocks.values()),
        "validated_count": sum(int(block.get("validated_count") or 0) for block in blocks.values()),
        "rejected_count": sum(int(block.get("rejected_count") or 0) for block in blocks.values()),
        "persisted_count": sum(int(block.get("persisted_count") or 0) for block in blocks.values()),
        "read_back_count": sum(int(block.get("read_back_count") or 0) for block in blocks.values()),
        "materialized_count": sum(int(block.get("materialized_count") or 0) for block in blocks.values()),
        "warnings": [warning for block in blocks.values() for warning in (block.get("warnings") or [])],
        "errors": [error for block in blocks.values() for error in (block.get("errors") or [])],
        "blocks": {
            name: {
                "status": block.get("status"),
                "provider_calls": block.get("provider_calls"),
                "cache_used": block.get("cache_used"),
                "fetched_count": block.get("fetched_count"),
                "persisted_count": block.get("persisted_count"),
                "read_back_count": block.get("read_back_count"),
                "materialized_count": block.get("materialized_count"),
            }
            for name, block in blocks.items()
        },
    }


def _with_runtime_fields(
    payload: dict[str, Any],
    *,
    enabled: bool,
    cache_used: bool,
    provider_calls: int,
    attempted: bool,
    persisted_count: int,
    read_back_count: int,
    materialized_count: int,
    item_count: int | None = None,
) -> dict[str, Any]:
    output = dict(payload)
    fetched = item_count if item_count is not None else _generic_item_count(output)
    rejected = _rejected_count(output)
    output.update(
        {
            "enabled": enabled,
            "attempted": attempted,
            "provider_calls": provider_calls,
            "cache_used": cache_used,
            "AI_called": False,
            "fetched_count": fetched,
            "validated_count": max(fetched - rejected, 0),
            "rejected_count": rejected,
            "persisted_count": persisted_count,
            "committed": persisted_count > 0 or provider_calls == 0,
            "read_back_count": read_back_count,
            "materialized_count": materialized_count,
            "excluded_count": _excluded_count(output),
            "exclusion_reasons": _exclusion_reasons(output),
        }
    )
    return output


def _missing_payload(name: str, source: str, enabled: bool, *, reason: str) -> dict[str, Any]:
    now = now_iso()
    return _with_runtime_fields(
        {
            "status": "not_found",
            "provider": source,
            "source": source,
            "source_url": None,
            "retrieved_at": now,
            "valid_until": None,
            "warnings": [reason],
            "errors": [],
            "diagnostics": {"reason": reason},
        },
        enabled=enabled,
        cache_used=False,
        provider_calls=0,
        attempted=False,
        persisted_count=0,
        read_back_count=0,
        materialized_count=0,
        item_count=0,
    )


def _provider_exception(name: str, source: str, exc: Exception) -> dict[str, Any]:
    now = now_iso()
    return {
        "status": "provider_failed",
        "provider": source,
        "source": source,
        "source_url": None,
        "retrieved_at": now,
        "valid_until": (datetime.now(UTC) + timedelta(minutes=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "warnings": [],
        "errors": [str(exc) or type(exc).__name__],
        "diagnostics": {"provider": name},
    }


def _count_investing_calendar(payload: dict[str, Any]) -> int:
    return len(payload.get("items") or [])


def _generic_item_count(payload: dict[str, Any]) -> int:
    for key in ("items", "holidays", "indices", "events", "constituents", "contracts", "markets"):
        value = payload.get(key)
        if isinstance(value, dict):
            return len(value)
        if isinstance(value, list):
            return len(value)
    return 1 if payload.get("status") in {"found", "valid", "reviewed_excluded"} else 0


def _rejected_count(payload: dict[str, Any]) -> int:
    diagnostics = payload.get("diagnostics") or {}
    return int(
        diagnostics.get("rejected_future_actual")
        or diagnostics.get("rejected_count")
        or diagnostics.get("rejected_invalid_probability")
        or 0
    )


def _excluded_count(payload: dict[str, Any]) -> int:
    diagnostics = payload.get("diagnostics") or {}
    return sum(
        int(diagnostics.get(key) or 0)
        for key in (
            "rejected_irrelevant",
            "rejected_weak_indirect",
            "rejected_low_relevance",
            "rejected_rules_only",
            "rejected_low_liquidity",
            "rejected_low_volume",
            "rejected_wide_spread",
            "rejected_expired",
        )
    )


def _exclusion_reasons(payload: dict[str, Any]) -> dict[str, int]:
    diagnostics = payload.get("diagnostics") or {}
    reasons: dict[str, int] = {}
    for key, value in diagnostics.items():
        if not key.startswith("rejected_"):
            continue
        try:
            count = int(value or 0)
        except (TypeError, ValueError):
            continue
        if count:
            reasons[key.removeprefix("rejected_")] = count
    return reasons


def _materialized(payload: dict[str, Any], item_count: int) -> bool:
    return item_count > 0 or payload.get("status") in {"not_found", "disabled", "restricted", "reviewed_excluded"}


def _fact_key(name: str, fact_type: str) -> str:
    return f"multi_source:{name}:{fact_type}:latest"


def _symbol_for(name: str) -> str | None:
    if name in {"nasdaq_qqq_options", "nasdaq_100", "nasdaq_earnings"}:
        return "QQQ"
    if name == "cboe_risk_indices":
        return "VVIX,SKEW"
    return None


def _reliability(result: dict[str, Any]) -> float:
    status = str(result.get("status") or "")
    if status in {"found", "valid"}:
        return 0.82
    if status in {"partial", "anomalous", "not_found", "restricted", "reviewed_excluded"}:
        return 0.55
    return 0.0


def _filter_symbols(events: list[dict[str, Any]], symbols: set[str]) -> list[dict[str, Any]]:
    return [item for item in events if str(item.get("symbol") or "").upper() in symbols]


def _risk_index_block(index: dict[str, Any] | None) -> dict[str, Any]:
    if not index:
        return {"status": "not_found", "value": None, "current_price": None}
    return {
        **index,
        "status": "found" if index.get("current_price") is not None else "not_found",
        "value": index.get("current_price"),
    }


def _merge_calendar_events(primary: list[dict[str, Any]], secondary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for item in secondary:
        date = str(item.get("date") or "")
        name = str(item.get("holiday_name") or item.get("name") or "")
        if date and name:
            merged[(date, name.lower())] = item
    for item in primary:
        date = str(item.get("date") or "")
        name = str(item.get("holiday_name") or item.get("name") or "")
        if date and name:
            merged[(date, name.lower())] = item
    return sorted(merged.values(), key=lambda item: (item.get("date") or "", item.get("holiday_name") or item.get("name") or ""))


def _should_persist(payload: dict[str, Any], item_count: int, persist_unmaterialized: bool) -> bool:
    if persist_unmaterialized:
        return True
    return item_count > 0 or payload.get("status") in {"found", "valid", "anomalous", "partial", "reviewed_excluded"}


def assert_fetcher_shape(fetcher: FetchCallable) -> None:
    if not inspect.iscoroutinefunction(fetcher):
        raise TypeError("provider fetcher must be async")
