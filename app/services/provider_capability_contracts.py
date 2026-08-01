from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import pkgutil
from collections import defaultdict
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence


LOCAL_PROBE_ADAPTER_PATH = "app.services.provider_capability_probe_hooks:LocalCapabilityProbeHook"


@dataclass(frozen=True, slots=True)
class ProbeOutputContract:
    """Independent CI contract for one registered, normalized probe output."""

    provider_id: str
    normalized_fields: frozenset[str]
    capability_schema_sha256: str
    probe_adapter_paths: tuple[str, ...]
    source_adapter_paths: tuple[str, ...]
    audit_only_fields: frozenset[str] = frozenset()
    terminal_audit_reason: str | None = None


def _split(value: str) -> tuple[str, ...]:
    return tuple(item for item in value.split(";") if item)


def _contract_rows() -> tuple[
    tuple[str, str, str, str, str, str],
    ...,
]:
    # This is deliberately not generated from PROVIDER_REGISTRY at import
    # time. It is the reviewable adapter-output schema snapshot that CI
    # compares with the registry. Adding a probe, capability, metric/field
    # assignment, or adapter therefore requires an intentional contract edit.
    return (
        (
            "probe.invesco.qqq_holdings",
            "INVESCO",
            "lineage,name,symbol,weight,weight_as_of",
            "0c6555a5f797ec1e232bc44c2f28290784c4a091e78e4d6f68bedb9ae1fb97cb",
            "app.providers.qqq_holdings_provider:QQQHoldingsProvider",
            "app.providers.qqq_holdings_provider:QQQHoldingsProvider",
        ),
        (
            "probe.alpha_vantage.etf_profile",
            "ALPHA_VANTAGE",
            "lineage,name,symbol,weight,weight_as_of",
            "0c6555a5f797ec1e232bc44c2f28290784c4a091e78e4d6f68bedb9ae1fb97cb",
            "app.providers.qqq_holdings_provider:QQQHoldingsProvider",
            "app.providers.qqq_holdings_provider:QQQHoldingsProvider",
        ),
        (
            "probe.alpha_vantage.global_quote",
            "ALPHA_VANTAGE",
            "change,change_pct,data_as_of,last_price,lineage,symbol,volume",
            "7bc977771b028bb3eb08495f4f7ec40c9244fd8a34d56f5025e39f91b079c7e5",
            "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
            "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
        ),
        (
            "probe.nasdaq.constituents",
            "NASDAQ",
            "data_as_of,lineage,market_cap,name,symbol",
            "84aebcd4776f2c30aff59a38cda0a10b7dacb4f38041802f1ccc5bf45c02162b",
            "app.providers.nasdaq_100_constituents_provider:Nasdaq100ConstituentsProvider",
            "app.providers.nasdaq_100_constituents_provider:Nasdaq100ConstituentsProvider",
        ),
        (
            "probe.nasdaq.earnings",
            "NASDAQ",
            "eps_estimate,event_at,event_date,lineage,revenue_estimate,symbol,temporal_precision,timing",
            "0ebdabaf1fd20f11e06da69717ac7911fcf6a15bf2f55644fa491226490669fd",
            "app.providers.nasdaq_earnings_provider:NasdaqEarningsProvider",
            "app.providers.nasdaq_earnings_provider:NasdaqEarningsProvider",
        ),
        (
            "probe.sec.class_shares",
            "SEC",
            "class,filing_at,lineage,shares_outstanding,symbol",
            "ec2ebcf8f211930a10f15bd26d197ac4ff7bb9c5e58a28f645039f30fd2d479e",
            "app.providers.sec_class_shares_provider:SecClassSharesProvider",
            "app.providers.sec_class_shares_provider:SecClassSharesProvider",
        ),
        (
            "probe.yahoo_finance.chart",
            "YAHOO_FINANCE_CHART",
            "change,change_pct,data_as_of,last_price,lineage,symbol,volume",
            "7bc977771b028bb3eb08495f4f7ec40c9244fd8a34d56f5025e39f91b079c7e5",
            "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
            "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
        ),
        (
            "probe.stooq.quote_csv",
            "STOOQ",
            "change,change_pct,data_as_of,last_price,lineage,symbol,volume",
            "7bc977771b028bb3eb08495f4f7ec40c9244fd8a34d56f5025e39f91b079c7e5",
            "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
            "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
        ),
        (
            "probe.yahoo_finance.quote",
            "YAHOO_FINANCE_QUOTE",
            "change,change_pct,data_as_of,last_price,lineage,symbol,volume",
            "7bc977771b028bb3eb08495f4f7ec40c9244fd8a34d56f5025e39f91b079c7e5",
            "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
            "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
        ),
        (
            "probe.tradier.quotes",
            "TRADIER",
            "advance_decline_ratio,advancers,data_as_of,decliners,lineage,percent_advancers,unchanged",
            "00392305b3f3249b4888fd51fff8f4cd11e400109b61d316e7411bdc09e50d39",
            "app.providers.tradier:TradierProvider",
            "app.providers.tradier:TradierProvider",
        ),
        (
            "probe.tradier.option_chain",
            "TRADIER",
            "data_as_of,expirations,iv_atm,lineage,open_interest,skew,strikes,volume",
            "ff88dc4388679656bf99fe41c3a98ad17e2ca0e7c8d522be0f6f117dd7fd8471",
            "app.providers.tradier:TradierProvider",
            "app.providers.tradier:TradierProvider",
        ),
        (
            "probe.fred.series",
            "FRED",
            "content_valid_until,data_as_of,frequency,lineage,observations,released_at,series_id,source,source_url,unit,units,value",
            "9f40b280afe734412d5466335c20034c0299854a02e8e09089b7e507592d347d",
            "app.providers.fred:FredProvider",
            "app.providers.fred:FredProvider",
        ),
        (
            "probe.cboe.risk_indices",
            "CBOE",
            "data_as_of,lineage,provider_timestamp,risk_score,risk_sentiment,skew,value,vvix",
            "8e2bab773eddb4b5109928d7fe399d31076d62b1ab54a9357521efc0ddf313b4",
            "app.providers.cboe_risk_indices_provider:CboeRiskIndicesProvider",
            "app.providers.cboe_risk_indices_provider:CboeRiskIndicesProvider",
        ),
        (
            "probe.cboe.put_call",
            "CBOE",
            "data_as_of,lineage,put_call_ratio",
            "e50223af77fb7fc641c42c81969a5029d3a54d90a2f6af0595991a7a152eeec1",
            "app.providers.cboe_put_call_provider:CboePutCallProvider",
            "app.providers.cboe_put_call_provider:CboePutCallProvider",
        ),
        (
            "probe.cboe.vix_futures",
            "CBOE",
            "data_as_of,lineage,settlement,term_structure",
            "e95ac910be131c008b02ba87aa239de0c41ae5cefb9a03d62f2b1000a9b71d53",
            "app.providers.cboe_vix_futures_provider:CboeVixFuturesProvider",
            "app.providers.cboe_vix_futures_provider:CboeVixFuturesProvider",
        ),
        (
            "probe.federal_reserve.calendar",
            "FEDERAL_RESERVE",
            "event_at,lineage,name,source_url",
            "0eedd49c236e70090d51051743707d9d0034aaa14d68cab79b53ef1d018d9fa6",
            "app.providers.fed_calendar:FederalReserveCalendarProvider",
            "app.providers.fed_calendar:FederalReserveCalendarProvider;app.providers.federal_reserve:FederalReserveRssProvider",
        ),
        (
            "probe.investing.fed_rate_monitor",
            "INVESTING_FED_RATE_MONITOR",
            "data_as_of,lineage,meeting_date,probabilities,target_ranges",
            "bc9077723180e24bfae0124d16a1a9eb1fd0d49db9a2f5e0d444ab6b509f5145",
            "app.providers.investing_fed_rate_monitor_provider:InvestingFedRateMonitorProvider",
            "app.providers.investing_fed_rate_monitor_provider:InvestingFedRateMonitorProvider",
        ),
        (
            "probe.bls.series",
            "BLS",
            "data_as_of,frequency,observations,series_id,source,source_url,units,value",
            "d984bafabeba04776a3b85889709e4ea1183604bed2a6028fbbb5da3b43f0916",
            "app.providers.bls:BlsProvider",
            "app.providers.bls:BlsProvider",
        ),
        (
            "probe.bls.release_calendar",
            "BLS",
            "actual,event_at,lineage,reference_period,released_at",
            "f3a1beccf4a9bdb84cc27a60f1843369955ff9dc6293e0a77e18112acadf7810",
            "app.providers.bls_calendar:BlsReleaseCalendarProvider",
            "app.providers.bls_calendar:BlsReleaseCalendarProvider",
        ),
        (
            "probe.bea.nipa",
            "BEA",
            "data_as_of,frequency,observations,series_id,source,source_url,units,value",
            "a6ca1ba3016297c2eb8134ebeb20cb64eb63bfa7779848ccbda3eed74d18a99a",
            "app.providers.bea:BeaProvider",
            "app.providers.bea:BeaProvider",
        ),
        (
            "probe.bea.release_schedule",
            "BEA",
            "actual,event_at,lineage,reference_period,released_at",
            "2df52e50eb7461dbcb93b7ae77908af810414789a846bbcef636b3e2894bd36e",
            "app.providers.bea_calendar:BeaReleaseScheduleProvider",
            "app.providers.bea_calendar:BeaReleaseScheduleProvider",
        ),
        (
            "probe.census.eits",
            "CENSUS",
            "data_as_of,frequency,lineage,observations,series_id,source,source_url,units,value",
            "611e7eeb5b520843c4c7296036de7098c370a085abdd8fa7266e363e16c678a3",
            "app.providers.census:CensusProvider",
            "app.providers.census:CensusProvider",
        ),
        (
            "probe.repository.event_values",
            "CANONICAL_EVENT_REPOSITORY",
            "actual,consensus,lineage,occurrence_id,previous,previous_revised,reference_period",
            "e784fd590a848a68a397c3ad6520f23ab3e00a6283bff59f58298abb30e85648",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.event_value_candidate_repository:EventValueCandidateRepository",
        ),
        (
            "probe.investing.economic_calendar",
            "INVESTING_ECONOMIC_CALENDAR",
            "actual,consensus,event_at,lineage,previous,reference_period",
            "d0626d6b35932042ad0896ae9917bd109e19890750bebd5c623070b294bfd7a5",
            "app.providers.investing_economic_calendar_provider:InvestingEconomicCalendarProvider",
            "app.providers.event_enrichment:InvestingEnrichmentProvider;app.providers.event_enrichment:PlaywrightInvestingProvider;app.providers.investing_economic_calendar_provider:InvestingEconomicCalendarProvider",
        ),
        (
            "probe.xtb.economic_calendar",
            "XTB",
            "actual,consensus,event_at,lineage,previous,reference_period",
            "d0626d6b35932042ad0896ae9917bd109e19890750bebd5c623070b294bfd7a5",
            "app.providers.xtb_economic_calendar_provider:XtbEconomicCalendarProvider",
            "app.providers.xtb_economic_calendar_provider:XtbEconomicCalendarProvider",
        ),
        (
            "probe.spglobal.flash_services_pmi",
            "SPGLOBAL",
            "data_as_of,frequency,lineage,observations,series_id,source,source_url,units,value",
            "082c4dee3985cb68ad6577bc8e78795138373f66fdf3017fa743c240318eec14",
            "app.providers.sp_global_pmi:SpGlobalPmiProvider",
            "app.providers.sp_global_pmi:SpGlobalPmiProvider",
        ),
        (
            "probe.investing.event_1062",
            "INVESTING_EVENT_1062",
            "actual,consensus,lineage,occurrence_id,previous,reference_period,released_at",
            "129a692cc841cb74c6b8acab1c9b44de4c0c79f883ef6c2452a3d16aaddb33f9",
            "app.providers.investing_flash_services_pmi:InvestingFlashServicesPmiProvider",
            "app.providers.investing_flash_services_pmi:InvestingFlashServicesPmiProvider",
        ),
        (
            "probe.fmp.earnings",
            "FMP_EARNINGS_CALENDAR",
            "eps_estimate,event_at,event_date,lineage,revenue_estimate,symbol,temporal_precision,timing",
            "0ebdabaf1fd20f11e06da69717ac7911fcf6a15bf2f55644fa491226490669fd",
            "app.providers.fmp_earnings_calendar_provider:FmpEarningsCalendarProvider",
            "app.providers.fmp_earnings_calendar_provider:FmpEarningsCalendarProvider",
        ),
        (
            "probe.cftc.cot",
            "CFTC",
            "contract_code,lineage,long,net_position,report_date,short",
            "46e8907f6530cd22513651d27907f42fc00766e0be74c2da48aa569249dace08",
            "app.providers.cftc_cot_provider:CftcCotProvider",
            "app.providers.cftc_cot_provider:CftcCotProvider",
        ),
        (
            "probe.news.alpha_vantage",
            "ALPHA_VANTAGE_NEWS_SENTIMENT",
            "canonical_url,lineage,published_at,publisher,symbols,title,topics",
            "b4fd8a865e8c1bfc70442a3287dd6209981a6487f7432ecf08ec553679d8d57e",
            "app.providers.news_provider:NewsProvider",
            "app.providers.news_provider:NewsProvider",
        ),
        (
            "probe.news.gdelt",
            "GDELT_DOC_API",
            "canonical_url,lineage,published_at,publisher,symbols,title,topics",
            "b4fd8a865e8c1bfc70442a3287dd6209981a6487f7432ecf08ec553679d8d57e",
            "app.providers.news_provider:NewsProvider",
            "app.providers.news_provider:NewsProvider",
        ),
        (
            "probe.news.federal_reserve_rss",
            "FEDERAL_RESERVE_RSS",
            "canonical_url,lineage,published_at,publisher,symbols,title,topics",
            "b4fd8a865e8c1bfc70442a3287dd6209981a6487f7432ecf08ec553679d8d57e",
            "app.providers.news_provider:NewsProvider",
            "app.providers.news_provider:NewsProvider",
        ),
        (
            "probe.news.bls_rss",
            "BLS_RSS",
            "canonical_url,lineage,published_at,publisher,symbols,title,topics",
            "b4fd8a865e8c1bfc70442a3287dd6209981a6487f7432ecf08ec553679d8d57e",
            "app.providers.news_provider:NewsProvider",
            "app.providers.news_provider:NewsProvider",
        ),
        (
            "probe.news.bea_rss",
            "BEA_RSS",
            "canonical_url,lineage,published_at,publisher,symbols,title,topics",
            "b4fd8a865e8c1bfc70442a3287dd6209981a6487f7432ecf08ec553679d8d57e",
            "app.providers.news_provider:NewsProvider",
            "app.providers.news_provider:NewsProvider",
        ),
        (
            "probe.news.yahoo_rss",
            "YAHOO_FINANCE_RSS",
            "canonical_url,lineage,published_at,publisher,symbols,title,topics",
            "b4fd8a865e8c1bfc70442a3287dd6209981a6487f7432ecf08ec553679d8d57e",
            "app.providers.news_provider:NewsProvider",
            "app.providers.news_provider:NewsProvider",
        ),
        (
            "probe.news.marketwatch_rss",
            "MARKETWATCH_RSS",
            "canonical_url,lineage,published_at,publisher,symbols,title,topics",
            "b4fd8a865e8c1bfc70442a3287dd6209981a6487f7432ecf08ec553679d8d57e",
            "app.providers.news_provider:NewsProvider",
            "app.providers.news_provider:NewsProvider",
        ),
        (
            "probe.news.google_rss",
            "GOOGLE_NEWS_RSS",
            "canonical_url,lineage,published_at,publisher,symbols,title,topics",
            "b4fd8a865e8c1bfc70442a3287dd6209981a6487f7432ecf08ec553679d8d57e",
            "app.providers.news_provider:NewsProvider",
            "app.providers.news_provider:NewsProvider",
        ),
        (
            "probe.nasdaq.market_info",
            "NASDAQ_MARKET_INFO",
            "close_at,data_as_of,holiday,lineage,open_at,session_date",
            "5a4d3e95e2c6e49aee9add9c064d0943f9e0ac263cf2eb0e3adeec144d36e78a",
            "app.providers.nasdaq_market_info_provider:NasdaqMarketInfoProvider",
            "app.providers.nasdaq_market_info_provider:NasdaqMarketInfoProvider",
        ),
        (
            "probe.cme.market_schedule",
            "CME",
            "close_at,data_as_of,lineage,maintenance_break,open_at,session_date",
            "7ba25ccace23949830a9c44d258fe49348c54c930624333a661ff09e853a7704",
            "app.providers.cme_market_schedule_provider:CmeMarketScheduleProvider",
            "app.providers.cme_market_schedule_provider:CmeMarketScheduleProvider",
        ),
        (
            "probe.investing.holidays",
            "INVESTING_HOLIDAYS",
            "holiday_date,lineage,market,name",
            "63b20e303d434cd009208be152c7882d30d09c1be4ae42f6985aca22aab31b03",
            "app.providers.investing_holiday_calendar_provider:InvestingHolidayCalendarProvider",
            "app.providers.investing_holiday_calendar_provider:InvestingHolidayCalendarProvider",
        ),
        (
            "probe.marketbeat.holidays",
            "MARKETBEAT",
            "holiday_date,lineage,market,name",
            "63b20e303d434cd009208be152c7882d30d09c1be4ae42f6985aca22aab31b03",
            "app.providers.marketbeat_holidays_provider:MarketBeatHolidaysProvider",
            "app.providers.marketbeat_holidays_provider:MarketBeatHolidaysProvider",
        ),
        (
            "probe.nasdaq.qqq_options",
            "NASDAQ_QQQ_OPTIONS",
            "expiration,implied_volatility,lineage,open_interest,option_type,strike,volume",
            "a9c6aa2eaef995933613a348960fa8ddaec57af981417b8736a686b3b3b367f6",
            "app.providers.nasdaq_qqq_option_chain_provider:NasdaqQQQOptionChainProvider",
            "app.providers.nasdaq_qqq_option_chain_provider:NasdaqQQQOptionChainProvider",
        ),
        (
            "probe.finnhub.earnings",
            "FINNHUB",
            "eps_estimate,event_date,lineage,revenue_estimate,symbol,timing",
            "f4f68818bb6aa231abbc7348e8dd42785eb370f1bcc3d3714f7adf5fdff0233e",
            "app.providers.finnhub:FinnhubProvider",
            "app.providers.finnhub:FinnhubProvider",
        ),
        (
            "probe.finnhub.news",
            "FINNHUB",
            "headline,lineage,published_at,source,url",
            "deac34a77a0f02db60943bfeaf2c615593ad2bbf47495902e5cf62104723075d",
            "app.providers.finnhub:FinnhubProvider",
            "app.providers.finnhub:FinnhubProvider",
        ),
        (
            "probe.legacy_earnings.chain",
            "LEGACY_EARNINGS_AGGREGATOR",
            "eps_estimate,event_date,lineage,revenue_estimate,symbol,timing",
            "9e8c4a84483fdfbf678943cffe40da918fb3d5ff26f943c674de7eed187ba560",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.providers.earnings_provider:EarningsProvider",
        ),
        (
            "probe.aaii.sentiment",
            "AAII",
            "bearish,bullish,lineage,neutral,survey_date",
            "2784a7fcd6b97d01eaaee72c69fd747dbb2ccabc22352379585c00fc03c9bd68",
            "app.providers.aaii_sentiment_provider:AaiiSentimentProvider",
            "app.providers.aaii_sentiment_provider:AaiiSentimentProvider",
        ),
        (
            "probe.macromicro.aaii",
            "MACROMICRO",
            "bearish,bullish,lineage,neutral,survey_date",
            "f26cf603300a4c92375ac76a592af943a9c6315413a1ea67d4c2ad69a8abb741",
            "app.providers.macromicro_aaii_crosscheck_provider:MacroMicroAaiiCrosscheckProvider",
            "app.providers.macromicro_aaii_crosscheck_provider:MacroMicroAaiiCrosscheckProvider",
        ),
        (
            "probe.hacker_news.social",
            "HACKER_NEWS",
            "lineage,published_at,score,symbols,title,url",
            "2c68cbd5727fb58beed8fe245421f937135811673fb92125414451e1f2622cd6",
            "app.providers.hacker_news_social_sentiment_provider:HackerNewsSocialSentimentProvider",
            "app.providers.hacker_news_social_sentiment_provider:HackerNewsSocialSentimentProvider",
        ),
        (
            "probe.polymarket.markets",
            "POLYMARKET",
            "data_as_of,lineage,market_id,probability,question,volume",
            "0b48726d8a892c47cbf868deb4273256ad749d633c4c78af47fa92b285d1fca6",
            "app.providers.polymarket_prediction_provider:PolymarketPredictionProvider",
            "app.providers.polymarket_prediction_provider:PolymarketPredictionProvider",
        ),
        (
            "probe.dailyfx.calendar",
            "DAILYFX",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:DailyFxEnrichmentProvider",
            "app.providers.event_enrichment:DailyFxEnrichmentProvider;app.providers.event_enrichment:PlaywrightDailyFXProvider",
        ),
        (
            "probe.forex_factory.calendar",
            "FOREX_FACTORY",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:ForexFactoryEnrichmentProvider",
            "app.providers.event_enrichment:ForexFactoryEnrichmentProvider;app.providers.event_enrichment:PlaywrightForexFactoryProvider",
        ),
        (
            "probe.fxstreet.calendar",
            "FXSTREET",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:FXStreetEconomicCalendarProvider",
            "app.providers.event_enrichment:FXStreetEconomicCalendarProvider",
        ),
        (
            "probe.marketwatch.calendar",
            "MARKETWATCH_CALENDAR",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:MarketWatchEconomicCalendarProvider",
            "app.providers.event_enrichment:MarketWatchEconomicCalendarProvider",
        ),
        (
            "probe.yahoo.economic_calendar",
            "YAHOO_ECONOMIC_CALENDAR",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:YahooEconomicCalendarProvider",
            "app.providers.event_enrichment:YahooEconomicCalendarProvider",
        ),
        (
            "probe.generic_search.calendar",
            "GENERIC_SEARCH_CALENDAR",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:GenericSearchSnippetCalendarProvider",
            "app.providers.event_enrichment:GenericSearchSnippetCalendarProvider",
        ),
        (
            "probe.targeted_search.event",
            "TARGETED_SEARCH_EVENT",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:TargetedSearchEventEnrichmentProvider",
            "app.providers.event_enrichment:TargetedSearchEventEnrichmentProvider",
        ),
        (
            "probe.manual_event.file",
            "MANUAL_EVENT_ENRICHMENT",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:ManualEventEnrichmentProvider",
            "app.providers.event_enrichment:ManualEventEnrichmentProvider",
        ),
        (
            "probe.ai.openai_event_enrichment",
            "OPENAI_EVENT_ENRICHMENT",
            "consensus,event_at,lineage,previous,reference_period",
            "cc1e4e8bbbe1bb1109b9ddb36143f48117434b68e9b49c05474be0762c7ec002",
            "app.providers.event_enrichment:OpenAIEventEnrichmentProvider",
            "app.providers.event_enrichment:OpenAIEventEnrichmentProvider",
        ),
        (
            "probe.scraper.calendar",
            "ECONOMIC_CALENDAR_SCRAPER",
            "event_at,importance,lineage,name",
            "4dc0422ef6fdbf1041aa5b43bfe3fa2452617fb6810b9fb34a06bd8ada68e166",
            "app.providers.scraper_calendar:EconomicCalendarScraperProvider",
            "app.providers.scraper_calendar:EconomicCalendarScraperProvider",
        ),
        (
            "probe.ai.researcher.macro_calendar",
            "AI_RESEARCHER",
            (
                "consensus,consensus_lineage,lineage,occurrence_id,previous,"
                "previous_lineage,reference_period,source,source_url"
            ),
            "1aa576bef59e68abb9bf8def98f364c216c5380ffd62838a577488be178adbba",
            "app.providers.ai_researcher_provider:AIResearcherProvider",
            "app.providers.ai_researcher_provider:AIResearcherProvider",
        ),
        (
            "probe.ai.researcher.earnings",
            "AI_RESEARCHER",
            "event_date,lineage,publisher,source_url,timing",
            "c15edce671175c1c5202ee350aadf4d1a79001463fa342684687f4bc790638a0",
            "app.providers.ai_researcher_provider:AIResearcherProvider",
            "app.providers.ai_researcher_provider:AIResearcherProvider",
        ),
        (
            "probe.ai.researcher.current_news",
            "AI_RESEARCHER",
            "canonical_url,lineage,published_at,publisher,title",
            "b29e10d8dda4abd7c2c24622bca2c1d8595da55460c659e6bd4ceff9c1d34a4a",
            "app.providers.ai_researcher_provider:AIResearcherProvider",
            "app.providers.ai_researcher_provider:AIResearcherProvider",
        ),
        (
            "probe.ai.codex_cli_research.profiles",
            "CODEX_CLI_RESEARCH_BACKEND",
            (
                "actual_eps,actual_revenue,advance_decline_ratio,advancers,"
                "as_of,atm_implied_volatility,authority,call_skew,call_volume,"
                "canonical_url,change,change_percent,consensus,constituents,content,"
                "contract,coverage_ratio,current_market_context,current_news,"
                "decision_at,decliners,dispersion,"
                "divergence_status,dominant_expirations,down_volume,eps_surprise,"
                "earnings_schedule,equity_put_call_ratio,"
                "estimated_gamma_concentration,"
                "estimated_gamma_exposure,event_at,event_end_at,event_start_at,"
                "event_type,expected_eps,expected_revenue,filing_type,forecast,"
                "guidance_direction,guidance_summary,highest_open_interest_strikes,"
                "highest_volume_strikes,holdings,index_put_call_ratio,instrument,"
                "issuer,issuer_announcement,leadership_concentration,lifecycle,"
                "management_commentary,mnq_relevance,net_position,new_highs,"
                "new_lows,next_refresh_at,outcome,"
                "percent_above_open,percent_above_previous_close,"
                "percent_above_vwap,previous,previous_revised,published_at,put_call,put_skew,"
                "put_volume,"
                "qqq_put_call_ratio,relationship_to_mnq,release_at,report_date,"
                "revenue_surprise,scheduled_event,semiconductor_breadth,skew,"
                "symbol_or_series,"
                "term_structure,ticker,timeframe,timing_status,"
                "total_put_call_ratio,transcript_url,unchanged,"
                "up_down_volume_ratio,up_volume,value,verified_corporate_metric,"
                "verified_market_metric,vix,vvix"
            ),
            "1611425b2a8b626714aabd757d8553e5e134d2f698d4122135a58bb21bdff384",
            (
                "app.services.ai_research_job_executor:"
                "PersistentAIJobExecutor"
            ),
            (
                "app.services.ai_research_job_executor:"
                "PersistentAIJobExecutor;"
                "app.services.research_source_gateway:ResearchSourceGateway;"
                "app.services.evidence_verification_service:"
                "DeterministicEvidenceVerifier;"
                "app.services.agentic_research_runtime:AgenticResearchRuntime;"
                "app.services.ai_research_worker:AIResearchWorker"
            ),
        ),
        (
            "probe.ai.openai_responses.profiles",
            "OPENAI_RESPONSES_RESEARCH",
            (
                "actual_eps,actual_revenue,advance_decline_ratio,advancers,"
                "as_of,atm_implied_volatility,authority,call_skew,call_volume,"
                "canonical_url,change,change_percent,consensus,constituents,content,"
                "contract,coverage_ratio,current_market_context,current_news,"
                "decision_at,decliners,dispersion,"
                "divergence_status,dominant_expirations,down_volume,eps_surprise,"
                "earnings_schedule,equity_put_call_ratio,"
                "estimated_gamma_concentration,"
                "estimated_gamma_exposure,event_at,event_end_at,event_start_at,"
                "event_type,expected_eps,expected_revenue,filing_type,forecast,"
                "guidance_direction,guidance_summary,highest_open_interest_strikes,"
                "highest_volume_strikes,holdings,index_put_call_ratio,instrument,"
                "issuer,issuer_announcement,leadership_concentration,lifecycle,"
                "management_commentary,mnq_relevance,net_position,new_highs,"
                "new_lows,next_refresh_at,outcome,"
                "percent_above_open,percent_above_previous_close,"
                "percent_above_vwap,previous,previous_revised,published_at,put_call,put_skew,"
                "put_volume,"
                "qqq_put_call_ratio,relationship_to_mnq,release_at,report_date,"
                "revenue_surprise,scheduled_event,semiconductor_breadth,skew,"
                "symbol_or_series,"
                "term_structure,ticker,timeframe,timing_status,"
                "total_put_call_ratio,transcript_url,unchanged,"
                "up_down_volume_ratio,up_volume,value,verified_corporate_metric,"
                "verified_market_metric,vix,vvix"
            ),
            "1611425b2a8b626714aabd757d8553e5e134d2f698d4122135a58bb21bdff384",
            "app.services.research_backend:OpenAIResponsesResearchBackend",
            (
                "app.services.research_backend:OpenAIResponsesResearchBackend;"
                "app.services.research_source_gateway:ResearchSourceGateway;"
                "app.services.evidence_verification_service:"
                "DeterministicEvidenceVerifier;"
                "app.services.agentic_research_runtime:AgenticResearchRuntime;"
                "app.services.ai_research_worker:AIResearchWorker"
            ),
        ),
        (
            "probe.repository.provider_cache",
            "PROVIDER_CACHE_REPOSITORY",
            "checksum,created_at,payload,stale_until,status,updated_at,valid_until",
            "c49a1e29b0716692ffc42f6eaeaa4dcf8e6dcff9f7a0978d9beff0c65a61403e",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.infrastructure.persistence.provider_cache_repository:ProviderCacheRepository",
        ),
        (
            "probe.repository.market_facts",
            "MARKET_FACT_REPOSITORY",
            "data_as_of,fact_key,fact_type,lineage,next_refresh_at,release_at,valid_until,value",
            "432333bc5fe9fdd0f47985ac7008870a9c3ed98772aac042145aed86f4891fc2",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.market_fact_repository:MarketFactRepository",
        ),
        (
            "probe.repository.market_news",
            "MARKET_NEWS_REPOSITORY",
            "canonical_url,lineage,published_at,publisher,valid_until",
            "a51405803b322a575719c7f90e5aa851b416addf1211cd49f9429219205b0471",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.market_news_repository:MarketNewsRepository",
        ),
        (
            "probe.repository.fed_expectations",
            "FED_EXPECTATIONS_REPOSITORY",
            "data_as_of,lineage,meeting_date,probabilities,valid_until",
            "4f0d3016756335cb4903eb9c3376c4c363e927411b4092e9bbb4c3cea74c4e9d",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.fed_expectations_repository:FedExpectationsRepository",
        ),
        (
            "probe.repository.risk_context",
            "RISK_CONTEXT_REPOSITORY",
            "data_as_of,lineage,risk_score,skew,valid_until,vvix",
            "1333fa6741f434e2217e1e88192ddcf1766656daf76b1074e49cdb28f2bd2644",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.risk_context_repository:RiskContextHistoryRepository",
        ),
        (
            "probe.repository.event_calendar_coverage",
            "EVENT_CALENDAR_COVERAGE_REPOSITORY",
            "checked_at,country,provider,status,window_end,window_start",
            "fbc0633ba1cee690fed1b4ce7248c18e36dea854ea7d525fa1d8b00751f0ab20",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.event_calendar_coverage_repository:EventCalendarCoverageRepository",
        ),
        (
            "probe.repository.market_context_snapshot",
            "MARKET_CONTEXT_SNAPSHOT_REPOSITORY",
            "checksum,generated_at,snapshot_id,snapshot_revision",
            "22b77941201d71d60d852994a4f678e8b4c403c4f3cfe87c2a8e121f33d9b7d9",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.market_context_snapshot_repository:MarketContextSnapshotRepository",
        ),
        (
            "probe.transform.official_actual",
            "OFFICIAL_ACTUAL_TRANSFORMATION",
            "actual,frequency,lineage,metric_id,reference_period,transformation",
            "7c7f07372008052cf109b996fc6ef9f1422236c9b2b46c03dac0139d44e5f39c",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.official_actual_semantics:derive_official_actual",
        ),
        (
            "probe.transform.macro_consensus",
            "MACRO_CONSENSUS_RECONCILIATION",
            "consensus,lineage,occurrence_id,previous,reference_period",
            "bfc60f409f7fb47512d4cb1dcd73f5badfe6a46d3513cebcb6fbd0257e636ec3",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.macro_consensus_service:merge_consensus_provider_payloads",
        ),
        (
            "probe.transform.provider_force_actual",
            "PROVIDER_FORCE_ACTUAL_RECONCILIATION",
            "actual,consensus,lineage,occurrence_id,previous,reference_period",
            "1d9f0763329fea25873084dbdc512c4a5864b714a824eb5c28d74d91a1e99c92",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.provider_force_actual_reconciliation_service:ProviderForceActualReconciliationService",
        ),
        (
            "probe.transform.request_accounting",
            "REQUEST_PROVIDER_ACCOUNTING",
            "correlation_id,database_lookup,provider_attempts,reason_code,request_id,selected_source",
            "221004922443eb006b4d99612f3d9776fe1456cfd4ad3cc2b5f88f205309a1ae",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.request_provider_accounting:RequestProviderAccountingCollector",
        ),
        (
            "probe.transform.senior_analyst_projection",
            "SENIOR_ANALYST_PROJECTION",
            "analytics,missing_data,provider_accounting,readiness",
            "196aad0cafe745226c21650221ced010c0bc7dff521fe688762c43088da8eca3",
            LOCAL_PROBE_ADAPTER_PATH,
            "app.services.senior_analyst_projection_v1:build_senior_analyst_payload_v1",
        ),
    )


_AUDIT_ONLY_FIELD_CONTRACTS = MappingProxyType(
    {
        "probe.ai.researcher.macro_calendar": frozenset(
            {
                "actual",
                "previous_revised",
                "previous_revised_lineage",
            }
        ),
    }
)
_TERMINAL_AUDIT_CONTRACTS: Mapping[str, str] = MappingProxyType({})
_SUPPORTING_SOURCE_OWNERS = MappingProxyType(
    {
        path: frozenset(
            {
                "CODEX_CLI_RESEARCH_BACKEND",
                "OPENAI_RESPONSES_RESEARCH",
            }
        )
        for path in (
            "app.services.research_source_gateway:ResearchSourceGateway",
            (
                "app.services.evidence_verification_service:"
                "DeterministicEvidenceVerifier"
            ),
            "app.services.agentic_research_runtime:AgenticResearchRuntime",
            "app.services.ai_research_worker:AIResearchWorker",
        )
    }
)
PROBE_OUTPUT_CONTRACTS: Mapping[str, ProbeOutputContract] = MappingProxyType(
    {
        probe_id: ProbeOutputContract(
            provider_id=provider_id,
            normalized_fields=frozenset(fields_csv.split(",")),
            capability_schema_sha256=capability_schema_sha256,
            probe_adapter_paths=_split(probe_adapter_paths),
            source_adapter_paths=_split(source_adapter_paths),
            audit_only_fields=_AUDIT_ONLY_FIELD_CONTRACTS.get(
                probe_id,
                frozenset(),
            ),
            terminal_audit_reason=_TERMINAL_AUDIT_CONTRACTS.get(probe_id),
        )
        for (
            probe_id,
            provider_id,
            fields_csv,
            capability_schema_sha256,
            probe_adapter_paths,
            source_adapter_paths,
        ) in _contract_rows()
    }
)


def capability_schema_sha256(capabilities: Iterable[Any]) -> str:
    payload = []
    for capability in sorted(
        capabilities,
        key=lambda item: (
            str(item.dataset_id),
            str(item.metric_id),
            tuple(item.supported_fields),
            tuple(getattr(item, "audit_only_fields", ())),
        ),
    ):
        row = {
            "dataset_id": str(capability.dataset_id),
            "metric_id": str(capability.metric_id),
            "supported_fields": sorted(
                str(field_name) for field_name in capability.supported_fields
            ),
        }
        audit_only_fields = tuple(
            getattr(capability, "audit_only_fields", ()) or ()
        )
        if audit_only_fields:
            row["audit_only_fields"] = sorted(
                str(field_name) for field_name in audit_only_fields
            )
        payload.append(row)
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_capability_output_contracts(
    providers: Sequence[Any],
    *,
    contracts: Mapping[str, ProbeOutputContract] | None = None,
) -> tuple[str, ...]:
    selected_contracts = contracts if contracts is not None else PROBE_OUTPUT_CONTRACTS
    errors: list[str] = []
    registered_probes: set[str] = set()
    provider_contract_sources: dict[str, set[str]] = defaultdict(set)

    for provider in providers:
        grouped: dict[str, list[Any]] = defaultdict(list)
        for capability in provider.capabilities:
            grouped[str(capability.probe_id)].append(capability)
            supported_fields = tuple(capability.supported_fields)
            audit_only_fields = tuple(
                getattr(capability, "audit_only_fields", ()) or ()
            )
            identity = (
                f"{provider.provider_id}:{capability.dataset_id}:"
                f"{capability.metric_id}"
            )
            if len(supported_fields) != len(set(supported_fields)):
                errors.append(
                    f"capability_supported_fields_duplicated:{identity}"
                )
            if len(audit_only_fields) != len(set(audit_only_fields)):
                errors.append(
                    f"capability_audit_only_fields_duplicated:{identity}"
                )
            if set(supported_fields) & set(audit_only_fields):
                errors.append(
                    f"capability_audit_only_fields_overlap:{identity}"
                )
        expected_provider_sources = {
            str(provider.adapter_path),
            *(str(path) for path in provider.additional_adapter_paths),
            *(
                str(capability.probe_adapter_path)
                for capability in provider.capabilities
                if capability.probe_adapter_path
                and capability.probe_adapter_path != LOCAL_PROBE_ADAPTER_PATH
            ),
            *(
                path
                for path, owners in _SUPPORTING_SOURCE_OWNERS.items()
                if str(provider.provider_id) in owners
            ),
        }
        for probe_id, capabilities in grouped.items():
            registered_probes.add(probe_id)
            contract = selected_contracts.get(probe_id)
            if contract is None:
                errors.append(
                    f"capability_output_contract_missing:{provider.provider_id}:{probe_id}"
                )
                continue
            provider_contract_sources[str(provider.provider_id)].update(
                contract.source_adapter_paths
            )
            if contract.provider_id != provider.provider_id:
                errors.append(
                    "capability_output_contract_provider_mismatch:"
                    f"{provider.provider_id}:{probe_id}:"
                    f"{contract.provider_id}"
                )
            registered_fields = frozenset(
                str(field_name)
                for capability in capabilities
                for field_name in capability.supported_fields
            )
            if registered_fields != contract.normalized_fields:
                errors.append(
                    f"capability_output_contract_fields_mismatch:{provider.provider_id}:{probe_id}"
                )
            registered_audit_only_fields = frozenset(
                str(field_name)
                for capability in capabilities
                for field_name in (
                    getattr(capability, "audit_only_fields", ()) or ()
                )
            )
            if (
                registered_audit_only_fields
                != contract.audit_only_fields
            ):
                errors.append(
                    "capability_output_contract_audit_only_fields_mismatch:"
                    f"{provider.provider_id}:{probe_id}"
                )
            if (
                str(getattr(provider, "terminal_audit_reason", "") or "")
                != str(contract.terminal_audit_reason or "")
            ):
                errors.append(
                    "capability_output_contract_terminal_semantics_mismatch:"
                    f"{provider.provider_id}:{probe_id}"
                )
            digest = capability_schema_sha256(capabilities)
            if digest != contract.capability_schema_sha256:
                errors.append(
                    f"capability_output_contract_schema_mismatch:{provider.provider_id}:{probe_id}"
                )
            effective_probe_paths = tuple(
                sorted(
                    {
                        str(capability.probe_adapter_path or provider.adapter_path)
                        for capability in capabilities
                    }
                )
            )
            if effective_probe_paths != tuple(sorted(contract.probe_adapter_paths)):
                errors.append(
                    "capability_output_contract_probe_adapter_mismatch:"
                    f"{provider.provider_id}:{probe_id}"
                )
            if (
                not contract.normalized_fields
                or not contract.probe_adapter_paths
                or not contract.source_adapter_paths
                or len(contract.capability_schema_sha256) != 64
            ):
                errors.append(
                    f"capability_output_contract_incomplete:{provider.provider_id}:{probe_id}"
                )
        if provider_contract_sources[str(provider.provider_id)] != expected_provider_sources:
            errors.append(f"provider_source_adapter_contract_mismatch:{provider.provider_id}")

    for probe_id in sorted(set(selected_contracts) - registered_probes):
        errors.append(f"orphan_capability_output_contract:{probe_id}")
    return tuple(errors)


_NON_SOURCE_REPOSITORIES = frozenset(
    {
        "app.services.ai_research_job_repository:AIResearchJobRepository",
        "app.services.enrichment_run_repository:EnrichmentRunRepository",
        "app.services.provider_observation_repository:ProviderObservationRepository",
        "app.services.research_runtime_repository:ResearchRuntimeRepository",
    }
)


def discover_capability_source_adapter_paths() -> tuple[str, ...]:
    """Discover source adapters by bounded source-role conventions."""

    discovered = set(_discover_provider_classes())
    discovered.update(_discover_repository_classes())
    discovered.update(_discover_ai_backends())
    discovered.update(_discover_service_sources())
    discovered.update(_discover_ai_runtime_components())
    return tuple(sorted(discovered))


def _discover_provider_classes() -> set[str]:
    import app.providers as provider_package

    excluded = {
        "app.providers.base:BaseProvider",
        "app.providers.event_enrichment:CalendarEnrichmentProvider",
        "app.providers.event_enrichment:BrowserCalendarEnrichmentProvider",
    }
    return (
        _discover_classes(
            provider_package,
            class_predicate=lambda name, _: name.endswith("Provider"),
        )
        - excluded
    )


def _discover_repository_classes() -> set[str]:
    import app.infrastructure.persistence as persistence_package
    import app.services as services_package

    discovered: set[str] = set()
    for package in (services_package, persistence_package):
        discovered.update(
            _discover_classes(
                package,
                module_predicate=lambda name: name.endswith("_repository"),
                class_predicate=lambda name, _: name.endswith("Repository"),
            )
        )
    return discovered - _NON_SOURCE_REPOSITORIES


def _discover_ai_backends() -> set[str]:
    module = importlib.import_module("app.services.research_backend")
    discovered = {
        f"{module.__name__}:{value.__qualname__}"
        for name, value in inspect.getmembers(module, inspect.isclass)
        if (
            name.endswith("Backend")
            and name != "ResearchBackend"
            and value.__module__ == module.__name__
        )
    }
    executor_module = importlib.import_module(
        "app.services.ai_research_job_executor"
    )
    executor = getattr(executor_module, "PersistentAIJobExecutor")
    discovered.add(
        f"{executor_module.__name__}:{executor.__qualname__}"
    )
    return discovered


def _discover_service_sources() -> set[str]:
    rules = (
        (
            "app.services.official_actual_semantics",
            lambda name, value: inspect.isfunction(value) and name.startswith("derive_"),
        ),
        (
            "app.services.macro_consensus_service",
            lambda name, value: inspect.isfunction(value) and name.startswith("merge_"),
        ),
        (
            "app.services.provider_force_actual_reconciliation_service",
            lambda name, value: inspect.isclass(value) and name.endswith("ReconciliationService"),
        ),
        (
            "app.services.request_provider_accounting",
            lambda name, value: inspect.isclass(value) and name.endswith("AccountingCollector"),
        ),
        (
            "app.services.senior_analyst_projection_v1",
            lambda name, value: (
                inspect.isfunction(value) and name.startswith("build_senior_analyst_")
            ),
        ),
    )
    discovered: set[str] = set()
    for module_name, predicate in rules:
        module = importlib.import_module(module_name)
        discovered.update(
            f"{module.__name__}:{value.__qualname__}"
            for name, value in inspect.getmembers(module)
            if predicate(name, value) and getattr(value, "__module__", None) == module.__name__
        )
    return discovered


def _discover_ai_runtime_components() -> set[str]:
    rules = (
        ("app.services.research_source_gateway", "Gateway"),
        ("app.services.evidence_verification_service", "Verifier"),
        ("app.services.agentic_research_runtime", "Runtime"),
        ("app.services.ai_research_worker", "Worker"),
    )
    discovered: set[str] = set()
    for module_name, suffix in rules:
        module = importlib.import_module(module_name)
        discovered.update(
            f"{module.__name__}:{value.__qualname__}"
            for name, value in inspect.getmembers(module, inspect.isclass)
            if name.endswith(suffix) and value.__module__ == module.__name__
        )
    return discovered


def _discover_classes(
    package: Any,
    *,
    class_predicate: Any,
    module_predicate: Any = lambda _: True,
) -> set[str]:
    discovered: set[str] = set()
    prefix = f"{package.__name__}."
    for module_info in pkgutil.walk_packages(package.__path__, prefix=prefix):
        if not module_predicate(module_info.name):
            continue
        module = importlib.import_module(module_info.name)
        discovered.update(
            f"{module.__name__}:{value.__qualname__}"
            for name, value in inspect.getmembers(module, inspect.isclass)
            if class_predicate(name, value) and value.__module__ == module.__name__
        )
    return discovered


__all__ = [
    "LOCAL_PROBE_ADAPTER_PATH",
    "PROBE_OUTPUT_CONTRACTS",
    "ProbeOutputContract",
    "capability_schema_sha256",
    "discover_capability_source_adapter_paths",
    "validate_capability_output_contracts",
]
