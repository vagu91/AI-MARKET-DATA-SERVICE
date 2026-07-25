from __future__ import annotations

from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.bea import BeaProvider
from app.providers.bea_calendar import BeaReleaseScheduleProvider
from app.providers.bls import BlsProvider
from app.providers.bls_calendar import BlsReleaseCalendarProvider
from app.providers.earnings_provider import EarningsProvider
from app.providers.event_enrichment import (
    DailyFxEnrichmentProvider,
    FXStreetEconomicCalendarProvider,
    ForexFactoryEnrichmentProvider,
    GenericSearchSnippetCalendarProvider,
    InvestingEnrichmentProvider,
    ManualEventEnrichmentProvider,
    MarketWatchEconomicCalendarProvider,
    OpenAIEventEnrichmentProvider,
    PlaywrightDailyFXProvider,
    PlaywrightForexFactoryProvider,
    PlaywrightInvestingProvider,
    TargetedSearchEventEnrichmentProvider,
    YahooEconomicCalendarProvider,
)
from app.providers.fed_calendar import FederalReserveCalendarProvider
from app.providers.federal_reserve import FederalReserveRssProvider
from app.providers.fred import FredProvider
from app.providers.census import CensusProvider
from app.providers.finnhub import FinnhubProvider
from app.providers.tradier import TradierProvider
from app.providers.mega_cap_snapshot_provider import MegaCapSnapshotProvider
from app.providers.news_provider import NewsProvider
from app.providers.qqq_holdings_provider import QQQHoldingsProvider
from app.providers.cftc_cot_provider import CftcCotProvider
from app.providers.cboe_put_call_provider import CboePutCallProvider
from app.providers.cboe_risk_indices_provider import CboeRiskIndicesProvider
from app.providers.cboe_vix_futures_provider import CboeVixFuturesProvider
from app.providers.scraper_calendar import EconomicCalendarScraperProvider
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.event_enrichment_service import EventEnrichmentService
from app.services.event_service import EventService
from app.services.event_window_service import EventWindowService
from app.services.macro_service import MacroService
from app.services.market_fact_repository import init_market_db
from app.services.market_news_repository import MarketNewsRepository
from app.services.nasdaq_data_service import NasdaqDataService
from app.services.ai_research_worker import AIResearchWorker
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.temporal_validation_service import TemporalValidationService
from app.infrastructure.persistence.database_safety import assert_test_database_isolated
from app.services.lifecycle_due_resolver import (
    DeterministicLifecycleDueResolver,
    existing_lifecycle_provider_adapters,
)
from app.services.deterministic_actual_resolver import (
    DeterministicActualResolver,
)
from app.services.research_agent_enablement import validate_research_agent_mapping
from app.services.deterministic_provider_runtime_service import (
    DeterministicProviderRuntimeService,
)


def build_application_state(
    settings: Settings,
    *,
    deterministic_provider_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validate_research_agent_mapping()
    assert_test_database_isolated(
        settings.database_path,
        environment=settings.environment,
    )
    cache = ProviderCacheRepository(settings.database_path)
    init_market_db(settings)

    deterministic_providers = {
        "fred": FredProvider(cache, settings),
        "bls": BlsProvider(cache, settings),
        "bea": BeaProvider(cache, settings),
        "census": CensusProvider(cache, settings),
        "finnhub": FinnhubProvider(cache, settings),
        "tradier": TradierProvider(cache, settings),
    }
    deterministic_providers.update(deterministic_provider_overrides or {})
    macro_providers = [
        deterministic_providers["fred"],
        deterministic_providers["bls"],
        deterministic_providers["bea"],
    ]
    macro_service = MacroService(providers=macro_providers)
    census_provider = deterministic_providers["census"]
    event_enrichment_service = EventEnrichmentService(
        cache=cache,
        providers=[
            DailyFxEnrichmentProvider(settings),
            ForexFactoryEnrichmentProvider(settings),
            InvestingEnrichmentProvider(settings),
            FXStreetEconomicCalendarProvider(settings),
            MarketWatchEconomicCalendarProvider(settings),
            YahooEconomicCalendarProvider(settings),
            GenericSearchSnippetCalendarProvider(settings),
            PlaywrightDailyFXProvider(settings),
            PlaywrightForexFactoryProvider(settings),
            PlaywrightInvestingProvider(settings),
            TargetedSearchEventEnrichmentProvider(settings),
            ManualEventEnrichmentProvider(settings),
            OpenAIEventEnrichmentProvider(settings),
        ],
    )
    temporal_validation = TemporalValidationService(settings)
    event_service = EventService(
        providers=[
            FederalReserveCalendarProvider(cache, settings),
            FederalReserveRssProvider(cache, settings),
            BlsReleaseCalendarProvider(cache, settings),
            BeaReleaseScheduleProvider(cache, settings),
            EconomicCalendarScraperProvider(cache, settings),
        ],
        enrichment_service=event_enrichment_service,
        temporal_validation=temporal_validation,
    )
    market_news_repository = MarketNewsRepository(settings)
    nasdaq_data_service = NasdaqDataService(
        qqq_holdings_provider=QQQHoldingsProvider(cache, settings),
        mega_cap_snapshot_provider=MegaCapSnapshotProvider(cache, settings),
        earnings_provider=EarningsProvider(cache, settings),
        news_provider=NewsProvider(cache, settings, market_news_repository=market_news_repository),
    )
    enrichment_orchestrator = EnrichmentOrchestrator(
        settings,
        event_enrichment_service=event_enrichment_service,
    )
    ai_job_repository = AIResearchJobRepository(settings)
    market_context_snapshots = MarketContextSnapshotRepository(settings)
    ai_research_worker = AIResearchWorker(
        settings,
        repository=ai_job_repository,
        snapshots=market_context_snapshots,
    )
    deterministic_provider_runtime = DeterministicProviderRuntimeService(
        settings,
        providers=deterministic_providers,
        cache=cache,
    )
    research_scheduler = ResearchSchedulerService(
        settings,
        deterministic_runtime=deterministic_provider_runtime,
    )
    official_actual_resolver = DeterministicActualResolver(
        settings,
        providers={
            getattr(provider, "source", ""): provider
            for provider in [*macro_providers, census_provider]
            if getattr(provider, "source", "") in {"BLS", "BEA", "CENSUS"}
        },
    )
    lifecycle_due_resolver = DeterministicLifecycleDueResolver(
        settings,
        adapters=existing_lifecycle_provider_adapters(
            macro_service=macro_service,
            event_service=event_service,
            nasdaq_data_service=nasdaq_data_service,
            settings=settings,
            official_actual_resolver=official_actual_resolver,
            cftc_provider=CftcCotProvider(settings),
            cboe_risk_indices_provider=CboeRiskIndicesProvider(settings),
            cboe_vix_futures_provider=CboeVixFuturesProvider(settings),
            cboe_put_call_provider=CboePutCallProvider(settings),
        ),
    )

    return {
        "settings": settings,
        "cache": cache,
        "macro_service": macro_service,
        "deterministic_providers": deterministic_providers,
        "deterministic_provider_runtime": deterministic_provider_runtime,
        "event_service": event_service,
        "event_enrichment_service": event_enrichment_service,
        "event_window_service": EventWindowService(event_service),
        "nasdaq_data_service": nasdaq_data_service,
        "enrichment_orchestrator": enrichment_orchestrator,
        "market_news_repository": market_news_repository,
        "ai_job_repository": ai_job_repository,
        "market_context_snapshots": market_context_snapshots,
        "ai_research_worker": ai_research_worker,
        "research_scheduler": research_scheduler,
        "lifecycle_due_resolver": lifecycle_due_resolver,
    }
