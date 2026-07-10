from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.api.routes import router
from app.core.cache import SQLiteCache
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.providers.bea import BeaProvider
from app.providers.bea_calendar import BeaReleaseScheduleProvider
from app.providers.bls import BlsProvider
from app.providers.bls_calendar import BlsReleaseCalendarProvider
from app.providers.fed_calendar import FederalReserveCalendarProvider
from app.providers.federal_reserve import FederalReserveRssProvider
from app.providers.fred import FredProvider
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
from app.providers.mega_cap_snapshot_provider import MegaCapSnapshotProvider
from app.providers.news_provider import NewsProvider
from app.providers.qqq_holdings_provider import QQQHoldingsProvider
from app.providers.scraper_calendar import EconomicCalendarScraperProvider
from app.services.event_window_service import EventWindowService
from app.services.event_enrichment_service import EventEnrichmentService
from app.services.event_service import EventService
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.macro_service import MacroService
from app.services.market_fact_repository import init_market_db
from app.services.market_news_repository import MarketNewsRepository
from app.services.nasdaq_data_service import NasdaqDataService


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    cache = SQLiteCache(settings.database_path)
    init_market_db(settings)

    macro_service = MacroService(
        providers=[
            FredProvider(cache, settings),
            BlsProvider(cache, settings),
            BeaProvider(cache, settings),
        ]
    )
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
    event_service = EventService(
        providers=[
            FederalReserveCalendarProvider(cache, settings),
            FederalReserveRssProvider(cache, settings),
            BlsReleaseCalendarProvider(cache, settings),
            BeaReleaseScheduleProvider(cache, settings),
            EconomicCalendarScraperProvider(cache, settings),
        ],
        enrichment_service=event_enrichment_service,
    )
    event_window_service = EventWindowService(event_service)
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

    app.state.settings = settings
    app.state.cache = cache
    app.state.macro_service = macro_service
    app.state.event_service = event_service
    app.state.event_enrichment_service = event_enrichment_service
    app.state.event_window_service = event_window_service
    app.state.nasdaq_data_service = nasdaq_data_service
    app.state.enrichment_orchestrator = enrichment_orchestrator
    app.state.market_news_repository = market_news_repository

    scheduler = None
    if settings.enable_scheduler:
        scheduler = AsyncIOScheduler(timezone=settings.timezone)
        scheduler.add_job(macro_service.latest, "interval", minutes=30, id="macro_latest")
        scheduler.add_job(event_service.upcoming, "interval", minutes=15, id="events_upcoming")
        scheduler.start()
        app.state.scheduler = scheduler

    yield

    if scheduler:
        scheduler.shutdown(wait=False)


app = FastAPI(
    title="AI-MARKET-DATA-SERVICE",
    version="0.1.0",
    description="Normalized macro and economic event data service for AI-TRADER.",
    lifespan=lifespan,
)
app.include_router(router)
