from __future__ import annotations

from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.investing_flash_services_pmi import (
    SOURCE as INVESTING_FLASH_SOURCE,
)
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
from app.services.market_context_sync_refresh_worker import (
    MarketContextSyncRefreshWorker,
)
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
from app.services.provider_adapter_factory import create_registered_adapter
from app.services.provider_capability_registry import (
    event_enrichment_runtime_adapter_bindings,
    event_runtime_adapter_bindings,
    macro_runtime_provider_ids,
    validate_registry,
)


def build_application_state(
    settings: Settings,
    *,
    deterministic_provider_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # One fail-fast registry governs runtime policy, audit probes, accounting,
    # and the generated source matrix.
    validate_registry()
    validate_research_agent_mapping()
    assert_test_database_isolated(
        settings.database_path,
        environment=settings.environment,
    )
    cache = ProviderCacheRepository(settings.database_path)
    init_market_db(settings)

    deterministic_providers = {
        "fred": create_registered_adapter("FRED", cache, settings),
        "bls": create_registered_adapter("BLS", cache, settings),
        "bea": create_registered_adapter("BEA", cache, settings),
        "census": create_registered_adapter("CENSUS", cache, settings),
        "spglobal": create_registered_adapter(
            "SPGLOBAL",
            cache,
            settings,
        ),
        "investing_flash_services_pmi": create_registered_adapter(
            INVESTING_FLASH_SOURCE,
            cache,
            settings,
        ),
        "finnhub": create_registered_adapter("FINNHUB", cache, settings),
        "tradier": create_registered_adapter("TRADIER", cache, settings),
    }
    deterministic_providers.update(deterministic_provider_overrides or {})
    macro_providers = [
        deterministic_providers[provider_id.casefold()]
        for provider_id in macro_runtime_provider_ids()
    ]
    macro_service = MacroService(providers=macro_providers)
    census_provider = deterministic_providers["census"]
    event_enrichment_service = EventEnrichmentService(
        cache=cache,
        providers=[
            create_registered_adapter(
                provider_id,
                settings,
                adapter_name=adapter_name,
            )
            for provider_id, adapter_name
            in event_enrichment_runtime_adapter_bindings()
        ],
    )
    temporal_validation = TemporalValidationService(settings)
    event_service = EventService(
        providers=[
            create_registered_adapter(
                provider_id,
                cache,
                settings,
                adapter_name=adapter_name,
            )
            for provider_id, adapter_name in event_runtime_adapter_bindings()
        ],
        enrichment_service=event_enrichment_service,
        temporal_validation=temporal_validation,
    )
    market_news_repository = MarketNewsRepository(settings)
    nasdaq_data_service = NasdaqDataService(
        qqq_holdings_provider=create_registered_adapter(
            "INVESCO",
            cache,
            settings,
        ),
        mega_cap_snapshot_provider=create_registered_adapter(
            "YAHOO_FINANCE_CHART",
            cache,
            settings,
        ),
        earnings_provider=create_registered_adapter(
            "LEGACY_EARNINGS_AGGREGATOR",
            cache,
            settings,
        ),
        news_provider=create_registered_adapter(
            "ALPHA_VANTAGE_NEWS_SENTIMENT",
            cache,
            settings,
            market_news_repository=market_news_repository,
        ),
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
    market_context_sync_refresh_worker = MarketContextSyncRefreshWorker(
        settings,
        deterministic_runtime=deterministic_provider_runtime,
    )
    official_actual_resolver = DeterministicActualResolver(
        settings,
        providers={
            getattr(provider, "source", ""): provider
            for provider in [
                *macro_providers,
                census_provider,
                deterministic_providers["spglobal"],
                deterministic_providers["investing_flash_services_pmi"],
            ]
            if getattr(provider, "source", "")
            in {
                "BLS",
                "BEA",
                "CENSUS",
                "FRED",
                "SPGLOBAL",
                INVESTING_FLASH_SOURCE,
            }
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
            cftc_provider=create_registered_adapter("CFTC", settings),
            cboe_risk_indices_provider=create_registered_adapter(
                "CBOE",
                settings,
            ),
            cboe_vix_futures_provider=create_registered_adapter(
                "CBOE",
                settings,
                adapter_name="CboeVixFuturesProvider",
            ),
            cboe_put_call_provider=create_registered_adapter(
                "CBOE",
                settings,
                adapter_name="CboePutCallProvider",
            ),
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
        "market_context_sync_refresh_worker": market_context_sync_refresh_worker,
        "lifecycle_due_resolver": lifecycle_due_resolver,
        "official_actual_resolver": official_actual_resolver,
    }
