from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.deps import (
    get_enrichment_orchestrator,
    get_event_service,
    get_event_window_service,
    get_macro_service,
    get_nasdaq_data_service,
)
from app.models.events import EconomicEvent
from app.models.ai_jobs import AIResearchEnqueueRequest, MarketResearchRunRequest
from app.models.macro import EventWindowsResponse, MacroLatestResponse
from app.models.nasdaq import (
    EarningsResponse,
    MegaCapBreadthResponse,
    MegaCapSnapshotResponse,
    NewsResponse,
    QQQHoldingsResponse,
)
from app.services.event_window_service import EventWindowService
from app.services.event_service import EventService
from app.services.diagnostics_service import (
    DiagnosticsService,
    _macro_pipeline_status,
    _news_pipeline_status,
    _positioning_context_from_runtime,
    _sentiment_context_from_runtime,
)
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.macro_service import MacroService
from app.services.macro_consensus_service import merge_consensus_provider_payloads
from app.services.market_fact_repository import MarketFactRepository, init_market_db
from app.services.market_context_builder import build_market_context_contract
from app.services.market_context_hardening_service import harden_market_context
from app.services.ai_research_diagnostics import record_final_consumer_events
from app.services.market_news_repository import MarketNewsRepository
from app.services.nasdaq_data_service import NasdaqDataService
from app.services.health_report_service import HealthReportService
from app.services.acquisition_status_service import AcquisitionStatusService
from app.services.credential_audit_service import credential_audit
from app.services.context_extensions_service import enrich_nasdaq_context
from app.services.positioning_runtime_service import PositioningRuntimeService
from app.services.multi_source_runtime_service import MultiSourceRuntimeService, apply_multi_source_context
from app.services.social_sentiment_service import SocialSentimentService
from app.core.logging import logging_rotation_config
from app.infrastructure.persistence.database import database_health
from app.infrastructure.persistence.database_maintenance import analyze_database
from app.infrastructure.persistence.migrations import migrate_database
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.infrastructure.storage_retention import retention_policy_report, storage_health
from app.services.temporal_domain_service import canonical_event_key, reconcile_calendar_events
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.event_value_candidate_repository import EventValueCandidateRepository
from app.services.ai_research_capability_service import AIResearchCapabilityService
from app.services.research_profiles import profile_for_job
from app.services.research_runtime_repository import ResearchRuntimeRepository

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "AI-MARKET-DATA-SERVICE"}


@router.get("/ai-research/jobs/latest")
async def ai_research_jobs_latest(
    limit: int = Query(default=20, ge=1, le=100),
    symbol: str = Query(default="MNQ", min_length=1, max_length=16),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> list[dict[str, object]]:
    return AIResearchJobRepository(enrichment_orchestrator.settings).latest(limit=limit, symbol=symbol)


@router.get("/ai-research/status")
async def ai_research_status(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    repository = AIResearchJobRepository(enrichment_orchestrator.settings)
    return {
        **repository.status(),
        "enrichment": AIResearchJobService(enrichment_orchestrator.settings).enrichment_status("MNQ"),
    }


@router.get("/ai-research/capabilities")
async def ai_research_capabilities(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return AIResearchCapabilityService(enrichment_orchestrator.settings).probe(persist=True)


@router.get("/ai-research/jobs/{job_id}")
async def ai_research_job(
    job_id: str,
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    job = AIResearchJobRepository(enrichment_orchestrator.settings).get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="AI research job not found")
    return job


@router.post("/ai-research/jobs", status_code=202)
async def enqueue_ai_research_job(
    request: AIResearchEnqueueRequest,
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    job, created = AIResearchJobService(enrichment_orchestrator.settings).enqueue_explicit(
        job_type=request.job_type,
        symbol=request.symbol,
        correlation_id=request.correlation_id,
        request_payload=request.request_payload,
        event_key=request.event_key,
        pending_fields=request.pending_fields,
        force=request.force_requeue,
    )
    return {"created": created, "job": job, "trading_actions": "not_supported"}


@router.post("/market-research/mnq/runs", status_code=202)
async def enqueue_mnq_market_research(
    request: MarketResearchRunRequest,
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    settings = enrichment_orchestrator.settings
    latest = MarketContextSnapshotRepository(settings).latest("MNQ")
    context = latest["debug_payload"] if latest else {}
    payload = {
        "database_context": {
            "snapshot_id": latest.get("snapshot_id") if latest else None,
            "snapshot_revision": latest.get("revision") if latest else None,
            "data_as_of": latest.get("data_as_of") if latest else None,
            "event_calendar": context.get("event_calendar") or {},
            "macro_snapshot": context.get("macro_snapshot") or {},
            "nasdaq_context": context.get("nasdaq_context") or {},
            "news_context": context.get("news_context") or {},
        },
        "context_date": (context.get("market_schedule") or {}).get("context_date"),
        "market_session": (context.get("market_schedule") or {}).get("market_session_status"),
        "pending_fields": [],
        "authorized_live_smoke": request.authorized_live_smoke,
    }
    job, created = AIResearchJobService(settings).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH", symbol="MNQ",
        correlation_id=request.correlation_id or f"mnq-research-{datetime.now(UTC).isoformat()}",
        request_payload=payload, pending_fields=[], force=request.force_requeue,
    )
    profile = profile_for_job("MNQ_MARKET_RESEARCH")
    run = ResearchRuntimeRepository(settings).ensure_run(job, profile.profile_id, profile.prompt_version)
    return {
        "created": created, "run_id": run["run_id"], "job_id": job["job_id"],
        "status": run["status"], "trading_actions": "not_supported",
    }


@router.get("/market-research/mnq/runs/{run_id}")
async def mnq_market_research_run(
    run_id: str,
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    run = ResearchRuntimeRepository(enrichment_orchestrator.settings).get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Market research run not found")
    return run


@router.get("/market-research/mnq/latest")
async def mnq_market_research_latest(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    run = ResearchRuntimeRepository(enrichment_orchestrator.settings).latest("MNQ")
    if run is None:
        raise HTTPException(status_code=404, detail="No market research run available")
    return run


@router.get("/market-research/mnq/status")
async def mnq_market_research_status(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    run = ResearchRuntimeRepository(enrichment_orchestrator.settings).latest("MNQ")
    return _research_summary(run)


@router.get("/market-research/mnq/evidence/{claim_id}")
async def mnq_market_research_evidence(
    claim_id: str,
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> list[dict[str, object]]:
    return ResearchRuntimeRepository(enrichment_orchestrator.settings).evidence_for_claim(claim_id)


@router.get("/macro/latest", response_model=MacroLatestResponse)
async def macro_latest(
    macro_service: MacroService = Depends(get_macro_service),
) -> MacroLatestResponse:
    return await macro_service.latest()


@router.get("/events/today", response_model=list[EconomicEvent])
async def events_today(
    country: str = Query(default="US", min_length=2, max_length=8),
    event_service: EventService = Depends(get_event_service),
) -> list[EconomicEvent]:
    return await event_service.today(country=country)


@router.get("/events/upcoming", response_model=list[EconomicEvent])
async def events_upcoming(
    country: str = Query(default="US", min_length=2, max_length=8),
    days: int = Query(default=7, ge=1, le=30),
    event_service: EventService = Depends(get_event_service),
) -> list[EconomicEvent]:
    return await event_service.upcoming(country=country, days=days)


@router.get("/events/enriched/upcoming", response_model=list[EconomicEvent])
async def events_enriched_upcoming(
    country: str = Query(default="US", min_length=2, max_length=8),
    days: int = Query(default=7, ge=1, le=30),
    event_service: EventService = Depends(get_event_service),
) -> list[EconomicEvent]:
    return await event_service.upcoming(country=country, days=days)


@router.get("/events/active-windows", response_model=EventWindowsResponse)
async def events_active_windows(
    symbol: str = Query(default="MNQ", min_length=1, max_length=16),
    event_window_service: EventWindowService = Depends(get_event_window_service),
) -> EventWindowsResponse:
    return await event_window_service.event_windows(symbol=symbol)


@router.get("/market-context/mnq")
async def market_context_mnq(
    refresh: str = Query(default="auto", pattern="^(auto|false|force)$"),
    view: str = Query(default="consumer", pattern="^(consumer|debug)$"),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    settings = enrichment_orchestrator.settings
    snapshots = MarketContextSnapshotRepository(settings)
    if refresh == "false":
        stored = snapshots.latest("MNQ")
        if stored is not None:
            return stored["debug_payload"] if view == "debug" else stored["consumer_payload"]
    diagnostics = DiagnosticsService(
        settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )
    if refresh in {"false", "force"}:
        contract = await diagnostics.full_model(
            country="US",
            days=30,
            symbol="MNQ",
            fetch_missing_nasdaq=refresh == "force",
            refresh=refresh,
        )
        return _materialize_market_context(contract, refresh=refresh, view=view, settings=settings)
    macro, macro_quality = await diagnostics._macro_db_first()
    events_today_data = await event_service.today(country="US")
    now = datetime.now(UTC)
    if hasattr(event_service, "list_events"):
        raw_upcoming = await event_service.list_events(
            country="US",
            start=now,
            end=now + timedelta(days=7),
            enrich=False,
        )
        upcoming, orchestrator_metadata = await enrichment_orchestrator.enrich_events(
            events=raw_upcoming,
            country="US",
            start=now,
            end=now + timedelta(days=7),
            trigger="market_context",
        )
    else:
        upcoming = await event_service.upcoming(country="US", days=7)
        orchestrator_metadata = {
            "data_quality": {},
            "service_role": "data provider only",
        }
    multi_runtime = MultiSourceRuntimeService(enrichment_orchestrator.settings)
    investing_refresh = "auto" if diagnostics.macro_consensus.needs_refresh(upcoming) else "false"
    investing_payload = await multi_runtime.provider("investing_economic_calendar", refresh=investing_refresh)
    xtb_payload = await multi_runtime.provider("xtb_economic_calendar", refresh="auto")
    candidates = EventValueCandidateRepository(settings)
    candidates.persist_provider_payload(investing_payload)
    candidates.persist_provider_payload(xtb_payload)
    upcoming = reconcile_calendar_events(upcoming, [investing_payload, xtb_payload], now=now)
    facts_repository = MarketFactRepository(enrichment_orchestrator.settings)
    for event in upcoming:
        facts_repository.upsert_economic_event(
            event,
            event_key=canonical_event_key(event),
            valid_until=enrichment_orchestrator.freshness.macro_valid_until(event),
        )
    ranked_consensus = merge_consensus_provider_payloads(investing_payload, xtb_payload)
    upcoming, consensus_quality, _ = diagnostics.macro_consensus.enrich_and_persist(
        upcoming,
        ranked_consensus,
        refresh_mode="auto",
    )
    if investing_payload.get("status") == "found":
        multi_runtime.persist_provider_result(
            "investing_economic_calendar",
            investing_payload,
            source="Investing Economic Calendar",
        )
    if xtb_payload.get("status") == "found":
        multi_runtime.persist_provider_result(
            "xtb_economic_calendar",
            xtb_payload,
            source="XTB Economic Calendar",
        )
    event_windows = await event_window_service.event_windows(symbol="MNQ")
    nasdaq_context, nasdaq_quality = await diagnostics._nasdaq_db_first(symbol="MNQ", fetch_missing=False)
    news_items = MarketNewsRepository(enrichment_orchestrator.settings).stored(days=30, limit=100)
    news_context, news_runtime = diagnostics.news_intelligence.materialize(
        news_items,
        refresh_mode="auto",
    )
    news_pipeline = _news_pipeline_status(news_items, materialized=news_context)
    macro_pipeline = _macro_pipeline_status(macro)
    pipeline_integrity = {
        "critical_fetch_completed": True,
        "critical_persistence_completed": True,
        "critical_commits_completed": True,
        "critical_read_back_completed": bool(macro.series) and bool(nasdaq_context) and news_pipeline["committed"],
        "snapshot_materialization_completed": news_pipeline["search_completed"] and bool(macro.series) and bool(nasdaq_context),
        "snapshot_built_from_db": True,
        "partial_response": False,
    }
    positioning_runtime = PositioningRuntimeService(enrichment_orchestrator.settings)
    cot_payload = await positioning_runtime.cot(refresh=refresh)
    aaii_payload = await positioning_runtime.aaii(refresh=refresh)
    quality = {
        **orchestrator_metadata.get("data_quality", {}),
        **consensus_quality,
        "macro": macro_quality,
        "nasdaq": nasdaq_quality,
        "pipeline_integrity": pipeline_integrity,
        "news_pipeline": news_pipeline,
        "news_intelligence": news_runtime,
        "macro_pipeline": macro_pipeline,
    }
    contract = build_market_context_contract(
        symbol="MNQ",
        macro=macro,
        events_today=events_today_data,
        upcoming_events=upcoming,
        event_windows=event_windows,
        nasdaq_context=nasdaq_context,
        news_items=news_items,
        data_quality=quality,
        db_summary=MarketFactRepository(enrichment_orchestrator.settings).db_summary(),
        event_facts=MarketFactRepository(enrichment_orchestrator.settings).search_facts(country="US", limit=500),
        positioning_context=_positioning_context_from_runtime(cot_payload),
        sentiment_context=_sentiment_context_from_runtime(aaii_payload),
        metadata={
            "event_enrichment": event_service.last_enrichment_metadata,
            "persistent_enrichment": orchestrator_metadata,
            "request_refresh_mode": refresh,
        },
        news_context_override=news_context,
    )
    contract["data_quality"]["macro_pipeline"] = _macro_pipeline_status(macro, contract.get("macro_snapshot") or {})
    fed_expectations_payload = await multi_runtime.provider("investing_fed_rate_monitor", refresh="auto")
    multi_source = await multi_runtime.snapshot(
        refresh="false",
        preloaded_blocks={
            "investing_economic_calendar": investing_payload,
            "xtb_economic_calendar": xtb_payload,
            "investing_fed_rate_monitor": fed_expectations_payload,
        },
    )
    apply_multi_source_context(contract, multi_source)
    contract["rates_expectations"] = diagnostics.fed_expectations.snapshot(
        refresh="auto",
        provider_payload=fed_expectations_payload,
        macro_snapshot=contract.get("macro_snapshot") or {},
        event_calendar=contract.get("event_calendar") or {},
        legacy_block=contract.get("rates_expectations") or {},
    )
    risk_context, risk_sentiment = await diagnostics.risk_context.snapshot(
        refresh="auto",
        macro_snapshot=contract.get("macro_snapshot") or {},
        preloaded_risk_indices=(multi_source.get("blocks") or {}).get("cboe_risk_indices") or {},
        preloaded_qqq_options=(multi_source.get("blocks") or {}).get("nasdaq_qqq_options") or {},
        existing_legacy=contract.get("risk_sentiment") or {},
    )
    contract["risk_context"] = risk_context
    contract["risk_sentiment"] = risk_sentiment
    contract["social_sentiment"] = await SocialSentimentService(enrichment_orchestrator.settings).snapshot(refresh=refresh)
    contract = harden_market_context(contract, settings=enrichment_orchestrator.settings)
    return _materialize_market_context(contract, refresh=refresh, view=view, settings=settings)


@router.get("/market-context/mnq/debug")
async def market_context_mnq_debug(
    refresh: str = Query(default="auto", pattern="^(auto|false|force)$"),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await market_context_mnq(
        refresh=refresh,
        view="debug",
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )


@router.get("/market-context/mnq/consumer")
async def market_context_mnq_consumer(
    refresh: str = Query(default="auto", pattern="^(auto|false|force)$"),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await market_context_mnq(
        refresh=refresh,
        view="consumer",
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )


def _materialize_market_context(
    contract: dict[str, object],
    *,
    refresh: str,
    view: str,
    settings,
) -> dict[str, object]:
    snapshots = MarketContextSnapshotRepository(settings)
    event_keys = _context_event_keys(contract)
    ai_enrichment = AIResearchJobService(settings).enrichment_status("MNQ", event_keys=event_keys)
    debug = dict(contract)
    debug["data_as_of"] = debug.get("generated_at_utc") or debug.get("generated_at")
    debug["ai_enrichment"] = ai_enrichment
    debug["research"] = _research_summary(ResearchRuntimeRepository(settings).latest("MNQ"))
    debug = harden_market_context(debug, settings=settings)
    stored = snapshots.save_next(
        symbol="MNQ",
        refresh_mode=refresh,
        debug_payload=debug,
        ai_enrichment=ai_enrichment,
        source_job_id=(ai_enrichment.get("job_ids") or [None])[0],
        job_ids=list(ai_enrichment.get("job_ids") or []),
    )
    consumer = stored["consumer_payload"]
    record_final_consumer_events(
        settings,
        (debug.get("data_quality") or {}).get("ai_diagnostic_artifact_dir"),
        consumer,
    )
    return stored["debug_payload"] if view == "debug" else stored["consumer_payload"]


def _context_event_keys(contract: dict[str, object]) -> list[str]:
    output: set[str] = set()
    calendar = contract.get("event_calendar") if isinstance(contract.get("event_calendar"), dict) else {}
    for section in ("critical_macro_events", "fed_communications", "other_economic_events"):
        for event in calendar.get(section) or []:
            if not isinstance(event, dict):
                continue
            output.add(str(event.get("canonical_event_key") or canonical_event_key(event)))
    return sorted(output)


def _research_summary(run: dict[str, object] | None) -> dict[str, object]:
    if run is None:
        return {
            "status": "NOT_REQUIRED", "run_id": None, "snapshot_id": None,
            "started_at": None, "completed_at": None, "data_as_of": None,
            "fresh_until": None, "coverage_score": 0.0, "required_topics": [],
            "completed_topics": [], "missing_topics": [], "blocking_gaps": [],
            "non_blocking_gaps": [], "claim_count": 0, "evidence_count": 0,
            "source_domains": [], "warnings": [],
        }
    result = run.get("result") if isinstance(run.get("result"), dict) else {}
    accepted = result.get("accepted_claims") or []
    return {
        "status": run.get("status"), "run_id": run.get("run_id"),
        "snapshot_id": result.get("snapshot_id"), "started_at": run.get("started_at"),
        "completed_at": run.get("completed_at"), "data_as_of": run.get("data_as_of"),
        "fresh_until": run.get("fresh_until"), "coverage_score": run.get("coverage_score") or 0.0,
        "required_topics": run.get("required_topics") or [],
        "completed_topics": run.get("completed_topics") or [], "missing_topics": run.get("missing_topics") or [],
        "blocking_gaps": run.get("blocking_gaps") or [], "non_blocking_gaps": run.get("non_blocking_gaps") or [],
        "claim_count": len(accepted), "evidence_count": result.get("evidence_count") or 0,
        "source_domains": run.get("source_domains") or [], "warnings": run.get("warnings") or [],
        "key_verified_drivers": [
            {"claim_id": item.get("claim_id"), "topic": item.get("topic"), "value": item.get("value")}
            for item in accepted[:8]
        ],
        "critical_evidence_references": [item.get("claim_id") for item in accepted[:8]],
    }


@router.get("/db/health")
async def db_health(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    init_market_db(enrichment_orchestrator.settings)
    repository = MarketFactRepository(enrichment_orchestrator.settings)
    settings = enrichment_orchestrator.settings
    health = database_health(settings.database_path)
    return {
        "status": "ok",
        "database_path": str(settings.database_path),
        "single_physical_database": True,
        "service_role": "data provider only",
        "schema_version": health["user_version"],
        "integrity": health["integrity_check"],
        "ai_researcher_enabled": settings.enable_ai_researcher,
        "ai_researcher_mode": settings.ai_researcher_mode,
        "db_summary": repository.db_summary(),
    }


@router.get("/db/health/details")
async def db_health_details(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    settings = enrichment_orchestrator.settings
    migrate_database(settings.database_path)
    health = database_health(settings.database_path)
    return {
        "status": "ok",
        "database_path": str(settings.database_path),
        "single_physical_database": True,
        "file_size": health["file_size"],
        "schema_version": health["user_version"],
        "integrity": health["integrity_check"],
        "journal_mode": health["journal_mode"],
        "foreign_keys": health["foreign_keys"],
        "busy_timeout": health["busy_timeout"],
        "tables": health["tables"],
        "pending_migrations": health["pending_migrations"],
        "cache_stats": ProviderCacheRepository(settings.database_path).stats(),
        "canonical_stats": MarketFactRepository(settings).db_summary(),
        "service_role": "data provider only",
    }


@router.get("/db/schema-version")
async def db_schema_version(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    settings = enrichment_orchestrator.settings
    migration = migrate_database(settings.database_path)
    return {
        "database_path": str(settings.database_path),
        "single_physical_database": True,
        "schema": migration,
        "service_role": "data provider only",
    }


@router.get("/db/cache/stats")
async def db_cache_stats(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    settings = enrichment_orchestrator.settings
    repository = ProviderCacheRepository(settings.database_path)
    return {
        "database_path": str(settings.database_path),
        "single_physical_database": True,
        "stats": repository.stats(),
        "service_role": "data provider only",
    }


@router.get("/storage/health")
async def storage_health_endpoint(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    settings = enrichment_orchestrator.settings
    health = storage_health(settings)
    return {
        **health,
        "database_maintenance": {
            "enabled": True,
            "analysis": analyze_database(settings),
        },
        "service_role": "data provider only",
    }


@router.get("/storage/retention-policy")
async def storage_retention_policy(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    settings = enrichment_orchestrator.settings
    return {
        "status": "ok",
        "storage_retention_enabled": True,
        "policies": retention_policy_report(settings),
        "log_rotation": logging_rotation_config(settings),
        "database_maintenance": {
            "provider_observations_retention_days": settings.provider_observations_retention_days,
            "enrichment_runs_retention_days": settings.enrichment_runs_retention_days,
            "expired_cache_retention_days": settings.expired_cache_retention_days,
            "market_news_retention_days": settings.market_news_retention_days,
            "market_facts_retention_days": settings.market_facts_retention_days,
            "economic_events_history_retention_days": settings.economic_events_history_retention_days,
            "snapshot_history_retention_days": settings.snapshot_history_retention_days,
            "vacuum_manual_or_scheduled": True,
        },
        "service_role": "data provider only",
    }


@router.get("/facts/lookup")
async def facts_lookup(
    fact_key: str,
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    fact = MarketFactRepository(enrichment_orchestrator.settings).get_fact(fact_key)
    return {"fact_key": fact_key, "found": fact is not None, "fact": fact, "service_role": "data provider only"}


@router.get("/facts/search")
async def facts_search(
    country: str | None = Query(default=None),
    category: str | None = Query(default=None),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    facts = MarketFactRepository(enrichment_orchestrator.settings).search_facts(country=country, category=category)
    return {"count": len(facts), "facts": facts, "service_role": "data provider only"}


@router.get("/facts/stale")
async def facts_stale(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    facts = MarketFactRepository(enrichment_orchestrator.settings).stale_facts()
    return {"count": len(facts), "facts": facts, "service_role": "data provider only"}


@router.get("/facts/coverage")
async def facts_coverage(
    country: str = Query(default="US", min_length=2, max_length=8),
    days: int = Query(default=30, ge=1, le=365),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    coverage = MarketFactRepository(enrichment_orchestrator.settings).coverage(country=country, days=days)
    coverage["service_role"] = "data provider only"
    return coverage


@router.get("/enrichment/run/status")
async def enrichment_run_status(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return {
        "latest_run": enrichment_orchestrator.runs.latest(),
        "service_role": "data provider only",
    }


@router.post("/enrichment/run")
async def enrichment_run(
    country: str = Query(default="US", min_length=2, max_length=8),
    days: int = Query(default=30, ge=1, le=365),
    event_service: EventService = Depends(get_event_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    now = datetime.now(UTC)
    if hasattr(event_service, "list_events"):
        events = await event_service.list_events(country=country, start=now, end=now + timedelta(days=days), enrich=False)
    else:
        events = await event_service.upcoming(country=country, days=days)
    enriched, metadata = await enrichment_orchestrator.enrich_events(
        events=events,
        country=country,
        start=now,
        end=now + timedelta(days=days),
        trigger="api",
    )
    return {
        "run_id": metadata["run_id"],
        "events_checked": len(enriched),
        "data_quality": metadata["data_quality"],
        "service_role": "data provider only",
    }


@router.get("/news/stored")
async def news_stored(
    symbols: str | None = Query(default=None),
    days: int = Query(default=7, ge=1, le=365),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    symbol_list = [item.strip().upper() for item in symbols.split(",") if item.strip()] if symbols else None
    news = MarketNewsRepository(enrichment_orchestrator.settings).stored(symbols=symbol_list, days=days)
    return {"count": len(news), "news": news, "service_role": "data provider only"}


@router.post("/diagnostics/e2e-cache-test")
async def diagnostics_e2e_cache_test(
    country: str = Query(default="US", min_length=2, max_length=8),
    days: int = Query(default=30, ge=1, le=365),
    symbol: str = Query(default="MNQ", min_length=1, max_length=16),
    reset_db: bool = Query(default=False),
    enable_ai: bool = Query(default=False),
    ai_mode: str = Query(default="codex_cli"),
    run_count: int = Query(default=1, ge=1, le=4),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    service = DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )
    return await service.e2e_cache_test(
        country=country,
        days=days,
        symbol=symbol,
        reset_db=reset_db,
        enable_ai=enable_ai,
        ai_mode=ai_mode,
        run_count=run_count,
    )


@router.get("/diagnostics/db-summary")
async def diagnostics_db_summary(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return MarketFactRepository(enrichment_orchestrator.settings).db_summary()


@router.get("/diagnostics/credential-audit")
async def diagnostics_credential_audit(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return credential_audit(enrichment_orchestrator.settings)


@router.get("/diagnostics/acquisition-status")
async def diagnostics_acquisition_status(
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return AcquisitionStatusService(enrichment_orchestrator.settings).status()


@router.get("/diagnostics/data-quality")
async def diagnostics_data_quality(
    country: str = Query(default="US", min_length=2, max_length=8),
    days: int = Query(default=30, ge=1, le=365),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await market_context_mnq_quality(
        country=country,
        days=days,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )


@router.get("/providers/investing/economic-calendar")
async def provider_investing_economic_calendar(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("investing_economic_calendar", refresh=refresh)


@router.get("/providers/investing/holidays")
async def provider_investing_holidays(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("investing_holidays", refresh=refresh)


@router.get("/providers/xtb/economic-calendar")
async def provider_xtb_economic_calendar(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("xtb_economic_calendar", refresh=refresh)


@router.get("/providers/marketbeat/holidays")
async def provider_marketbeat_holidays(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("marketbeat_holidays", refresh=refresh)


@router.get("/providers/investing/fed-rate-monitor")
async def provider_investing_fed_rate_monitor(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("investing_fed_rate_monitor", refresh=refresh)


@router.get("/providers/cboe/risk-indices")
async def provider_cboe_risk_indices(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("cboe_risk_indices", refresh=refresh)


@router.get("/providers/nasdaq/earnings-calendar")
async def provider_nasdaq_earnings_calendar(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("nasdaq_earnings", refresh=refresh)


@router.get("/providers/nasdaq/nasdaq-100")
async def provider_nasdaq_100(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("nasdaq_100", refresh=refresh)


@router.get("/providers/nasdaq/market-info")
async def provider_nasdaq_market_info(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("nasdaq_market_info", refresh=refresh)


@router.get("/providers/nasdaq/qqq-options")
async def provider_nasdaq_qqq_options(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("nasdaq_qqq_options", refresh=refresh)


@router.get("/providers/sentiment/aaii")
async def provider_sentiment_aaii(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("aaii_sentiment", refresh=refresh)


@router.get("/providers/sentiment/macromicro-aaii")
async def provider_sentiment_macromicro_aaii(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("macromicro_aaii_crosscheck", refresh=refresh)


@router.get("/providers/polymarket/markets")
async def provider_polymarket_markets(
    refresh: str = Query(default="auto", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return await MultiSourceRuntimeService(enrichment_orchestrator.settings).provider("polymarket_prediction_markets", refresh=refresh)


@router.get("/diagnostics/full-model")
async def diagnostics_full_model(
    symbol: str = Query(default="MNQ", min_length=1, max_length=16),
    country: str = Query(default="US", min_length=2, max_length=8),
    days: int = Query(default=30, ge=1, le=365),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    service = DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )
    return await service.full_model(country=country, days=days, symbol=symbol)


@router.get("/diagnostics/temporal-integrity")
async def diagnostics_temporal_integrity(
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    ).temporal_integrity()


@router.get("/diagnostics/release-refresh-status")
async def diagnostics_release_refresh_status(
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    ).release_refresh_status()


@router.get("/diagnostics/news-freshness")
async def diagnostics_news_freshness(
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    ).news_freshness()


@router.get("/diagnostics/source-classification")
async def diagnostics_source_classification(
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    return DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    ).source_classification()


@router.get("/diagnostics/health-summary")
async def diagnostics_health_summary(
    refresh: str = Query(default="false", pattern="^(false|auto)$"),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    diagnostics = DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )
    model = await diagnostics.full_model(
        country="US",
        days=30,
        symbol="MNQ",
        fetch_missing_nasdaq=refresh == "auto",
        refresh=refresh,
    )
    db_summary = MarketFactRepository(enrichment_orchestrator.settings).db_summary()
    return HealthReportService().build_report(
        base_url="in-process",
        refresh_mode=refresh,
        service_status="ok",
        db_health={"status": "ok", "db_summary": db_summary},
        market_context=model,
        temporal_integrity=diagnostics.temporal_integrity(),
        release_refresh=diagnostics.release_refresh_status(),
        news_freshness=diagnostics.news_freshness(),
        source_classification=diagnostics.source_classification(),
        db_summary=db_summary,
        ai_researcher_enabled=enrichment_orchestrator.settings.enable_ai_researcher,
        ai_researcher_mode=enrichment_orchestrator.settings.ai_researcher_mode,
    )


async def _extended_market_model(
    *,
    refresh: str,
    macro_service: MacroService,
    event_service: EventService,
    event_window_service: EventWindowService,
    nasdaq_service: NasdaqDataService,
    enrichment_orchestrator: EnrichmentOrchestrator,
) -> dict[str, object]:
    diagnostics = DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )
    return await diagnostics.full_model(
        country="US",
        days=30,
        symbol="MNQ",
        fetch_missing_nasdaq=refresh == "force",
        refresh=refresh,
    )


@router.get("/positioning/cot")
async def positioning_cot(
    refresh: str = Query(default="false", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    cot = await PositioningRuntimeService(enrichment_orchestrator.settings).cot(refresh=refresh)
    return cot


@router.get("/sentiment")
async def sentiment(
    refresh: str = Query(default="false", pattern="^(false|auto|force)$"),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    aaii = await PositioningRuntimeService(enrichment_orchestrator.settings).aaii(refresh=refresh)
    return {"status": aaii.get("status"), "aaii": aaii, "service_role": "data provider only"}


@router.get("/news/digest")
async def news_digest(
    refresh: str = Query(default="false", pattern="^(false|auto|force)$"),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    model = await _extended_market_model(refresh=refresh, macro_service=macro_service, event_service=event_service, event_window_service=event_window_service, nasdaq_service=nasdaq_service, enrichment_orchestrator=enrichment_orchestrator)
    return model.get("news_digest", {})


@router.get("/news/latest")
async def news_latest_context(
    refresh: str = Query(default="false", pattern="^(false|auto|force)$"),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    model = await _extended_market_model(refresh=refresh, macro_service=macro_service, event_service=event_service, event_window_service=event_window_service, nasdaq_service=nasdaq_service, enrichment_orchestrator=enrichment_orchestrator)
    return model.get("news_context", {})


@router.get("/events/windows")
async def events_windows(
    refresh: str = Query(default="false", pattern="^(false|auto|force)$"),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    model = await _extended_market_model(refresh=refresh, macro_service=macro_service, event_service=event_service, event_window_service=event_window_service, nasdaq_service=nasdaq_service, enrichment_orchestrator=enrichment_orchestrator)
    return model.get("event_windows", {})

@router.get("/market-context/mnq/quality")
async def market_context_mnq_quality(
    country: str = Query(default="US", min_length=2, max_length=8),
    days: int = Query(default=30, ge=1, le=365),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    service = DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )
    model = await service.full_model(country=country, days=days, symbol="MNQ", fetch_missing_nasdaq=False)
    quality = model.get("data_quality", {})
    return {
        "symbol": "MNQ",
        "section_quality": quality.get("section_quality", {}),
        "overall_data_quality": quality.get("overall_data_quality", {}),
        "missing_critical_data": quality.get("missing_critical_fields", []),
        "stale_data": quality.get("stale_fields", []),
        "db_summary": model.get("db_summary", {}),
        "metadata": model.get("metadata", {}),
        "service_role": "data provider only",
    }


@router.get("/nasdaq/qqq/holdings", response_model=QQQHoldingsResponse)
async def nasdaq_qqq_holdings(
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
) -> QQQHoldingsResponse:
    return await nasdaq_service.qqq_holdings()


@router.get("/nasdaq/mega-cap/snapshot", response_model=MegaCapSnapshotResponse)
async def nasdaq_mega_cap_snapshot(
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
) -> MegaCapSnapshotResponse:
    return await nasdaq_service.mega_cap_snapshot()


@router.get("/nasdaq/mega-cap/breadth", response_model=MegaCapBreadthResponse)
async def nasdaq_mega_cap_breadth(
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
) -> MegaCapBreadthResponse:
    return await nasdaq_service.mega_cap_breadth()


@router.get("/nasdaq/earnings/upcoming", response_model=EarningsResponse)
async def nasdaq_earnings_upcoming(
    days: int = Query(default=14, ge=1, le=90),
    tickers: str | None = Query(default=None),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
) -> EarningsResponse:
    ticker_list = [item.strip().upper() for item in tickers.split(",") if item.strip()] if tickers else None
    return await nasdaq_service.earnings(days=days, tickers=ticker_list)


@router.get("/news/latest", response_model=NewsResponse)
async def news_latest(
    symbols: str = Query(default="NVDA,AAPL,MSFT,QQQ"),
    limit: int = Query(default=20, ge=1, le=100),
    recency_days: int = Query(default=14, ge=1, le=90),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
) -> NewsResponse:
    symbol_list = [item.strip().upper() for item in symbols.split(",") if item.strip()]
    return await nasdaq_service.latest_news(
        symbols=symbol_list,
        limit=limit,
        recency_days=recency_days,
    )


@router.get("/nasdaq/context")
async def nasdaq_context(
    refresh: str = Query(default="false", pattern="^(false|auto|force)$"),
    macro_service: MacroService = Depends(get_macro_service),
    event_service: EventService = Depends(get_event_service),
    event_window_service: EventWindowService = Depends(get_event_window_service),
    nasdaq_service: NasdaqDataService = Depends(get_nasdaq_data_service),
    enrichment_orchestrator: EnrichmentOrchestrator = Depends(get_enrichment_orchestrator),
) -> dict[str, object]:
    diagnostics = DiagnosticsService(
        enrichment_orchestrator.settings,
        macro_service=macro_service,
        event_service=event_service,
        event_window_service=event_window_service,
        nasdaq_data_service=nasdaq_service,
        enrichment_orchestrator=enrichment_orchestrator,
    )
    nasdaq_context, quality = await diagnostics._nasdaq_db_first(
        symbol="MNQ",
        fetch_missing=refresh != "false",
        force=refresh == "force",
    )
    enriched = enrich_nasdaq_context(nasdaq_context or {}, {})
    enriched["data_quality"] = {**(enriched.get("data_quality") or {}), **quality}
    enriched["service_role"] = "data provider only"
    return enriched
