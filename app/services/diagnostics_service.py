from __future__ import annotations

import uuid
import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import Settings
from app.models.common import Freshness, ProviderMetadata, ProviderType
from app.models.macro import MacroLatestResponse
from app.models.macro import MacroSeries
from app.models.nasdaq import NasdaqContextResponse
from app.services.data_freshness_service import (
    CanonicalFreshnessResult,
    DataFreshnessService,
    parse_datetime,
)
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.economic_event_materialization_service import EconomicEventMaterializationService
from app.services.event_service import EventService
from app.services.event_window_service import EventWindowService
from app.services.macro_service import MacroService
from app.services.market_fact_repository import MarketFactRepository, connect_market_db, now_iso
from app.services.market_context_builder import (
    build_news_context,
    build_market_context_contract,
    materialize_nasdaq_context_from_facts,
    normalize_nasdaq_context,
)
from app.services.market_context_hardening_service import harden_market_context
from app.services.qqq_weight_intelligence_service import log_weight_event
from app.services.bls_required_series import (
    bls_required_series_status_from_macro_series,
    bls_required_series_status_from_macro_snapshot,
)
from app.services.data_integrity_service import (
    classify_source,
    fact_temporal_kind,
    fact_temporal_status,
    freshness_label,
    news_content_status,
    next_release_refresh_at,
    parse_retry_seconds,
)
from app.services.market_news_repository import MarketNewsRepository
from app.services.macro_consensus_service import MacroConsensusService, merge_consensus_provider_payloads
from app.services.news_intelligence_runtime_service import NewsIntelligenceRuntimeService
from app.services.nasdaq_data_service import NasdaqDataService
from app.services.positioning_runtime_service import PositioningRuntimeService
from app.services.multi_source_runtime_service import MultiSourceRuntimeService, apply_multi_source_context
from app.services.fed_expectations_service import FedExpectationsService
from app.services.force_generation_staging_service import (
    ForceGenerationStaging,
)
from app.services.risk_context_runtime_service import RiskContextRuntimeService
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.social_sentiment_service import SocialSentimentService
from app.services.temporal_domain_service import exact_occurrence_key, reconcile_calendar_events
from app.services.event_value_candidate_repository import EventValueCandidateRepository
from app.services.execution_context import ExecutionContext
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
    provider_attempt,
)


MACRO_ACCOUNTING_SERIES: dict[str, tuple[str, ...]] = {
    "treasury_rates": (
        "DGS2",
        "DGS10",
        "DGS30",
        "T10Y2Y",
        "T10Y3M",
        "NFCI",
    ),
    "fed_funds": ("DFF", "FEDFUNDS", "SOFR"),
    "target_range": ("DFEDTARL", "DFEDTARU"),
    "cpi": ("CUSR0000SA0", "CUSR0000SA0L1E"),
    "ppi": ("WPUFD4",),
    "pce": (
        "BEA:PCE",
        "BEA:PCE_PRICE_INDEX",
        "BEA:CORE_PCE",
    ),
    "gdp": ("BEA:GDP", "GDP"),
    "employment": ("LNS14000000", "UNRATE"),
    "wages": ("CES0500000003",),
    "nfp": ("CES0000000001", "PAYEMS"),
    "jobless_claims": ("ICSA",),
}
NASDAQ_ACCOUNTING_FACT_TYPES: dict[str, tuple[str, ...]] = {
    "nasdaq_100": ("qqq_holdings", "nasdaq_context"),
    "mega_cap_quotes": ("mega_cap_snapshot", "mega_cap_breadth"),
    "earnings": ("earnings_event",),
}
MACRO_ACCOUNTING_MAX_AGE = {
    "treasury_rates": timedelta(days=2),
    "fed_funds": timedelta(days=2),
    "target_range": timedelta(days=45),
    "cpi": timedelta(days=45),
    "ppi": timedelta(days=45),
    "pce": timedelta(days=45),
    "gdp": timedelta(days=120),
    "employment": timedelta(days=45),
    "wages": timedelta(days=45),
    "nfp": timedelta(days=45),
    "jobless_claims": timedelta(days=14),
}
MACRO_PROVIDER_SERIES = {
    "FRED": {
        "VIXCLS",
        "DGS2",
        "DGS10",
        "DGS30",
        "T10Y2Y",
        "T10Y3M",
        "NFCI",
        "DFF",
        "FEDFUNDS",
        "SOFR",
        "DFEDTARL",
        "DFEDTARU",
        "ICSA",
        "VIXCLS",
    },
    "BLS": {
        "CUSR0000SA0",
        "CUSR0000SA0L1E",
        "WPUFD4",
        "LNS14000000",
        "CES0500000003",
        "CES0000000001",
    },
    "BEA": {
        "BEA:PCE",
        "BEA:PCE_PRICE_INDEX",
        "BEA:CORE_PCE",
        "BEA:GDP",
    },
}
NASDAQ_ACCOUNTING_MAX_AGE = {
    "nasdaq_100": timedelta(hours=12),
    "mega_cap_quotes": timedelta(hours=12),
    "earnings": timedelta(days=14),
}
NEWS_ACCOUNTING_PROVIDER_NAMES = {
    "ALPHA_VANTAGE_NEWS_SENTIMENT": (
        "Alpha Vantage NEWS_SENTIMENT"
    ),
    "GDELT_DOC_API": "GDELT Doc API",
    "FEDERAL_RESERVE_RSS": "Federal Reserve RSS",
    "BLS_RSS": "BLS RSS",
    "BEA_RSS": "BEA RSS",
    "YAHOO_FINANCE_RSS": "Yahoo Finance RSS",
    "MARKETWATCH_RSS": "MarketWatch RSS",
    "GOOGLE_NEWS_RSS": "Google News RSS",
}
VIX_MAX_AGE = timedelta(days=2)


def _merge_nasdaq_materializations(
    cached: dict[str, Any] | None,
    acquired: dict[str, Any] | None,
    *,
    provider_datasets: set[str],
) -> dict[str, Any] | None:
    output = dict(cached or {})
    acquired = acquired or {}
    if "nasdaq_100" in provider_datasets:
        for key in (
            "qqq_holdings",
            "qqq_holdings_summary",
            "sector_exposure",
        ):
            if key in acquired:
                output[key] = acquired[key]
    if "mega_cap_quotes" in provider_datasets:
        for key in (
            "mega_cap_snapshot",
            "mega_cap_breadth",
        ):
            if key in acquired:
                output[key] = acquired[key]
    if acquired.get("data_quality"):
        output["data_quality"] = acquired["data_quality"]
    return output or None


class DiagnosticsService:
    def __init__(
        self,
        settings: Settings,
        *,
        macro_service: MacroService,
        event_service: EventService,
        event_window_service: EventWindowService,
        nasdaq_data_service: NasdaqDataService,
        enrichment_orchestrator: EnrichmentOrchestrator,
    ) -> None:
        self.settings = settings
        self.macro_service = macro_service
        self.event_service = event_service
        self.event_window_service = event_window_service
        self.nasdaq_data_service = nasdaq_data_service
        self.enrichment_orchestrator = enrichment_orchestrator
        self.facts = MarketFactRepository(settings)
        self.event_materializer = EconomicEventMaterializationService(settings, facts=self.facts)
        self.macro_consensus = MacroConsensusService(settings, facts=self.facts)
        self.news = MarketNewsRepository(settings)
        self.news_intelligence = NewsIntelligenceRuntimeService(settings, facts=self.facts)
        self.freshness = DataFreshnessService(settings)
        self.positioning_runtime = PositioningRuntimeService(settings)
        self.fed_expectations = FedExpectationsService(settings)
        self.risk_context = RiskContextRuntimeService(settings)
        self.force_generation_plan: dict[str, Any] = {}

    async def e2e_cache_test(
        self,
        *,
        country: str = "US",
        days: int = 30,
        symbol: str = "MNQ",
        reset_db: bool = False,
        enable_ai: bool = False,
        ai_mode: str = "codex_cli",
        run_count: int = 1,
    ) -> dict[str, Any]:
        if reset_db:
            self.facts.reset_data_tables()
        previous_ai_enabled = self.settings.enable_ai_researcher
        previous_ai_mode = self.settings.ai_researcher_mode
        self.settings.enable_ai_researcher = enable_ai
        self.settings.ai_researcher_mode = ai_mode
        test_id = str(uuid.uuid4())
        runs = []
        preview: dict[str, Any] = {}
        try:
            for index in range(1, run_count + 1):
                result = await self._single_run(country=country, days=days, symbol=symbol)
                result["run_number"] = index
                runs.append(result)
                preview = result.pop("_model_preview", preview)
        finally:
            self.settings.enable_ai_researcher = previous_ai_enabled
            self.settings.ai_researcher_mode = previous_ai_mode
        return {
            "test_id": test_id,
            "reset_db": reset_db,
            "ai_enabled": enable_ai,
            "ai_mode": ai_mode,
            "runs": runs,
            "model_preview": preview,
            "service_role": "data provider only",
        }

    async def full_model(
        self,
        *,
        country: str = "US",
        days: int = 30,
        symbol: str = "MNQ",
        fetch_missing_nasdaq: bool = True,
        refresh: str = "auto",
        request_id: str | None = None,
        execution_context: ExecutionContext | None = None,
        accounting_collector: (
            RequestProviderAccountingCollector | None
        ) = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        vix_preflight = self._vix_database_lookup()
        fed_expectations_preflight = (
            self.fed_expectations.lookup_canonical()
        )
        risk_context_preflight = (
            self.risk_context.lookup_canonical()
        )
        fetch_missing = refresh != "false"
        force = refresh == "force"
        initial_news_items = self.news.stored(
            days=days,
            limit=None,
            include_quarantined=True,
        )
        (
            initial_news_item,
            initial_news_freshness,
        ) = _select_current_news_database_candidate(
            initial_news_items,
            freshness_service=self.freshness,
        )
        news_database_valid = bool(
            initial_news_item
            and initial_news_freshness.usable
        )
        staged_force_events: list[Any] = []
        pending_calendar_accounting: dict[str, Any] = {}
        self.force_generation_plan = {}
        request_context = execution_context or (
            ExecutionContext.provider_only(
                correlation_id=(
                    request_id
                    or f"market-context-{uuid.uuid4()}"
                ),
                allow_live_providers=fetch_missing,
            )
        )
        if (
            request_id
            and request_context.correlation_id != request_id
        ):
            raise ValueError(
                "market_context_request_correlation_mismatch"
            )
        force_schedule_coverage: dict[str, Any] = {}
        force_schedule_preflight_performed = False
        if (
            force
            and callable(getattr(self.event_service, "list_events", None))
            and (
                callable(
                    getattr(self.event_service, "coverage_targets", None)
                )
                or (
                    self.settings.event_calendar_catchup_enabled
                    and any(
                        hasattr(self.event_service, attribute)
                        for attribute in (
                            "coverage_proof",
                            "last_coverage_proof",
                            "last_provider_coverage_proofs",
                        )
                    )
                )
            )
        ):
            force_schedule_coverage = await self._force_schedule_catch_up(
                now=now,
                stage_for_atomic_finalization=True,
            )
            force_schedule_preflight_performed = True
            self.force_generation_plan = dict(
                force_schedule_coverage.pop(
                    "_canonical_generation_plan",
                    {},
                )
            )
            staged_force_events = list(
                force_schedule_coverage.pop("_staged_events", [])
            )

        def capture_calendar_accounting(
            **values: Any,
        ) -> None:
            pending_calendar_accounting.clear()
            pending_calendar_accounting.update(values)

        async def load_macro() -> tuple[MacroLatestResponse, dict[str, Any]]:
            try:
                return await asyncio.wait_for(
                    self._macro_db_first(
                        fetch_missing=fetch_missing,
                        force=force,
                        accounting_collector=accounting_collector,
                        vix_preflight=vix_preflight,
                    ),
                    timeout=max(float(self.settings.timeout_macro_seconds), 1.0),
                )
            except TimeoutError:
                self._record_macro_accounting(
                    accounting_collector,
                    lookup_performed=True,
                    lookup_rows=[],
                    macro=MacroLatestResponse(),
                    provider_batch_called=True,
                    provider_not_called_reason=None,
                )
                return MacroLatestResponse(), {
                    "db_hits": 0,
                    "db_misses": 1,
                    "provider_hits": 0,
                    "provider_failures": 1,
                    "warnings": [f"macro_provider_timeout_after_{self.settings.timeout_macro_seconds}s"],
                    "errors": [],
                }

        async def load_events() -> tuple[list[Any], dict[str, Any]]:
            if refresh == "false":
                events, materialization = self.event_materializer.load_from_history(
                    country=country,
                    start=now,
                    end=now + timedelta(days=days),
                    refresh_mode="false",
                )
                capture_calendar_accounting(
                    events=events,
                    database_lookup_performed=True,
                    provider_called=False,
                    provider_result="NOT_CALLED",
                    provider_not_called_reason=(
                        "REFRESH_FALSE_DATABASE_ONLY"
                    ),
                    acquisition_reason_code=(
                        "CANONICAL_EVENT_HISTORY_SELECTED"
                    ),
                )
                return events, {
                    "data_quality": {
                        "refresh_mode": "false",
                        "events_found": len(events),
                        "enrichment_status": "cache_only",
                        "db_hits": materialization["enrichment_fact_hit_count"],
                        "db_misses": materialization["enrichment_fact_miss_count"],
                        "cache_used": True,
                        "ai_research_called": False,
                        "ai_research_requests": 0,
                        **materialization,
                    }
                }
            if force_schedule_preflight_performed:
                events = staged_force_events or (
                    self._canonical_three_week_events(
                        country=country,
                        now=now,
                    )
                )
                calendar_db_lookup = True
                calendar_provider_called = bool(
                    force_schedule_coverage.get(
                        "provider_calls_executed"
                    )
                )
                calendar_catch_up_complete = bool(
                    calendar_provider_called
                    and _calendar_catch_up_succeeded(
                        force_schedule_coverage
                    )
                )
                calendar_provider_result = (
                    "SCHEDULE_CATCH_UP_COMPLETED"
                    if calendar_catch_up_complete
                    else "SCHEDULE_CATCH_UP_INCOMPLETE"
                    if calendar_provider_called
                    else "NOT_CALLED"
                )
                calendar_not_called_reason = (
                    None
                    if calendar_provider_called
                    else "CANONICAL_EVENT_HISTORY_AVAILABLE"
                )
            else:
                try:
                    events = await asyncio.wait_for(
                        self._fetch_official_events(
                            country=country,
                            start=now,
                            end=now + timedelta(days=days),
                        ),
                        timeout=max(float(self.settings.timeout_events_seconds), 1.0),
                    )
                    self._persist_official_events(events)
                    calendar_db_lookup = False
                    calendar_provider_called = True
                    calendar_provider_result = (
                        "SUCCESS" if events else "NO_DATA"
                    )
                    calendar_not_called_reason = None
                except TimeoutError:
                    events, materialization = self.event_materializer.load_from_history(
                        country=country,
                        start=now,
                        end=now + timedelta(days=days),
                        refresh_mode=refresh,
                    )
                    capture_calendar_accounting(
                        events=events,
                        database_lookup_performed=True,
                        provider_called=True,
                        provider_result="TIMEOUT",
                        provider_not_called_reason=None,
                        acquisition_reason_code=(
                            "PROVIDER_TIMEOUT_DATABASE_HISTORY_SELECTED"
                        ),
                    )
                    return events, {
                        "data_quality": {
                            "refresh_mode": refresh,
                            "events_found": len(events),
                            "enrichment_status": "events_timeout_fallback_to_history" if events else "events_timeout",
                            "missing_critical_fields": [] if events else ["events_not_available"],
                            "warnings": [f"events_fetch_timeout_after_{self.settings.timeout_events_seconds}s"],
                            **materialization,
                        }
                    }
            enrichment_timeout = max(float(self.settings.timeout_events_seconds), 1.0)
            enrichment_timeout += 5.0
            try:
                result = await asyncio.wait_for(
                    self.enrichment_orchestrator.enrich_events(
                        events=events,
                        country=country,
                        start=now,
                        end=now + timedelta(days=days),
                        trigger="diagnostics_full_model" if not force else "diagnostics_full_model_force",
                        force=force,
                        execution_context=request_context,
                    ),
                    timeout=enrichment_timeout,
                )
                capture_calendar_accounting(
                    events=result[0],
                    database_lookup_performed=(
                        calendar_db_lookup
                    ),
                    provider_called=calendar_provider_called,
                    provider_result=calendar_provider_result,
                    provider_not_called_reason=(
                        calendar_not_called_reason
                    ),
                    acquisition_reason_code=(
                        "CANONICAL_EVENT_ACQUISITION_COMPLETED"
                    ),
                )
                return result
            except TimeoutError:
                # The orchestrator persists this counter in its finally block.  It
                # lets the outer deadline distinguish a skipped pipeline from an
                # AI dispatch that was cancelled by the diagnostics deadline.
                latest_run = self.enrichment_orchestrator.runs.latest() or {}
                ai_started = bool(latest_run.get("ai_research_requests"))
                ai_candidates = (
                    self.enrichment_orchestrator._ai_candidates(events)
                    if ai_started
                    else []
                )
                capture_calendar_accounting(
                    events=events,
                    database_lookup_performed=(
                        calendar_db_lookup
                    ),
                    provider_called=calendar_provider_called,
                    provider_result=calendar_provider_result,
                    provider_not_called_reason=(
                        calendar_not_called_reason
                    ),
                    acquisition_reason_code=(
                        "CANONICAL_EVENT_ACQUISITION_COMPLETED"
                    ),
                )
                return events, {
                    "data_quality": {
                        "refresh_mode": refresh,
                        "events_found": len(events),
                        "enrichment_complete": False,
                        "enrichment_partial": bool(events),
                        # The outer enrichment deadline does not prove that the AI
                        # dispatcher ran.  Keep this as an optional skip, never as
                        # an AI timeout.
                        "enrichment_timeout": False,
                        "enrichment_status": "not_required",
                        "enrichment_not_attempted": not ai_started,
                        "ai_research_enabled": bool(self.settings.enable_ai_researcher),
                        "ai_research_configured": bool(
                            self.settings.enable_ai_researcher
                            and self.settings.ai_researcher_mode in {"codex_cli", "openai_api"}
                        ),
                        "ai_research_mode": self.settings.ai_researcher_mode,
                        "ai_research_called": ai_started,
                        "ai_candidate_event_ids": [event.event_id for event in ai_candidates],
                        "ai_research_status": "cancelled" if ai_started else "not_required",
                        "ai_failure_reason": "diagnostics_enrichment_deadline" if ai_started else None,
                        "missing_critical_fields": [],
                        "warnings": [f"optional_event_enrichment_skipped_after_{enrichment_timeout}s"],
                    }
                }

        async def load_nasdaq() -> tuple[dict[str, Any] | None, dict[str, Any]]:
            try:
                return await asyncio.wait_for(
                    self._nasdaq_db_first(
                        symbol=symbol,
                        fetch_missing=fetch_missing and fetch_missing_nasdaq,
                        force=force,
                        accounting_collector=accounting_collector,
                        fetch_news=not news_database_valid,
                    ),
                    timeout=max(float(self.settings.timeout_nasdaq_seconds), 1.0),
                )
            except TimeoutError:
                self._record_nasdaq_accounting(
                    accounting_collector,
                    lookup_performed=True,
                    lookup_rows={},
                    provider_invoked=True,
                    context=None,
                    provider_error="TIMEOUT",
                )
                return None, {
                    "db_hits": 0,
                    "db_misses": 1,
                    "provider_hits": 0,
                    "provider_failures": 1,
                    "warnings": [f"nasdaq_context_timeout_after_{self.settings.timeout_nasdaq_seconds}s"],
                    "errors": [],
                }

        async def load_event_windows():
            if refresh in {"false", "force"}:
                return None
            try:
                return await asyncio.wait_for(
                    self.event_window_service.event_windows(symbol=symbol),
                    timeout=max(float(self.settings.timeout_events_seconds), 1.0),
                )
            except TimeoutError:
                return {}

        (macro, macro_quality), (enriched, enrichment_metadata), (nasdaq_context, nasdaq_quality), event_windows = await asyncio.gather(
            load_macro(),
            load_events(),
            load_nasdaq(),
            load_event_windows(),
        )
        news_provider_quality = dict(
            nasdaq_quality.pop("_news_provider_quality", {})
            or {}
        )
        earnings_preloaded_block = nasdaq_quality.pop(
            "_earnings_preloaded_block",
            None,
        )
        multi_runtime = MultiSourceRuntimeService(self.settings)
        primary_calendar_succeeded = (
            _calendar_primary_acquisition_succeeded(
                pending_calendar_accounting,
                events=enriched,
            )
        )
        investing_refresh = (
            "false"
            if primary_calendar_succeeded
            else refresh
            if force
            or (
                refresh == "auto"
                and self.macro_consensus.needs_refresh(enriched)
            )
            else "false"
        )
        investing_payload = await multi_runtime.provider(
            "investing_economic_calendar",
            refresh=investing_refresh,
        )
        xtb_payload = await multi_runtime.provider(
            "xtb_economic_calendar",
            refresh=(
                "false"
                if primary_calendar_succeeded
                or _runtime_block_available(investing_payload)
                else refresh
                if refresh in {"false", "force"}
                else "auto"
            ),
        )
        if pending_calendar_accounting:
            self._record_calendar_accounting(
                accounting_collector,
                **pending_calendar_accounting,
                coverage=force_schedule_coverage,
                fallback_blocks=[
                    investing_payload,
                    xtb_payload,
                ],
            )
        if refresh != "false":
            candidates = EventValueCandidateRepository(self.settings)
            candidates.persist_provider_payload(investing_payload)
            candidates.persist_provider_payload(xtb_payload)
        canonical_events = self._canonical_three_week_events(
            country=country,
            now=now,
        )
        enriched = reconcile_calendar_events(
            [*enriched, *canonical_events],
            [investing_payload, xtb_payload],
            now=now,
            temporal_validation=self.facts.temporal_validation,
        )
        for event in enriched:
            self.facts.upsert_economic_event(
                event,
                event_key=exact_occurrence_key(event),
                valid_until=self.freshness.macro_valid_until(event),
            )
        consensus_quality = {field: 0 for field in (
            "consensus_lookup_count", "consensus_candidate_count", "consensus_match_count",
            "consensus_rejected_count", "consensus_persisted_count", "consensus_read_back_count",
            "consensus_materialized_count", "consensus_missing_count",
        )}
        if refresh != "false":
            ranked_consensus = merge_consensus_provider_payloads(investing_payload, xtb_payload)
            enriched, consensus_quality, _ = self.macro_consensus.enrich_and_persist(
                enriched,
                ranked_consensus,
                refresh_mode=refresh,
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
        if refresh in {"false", "force"}:
            if hasattr(self.event_window_service, "from_events"):
                event_windows = self.event_window_service.from_events(symbol=symbol, events=enriched)
            else:
                from app.models.macro import EventWindowsResponse

                event_windows = EventWindowsResponse(symbol=symbol, checked_at_utc=datetime.now(UTC).isoformat())
        news_provider_executed = any(
            int(item.get("calls") or 0) > 0
            for item in news_provider_quality.get(
                "provider_accounting",
                [],
            )
            if isinstance(item, dict)
        )
        news_items = (
            initial_news_items
            if news_database_valid
            or not news_provider_executed
            else self.news.stored(
                days=days,
                limit=None,
                include_quarantined=True,
            )
        )
        self._record_news_accounting(
            accounting_collector,
            news_items=news_items,
            database_item=initial_news_item,
            database_freshness=initial_news_freshness,
            provider_quality=news_provider_quality,
        )
        news_context, news_runtime = self.news_intelligence.materialize(
            news_items,
            refresh_mode=refresh,
        )
        news_pipeline = _news_pipeline_status(news_items, materialized=news_context)
        macro_pipeline = _macro_pipeline_status(macro)
        pipeline_integrity = {
            "critical_fetch_completed": (not fetch_missing) or macro_quality.get("provider_hits", 0) > 0 or macro_quality.get("db_hits", 0) > 0,
            "critical_persistence_completed": macro_quality.get("provider_hits", 0) == 0 or macro_quality.get("read_back_count", macro_quality.get("db_hits", 0)) > 0,
            "critical_commits_completed": macro_quality.get("provider_hits", 0) == 0 or macro_quality.get("read_back_count", 0) > 0,
            "critical_read_back_completed": bool(macro.series) and bool(nasdaq_context) and news_pipeline["committed"],
            "snapshot_materialization_completed": news_pipeline["search_completed"] and bool(macro.series) and bool(nasdaq_context),
            "snapshot_built_from_db": True,
            "partial_response": not (bool(macro.series) and bool(nasdaq_context)),
        }
        cot_payload = await self.positioning_runtime.cot(refresh=refresh)
        aaii_payload = await self.positioning_runtime.aaii(refresh=refresh)
        self._record_positioning_accounting(
            accounting_collector,
            payload=cot_payload,
            refresh=refresh,
        )
        positioning_context = _positioning_context_from_runtime(cot_payload)
        sentiment_context = _sentiment_context_from_runtime(aaii_payload)
        event_facts = self.facts.search_facts(country=country, limit=500)
        quality = {
            **enrichment_metadata.get("data_quality", {}),
            **consensus_quality,
            "macro": macro_quality,
            "nasdaq": nasdaq_quality,
            "missing_critical_fields": enrichment_metadata.get("data_quality", {}).get("missing_critical_fields", []),
            "stale_fields": enrichment_metadata.get("data_quality", {}).get("stale_fields", []),
            "provider_observations_summary": self._provider_observation_summary(),
            "pipeline_integrity": pipeline_integrity,
            "news_pipeline": news_pipeline,
            "news_intelligence": news_runtime,
            "macro_pipeline": macro_pipeline,
            "force_schedule_coverage": force_schedule_coverage,
        }
        contract = build_market_context_contract(
            symbol=symbol,
            macro=macro,
            events_today=[],
            upcoming_events=enriched,
            event_windows=event_windows,
            nasdaq_context=nasdaq_context,
            news_items=news_items,
            data_quality=quality,
            db_summary=self.facts.db_summary(),
            event_facts=event_facts,
            metadata={
                "event_enrichment": _event_enrichment_metadata(enrichment_metadata, enriched, settings=self.settings),
                "persistent_enrichment": enrichment_metadata,
                "request_refresh_mode": refresh,
            },
            positioning_context=positioning_context,
            sentiment_context=sentiment_context,
            news_context_override=news_context,
        )
        contract["data_quality"]["macro_pipeline"] = _macro_pipeline_status(macro, contract.get("macro_snapshot") or {})
        overall_quality = contract["data_quality"].get("overall_data_quality") or {}
        contract["data_quality"]["missing_critical_fields"] = overall_quality.get("missing_critical_fields") or contract["data_quality"].get("missing_critical_fields") or []
        multi_refresh = "force" if refresh == "force" else "false"
        multi_source_preloaded = {
            "investing_economic_calendar": investing_payload,
            "xtb_economic_calendar": xtb_payload,
        }
        if earnings_preloaded_block:
            multi_source_preloaded["nasdaq_earnings"] = (
                earnings_preloaded_block
            )
        fed_record, fed_freshness = (
            fed_expectations_preflight
        )
        if fed_record and fed_freshness.usable:
            multi_source_preloaded[
                "investing_fed_rate_monitor"
            ] = _canonical_preflight_runtime_block(
                fed_record,
                lookup=(
                    self.fed_expectations.last_database_lookup
                    or {}
                ),
                provider_source=(
                    "Investing.com Fed Rate Monitor"
                ),
            )
        risk_record, risk_freshness = risk_context_preflight
        if risk_record and risk_freshness.usable:
            multi_source_preloaded[
                "cboe_risk_indices"
            ] = _canonical_preflight_runtime_block(
                risk_record,
                lookup=(
                    self.risk_context.last_database_lookup
                    or {}
                ),
                provider_source="CBOE",
            )
        multi_source = await multi_runtime.snapshot(
            refresh=multi_refresh,
            preloaded_blocks=multi_source_preloaded,
        )
        apply_multi_source_context(contract, multi_source)
        contract["rates_expectations"] = self.fed_expectations.snapshot(
            refresh=refresh,
            provider_payload=(multi_source.get("blocks") or {}).get("investing_fed_rate_monitor") or {},
            macro_snapshot=contract.get("macro_snapshot") or {},
            event_calendar=contract.get("event_calendar") or {},
            legacy_block=contract.get("rates_expectations") or {},
            canonical_preflight=fed_expectations_preflight,
        )
        risk_context, risk_sentiment = await self.risk_context.snapshot(
            refresh=refresh,
            macro_snapshot=contract.get("macro_snapshot") or {},
            preloaded_risk_indices=(multi_source.get("blocks") or {}).get("cboe_risk_indices") or {},
            preloaded_qqq_options=(multi_source.get("blocks") or {}).get("nasdaq_qqq_options") or {},
            existing_legacy=contract.get("risk_sentiment") or {},
            canonical_preflight=risk_context_preflight,
        )
        contract["risk_context"] = risk_context
        contract["risk_sentiment"] = risk_sentiment
        self._record_multi_source_accounting(
            accounting_collector,
            blocks=multi_source.get("blocks") or {},
            risk_context=risk_context,
            vix_database_lookup=_canonical_freshness_evidence(
                vix_preflight[1],
            ),
            vix_provider_evidence=(
                macro_quality.get("vix_provider_evidence")
                if isinstance(macro_quality, dict)
                else None
            ),
            fed_database_lookup=getattr(
                self.fed_expectations,
                "last_database_lookup",
                None,
            ),
            risk_database_lookup=getattr(
                self.risk_context,
                "last_database_lookup",
                None,
            ),
        )
        contract["social_sentiment"] = await SocialSentimentService(self.settings).snapshot(refresh=refresh)
        return harden_market_context(contract, settings=self.settings)

    def _canonical_three_week_events(
        self,
        *,
        country: str,
        now: datetime,
        materializer: EconomicEventMaterializationService | None = None,
    ) -> list[Any]:
        """Read the complete previous/current/next local-week DB window."""

        calendar_timezone = ZoneInfo(
            str(
                self.settings.event_calendar_timezone
                or "America/New_York"
            )
        )
        local_now = now.astimezone(calendar_timezone)
        current_week_start = local_now.date() - timedelta(
            days=local_now.weekday()
        )
        window_start = datetime.combine(
            current_week_start - timedelta(days=7),
            datetime.min.time(),
            calendar_timezone,
        )
        window_end = datetime.combine(
            current_week_start + timedelta(days=13),
            datetime.max.time(),
            calendar_timezone,
        )
        events, _ = (
            materializer or self.event_materializer
        ).load_from_history(
            country=country,
            start=window_start,
            end=window_end,
            refresh_mode="false",
        )
        return events

    async def _force_schedule_catch_up(
        self,
        *,
        now: datetime,
        stage_for_atomic_finalization: bool = False,
    ) -> dict[str, Any]:
        if not stage_for_atomic_finalization:
            scheduler = ResearchSchedulerService(self.settings)
            result: dict[str, Any] = {}
            deadline = perf_counter() + max(
                float(self.settings.timeout_events_seconds),
                5.0,
            )
            while perf_counter() < deadline:
                result = await asyncio.to_thread(
                    scheduler._seed_canonical_schedule_gaps,
                    schedule_acquire=self.event_service.list_events,
                    now=now,
                    materialize_snapshot=False,
                )
                if (
                    result.get("reason")
                    != "schedule_catchup_single_flight_active"
                ):
                    return result
                await asyncio.sleep(0.05)
            raise TimeoutError(
                "force_schedule_single_flight_timeout"
            )
        with ForceGenerationStaging(self.settings) as staging:
            scheduler = ResearchSchedulerService(
                staging.stage_settings
            )
            result: dict[str, Any] = {}
            deadline = perf_counter() + max(
                float(self.settings.timeout_events_seconds),
                5.0,
            )
            while perf_counter() < deadline:
                result = await asyncio.to_thread(
                    scheduler._seed_canonical_schedule_gaps,
                    schedule_acquire=self.event_service.list_events,
                    now=now,
                    materialize_snapshot=False,
                )
                if (
                    result.get("reason")
                    != "schedule_catchup_single_flight_active"
                ):
                    stage_materializer = (
                        EconomicEventMaterializationService(
                            staging.stage_settings
                        )
                    )
                    events = self._canonical_three_week_events(
                        country="US",
                        now=now,
                        materializer=stage_materializer,
                    )
                    staged_result = {
                        **result,
                        "_canonical_generation_plan": (
                            staging.generation_plan()
                        ),
                        "_staged_events": events,
                    }
                    del stage_materializer
                    del scheduler
                    return staged_result
                await asyncio.sleep(0.05)
        raise TimeoutError("force_schedule_single_flight_timeout")

    def temporal_integrity(self) -> dict[str, Any]:
        model_facts = self.facts.search_facts(limit=1000)
        events = self._events_from_history(country="US", start=datetime.now(UTC) - timedelta(days=365), end=datetime.now(UTC) + timedelta(days=365))
        future_actual = []
        stale_as_recent = []
        awaiting_actual = []
        invalid_period = []
        duplicates = []
        now = datetime.now(UTC)
        for fact in model_facts:
            release_at = fact.get("release_at")
            kind = fact_temporal_kind(fact)
            state = fact_temporal_status(fact, now=now)
            if state == "pre_release" and fact.get("actual") not in (None, ""):
                future_actual.append(fact.get("fact_key"))
            freshness = freshness_label(valid_until=fact.get("valid_until"), release_at=release_at, actual=fact.get("actual"), now=now)
            if freshness in {"STALE", "EXPIRED"} and str(fact.get("freshness", "")).upper() == "RECENT":
                stale_as_recent.append(fact.get("fact_key"))
            if kind == "scheduled_release_event" and state == "awaiting_actual":
                awaiting_actual.append(fact.get("fact_key"))
        for event in events:
            summary = event.enrichment.summary or {}
            if summary.get("invalid_period_mapping"):
                invalid_period.append(event.event_id)
            if summary.get("is_duplicate"):
                duplicates.append(event.event_id)
        blocking = []
        if future_actual:
            blocking.append("future_actual_detected")
        if stale_as_recent:
            blocking.append("stale_as_recent_detected")
        return {
            "future_actual_count": len(future_actual),
            "stale_as_recent_count": len(stale_as_recent),
            "released_without_actual_count": len(awaiting_actual),
            "awaiting_actual_count": len(awaiting_actual),
            "invalid_period_mapping_count": len(invalid_period),
            "duplicates_count": len(duplicates),
            "future_actual": future_actual,
            "stale_as_recent": stale_as_recent,
            "awaiting_actual": awaiting_actual,
            "blocking_errors": blocking,
            "service_role": "data provider only",
        }

    def release_refresh_status(self) -> dict[str, Any]:
        retry_seconds = parse_retry_seconds(self.settings.release_refresh_retry_seconds)
        facts = self.facts.search_facts(limit=1000)
        awaiting = []
        for fact in facts:
            if fact_temporal_kind(fact) != "scheduled_release_event":
                continue
            state = fact_temporal_status(fact)
            if state != "awaiting_actual":
                continue
            raw = fact.get("raw_payload") if isinstance(fact.get("raw_payload"), dict) else {}
            attempt_count = int(raw.get("refresh_attempt_count") or 0)
            awaiting.append(
                {
                    "fact_key": fact.get("fact_key"),
                    "release_at": fact.get("release_at"),
                    "status": state,
                    "last_refresh_attempt_at": raw.get("last_refresh_attempt_at"),
                    "next_refresh_at": fact.get("next_refresh_at") or next_release_refresh_at(
                        release_at=fact.get("release_at"),
                        attempt_count=attempt_count,
                        retry_seconds=retry_seconds,
                    ),
                    "attempt_count": attempt_count,
                    "last_error": raw.get("last_error"),
                }
            )
        return {
            "retry_seconds": retry_seconds,
            "max_attempts": self.settings.max_release_refresh_attempts,
            "awaiting_actual": awaiting,
            "service_role": "data provider only",
        }

    def news_freshness(self) -> dict[str, Any]:
        rows = self.news.stored(days=365, limit=1000)
        invalid = [item for item in rows if news_content_status(item) == "invalid_content"]
        latest = [
            item
            for item in rows
            if news_content_status(item) != "invalid_content"
            and freshness_label(valid_until=item.get("valid_until")) not in {"STALE", "EXPIRED"}
        ]
        expired = [item for item in rows if freshness_label(valid_until=item.get("valid_until")) in {"STALE", "EXPIRED"}]
        return {
            "total_news": len(rows),
            "latest_eligible_count": len(latest),
            "expired_count": len(expired),
            "invalid_content_count": len(invalid),
            "stale_as_recent_count": 0,
            "expired_sample": [item.get("source_url") for item in expired[:10]],
            "service_role": "data provider only",
        }

    def source_classification(self) -> dict[str, Any]:
        news = self.news.stored(days=365, limit=1000)
        classified = [
            {
                "title": item.get("title"),
                "source": item.get("source"),
                "source_url": item.get("source_url"),
                **classify_source(item.get("source"), item.get("source_url")),
            }
            for item in news
        ]
        return {
            "official_count": sum(1 for item in classified if item["is_official_source"]),
            "market_count": sum(1 for item in classified if item["source_classification"] == "market_source"),
            "items": classified[:100],
            "service_role": "data provider only",
        }

    async def _single_run(self, *, country: str, days: int, symbol: str) -> dict[str, Any]:
        started = perf_counter()
        now = datetime.now(UTC)
        macro, macro_quality = await self._macro_db_first()
        events = await self._official_events(country=country, start=now, end=now + timedelta(days=days))
        enriched, enrichment_metadata = await self.enrichment_orchestrator.enrich_events(
            events=events,
            country=country,
            start=now,
            end=now + timedelta(days=days),
            trigger="diagnostics_e2e",
        )
        nasdaq_context, nasdaq_quality = await self._nasdaq_db_first(symbol=symbol)
        db_summary = self.facts.db_summary()
        quality = enrichment_metadata.get("data_quality", {})
        ai_diagnostics = self._latest_ai_diagnostics()
        db_hits = int(quality.get("db_hits", 0)) + macro_quality["db_hits"] + nasdaq_quality["db_hits"]
        db_misses = int(quality.get("db_misses", 0)) + macro_quality["db_misses"] + nasdaq_quality["db_misses"]
        provider_hits = int(quality.get("provider_hits", 0)) + macro_quality["provider_hits"] + nasdaq_quality["provider_hits"]
        provider_failures = int(quality.get("provider_failures", 0)) + macro_quality["provider_failures"] + nasdaq_quality["provider_failures"]
        return {
            "duration_ms": int((perf_counter() - started) * 1000),
            "db_hits": db_hits,
            "db_misses": db_misses,
            "provider_hits": provider_hits,
            "provider_failures": provider_failures,
            "ai_research_used": bool(quality.get("ai_research_used", False)),
            "ai_research_called": bool(quality.get("ai_research_called", False)),
            "ai_research_succeeded": bool(quality.get("ai_research_succeeded", False)),
            "ai_research_requests": int(quality.get("ai_research_requests", 0) or 0),
            "ai_events_requested": int(quality.get("ai_events_requested", 0) or 0),
            "ai_results_valid": int(quality.get("ai_results_valid", 0) or 0),
            "ai_results_rejected": int(quality.get("ai_results_rejected", 0) or 0),
            "ai_failure_reason": quality.get("ai_failure_reason"),
            "prompt_length_chars": ai_diagnostics.get("prompt_length_chars"),
            "prompt_line_count": ai_diagnostics.get("prompt_line_count"),
            "prompt_contains_input": ai_diagnostics.get("prompt_contains_input"),
            "input_event_count": ai_diagnostics.get("input_event_count"),
            "web_search_enabled": ai_diagnostics.get("web_search_enabled"),
            "stdout_length": ai_diagnostics.get("stdout_length"),
            "json_found": ai_diagnostics.get("json_found"),
            "parsed_result_count": ai_diagnostics.get("parsed_result_count"),
            "validation_errors": ai_diagnostics.get("validation_errors", []),
            "facts_total_after_run": db_summary["market_facts"]["total"],
            "active_facts_after_run": db_summary["market_facts"]["active"],
            "news_total_after_run": db_summary["market_news"]["total"],
            "missing_critical_fields": quality.get("missing_critical_fields", []),
            "stale_fields": quality.get("stale_fields", []),
            "warnings": quality.get("warnings", []) + macro_quality["warnings"] + nasdaq_quality["warnings"],
            "errors": quality.get("errors", []) + macro_quality["errors"] + nasdaq_quality["errors"],
            "_model_preview": {
                "macro_series_count": len(macro.series),
                "upcoming_high_impact_events_count": len(enriched),
                "news_count": self._news_count(nasdaq_context),
                "nasdaq_context_sections": (
                    ["qqq_holdings_summary", "mega_cap_snapshot", "mega_cap_breadth", "upcoming_earnings", "latest_news"]
                    if nasdaq_context else []
                ),
            },
        }

    def _vix_database_lookup(
        self,
    ) -> tuple[
        dict[str, Any] | None,
        CanonicalFreshnessResult,
    ]:
        rows = self.facts.get_valid_facts_by_type(
            "official_macro_latest",
            allow_stale=True,
        )
        fact = _matching_macro_fact(rows, ("VIXCLS",))
        freshness = self.freshness.evaluate_canonical(
            fact,
            max_age=VIX_MAX_AGE,
            data_reference_mode="point_in_time",
        )
        return fact, freshness

    async def _macro_db_first(
        self,
        *,
        fetch_missing: bool = True,
        force: bool = False,
        accounting_collector: (
            RequestProviderAccountingCollector | None
        ) = None,
        vix_preflight: (
            tuple[
                dict[str, Any] | None,
                CanonicalFreshnessResult,
            ]
            | None
        ) = None,
    ) -> tuple[MacroLatestResponse, dict[str, Any]]:
        # ``force`` starts a new acquisition decision; it never bypasses the
        # canonical repository.
        lookup_performed = True
        lookup_rows = self.facts.get_valid_facts_by_type(
            "official_macro_latest",
            allow_stale=True,
        )
        include_vix = vix_preflight is not None
        vix_fact = vix_preflight[0] if vix_preflight else None
        vix_freshness = vix_preflight[1] if vix_preflight else None
        cached_by_dataset: dict[str, dict[str, Any]] = {}
        for dataset_id, series_ids in MACRO_ACCOUNTING_SERIES.items():
            fact = _matching_macro_fact(lookup_rows, series_ids)
            decision = self.freshness.evaluate_canonical(
                fact,
                max_age=MACRO_ACCOUNTING_MAX_AGE[dataset_id],
            )
            if fact is not None and decision.usable:
                cached_by_dataset[dataset_id] = fact
        cached = list(
            {
                str(fact.get("fact_key")): fact
                for fact in (
                    *cached_by_dataset.values(),
                    *(
                        (vix_fact,)
                        if include_vix
                        and vix_fact is not None
                        and vix_freshness is not None
                        and vix_freshness.usable
                        else ()
                    ),
                )
            }.values()
        )
        if (
            len(cached_by_dataset)
            == len(MACRO_ACCOUNTING_SERIES)
            and (
                not include_vix
                or (
                    vix_freshness is not None
                    and vix_freshness.usable
                )
            )
        ):
            macro = self._macro_from_facts(cached)
            self._record_macro_accounting(
                accounting_collector,
                lookup_performed=lookup_performed,
                lookup_rows=lookup_rows,
                macro=macro,
                provider_batch_called=False,
                provider_not_called_reason=(
                    "VALID_DATABASE_RECORD_SELECTED"
                ),
            )
            return macro, {
                "db_hits": len(cached),
                "db_misses": 0,
                "provider_hits": 0,
                "provider_failures": 0,
                "provider_calls": 0,
                "actual_network_calls": 0,
                "warnings": ["macro_loaded_from_db_preview_only"],
                "errors": [],
            }
        if not fetch_missing:
            self._record_macro_accounting(
                accounting_collector,
                lookup_performed=lookup_performed,
                lookup_rows=lookup_rows,
                macro=MacroLatestResponse(),
                provider_batch_called=False,
                provider_not_called_reason=(
                    "REFRESH_DISABLED_AFTER_DATABASE_LOOKUP"
                ),
            )
            return MacroLatestResponse(), {
                "db_hits": 0,
                "db_misses": 1,
                "provider_hits": 0,
                "provider_failures": 0,
                "provider_calls": 0,
                "actual_network_calls": 0,
                "warnings": ["macro_not_in_db_refresh_false"],
                "errors": [],
            }
        latest_parameters = inspect.signature(
            self.macro_service.latest
        ).parameters
        due_series = {
            series_id
            for dataset_id, series_ids in MACRO_ACCOUNTING_SERIES.items()
            if dataset_id not in cached_by_dataset
            for series_id in series_ids
        }
        if (
            include_vix
            and vix_freshness is not None
            and not vix_freshness.usable
        ):
            due_series.add("VIXCLS")
        requested_series = {
            provider: tuple(
                sorted(due_series.intersection(provider_series))
            )
            for provider, provider_series in MACRO_PROVIDER_SERIES.items()
            if due_series.intersection(provider_series)
        }
        latest_kwargs: dict[str, Any] = {}
        if "force" in latest_parameters:
            latest_kwargs["force"] = force
        if "requested_series" in latest_parameters:
            latest_kwargs["requested_series"] = requested_series
        macro = await self.macro_service.latest(**latest_kwargs)
        vix_provider_evidence = _vix_provider_evidence(
            macro
        )
        written = self._save_macro(macro)
        read_back_rows = self.facts.get_valid_facts_by_type(
            "official_macro_latest",
            allow_stale=True,
        )
        read_back = []
        for dataset_id, series_ids in MACRO_ACCOUNTING_SERIES.items():
            fact = _matching_macro_fact(read_back_rows, series_ids)
            if (
                fact is not None
                and self.freshness.evaluate_canonical(
                    fact,
                    max_age=MACRO_ACCOUNTING_MAX_AGE[dataset_id],
                ).usable
            ):
                read_back.append(fact)
        if include_vix:
            read_back_vix = _matching_macro_fact(
                read_back_rows,
                ("VIXCLS",),
            )
            if (
                read_back_vix is not None
                and self.freshness.evaluate_canonical(
                    read_back_vix,
                    max_age=VIX_MAX_AGE,
                    data_reference_mode="point_in_time",
                ).usable
            ):
                read_back.append(read_back_vix)
        read_back = list(
            {
                str(fact.get("fact_key")): fact
                for fact in [*cached, *read_back]
            }.values()
        )
        read_back_macro = self._macro_from_facts(read_back) if read_back else MacroLatestResponse(provider_results=macro.provider_results)
        provider_failures = sum(1 for item in macro.provider_results if item.errors)
        self._record_macro_accounting(
            accounting_collector,
            lookup_performed=lookup_performed,
            lookup_rows=lookup_rows,
            macro=macro,
            provider_batch_called=True,
            provider_not_called_reason=None,
        )
        return read_back_macro, {
            "db_hits": len(read_back),
            "db_misses": 1,
            "provider_hits": written,
            "provider_failures": provider_failures,
            "provider_calls": len(macro.provider_results),
            "actual_network_calls": len(macro.provider_results),
            "vix_provider_evidence": vix_provider_evidence,
            "read_back_count": len(read_back),
            "materialized_count": len(read_back_macro.series),
            "warnings": [],
            "errors": [error for item in macro.provider_results for error in item.errors],
        }

    def _macro_from_facts(self, facts: list[dict[str, Any]]) -> MacroLatestResponse:
        metadata_by_source: dict[str, ProviderMetadata] = {}
        series = []
        for fact in facts:
            raw = fact.get("raw_payload") if isinstance(fact.get("raw_payload"), dict) else {}
            series_id = raw.get("series_id") or fact.get("category") or fact.get("fact_key")
            source = fact.get("source") or "DB"
            provider_type = fact.get("provider_type") or "DB"
            try:
                parsed_provider_type = ProviderType(provider_type)
            except ValueError:
                parsed_provider_type = ProviderType.DB
            retrieved_at_raw = str(fact.get("retrieved_at") or datetime.now(UTC).isoformat()).replace("Z", "+00:00")
            metadata_by_source.setdefault(
                source,
                ProviderMetadata(
                    source=source,
                    provider_type=parsed_provider_type,
                    retrieved_at=datetime.fromisoformat(retrieved_at_raw),
                    freshness=Freshness.RECENT,
                    reliability=fact.get("reliability") or 0,
                    is_fallback=False,
                ),
            )
            try:
                value = float(fact["value"]) if fact.get("value") is not None else None
            except (TypeError, ValueError):
                value = None
            series.append(
                MacroSeries(
                    series_id=str(series_id),
                    name=str(fact.get("event_name") or fact.get("category") or "macro fact"),
                    value=value,
                    units=fact.get("unit"),
                    data_as_of=fact.get("release_at"),
                    source=source,
                    metadata=metadata_by_source[source],
                )
            )
        return MacroLatestResponse(series=series, provider_results=list(metadata_by_source.values()))

    def _save_macro(self, macro: MacroLatestResponse) -> int:
        count = 0
        valid_until = (datetime.now(UTC) + timedelta(hours=self.settings.default_fact_ttl_hours)).isoformat()
        for series in _canonical_macro_series(macro.series):
            self.facts.upsert_fact(
                {
                    "fact_key": f"US:{str(series.series_id).upper()}:latest:official_macro_latest",
                    "fact_type": "official_macro_latest",
                    "country": "US",
                    "category": str(series.series_id).upper(),
                    "event_name": series.name,
                    "value": None if series.value is None else str(series.value),
                    "unit": series.units,
                    "source": series.source,
                    "provider_type": (
                        ProviderType.API.value
                        if (
                            series.metadata.provider_type
                            == ProviderType.CACHE
                            and str(series.source).upper()
                            in {"FRED", "BLS", "BEA", "CENSUS"}
                        )
                        else series.metadata.provider_type.value
                    ),
                    "reliability": series.metadata.reliability,
                    "confidence": series.metadata.reliability,
                    "retrieved_at": series.metadata.retrieved_at.isoformat(),
                    "release_at": series.data_as_of,
                    "valid_until": valid_until,
                    "next_refresh_at": valid_until,
                    "raw_payload_json": series.model_dump(mode="json"),
                }
            )
            count += 1
        return count

    async def _official_events(self, *, country: str, start: datetime, end: datetime):
        events = await self._fetch_official_events(
            country=country,
            start=start,
            end=end,
        )
        self._persist_official_events(events)
        return events

    async def _fetch_official_events(
        self,
        *,
        country: str,
        start: datetime,
        end: datetime,
    ):
        if hasattr(self.event_service, "list_events"):
            events = await self.event_service.list_events(country=country, start=start, end=end, enrich=False)
        else:
            events = await self.event_service.upcoming(country=country, days=max(1, (end - start).days))
        return events

    def _persist_official_events(self, events: list[Any]) -> None:
        for event in events:
            valid_until = self.freshness.macro_valid_until(event)
            self.facts.upsert_economic_event(
                event,
                event_key=f"{event.country}:{event.date}:{event.event_id}",
                valid_until=valid_until,
            )

    def _events_from_history(self, *, country: str, start: datetime, end: datetime) -> list:
        events, _ = self.event_materializer.load_from_history(
            country=country,
            start=start,
            end=end,
            refresh_mode="false",
        )
        return events

    async def _nasdaq_db_first(
        self,
        *,
        symbol: str,
        fetch_missing: bool = True,
        force: bool = False,
        fetch_news: bool = True,
        accounting_collector: (
            RequestProviderAccountingCollector | None
        ) = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        lookup_performed = True
        lookup_rows = {
            fact_type: self.facts.get_valid_facts_by_type(
                fact_type,
                allow_stale=True,
            )
            for fact_type in (
                "qqq_holdings",
                "mega_cap_snapshot",
                "mega_cap_breadth",
                "earnings_event",
                "nasdaq_context",
            )
        }
        facts_by_type: dict[str, list[dict[str, Any]]] = {
            fact_type: [] for fact_type in lookup_rows
        }
        valid_datasets: set[str] = set()
        for dataset_id, fact_types in NASDAQ_ACCOUNTING_FACT_TYPES.items():
            for fact_type in fact_types:
                for fact in lookup_rows[fact_type]:
                    if self.freshness.evaluate_canonical(
                        fact,
                        max_age=NASDAQ_ACCOUNTING_MAX_AGE[dataset_id],
                        data_reference_mode="point_in_time",
                    ).usable:
                        facts_by_type[fact_type].append(fact)
                        valid_datasets.add(dataset_id)
        cached_count = sum(len(items) for items in facts_by_type.values())
        materialized = materialize_nasdaq_context_from_facts(
            facts_by_type
        )
        if materialized:
            qqq = materialized.get("qqq_holdings") or {}
            log_weight_event(
                "qqq_weight_materialized",
                source=qqq.get("weight_source")
                or qqq.get("source"),
                method=qqq.get("weight_method"),
                constituent_count=qqq.get("holdings_count"),
                stale=False,
            )

        provider_datasets = {
            dataset_id
            for dataset_id in ("nasdaq_100", "mega_cap_quotes")
            if dataset_id not in valid_datasets
        }
        provider_required = bool(
            fetch_missing
            and (provider_datasets or fetch_news)
        )
        earnings_preloaded = self._earnings_preloaded_block(
            lookup_rows,
        )
        base_quality: dict[str, Any] = {
            "db_hits": cached_count,
            "db_misses": len(provider_datasets),
            "provider_hits": 0,
            "provider_failures": 0,
            "provider_calls": 0,
            "actual_network_calls": 0,
            "warnings": [],
            "errors": [],
        }
        if earnings_preloaded is not None:
            base_quality["_earnings_preloaded_block"] = (
                earnings_preloaded
            )

        if not provider_required:
            self._record_nasdaq_accounting(
                accounting_collector,
                lookup_performed=lookup_performed,
                lookup_rows=lookup_rows,
                provider_invoked=False,
                provider_invoked_datasets=set(),
                context=None,
                provider_error=None,
            )
            if materialized is None:
                base_quality["warnings"] = [
                    "nasdaq_context_not_in_db"
                ]
            return materialized, base_quality
        try:
            context_parameters = inspect.signature(
                self.nasdaq_data_service.context
            ).parameters
            context_kwargs: dict[str, Any] = {"force": force}
            optional_context_kwargs = {
                "fetch_news": fetch_news,
                "fetch_holdings": (
                    "nasdaq_100" in provider_datasets
                ),
                "fetch_mega_cap": (
                    "mega_cap_quotes" in provider_datasets
                ),
                "fetch_earnings": False,
                "preloaded_holdings": (
                    (materialized or {}).get("qqq_holdings")
                    if "nasdaq_100" in valid_datasets
                    else None
                ),
            }
            context_kwargs.update(
                {
                    name: value
                    for name, value in optional_context_kwargs.items()
                    if name in context_parameters
                }
            )
            context = await self.nasdaq_data_service.context(
                **context_kwargs,
            )
        except Exception as exc:
            self._record_nasdaq_accounting(
                accounting_collector,
                lookup_performed=lookup_performed,
                lookup_rows=lookup_rows,
                provider_invoked=True,
                provider_invoked_datasets=provider_datasets,
                context=None,
                provider_error=(
                    str(exc) or type(exc).__name__
                ),
            )
            return materialized, {
                **base_quality,
                "provider_hits": 0,
                "provider_failures": 1,
                "errors": [str(exc) or type(exc).__name__],
            }
        written = self._save_nasdaq_context(
            context,
            include_datasets=provider_datasets,
        )
        self._record_nasdaq_accounting(
            accounting_collector,
            lookup_performed=lookup_performed,
            lookup_rows=lookup_rows,
            provider_invoked=True,
            provider_invoked_datasets=provider_datasets,
            context=context,
            provider_error=None,
        )
        provider_materialized = normalize_nasdaq_context(context)
        merged = _merge_nasdaq_materializations(
            materialized,
            provider_materialized,
            provider_datasets=provider_datasets,
        )
        news_quality_model = getattr(
            context.latest_news,
            "data_quality",
            None,
        )
        news_quality = (
            news_quality_model.model_dump(mode="json")
            if hasattr(news_quality_model, "model_dump")
            else dict(news_quality_model or {})
        )
        return merged, {
            **base_quality,
            "provider_hits": written,
            "provider_calls": int(context.metadata.get("provider_calls") or 0),
            "actual_network_calls": int(context.metadata.get("actual_network_calls") or 0),
            "run_deduplicated_calls": int(context.metadata.get("run_deduplicated_calls") or 0),
            "warnings": list(context.metadata.get("warnings", [])),
            "errors": list(context.metadata.get("critical_errors", [])),
            "_news_provider_quality": news_quality,
        }

    def _earnings_preloaded_block(
        self,
        lookup_rows: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any] | None:
        for fact in lookup_rows.get("earnings_event", []):
            freshness = self.freshness.evaluate_canonical(
                fact,
                max_age=NASDAQ_ACCOUNTING_MAX_AGE["earnings"],
                data_reference_mode="point_in_time",
            )
            if not freshness.usable:
                continue
            raw = (
                fact.get("raw_payload")
                if isinstance(fact.get("raw_payload"), dict)
                else {}
            )
            payload = {
                **raw,
                "events": list(raw.get("events") or []),
                "relevant_upcoming": list(
                    raw.get("relevant_upcoming")
                    or raw.get("events")
                    or []
                ),
            }
            return {
                **payload,
                **_canonical_preflight_runtime_block(
                    payload,
                    lookup=_canonical_freshness_evidence(
                        freshness
                    ),
                    provider_source=(
                        "Nasdaq Earnings Calendar"
                    ),
                ),
            }
        return None

    def _save_nasdaq_context(
        self,
        context: NasdaqContextResponse,
        *,
        include_datasets: set[str] | None = None,
    ) -> int:
        valid_until = (datetime.now(UTC) + timedelta(hours=self.settings.qqq_holdings_ttl_hours)).isoformat()
        payload = context.model_dump(mode="json")
        facts = [
            ("nasdaq_context:qqq_holdings", "qqq_holdings", payload.get("qqq_holdings") or payload.get("qqq_holdings_summary")),
            ("nasdaq_context:mega_cap_snapshot", "mega_cap_snapshot", payload.get("mega_cap_snapshot")),
            ("nasdaq_context:mega_cap_breadth", "mega_cap_breadth", payload.get("mega_cap_breadth")),
            ("nasdaq_context:earnings", "earnings_event", payload.get("upcoming_earnings")),
        ]
        written = 0
        for fact_key, fact_type, raw in facts:
            dataset_id = (
                "nasdaq_100"
                if fact_type == "qqq_holdings"
                else "mega_cap_quotes"
                if fact_type
                in {"mega_cap_snapshot", "mega_cap_breadth"}
                else "earnings"
            )
            if (
                include_datasets is not None
                and dataset_id not in include_datasets
            ):
                continue
            if not raw:
                continue
            retrieved_at = (
                raw.get("retrieved_at", now_iso())
                if isinstance(raw, dict)
                else now_iso()
            )
            data_as_of = (
                raw.get("data_as_of")
                or raw.get("as_of")
                or raw.get("weight_as_of")
                or retrieved_at
                if isinstance(raw, dict)
                else retrieved_at
            )
            fact_valid_until = (
                raw.get("weight_valid_until")
                if fact_type == "qqq_holdings" and isinstance(raw, dict)
                else valid_until
            ) or valid_until
            persisted_raw = (
                {
                    **raw,
                    "data_as_of": data_as_of,
                    "content_valid_until": fact_valid_until,
                    "refresh_due_at": fact_valid_until,
                }
                if isinstance(raw, dict)
                else raw
            )
            self.facts.upsert_fact(
                {
                    "fact_key": fact_key,
                    "fact_type": fact_type,
                    "symbol": "QQQ",
                    "category": fact_type,
                    "source": raw.get("source") if isinstance(raw, dict) else "Nasdaq context",
                    "provider_type": raw.get("provider_type") if isinstance(raw, dict) else "API",
                    "reliability": raw.get("reliability", 0) if isinstance(raw, dict) else 0,
                    "confidence": raw.get("reliability", 0) if isinstance(raw, dict) else 0,
                    "retrieved_at": retrieved_at,
                    "release_at": data_as_of,
                    "valid_until": fact_valid_until,
                    "next_refresh_at": fact_valid_until,
                    "raw_payload_json": persisted_raw,
                }
            )
            if fact_type == "qqq_holdings" and isinstance(raw, dict):
                log_weight_event(
                    "qqq_weight_persisted",
                    source=raw.get("weight_source") or raw.get("source"),
                    method=raw.get("weight_method"),
                    constituent_count=raw.get("holdings_count") or len(raw.get("holdings") or []),
                    total_weight_pct=(raw.get("data_quality") or {}).get("total_weight_pct"),
                )
            written += 1
        for article in (payload.get("latest_news") or {}).get("articles", []):
            try:
                self.news.upsert_news(article)
            except Exception:
                continue
        return written

    @staticmethod
    def _news_count(nasdaq_context: dict[str, Any] | None) -> int:
        if not nasdaq_context:
            return 0
        latest_news = nasdaq_context.get("latest_news") or {}
        return len(latest_news.get("articles") or [])
    def _latest_ai_requests(self) -> int:
        with connect_market_db(self.settings) as conn:
            row = conn.execute(
                "SELECT ai_research_requests FROM enrichment_runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
        return int(row["ai_research_requests"]) if row else 0

    def _latest_ai_diagnostics(self) -> dict[str, Any]:
        with connect_market_db(self.settings) as conn:
            row = conn.execute(
                """
                SELECT raw_payload_json
                FROM provider_observations
                WHERE provider_name = 'ai_researcher'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        if not row or not row["raw_payload_json"]:
            return {}
        import json

        try:
            payload = json.loads(row["raw_payload_json"])
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _provider_observation_summary(self) -> dict[str, Any]:
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                "SELECT provider_name, status, COUNT(*) c FROM provider_observations GROUP BY provider_name, status"
            ).fetchall()
        return {"by_provider_status": [dict(row) for row in rows]}

    def _record_macro_accounting(
        self,
        collector: RequestProviderAccountingCollector | None,
        *,
        lookup_performed: bool,
        lookup_rows: list[dict[str, Any]],
        macro: MacroLatestResponse,
        provider_batch_called: bool,
        provider_not_called_reason: str | None,
    ) -> None:
        if collector is None:
            return
        shared = tuple(MACRO_ACCOUNTING_SERIES)
        provider_results = {
            str(item.source or "").upper(): item
            for item in macro.provider_results
        }
        configured_sources = {
            str(
                getattr(
                    provider,
                    "source",
                    type(provider).__name__.replace("Provider", ""),
                )
            ).upper()
            for provider in self.macro_service.providers
        }
        for dataset_id, series_ids in MACRO_ACCOUNTING_SERIES.items():
            policy = collector.policies[dataset_id]
            fact = _matching_macro_fact(lookup_rows, series_ids)
            freshness = (
                self.freshness.evaluate_canonical(
                    fact,
                    max_age=MACRO_ACCOUNTING_MAX_AGE[dataset_id],
                )
                if fact is not None
                else None
            )
            selected = next(
                (
                    item
                    for item in macro.series
                    if str(item.series_id).upper() in series_ids
                ),
                None,
            )
            def observed_attempt(provider: str) -> tuple[dict[str, Any], bool]:
                if fact is not None and freshness and freshness.usable:
                    return (
                        provider_attempt(
                            provider,
                            called=False,
                            attempts=0,
                            result="NOT_CALLED",
                            not_called_reason="VALID_DATABASE_RECORD_SELECTED",
                            execution_origin="CACHE_DECISION",
                        ),
                        True,
                    )
                metadata = provider_results.get(provider)
                if provider_batch_called and metadata is not None:
                    from_cache = (
                        metadata.provider_type == ProviderType.CACHE
                    )
                    matching_value = bool(
                        selected is not None
                        and str(selected.source or "").upper()
                        == provider
                    )
                    return (
                        provider_attempt(
                            provider,
                            called=not from_cache,
                            attempts=0 if from_cache else 1,
                            result=(
                                "CACHE_HIT"
                                if from_cache
                                else "FAILED"
                                if metadata.errors
                                else "SUCCESS"
                                if matching_value
                                else "NO_DATA"
                            ),
                            not_called_reason=(
                                "PROVIDER_ADAPTER_CACHE_HIT"
                                if from_cache
                                else None
                            ),
                            execution_origin=(
                                "CACHE_DECISION"
                                if from_cache
                                else "PROVIDER_CALL"
                            ),
                        ),
                        not from_cache,
                    )
                provider_configured = (
                    provider.replace("_", "")
                    in {
                        item.replace("_", "")
                        for item in configured_sources
                    }
                )
                return (
                    provider_attempt(
                        provider,
                        called=False,
                        attempts=0,
                        result="NOT_CALLED",
                        not_called_reason=(
                            "PROVIDER_NOT_CONFIGURED_IN_MACRO_BATCH"
                            if not provider_configured
                            else provider_not_called_reason
                            or "PROVIDER_RESULT_EVIDENCE_MISSING"
                        ),
                        execution_origin="OBSERVED_SKIP",
                    ),
                    not provider_configured,
                )

            primary, primary_complete = observed_attempt(
                policy.primary_provider
            )
            fallback_attempts: list[dict[str, Any]] = []
            fallback_complete = True
            for fallback_provider in policy.fallback_providers:
                attempt, complete = observed_attempt(fallback_provider)
                fallback_attempts.append(attempt)
                fallback_complete = fallback_complete and complete
            evidence_complete = primary_complete and fallback_complete
            collector.record(
                dataset_id,
                acquisition_id="macro_db_provider_batch",
                shared_dataset_ids=shared,
                database_lookup_performed=lookup_performed,
                database_lookup_reason=(
                    "OFFICIAL_MACRO_DATABASE_LOOKUP"
                    if lookup_performed
                    else "FORCE_REFRESH_BYPASSED_DATABASE_LOOKUP"
                ),
                database_record_found=(
                    fact is not None if lookup_performed else None
                ),
                database_data_as_of=(
                    freshness.data_as_of
                    if lookup_performed and freshness
                    else None
                ),
                database_content_valid_until=(
                    freshness.content_valid_until
                    if lookup_performed and freshness
                    else None
                ),
                database_refresh_due_at=(
                    freshness.refresh_due_at
                    if lookup_performed and freshness
                    else None
                ),
                database_lifecycle_status=(
                    freshness.lifecycle
                    if lookup_performed and freshness
                    else None
                ),
                database_record_expired=(
                    bool(fact and freshness and freshness.expired)
                    if lookup_performed
                    else None
                ),
                database_freshness_evaluation=(
                    freshness.evaluation
                    if fact and freshness
                    else "NOT_FOUND"
                    if lookup_performed
                    else "NOT_LOOKED_UP"
                ),
                primary_provider=primary,
                fallbacks=fallback_attempts,
                acquisition_selected_source=(
                    selected.source
                    if selected is not None
                    else fact.get("source")
                    if fact
                    else None
                ),
                acquisition_reason_code=(
                    "PROVIDER_VALUE_ACQUIRED"
                    if selected is not None and provider_batch_called
                    else "VALID_DATABASE_RECORD_SELECTED"
                    if fact and freshness and freshness.usable
                    else "VALUE_NOT_ACQUIRED"
                ),
                evidence_complete=evidence_complete,
            )

    def _record_nasdaq_accounting(
        self,
        collector: RequestProviderAccountingCollector | None,
        *,
        lookup_performed: bool,
        lookup_rows: dict[str, list[dict[str, Any]]],
        provider_invoked: bool,
        provider_invoked_datasets: set[str] | None = None,
        context: NasdaqContextResponse | None,
        provider_error: str | None,
    ) -> None:
        if collector is None:
            return
        invoked_datasets = (
            set(provider_invoked_datasets)
            if provider_invoked_datasets is not None
            else {
                "nasdaq_100",
                "mega_cap_quotes",
            }
            if provider_invoked
            else set()
        )
        for dataset_id, fact_types in NASDAQ_ACCOUNTING_FACT_TYPES.items():
            # Earnings is acquired and accounted by the dedicated Nasdaq
            # earnings block later in this same request.
            if dataset_id == "earnings":
                continue
            policy = collector.policies[dataset_id]
            facts = [
                fact
                for fact_type in fact_types
                for fact in lookup_rows.get(fact_type, [])
            ]
            evaluated_facts = [
                (
                    fact,
                    self.freshness.evaluate_canonical(
                        fact,
                        max_age=NASDAQ_ACCOUNTING_MAX_AGE[
                            dataset_id
                        ],
                        data_reference_mode="point_in_time",
                    ),
                )
                for fact in facts
            ]
            selected_evidence = next(
                (
                    item
                    for item in evaluated_facts
                    if item[1].usable
                ),
                evaluated_facts[0]
                if evaluated_facts
                else None,
            )
            fact = (
                selected_evidence[0]
                if selected_evidence
                else None
            )
            freshness = (
                selected_evidence[1]
                if selected_evidence
                else None
            )
            quality = {}
            section = None
            if context is not None:
                section = (
                    context.qqq_holdings
                    if dataset_id == "nasdaq_100"
                    else context.mega_cap_snapshot
                )
                quality_model = getattr(section, "data_quality", None)
                quality = (
                    quality_model.model_dump(mode="json")
                    if hasattr(quality_model, "model_dump")
                    else dict(quality_model or {})
                )
            accounts = {
                str(item.get("provider")): item
                for item in quality.get(
                    "provider_accounting",
                    [],
                )
                if isinstance(item, dict)
                and item.get("provider")
            }
            if fact is not None and freshness and freshness.usable:
                attempts = [
                    _database_selected_attempt(provider)
                    for provider in (
                        policy.primary_provider,
                        *policy.fallback_providers,
                    )
                ]
                complete = True
            else:
                attempts = []
                complete = (
                    dataset_id in invoked_datasets
                    and provider_error is None
                )
                for provider in (
                    policy.primary_provider,
                    *policy.fallback_providers,
                ):
                    account = accounts.get(provider)
                    if account is None:
                        attempts.append(
                            provider_attempt(
                                provider,
                                called=False,
                                attempts=0,
                                result="EVIDENCE_NOT_AVAILABLE",
                                not_called_reason=(
                                    "PROVIDER_EXECUTION_EVIDENCE_MISSING"
                                    if dataset_id
                                    in invoked_datasets
                                    else "REFRESH_DISABLED_AFTER_DATABASE_LOOKUP"
                                ),
                                execution_origin="OBSERVED_SKIP",
                            )
                        )
                        complete = False
                        continue
                    calls = int(account.get("calls") or 0)
                    called = bool(
                        account.get("called")
                        if "called" in account
                        else calls > 0
                    )
                    reason = str(
                        account.get("reason_code") or ""
                    ) or None
                    attempts.append(
                        provider_attempt(
                            provider,
                            called=called,
                            attempts=(
                                max(calls, 1)
                                if called
                                else 0
                            ),
                            result=str(
                                account.get("status")
                                or account.get("result")
                                or "UNKNOWN"
                            ),
                            not_called_reason=(
                                None if called else reason
                            ),
                            execution_origin=(
                                "PROVIDER_CALL"
                                if called
                                else "OBSERVED_SKIP"
                            ),
                        )
                    )
                    if not called and not reason:
                        complete = False
            attempt = attempts[0]
            fallback_attempts = attempts[1:]
            selected_source = (
                getattr(section, "source", None)
                if section is not None
                and bool(
                    quality.get("final_data_available")
                )
                else None
            )
            collector.record(
                dataset_id,
                acquisition_id=(
                    "qqq_holdings_db_provider_cascade"
                    if dataset_id == "nasdaq_100"
                    else "mega_cap_quotes_db_provider_cascade"
                ),
                shared_dataset_ids=(dataset_id,),
                database_lookup_performed=lookup_performed,
                database_lookup_reason=(
                    "NASDAQ_CONTEXT_DATABASE_LOOKUP"
                    if lookup_performed
                    else "FORCE_REFRESH_BYPASSED_DATABASE_LOOKUP"
                ),
                database_record_found=(
                    fact is not None if lookup_performed else None
                ),
                database_data_as_of=(
                    freshness.data_as_of
                    if lookup_performed and freshness
                    else None
                ),
                database_content_valid_until=(
                    freshness.content_valid_until
                    if lookup_performed and freshness
                    else None
                ),
                database_refresh_due_at=(
                    freshness.refresh_due_at
                    if lookup_performed and freshness
                    else None
                ),
                database_lifecycle_status=(
                    freshness.lifecycle
                    if lookup_performed and freshness
                    else None
                ),
                database_record_expired=(
                    bool(fact and freshness and freshness.expired)
                    if lookup_performed
                    else None
                ),
                database_freshness_evaluation=(
                    freshness.evaluation
                    if fact and freshness
                    else "NOT_FOUND"
                    if lookup_performed
                    else "NOT_LOOKED_UP"
                ),
                primary_provider=attempt,
                fallbacks=fallback_attempts,
                acquisition_selected_source=(
                    fact.get("source")
                    if fact
                    and freshness
                    and freshness.usable
                    else selected_source
                ),
                acquisition_reason_code=(
                    "NASDAQ_PROVIDER_VALUE_ACQUIRED"
                    if selected_source
                    and dataset_id in invoked_datasets
                    else "VALID_DATABASE_RECORD_SELECTED"
                    if fact and freshness and freshness.usable
                    else "NASDAQ_PROVIDER_CHAIN_EXHAUSTED"
                ),
                evidence_complete=complete,
            )

    def _record_calendar_accounting(
        self,
        collector: RequestProviderAccountingCollector | None,
        *,
        events: list[Any],
        database_lookup_performed: bool,
        provider_called: bool,
        provider_result: str,
        provider_not_called_reason: str | None,
        acquisition_reason_code: str,
        coverage: dict[str, Any] | None = None,
        fallback_blocks: list[dict[str, Any]] | None = None,
    ) -> None:
        if collector is None:
            return
        policy = collector.policies["macro_calendar"]
        event = events[0] if events else None
        lookup = _calendar_database_evidence(
            coverage or {},
            events=events,
            provider_called=provider_called,
        )
        database_valid = _database_lookup_is_valid(lookup)
        primary = provider_attempt(
            policy.primary_provider,
            called=provider_called,
            attempts=1 if provider_called else 0,
            result=provider_result,
            not_called_reason=provider_not_called_reason,
            execution_origin=(
                "PROVIDER_CALL"
                if provider_called
                else "CACHE_DECISION"
                if database_valid
                else "OBSERVED_SKIP"
            ),
        )
        fallback_attempts = [
            _runtime_or_database_skip_attempt(
                provider,
                block,
                database_valid=database_valid,
            )
            for provider, block in zip(
                policy.fallback_providers,
                fallback_blocks or [],
                strict=False,
            )
        ]
        while len(fallback_attempts) < len(
            policy.fallback_providers
        ):
            provider = policy.fallback_providers[
                len(fallback_attempts)
            ]
            fallback_attempts.append(
                provider_attempt(
                    provider,
                    called=False,
                    attempts=0,
                    result="NOT_CALLED",
                    not_called_reason=(
                        "VALID_DATABASE_RECORD_SELECTED"
                        if database_valid
                        else "PRIOR_PROVIDER_SUCCEEDED"
                        if _attempt_succeeded(primary)
                        else "FALLBACK_EXECUTION_EVIDENCE_MISSING"
                    ),
                    execution_origin=(
                        "CACHE_DECISION"
                        if database_valid
                        else "OBSERVED_SKIP"
                    ),
                )
            )
        collector.record(
            "macro_calendar",
            acquisition_id="canonical_event_calendar",
            shared_dataset_ids=("macro_calendar",),
            database_lookup_performed=True,
            database_lookup_reason=(
                "CANONICAL_EVENT_HISTORY_LOOKUP"
                if database_lookup_performed
                else "FORCE_EVENT_PROVIDER_PATH"
            ),
            database_record_found=lookup["found"],
            database_data_as_of=lookup["data_as_of"],
            database_content_valid_until=lookup[
                "content_valid_until"
            ],
            database_refresh_due_at=lookup["refresh_due_at"],
            database_lifecycle_status=lookup.get(
                "lifecycle_status"
            ),
            database_record_expired=lookup["expired"],
            database_freshness_evaluation=lookup["freshness"],
            primary_provider=primary,
            fallbacks=fallback_attempts,
            acquisition_selected_source=(
                _event_value(event, "source", "provider")
                if event is not None
                else None
            ),
            acquisition_reason_code=acquisition_reason_code,
            evidence_complete=bool(
                lookup["complete"]
                and len(fallback_attempts)
                == len(policy.fallback_providers)
            ),
        )

    def _record_news_accounting(
        self,
        collector: RequestProviderAccountingCollector | None,
        *,
        news_items: list[dict[str, Any]],
        database_item: dict[str, Any] | None,
        database_freshness: Any,
        provider_quality: dict[str, Any],
    ) -> None:
        if collector is None:
            return
        item = next(
            (
                candidate
                for candidate in news_items
                if _news_exclusion_reason(candidate) is None
                and self.freshness.evaluate_canonical(
                    candidate,
                    max_age=timedelta(hours=24),
                ).usable
            ),
            None,
        )
        database_valid = bool(
            database_item
            and database_freshness.usable
        )
        policy = collector.policies["current_news"]
        policy_providers = (
            policy.primary_provider,
            *policy.fallback_providers,
        )
        if database_valid:
            attempts = [
                _database_selected_attempt(provider)
                for provider in policy_providers
            ]
            evidence_complete = True
        else:
            raw_account_rows = [
                account
                for account in provider_quality.get(
                    "provider_accounting",
                    [],
                )
                if isinstance(account, dict)
            ]
            raw_accounts = {
                actual_name: account
                for account in raw_account_rows
                if (
                    actual_name := str(
                        account.get("provider") or ""
                    )
                )
            }
            attempts = []
            evidence_complete = bool(
                len(raw_account_rows) == len(policy_providers)
                and len(raw_accounts) == len(policy_providers)
                and set(raw_accounts)
                == set(NEWS_ACCOUNTING_PROVIDER_NAMES.values())
            )
            for provider in policy_providers:
                actual_name = NEWS_ACCOUNTING_PROVIDER_NAMES[
                    provider
                ]
                account = raw_accounts.get(actual_name)
                if account is None:
                    attempts.append(
                        provider_attempt(
                            provider,
                            called=False,
                            attempts=0,
                            result="EVIDENCE_NOT_AVAILABLE",
                            not_called_reason=(
                                "NEWS_PROVIDER_EXECUTION_EVIDENCE_MISSING"
                            ),
                            execution_origin="OBSERVED_SKIP",
                        )
                    )
                    evidence_complete = False
                    continue
                raw_calls = account.get("calls")
                calls = (
                    raw_calls
                    if isinstance(raw_calls, int)
                    and not isinstance(raw_calls, bool)
                    and raw_calls >= 0
                    else 0
                )
                declared_origin = str(
                    account.get("execution_origin") or ""
                ).upper()
                called = bool(
                    calls > 0
                    and declared_origin != "CACHE_DECISION"
                )
                reason = str(
                    account.get("reason_code") or ""
                ) or None
                attempts.append(
                    provider_attempt(
                        provider,
                        called=called,
                        attempts=calls,
                        result=str(
                            account.get("status")
                            or "UNKNOWN"
                        ),
                        not_called_reason=(
                            None if called else reason
                        ),
                        execution_origin=(
                            declared_origin
                            if declared_origin
                            in {
                                "PROVIDER_CALL",
                                "OBSERVED_SKIP",
                                "CACHE_DECISION",
                            }
                            else "PROVIDER_CALL"
                            if called
                            else "OBSERVED_SKIP"
                        ),
                    )
                )
                if not called and not reason:
                    evidence_complete = False
                if not _news_provider_execution_evidence_complete(
                    account
                ):
                    evidence_complete = False
        primary = attempts[0]
        fallbacks = attempts[1:]
        called_count = sum(
            attempt.get("called") is True
            for attempt in attempts
        )
        technical_cache_selected = bool(
            evidence_complete
            and attempts
            and all(
                attempt.get("execution_origin")
                == "CACHE_DECISION"
                for attempt in attempts
            )
        )
        collector.record(
            "current_news",
            acquisition_id="stored_market_news_lookup",
            shared_dataset_ids=("current_news",),
            database_lookup_performed=True,
            database_lookup_reason="MARKET_NEWS_DATABASE_LOOKUP",
            database_record_found=bool(database_item),
            database_data_as_of=(
                database_freshness.data_as_of
                if database_item
                else None
            ),
            database_content_valid_until=(
                database_freshness.content_valid_until
                if database_item
                else None
            ),
            database_refresh_due_at=(
                database_freshness.refresh_due_at
                if database_item
                else None
            ),
            database_lifecycle_status=(
                database_freshness.lifecycle
                if database_item
                else None
            ),
            database_record_expired=(
                database_freshness.expired
                if database_item
                else False
            ),
            database_freshness_evaluation=(
                database_freshness.evaluation
                if database_item
                else "NOT_FOUND"
            ),
            primary_provider=primary,
            fallbacks=fallbacks,
            acquisition_selected_source=(
                item.get("acquisition_provider")
                or item.get("provider")
                or item.get("source")
                if item
                else None
            ),
            acquisition_reason_code=(
                "PERSISTED_NEWS_SELECTED"
                if item and database_valid
                else "NEWS_TECHNICAL_CACHE_VALUE_SELECTED"
                if item and technical_cache_selected
                else "NEWS_FAN_IN_VALUE_ACQUIRED"
                if item and called_count
                else "NEWS_TECHNICAL_CACHE_NO_CURRENT_DATA"
                if technical_cache_selected
                else "NEWS_FAN_IN_COMPLETED_NO_CURRENT_DATA"
                if evidence_complete
                else "CURRENT_NEWS_NOT_AVAILABLE"
            ),
            evidence_complete=evidence_complete,
        )

    def _record_positioning_accounting(
        self,
        collector: RequestProviderAccountingCollector | None,
        *,
        payload: dict[str, Any],
        refresh: str,
    ) -> None:
        if collector is None:
            return
        lookup = (
            payload.get("database_lookup")
            if isinstance(payload.get("database_lookup"), dict)
            else {}
        )
        database_valid = _database_lookup_is_valid(lookup)
        called = bool(
            payload.get("attempted")
            and int(payload.get("provider_calls") or 0) > 0
        )
        collector.record(
            "positioning",
            acquisition_id="cftc_cot_db_provider",
            shared_dataset_ids=("positioning",),
            database_lookup_performed=bool(
                lookup.get("performed")
            ),
            database_lookup_reason=(
                "CFTC_COT_DATABASE_LOOKUP"
            ),
            database_record_found=lookup.get("found"),
            database_data_as_of=lookup.get("data_as_of"),
            database_content_valid_until=lookup.get(
                "content_valid_until"
            ),
            database_refresh_due_at=lookup.get("refresh_due_at"),
            database_lifecycle_status=lookup.get(
                "lifecycle_status"
            ),
            database_record_expired=lookup.get("expired"),
            database_freshness_evaluation=str(
                lookup.get("freshness") or "NOT_LOOKED_UP"
            ),
            primary_provider=provider_attempt(
                "CFTC",
                called=called,
                attempts=int(payload.get("provider_calls") or 0),
                result=(
                    str(payload.get("status") or "NO_DATA").upper()
                    if called
                    else "NOT_CALLED"
                ),
                not_called_reason=(
                    None
                    if called
                    else "VALID_DATABASE_RECORD_SELECTED"
                    if database_valid
                    else "PROVIDER_EXECUTION_SKIPPED"
                ),
                execution_origin=(
                    "PROVIDER_CALL"
                    if called
                    else "CACHE_DECISION"
                    if database_valid
                    else "OBSERVED_SKIP"
                ),
            ),
            fallbacks=[],
            acquisition_selected_source=(
                payload.get("source")
                if payload.get("status") == "found"
                else None
            ),
            acquisition_reason_code=(
                "CFTC_POSITIONING_ACQUIRED"
                if payload.get("status") == "found"
                else "CFTC_POSITIONING_NOT_AVAILABLE"
            ),
        )

    def _record_multi_source_accounting(
        self,
        collector: RequestProviderAccountingCollector | None,
        *,
        blocks: dict[str, dict[str, Any]],
        risk_context: dict[str, Any],
        vix_database_lookup: dict[str, Any] | None,
        vix_provider_evidence: dict[str, Any] | None,
        fed_database_lookup: dict[str, Any] | None,
        risk_database_lookup: dict[str, Any] | None,
    ) -> None:
        if collector is None:
            return
        self._record_block_dataset(
            collector,
            dataset_id="fomc_expectations",
            acquisition_id="investing_fed_rate_monitor",
            block=blocks.get("investing_fed_rate_monitor") or {},
            lookup_override=fed_database_lookup,
            database_lookup_reason=(
                "FED_EXPECTATIONS_DATABASE_LOOKUP"
            ),
        )
        self._record_block_dataset(
            collector,
            dataset_id="earnings",
            acquisition_id="nasdaq_earnings_calendar",
            block=blocks.get("nasdaq_earnings") or {},
            fallback_blocks=[
                blocks.get("fmp_earnings") or {},
            ],
        )
        risk_block = blocks.get("cboe_risk_indices") or {}
        for dataset_id in ("vix", "vvix", "risk"):
            lookup = (
                vix_database_lookup
                if dataset_id == "vix"
                else risk_database_lookup
            ) or {
                "performed": False,
                "found": None,
                "data_as_of": None,
                "content_valid_until": None,
                "refresh_due_at": None,
                "expired": None,
                "freshness": "NOT_LOOKED_UP",
            }
            database_valid = bool(
                lookup.get("found")
                and not lookup.get("expired")
                and lookup.get("freshness") == "VALID"
            )
            cboe_attempt = _attempt_from_runtime_block(
                "CBOE",
                risk_block,
            )
            if dataset_id == "vix":
                if database_valid:
                    primary = _database_selected_attempt("FRED")
                    fallbacks = [
                        _database_selected_attempt("CBOE")
                    ]
                else:
                    evidence = vix_provider_evidence or {}
                    primary = provider_attempt(
                        "FRED",
                        called=bool(evidence.get("called")),
                        attempts=int(evidence.get("attempts") or 0),
                        result=str(
                            evidence.get("result") or "NOT_CALLED"
                        ),
                        not_called_reason=(
                            None
                            if evidence.get("called")
                            else "PROVIDER_EXECUTION_EVIDENCE_NOT_AVAILABLE"
                        ),
                        execution_origin=(
                            "PROVIDER_CALL"
                            if evidence.get("called")
                            else "OBSERVED_SKIP"
                        ),
                    )
                    fallbacks = [
                        provider_attempt(
                            "CBOE",
                            called=False,
                            attempts=0,
                            result="NOT_CALLED",
                            not_called_reason=(
                                "PRIOR_PROVIDER_SUCCEEDED"
                            ),
                            execution_origin="OBSERVED_SKIP",
                        )
                        if _attempt_succeeded(primary)
                        else cboe_attempt
                    ]
            else:
                primary = cboe_attempt
                fallbacks = []
            risk_value = (
                risk_context.get(dataset_id)
                if dataset_id in {"vix", "vvix"}
                else risk_context.get("derived_context")
            )
            collector.record(
                dataset_id,
                acquisition_id=(
                    "vix_fred_cboe_provider_chain"
                    if dataset_id == "vix"
                    else "cboe_risk_context"
                ),
                shared_dataset_ids=(
                    ("vix",)
                    if dataset_id == "vix"
                    else ("vvix", "risk")
                ),
                database_lookup_performed=bool(lookup.get("performed")),
                database_lookup_reason=(
                    "VIX_CANONICAL_DATABASE_LOOKUP"
                    if dataset_id == "vix"
                    else "RISK_CONTEXT_DATABASE_LOOKUP"
                ),
                database_record_found=lookup.get("found"),
                database_data_as_of=lookup.get("data_as_of"),
                database_content_valid_until=lookup.get(
                    "content_valid_until"
                ),
                database_refresh_due_at=lookup.get(
                    "refresh_due_at"
                ),
                database_lifecycle_status=lookup.get(
                    "lifecycle_status"
                ),
                database_record_expired=lookup.get("expired"),
                database_freshness_evaluation=str(
                    lookup.get("freshness") or "NOT_LOOKED_UP"
                ),
                primary_provider=primary,
                fallbacks=fallbacks,
                acquisition_selected_source=(
                    (risk_context.get("source_summary") or {})
                    .get("selected_sources", {})
                    .get(dataset_id)
                    or risk_block.get("source")
                    if risk_value
                    else None
                ),
                acquisition_reason_code=(
                    "RISK_CONTEXT_ACQUIRED"
                    if risk_value
                    else "RISK_CONTEXT_NOT_AVAILABLE"
                ),
            )
        schedule_blocks = [
            blocks.get("nasdaq_market_info") or {},
            blocks.get("cme_market_schedule") or {},
            blocks.get("investing_holidays") or {},
            blocks.get("marketbeat_holidays") or {},
        ]
        policy = collector.policies["market_schedule"]
        schedule_lookup = (
            schedule_blocks[0].get("database_lookup") or {}
        )
        schedule_database_valid = _database_lookup_is_valid(
            schedule_lookup
        )
        schedule_attempts = [
            _runtime_or_database_skip_attempt(
                provider,
                block,
                database_valid=schedule_database_valid,
            )
            for provider, block in zip(
                (
                    policy.primary_provider,
                    *policy.fallback_providers,
                ),
                schedule_blocks,
                strict=True,
            )
        ]
        collector.record(
            "market_schedule",
            acquisition_id="market_schedule_provider_chain",
            shared_dataset_ids=("market_schedule",),
            database_lookup_performed=bool(
                schedule_lookup.get("performed")
            ),
            database_lookup_reason=(
                "MARKET_SCHEDULE_CANONICAL_CACHE_LOOKUP"
            ),
            database_record_found=(
                schedule_lookup
            ).get("found"),
            database_data_as_of=(
                schedule_lookup
            ).get("data_as_of"),
            database_content_valid_until=(
                schedule_lookup
            ).get("content_valid_until"),
            database_refresh_due_at=(
                schedule_lookup
            ).get("refresh_due_at"),
            database_lifecycle_status=(
                schedule_lookup
            ).get("lifecycle_status"),
            database_record_expired=(
                schedule_lookup
            ).get("expired"),
            database_freshness_evaluation=str(
                (
                    schedule_lookup
                ).get("freshness")
                or "NOT_LOOKED_UP"
            ),
            primary_provider=schedule_attempts[0],
            fallbacks=schedule_attempts[1:],
            acquisition_selected_source=next(
                (
                    block.get("source")
                    for block in schedule_blocks
                    if str(block.get("status") or "").lower()
                    in {"found", "available", "valid", "partial"}
                ),
                None,
            ),
            acquisition_reason_code="MARKET_SCHEDULE_PROVIDER_CHAIN_COMPLETED",
        )

    def _record_block_dataset(
        self,
        collector: RequestProviderAccountingCollector,
        *,
        dataset_id: str,
        acquisition_id: str,
        block: dict[str, Any],
        fallback_blocks: list[dict[str, Any]] | None = None,
        lookup_override: dict[str, Any] | None = None,
        database_lookup_reason: str = (
            "MULTI_SOURCE_RUNTIME_DATABASE_LOOKUP"
        ),
    ) -> None:
        policy = collector.policies[dataset_id]
        lookup = (
            lookup_override
            if lookup_override is not None
            else block.get("database_lookup")
            if isinstance(
                block.get("database_lookup"),
                dict,
            )
            else {}
        )
        primary = _attempt_from_runtime_block(
            policy.primary_provider,
            block,
        )
        database_valid = _database_lookup_is_valid(lookup)
        fallback_attempts: list[dict[str, Any]] = []
        primary_succeeded = _attempt_succeeded(primary)
        supplied_fallbacks = fallback_blocks or []
        for index, provider in enumerate(
            policy.fallback_providers
        ):
            if index < len(supplied_fallbacks):
                fallback_attempts.append(
                    _runtime_or_database_skip_attempt(
                        provider,
                        supplied_fallbacks[index],
                        database_valid=database_valid,
                    )
                )
            else:
                fallback_attempts.append(
                    provider_attempt(
                        provider,
                        called=False,
                        attempts=0,
                        result="NOT_CALLED",
                        not_called_reason=(
                            "VALID_DATABASE_RECORD_SELECTED"
                            if database_valid
                            else "PRIOR_PROVIDER_SUCCEEDED"
                            if primary_succeeded
                            else "FALLBACK_EXECUTION_EVIDENCE_NOT_AVAILABLE"
                        ),
                        execution_origin=(
                            "CACHE_DECISION"
                            if database_valid
                            else "OBSERVED_SKIP"
                        ),
                    )
                )
        acquisition_blocks = [
            block,
            *supplied_fallbacks,
        ]
        acquisition_attempts = [
            primary,
            *fallback_attempts,
        ]
        selected_block = next(
            (
                candidate
                for index, candidate in enumerate(
                    acquisition_blocks
                )
                if isinstance(candidate, dict)
                and str(
                    candidate.get("status") or ""
                ).lower()
                in {"found", "available", "valid", "partial"}
                and (
                    (
                        database_valid
                        and index == 0
                    )
                    or (
                        index < len(acquisition_attempts)
                        and _attempt_succeeded(
                            acquisition_attempts[index]
                        )
                    )
                )
            ),
            None,
        )
        collector.record(
            dataset_id,
            acquisition_id=acquisition_id,
            shared_dataset_ids=(dataset_id,),
            database_lookup_performed=bool(lookup.get("performed")),
            database_lookup_reason=database_lookup_reason,
            database_record_found=lookup.get("found"),
            database_data_as_of=lookup.get("data_as_of"),
            database_content_valid_until=lookup.get(
                "content_valid_until"
            ),
            database_refresh_due_at=lookup.get("refresh_due_at"),
            database_lifecycle_status=lookup.get(
                "lifecycle_status"
            ),
            database_record_expired=lookup.get("expired"),
            database_freshness_evaluation=str(
                lookup.get("freshness") or "NOT_LOOKED_UP"
            ),
            primary_provider=primary,
            fallbacks=fallback_attempts,
            acquisition_selected_source=(
                selected_block.get("source")
                if selected_block is not None
                else None
            ),
            acquisition_reason_code=(
                "PROVIDER_BLOCK_ACQUIRED"
                if selected_block is not None
                else "PROVIDER_BLOCK_NOT_AVAILABLE"
            ),
            evidence_complete=(
                _database_lookup_shape_complete(lookup)
                and _provider_attempt_chain_complete(
                    database_valid=database_valid,
                    attempts=[primary, *fallback_attempts],
                )
            ),
        )


def _vix_provider_evidence(
    macro: MacroLatestResponse,
) -> dict[str, Any]:
    fred_results = [
        result
        for result in macro.provider_results
        if str(getattr(result, "source", "")).upper()
        == "FRED"
    ]
    found = any(
        str(getattr(series, "series_id", "")).upper()
        == "VIXCLS"
        and getattr(series, "value", None) is not None
        and str(getattr(series, "source", "")).upper()
        == "FRED"
        for series in macro.series
    )
    failed = bool(
        fred_results
        and any(
            getattr(result, "errors", None)
            for result in fred_results
        )
    )
    return {
        "called": bool(fred_results),
        "attempts": len(fred_results),
        "result": (
            "FOUND"
            if found
            else "FAILED"
            if failed
            else "NO_DATA"
            if fred_results
            else "NOT_CALLED"
        ),
    }


def _canonical_freshness_evidence(
    result: CanonicalFreshnessResult,
) -> dict[str, Any]:
    return {
        "performed": True,
        "found": result.found,
        "data_as_of": result.data_as_of,
        "content_valid_until": result.content_valid_until,
        "refresh_due_at": result.refresh_due_at,
        "lifecycle_status": result.lifecycle,
        "expired": result.expired,
        "freshness": result.evaluation,
        "reason_code": result.reason_code,
    }


def _database_selected_attempt(
    provider: str,
) -> dict[str, Any]:
    return provider_attempt(
        provider,
        called=False,
        attempts=0,
        result="NOT_CALLED",
        not_called_reason="VALID_DATABASE_RECORD_SELECTED",
        execution_origin="CACHE_DECISION",
    )


def _matching_macro_fact(
    rows: list[dict[str, Any]],
    series_ids: tuple[str, ...],
) -> dict[str, Any] | None:
    expected = {item.upper() for item in series_ids}
    for fact in rows:
        raw = (
            fact.get("raw_payload")
            if isinstance(fact.get("raw_payload"), dict)
            else {}
        )
        observed = str(
            raw.get("series_id")
            or fact.get("category")
            or fact.get("fact_key")
            or ""
        ).upper()
        if observed in expected or any(
            f":{series_id}:" in observed
            for series_id in expected
        ):
            return fact
    return None


def _event_value(event: Any, *keys: str) -> Any:
    for key in keys:
        value = (
            event.get(key)
            if isinstance(event, dict)
            else getattr(event, key, None)
        )
        if value not in (None, ""):
            return value
    return None


def _canonical_preflight_runtime_block(
    payload: dict[str, Any],
    *,
    lookup: dict[str, Any],
    provider_source: str,
) -> dict[str, Any]:
    selected_source = (
        (payload.get("source_summary") or {}).get(
            "selected_source"
        )
        or provider_source
    )
    return {
        "status": str(payload.get("status") or "available"),
        "provider": provider_source,
        "source": selected_source,
        "retrieved_at": payload.get("retrieved_at"),
        "data_as_of": lookup.get("data_as_of"),
        "content_valid_until": lookup.get(
            "content_valid_until"
        ),
        "valid_until": lookup.get("content_valid_until"),
        "refresh_due_at": lookup.get("refresh_due_at"),
        "next_refresh_at": lookup.get("refresh_due_at"),
        "attempted": False,
        "provider_calls": 0,
        "actual_network_calls": 0,
        "cache_used": True,
        "AI_called": False,
        "fetched_count": 0,
        "validated_count": 1,
        "rejected_count": 0,
        "persisted_count": 0,
        "read_back_count": 1,
        "materialized_count": 1,
        "committed": True,
        "database_lookup": dict(lookup),
        "reason": "VALID_CANONICAL_RECORD_SELECTED_BEFORE_PROVIDER",
        "warnings": [],
        "errors": [],
    }


def _attempt_from_runtime_block(
    provider: str,
    block: dict[str, Any],
) -> dict[str, Any]:
    calls = int(block.get("provider_calls") or 0)
    attempted = bool(block.get("attempted"))
    cache_used = bool(block.get("cache_used"))
    if attempted and calls:
        return provider_attempt(
            provider,
            called=True,
            attempts=calls,
            result=str(block.get("status") or "NO_DATA").upper(),
            execution_origin="PROVIDER_CALL",
        )
    if cache_used:
        lookup = (
            block.get("database_lookup")
            if isinstance(block.get("database_lookup"), dict)
            else {}
        )
        if not _database_lookup_is_valid(lookup):
            return provider_attempt(
                provider,
                called=False,
                attempts=0,
                result="CACHE_EVIDENCE_INVALID",
                not_called_reason=(
                    str(lookup.get("reason_code") or "").upper()
                    or "CACHE_HIT_WITHOUT_VALID_DATABASE_EVIDENCE"
                ),
                execution_origin="OBSERVED_SKIP",
            )
        return provider_attempt(
            provider,
            called=False,
            attempts=0,
            result="CACHE_HIT",
            not_called_reason="VALID_DATABASE_RECORD_SELECTED",
            execution_origin="CACHE_DECISION",
        )
    return provider_attempt(
        provider,
        called=False,
        attempts=0,
        result="NOT_CALLED",
        not_called_reason=(
            str(block.get("reason") or "").upper()
            or "PROVIDER_DISABLED_OR_NOT_REQUIRED"
        ),
        execution_origin="OBSERVED_SKIP",
    )


def _database_lookup_is_valid(
    lookup: dict[str, Any],
) -> bool:
    return bool(
        lookup.get("performed") is True
        and lookup.get("found") is True
        and lookup.get("expired") is False
        and str(lookup.get("freshness") or "").upper() == "VALID"
    )


def _database_lookup_shape_complete(
    lookup: dict[str, Any],
) -> bool:
    if lookup.get("performed") is not True:
        return False
    found = lookup.get("found")
    expired = lookup.get("expired")
    freshness = str(lookup.get("freshness") or "").upper()
    if found is False:
        return bool(
            expired is False
            and freshness == "NOT_FOUND"
            and lookup.get("data_as_of") is None
            and lookup.get("content_valid_until") is None
            and lookup.get("refresh_due_at") is None
        )
    return bool(
        found is True
        and type(expired) is bool
        and freshness
        and lookup.get("data_as_of")
        and lookup.get("content_valid_until")
        and lookup.get("refresh_due_at")
    )


def _provider_attempt_chain_complete(
    *,
    database_valid: bool,
    attempts: list[dict[str, Any]],
) -> bool:
    if not attempts:
        return False
    if database_valid:
        return all(
            attempt.get("called") is False
            and attempt.get("execution_origin") == "CACHE_DECISION"
            and attempt.get("not_called_reason")
            == "VALID_DATABASE_RECORD_SELECTED"
            for attempt in attempts
        )
    prior_succeeded = False
    prior_failed = False
    for index, attempt in enumerate(attempts):
        called = attempt.get("called") is True
        if index == 0 and not called:
            if (
                len(attempts) == 1
                or "NOT_CONFIGURED"
                not in str(
                    attempt.get("not_called_reason") or ""
                ).upper()
            ):
                return False
            prior_failed = True
            continue
        if index > 0:
            if prior_succeeded:
                if called:
                    return False
                continue
            if not prior_failed or not called:
                return False
        succeeded = _attempt_succeeded(attempt)
        prior_succeeded = succeeded
        prior_failed = called and not succeeded
    return True


def _runtime_or_database_skip_attempt(
    provider: str,
    block: dict[str, Any],
    *,
    database_valid: bool,
) -> dict[str, Any]:
    if (
        database_valid
        and not block.get("attempted")
        and int(block.get("provider_calls") or 0) == 0
    ):
        return provider_attempt(
            provider,
            called=False,
            attempts=0,
            result="NOT_CALLED",
            not_called_reason="VALID_DATABASE_RECORD_SELECTED",
            execution_origin="CACHE_DECISION",
        )
    return _attempt_from_runtime_block(provider, block)


def _attempt_succeeded(attempt: dict[str, Any]) -> bool:
    if not attempt.get("called"):
        return False
    result = str(attempt.get("result") or "").upper()
    if result.startswith("SCHEDULE_CATCH_UP_"):
        return result == "SCHEDULE_CATCH_UP_COMPLETED"
    return any(
        token in result
        for token in ("SUCCESS", "FOUND", "AVAILABLE", "VALID", "PARTIAL")
    ) and not any(
        token in result
        for token in ("FAIL", "ERROR", "NO_DATA", "NOT_FOUND", "TIMEOUT")
    )


def _calendar_primary_acquisition_succeeded(
    accounting: dict[str, Any],
    *,
    events: list[Any],
) -> bool:
    if accounting.get("provider_called") is True:
        return _attempt_succeeded(
            {
                "called": True,
                "result": accounting.get("provider_result"),
            }
        )
    return bool(
        events
        and accounting.get("database_lookup_performed") is True
    )


def _calendar_catch_up_succeeded(coverage: dict[str, Any]) -> bool:
    daily_matrix = (
        coverage.get("daily_matrix")
        if isinstance(coverage.get("daily_matrix"), dict)
        else {}
    )
    return bool(
        int(coverage.get("provider_calls_executed") or 0) > 0
        and int(coverage.get("provider_success_count") or 0) > 0
        and str(coverage.get("status") or "").upper()
        == "VERIFIED_COMPLETE"
        and str(daily_matrix.get("status") or "").upper()
        == "VERIFIED_COMPLETE"
        and not coverage.get("unknown_coverage_days")
        and not coverage.get("partial_coverage_days")
        and int(coverage.get("quarantined_occurrence_count") or 0)
        == 0
    )


def _news_provider_execution_evidence_complete(
    account: dict[str, Any],
) -> bool:
    calls = account.get("calls")
    status = str(account.get("status") or "").strip().upper()
    if (
        not account.get("provider")
        or not isinstance(calls, int)
        or isinstance(calls, bool)
        or calls < 0
        or not status
        or status
        in {
            "UNKNOWN",
            "MISSING",
            "EVIDENCE_NOT_AVAILABLE",
        }
    ):
        return False
    if calls == 0 and not str(
        account.get("reason_code") or ""
    ).strip():
        return False
    if calls > 0 and status in {
        "NOT_CALLED",
        "DISABLED",
        "NOT_CONFIGURED",
    }:
        return False
    origin = str(account.get("execution_origin") or "").upper()
    if origin and origin not in {
        "PROVIDER_CALL",
        "OBSERVED_SKIP",
        "CACHE_DECISION",
    }:
        return False
    if origin == "PROVIDER_CALL" and calls == 0:
        return False
    if origin in {"OBSERVED_SKIP", "CACHE_DECISION"} and calls != 0:
        return False
    if origin == "CACHE_DECISION" and status != "CACHE_HIT":
        return False
    return True


def _runtime_block_available(block: dict[str, Any]) -> bool:
    return bool(
        str(block.get("status") or "").lower()
        in {"found", "available", "valid", "partial"}
        and (
            int(block.get("fetched_count") or 0) > 0
            or int(block.get("materialized_count") or 0) > 0
        )
    )


def _calendar_database_evidence(
    coverage: dict[str, Any],
    *,
    events: list[Any],
    provider_called: bool,
) -> dict[str, Any]:
    del provider_called
    preflight = coverage.get("database_lookup_daily_matrix")
    matrices = (
        (preflight or {}).get("by_provider")
        if isinstance(preflight, dict)
        else (coverage.get("daily_matrix") or {}).get("by_provider")
        if isinstance(coverage.get("daily_matrix"), dict)
        else {}
    )
    rows: list[tuple[str, dict[str, Any]]] = []
    for matrix in (matrices or {}).values():
        for day, value in (
            (matrix.get("by_date") or {}).items()
            if isinstance(matrix, dict)
            else ()
        ):
            if isinstance(value, dict):
                rows.append((str(day), value))
    today = datetime.now(UTC).date().isoformat()
    selected = next(
        (item for item in rows if item[0] == today),
        rows[0] if rows else None,
    )
    if selected is None:
        return {
            "performed": True,
            "found": False,
            "data_as_of": None,
            "content_valid_until": None,
            "refresh_due_at": None,
            "expired": False,
            "lifecycle_status": None,
            "freshness": "NOT_FOUND",
            "complete": True,
        }
    day, row = selected
    status = str(row.get("status") or "").upper()
    valid_until = row.get("valid_until")
    refresh_due_at = (
        row.get("next_revision_check_at")
        or row.get("next_retry_at")
    )
    decision_at = datetime.now(UTC)
    valid_deadline = parse_datetime(valid_until)
    refresh_deadline = parse_datetime(refresh_due_at)
    expired = bool(
        status not in {"VERIFIED_COMPLETE", "VERIFIED_EMPTY"}
        or
        valid_deadline is None
        or refresh_deadline is None
        or decision_at >= valid_deadline
        or decision_at >= refresh_deadline
    )
    return {
        "performed": True,
        "found": True,
        "data_as_of": day,
        "content_valid_until": valid_until,
        "refresh_due_at": refresh_due_at,
        "lifecycle_status": (
            "VALID"
            if status in {"VERIFIED_COMPLETE", "VERIFIED_EMPTY"}
            else "INVALID"
        ),
        "expired": expired,
        "freshness": (
            "VALID"
            if not expired
            else "INVALID_LIFECYCLE"
            if status not in {"VERIFIED_COMPLETE", "VERIFIED_EMPTY"}
            else "REFRESH_DUE"
        ),
        "complete": bool(valid_deadline and refresh_deadline),
    }


def _event_enrichment_metadata(
    metadata: dict[str, Any],
    events: list[Any],
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Materialize the AI enrichment state machine from actual dispatcher telemetry."""
    quality = metadata.get("data_quality") or {}
    warnings = quality.get("warnings") or metadata.get("warnings") or []
    enabled = bool(quality.get("ai_research_enabled", getattr(settings, "enable_ai_researcher", False)))
    mode = str(quality.get("ai_research_mode") or getattr(settings, "ai_researcher_mode", "codex_cli"))
    configured = bool(quality.get("ai_research_configured", enabled and mode in {"codex_cli", "openai_api"}))
    ai_called = bool(quality.get("ai_research_called"))
    ai_queued = int(quality.get("ai_research_requests") or 0) > 0 or str(quality.get("ai_research_status") or "").upper() == "PENDING"
    candidate_ids = {str(value) for value in quality.get("ai_candidate_event_ids") or []}
    raw_status = str(quality.get("ai_research_status") or "").lower()
    failure_reason = quality.get("ai_failure_reason")
    duration_ms = quality.get("ai_duration_ms")

    if not enabled:
        overall_status, reason = "disabled", "AI enrichment is disabled"
    elif not configured:
        overall_status, reason = "not_configured", "AI enrichment is not configured"
    elif quality.get("ai_not_available"):
        overall_status, reason = "not_available", str(failure_reason or "AI enrichment is unavailable")
    elif ai_queued and not ai_called:
        overall_status, reason = "pending", "AI enrichment is queued for the persistent worker"
    elif not ai_called:
        overall_status = "not_required"
        reason = "optional enrichment skipped" if quality.get("enrichment_not_attempted") else "no AI-eligible event required enrichment"
    elif raw_status in {"timeout"} or "timeout" in str(failure_reason or "").lower():
        overall_status, reason = "timeout", str(failure_reason or "AI enrichment timed out")
    elif raw_status in {"cancelled", "canceled"}:
        overall_status, reason = "cancelled", str(failure_reason or "AI enrichment was cancelled")
    elif raw_status in {"rejected"} or int(quality.get("ai_results_rejected") or 0) and not int(quality.get("ai_results_valid") or 0):
        overall_status, reason = "rejected", str(failure_reason or "AI enrichment results were rejected")
    elif raw_status in {"success", "no_data_available"}:
        overall_status, reason = "completed", None
    else:
        overall_status, reason = "failed", str(failure_reason or raw_status or "AI enrichment failed")

    event_rows: list[dict[str, Any]] = []
    for event in events:
        enrichment = getattr(event, "enrichment", None)
        attempted = ai_called and str(getattr(event, "event_id", "")) in candidate_ids
        queued_for_event = ai_queued and str(getattr(event, "event_id", "")) in candidate_ids
        status = overall_status if attempted or queued_for_event else (overall_status if overall_status in {"disabled", "not_configured", "not_available"} else "not_required")
        source_url = getattr(enrichment, "source_url", None)
        values = [field for field in ("forecast", "previous", "consensus", "actual") if getattr(enrichment, field, None) not in (None, "")]
        accepted_fields = values if attempted and status == "completed" and source_url else []
        rejected_fields = values if attempted and status == "rejected" else []
        persistence = getattr(enrichment, "summary", {}).get("persistence", {}) if enrichment else {}
        event_rows.append(
            {
                "event_id": getattr(event, "event_id", None),
                "event_name": getattr(event, "name", None),
                "AI_called": attempted,
                "attempted": attempted,
                "status": status,
                "failure_type": status if status in {"failed", "timeout", "cancelled", "rejected"} else None,
                "timeout": status == "timeout",
                "duration_ms": duration_ms if attempted else None,
                "persisted": bool(persistence.get("persisted")) or getattr(enrichment, "cache_status", None) == "refreshed",
                "read_back": bool(persistence.get("read_back")) or getattr(enrichment, "cache_status", None) == "hit",
                "accepted_fields": accepted_fields,
                "rejected_fields": rejected_fields,
                "source_urls": [source_url] if source_url else [],
                "source_url": source_url,
                "confidence": getattr(enrichment, "confidence", None),
                "reliability": getattr(enrichment, "reliability", None),
                "reason": reason if attempted or status != "not_required" else None,
            }
        )

    return {
        "enabled": enabled,
        "configured": configured,
        "mode": mode,
        "AI_called": ai_called,
        "attempted_event_count": sum(1 for row in event_rows if row["attempted"]),
        "completed_event_count": sum(1 for row in event_rows if row["status"] == "completed"),
        "timeout_event_count": sum(1 for row in event_rows if row["timeout"]),
        "failed_event_count": sum(1 for row in event_rows if row["status"] == "failed"),
        "rejected_event_count": sum(1 for row in event_rows if row["attempted"] and row["status"] == "rejected"),
        "rejected_field_count": sum(len(row["rejected_fields"]) for row in event_rows),
        "accepted_event_count": sum(1 for row in event_rows if row["accepted_fields"]),
        "accepted_field_count": sum(len(row["accepted_fields"]) for row in event_rows),
        "no_data_event_count": sum(
            1
            for row in event_rows
            if row["attempted"] and row["status"] == "completed" and not row["accepted_fields"]
        ),
        "persisted_event_count": sum(1 for row in event_rows if row["persisted"]),
        "read_back_event_count": sum(1 for row in event_rows if row["read_back"]),
        "duration_ms": duration_ms if ai_called else None,
        "status": overall_status,
        "reason": reason,
        "warnings": warnings,
        "events": sorted(event_rows, key=lambda row: (not row["attempted"], str(row["event_name"] or "")))[:25],
    }


def _news_pipeline_status(
    news_items: list[dict[str, Any]],
    *,
    materialized: dict[str, Any] | None = None,
) -> dict[str, Any]:
    materialized = materialized or build_news_context(news_items)
    diagnostics = dict(materialized.get("diagnostics") or {})
    exclusions: list[dict[str, Any]] = []
    eligible_count = 0
    for item in news_items:
        reason = _news_exclusion_reason(item)
        if reason is None:
            eligible_count += 1
        else:
            exclusions.append(
                {
                    "article_id": item.get("news_key") or item.get("source_url") or item.get("url") or item.get("title"),
                    "reason": reason,
                }
            )
    if diagnostics:
        eligible_count = int(
            materialized.get("accepted_article_count")
            if materialized.get("accepted_article_count") is not None
            else diagnostics.get("accepted_count") or 0
        )
        exclusions = list(materialized.get("excluded") or [])
    materialized_count = len(
        materialized.get("articles") or materialized.get("latest") or []
    )
    return {
        "fetched_count": len(news_items),
        "validated_count": len(news_items),
        "persisted_count": len(news_items),
        "committed": True,
        "read_back_count": len(news_items),
        "eligible_count": eligible_count,
        "materialized_count": materialized_count,
        "excluded_count": len(exclusions),
        "exclusion_reasons": _reason_counts(exclusions),
        "exclusions": exclusions[:50],
        "diagnostics": diagnostics,
        "quality": materialized.get("quality") or {},
        "eligible_news_not_materialized": max(eligible_count - materialized_count, 0) if materialized_count == 0 else 0,
        "search_completed": True,
        "provider_attempt_count": 1 if news_items else 0,
        "provider_success_count": 1 if news_items else 0,
        "provider_failure_count": 0,
    }


def _news_exclusion_reason(item: dict[str, Any]) -> str | None:
    audit_status = str(
        item.get("source_audit_status") or ""
    ).upper()
    if audit_status and audit_status != "ACTIVE":
        return "source_quarantined"
    disposition = str(item.get("disposition") or "").upper()
    if disposition in {
        "QUARANTINED",
        "WITHHELD",
        "TECHNICALLY_INVALID",
        "TECHNICALLY_REJECTED",
    }:
        return "source_quarantined"
    if item.get("accepted") is False:
        return "source_rejected"
    if news_content_status(item) == "invalid_content":
        return "invalid_content"
    if not (item.get("source_url") or item.get("url")) and not any(
        item.get(key) not in (None, "")
        for key in (
            "news_key",
            "provider_record_id",
            "occurrence_id",
            "article_id",
            "record_id",
        )
    ):
        return "missing_source_identity"
    published = item.get("published_at")
    if published:
        parsed = _parse_dt(published)
        if parsed and parsed > datetime.now(UTC) + timedelta(minutes=1):
            return "future_published"
    return None


def _select_current_news_database_candidate(
    items: list[dict[str, Any]],
    *,
    freshness_service: Any,
) -> tuple[dict[str, Any] | None, Any]:
    observed_item: dict[str, Any] | None = None
    observed_freshness: Any = None
    for item in items:
        if _news_exclusion_reason(item) is not None:
            continue
        freshness = freshness_service.evaluate_canonical(
            item,
            max_age=timedelta(hours=24),
        )
        if observed_item is None:
            observed_item = item
            observed_freshness = freshness
        if freshness.usable:
            return item, freshness
    if observed_item is not None:
        return observed_item, observed_freshness
    return (
        None,
        freshness_service.evaluate_canonical(
            None,
            max_age=timedelta(hours=24),
        ),
    )


def _macro_pipeline_status(macro: MacroLatestResponse, macro_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    bls_status = (
        bls_required_series_status_from_macro_snapshot(macro_snapshot)
        if macro_snapshot is not None
        else bls_required_series_status_from_macro_series(macro.series)
    )
    provider_sources = [item.source for item in macro.provider_results]
    if macro_snapshot is not None:
        for item in macro_snapshot.get("provider_results") or []:
            if isinstance(item, dict) and item.get("source") and item.get("source") not in provider_sources:
                provider_sources.append(item["source"])
    return {
        "provider_sources": provider_sources,
        "series_count": len(macro.series),
        "bls_required_series": bls_status,
        "required_bls_series": bls_status["required"],
        "required_bls_present": bls_status["present"],
        "required_bls_missing": bls_status["missing"],
        "required_bls_invalid": bls_status["invalid"],
        "materialized_bls_series": bls_status["materialized"],
        "materialized_count": len(macro.series),
    }


def _canonical_macro_series(series: list[Any]) -> list[Any]:
    selected: dict[str, Any] = {}
    for item in series:
        series_id = str(getattr(item, "series_id", "") or "").upper()
        if not series_id:
            continue
        current = selected.get(series_id)
        if current is None or _macro_series_rank(item) > _macro_series_rank(current):
            selected[series_id] = item
    return list(selected.values())


def _macro_series_rank(series: Any) -> tuple[int, str, str, float]:
    source = str(getattr(series, "source", "") or "").lower()
    source_rank = 1 if " via fred" in source else 3 if any(token in source for token in ("bls", "bea")) else 2
    metadata = getattr(series, "metadata", None)
    retrieved_at = str(getattr(metadata, "retrieved_at", "") or "")
    reliability = float(getattr(metadata, "reliability", 0) or 0)
    return source_rank, str(getattr(series, "data_as_of", "") or ""), retrieved_at, reliability


def _reason_counts(exclusions: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in exclusions:
        reason = str(item.get("reason") or "other")
        counts[reason] = counts.get(reason, 0) + 1
    return counts


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _positioning_context_from_runtime(cot: dict[str, Any]) -> dict[str, Any]:
    status = cot.get("status") or "not_found"
    return {
        "status": "available" if status == "found" else status,
        "data_as_of": cot.get("report_date"),
        "retrieved_at": cot.get("retrieved_at"),
        "valid_until": cot.get("valid_until"),
        "next_refresh_at": cot.get("next_retry_at") or cot.get("valid_until"),
        "source": cot.get("source") or "CFTC",
        "source_url": cot.get("source_url"),
        "provider_type": "OFFICIAL_WEB",
        "freshness": "WEEKLY",
        "reliability": cot.get("reliability"),
        "confidence": cot.get("reliability"),
        "provider_calls": int(cot.get("provider_calls") or 0),
        "actual_network_calls": int(cot.get("actual_network_calls") or cot.get("provider_calls") or 0),
        "cache_used": bool(cot.get("cache_used") or cot.get("cache_status") == "hit"),
        "warnings": cot.get("warnings") or [],
        "errors": cot.get("errors") or [],
        "cot": {"nasdaq_100": cot},
    }


def _sentiment_context_from_runtime(aaii: dict[str, Any]) -> dict[str, Any]:
    status = aaii.get("status") or "not_found"
    return {
        "status": "available" if status == "found" else status,
        "data_as_of": aaii.get("survey_date"),
        "retrieved_at": aaii.get("retrieved_at"),
        "valid_until": aaii.get("valid_until"),
        "next_refresh_at": aaii.get("next_retry_at") or aaii.get("valid_until"),
        "source": aaii.get("source") or "AAII",
        "source_url": aaii.get("source_url"),
        "provider_type": "OFFICIAL_WEB",
        "freshness": "WEEKLY",
        "reliability": aaii.get("reliability"),
        "confidence": aaii.get("reliability"),
        "provider_calls": int(aaii.get("provider_calls") or 0),
        "actual_network_calls": int(aaii.get("actual_network_calls") or aaii.get("provider_calls") or 0),
        "cache_used": bool(aaii.get("cache_used") or aaii.get("cache_status") == "hit"),
        "warnings": aaii.get("warnings") or [],
        "errors": aaii.get("errors") or [],
        "aaii": aaii,
        "retail_social": {
            "QQQ": {
                "sentiment_score": None,
                "bullish_messages": None,
                "bearish_messages": None,
                "message_volume": None,
                "message_volume_change_pct": None,
                "source": None,
                "source_url": None,
                "freshness": None,
                "reliability": None,
                "warnings": ["social_sentiment_optional_not_configured"],
            }
        },
    }
