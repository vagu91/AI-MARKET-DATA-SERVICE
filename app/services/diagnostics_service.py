from __future__ import annotations

import uuid
import asyncio
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

from app.core.config import Settings
from app.models.common import Freshness, ProviderMetadata, ProviderType
from app.models.macro import MacroLatestResponse
from app.models.macro import MacroSeries
from app.models.nasdaq import NasdaqContextResponse
from app.services.data_freshness_service import DataFreshnessService
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
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
from app.services.nasdaq_data_service import NasdaqDataService
from app.services.positioning_runtime_service import PositioningRuntimeService
from app.services.multi_source_runtime_service import MultiSourceRuntimeService, apply_multi_source_context
from app.services.social_sentiment_service import SocialSentimentService


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
        self.news = MarketNewsRepository(settings)
        self.freshness = DataFreshnessService(settings)
        self.positioning_runtime = PositioningRuntimeService(settings)

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
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        fetch_missing = refresh != "false"
        force = refresh == "force"

        async def load_macro() -> tuple[MacroLatestResponse, dict[str, Any]]:
            try:
                return await asyncio.wait_for(
                    self._macro_db_first(fetch_missing=fetch_missing, force=force),
                    timeout=max(float(self.settings.timeout_macro_seconds), 1.0),
                )
            except TimeoutError:
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
                events = self._events_from_history(country=country, start=now, end=now + timedelta(days=days))
                return events, {"data_quality": {"refresh_mode": "false", "missing_critical_fields": [], "events_found": len(events), "enrichment_status": "cache_only"}}
            try:
                events = await asyncio.wait_for(
                    self._official_events(country=country, start=now, end=now + timedelta(days=days)),
                    timeout=max(float(self.settings.timeout_events_seconds), 1.0),
                )
            except TimeoutError:
                events = self._events_from_history(country=country, start=now, end=now + timedelta(days=days))
                return events, {
                    "data_quality": {
                        "refresh_mode": refresh,
                        "events_found": len(events),
                        "enrichment_status": "events_timeout_fallback_to_history" if events else "events_timeout",
                        "missing_critical_fields": [] if events else ["events_not_available"],
                        "warnings": [f"events_fetch_timeout_after_{self.settings.timeout_events_seconds}s"],
                    }
                }
            enrichment_timeout = max(float(self.settings.timeout_events_seconds), 1.0)
            try:
                return await asyncio.wait_for(
                    self.enrichment_orchestrator.enrich_events(
                        events=events,
                        country=country,
                        start=now,
                        end=now + timedelta(days=days),
                        trigger="diagnostics_full_model" if not force else "diagnostics_full_model_force",
                        force=force,
                    ),
                    timeout=enrichment_timeout,
                )
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
                    ),
                    timeout=max(float(self.settings.timeout_nasdaq_seconds), 1.0),
                )
            except TimeoutError:
                return None, {
                    "db_hits": 0,
                    "db_misses": 1,
                    "provider_hits": 0,
                    "provider_failures": 1,
                    "warnings": [f"nasdaq_context_timeout_after_{self.settings.timeout_nasdaq_seconds}s"],
                    "errors": [],
                }

        async def load_event_windows():
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
        news_items = self.news.stored(days=days, limit=100)
        news_pipeline = _news_pipeline_status(news_items)
        macro_pipeline = _macro_pipeline_status(macro)
        pipeline_integrity = {
            "critical_fetch_completed": (not fetch_missing) or macro_quality.get("provider_hits", 0) > 0 or macro_quality.get("db_hits", 0) > 0,
            "critical_persistence_completed": macro_quality.get("provider_hits", 0) == 0 or macro_quality.get("read_back_count", macro_quality.get("db_hits", 0)) > 0,
            "critical_commits_completed": macro_quality.get("provider_hits", 0) == 0 or macro_quality.get("read_back_count", 0) > 0,
            "critical_read_back_completed": bool(macro.series) and bool(nasdaq_context) and news_pipeline["read_back_count"] > 0,
            "snapshot_materialization_completed": news_pipeline["materialized_count"] > 0 and bool(macro.series) and bool(nasdaq_context),
            "snapshot_built_from_db": True,
            "partial_response": not (bool(macro.series) and bool(nasdaq_context)),
        }
        cot_payload = await self.positioning_runtime.cot(refresh=refresh)
        aaii_payload = await self.positioning_runtime.aaii(refresh=refresh)
        positioning_context = _positioning_context_from_runtime(cot_payload)
        sentiment_context = _sentiment_context_from_runtime(aaii_payload)
        event_facts = self.facts.search_facts(country=country, limit=500)
        quality = {
            **enrichment_metadata.get("data_quality", {}),
            "macro": macro_quality,
            "nasdaq": nasdaq_quality,
            "missing_critical_fields": enrichment_metadata.get("data_quality", {}).get("missing_critical_fields", []),
            "stale_fields": enrichment_metadata.get("data_quality", {}).get("stale_fields", []),
            "provider_observations_summary": self._provider_observation_summary(),
            "pipeline_integrity": pipeline_integrity,
            "news_pipeline": news_pipeline,
            "macro_pipeline": macro_pipeline,
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
            metadata={"event_enrichment": _event_enrichment_metadata(enrichment_metadata, enriched, settings=self.settings)},
            positioning_context=positioning_context,
            sentiment_context=sentiment_context,
        )
        contract["data_quality"]["macro_pipeline"] = _macro_pipeline_status(macro, contract.get("macro_snapshot") or {})
        overall_quality = contract["data_quality"].get("overall_data_quality") or {}
        contract["data_quality"]["missing_critical_fields"] = overall_quality.get("missing_critical_fields") or contract["data_quality"].get("missing_critical_fields") or []
        multi_refresh = "force" if refresh == "force" else "false"
        multi_source = await MultiSourceRuntimeService(self.settings).snapshot(refresh=multi_refresh)
        apply_multi_source_context(contract, multi_source)
        contract["social_sentiment"] = await SocialSentimentService(self.settings).snapshot(refresh=refresh)
        return contract

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

    async def _macro_db_first(self, *, fetch_missing: bool = True, force: bool = False) -> tuple[MacroLatestResponse, dict[str, Any]]:
        cached = [] if force else self.facts.get_valid_facts_by_type("official_macro_latest")
        if cached:
            macro = self._macro_from_facts(cached)
            return macro, {
                "db_hits": len(cached),
                "db_misses": 0,
                "provider_hits": 0,
                "provider_failures": 0,
                "warnings": ["macro_loaded_from_db_preview_only"],
                "errors": [],
            }
        if not fetch_missing:
            return MacroLatestResponse(), {
                "db_hits": 0,
                "db_misses": 1,
                "provider_hits": 0,
                "provider_failures": 0,
                "warnings": ["macro_not_in_db_refresh_false"],
                "errors": [],
            }
        macro = await self.macro_service.latest()
        written = self._save_macro(macro)
        read_back = self.facts.get_valid_facts_by_type("official_macro_latest")
        read_back_macro = self._macro_from_facts(read_back) if read_back else MacroLatestResponse(provider_results=macro.provider_results)
        provider_failures = sum(1 for item in macro.provider_results if item.errors)
        return read_back_macro, {
            "db_hits": len(read_back),
            "db_misses": 1,
            "provider_hits": written,
            "provider_failures": provider_failures,
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
        for series in macro.series:
            self.facts.upsert_fact(
                {
                    "fact_key": f"{series.source}:{series.series_id}:latest:official_macro_latest",
                    "fact_type": "official_macro_latest",
                    "country": "US",
                    "category": str(series.series_id).upper(),
                    "event_name": series.name,
                    "value": None if series.value is None else str(series.value),
                    "unit": series.units,
                    "source": series.source,
                    "provider_type": series.metadata.provider_type.value,
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
        if hasattr(self.event_service, "list_events"):
            events = await self.event_service.list_events(country=country, start=start, end=end, enrich=False)
        else:
            events = await self.event_service.upcoming(country=country, days=max(1, (end - start).days))
        for event in events:
            valid_until = self.freshness.macro_valid_until(event)
            self.facts.upsert_economic_event(
                event,
                event_key=f"{event.country}:{event.date}:{event.event_id}",
                valid_until=valid_until,
            )
        return events

    def _events_from_history(self, *, country: str, start: datetime, end: datetime) -> list:
        from app.models.events import EconomicEvent

        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                """
                SELECT raw_payload_json
                FROM economic_events_history
                WHERE country = ? AND date >= ? AND date <= ?
                ORDER BY time_utc ASC
                """,
                (country.upper(), start.date().isoformat(), end.date().isoformat()),
            ).fetchall()
        output = []
        import json

        for row in rows:
            try:
                payload = json.loads(row["raw_payload_json"])
                output.append(EconomicEvent.model_validate(payload))
            except Exception:
                continue
        return output

    async def _nasdaq_db_first(self, *, symbol: str, fetch_missing: bool = True, force: bool = False) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        facts_by_type = {
            "qqq_holdings": [] if force else self.facts.get_valid_facts_by_type("qqq_holdings"),
            "mega_cap_snapshot": [] if force else self.facts.get_valid_facts_by_type("mega_cap_snapshot"),
            "mega_cap_breadth": [] if force else self.facts.get_valid_facts_by_type("mega_cap_breadth"),
            "earnings_event": [] if force else self.facts.get_valid_facts_by_type("earnings_event"),
            "nasdaq_context": [] if force else self.facts.get_valid_facts_by_type("nasdaq_context"),
        }
        cached_count = sum(len(items) for items in facts_by_type.values())
        if cached_count:
            return materialize_nasdaq_context_from_facts(facts_by_type), {
                "db_hits": cached_count,
                "db_misses": 0,
                "provider_hits": 0,
                "provider_failures": 0,
                "warnings": [],
                "errors": [],
            }
        if not fetch_missing:
            return None, {
                "db_hits": 0,
                "db_misses": 1,
                "provider_hits": 0,
                "provider_failures": 0,
                "warnings": ["nasdaq_context_not_in_db"],
                "errors": [],
            }
        try:
            context = await self.nasdaq_data_service.context()
        except Exception as exc:
            return None, {
                "db_hits": 0,
                "db_misses": 1,
                "provider_hits": 0,
                "provider_failures": 1,
                "warnings": [],
                "errors": [str(exc) or type(exc).__name__],
            }
        written = self._save_nasdaq_context(context)
        return normalize_nasdaq_context(context), {
            "db_hits": 0,
            "db_misses": 1,
            "provider_hits": written,
            "provider_failures": 0,
            "warnings": list(context.metadata.get("warnings", [])),
            "errors": list(context.metadata.get("critical_errors", [])),
        }

    def _save_nasdaq_context(self, context: NasdaqContextResponse) -> int:
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
            if not raw:
                continue
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
                    "retrieved_at": raw.get("retrieved_at", now_iso()) if isinstance(raw, dict) else now_iso(),
                    "valid_until": valid_until,
                    "next_refresh_at": valid_until,
                    "raw_payload_json": raw,
                }
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
        status = overall_status if attempted else (overall_status if overall_status in {"disabled", "not_configured", "not_available"} else "not_required")
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
        "rejected_event_count": sum(len(row["rejected_fields"]) for row in event_rows),
        "accepted_event_count": sum(len(row["accepted_fields"]) for row in event_rows),
        "persisted_event_count": sum(1 for row in event_rows if row["persisted"]),
        "read_back_event_count": sum(1 for row in event_rows if row["read_back"]),
        "duration_ms": duration_ms if ai_called else None,
        "status": overall_status,
        "reason": reason,
        "warnings": warnings,
        "events": event_rows[:25],
    }


def _news_pipeline_status(news_items: list[dict[str, Any]]) -> dict[str, Any]:
    materialized = build_news_context(news_items)
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
    materialized_count = len(materialized.get("latest") or [])
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
        "eligible_news_not_materialized": max(eligible_count - materialized_count, 0) if materialized_count == 0 else 0,
    }


def _news_exclusion_reason(item: dict[str, Any]) -> str | None:
    if news_content_status(item) == "invalid_content":
        return "invalid_content"
    if not (item.get("source_url") or item.get("url")):
        return "missing_url"
    if not item.get("source"):
        return "missing_source"
    if freshness_label(valid_until=item.get("valid_until")) in {"STALE", "EXPIRED"}:
        return "expired"
    published = item.get("published_at")
    if published:
        parsed = _parse_dt(published)
        if parsed and parsed > datetime.now(UTC) + timedelta(minutes=1):
            return "future_published"
    return None


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
