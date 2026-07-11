from __future__ import annotations

import uuid
import logging
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

from app.core.config import Settings
from app.models.common import Impact, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.services.ai_researcher_service import AIResearcherService
from app.services.ai_research_diagnostics import AIResearchDiagnostics
from app.services.data_freshness_service import DataFreshnessService
from app.services.enrichment_run_repository import EnrichmentRunRepository
from app.services.event_enrichment_service import EventEnrichmentService
from app.services.economic_event_materialization_service import EconomicEventMaterializationService
from app.services.market_fact_repository import MarketFactRepository
from app.services.provider_observation_repository import ProviderObservationRepository


VALUE_FIELDS = ("forecast", "previous", "consensus", "actual")
AI_PRIORITY = {"CPI": 1, "PPI": 2, "NFP": 3, "GDP": 4, "PCE": 5, "FOMC": 6}
logger = logging.getLogger(__name__)


class EnrichmentOrchestrator:
    def __init__(
        self,
        settings: Settings,
        *,
        event_enrichment_service: EventEnrichmentService | None = None,
        ai_researcher_service: AIResearcherService | None = None,
    ) -> None:
        self.settings = settings
        self.event_enrichment_service = event_enrichment_service
        self.ai_researcher_service = ai_researcher_service or AIResearcherService(settings)
        self.facts = MarketFactRepository(settings)
        self.event_materializer = EconomicEventMaterializationService(settings, facts=self.facts)
        self.freshness = DataFreshnessService(settings)
        self.observations = ProviderObservationRepository(settings)
        self.runs = EnrichmentRunRepository(settings)

    async def enrich_events(
        self,
        *,
        events: list[EconomicEvent],
        country: str,
        start: datetime,
        end: datetime,
        trigger: str,
        force: bool = False,
    ) -> tuple[list[EconomicEvent], dict[str, Any]]:
        run_id = str(uuid.uuid4())
        metrics: dict[str, Any] = {
            "events_checked": 0,
            "db_hits": 0,
            "db_misses": 0,
            "db_bypassed_force": 0,
            "provider_hits": 0,
            "provider_misses": 0,
            "ai_research_requests": 0,
            "ai_events_requested": 0,
            "ai_results_valid": 0,
            "ai_results_rejected": 0,
            "ai_candidate_event_ids": [],
            "ai_research_status": "not_required",
            "ai_duration_ms": None,
            "ai_not_available": False,
            "facts_written": 0,
            "news_written": 0,
            "warnings_json": [],
            "errors_json": [],
            **self.event_materializer.empty_metrics(),
        }
        self.runs.start(run_id=run_id, trigger=trigger)
        enriched_by_id: dict[str, EconomicEvent] = {}
        missing: list[EconomicEvent] = []
        status = "completed"
        provider_metadata: dict[str, Any] = {}
        try:
            for event in events:
                metrics["events_checked"] += 1
                self.facts.upsert_economic_event(
                    event,
                    event_key=f"{event.country}:{event.date}:{event.event_id}",
                    valid_until=self.freshness.macro_valid_until(event),
                )
                if not self._should_enrich(event):
                    updated = event.model_copy(deep=True)
                    updated.enrichment.warnings.append("no_data_available: enrichment skipped by high-impact filter")
                    enriched_by_id[event.event_id] = updated
                    continue
                fact_key = self.fact_key(event)
                fact, freshness = self.event_materializer.lookup_fact(
                    event,
                    refresh_mode="force" if force else "auto",
                    metrics=metrics,
                )
                if fact:
                    if freshness.usable and force:
                        metrics["db_bypassed_force"] += 1
                        logger.info(
                            "event_enrichment_cache_bypassed_force",
                            extra={"event_id": event.event_id, "fact_key": fact_key, "fact_status": fact.get("status")},
                        )
                        self.observations.record(
                            run_id=run_id,
                            provider_name="market_facts",
                            provider_type="DB",
                            status="cache_bypassed_force",
                            country=event.country,
                            category=event.category,
                            query=fact_key,
                            item_count=1,
                        )
                    elif freshness.usable:
                        updated = self.event_materializer.apply_fact(
                            event,
                            fact,
                            cache_status=freshness.cache_status,
                            warnings=freshness.warnings,
                            refresh_mode="auto",
                            metrics=metrics,
                        )
                        enriched_by_id[event.event_id] = updated
                        metrics["db_hits"] += 1
                        self.observations.record(
                            run_id=run_id,
                            provider_name="market_facts",
                            provider_type="DB",
                            status="cache_hit",
                            country=event.country,
                            category=event.category,
                            query=fact_key,
                            item_count=1,
                        )
                        continue
                metrics["db_misses"] += 1
                self.observations.record(
                    run_id=run_id,
                    provider_name="market_facts",
                    provider_type="DB",
                    status="cache_miss",
                    country=event.country,
                    category=event.category,
                    query=fact_key,
                )
                missing.append(event)

            provider_missing: list[EconomicEvent] = []
            if missing and self.event_enrichment_service:
                provider_events, provider_metadata = await self.event_enrichment_service.enrich_events(
                    events=missing,
                    country=country,
                    start=start,
                    end=end,
                )
                self.observations.record(
                    run_id=run_id,
                    provider_name="event_enrichment_service",
                    provider_type="SCRAPER",
                    status="success" if any(_has_values(event.enrichment) for event in provider_events) else "no_data_available",
                    country=country,
                    item_count=sum(1 for event in provider_events if _has_values(event.enrichment)),
                    raw_payload_json=provider_metadata,
                )
                for event in provider_events:
                    if _has_values(event.enrichment):
                        fact = self._fact_from_event(event, provider_type=event.enrichment.provider_type)
                        self.facts.upsert_fact(fact)
                        read_back = self.facts.get_event_enrichment_fact(self.fact_key(event))
                        event = self.event_materializer.apply_fact(
                            event,
                            read_back or fact,
                            cache_status="refreshed",
                            warnings=[] if read_back else ["provider_fact_read_back_failed"],
                            refresh_mode="force" if force else "auto",
                            metrics=metrics,
                        )
                        enriched_by_id[event.event_id] = event
                        metrics["provider_hits"] += 1
                        metrics["facts_written"] += 1
                    else:
                        provider_missing.append(event)
                metrics["provider_misses"] += len(provider_missing)
            else:
                provider_missing = missing
                if missing:
                    metrics["warnings_json"].append("event_enrichment_service_unavailable")

            ai_used = False
            ai_called = False
            ai_succeeded = False
            ai_failure_reason = None
            ai_diagnostic_artifact_dir: str | None = None
            if provider_missing and self.settings.enable_ai_researcher:
                all_ai_candidates = self._ai_candidates(provider_missing, limit=False)
                ai_candidates = all_ai_candidates[: self.settings.ai_researcher_max_events]
                deferred_ai_candidates = all_ai_candidates[self.settings.ai_researcher_max_events :]
                metrics["ai_events_requested"] = len(ai_candidates)
                metrics["ai_candidate_event_ids"] = [event.event_id for event in ai_candidates]
                facts: list[dict[str, Any]] = []
                ai_status: dict[str, Any] = {"status": "not_required", "warning": "no_ai_candidates"}
                if ai_candidates:
                    metrics["ai_research_requests"] = 1
                    ai_called = True
                    started = perf_counter()
                    facts, ai_status = await self.ai_researcher_service.research_and_save(
                        [self._event_payload(event) for event in ai_candidates]
                    )
                    metrics["ai_duration_ms"] = int((perf_counter() - started) * 1000)
                    metrics["ai_research_status"] = ai_status.get("status") or "failed"
                    ai_diagnostic_artifact_dir = ai_status.get("diagnostic_artifact_dir")
                    ai_succeeded = ai_status.get("status") == "success"
                    ai_failure_reason = ai_status.get("failure_reason") or ai_status.get("error")
                    metrics["ai_results_valid"] = int(ai_status.get("results_valid") or len(facts))
                    metrics["ai_results_rejected"] = int(ai_status.get("results_rejected") or 0)
                    self.observations.record(
                        run_id=run_id,
                        provider_name="ai_researcher",
                        provider_type="AI_RESEARCHER_CODEX_CLI",
                        status="ai_research_used" if facts else ai_status.get("status", "no_data_available"),
                        country=country,
                        item_count=len(facts),
                        warning=";".join(ai_status.get("warnings", [])) if isinstance(ai_status.get("warnings"), list) else ai_status.get("warning"),
                        error=ai_status.get("error") or ai_status.get("failure_reason"),
                        raw_payload_json=ai_status,
                    )
                    metrics["facts_written"] += len(facts)
                    if not facts:
                        negative_reason = ai_status.get("failure_reason") or ai_status.get("status") or "no_data_available"
                        for event in ai_candidates:
                            negative = self._negative_ai_fact(event, reason=str(negative_reason))
                            self.facts.upsert_fact(negative)
                            facts.append(negative)
                        metrics["facts_written"] += len(ai_candidates)
                    for event in deferred_ai_candidates:
                        self.facts.upsert_fact(self._negative_ai_fact(event, reason="ai_batch_deferred"))
                        metrics["facts_written"] += 1
                else:
                    metrics["ai_research_status"] = "not_required"
                facts_by_key = {fact["fact_key"]: fact for fact in facts}
                diagnostics = AIResearchDiagnostics(self.settings, artifact_dir=ai_diagnostic_artifact_dir)
                for event in provider_missing:
                    fact = facts_by_key.get(self.fact_key(event))
                    updated = event.model_copy(deep=True)
                    if fact:
                        persisted = self.facts.upsert_fact(fact)
                        read_back = self.facts.get_event_enrichment_fact(self.fact_key(event))
                        diagnostics.event_json(event.event_id, "persistence_payload.json", persisted)
                        diagnostics.event_json(event.event_id, "read_back.json", read_back)
                        if read_back:
                            updated = self.event_materializer.apply_fact(
                                event,
                                read_back,
                                cache_status="refreshed",
                                warnings=[],
                                refresh_mode="force" if force else "auto",
                                metrics=metrics,
                            )
                        else:
                            updated = self.event_materializer.apply_fact(
                                event,
                                fact,
                                cache_status="refreshed",
                                warnings=["ai_fact_read_back_failed"],
                                refresh_mode="force" if force else "auto",
                                metrics=metrics,
                            )
                        updated.enrichment.summary["persistence"] = {
                            "persisted": True,
                            "read_back": read_back is not None,
                        }
                        if _has_values(updated.enrichment):
                            ai_used = True
                    else:
                        updated.enrichment.warnings.append("missing_enrichment_data")
                    enriched_by_id[event.event_id] = updated
            else:
                metrics["ai_research_status"] = "disabled" if not self.settings.enable_ai_researcher else "not_required"
                if provider_missing:
                    metrics["warnings_json"].append("ai_researcher_disabled")
                for event in provider_missing:
                    updated = event.model_copy(deep=True)
                    if not updated.enrichment.warnings:
                        updated.enrichment.warnings.append("missing_enrichment_data")
                    enriched_by_id[event.event_id] = updated

            result = [enriched_by_id.get(event.event_id, event) for event in events]
            data_quality = {
                "events_found": len(events),
                "enrichment_complete": metrics["provider_misses"] == 0,
                "enrichment_partial": metrics["provider_hits"] > 0 and metrics["provider_misses"] > 0,
                "enrichment_timeout": False,
                "enrichment_status": "enrichment_complete" if metrics["provider_misses"] == 0 else ("enrichment_partial" if metrics["provider_hits"] else "enrichment_missing"),
                "db_hits": metrics["db_hits"],
                "db_misses": metrics["db_misses"],
                "db_bypassed_force": metrics["db_bypassed_force"],
                "refresh_mode": "force" if force else "auto",
                "provider_hits": metrics["provider_hits"],
                "provider_failures": metrics["provider_misses"],
                "ai_research_used": ai_used,
                "ai_research_enabled": bool(self.settings.enable_ai_researcher),
                "ai_research_configured": bool(self.settings.enable_ai_researcher and self.settings.ai_researcher_mode in {"codex_cli", "openai_api"}),
                "ai_research_mode": self.settings.ai_researcher_mode,
                "ai_research_called": ai_called,
                "ai_research_succeeded": ai_succeeded,
                "ai_research_requests": metrics["ai_research_requests"],
                "ai_events_requested": metrics["ai_events_requested"],
                "ai_candidate_event_ids": metrics["ai_candidate_event_ids"],
                "ai_research_status": metrics["ai_research_status"],
                "ai_duration_ms": metrics["ai_duration_ms"],
                "ai_not_available": metrics["ai_not_available"],
                "ai_diagnostic_artifact_dir": ai_diagnostic_artifact_dir,
                "ai_results_valid": metrics["ai_results_valid"],
                "ai_results_rejected": metrics["ai_results_rejected"],
                "ai_failure_reason": ai_failure_reason,
                "missing_critical_fields": [
                    self.fact_key(event) for event in result if self._is_ai_researchable(event) and not _has_values(event.enrichment)
                ],
                "stale_fields": [
                    self.fact_key(event) for event in result if "stale_fact" in event.enrichment.warnings
                ],
                "warnings": metrics["warnings_json"],
                "errors": metrics["errors_json"],
                **{name: metrics[name] for name in self.event_materializer.empty_metrics()},
            }
            metadata = {
                "run_id": run_id,
                "service_role": "data provider only",
                "data_quality": data_quality,
                "provider_metadata": provider_metadata,
                "decisions_delegated_to": "AI-TRADER",
                "trading_logic": "not implemented; data service only",
            }
            return result, metadata
        except BaseException as exc:
            status = "failed"
            metrics["errors_json"].append(str(exc) or exc.__class__.__name__)
            raise
        finally:
            self.runs.finish(run_id=run_id, status=status, metrics=metrics)

    def fact_key(self, event: EconomicEvent) -> str:
        return self.event_materializer.fact_key(event)

    def _should_enrich(self, event: EconomicEvent) -> bool:
        if self.settings.ai_researcher_only_high_impact and event.impact != Impact.HIGH:
            return False
        return event.country.upper() == "US"

    def _fact_from_event(self, event: EconomicEvent, provider_type: ProviderType | str | None) -> dict[str, Any]:
        valid_until = self.freshness.macro_valid_until(event)
        return {
            "fact_key": self.fact_key(event),
            "fact_type": "macro_event_enrichment",
            "country": event.country.upper(),
            "category": event.category.upper(),
            "event_name": event.name,
            "forecast": event.enrichment.forecast,
            "previous": event.enrichment.previous,
            "consensus": event.enrichment.consensus,
            "actual": event.enrichment.actual,
            "source": event.enrichment.source,
            "source_url": event.enrichment.source_url,
            "provider_type": str(provider_type or "SCRAPER").split(".")[-1],
            "reliability": event.enrichment.reliability,
            "confidence": event.enrichment.confidence or event.enrichment.reliability,
            "retrieved_at": (event.enrichment.retrieved_at or datetime.now(UTC)).isoformat(),
            "release_at": event.time_utc.isoformat() if event.time_utc else None,
            "valid_until": valid_until,
            "next_refresh_at": self.freshness.next_refresh_at(valid_until),
            "warnings_json": event.enrichment.warnings,
            "errors_json": event.enrichment.errors,
            "raw_payload_json": event.model_dump(mode="json"),
        }

    def _event_payload(self, event: EconomicEvent) -> dict[str, Any]:
        payload = event.model_dump(mode="json")
        payload["fact_key"] = self.fact_key(event)
        payload["valid_until"] = self.freshness.macro_valid_until(event)
        return payload

    def _ai_candidates(self, events: list[EconomicEvent], *, limit: bool = True) -> list[EconomicEvent]:
        candidates = []
        for event in events:
            if self._is_ai_researchable(event):
                candidates.append(event)
        sorted_candidates = sorted(
            candidates,
            key=lambda event: AI_PRIORITY.get(_priority_category(event), 99),
        )
        return sorted_candidates[: self.settings.ai_researcher_max_events] if limit else sorted_candidates

    def _is_ai_researchable(self, event: EconomicEvent) -> bool:
        category = _priority_category(event)
        name = event.name.upper()
        if event.impact != Impact.HIGH:
            return False
        if "FED SPEECH" in name or ("SPEECH" in name and "FED" in name) or "PRESS CONFERENCE" in name:
            return False
        if _invalid_employment_period_mapping(event):
            return False
        return category in AI_PRIORITY or any(key in name for key in AI_PRIORITY)

    def _negative_ai_fact(self, event: EconomicEvent, *, reason: str) -> dict[str, Any]:
        valid_until = (datetime.now(UTC) + timedelta(hours=2)).replace(microsecond=0).isoformat()
        return {
            "fact_key": self.fact_key(event),
            "fact_type": "macro_event_enrichment",
            "country": event.country.upper(),
            "category": event.category.upper(),
            "event_name": event.name,
            "source": "AI Researcher",
            "source_url": event.source_url,
            "provider_type": "AI_RESEARCHER_CODEX_CLI",
            "reliability": 0,
            "confidence": 0,
            "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "release_at": event.time_utc.isoformat() if event.time_utc else None,
            "valid_until": valid_until,
            "next_refresh_at": valid_until,
            "status": "no_data_available",
            "notes": reason,
            "warnings_json": [f"ai_negative_cache:{reason}"],
            "raw_payload_json": self._event_payload(event),
        }


def _has_values(enrichment: EventEnrichment) -> bool:
    if any(getattr(enrichment, field) not in (None, "") for field in VALUE_FIELDS):
        return True
    for metric in enrichment.metrics:
        if isinstance(metric, dict) and any(metric.get(field) not in (None, "") for field in VALUE_FIELDS):
            return True
    fomc = enrichment.fomc_context or {}
    return any(fomc.get(field) not in (None, "", "unknown") for field in ("expected_action", "probability_hold", "probability_cut", "probability_hike"))


def _priority_category(event: EconomicEvent) -> str:
    category = event.category.upper()
    name = event.name.upper()
    if "NFP" in category or "NONFARM" in category or "EMPLOYMENT SITUATION" in name:
        return "NFP"
    for key in AI_PRIORITY:
        if key in category or key in name:
            return key
    return category


def _invalid_employment_period_mapping(event: EconomicEvent) -> bool:
    name = event.name.upper()
    category = event.category.upper()
    if "EMPLOYMENT SITUATION" not in name and "NONFARM" not in category and "NFP" not in category:
        return False
    months = {
        "JANUARY": 1,
        "FEBRUARY": 2,
        "MARCH": 3,
        "APRIL": 4,
        "MAY": 5,
        "JUNE": 6,
        "JULY": 7,
        "AUGUST": 8,
        "SEPTEMBER": 9,
        "OCTOBER": 10,
        "NOVEMBER": 11,
        "DECEMBER": 12,
    }
    named_month = next((number for label, number in months.items() if label in name), None)
    if named_month is None:
        return False
    try:
        release = datetime.fromisoformat(event.date)
    except ValueError:
        return False
    expected_period_month = release.month - 1 or 12
    return named_month != expected_period_month
