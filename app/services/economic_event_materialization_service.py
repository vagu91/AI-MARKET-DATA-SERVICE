from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from app.core.config import Settings
from app.models.common import ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.services.data_freshness_service import DataFreshnessService
from app.services.fact_key_service import FactKeyService
from app.services.market_fact_repository import (
    CANONICAL_EVENT_ENRICHMENT_TYPE,
    LEGACY_EVENT_ENRICHMENT_TYPE,
    MarketFactRepository,
)

logger = logging.getLogger(__name__)

MATERIALIZATION_METRICS = (
    "history_event_count",
    "enrichment_fact_lookup_count",
    "enrichment_fact_hit_count",
    "enrichment_fact_miss_count",
    "enrichment_fact_stale_count",
    "enrichment_materialized_count",
    "legacy_fact_type_count",
)


class EconomicEventMaterializationService:
    def __init__(self, settings: Settings, *, facts: MarketFactRepository | None = None) -> None:
        self.settings = settings
        self.facts = facts or MarketFactRepository(settings)
        self.keys = FactKeyService()
        self.freshness = DataFreshnessService(settings)

    @staticmethod
    def empty_metrics(*, history_event_count: int = 0) -> dict[str, int]:
        return {name: history_event_count if name == "history_event_count" else 0 for name in MATERIALIZATION_METRICS}

    def load_from_history(
        self,
        *,
        country: str,
        start: datetime,
        end: datetime,
        refresh_mode: str = "false",
    ) -> tuple[list[EconomicEvent], dict[str, int]]:
        payloads = self.facts.economic_event_payloads(
            country=country,
            start_date=start.date().isoformat(),
            end_date=end.date().isoformat(),
        )
        events: list[EconomicEvent] = []
        for payload in payloads:
            try:
                event = EconomicEvent.model_validate(payload)
            except Exception:
                continue
            events.append(event)
            logger.info(
                "event_history_loaded",
                extra={"event_id": event.event_id, "refresh_mode": refresh_mode},
            )
        metrics = self.empty_metrics(history_event_count=len(events))
        return self.materialize_events(events, refresh_mode=refresh_mode, metrics=metrics)

    def materialize_events(
        self,
        events: list[EconomicEvent],
        *,
        refresh_mode: str,
        metrics: dict[str, int] | None = None,
    ) -> tuple[list[EconomicEvent], dict[str, int]]:
        counters = metrics or self.empty_metrics()
        output = [self.materialize_event(event, refresh_mode=refresh_mode, metrics=counters) for event in events]
        return output, counters

    def lookup_fact(
        self,
        event: EconomicEvent,
        *,
        refresh_mode: str,
        metrics: dict[str, int],
    ) -> tuple[dict[str, Any] | None, Any | None]:
        fact_key = self.fact_key(event)
        metrics["enrichment_fact_lookup_count"] += 1
        logger.info(
            "event_enrichment_fact_lookup",
            extra={"event_id": event.event_id, "fact_key": fact_key, "refresh_mode": refresh_mode},
        )
        fact = self.facts.get_event_enrichment_fact(fact_key)
        if fact is None:
            metrics["enrichment_fact_miss_count"] += 1
            logger.info(
                "event_enrichment_missing",
                extra={"event_id": event.event_id, "fact_key": fact_key, "refresh_mode": refresh_mode},
            )
            return None, None

        freshness = self.freshness.evaluate(fact)
        fact_type = str(fact.get("fact_type") or "")
        if fact_type == LEGACY_EVENT_ENRICHMENT_TYPE:
            metrics["legacy_fact_type_count"] += 1
            logger.info(
                "event_enrichment_fact_legacy_type_used",
                extra=self._log_context(event, fact, refresh_mode, freshness.cache_status),
            )
        if not freshness.usable:
            metrics["enrichment_fact_stale_count"] += 1
            logger.info(
                "event_enrichment_stale",
                extra=self._log_context(event, fact, refresh_mode, freshness.cache_status),
            )
            return fact, freshness

        metrics["enrichment_fact_hit_count"] += 1
        logger.info(
            "event_enrichment_fact_found",
            extra=self._log_context(event, fact, refresh_mode, freshness.cache_status),
        )
        return fact, freshness

    def materialize_event(
        self,
        event: EconomicEvent,
        *,
        refresh_mode: str,
        metrics: dict[str, int],
    ) -> EconomicEvent:
        fact, freshness = self.lookup_fact(event, refresh_mode=refresh_mode, metrics=metrics)
        if fact is None:
            return event.model_copy(deep=True)
        if freshness is None or not freshness.usable:
            updated = event.model_copy(deep=True)
            updated.enrichment.cache_status = freshness.cache_status if freshness else "miss"
            updated.enrichment.warnings.extend(freshness.warnings if freshness else ["event_enrichment_fact_missing"])
            return updated
        return self.apply_fact(
            event,
            fact,
            cache_status=freshness.cache_status,
            warnings=freshness.warnings,
            refresh_mode=refresh_mode,
            metrics=metrics,
        )

    def apply_fact(
        self,
        event: EconomicEvent,
        fact: dict[str, Any],
        *,
        cache_status: str,
        warnings: list[str],
        refresh_mode: str,
        metrics: dict[str, int] | None = None,
    ) -> EconomicEvent:
        updated = event.model_copy(deep=True)
        updated.enrichment = self.enrichment_from_fact(fact, cache_status, warnings)
        if metrics is not None:
            metrics["enrichment_materialized_count"] += 1
        logger.info(
            "event_enrichment_materialized",
            extra=self._log_context(event, fact, refresh_mode, cache_status),
        )
        return updated

    def enrichment_from_fact(self, fact: dict[str, Any], cache_status: str, warnings: list[str]) -> EventEnrichment:
        provider_type = str(fact.get("provider_type") or "DB").split(".")[-1]
        try:
            parsed_provider_type = ProviderType(provider_type)
        except ValueError:
            parsed_provider_type = ProviderType.DB
        raw_payload = fact.get("raw_payload") if isinstance(fact.get("raw_payload"), dict) else {}
        fact_warnings = list(fact.get("warnings") or [])
        if fact.get("status") == "no_data_available" and "no_data_available" not in fact_warnings:
            fact_warnings.append("no_data_available")
        return EventEnrichment(
            forecast=fact.get("forecast"),
            previous=fact.get("previous"),
            consensus=fact.get("consensus"),
            actual=fact.get("actual"),
            metrics=list(raw_payload.get("metrics") or []),
            summary=dict(raw_payload.get("summary") or {}),
            fomc_context=raw_payload.get("fomc_context"),
            source=fact.get("source"),
            source_url=fact.get("source_url"),
            provider_type=parsed_provider_type,
            retrieved_at=fact.get("retrieved_at"),
            valid_until=fact.get("valid_until"),
            next_refresh_at=fact.get("next_refresh_at"),
            reliability=fact.get("reliability") or 0,
            confidence=fact.get("confidence") or 0,
            cache_status=cache_status,
            warnings=[*warnings, *fact_warnings],
            errors=list(fact.get("errors") or []),
        )

    def fact_key(self, event: EconomicEvent) -> str:
        return self.keys.macro_event_key(
            country=event.country,
            category=event.category,
            event_date=event.date,
            event_name=event.name,
            fact_type=CANONICAL_EVENT_ENRICHMENT_TYPE,
        )

    def _log_context(
        self,
        event: EconomicEvent,
        fact: dict[str, Any],
        refresh_mode: str,
        cache_status: str,
    ) -> dict[str, Any]:
        return {
            "event_id": event.event_id,
            "fact_key": fact.get("fact_key") or self.fact_key(event),
            "fact_type": fact.get("fact_type"),
            "refresh_mode": refresh_mode,
            "cache_status": cache_status,
            "provider_type": fact.get("provider_type"),
            "valid_until": fact.get("valid_until"),
        }
