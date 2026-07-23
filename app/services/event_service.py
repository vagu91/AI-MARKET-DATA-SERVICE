from datetime import UTC, datetime, timedelta

from app.models.events import EconomicEvent
from app.providers.bea_calendar import BeaReleaseScheduleProvider
from app.providers.bls_calendar import BlsReleaseCalendarProvider
from app.providers.fed_calendar import FederalReserveCalendarProvider
from app.providers.federal_reserve import FederalReserveRssProvider
from app.providers.scraper_calendar import EconomicCalendarScraperProvider
from app.services.event_enrichment_service import EventEnrichmentService
from app.services.temporal_validation_service import TemporalValidationService


class EventService:
    def __init__(
        self,
        providers: list[
            FederalReserveCalendarProvider
            | FederalReserveRssProvider
            | BlsReleaseCalendarProvider
            | BeaReleaseScheduleProvider
            | EconomicCalendarScraperProvider
        ],
        enrichment_service: EventEnrichmentService | None = None,
        temporal_validation: TemporalValidationService | None = None,
    ) -> None:
        self.providers = providers
        self.enrichment_service = enrichment_service
        self.temporal_validation = temporal_validation
        self.last_enrichment_metadata: dict[str, object] = {}

    async def list_events(
        self,
        country: str = "US",
        start: datetime | None = None,
        end: datetime | None = None,
        enrich: bool = True,
    ) -> list[EconomicEvent]:
        events: list[EconomicEvent] = []
        for provider in self.providers:
            result = await provider.fetch_safe()
            if not isinstance(result.data, list):
                continue
            for raw in result.data:
                event = EconomicEvent.model_validate(raw)
                if event.country.upper() != country.upper():
                    continue
                event_payload = event.model_dump(mode="json")
                if (
                    self.temporal_validation is not None
                    and self.temporal_validation.quarantine_if_invalid(
                        event_payload,
                        entity_table="provider_ingestion",
                    )
                ):
                    continue
                if event.time_utc:
                    event_time = event.time_utc.astimezone(UTC)
                    if start and event_time < start:
                        continue
                    if end and event_time > end:
                        continue
                elif event.incomplete_time:
                    event_date = datetime.fromisoformat(event.date).replace(tzinfo=UTC)
                    if start and event_date.date() < start.date():
                        continue
                    if end and event_date.date() > end.date():
                        continue
                events.append(event)
        events = sorted(events, key=lambda event: event.time_utc or datetime.max.replace(tzinfo=UTC))
        if enrich and self.enrichment_service and start and end:
            events, metadata = await self.enrichment_service.enrich_events(
                events=events,
                country=country,
                start=start,
                end=end,
            )
            self.last_enrichment_metadata = metadata
        else:
            self.last_enrichment_metadata = {}
        return events

    async def today(self, country: str = "US") -> list[EconomicEvent]:
        now = datetime.now(UTC)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        return await self.list_events(country=country, start=start, end=end)

    async def upcoming(self, country: str = "US", days: int = 7) -> list[EconomicEvent]:
        now = datetime.now(UTC)
        return await self.list_events(country=country, start=now, end=now + timedelta(days=days))
