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
        self.last_provider_results: list[object] = []
        self.last_coverage_proof: dict[str, object] = {}

    async def list_events(
        self,
        country: str = "US",
        start: datetime | None = None,
        end: datetime | None = None,
        enrich: bool = True,
    ) -> list[EconomicEvent]:
        events: list[EconomicEvent] = []
        self.last_provider_results = []
        self.last_coverage_proof = {}
        parsed_sources = 0
        quarantined_count = 0
        for provider in self.providers:
            result = await provider.fetch_safe()
            self.last_provider_results.append(result.metadata)
            if not isinstance(result.data, list):
                continue
            parsed_sources += 1
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
                    quarantined_count += 1
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
        errors = [
            error
            for metadata in self.last_provider_results
            for error in list(getattr(metadata, "errors", []) or [])
        ]
        expected_sources_complete = (
            len(self.last_provider_results) == len(self.providers)
            and parsed_sources == len(self.providers)
        )
        self.last_coverage_proof = {
            "request_succeeded": not errors and expected_sources_complete,
            "scope_match": start is not None and end is not None,
            "pagination_complete": expected_sources_complete,
            "parsing_succeeded": parsed_sources == len(self.providers),
            "records_valid": quarantined_count == 0,
            "expected_sources_complete": expected_sources_complete,
            # Empty provider datasets lack affirmative range-coverage proof.
            "authentic_empty": False,
            "requested_scope": {
                "country": country.upper(),
                "start": start.isoformat() if start else None,
                "end": end.isoformat() if end else None,
            },
            "quarantined_count": quarantined_count,
        }
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
