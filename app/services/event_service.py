import inspect
from datetime import UTC, date, datetime, timedelta

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
        self.last_provider_coverage_proofs: list[dict[str, object]] = []

    def coverage_targets(
        self,
        *,
        country: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, object]]:
        return [
            {
                "provider_id": str(
                    getattr(
                        provider,
                        "_registry_provider_id",
                        provider.source,
                    )
                ),
                "provider_name": provider.source,
                "query_scope": f"country={country.upper()}",
            }
            for provider in self.providers
            if bool(
                getattr(provider, "authoritative_calendar_coverage", False)
            )
        ]

    async def list_events(
        self,
        country: str = "US",
        start: datetime | None = None,
        end: datetime | None = None,
        enrich: bool = True,
        provider_names: list[str] | None = None,
        force: bool = False,
    ) -> list[EconomicEvent]:
        events: list[EconomicEvent] = []
        self.last_provider_results = []
        self.last_coverage_proof = {}
        self.last_provider_coverage_proofs = []
        parsed_sources = 0
        quarantined_count = 0
        selected = [
            provider
            for provider in self.providers
            if not provider_names or provider.source in set(provider_names)
        ]
        for provider in selected:
            fetch_call = provider.fetch_safe
            parameters = inspect.signature(fetch_call).parameters
            fetch_kwargs = (
                {"force": force}
                if (
                    "force" in parameters
                    or any(
                        parameter.kind
                        == inspect.Parameter.VAR_KEYWORD
                        for parameter in parameters.values()
                    )
                )
                else {}
            )
            result = await fetch_call(**fetch_kwargs)
            self.last_provider_results.append(result.metadata)
            if not isinstance(result.data, list):
                continue
            parsed_sources += 1
            provider_events: list[EconomicEvent] = []
            provider_quarantined = 0
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
                    provider_quarantined += 1
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
                provider_events.append(event)
            if bool(
                getattr(provider, "authoritative_calendar_coverage", False)
            ):
                covered_dates = (
                    provider.coverage_dates(start=start, end=end)
                    if start is not None
                    and end is not None
                    and callable(getattr(provider, "coverage_dates", None))
                    else []
                )
                event_dates = {
                    _event_date(event)
                    for event in provider_events
                    if _event_date(event) is not None
                }
                errors = list(result.metadata.errors or [])
                succeeded = not errors
                self.last_provider_coverage_proofs.append(
                    {
                        "provider_name": provider.source,
                        "query_scope": f"country={country.upper()}",
                        "request_succeeded": succeeded,
                        "scope_match": bool(covered_dates),
                        "pagination_complete": succeeded,
                        "parsing_succeeded": True,
                        "records_valid": provider_quarantined == 0,
                        "expected_sources_complete": succeeded,
                        "authentic_empty": False,
                        "covered_dates": sorted(
                            item.isoformat() for item in covered_dates
                        ),
                        "authentic_empty_dates": sorted(
                            item.isoformat()
                            for item in set(covered_dates) - event_dates
                        ),
                        "record_counts_by_date": {
                            day.isoformat(): sum(
                                _event_date(item) == day
                                for item in provider_events
                            )
                            for day in covered_dates
                        },
                    }
                )
        errors = [
            error
            for metadata in self.last_provider_results
            for error in list(getattr(metadata, "errors", []) or [])
        ]
        expected_sources_complete = (
            len(self.last_provider_results) == len(selected)
            and parsed_sources == len(selected)
        )
        self.last_coverage_proof = {
            "request_succeeded": not errors and expected_sources_complete,
            "scope_match": start is not None and end is not None,
            "pagination_complete": expected_sources_complete,
            "parsing_succeeded": parsed_sources == len(selected),
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


def _event_date(event: EconomicEvent) -> date | None:
    try:
        return date.fromisoformat(event.date)
    except (TypeError, ValueError):
        return (
            event.time_utc.astimezone(UTC).date()
            if event.time_utc is not None
            else None
        )
