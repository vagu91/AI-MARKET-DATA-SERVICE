from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import httpx

from app.core.cache import SQLiteCache
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.events import EconomicEvent
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import (
    REQUEST_HEADERS,
    classify_event,
    eastern_to_utc,
    event_id,
    html_text_lines,
    parse_time,
)


MONTHS = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}


class BeaReleaseScheduleProvider(BaseProvider):
    source = "BEA Release Schedule"
    provider_type = ProviderType.SCRAPER
    reliability = 0.84
    cache_key = "provider:bea_release_schedule:events:v2"

    def __init__(self, cache: SQLiteCache, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings
        self.local_tz = ZoneInfo(settings.timezone)

    async def fetch(self) -> ProviderResult:
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            response = await client.get(
                self.settings.bea_release_schedule_url,
                headers=REQUEST_HEADERS,
            )
            response.raise_for_status()

        now = datetime.now(UTC)
        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=now,
                freshness=Freshness.RECENT,
            ),
            data=self._parse(response.text),
        )

    def _parse(self, html: str) -> list[dict[str, object]]:
        lines = html_text_lines(html)
        year = datetime.now(UTC).year
        for idx, line in enumerate(lines):
            if line.startswith("Year "):
                for token in line.split():
                    if token.isdigit() and len(token) == 4:
                        year = int(token)
                        break
                lines = lines[idx + 1 :]
                break

        events: list[dict[str, object]] = []
        i = 0
        while i < len(lines) - 3:
            month_day = lines[i].split()
            if len(month_day) == 2 and month_day[0] in MONTHS and month_day[1].isdigit():
                local_date = date(year, MONTHS[month_day[0]], int(month_day[1]))
                time_value = parse_time(lines[i + 1])
                name_idx = i + 2
                while name_idx < len(lines) and lines[name_idx] in {
                    "N",
                    "D",
                    "V",
                    "A",
                    "ews",
                    "ata",
                    "isual Data",
                    "rticle",
                    "News",
                    "N ews",
                    "Data",
                    "D ata",
                    "Visual Data",
                    "V isual Data",
                    "Article",
                    "A rticle",
                    "Release",
                }:
                    name_idx += 1
                name = lines[name_idx] if name_idx < len(lines) else ""
                if time_value and name:
                    impact, category, has_default_window = classify_event(name)
                    time_utc, time_local_et = eastern_to_utc(local_date, time_value)
                    event = EconomicEvent(
                        event_id=event_id("bea-cal", name, local_date.isoformat()),
                        name=name,
                        country="US",
                        category=category,
                        date=local_date.isoformat(),
                        time_utc=time_utc,
                        time_local=time_utc.astimezone(self.local_tz) if time_utc else time_local_et,
                        impact=impact,
                        source=self.source,
                        source_url=self.settings.bea_release_schedule_url,
                        reliability=self.reliability,
                        incomplete_time=time_utc is None,
                        event_risk_level=impact,
                        default_risk_window_before_minutes=(
                            30 if has_default_window and time_utc else 0
                        ),
                        default_risk_window_after_minutes=(
                            30 if has_default_window and time_utc else 0
                        ),
                    )
                    events.append(event.model_dump(mode="json"))
                    i = name_idx + 1
                    continue
            i += 1
        return events
