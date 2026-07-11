from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.events import EconomicEvent
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import (
    classify_event,
    eastern_to_utc,
    event_id,
    html_text_lines,
    parse_time,
    REQUEST_HEADERS,
)


class BlsReleaseCalendarProvider(BaseProvider):
    source = "BLS Release Calendar"
    provider_type = ProviderType.SCRAPER
    reliability = 0.82
    cache_key = "provider:bls_release_calendar:events:v2"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings
        self.local_tz = ZoneInfo(settings.timezone)

    async def fetch(self) -> ProviderResult:
        now = datetime.now(UTC)
        months = [(now.year, now.month)]
        if now.month == 12:
            months.append((now.year + 1, 1))
        else:
            months.append((now.year, now.month + 1))

        events: list[dict[str, object]] = []
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            for year, month in months:
                url = f"{self.settings.bls_schedule_base_url}/{year}/{month:02d}_sched.htm"
                response = await client.get(url, headers=REQUEST_HEADERS)
                response.raise_for_status()
                events.extend(self._parse_month(response.text, url, year, month))

        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=now,
                freshness=Freshness.RECENT,
            ),
            data=events,
        )

    def _parse_month(self, html: str, source_url: str, year: int, month: int) -> list[dict[str, object]]:
        lines = html_text_lines(html)
        events: list[dict[str, object]] = []
        current_day: int | None = None
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.isdigit() and 1 <= int(line) <= 31:
                current_day = int(line)
                i += 1
                continue
            if current_day and i + 2 < len(lines):
                name = line
                period = lines[i + 1]
                time_value = parse_time(lines[i + 2])
                if time_value and not name.isdigit() and not period.isdigit():
                    impact, category, has_default_window = classify_event(name)
                    local_date = date(year, month, current_day)
                    time_utc, time_local_et = eastern_to_utc(local_date, time_value)
                    event = EconomicEvent(
                        event_id=event_id("bls", name, period, local_date.isoformat()),
                        name=f"{name} ({period})",
                        country="US",
                        category=category,
                        date=local_date.isoformat(),
                        time_utc=time_utc,
                        time_local=time_utc.astimezone(self.local_tz) if time_utc else time_local_et,
                        impact=impact,
                        source=self.source,
                        source_url=source_url,
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
                    i += 3
                    continue
            i += 1
        return events
