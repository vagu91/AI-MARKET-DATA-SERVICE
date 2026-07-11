from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, Impact, ProviderResult, ProviderType
from app.models.events import EconomicEvent
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import event_id, html_text_lines, parse_time
from app.providers.calendar_utils import REQUEST_HEADERS


MONTH_NAMES = {
    1: "january",
    2: "february",
    3: "march",
    4: "april",
    5: "may",
    6: "june",
    7: "july",
    8: "august",
    9: "september",
    10: "october",
    11: "november",
    12: "december",
}


class FederalReserveCalendarProvider(BaseProvider):
    source = "Federal Reserve Calendar"
    provider_type = ProviderType.SCRAPER
    reliability = 0.86
    cache_key = "provider:federal_reserve_calendar:events:v2"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings
        self.local_tz = ZoneInfo(settings.timezone)
        self.eastern_tz = ZoneInfo("America/New_York")

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
                url = (
                    f"{self.settings.federal_reserve_calendar_base_url}/"
                    f"{year}-{MONTH_NAMES[month]}.htm"
                )
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
        category: str | None = None
        i = 0
        headings = {
            "Speeches",
            "FOMC Meetings",
            "Beige Book",
            "Statistical Releases",
            "Other",
            "Conferences",
        }
        while i < len(lines):
            if lines[i] in headings:
                category = lines[i] if lines[i] in {"Speeches", "FOMC Meetings"} else None
                i += 1
                continue
            if category and parse_time(lines[i]):
                event_time = parse_time(lines[i])
                name_parts: list[str] = []
                j = i + 1
                while j < len(lines) and not lines[j].isdigit() and not parse_time(lines[j]):
                    if lines[j] not in {"Time:", "Release Date(s):", "Watch Live"}:
                        name_parts.append(lines[j])
                    j += 1
                if j < len(lines) and lines[j].isdigit():
                    day = int(lines[j])
                    name = " - ".join(name_parts[:3]).strip()
                    if name:
                        events.append(
                            self._event(name, category, date(year, month, day), event_time, source_url)
                        )
                    i = j + 1
                    continue
            i += 1
        return events

    def _event(
        self,
        name: str,
        section: str,
        event_date: date,
        event_time,
        source_url: str,
    ) -> dict[str, object]:
        local_et = datetime.combine(event_date, event_time, tzinfo=self.eastern_tz)
        time_utc = local_et.astimezone(UTC)
        lower = name.lower()
        is_fomc = section == "FOMC Meetings" or "fomc" in lower
        is_chair = "chair" in lower or "powell" in lower
        impact = Impact.HIGH if is_fomc or is_chair else Impact.MEDIUM
        if "minutes" in lower:
            category = "FOMC Minutes"
        elif is_fomc:
            category = "FOMC"
        elif section == "Speeches":
            category = "Fed Speech"
        else:
            category = "Federal Reserve"
        return EconomicEvent(
            event_id=event_id("fed-cal", name, time_utc.isoformat()),
            name=name,
            country="US",
            category=category,
            date=event_date.isoformat(),
            time_utc=time_utc,
            time_local=time_utc.astimezone(self.local_tz),
            impact=impact,
            source=self.source,
            source_url=source_url,
            reliability=self.reliability,
            incomplete_time=False,
            event_risk_level=impact,
            default_risk_window_before_minutes=30 if impact == Impact.HIGH else 0,
            default_risk_window_after_minutes=30 if impact == Impact.HIGH else 0,
        ).model_dump(mode="json")
