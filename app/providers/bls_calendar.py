import re
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from typing import Any
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
    parse_time,
    REQUEST_HEADERS,
)


def _covered_dates(
    start: datetime,
    end: datetime,
    months: set[tuple[int, int]],
) -> list[date]:
    timezone = ZoneInfo("America/New_York")
    first = start.astimezone(timezone).date()
    last = (end.astimezone(timezone) - datetime.resolution).date()
    return [
        first + (date.resolution * offset)
        for offset in range((last - first).days + 1)
        if (
            (first + (date.resolution * offset)).year,
            (first + (date.resolution * offset)).month,
        )
        in months
    ]


class BlsReleaseCalendarProvider(BaseProvider):
    source = "BLS Release Calendar"
    canonical_source = "BLS"
    query_scope = "bls_release_occurrence"
    provider_type = ProviderType.SCRAPER
    reliability = 0.82
    cache_key = "provider:bls_release_calendar:events:v2"
    authoritative_calendar_coverage = True

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
                url = (
                    f"{self.settings.bls_schedule_base_url}/{year}/"
                    f"{month:02d}_sched_list.htm"
                )
                response = await client.get(url, headers=REQUEST_HEADERS)
                response.raise_for_status()
                events.extend(
                    self._parse_month(
                        response.text,
                        url,
                        year,
                        month,
                        retrieved_at=now,
                    )
                )

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

    def coverage_dates(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[date]:
        now = datetime.now(UTC)
        months = {(now.year, now.month)}
        months.add(
            (now.year + 1, 1)
            if now.month == 12
            else (now.year, now.month + 1)
        )
        return _covered_dates(start, end, months)

    def _parse_month(
        self,
        html: str,
        source_url: str,
        year: int,
        month: int,
        *,
        retrieved_at: datetime | None = None,
    ) -> list[dict[str, object]]:
        rows = _BlsListTableParser.parse(html)
        return [
            event
            for row in rows
            if (
                event := self._event_from_list_row(
                    row,
                    source_url=source_url,
                    expected_year=year,
                    expected_month=month,
                    retrieved_at=retrieved_at,
                )
            )
            is not None
        ]

    def _event_from_list_row(
        self,
        row: list[str],
        *,
        source_url: str,
        expected_year: int,
        expected_month: int,
        retrieved_at: datetime | None,
    ) -> dict[str, object] | None:
        if len(row) < 3:
            return None
        release_date = _parse_full_date(row[0])
        time_value = parse_time(row[1])
        if (
            release_date is None
            or release_date.year != expected_year
            or release_date.month != expected_month
            or time_value is None
        ):
            return None
        name, period = _split_release_and_period(" ".join(row[2:]))
        if not name or not period:
            return None
        impact, category, has_default_window = classify_event(name)
        time_utc, time_local_et = eastern_to_utc(release_date, time_value)
        provider_event_id = event_id(
            "bls",
            name,
            period,
            release_date.isoformat(),
        )
        event = EconomicEvent(
            event_id=provider_event_id,
            provider="BLS",
            provider_event_id=provider_event_id,
            source_event_id=provider_event_id,
            occurrence_id=f"bls:{provider_event_id}:{release_date.isoformat()}",
            name=f"{name} ({period})",
            country="US",
            category=category,
            reference_period=period,
            frequency=_period_frequency(period),
            date=release_date.isoformat(),
            time_utc=time_utc,
            release_at=time_utc,
            time_local=(
                time_utc.astimezone(self.local_tz)
                if time_utc
                else time_local_et
            ),
            impact=impact,
            source=self.source,
            source_url=source_url,
            source_timezone="America/New_York",
            retrieved_at=retrieved_at,
            validation={
                "status": "accepted",
                "reason_code": None,
                "checks": [
                    "full_release_date",
                    "weekday",
                    "reference_period",
                    "timezone",
                    "source_lineage",
                ],
            },
            reliability=self.reliability,
            incomplete_time=False,
            event_risk_level=impact,
            default_risk_window_before_minutes=(
                30 if has_default_window and time_utc else 0
            ),
            default_risk_window_after_minutes=(
                30 if has_default_window and time_utc else 0
            ),
        )
        return event.model_dump(mode="json")


class _BlsListTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._row: dict[int, str] | None = None
        self._cell: list[str] | None = None
        self._cell_column: int | None = None
        self._cell_rowspan = 1
        self._next_column = 0
        self._rowspans: dict[int, tuple[str, int]] = {}

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.lower() == "tr":
            self._row = {}
            self._next_column = 0
            for column, (value, remaining) in list(
                self._rowspans.items()
            ):
                self._row[column] = value
                if remaining <= 1:
                    del self._rowspans[column]
                else:
                    self._rowspans[column] = (
                        value,
                        remaining - 1,
                    )
        elif tag.lower() in {"td", "th"} and self._row is not None:
            while self._next_column in self._row:
                self._next_column += 1
            self._cell_column = self._next_column
            self._next_column += 1
            raw_rowspan = next(
                (
                    value
                    for key, value in attrs
                    if key.lower() == "rowspan"
                ),
                None,
            )
            try:
                self._cell_rowspan = max(int(raw_rowspan or 1), 1)
            except ValueError:
                self._cell_rowspan = 1
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            value = " ".join(data.split())
            if value:
                self._cell.append(value)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"td", "th"} and self._cell is not None:
            if self._row is not None and self._cell_column is not None:
                value = " ".join(self._cell).strip()
                self._row[self._cell_column] = value
                if self._cell_rowspan > 1:
                    self._rowspans[self._cell_column] = (
                        value,
                        self._cell_rowspan - 1,
                    )
            self._cell = None
            self._cell_column = None
            self._cell_rowspan = 1
        elif lowered == "tr" and self._row is not None:
            row = [
                self._row.get(index, "")
                for index in range(max(self._row, default=-1) + 1)
            ]
            if len(row) >= 3 and _parse_full_date(row[0]):
                self.rows.append(row)
            self._row = None
            self._cell = None

    @classmethod
    def parse(cls, html: str) -> list[list[str]]:
        parser = cls()
        parser.feed(html)
        return parser.rows


def _parse_full_date(value: Any) -> date | None:
    text = " ".join(str(value or "").replace("\xa0", " ").split())
    text = re.sub(r"^[A-Za-z]+,\s*", "", text)
    for fmt in ("%B %d, %Y", "%B %d %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _split_release_and_period(value: str) -> tuple[str, str | None]:
    text = " ".join(value.split())
    match = re.match(r"^(?P<name>.+?)\s+for\s+(?P<period>.+)$", text)
    if not match:
        return text, None
    return match.group("name").strip(), match.group("period").strip()


def _period_frequency(value: str) -> str | None:
    lowered = value.lower()
    if re.search(r"\b(?:january|february|march|april|may|june|july|"
                 r"august|september|october|november|december)\s+\d{4}\b", lowered):
        return "monthly"
    if "quarter" in lowered:
        return "quarterly"
    if "annual" in lowered:
        return "annual"
    return None
