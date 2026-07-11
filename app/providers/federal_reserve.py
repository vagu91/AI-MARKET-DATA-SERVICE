import hashlib
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, Impact, ProviderResult, ProviderType
from app.models.events import EconomicEvent
from app.providers.base import BaseProvider, metadata


HIGH_IMPACT_KEYWORDS = {
    "cpi",
    "core cpi",
    "ppi",
    "nonfarm payrolls",
    "nfp",
    "unemployment rate",
    "jobless claims",
    "pce",
    "core pce",
    "gdp",
    "ism manufacturing",
    "ism services",
    "retail sales",
    "fomc",
    "fomc minutes",
    "powell",
    "fed chair",
}


class FederalReserveRssProvider(BaseProvider):
    source = "Federal Reserve"
    provider_type = ProviderType.RSS
    reliability = 0.9
    cache_key = "provider:federal_reserve:events:v2"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings
        self.local_tz = ZoneInfo(settings.timezone)

    async def fetch(self) -> ProviderResult:
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            response = await client.get(self.settings.federal_reserve_rss_url)
            response.raise_for_status()
            xml_text = response.text

        root = ET.fromstring(xml_text)
        events: list[dict[str, object]] = []
        latest_as_of: datetime | None = None
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or self.settings.federal_reserve_rss_url).strip()
            published_raw = item.findtext("pubDate")
            if not title or not published_raw:
                continue
            title_lower = title.lower()
            if not any(keyword in title_lower for keyword in HIGH_IMPACT_KEYWORDS):
                continue
            published = parsedate_to_datetime(published_raw)
            if published.tzinfo is None:
                published = published.replace(tzinfo=UTC)
            published_utc = published.astimezone(UTC)
            latest_as_of = max(latest_as_of, published_utc) if latest_as_of else published_utc
            event = EconomicEvent(
                event_id=self._event_id(title, published_utc.isoformat()),
                name=title,
                country="US",
                category=self._category(title),
                date=published_utc.date().isoformat(),
                time_utc=published_utc,
                time_local=published_utc.astimezone(self.local_tz),
                impact=Impact.HIGH,
                source=self.source,
                source_url=link,
                reliability=self.reliability,
                event_risk_level=Impact.HIGH,
                default_risk_window_before_minutes=30,
                default_risk_window_after_minutes=30,
            )
            events.append(event.model_dump(mode="json"))

        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=latest_as_of,
                freshness=Freshness.RECENT,
            ),
            data=events,
        )

    @staticmethod
    def _event_id(name: str, timestamp: str) -> str:
        digest = hashlib.sha256(f"{name}:{timestamp}".encode("utf-8")).hexdigest()[:16]
        return f"fed-{digest}"

    @staticmethod
    def _category(name: str) -> str:
        lower = name.lower()
        if "minutes" in lower:
            return "FOMC Minutes"
        if "fomc" in lower:
            return "FOMC"
        if "powell" in lower or "fed chair" in lower or "speech" in lower:
            return "Fed Speech"
        return "Federal Reserve"
