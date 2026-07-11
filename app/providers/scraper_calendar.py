from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import ProviderResult, ProviderType
from app.providers.base import BaseProvider, ProviderError


class EconomicCalendarScraperProvider(BaseProvider):
    source = "Economic Calendar Scraper Fallback"
    provider_type = ProviderType.SCRAPER
    reliability = 0.45
    cache_key = "provider:scraper_calendar:events:v2"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        if not self.settings.enable_scraper_fallbacks:
            raise ProviderError(
                "Scraper fallbacks are disabled by config "
                "(DailyFX, ForexFactory, Investing)"
            )
        raise ProviderError("Scraper fallback adapters are not implemented in MVP")
