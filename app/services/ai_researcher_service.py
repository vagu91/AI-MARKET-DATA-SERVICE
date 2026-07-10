from __future__ import annotations

from app.core.config import Settings
from app.providers.ai_researcher_provider import AIResearcherProvider
from app.services.market_fact_repository import MarketFactRepository


class AIResearcherService:
    def __init__(self, settings: Settings) -> None:
        self.provider = AIResearcherProvider(settings)
        self.facts = MarketFactRepository(settings)

    async def research_and_save(self, events: list[dict]) -> tuple[list[dict], dict]:
        facts, status = await self.provider.research(events)
        for fact in facts:
            self.facts.upsert_fact(fact)
        return facts, status
