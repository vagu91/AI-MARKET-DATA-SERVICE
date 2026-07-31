from __future__ import annotations

from app.core.config import Settings
from app.services.market_fact_repository import MarketFactRepository
from app.services.provider_adapter_factory import create_registered_adapter
from app.services.provider_capability_registry import (
    automatic_ai_delivery_authorized,
)


class AIResearcherService:
    def __init__(self, settings: Settings) -> None:
        self.provider = create_registered_adapter("AI_RESEARCHER", settings)
        self.facts = MarketFactRepository(settings)

    async def research_and_save(self, events: list[dict]) -> tuple[list[dict], dict]:
        if not automatic_ai_delivery_authorized():
            return [], {
                "status": "not_authorized",
                "warning": "AI_RUNTIME_CAPABILITY_NOT_CERTIFIED",
            }
        facts, status = await self.provider.research(events)
        for fact in facts:
            self.facts.upsert_fact(fact)
        return facts, status
