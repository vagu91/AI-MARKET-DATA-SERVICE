from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.providers.hacker_news_social_sentiment_provider import HackerNewsSocialSentimentProvider
from app.services.market_fact_repository import MarketFactRepository
from app.services.provider_observation_repository import ProviderObservationRepository


class SocialSentimentService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.facts = MarketFactRepository(settings)
        self.observations = ProviderObservationRepository(settings)
        self.provider = HackerNewsSocialSentimentProvider(settings)

    async def snapshot(self, *, refresh: str = "auto") -> dict[str, Any]:
        cached = [] if refresh == "force" else self.facts.get_valid_facts_by_type("social_sentiment")
        if cached:
            raw = cached[0].get("raw_payload") if isinstance(cached[0].get("raw_payload"), dict) else {}
            return {**raw, "cache_used": True, "provider_calls": 0}
        if refresh == "false":
            return _empty("not_found", "social_sentiment_not_in_db_refresh_false", provider_calls=0)
        result = await self.provider.fetch()
        self.observations.record(
            provider_name="hacker_news_social_sentiment",
            provider_type="PUBLIC_HTTP",
            status=result.get("status"),
            country="US",
            category="social_sentiment",
            url=result.get("source_url"),
            item_count=int(result.get("mention_count") or 0),
            warning="; ".join(result.get("warnings") or []) or None,
            error="; ".join(result.get("errors") or []) or None,
            raw_payload_json={"status": result.get("status"), "diagnostics": result.get("diagnostics") or {}},
        )
        if result.get("status") in {"found", "partial"}:
            valid_until = result.get("valid_until") or (datetime.now(UTC) + timedelta(minutes=self.settings.social_sentiment_ttl_minutes)).replace(microsecond=0).isoformat()
            self.facts.upsert_fact(
                {
                    "fact_key": "social_sentiment:hacker_news",
                    "fact_type": "social_sentiment",
                    "country": "US",
                    "category": "social_sentiment",
                    "event_name": "Hacker News social sentiment",
                    "source": result.get("source"),
                    "source_url": result.get("source_url"),
                    "provider_type": "PUBLIC_HTTP",
                    "reliability": result.get("reliability") or 0,
                    "confidence": result.get("reliability") or 0,
                    "retrieved_at": result.get("retrieved_at"),
                    "valid_until": valid_until,
                    "next_refresh_at": valid_until,
                    "raw_payload_json": result,
                    "warnings_json": result.get("warnings") or [],
                    "errors_json": result.get("errors") or [],
                }
            )
        return {**result, "cache_used": False, "provider_calls": 1}


def _empty(status: str, warning: str, *, provider_calls: int) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "status": status,
        "retrieved_at": now,
        "source_count": 0,
        "mention_count": 0,
        "social_market_sentiment": {},
        "social_symbol_sentiment": {},
        "warnings": [warning],
        "errors": [],
        "reliability": 0.0,
        "cache_used": False,
        "provider_calls": provider_calls,
        "service_role": "data provider only",
    }
