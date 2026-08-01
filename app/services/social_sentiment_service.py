from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.services.market_fact_repository import MarketFactRepository
from app.services.provider_adapter_factory import create_registered_adapter
from app.services.provider_observation_repository import ProviderObservationRepository


class SocialSentimentService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.facts = MarketFactRepository(settings)
        self.observations = ProviderObservationRepository(settings)
        self.provider = create_registered_adapter("HACKER_NEWS", settings)

    async def snapshot(self, *, refresh: str = "auto") -> dict[str, Any]:
        cached = self.facts.get_valid_facts_by_type("social_sentiment")
        cache_rejection_reason = None
        if cached and not _social_cache_provenance_valid(
            cached[0],
            algolia_url=self.settings.hacker_news_algolia_url,
        ):
            cache_rejection_reason = "SOCIAL_CACHE_PROVENANCE_NOT_CERTIFIED"
            cached = []
        if cached:
            raw = cached[0].get("raw_payload") if isinstance(cached[0].get("raw_payload"), dict) else {}
            return {**raw, "cache_used": True, "provider_calls": 0}
        if refresh == "false":
            return {
                **_empty(
                    "not_found",
                    "social_sentiment_not_in_db_refresh_false",
                    provider_calls=0,
                ),
                "cache_rejection_reason": cache_rejection_reason,
            }
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
        return {
            **result,
            "cache_used": False,
            "provider_calls": 1,
            "cache_rejection_reason": cache_rejection_reason,
        }


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


def _social_cache_provenance_valid(
    row: dict[str, Any],
    *,
    algolia_url: str,
) -> bool:
    """Reject persisted values produced by the uncertified RSS fallback."""

    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        return False
    return bool(
        str(raw.get("provider") or "").strip()
        == "hacker_news_social_sentiment"
        and str(raw.get("source") or "").strip()
        == "Hacker News Algolia public API"
        and str(raw.get("source_url") or "").strip()
        == str(algolia_url).strip()
        and str(row.get("source_url") or "").strip()
        == str(algolia_url).strip()
    )
