from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.providers.aaii_sentiment_provider import AaiiSentimentProvider
from app.providers.cftc_cot_provider import CftcCotProvider
from app.services.market_fact_repository import MarketFactRepository
from app.services.provider_observation_repository import ProviderObservationRepository


class PositioningRuntimeService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.facts = MarketFactRepository(settings)
        self.observations = ProviderObservationRepository(settings)
        self.cot_provider = CftcCotProvider(settings)
        self.aaii_provider = AaiiSentimentProvider(settings)

    async def cot(self, *, refresh: str = "false", run_id: str | None = None) -> dict[str, Any]:
        cached = [] if refresh == "force" else self.facts.get_valid_facts_by_type("cot_positioning")
        if cached:
            raw = cached[0].get("raw_payload")
            if isinstance(raw, dict):
                return _with_runtime_metadata(
                    raw,
                    cache_status="hit",
                    cache_used=True,
                    provider_calls=0,
                    attempted=False,
                    persisted_count=1,
                    read_back_count=1,
                    materialized_count=1,
                )
        if refresh == "false":
            return _cot_status("not_found", "cot_not_in_db_refresh_false")
        result = await self.cot_provider.fetch_nasdaq()
        self._record("cftc_cot", result, run_id=run_id)
        persisted_count = 0
        read_back_count = 0
        materialized_count = 0
        if result.get("status") == "found":
            self._save("cot:nasdaq_100", "cot_positioning", result, source="CFTC")
            persisted_count = 1
            if self.facts.get_fact("cot:nasdaq_100"):
                read_back_count = 1
                materialized_count = 1
        return _with_runtime_metadata(
            result,
            cache_status="miss",
            cache_used=False,
            provider_calls=1,
            attempted=True,
            persisted_count=persisted_count,
            read_back_count=read_back_count,
            materialized_count=materialized_count,
        )

    async def aaii(self, *, refresh: str = "false", run_id: str | None = None) -> dict[str, Any]:
        cached = [] if refresh == "force" else self.facts.get_valid_facts_by_type("aaii_sentiment")
        if cached:
            raw = cached[0].get("raw_payload")
            if isinstance(raw, dict):
                raw["cache_status"] = "hit"
                return raw
        if refresh == "false":
            return _aaii_status("not_found", "aaii_not_in_db_refresh_false")
        result = await self.aaii_provider.fetch()
        self._record("aaii_sentiment", result, run_id=run_id)
        self._save("sentiment:aaii", "aaii_sentiment", result, source="AAII")
        return {**result, "cache_status": "miss"}

    def _save(self, fact_key: str, fact_type: str, result: dict[str, Any], *, source: str) -> None:
        now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        valid_until = result.get("valid_until") or (datetime.now(UTC) + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.facts.upsert_fact(
            {
                "fact_key": fact_key,
                "fact_type": fact_type,
                "country": "US",
                "symbol": "NQ" if fact_type == "cot_positioning" else None,
                "category": fact_type,
                "event_name": source,
                "source": source,
                "source_url": result.get("source_url"),
                "provider_type": "OFFICIAL_WEB",
                "reliability": result.get("reliability") or 0.0,
                "confidence": result.get("reliability") or 0.0,
                "retrieved_at": result.get("retrieved_at") or now,
                "valid_until": valid_until,
                "next_refresh_at": result.get("next_retry_at") or valid_until,
                "status": "active",
                "raw_payload_json": result,
                "warnings_json": result.get("warnings") or [],
                "errors_json": result.get("errors") or [],
            }
        )

    def _record(self, provider_name: str, result: dict[str, Any], *, run_id: str | None) -> None:
        self.observations.record(
            run_id=run_id,
            provider_name=provider_name,
            provider_type="OFFICIAL_WEB",
            status=result.get("status"),
            country="US",
            symbol="NQ" if provider_name == "cftc_cot" else None,
            category=provider_name,
            url=result.get("source_url"),
            item_count=1 if result.get("status") == "found" else 0,
            error="; ".join(result.get("errors") or []) or None,
            warning="; ".join(result.get("warnings") or []) or None,
            duration_ms=result.get("duration_ms"),
            raw_payload_json=result,
        )


def _cot_status(status: str, reason: str) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "status": status,
        "report_date": None,
        "publication_date": None,
        "market_name": None,
        "cftc_contract_market_code": None,
        "report_type": None,
        "asset_managers": {"long": None, "short": None, "spreading": None, "net": None, "net_change_week": None},
        "leveraged_funds": {"long": None, "short": None, "spreading": None, "net": None, "net_change_week": None},
        "dealers": {"long": None, "short": None, "net": None},
        "open_interest": None,
        "source": "CFTC",
        "source_url": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
        "retrieved_at": now,
        "valid_until": None,
        "reliability": 0.0,
        "attempted_sources": [],
        "reason": reason,
        "next_retry_at": None,
        "warnings": [reason],
        "errors": [],
        "cache_status": "miss",
        "cache_used": False,
        "attempted": False,
        "provider_calls": 0,
        "AI_called": False,
        "persisted_count": 0,
        "read_back_count": 0,
        "materialized_count": 0,
    }


def _with_runtime_metadata(
    payload: dict[str, Any],
    *,
    cache_status: str,
    cache_used: bool,
    provider_calls: int,
    attempted: bool,
    persisted_count: int,
    read_back_count: int,
    materialized_count: int,
) -> dict[str, Any]:
    return {
        **payload,
        "cache_status": cache_status,
        "cache_used": cache_used,
        "attempted": attempted,
        "provider_calls": provider_calls,
        "AI_called": False,
        "persisted_count": persisted_count,
        "read_back_count": read_back_count,
        "materialized_count": materialized_count,
    }


def _aaii_status(status: str, reason: str) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "status": status,
        "survey_date": None,
        "bullish_pct": None,
        "neutral_pct": None,
        "bearish_pct": None,
        "bull_bear_spread": None,
        "source": "AAII",
        "source_url": "https://www.aaii.com/sentimentsurvey",
        "retrieved_at": now,
        "valid_until": None,
        "attempted_sources": [],
        "reason": reason,
        "next_retry_at": None,
        "warnings": [reason],
        "errors": [],
    }
