from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.services.data_freshness_service import DataFreshnessService
from app.services.market_fact_repository import MarketFactRepository
from app.services.news_intelligence_service import build_news_context, news_snapshot_valid_until

logger = logging.getLogger(__name__)

NEWS_SNAPSHOT_KEY = "US:MNQ:news_context_snapshot"
NEWS_SNAPSHOT_TYPE = "news_context_snapshot"


class NewsIntelligenceRuntimeService:
    def __init__(self, settings: Settings, *, facts: MarketFactRepository | None = None) -> None:
        self.settings = settings
        self.facts = facts or MarketFactRepository(settings)
        self.freshness = DataFreshnessService(settings)

    def materialize(
        self,
        news_items: list[dict[str, Any]],
        *,
        refresh_mode: str,
        limit: int = 12,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        cached = None if refresh_mode == "force" else self.facts.get_fact(NEWS_SNAPSHOT_KEY)
        if cached is not None:
            freshness = self.freshness.evaluate(cached, allow_stale=refresh_mode == "false")
            if freshness.usable:
                raw = cached.get("raw_payload") if isinstance(cached.get("raw_payload"), dict) else {}
                context = raw.get("news_context") if isinstance(raw.get("news_context"), dict) else None
                if context is not None:
                    output = _with_runtime(context, refresh_mode=refresh_mode, cache_status=freshness.cache_status)
                    logger.info("news_digest_materialized", extra=_runtime_log(output, cache_status=freshness.cache_status))
                    return output, _runtime_metrics(output, cache_status=freshness.cache_status, persisted=0, read_back=1)

        if refresh_mode == "false":
            context = build_news_context(news_items, limit=limit)
            output = _with_runtime(context, refresh_mode="false", cache_status="legacy_db_materialized")
            logger.info("news_digest_materialized", extra=_runtime_log(output, cache_status="legacy_db_materialized"))
            return output, _runtime_metrics(output, cache_status="legacy_db_materialized", persisted=0, read_back=0)

        context = build_news_context(news_items, limit=limit)
        now = datetime.now(UTC).replace(microsecond=0).isoformat()
        valid_until = news_snapshot_valid_until(context)
        payload = {
            "news_context": context,
            "news_digest": context.get("digest") or {},
            "diagnostics": context.get("diagnostics") or {},
            "quality": context.get("quality") or {},
        }
        self.facts.upsert_fact(
            {
                "fact_key": NEWS_SNAPSHOT_KEY,
                "fact_type": NEWS_SNAPSHOT_TYPE,
                "country": "US",
                "symbol": "MNQ",
                "category": "news_context",
                "event_name": "MNQ News Intelligence Snapshot",
                "source": "market_news DB-derived",
                "provider_type": "DB",
                "reliability": (context.get("digest") or {}).get("reliability") or 0,
                "confidence": (context.get("digest") or {}).get("confidence") or 0,
                "retrieved_at": now,
                "valid_until": valid_until,
                "next_refresh_at": valid_until,
                "status": "active" if context.get("latest") else "no_data_available",
                "raw_payload_json": payload,
                "warnings_json": (context.get("digest") or {}).get("warnings") or [],
                "errors_json": [],
            }
        )
        logger.info("news_digest_persisted", extra=_runtime_log(context, cache_status="refreshed"))
        read_back = self.facts.get_fact(NEWS_SNAPSHOT_KEY)
        raw = read_back.get("raw_payload") if read_back and isinstance(read_back.get("raw_payload"), dict) else {}
        restored = raw.get("news_context") if isinstance(raw.get("news_context"), dict) else context
        logger.info("news_digest_read_back", extra=_runtime_log(restored, cache_status="refreshed"))
        output = _with_runtime(restored, refresh_mode=refresh_mode, cache_status="refreshed")
        logger.info("news_digest_materialized", extra=_runtime_log(output, cache_status="refreshed"))
        return output, _runtime_metrics(output, cache_status="refreshed", persisted=1, read_back=1 if read_back else 0)


def _with_runtime(context: dict[str, Any], *, refresh_mode: str, cache_status: str) -> dict[str, Any]:
    output = dict(context)
    if cache_status in {"expired", "stale", "stale_acceptable"} and output.get("latest"):
        output["last_known_good_used"] = True
        output["historical_articles"] = list(output.get("latest") or [])
        output["latest"] = []
        output["articles"] = []
        output["current_drivers"] = []
        output["usable_for_analysis"] = False
        output["status"] = "STALE_LAST_KNOWN_GOOD"
        output["freshness"] = "STALE"
        warnings = list(output.get("warnings") or [])
        if "expired_news_snapshot_not_current" not in warnings:
            warnings.append("expired_news_snapshot_not_current")
        output["warnings"] = warnings
    metadata = dict(output.get("metadata") or {})
    metadata.update(
        {
            "refresh_mode": refresh_mode,
            "cache_status": cache_status,
            "provider_calls": 0 if refresh_mode == "false" else None,
            "browser_calls": 0 if refresh_mode == "false" else None,
            "AI_called": False,
        }
    )
    output["metadata"] = metadata
    return output


def _runtime_metrics(context: dict[str, Any], *, cache_status: str, persisted: int, read_back: int) -> dict[str, Any]:
    diagnostics = dict(context.get("diagnostics") or {})
    return {
        **diagnostics,
        "cache_status": cache_status,
        "cache_used": cache_status in {"hit", "expired", "legacy_db_materialized"},
        "persisted_count": persisted,
        "read_back_count": read_back,
        "materialized_count": len(context.get("latest") or []),
        "AI_called": False,
    }


def _runtime_log(context: dict[str, Any], *, cache_status: str) -> dict[str, Any]:
    diagnostics = context.get("diagnostics") or {}
    return {
        "cache_status": cache_status,
        "accepted_count": diagnostics.get("accepted_count"),
        "excluded_count": diagnostics.get("excluded_count"),
        "cluster_count": diagnostics.get("cluster_count"),
        "official_source_count": diagnostics.get("official_source_count"),
        "high_reliability_source_count": diagnostics.get("high_reliability_source_count"),
    }
