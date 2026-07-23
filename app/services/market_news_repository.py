from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.text_normalization import normalize_payload_text
from app.core.redaction import redact_payload
from app.core.config import Settings
from app.services.data_freshness_service import DataFreshnessService, parse_datetime
from app.services.data_integrity_service import news_content_status
from app.services.fact_key_service import FactKeyService
from app.services.market_fact_repository import (
    _record_source_quarantine,
    connect_market_db,
    decode,
    encode,
    init_market_db,
    now_iso,
)
from app.services.news_intelligence_service import normalize_news_article
from app.services.source_policy_service import SourcePolicyService


class MarketNewsRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.keys = FactKeyService()
        self.freshness = DataFreshnessService(settings)
        self.source_policy = SourcePolicyService(settings.source_policy_path)
        init_market_db(settings)

    def upsert_news(self, article: dict[str, Any]) -> dict[str, Any]:
        article = normalize_news_article(normalize_payload_text(redact_payload(dict(article))))
        content_status = news_content_status(article)
        if content_status == "invalid_content":
            article["content_status"] = "invalid_content"
            warnings = list(article.get("warnings") or [])
            if "invalid_content" not in warnings:
                warnings.append("invalid_content")
            article["warnings"] = warnings
        source_url = str(article.get("source_url") or article.get("url") or "")
        if not source_url:
            raise ValueError("news source_url is required")
        timestamp = now_iso()
        published_at = parse_datetime(article.get("published_at"))
        if published_at and published_at > datetime.now(UTC) + timedelta(minutes=5):
            raise ValueError("future_news_timestamp")
        policy = self.source_policy.validate(article, field_semantics="news")
        source_validation = self.source_policy.validate_url(
            source_url,
            allow_test_reserved=self.settings.environment.lower() == "test",
        )
        topics = list(article.get("topics") or [])
        payload = {
            "news_key": article.get("news_key") or self.keys.news_key(title=str(article.get("title") or ""), source_url=source_url),
            "title": article.get("title") or "",
            "summary": article.get("summary"),
            "content_snippet": article.get("content_snippet"),
            "source": article.get("source"),
            "source_url": source_url,
            "published_at": article.get("published_at"),
            "retrieved_at": article.get("retrieved_at") or timestamp,
            "valid_from": article.get("valid_from") or article.get("published_at") or timestamp,
            "symbols_json": encode(article.get("symbols") or []),
            "topics_json": encode(topics),
            "country": article.get("country"),
            "category": article.get("category"),
            "relevance": article.get("relevance"),
            "reliability": article.get("reliability") or 0,
            "confidence": article.get("confidence") or article.get("reliability") or 0,
            "provider_type": article.get("provider_type"),
            "is_official": 1 if article.get("is_official_source") or article.get("is_official") else 0,
            "is_duplicate": 1 if article.get("is_duplicate") else 0,
            "raw_payload_json": encode(article),
            "created_at": article.get("created_at") or timestamp,
            "updated_at": timestamp,
            "canonical_url": article.get("canonical_url") or source_url,
            "aggregator_url": article.get("aggregator_url"),
            "original_publisher": article.get("original_publisher") or article.get("publisher") or article.get("source"),
            "source_tier": article.get("source_tier") or policy.tier,
            "source_classification": article.get("source_classification") or policy.classification,
            "source_audit_status": (
                "ACTIVE" if source_validation.accepted else "QUARANTINED"
            ),
            "source_invalid_reason": source_validation.reason_code,
        }
        if not source_validation.accepted:
            payload["reliability"] = 0.0
            payload["confidence"] = 0.0
            payload["is_official"] = 0
        payload["valid_until"] = article.get("valid_until") or self.freshness.news_valid_until(
            published_at=payload["published_at"],
            retrieved_at=payload["retrieved_at"],
            topics=topics,
        )
        payload["next_refresh_at"] = article.get("next_refresh_at") or self.freshness.next_refresh_at(payload["valid_until"])
        payload["lifecycle_status"] = "CURRENT" if (parse_datetime(payload["valid_until"]) or datetime.min.replace(tzinfo=UTC)) > datetime.now(UTC) else "EXPIRED"
        columns = list(payload)
        updates = ", ".join(
            (
                f"{column}=CASE WHEN market_news.source_audit_status='QUARANTINED' "
                f"THEN market_news.{column} ELSE excluded.{column} END"
                if column in {
                    "source_url",
                    "canonical_url",
                    "reliability",
                    "confidence",
                    "is_official",
                    "source_audit_status",
                    "source_invalid_reason",
                    "raw_payload_json",
                }
                else f"{column}=excluded.{column}"
            )
            for column in columns
            if column not in {"news_key", "created_at"}
        )
        with connect_market_db(self.settings) as conn:
            conn.execute(
                f"""
                INSERT INTO market_news ({", ".join(columns)}) VALUES ({", ".join("?" for _ in columns)})
                ON CONFLICT(news_key) DO UPDATE SET {updates}
                """,
                [payload[column] for column in columns],
            )
            if not source_validation.accepted:
                _record_source_quarantine(
                    conn,
                    entity_table="market_news",
                    entity_key=str(payload["news_key"]),
                    invalid=source_validation,
                    previous_status="ACTIVE",
                    lineage={"canonical_url": payload["canonical_url"]},
                )
            conn.commit()
        return payload

    def stored(
        self,
        *,
        symbols: list[str] | None = None,
        days: int = 7,
        limit: int = 200,
        current_only: bool = False,
    ) -> list[dict[str, Any]]:
        cutoff = (datetime.now(UTC) - timedelta(days=max(days, 1))).replace(microsecond=0).isoformat()
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                """
                SELECT * FROM market_news
                WHERE COALESCE(published_at, retrieved_at) >= ?
                  AND source_audit_status='ACTIVE'
                ORDER BY COALESCE(published_at, retrieved_at) DESC LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
        items = [self._row(row) for row in rows]
        now = datetime.now(UTC)
        for item in items:
            valid_until = parse_datetime(item.get("valid_until"))
            item["lifecycle_status"] = "CURRENT" if valid_until and valid_until > now else "EXPIRED"
            item["historical"] = item["lifecycle_status"] == "EXPIRED"
        if current_only:
            items = [item for item in items if item["lifecycle_status"] == "CURRENT"]
        if symbols:
            wanted = {symbol.upper() for symbol in symbols}
            items = [item for item in items if wanted.intersection({symbol.upper() for symbol in item.get("symbols", [])})]
        return items

    def current(self, *, symbols: list[str] | None = None, days: int = 7, limit: int = 200) -> list[dict[str, Any]]:
        return self.stored(symbols=symbols, days=days, limit=limit, current_only=True)

    def _row(self, row) -> dict[str, Any]:
        data = dict(row)
        data["symbols"] = decode(data.pop("symbols_json", None), [])
        data["topics"] = decode(data.pop("topics_json", None), [])
        data["raw_payload"] = decode(data.pop("raw_payload_json", None), None)
        if isinstance(data["raw_payload"], dict) and data["raw_payload"].get("content_status"):
            data["content_status"] = data["raw_payload"]["content_status"]
        if isinstance(data["raw_payload"], dict):
            for key in (
                "accepted", "article_id", "author", "canonical_url", "aggregator_url", "canonical_status",
                "redirect_chain", "summary_source_type", "summary_source_url", "summary_quality",
                "summary_is_generated", "summary_reliability", "source_text_available", "original_publisher",
                "source_classification", "is_official", "is_official_source", "is_primary_source", "entities",
                "matched_entities", "topic_classifications", "relevance_score", "relevance_reasons",
                "relevance_tier", "exclusion_reason", "duplicate_group_id", "duplicate_of", "syndication_group",
                "independent_source_count", "pipeline_version", "warnings", "content_status",
            ):
                if data.get(key) in (None, "") and key in data["raw_payload"]:
                    data[key] = data["raw_payload"][key]
        return data
