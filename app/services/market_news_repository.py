from __future__ import annotations

from typing import Any

from app.core.text_normalization import normalize_payload_text
from app.core.redaction import redact_payload
from app.core.config import Settings
from app.services.data_freshness_service import DataFreshnessService
from app.services.data_integrity_service import news_content_status
from app.services.fact_key_service import FactKeyService
from app.services.market_fact_repository import connect_market_db, encode, init_market_db, now_iso, decode


class MarketNewsRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.keys = FactKeyService()
        self.freshness = DataFreshnessService(settings)
        init_market_db(settings)

    def upsert_news(self, article: dict[str, Any]) -> dict[str, Any]:
        article = normalize_payload_text(redact_payload(dict(article)))
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
            "is_official": 1 if article.get("is_official") else 0,
            "is_duplicate": 0,
            "raw_payload_json": encode(article),
            "created_at": article.get("created_at") or timestamp,
            "updated_at": timestamp,
        }
        payload["valid_until"] = article.get("valid_until") or self.freshness.news_valid_until(
            published_at=payload["published_at"],
            retrieved_at=payload["retrieved_at"],
            topics=topics,
        )
        payload["next_refresh_at"] = article.get("next_refresh_at") or self.freshness.next_refresh_at(payload["valid_until"])
        columns = list(payload)
        updates = ", ".join(f"{column}=excluded.{column}" for column in columns if column not in {"news_key", "created_at"})
        with connect_market_db(self.settings) as conn:
            conn.execute(
                f"""
                INSERT INTO market_news ({", ".join(columns)}) VALUES ({", ".join("?" for _ in columns)})
                ON CONFLICT(news_key) DO UPDATE SET {updates}
                """,
                [payload[column] for column in columns],
            )
            conn.commit()
        return payload

    def stored(self, *, symbols: list[str] | None = None, days: int = 7, limit: int = 200) -> list[dict[str, Any]]:
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(
                "SELECT * FROM market_news ORDER BY COALESCE(published_at, retrieved_at) DESC LIMIT ?",
                (limit,),
            ).fetchall()
        items = [self._row(row) for row in rows]
        if symbols:
            wanted = {symbol.upper() for symbol in symbols}
            items = [item for item in items if wanted.intersection({symbol.upper() for symbol in item.get("symbols", [])})]
        return items

    def _row(self, row) -> dict[str, Any]:
        data = dict(row)
        data["symbols"] = decode(data.pop("symbols_json", None), [])
        data["topics"] = decode(data.pop("topics_json", None), [])
        data["raw_payload"] = decode(data.pop("raw_payload_json", None), None)
        if isinstance(data["raw_payload"], dict) and data["raw_payload"].get("content_status"):
            data["content_status"] = data["raw_payload"]["content_status"]
        if isinstance(data["raw_payload"], dict):
            for key in (
                "canonical_url",
                "aggregator_url",
                "canonical_status",
                "redirect_chain",
                "summary_source_type",
                "summary_source_url",
                "source_text_available",
                "is_official",
            ):
                if data.get(key) in (None, "") and key in data["raw_payload"]:
                    data[key] = data["raw_payload"][key]
        return data
