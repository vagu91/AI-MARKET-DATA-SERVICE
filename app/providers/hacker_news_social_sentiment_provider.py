from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.core.text_normalization import normalize_text

KEYWORDS = ("AI", "semiconductors", "Nvidia", "OpenAI", "Fed", "inflation", "rates", "recession")
POSITIVE = {"beat", "growth", "strong", "record", "surge", "optimistic", "accelerate", "breakthrough"}
NEGATIVE = {"risk", "slowdown", "weak", "lawsuit", "ban", "cut", "recession", "inflation", "bubble"}


class HackerNewsSocialSentimentProvider:
    source = "Hacker News Algolia public API"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_social_sentiment:
            return _status("disabled", "social_sentiment_disabled", started, self.settings.hacker_news_algolia_url)
        since = int((started - timedelta(hours=24)).timestamp())
        query = "AI"
        try:
            async with httpx.AsyncClient(timeout=self.settings.social_sentiment_timeout_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(
                        self.settings.hacker_news_algolia_url,
                        params={
                            "query": query,
                            "tags": "story",
                            "numericFilters": f"created_at_i>{since}",
                            "hitsPerPage": self.settings.social_sentiment_max_items,
                        },
                    ),
                    timeout=max(float(self.settings.social_sentiment_timeout_seconds), 1.0),
                )
                response.raise_for_status()
                payload = response.json()
        except TimeoutError:
            return await self._fetch_rss_fallback(started, "algolia_timeout")
        except Exception as exc:
            fallback = await self._fetch_rss_fallback(started, str(exc) or "algolia_failed")
            if fallback.get("status") == "found":
                return fallback
            return _status("provider_failed", str(exc) or "hacker_news_social_sentiment_failed", started, self.settings.hacker_news_algolia_url)
        items = [_normalize_hit(hit) for hit in payload.get("hits") or [] if isinstance(hit, dict)]
        return build_social_sentiment(items, started=started, source_url=self.settings.hacker_news_algolia_url, ttl_minutes=self.settings.social_sentiment_ttl_minutes)

    async def _fetch_rss_fallback(self, started: datetime, reason: str) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=max(float(self.settings.social_sentiment_timeout_seconds), 2.0)) as client:
                response = await client.get(self.settings.hacker_news_rss_url)
                response.raise_for_status()
        except Exception:
            return _status("provider_timeout", "hacker_news_social_sentiment_timeout", started, self.settings.hacker_news_algolia_url)
        items = _parse_hn_rss(response.text)
        result = build_social_sentiment(items, started=started, source_url=self.settings.hacker_news_rss_url, ttl_minutes=self.settings.social_sentiment_ttl_minutes)
        result["provider"] = "hacker_news_rss_social_sentiment"
        warnings = list(result.get("warnings") or [])
        warnings.append(f"algolia_fallback:{reason}")
        result["warnings"] = warnings
        return result


def build_social_sentiment(items: list[dict[str, Any]], *, started: datetime, source_url: str, ttl_minutes: int) -> dict[str, Any]:
    deduped: dict[str, dict[str, Any]] = {}
    spam_filtered = 0
    for item in items:
        key = str(item.get("url") or item.get("object_id") or item.get("title") or "")
        if not key:
            spam_filtered += 1
            continue
        if _looks_spammy(item):
            spam_filtered += 1
            continue
        deduped.setdefault(key, item)
    usable = list(deduped.values())
    scored = [_score_item(item) for item in usable]
    mention_count = sum(len(item["matched_keywords"]) for item in scored)
    bullish = sum(1 for item in scored if item["sentiment_label"] == "bullish")
    bearish = sum(1 for item in scored if item["sentiment_label"] == "bearish")
    neutral = max(0, len(scored) - bullish - bearish)
    total = len(scored) or 1
    score = round(sum(float(item["sentiment_score"]) for item in scored) / total, 4) if scored else None
    now = datetime.now(UTC)
    return {
        "status": "found" if scored else "not_found",
        "provider": "hacker_news_social_sentiment",
        "source": "Hacker News Algolia public API",
        "source_url": source_url,
        "retrieved_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (now + timedelta(minutes=ttl_minutes)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "coverage": {"window_hours": 24, "source_scope": "public_hacker_news_stories", "keywords": list(KEYWORDS)},
        "social_market_sentiment": {
            "sentiment_score": score,
            "bullish_ratio": round(bullish / total, 4),
            "bearish_ratio": round(bearish / total, 4),
            "neutral_ratio": round(neutral / total, 4),
            "discussion_volume": len(scored),
        },
        "social_symbol_sentiment": _symbol_sentiment(scored),
        "source_count": 1 if scored else 0,
        "mention_count": mention_count,
        "unique_authors": len({item.get("author") for item in scored if item.get("author")}) or None,
        "spam_filtered_count": spam_filtered,
        "bot_suspected_count": None,
        "reliability": 0.45 if scored else 0.0,
        "warnings": [] if scored else ["social_sentiment_not_available"],
        "items": scored[:20],
        "diagnostics": {"duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000), "items_seen": len(items)},
        "service_role": "data provider only",
    }


def _normalize_hit(hit: dict[str, Any]) -> dict[str, Any]:
    title = normalize_text(hit.get("title") or hit.get("story_title") or "")
    return {
        "object_id": hit.get("objectID"),
        "title": title,
        "url": hit.get("url") or hit.get("story_url"),
        "author": hit.get("author"),
        "points": hit.get("points") or 0,
        "comment_count": hit.get("num_comments") or 0,
        "created_at": hit.get("created_at"),
    }


def _parse_hn_rss(text: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []
    items = []
    for item in root.findall(".//item"):
        title = normalize_text(item.findtext("title") or "")
        link = item.findtext("link")
        pub_date = item.findtext("pubDate")
        if not title:
            continue
        items.append({"object_id": link or title, "title": title, "url": link, "author": None, "points": 0, "comment_count": 0, "created_at": pub_date})
    return items


def _score_item(item: dict[str, Any]) -> dict[str, Any]:
    text = str(item.get("title") or "")
    words = set(re.findall(r"[A-Za-z][A-Za-z0-9+.-]*", text.lower()))
    pos = len(words & POSITIVE)
    neg = len(words & NEGATIVE)
    score = 0.0 if pos == neg else min(1.0, (pos - neg) / max(pos + neg, 1))
    matched = [keyword for keyword in KEYWORDS if keyword.lower() in text.lower()]
    label = "bullish" if score > 0.15 else "bearish" if score < -0.15 else "neutral"
    return {**item, "matched_keywords": matched, "sentiment_score": round(score, 4), "sentiment_label": label}


def _symbol_sentiment(items: list[dict[str, Any]]) -> dict[str, Any]:
    symbols = ("QQQ", "NDX", "MNQ", "NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "TSLA", "AMD", "AVGO", "MU")
    output: dict[str, Any] = {}
    for symbol in symbols:
        matched = [item for item in items if re.search(rf"\b{re.escape(symbol)}\b", str(item.get("title") or ""), flags=re.I)]
        if not matched:
            continue
        output[symbol] = {
            "mention_count": len(matched),
            "sentiment_score": round(sum(float(item["sentiment_score"]) for item in matched) / len(matched), 4),
            "source_count": 1,
        }
    return output


def _looks_spammy(item: dict[str, Any]) -> bool:
    title = str(item.get("title") or "")
    return len(title) < 8 or title.count("$") > 3


def _status(status: str, reason: str, started: datetime, source_url: str) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "hacker_news_social_sentiment",
        "source": "Hacker News Algolia public API",
        "source_url": source_url,
        "retrieved_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (now + timedelta(minutes=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_count": 0,
        "mention_count": 0,
        "reliability": 0.0,
        "coverage": {"window_hours": 24, "source_scope": "public_hacker_news_stories"},
        "social_market_sentiment": {},
        "social_symbol_sentiment": {},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "diagnostics": {"duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000)},
        "service_role": "data provider only",
    }
