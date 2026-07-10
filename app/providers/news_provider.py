import asyncio
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
import html
import re
import xml.etree.ElementTree as ET

import httpx

from app.core.cache import SQLiteCache
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.nasdaq import Relevance
from app.providers.alpha_vantage import ensure_alpha_payload_ok
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import REQUEST_HEADERS
from app.services.market_news_repository import MarketNewsRepository

TOPIC_KEYWORDS = {
    "Fed": ["federal reserve", "fed ", "fomc", "powell"],
    "inflation": ["inflation", "cpi", "pce", "prices"],
    "jobs": ["jobs", "payrolls", "unemployment", "jobless"],
    "yields": ["yield", "treasury", "rates"],
    "semiconductors": ["semiconductor", "chip", "chips", "nvda", "amd", "avgo"],
    "AI chips": ["ai chip", "artificial intelligence chip", "gpu"],
    "earnings": ["earnings", "revenue", "eps"],
    "antitrust": ["antitrust", "competition"],
    "regulation": ["regulation", "regulator"],
    "China": ["china", "chinese"],
    "export controls": ["export control", "export restrictions"],
    "mega-cap": ["apple", "microsoft", "amazon", "meta", "tesla", "nvidia", "netflix"],
    "macro": ["gdp", "inflation", "payrolls", "federal reserve", "treasury"],
}


class NewsProvider(BaseProvider):
    source = "Market News"
    provider_type = ProviderType.API
    reliability = 0.72
    cache_key = "provider:news_latest:v2"

    def __init__(
        self,
        cache: SQLiteCache,
        settings: Settings,
        market_news_repository: MarketNewsRepository | None = None,
    ) -> None:
        super().__init__(cache)
        self.settings = settings
        self.market_news_repository = market_news_repository

    async def fetch(self) -> ProviderResult:
        symbols = ["NVDA", "AAPL", "MSFT", "QQQ"]
        return await self.fetch_for_symbols(symbols=symbols, limit=20)

    async def fetch_for_symbols(
        self,
        symbols: list[str],
        limit: int,
        recency_days: int = 14,
    ) -> ProviderResult:
        warnings: list[str] = []
        query = " OR ".join(symbols + ["Federal Reserve", "Nasdaq", "QQQ"])
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            if self.settings.alpha_vantage_api_key:
                try:
                    response = await client.get(
                        self.settings.alpha_vantage_base_url,
                        params={
                            "function": "NEWS_SENTIMENT",
                            "tickers": ",".join(symbols),
                            "apikey": self.settings.alpha_vantage_api_key,
                        },
                        headers=REQUEST_HEADERS,
                        timeout=min(float(self.settings.http_timeout_seconds), 3.0),
                    )
                    response.raise_for_status()
                    payload = response.json()
                    ensure_alpha_payload_ok(payload)
                    articles = filter_recent_articles(
                        parse_alpha_vantage_news(payload, symbols, limit),
                        recency_days=recency_days,
                    )
                    if articles:
                        return self._store_and_return(_news_result(
                            source="Alpha Vantage NEWS_SENTIMENT",
                            provider_type=ProviderType.API,
                            reliability=0.74,
                            articles=articles,
                            errors=[],
                            warnings=[],
                            fallback_used=False,
                        ))
                    warnings.append("Alpha Vantage NEWS_SENTIMENT returned no articles")
                except Exception as exc:
                    message = str(exc) or "Alpha Vantage NEWS_SENTIMENT failed"
                    warnings.append(f"Alpha Vantage NEWS_SENTIMENT {_category(message)}: {message}")

            try:
                response = await client.get(
                    self.settings.gdelt_doc_api_url,
                    params={
                        "query": query,
                        "mode": "artlist",
                        "format": "json",
                        "maxrecords": min(limit, 250),
                        "sort": "datedesc",
                    },
                    headers=REQUEST_HEADERS,
                    timeout=min(float(self.settings.http_timeout_seconds), 3.0),
                )
                response.raise_for_status()
                payload = response.json()
                articles = filter_recent_articles(
                    parse_gdelt_articles(payload, symbols, limit),
                    recency_days=recency_days,
                )
                if articles:
                    return self._store_and_return(_news_result(
                        source="GDELT Doc API",
                        provider_type=ProviderType.API,
                        reliability=0.66,
                        articles=articles,
                        errors=[],
                        warnings=warnings,
                        fallback_used=bool(warnings),
                    ))
                warnings.append("GDELT Doc API returned no articles")
            except Exception as exc:
                warnings.append(f"GDELT Doc API {_category(str(exc))}: {exc or 'empty error detail'}")

            rss_articles, rss_warnings = await self._fetch_rss_fallbacks(
                client=client,
                symbols=symbols,
                limit=limit,
                recency_days=recency_days,
            )
            warnings.extend(rss_warnings)
            if rss_articles:
                return self._store_and_return(_news_result(
                    source="RSS fallback",
                    provider_type=ProviderType.RSS,
                    reliability=0.62,
                    articles=rss_articles[:limit],
                    errors=[],
                    warnings=warnings,
                    fallback_used=True,
                ))

        errors = _dedupe_errors(warnings or ["No news provider returned articles"])
        return self._store_and_return(_news_result(
            source=self.source,
            provider_type=self.provider_type,
            reliability=0.0,
            articles=[],
            errors=errors,
            warnings=[],
            fallback_used=bool(errors),
        ))

    def _store_and_return(self, result: ProviderResult) -> ProviderResult:
        if not self.market_news_repository:
            return result
        for article in result.data.get("articles", []) if isinstance(result.data, dict) else []:
            try:
                self.market_news_repository.upsert_news(article)
            except Exception:
                continue
        return result

    async def _fetch_rss_fallbacks(
        self,
        client: httpx.AsyncClient,
        symbols: list[str],
        limit: int,
        recency_days: int,
    ) -> tuple[list[dict[str, object]], list[str]]:
        query = f"{' OR '.join(symbols)} Nasdaq"
        feeds = [
            ("Federal Reserve RSS", self.settings.federal_reserve_rss_url, {}, 0.76),
            ("BLS RSS", self.settings.bls_rss_url, {}, 0.86),
            ("BEA RSS", self.settings.bea_rss_url, {}, 0.86),
            ("Yahoo Finance RSS", self.settings.yahoo_finance_rss_url, {}, 0.58),
            ("MarketWatch RSS", self.settings.marketwatch_rss_url, {}, 0.56),
            (
                "Google News RSS",
                self.settings.google_news_rss_url,
                {"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"},
                0.64,
            ),
        ]
        articles: list[dict[str, object]] = []
        warnings: list[str] = []
        seen: set[str] = set()
        results = await asyncio.gather(
            *[
                _fetch_one_rss_feed(
                    client=client,
                    source=source,
                    url=url,
                    params=params,
                    reliability=reliability,
                    symbols=symbols,
                    limit=limit,
                    recency_days=recency_days,
                    timeout=min(float(self.settings.http_timeout_seconds), 4.0),
                )
                for source, url, params, reliability in feeds
            ]
        )
        for parsed, feed_warnings in results:
            warnings.extend(feed_warnings)
            for article in parsed:
                key = _article_key(article)
                if key in seen:
                    continue
                seen.add(key)
                articles.append(article)
                if len(articles) >= limit:
                    return articles, _dedupe_errors(warnings)
        return articles, _dedupe_errors(warnings)


async def _fetch_one_rss_feed(
    *,
    client: httpx.AsyncClient,
    source: str,
    url: str,
    params: dict[str, str],
    reliability: float,
    symbols: list[str],
    limit: int,
    recency_days: int,
    timeout: float,
) -> tuple[list[dict[str, object]], list[str]]:
    try:
        response = await client.get(
            url,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        parsed, parse_warnings = parse_rss_articles(
            response.text,
            symbols=symbols,
            limit=limit,
            source_name=source,
            reliability=reliability,
        )
        return filter_recent_articles(parsed, recency_days=recency_days), parse_warnings
    except Exception as exc:
        return [], [f"{source} {_category(str(exc))}: {exc or 'empty error detail'}"]


def parse_gdelt_articles(payload: dict, symbols: list[str], limit: int) -> list[dict[str, object]]:
    articles = []
    for item in payload.get("articles", [])[:limit]:
        title = item.get("title") or ""
        url = item.get("url") or ""
        if not title or not url:
            continue
        matched_symbols = [
            symbol for symbol in symbols if symbol.upper() in f"{title} {url}".upper()
        ]
        topics = tag_topics(title)
        articles.append(
            {
                "title": title,
                "source": item.get("sourceCountry") or item.get("domain") or "GDELT",
                "published_at": parse_gdelt_date(item.get("seendate")),
                "url": url,
                "source_url": url,
                "canonical_url": url,
                "canonical_status": "resolved",
                "summary": item.get("summary") or item.get("description"),
                "summary_source_type": "api" if item.get("summary") or item.get("description") else None,
                "source_text_available": bool(item.get("summary") or item.get("description")),
                "symbols": matched_symbols,
                "topics": topics,
                "relevance": relevance(matched_symbols, topics),
                "provider_type": ProviderType.API.value,
                "reliability": 0.66,
            }
        )
    return articles


def parse_alpha_vantage_news(
    payload: dict,
    symbols: list[str],
    limit: int,
) -> list[dict[str, object]]:
    ensure_alpha_payload_ok(payload)
    articles = []
    for item in (payload.get("feed") or [])[:limit]:
        title = item.get("title") or ""
        url = item.get("url") or ""
        if not title or not url:
            continue
        ticker_sentiment = item.get("ticker_sentiment") or []
        matched_symbols = [
            entry.get("ticker", "").upper()
            for entry in ticker_sentiment
            if entry.get("ticker", "").upper() in {symbol.upper() for symbol in symbols}
        ]
        if not matched_symbols:
            matched_symbols = [
                symbol for symbol in symbols if symbol.upper() in f"{title} {url}".upper()
            ]
        topics = sorted(set(tag_topics(title) + _topics_from_av(item.get("topics") or [])))
        articles.append(
            {
                "title": title,
                "source": item.get("source") or "Alpha Vantage",
                "published_at": parse_alpha_time(item.get("time_published")),
                "url": url,
                "source_url": url,
                "canonical_url": url,
                "canonical_status": "resolved",
                "summary": item.get("summary"),
                "summary_source_type": "api" if item.get("summary") else None,
                "source_text_available": bool(item.get("summary")),
                "symbols": matched_symbols,
                "topics": topics,
                "relevance": relevance(matched_symbols, topics),
                "provider_type": ProviderType.API.value,
                "reliability": 0.74,
            }
        )
    return articles


def parse_rss_articles(
    text: str,
    symbols: list[str],
    limit: int,
    source_name: str,
    reliability: float,
) -> tuple[list[dict[str, object]], list[str]]:
    root = ET.fromstring(text)
    articles = []
    warnings = []
    for item in root.findall(".//item")[:limit]:
        title = (item.findtext("title") or "").strip()
        url = (item.findtext("link") or "").strip()
        if not title or not url:
            continue
        description = _rss_text(item, "description")
        content_encoded = _rss_text(item, "{http://purl.org/rss/1.0/modules/content/}encoded")
        summary = _clean_markup(content_encoded or description)
        published_at = parse_rss_date(item.findtext("pubDate"))
        article_reliability = reliability
        if not published_at:
            article_reliability = max(reliability - 0.12, 0.0)
            warnings.append(f"{source_name} article missing published_at: {title[:80]}")
        source = item.findtext("source") or source_name
        is_official = source_name in {"Federal Reserve RSS", "BLS RSS", "BEA RSS"}
        canonical_url = None if "Google News RSS" in source_name else url
        aggregator_url = url if "Google News RSS" in source_name else None
        text_for_tags = f"{title} {url}"
        matched_symbols = [
            symbol for symbol in symbols if symbol.upper() in text_for_tags.upper()
        ]
        topics = tag_topics(text_for_tags)
        articles.append(
            {
                "title": title,
                "source": source,
                "published_at": published_at,
                "url": url,
                "source_url": url,
                "canonical_url": canonical_url,
                "aggregator_url": aggregator_url,
                "canonical_status": "unresolved" if aggregator_url else "resolved",
                "summary": summary,
                "content_snippet": summary,
                "summary_source_type": "content_encoded" if content_encoded else ("rss_description" if description else None),
                "summary_source_url": url,
                "source_text_available": bool(summary),
                "is_official": is_official,
                "symbols": matched_symbols,
                "topics": topics,
                "relevance": relevance(matched_symbols, topics),
                "provider_type": ProviderType.RSS.value,
                "reliability": article_reliability,
            }
        )
    return articles, _dedupe_errors(warnings)


def _rss_text(item: ET.Element, tag: str) -> str | None:
    value = item.findtext(tag)
    if value:
        return value
    for child in item:
        if child.tag.endswith(tag.split("}")[-1]):
            return child.text
    return None


def _clean_markup(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def parse_rss_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def tag_topics(text: str) -> list[str]:
    lowered = text.lower()
    return [
        topic
        for topic, keywords in TOPIC_KEYWORDS.items()
        if any(keyword in lowered for keyword in keywords)
    ]


def relevance(symbols: list[str], topics: list[str]) -> str:
    if symbols and topics:
        return Relevance.HIGH.value
    if symbols or topics:
        return Relevance.MEDIUM.value
    return Relevance.LOW.value


def parse_gdelt_date(value: str | None) -> str | None:
    if not value:
        return None
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC).isoformat()
        except ValueError:
            continue
    return None


def parse_alpha_time(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%S").replace(tzinfo=UTC).isoformat()
    except ValueError:
        return None


def _topics_from_av(topics: list[dict]) -> list[str]:
    raw = " ".join(str(item.get("topic", "")) for item in topics)
    return tag_topics(raw)


def filter_recent_articles(
    articles: list[dict[str, object]],
    recency_days: int,
) -> list[dict[str, object]]:
    cutoff = datetime.now(UTC) - timedelta(days=recency_days)
    filtered = []
    for article in articles:
        value = article.get("published_at")
        if not value:
            filtered.append(article)
            continue
        try:
            published = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            filtered.append(article)
            continue
        if published >= cutoff:
            filtered.append(article)
    return filtered


def _news_result(
    source: str,
    provider_type: ProviderType,
    reliability: float,
    articles: list[dict[str, object]],
    errors: list[str],
    warnings: list[str],
    fallback_used: bool,
) -> ProviderResult:
    errors = _dedupe_errors(errors)
    warnings = _dedupe_errors(warnings)
    has_articles = bool(articles)
    return ProviderResult(
        metadata=metadata(
            source=source,
            provider_type=provider_type,
            reliability=reliability if has_articles else 0.0,
            freshness=Freshness.RECENT if has_articles else Freshness.UNKNOWN,
            is_fallback=fallback_used,
            errors=errors,
        ),
        data={
            "articles": articles,
            "data_quality": {
                "errors": errors,
                "warnings": warnings,
                "fallback_used": fallback_used,
                "final_data_available": has_articles,
                "no_data_found": not has_articles,
                "provider_failed": bool(errors) and not has_articles,
                "rate_limited": any(_is_rate_limited(message) for message in errors + warnings),
            },
        },
    )


def _article_key(article: dict[str, object]) -> str:
    url = str(article.get("url") or "").strip().lower()
    title = str(article.get("title") or "").strip().lower()
    return url or title


def _dedupe_errors(errors: list[str]) -> list[str]:
    deduped = []
    for error in errors:
        if error and error not in deduped:
            deduped.append(error)
    return deduped


def _category(message: str) -> str:
    lowered = message.lower()
    if _is_rate_limited(lowered):
        return "rate_limited"
    if "no articles" in lowered:
        return "no_data_found"
    return "provider_failed"


def _is_rate_limited(message: str) -> bool:
    lowered = message.lower()
    return (
        "rate" in lowered
        or "429" in lowered
        or "too many requests" in lowered
        or "thank you for using alpha vantage" in lowered
        or "25 requests" in lowered
    )
