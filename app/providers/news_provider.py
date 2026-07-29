import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
import hashlib
import html
import json
import re
from typing import Any
import xml.etree.ElementTree as ET

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.nasdaq import Relevance
from app.providers.alpha_vantage import ensure_alpha_payload_ok
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import REQUEST_HEADERS
from app.services.market_news_repository import MarketNewsRepository
from app.services.news_intelligence_service import (
    extract_page_metadata,
    normalize_news_article,
)

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
        cache: ProviderCacheProtocol,
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
        requested_limit = max(int(limit), 1)
        query = " OR ".join(symbols + ["Federal Reserve", "Nasdaq", "QQQ"])
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            tasks: list[Any] = []
            if self.settings.alpha_vantage_api_key:
                tasks.append(
                    self._fetch_alpha_vantage(
                        client=client,
                        symbols=symbols,
                        limit=min(
                            requested_limit,
                            self.settings.news_alpha_vantage_limit,
                        ),
                        recency_days=recency_days,
                    )
                )
            if self.settings.news_gdelt_enabled:
                tasks.append(
                    self._fetch_gdelt(
                        client=client,
                        symbols=symbols,
                        query=query,
                        limit=min(requested_limit, self.settings.news_gdelt_limit),
                        recency_days=recency_days,
                    )
                )
            if self.settings.news_rss_enabled:
                tasks.extend(
                    self._rss_tasks(
                        client=client,
                        symbols=symbols,
                        limit=min(
                            requested_limit,
                            self.settings.news_rss_limit_per_feed,
                        ),
                        recency_days=recency_days,
                    )
                )
            batches = list(await asyncio.gather(*tasks)) if tasks else []
            articles = [
                article
                for batch in batches
                for article in batch.get("_articles", [])
            ]
            articles = await self._enrich_missing_metadata(
                client,
                articles,
                batches=batches,
            )
            articles, post_enrichment_outside = _partition_recency(
                articles,
                recency_days=recency_days,
            )
            for exclusion in post_enrichment_outside:
                batch = next(
                    (
                        item
                        for item in batches
                        if item.get("provider") == exclusion.get("provider")
                    ),
                    None,
                )
                if batch is None:
                    continue
                existing_ids = {
                    item.get("record_id")
                    for item in batch.get("explicit_out_of_scope") or []
                }
                if exclusion.get("record_id") not in existing_ids:
                    batch.setdefault("explicit_out_of_scope", []).append(
                        exclusion
                    )

        provider_errors = [
            message
            for batch in batches
            for message in batch.get("errors", [])
        ]
        provider_warnings = [
            message
            for batch in batches
            for message in batch.get("warnings", [])
        ]
        successful_reliability = [
            float(batch.get("reliability") or 0.0)
            for batch in batches
            if batch.get("status") in {"COMPLETE", "PARTIAL"}
        ]
        result = _news_result(
            source="News provider fan-in",
            provider_type=ProviderType.MIXED,
            reliability=max(successful_reliability, default=0.0),
            articles=articles,
            errors=provider_errors if not articles else [],
            warnings=provider_warnings + provider_errors,
            fallback_used=False,
        )
        if isinstance(result.data, dict):
            result.data["provider_accounting"] = [
                {key: value for key, value in batch.items() if key != "_articles"}
                for batch in batches
            ]
            quality = dict(result.data.get("data_quality") or {})
            quality.update(
                {
                    "fan_in": True,
                    "global_limit_applied": False,
                    "requested_limit_semantics": "PER_PROVIDER",
                    "provider_calls": sum(
                        int(batch.get("calls") or 0) for batch in batches
                    ),
                    "raw_acquired_count": sum(
                        len(batch.get("raw_record_ids") or [])
                        for batch in batches
                    ),
                    "provider_success_count": sum(
                        batch.get("status") in {"COMPLETE", "PARTIAL"}
                        for batch in batches
                    ),
                    "provider_failure_count": sum(
                        batch.get("status") == "FAILED" for batch in batches
                    ),
                }
            )
            result.data["data_quality"] = quality
        return self._store_and_return(result)

    def _store_and_return(self, result: ProviderResult) -> ProviderResult:
        if not isinstance(result.data, dict):
            return result

        normalized = [
            normalize_news_article(article)
            for article in result.data.get("articles", [])
        ]
        accounts = list(result.data.get("provider_accounting") or [])
        account_by_provider = {
            str(account.get("provider")): account for account in accounts
        }
        quality = dict(result.data.get("data_quality") or {})
        persistence_results: list[dict[str, Any]] = []
        deliverable: list[dict[str, Any]] = []
        seen_technical: dict[str, dict[str, Any]] = {}

        for article in normalized:
            provider = str(
                article.get("acquisition_provider")
                or article.get("provider")
                or article.get("provider_type")
                or "UNKNOWN"
            )
            account = account_by_provider.get(provider)
            technical_id = str(article.get("technical_acquisition_id") or "")
            raw_record_id = str(article.get("raw_record_id") or technical_id)
            if not bool(article.get("accepted")):
                rejection = {
                    "record_id": raw_record_id,
                    "technical_acquisition_id": technical_id,
                    "reason_code": str(
                        article.get("exclusion_reason")
                        or "NORMALIZATION_TECHNICAL_REJECTION"
                    ).upper(),
                    "retryable": False,
                    "lineage": article.get("raw_source_identity"),
                }
                if account is not None:
                    account.setdefault("technical_rejections", []).append(
                        rejection
                    )
                persistence_results.append(
                    {
                        **rejection,
                        "provider": provider,
                        "result": "TECHNICALLY_REJECTED",
                    }
                )
                continue
            if technical_id in seen_technical:
                rejection = {
                    "record_id": raw_record_id,
                    "technical_acquisition_id": technical_id,
                    "reason_code": "EXACT_TECHNICAL_DUPLICATE",
                    "retryable": False,
                    "duplicate_of": seen_technical[technical_id].get(
                        "raw_record_id"
                    ),
                    "lineage": article.get("raw_source_identity"),
                }
                if account is not None:
                    account.setdefault("technical_rejections", []).append(
                        rejection
                    )
                persistence_results.append(
                    {
                        **rejection,
                        "provider": provider,
                        "result": "TECHNICALLY_REJECTED",
                    }
                )
                continue
            seen_technical[technical_id] = article

            if not self.market_news_repository:
                persistence_results.append(
                    {
                        "record_id": raw_record_id,
                        "technical_acquisition_id": technical_id,
                        "provider": provider,
                        "result": "NOT_ATTEMPTED",
                        "reason_code": "PERSISTENCE_REPOSITORY_NOT_CONFIGURED",
                        "retryable": False,
                    }
                )
                continue
            try:
                stored = self.market_news_repository.upsert_news(article)
            except Exception as exc:
                reason_code, retryable = _persistence_error(exc)
                rejection = {
                    "record_id": raw_record_id,
                    "technical_acquisition_id": technical_id,
                    "reason_code": reason_code,
                    "error_type": type(exc).__name__,
                    "error": str(exc) or type(exc).__name__,
                    "retryable": retryable,
                }
                if account is not None:
                    account.setdefault("technical_rejections", []).append(
                        rejection
                    )
                persistence_results.append(
                    {
                        **rejection,
                        "provider": provider,
                        "result": "PERSISTENCE_REJECTED",
                    }
                )
                continue

            article["persistence"] = {
                "result": "PERSISTED",
                "record_id": raw_record_id,
                "news_key": stored.get("news_key"),
                "retryable": False,
                "reason_code": None,
            }
            deliverable.append(article)
            if account is not None:
                account.setdefault("persisted_record_ids", []).append(
                    raw_record_id
                )
            persistence_results.append(
                {
                    "record_id": raw_record_id,
                    "technical_acquisition_id": technical_id,
                    "provider": provider,
                    "result": "PERSISTED",
                    "news_key": stored.get("news_key"),
                    "reason_code": None,
                    "retryable": False,
                }
            )

        accounting_valid = True
        for account in accounts:
            raw_ids = set(account.get("raw_record_ids") or [])
            persisted_ids = set(account.get("persisted_record_ids") or [])
            rejected_ids = {
                str(item.get("record_id"))
                for item in account.get("technical_rejections") or []
                if item.get("record_id")
            }
            outside_ids = {
                str(item.get("record_id"))
                for item in account.get("explicit_out_of_scope") or []
                if item.get("record_id")
            }
            partitions = (persisted_ids, rejected_ids, outside_ids)
            disjoint = all(
                not left.intersection(right)
                for index, left in enumerate(partitions)
                for right in partitions[index + 1 :]
            )
            accounted_ids = set().union(*partitions)
            account["persisted_count"] = len(persisted_ids)
            account["technically_rejected_count"] = len(rejected_ids)
            account["explicit_out_of_scope_count"] = len(outside_ids)
            account["unaccounted_record_ids"] = sorted(raw_ids - accounted_ids)
            account["unexpected_accounted_record_ids"] = sorted(
                accounted_ids - raw_ids
            )
            account["accounting_disjoint"] = disjoint
            provider_persistence_results = [
                item
                for item in persistence_results
                if item.get("provider") == account.get("provider")
            ]
            provider_persistence_failed = sum(
                item.get("result") == "PERSISTENCE_REJECTED"
                for item in provider_persistence_results
            )
            provider_not_attempted = sum(
                item.get("result") == "NOT_ATTEMPTED"
                for item in provider_persistence_results
            )
            account["persistence_status"] = (
                "NOT_CONFIGURED"
                if provider_not_attempted
                else "PARTIAL"
                if provider_persistence_failed and persisted_ids
                else "FAILED"
                if provider_persistence_failed
                else "COMPLETE"
            )
            account["accounting_valid"] = (
                disjoint
                and not account["unaccounted_record_ids"]
                and not account["unexpected_accounted_record_ids"]
            )
            accounting_valid = accounting_valid and bool(
                account["accounting_valid"]
            )

        attempted = sum(
            item["result"] in {"PERSISTED", "PERSISTENCE_REJECTED"}
            for item in persistence_results
        )
        failed = sum(
            item["result"] == "PERSISTENCE_REJECTED"
            for item in persistence_results
        )
        duplicates = sum(
            item.get("reason_code") == "EXACT_TECHNICAL_DUPLICATE"
            for item in persistence_results
        )
        if not self.market_news_repository:
            persistence_status = "NOT_CONFIGURED"
        elif failed and deliverable:
            persistence_status = "PARTIAL"
        elif failed:
            persistence_status = "FAILED"
        else:
            persistence_status = "COMPLETE"

        provider_degraded = any(
            account.get("status") in {"FAILED", "PARTIAL"} for account in accounts
        )
        readiness = (
            "UNAVAILABLE"
            if persistence_status in {"NOT_CONFIGURED", "FAILED"}
            else "DEGRADED"
            if persistence_status == "PARTIAL"
            or provider_degraded
            or not accounting_valid
            else "AVAILABLE"
            if deliverable
            else "NO_DATA"
        )
        result.data["acquired_articles"] = normalized
        result.data["articles"] = deliverable
        result.data["provider_accounting"] = accounts
        quality.update(
            {
                "raw_article_count": len(normalized),
                "accepted_count": len(deliverable),
                "excluded_count": len(normalized) - len(deliverable),
                "exclusion_breakdown": dict(
                    Counter(
                        str(item.get("reason_code"))
                        for item in persistence_results
                        if item.get("reason_code")
                    )
                ),
                "persistence_status": persistence_status,
                "persistence_attempted_count": attempted,
                "persisted_count": len(deliverable),
                "persistence_failed_count": failed,
                "exact_technical_duplicate_count": duplicates,
                "persistence_results": persistence_results,
                "provider_accounting_valid": accounting_valid,
                "provider_accounting": accounts,
                "readiness": readiness,
                "final_data_available": bool(deliverable),
                "no_data_found": not deliverable and not normalized,
                "provider_failed": readiness == "UNAVAILABLE",
            }
        )
        result.data["data_quality"] = quality
        if failed:
            result.metadata.errors = _dedupe_errors(
                [
                    *result.metadata.errors,
                    f"News persistence {persistence_status.lower()}: "
                    f"{failed} record(s) rejected",
                ]
            )
        return result

    async def _fetch_alpha_vantage(
        self,
        *,
        client: httpx.AsyncClient,
        symbols: list[str],
        limit: int,
        recency_days: int,
    ) -> dict[str, Any]:
        provider = "Alpha Vantage NEWS_SENTIMENT"
        try:
            response = await client.get(
                self.settings.alpha_vantage_base_url,
                params={
                    "function": "NEWS_SENTIMENT",
                    "tickers": ",".join(symbols),
                    "apikey": self.settings.alpha_vantage_api_key,
                    "limit": limit,
                },
                headers=REQUEST_HEADERS,
                timeout=min(float(self.settings.http_timeout_seconds), 3.0),
            )
            response.raise_for_status()
            payload = response.json()
            ensure_alpha_payload_ok(payload)
            articles, rejected, outside, raw_ids = (
                parse_alpha_vantage_news_with_accounting(
                    payload,
                    symbols,
                    limit,
                    provider=provider,
                )
            )
            articles, recency_outside = _partition_recency(
                articles,
                recency_days=recency_days,
            )
            return _provider_batch(
                provider=provider,
                provider_type=ProviderType.API,
                reliability=0.74,
                limit=limit,
                raw_record_ids=raw_ids,
                articles=articles,
                technical_rejections=rejected,
                explicit_out_of_scope=[*outside, *recency_outside],
            )
        except Exception as exc:
            return _failed_provider_batch(
                provider=provider,
                provider_type=ProviderType.API,
                reliability=0.74,
                limit=limit,
                error=exc,
            )

    async def _fetch_gdelt(
        self,
        *,
        client: httpx.AsyncClient,
        symbols: list[str],
        query: str,
        limit: int,
        recency_days: int,
    ) -> dict[str, Any]:
        provider = "GDELT Doc API"
        try:
            response = await client.get(
                self.settings.gdelt_doc_api_url,
                params={
                    "query": query,
                    "mode": "artlist",
                    "format": "json",
                    "maxrecords": limit,
                    "sort": "datedesc",
                },
                headers=REQUEST_HEADERS,
                timeout=min(float(self.settings.http_timeout_seconds), 3.0),
            )
            response.raise_for_status()
            payload = response.json()
            articles, rejected, outside, raw_ids = (
                parse_gdelt_articles_with_accounting(
                    payload,
                    symbols,
                    limit,
                    provider=provider,
                )
            )
            articles, recency_outside = _partition_recency(
                articles,
                recency_days=recency_days,
            )
            return _provider_batch(
                provider=provider,
                provider_type=ProviderType.API,
                reliability=0.66,
                limit=limit,
                raw_record_ids=raw_ids,
                articles=articles,
                technical_rejections=rejected,
                explicit_out_of_scope=[*outside, *recency_outside],
            )
        except Exception as exc:
            return _failed_provider_batch(
                provider=provider,
                provider_type=ProviderType.API,
                reliability=0.66,
                limit=limit,
                error=exc,
            )

    def _rss_tasks(
        self,
        *,
        client: httpx.AsyncClient,
        symbols: list[str],
        limit: int,
        recency_days: int,
    ) -> list[Any]:
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
        return [
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
            if url
        ]

    async def _enrich_missing_metadata(
        self,
        client: httpx.AsyncClient,
        articles: list[dict[str, object]],
        *,
        batches: list[dict[str, Any]],
    ) -> list[dict[str, object]]:
        candidates: list[dict[str, object]] = []
        per_provider_count: Counter[str] = Counter()
        per_provider_limit = (
            self.settings.news_metadata_enrichment_limit_per_provider
        )
        for article in articles:
            provider = str(
                article.get("acquisition_provider")
                or article.get("provider")
                or article.get("provider_type")
                or "UNKNOWN"
            )
            eligible = (
                (not article.get("published_at") or not article.get("summary"))
                and article.get("source_url")
                and not any(
                    host in str(article.get("source_url") or "")
                    for host in ("news.google.com",)
                )
            )
            if not eligible or per_provider_count[provider] >= per_provider_limit:
                continue
            per_provider_count[provider] += 1
            candidates.append(article)

        async def enrich(article: dict[str, object]) -> None:
            provider = str(
                article.get("acquisition_provider")
                or article.get("provider")
                or article.get("provider_type")
                or "UNKNOWN"
            )
            batch = next(
                (
                    item
                    for item in batches
                    if str(item.get("provider")) == provider
                ),
                None,
            )
            record_id = str(
                article.get("raw_record_id")
                or article.get("provider_record_id")
                or ""
            )
            if batch is not None:
                batch["calls"] = int(batch.get("calls") or 0) + 1
                batch["metadata_enrichment_calls"] = (
                    int(batch.get("metadata_enrichment_calls") or 0) + 1
                )
            try:
                response = await client.get(
                    str(article.get("source_url")),
                    headers=REQUEST_HEADERS,
                    follow_redirects=True,
                    timeout=min(float(self.settings.http_timeout_seconds), 2.5),
                )
                response.raise_for_status()
                metadata = extract_page_metadata(response.text, page_url=str(response.url))
            except Exception as exc:
                if batch is not None:
                    error_message = _redact_provider_error(
                        str(exc) or type(exc).__name__
                    )
                    batch["status"] = "PARTIAL"
                    batch["coverage_status"] = "PARTIAL"
                    batch["metadata_enrichment_status"] = "PARTIAL"
                    batch.setdefault("warnings", []).append(
                        f"{provider} metadata_enrichment_failed: {error_message}"
                    )
                    batch.setdefault("metadata_enrichment_results", []).append(
                        {
                            "record_id": record_id,
                            "status": "FAILED",
                            "reason_code": "METADATA_ENRICHMENT_FAILED",
                            "error_type": type(exc).__name__,
                            "error": error_message,
                        }
                    )
                return
            for key in ("published_at", "published_at_source", "summary", "summary_source_type", "summary_source_url", "author", "canonical_url"):
                if not article.get(key) and metadata.get(key):
                    article[key] = metadata[key]
            if article.get("canonical_url"):
                article["canonical_status"] = "resolved"
                article["canonical_url_status"] = "RESOLVED"
            if article.get("summary"):
                article["source_text_available"] = True
            if batch is not None:
                batch.setdefault("metadata_enrichment_results", []).append(
                    {
                        "record_id": record_id,
                        "status": "COMPLETE",
                        "reason_code": None,
                    }
                )
                if batch.get("metadata_enrichment_status") != "PARTIAL":
                    batch["metadata_enrichment_status"] = "COMPLETE"

        await asyncio.gather(*(enrich(article) for article in candidates))
        return articles


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
) -> dict[str, Any]:
    try:
        response = await client.get(
            url,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        parsed, parse_warnings, rejected, outside, raw_ids = (
            parse_rss_articles_with_accounting(
            response.text,
            symbols=symbols,
            limit=limit,
            source_name=source,
            reliability=reliability,
            )
        )
        parsed, recency_outside = _partition_recency(
            parsed,
            recency_days=recency_days,
        )
        return _provider_batch(
            provider=source,
            provider_type=ProviderType.RSS,
            reliability=reliability,
            limit=limit,
            raw_record_ids=raw_ids,
            articles=parsed,
            technical_rejections=rejected,
            explicit_out_of_scope=[*outside, *recency_outside],
            warnings=parse_warnings,
        )
    except Exception as exc:
        return _failed_provider_batch(
            provider=source,
            provider_type=ProviderType.RSS,
            reliability=reliability,
            limit=limit,
            error=exc,
        )


def parse_gdelt_articles(payload: dict, symbols: list[str], limit: int) -> list[dict[str, object]]:
    articles, _, _, _ = parse_gdelt_articles_with_accounting(
        payload,
        symbols,
        limit,
    )
    return articles


def parse_gdelt_articles_with_accounting(
    payload: dict,
    symbols: list[str],
    limit: int,
    *,
    provider: str = "GDELT Doc API",
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[str],
]:
    articles: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []
    outside: list[dict[str, object]] = []
    items = list(payload.get("articles") or [])
    raw_ids = [
        _raw_record_id(provider, index, item)
        for index, item in enumerate(items)
    ]
    for index, item in enumerate(items):
        raw_record_id = raw_ids[index]
        if index >= limit:
            outside.append(
                _out_of_scope(
                    raw_record_id,
                    "PER_PROVIDER_LIMIT",
                    provider=provider,
                )
            )
            continue
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip() or None
        summary = item.get("summary") or item.get("description")
        provider_seen_at = parse_gdelt_date(item.get("seendate"))
        provider_record_id, identity_status = _source_record_identity(
            provider=provider,
            native_value=(
                item.get("id")
                or item.get("documentIdentifier")
                or item.get("record_id")
            ),
            url=url,
            timestamp=provider_seen_at,
            title=title,
            content=summary,
        )
        rejection = _identity_rejection(
            raw_record_id=raw_record_id,
            provider=provider,
            title=title,
            content=summary,
            provider_record_id=provider_record_id,
        )
        if rejection:
            rejected.append(rejection)
            continue
        matched_symbols = [
            symbol
            for symbol in symbols
            if symbol.upper() in f"{title} {url or ''}".upper()
        ]
        topics = tag_topics(title)
        articles.append(
            {
                "raw_record_id": raw_record_id,
                "provider_record_id": provider_record_id,
                "source_identity_status": identity_status,
                "title": title,
                "source": item.get("sourceCountry") or item.get("domain") or "GDELT",
                "original_publisher": item.get("domain")
                or item.get("sourceCountry")
                or None,
                "published_at": None,
                "provider_seen_at": provider_seen_at,
                "editorial_updated_at": parse_gdelt_date(
                    item.get("updated_at") or item.get("updated")
                ),
                "url": url,
                "source_url": url,
                "canonical_url": url,
                "canonical_status": "resolved" if url else "unavailable",
                "canonical_url_status": (
                    "RESOLVED"
                    if url
                    else "ABSENT_SOURCE_IDENTITY_SUFFICIENT"
                ),
                "summary": summary,
                "summary_source_type": "api" if summary else None,
                "source_text_available": bool(summary),
                "symbols": matched_symbols,
                "topics": topics,
                "relevance": relevance(matched_symbols, topics),
                "provider_type": ProviderType.API.value,
                "provider": provider,
                "acquisition_provider": provider,
                "distribution_source": None,
                "reliability": 0.66,
            }
        )
    return articles, rejected, outside, raw_ids


def parse_alpha_vantage_news(
    payload: dict,
    symbols: list[str],
    limit: int,
) -> list[dict[str, object]]:
    articles, _, _, _ = parse_alpha_vantage_news_with_accounting(
        payload,
        symbols,
        limit,
    )
    return articles


def parse_alpha_vantage_news_with_accounting(
    payload: dict,
    symbols: list[str],
    limit: int,
    *,
    provider: str = "Alpha Vantage NEWS_SENTIMENT",
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[str],
]:
    ensure_alpha_payload_ok(payload)
    articles: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []
    outside: list[dict[str, object]] = []
    items = list(payload.get("feed") or [])
    raw_ids = [
        _raw_record_id(provider, index, item)
        for index, item in enumerate(items)
    ]
    for index, item in enumerate(items):
        raw_record_id = raw_ids[index]
        if index >= limit:
            outside.append(
                _out_of_scope(
                    raw_record_id,
                    "PER_PROVIDER_LIMIT",
                    provider=provider,
                )
            )
            continue
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip() or None
        summary = item.get("summary")
        published_at = parse_alpha_time(item.get("time_published"))
        provider_record_id, identity_status = _source_record_identity(
            provider=provider,
            native_value=item.get("id") or item.get("guid"),
            url=url,
            timestamp=published_at,
            title=title,
            content=summary,
        )
        rejection = _identity_rejection(
            raw_record_id=raw_record_id,
            provider=provider,
            title=title,
            content=summary,
            provider_record_id=provider_record_id,
        )
        if rejection:
            rejected.append(rejection)
            continue
        ticker_sentiment = item.get("ticker_sentiment") or []
        matched_symbols = [
            entry.get("ticker", "").upper()
            for entry in ticker_sentiment
            if entry.get("ticker", "").upper() in {symbol.upper() for symbol in symbols}
        ]
        if not matched_symbols:
            matched_symbols = [
                symbol
                for symbol in symbols
                if symbol.upper() in f"{title} {url or ''}".upper()
            ]
        topics = sorted(set(tag_topics(title) + _topics_from_av(item.get("topics") or [])))
        articles.append(
            {
                "raw_record_id": raw_record_id,
                "provider_record_id": provider_record_id,
                "source_identity_status": identity_status,
                "title": title,
                "source": item.get("source") or "Alpha Vantage",
                "original_publisher": item.get("source") or None,
                "published_at": published_at,
                "editorial_updated_at": parse_alpha_time(
                    item.get("time_updated")
                ),
                "url": url,
                "source_url": url,
                "canonical_url": url,
                "canonical_status": "resolved" if url else "unavailable",
                "canonical_url_status": (
                    "RESOLVED"
                    if url
                    else "ABSENT_SOURCE_IDENTITY_SUFFICIENT"
                ),
                "summary": summary,
                "summary_source_type": "api" if summary else None,
                "source_text_available": bool(summary),
                "symbols": matched_symbols,
                "topics": topics,
                "relevance": relevance(matched_symbols, topics),
                "provider_type": ProviderType.API.value,
                "provider": provider,
                "acquisition_provider": provider,
                "distribution_source": "Alpha Vantage",
                "reliability": 0.74,
            }
        )
    return articles, rejected, outside, raw_ids


def parse_rss_articles(
    text: str,
    symbols: list[str],
    limit: int,
    source_name: str,
    reliability: float,
) -> tuple[list[dict[str, object]], list[str]]:
    articles, warnings, _, _, _ = parse_rss_articles_with_accounting(
        text,
        symbols=symbols,
        limit=limit,
        source_name=source_name,
        reliability=reliability,
    )
    return articles, warnings


def parse_rss_articles_with_accounting(
    text: str,
    symbols: list[str],
    limit: int,
    source_name: str,
    reliability: float,
) -> tuple[
    list[dict[str, object]],
    list[str],
    list[dict[str, object]],
    list[dict[str, object]],
    list[str],
]:
    root = ET.fromstring(text)
    articles: list[dict[str, object]] = []
    warnings: list[str] = []
    rejected: list[dict[str, object]] = []
    outside: list[dict[str, object]] = []
    rss_items = list(root.findall(".//item"))
    atom_items = [node for node in root.iter() if node.tag.split("}")[-1] == "entry"]
    items = [*rss_items, *atom_items]
    raw_ids = [
        _raw_record_id(
            source_name,
            index,
            ET.tostring(item, encoding="unicode"),
        )
        for index, item in enumerate(items)
    ]
    for index, item in enumerate(items):
        raw_record_id = raw_ids[index]
        if index >= limit:
            outside.append(
                _out_of_scope(
                    raw_record_id,
                    "PER_PROVIDER_LIMIT",
                    provider=source_name,
                )
            )
            continue
        is_atom = item.tag.split("}")[-1] == "entry"
        title = (_node_text(item, "title") or "").strip()
        url = (_node_link(item) or "").strip() or None
        description = _rss_text(item, "description") or _rss_text(item, "summary")
        content_encoded = _rss_text(item, "{http://purl.org/rss/1.0/modules/content/}encoded")
        atom_content = _rss_text(item, "content") if is_atom else None
        summary = _clean_markup(description or content_encoded or atom_content)
        pub_date = _node_text(item, "pubDate")
        atom_published = _node_text(item, "published")
        atom_updated = _node_text(item, "updated")
        published_at = (
            parse_rss_date(pub_date)
            or parse_atom_date(atom_published)
            or parse_atom_date(atom_updated)
        )
        editorial_updated_at = parse_atom_date(atom_updated)
        guid = (_node_text(item, "guid") or _node_text(item, "id") or "").strip()
        provider_record_id, identity_status = _source_record_identity(
            provider=source_name,
            native_value=guid,
            url=url,
            timestamp=published_at or editorial_updated_at,
            title=title,
            content=summary,
        )
        rejection = _identity_rejection(
            raw_record_id=raw_record_id,
            provider=source_name,
            title=title,
            content=summary,
            provider_record_id=provider_record_id,
        )
        if rejection:
            rejected.append(rejection)
            continue
        article_reliability = reliability
        if not published_at:
            article_reliability = max(reliability - 0.12, 0.0)
            warnings.append(f"{source_name} article missing published_at: {title[:80]}")
        source = _node_text(item, "source") or _node_text(item, "author") or source_name
        is_official = source_name in {"Federal Reserve RSS", "BLS RSS", "BEA RSS"}
        canonical_url = None if "Google News RSS" in source_name else url
        aggregator_url = url if "Google News RSS" in source_name else None
        distribution_source = (
            "Google News"
            if "Google News RSS" in source_name
            else "Yahoo Finance"
            if "Yahoo Finance RSS" in source_name
            else None
        )
        text_for_tags = f"{title} {url or ''}"
        matched_symbols = [
            symbol for symbol in symbols if symbol.upper() in text_for_tags.upper()
        ]
        topics = tag_topics(text_for_tags)
        articles.append(
            {
                "raw_record_id": raw_record_id,
                "provider_record_id": provider_record_id,
                "source_identity_status": identity_status,
                "title": title,
                "source": source,
                "original_publisher": source,
                "published_at": published_at,
                "published_at_source": (
                    "rss_pub_date"
                    if pub_date and published_at
                    else "atom_published"
                    if atom_published and published_at
                    else "atom_updated"
                    if atom_updated and published_at
                    else None
                ),
                "editorial_updated_at": editorial_updated_at,
                "url": url,
                "source_url": url,
                "canonical_url": canonical_url,
                "aggregator_url": aggregator_url,
                "canonical_status": (
                    "unresolved"
                    if aggregator_url
                    else "resolved"
                    if canonical_url
                    else "unavailable"
                ),
                "canonical_url_status": (
                    "UNRESOLVED_DISTRIBUTOR_URL"
                    if aggregator_url
                    else "RESOLVED"
                    if canonical_url
                    else "ABSENT_SOURCE_IDENTITY_SUFFICIENT"
                ),
                "summary": summary,
                "content_snippet": summary,
                "summary_source_type": "rss_description" if description else ("content_encoded" if content_encoded else "atom_content" if atom_content else None),
                "summary_source_url": url,
                "source_text_available": bool(summary),
                "is_official": is_official,
                "symbols": matched_symbols,
                "topics": topics,
                "relevance": relevance(matched_symbols, topics),
                "provider_type": ProviderType.RSS.value,
                "provider": source_name,
                "acquisition_provider": source_name,
                "distribution_source": distribution_source,
                "reliability": article_reliability,
            }
        )
    return (
        articles,
        _dedupe_errors(warnings),
        rejected,
        outside,
        raw_ids,
    )


def _rss_text(item: ET.Element, tag: str) -> str | None:
    value = item.findtext(tag)
    if value:
        return value
    for child in item:
        if child.tag.endswith(tag.split("}")[-1]):
            return child.text
    return None


def _node_text(item: ET.Element, local_name: str) -> str | None:
    for child in item:
        if child.tag.split("}")[-1] != local_name:
            continue
        if local_name == "author":
            nested = next((node.text for node in child if node.tag.split("}")[-1] == "name" and node.text), None)
            return nested or child.text
        if local_name == "source":
            nested = next((node.text for node in child if node.tag.split("}")[-1] == "title" and node.text), None)
            return nested or child.text
        return child.text
    return None


def _node_link(item: ET.Element) -> str | None:
    for child in item:
        if child.tag.split("}")[-1] != "link":
            continue
        href = child.attrib.get("href")
        rel = child.attrib.get("rel", "alternate")
        if href and rel in {"alternate", ""}:
            return href
        if child.text:
            return child.text
    return None


def _clean_markup(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _raw_record_id(provider: str, index: int, payload: Any) -> str:
    serialized = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256(
        f"{provider}:{index}:{serialized}".encode("utf-8")
    ).hexdigest()
    return f"raw:{digest}"


def _source_record_identity(
    *,
    provider: str,
    native_value: Any,
    url: str | None,
    timestamp: str | None,
    title: str,
    content: Any,
) -> tuple[str | None, str]:
    native = str(native_value or "").strip()
    if native:
        return native, "PROVIDER_NATIVE"
    useful_content = _clean_markup(str(content)) if content not in (None, "") else None
    if not url and (not timestamp or not (title or useful_content)):
        return None, "MISSING"
    identity = {
        "provider": provider,
        "url": str(url or "").strip() or None,
        "timestamp": timestamp,
        "title": re.sub(r"\s+", " ", title.casefold()).strip() or None,
        "content_sha256": (
            hashlib.sha256(useful_content.encode("utf-8")).hexdigest()
            if useful_content
            else None
        ),
    }
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return f"derived:{digest}", "DERIVED_STABLE"


def _identity_rejection(
    *,
    raw_record_id: str,
    provider: str,
    title: str,
    content: Any,
    provider_record_id: str | None,
) -> dict[str, object] | None:
    useful_content = bool(title.strip() or _clean_markup(str(content or "")))
    if not useful_content:
        return {
            "record_id": raw_record_id,
            "provider": provider,
            "reason_code": "MISSING_USEFUL_CONTENT",
            "retryable": False,
        }
    if not provider_record_id:
        return {
            "record_id": raw_record_id,
            "provider": provider,
            "reason_code": "MISSING_SOURCE_IDENTITY",
            "retryable": False,
        }
    return None


def _out_of_scope(
    record_id: str,
    reason_code: str,
    *,
    provider: str,
) -> dict[str, object]:
    return {
        "record_id": record_id,
        "provider": provider,
        "reason_code": reason_code,
        "retryable": False,
    }


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


def parse_atom_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
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
    filtered, _ = _partition_recency(
        articles,
        recency_days=recency_days,
    )
    return filtered


def _partition_recency(
    articles: list[dict[str, object]],
    *,
    recency_days: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    cutoff = datetime.now(UTC) - timedelta(days=recency_days)
    filtered: list[dict[str, object]] = []
    outside: list[dict[str, object]] = []
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
            continue
        outside.append(
            _out_of_scope(
                str(
                    article.get("raw_record_id")
                    or article.get("provider_record_id")
                    or ""
                ),
                "OUTSIDE_RECENCY_WINDOW",
                provider=str(
                    article.get("acquisition_provider")
                    or article.get("provider")
                    or article.get("provider_type")
                    or "UNKNOWN"
                ),
            )
        )
    return filtered, outside


def _provider_batch(
    *,
    provider: str,
    provider_type: ProviderType,
    reliability: float,
    limit: int,
    raw_record_ids: list[str],
    articles: list[dict[str, object]],
    technical_rejections: list[dict[str, object]],
    explicit_out_of_scope: list[dict[str, object]],
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    status = "PARTIAL" if technical_rejections else "COMPLETE"
    return {
        "provider": provider,
        "provider_type": provider_type.value,
        "status": status,
        "coverage_status": status,
        "reliability": reliability,
        "calls": 1,
        "pages": 1,
        "metadata_enrichment_calls": 0,
        "metadata_enrichment_status": "NOT_REQUIRED",
        "metadata_enrichment_results": [],
        "per_provider_limit": limit,
        "pagination_supported": False,
        "pagination_complete": True,
        "raw_record_ids": raw_record_ids,
        "raw_count": len(raw_record_ids),
        "parsed_record_ids": [
            str(article.get("raw_record_id"))
            for article in articles
            if article.get("raw_record_id")
        ],
        "parsed_count": len(articles),
        "technical_rejections": list(technical_rejections),
        "explicit_out_of_scope": list(explicit_out_of_scope),
        "persisted_record_ids": [],
        "warnings": _dedupe_errors(warnings or []),
        "errors": [],
        "_articles": articles,
    }


def _failed_provider_batch(
    *,
    provider: str,
    provider_type: ProviderType,
    reliability: float,
    limit: int,
    error: Exception,
) -> dict[str, Any]:
    message = _redact_provider_error(str(error) or type(error).__name__)
    return {
        "provider": provider,
        "provider_type": provider_type.value,
        "status": "FAILED",
        "coverage_status": "FAILED",
        "reliability": reliability,
        "calls": 1,
        "pages": 0,
        "metadata_enrichment_calls": 0,
        "metadata_enrichment_status": "NOT_REQUIRED",
        "metadata_enrichment_results": [],
        "per_provider_limit": limit,
        "pagination_supported": False,
        "pagination_complete": False,
        "raw_record_ids": [],
        "raw_count": 0,
        "parsed_record_ids": [],
        "parsed_count": 0,
        "technical_rejections": [],
        "explicit_out_of_scope": [],
        "persisted_record_ids": [],
        "warnings": [],
        "errors": [
            f"{provider} {_category(message)}: {message}"
        ],
        "_articles": [],
    }


def _redact_provider_error(message: str) -> str:
    redacted = re.sub(
        r"(?i)(apikey|api_key|access_token|token|key)=([^&\s]+)",
        r"\1=REDACTED",
        str(message),
    )
    redacted = re.sub(
        r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+",
        "Bearer REDACTED",
        redacted,
    )
    return re.sub(
        r"(https?://)[^/@\s]+:[^/@\s]+@",
        r"\1REDACTED@",
        redacted,
    )


def _persistence_error(exc: Exception) -> tuple[str, bool]:
    if isinstance(exc, ValueError):
        detail = re.sub(r"[^A-Z0-9]+", "_", str(exc).upper()).strip("_")
        return f"PERSISTENCE_VALIDATION_{detail or 'REJECTED'}", False
    if isinstance(exc, TimeoutError):
        return "PERSISTENCE_TIMEOUT", True
    error_name = type(exc).__name__.upper()
    retryable = error_name in {
        "OPERATIONALERROR",
        "INTERFACEERROR",
        "DATABASEERROR",
    }
    return f"PERSISTENCE_{error_name}", retryable


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
    content = _clean_markup(
        str(
            article.get("content")
            or article.get("full_content")
            or article.get("content_snippet")
            or article.get("summary")
            or ""
        )
    )
    identity = {
        "provider_record_id": article.get("provider_record_id"),
        "original_publisher": str(
            article.get("original_publisher")
            or article.get("source")
            or ""
        ).casefold(),
        "canonical_url": str(
            article.get("canonical_url")
            or article.get("source_url")
            or article.get("url")
            or ""
        ).strip().casefold(),
        "published_at": article.get("published_at"),
        "editorial_updated_at": article.get("editorial_updated_at"),
        "title": re.sub(
            r"\s+",
            " ",
            str(article.get("title") or "").casefold(),
        ).strip(),
        "content_sha256": (
            hashlib.sha256(content.encode("utf-8")).hexdigest()
            if content
            else None
        ),
        "acquisition_provider": article.get("acquisition_provider"),
        "distribution_source": article.get("distribution_source"),
    }
    return hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


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
