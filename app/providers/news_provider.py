import asyncio
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
import hashlib
import html
import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import httpx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.config import Settings
from app.models.common import Freshness, ProviderResult, ProviderType
from app.models.nasdaq import Relevance
from app.providers.alpha_vantage import ensure_alpha_payload_ok
from app.providers.base import BaseProvider, metadata
from app.providers.calendar_utils import REQUEST_HEADERS
from app.services.provider_capability_registry import (
    dataset_runtime_provider_order,
)
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

DIRECT_RSS_PUBLISHERS = {
    "Federal Reserve RSS": "Federal Reserve",
    "BLS RSS": "U.S. Bureau of Labor Statistics",
    "BEA RSS": "U.S. Bureau of Economic Analysis",
    "MarketWatch RSS": "MarketWatch",
}

YAHOO_METADATA_REDIRECT_HOSTS = {"finance.yahoo.com"}


class MetadataRedirectError(RuntimeError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(detail)
        self.reason_code = reason_code


class _NewsRequestCallEvidence:
    """Per-invocation evidence of provider HTTP attempts."""

    def __init__(self) -> None:
        self._calls: Counter[str] = Counter()

    def record_call(self, provider: str) -> None:
        self._calls[provider] += 1

    def calls(self, provider: str) -> int:
        return int(self._calls[provider])


@dataclass(frozen=True, slots=True)
class _NewsProviderRuntimeSpec:
    provider_id: str
    provider: str
    provider_type: ProviderType
    reliability: float
    enabled_field: str
    url_field: str | None
    dispatch: str
    limit_field: str
    query_mode: str | None = None


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
        network_observer: Any | None = None,
    ) -> None:
        super().__init__(cache)
        self.settings = settings
        self.market_news_repository = market_news_repository
        self.network_observer = network_observer

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
        provider_specs = _news_runtime_provider_specs()
        loop = asyncio.get_running_loop()
        total_budget, acquisition_budget = _news_request_budgets(
            self.settings
        )
        internal_deadline = loop.time() + total_budget
        execution_evidence = _NewsRequestCallEvidence()
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            task_specs = self._provider_tasks(
                provider_specs=provider_specs,
                client=client,
                symbols=symbols,
                query=query,
                requested_limit=requested_limit,
                recency_days=recency_days,
                execution_evidence=execution_evidence,
            )
            observed_batches = await _bounded_news_provider_batches(
                task_specs,
                provider_specs=provider_specs,
                execution_evidence=execution_evidence,
                limit=requested_limit,
                timeout_seconds=acquisition_budget,
            )
            batches = _complete_news_provider_batches(
                observed_batches,
                settings=self.settings,
                limit=requested_limit,
                provider_specs=provider_specs,
            )
            if self.network_observer is not None:
                self.network_observer.register_provider_batches(batches)
            articles = [
                article
                for batch in batches
                for article in batch.get("_articles", [])
            ]
            articles = await self._enrich_missing_metadata(
                client,
                articles,
                batches=batches,
                timeout_seconds=max(
                    internal_deadline - loop.time() - 0.05,
                    0.0,
                ),
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
                    "provider_temporarily_unavailable_count": sum(
                        batch.get("status") == "TEMPORARILY_UNAVAILABLE"
                        for batch in batches
                    ),
                }
            )
            result.data["data_quality"] = quality
        return self._store_and_return(result)

    def _provider_tasks(
        self,
        *,
        provider_specs: tuple[_NewsProviderRuntimeSpec, ...],
        client: httpx.AsyncClient,
        symbols: list[str],
        query: str,
        requested_limit: int,
        recency_days: int,
        execution_evidence: _NewsRequestCallEvidence,
    ) -> list[tuple[str, Any]]:
        tasks: list[tuple[str, Any]] = []
        for spec in provider_specs:
            if not bool(
                getattr(self.settings, spec.enabled_field, False)
            ):
                continue
            url = (
                getattr(self.settings, spec.url_field, None)
                if spec.url_field
                else None
            )
            if spec.url_field and not url:
                continue
            provider_limit = min(
                requested_limit,
                int(getattr(self.settings, spec.limit_field)),
            )
            if spec.dispatch == "ALPHA_VANTAGE":
                awaitable = self._fetch_alpha_vantage(
                    client=client,
                    symbols=symbols,
                    limit=provider_limit,
                    recency_days=recency_days,
                    execution_evidence=execution_evidence,
                )
            elif spec.dispatch == "GDELT":
                awaitable = self._fetch_gdelt(
                    client=client,
                    symbols=symbols,
                    query=query,
                    limit=provider_limit,
                    recency_days=recency_days,
                    execution_evidence=execution_evidence,
                )
            elif spec.dispatch == "RSS":
                params = (
                    {
                        "q": f"{' OR '.join(symbols)} Nasdaq",
                        "hl": "en-US",
                        "gl": "US",
                        "ceid": "US:en",
                    }
                    if spec.query_mode == "GOOGLE_NEWS"
                    else {}
                )
                awaitable = _fetch_one_rss_feed(
                    client=client,
                    source=spec.provider,
                    url=str(url),
                    params=params,
                    reliability=spec.reliability,
                    symbols=symbols,
                    limit=provider_limit,
                    recency_days=recency_days,
                    timeout=min(
                        float(self.settings.http_timeout_seconds),
                        4.0,
                    ),
                    execution_evidence=execution_evidence,
                )
            else:
                raise RuntimeError(
                    "NEWS_RUNTIME_DISPATCH_UNMAPPED:"
                    f"{spec.provider_id}:{spec.dispatch}"
                )
            tasks.append((spec.provider, awaitable))
        return tasks

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
                    account.setdefault("persistence_rejections", []).append(
                        rejection
                    )
                persistence_results.append(
                    {
                        **rejection,
                        "provider": provider,
                        "result": "PERSISTENCE_REJECTED",
                        "error_type": "NormalizationRejected",
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
                    account.setdefault("exact_technical_duplicates", []).append(
                        rejection
                    )
                persistence_results.append(
                    {
                        **rejection,
                        "provider": provider,
                        "result": "EXACT_TECHNICAL_DUPLICATE",
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
                    account.setdefault("persistence_rejections", []).append(
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
            raw_capture_ids = {
                str(item.get("record_id"))
                for item in account.get("raw_capture") or []
                if item.get("record_id")
            }
            parsed_ids = set(account.get("parsed_record_ids") or [])
            persisted_ids = set(account.get("persisted_record_ids") or [])
            rejected_ids = {
                str(item.get("record_id"))
                for item in account.get("technical_rejections") or []
                if item.get("record_id")
            }
            persistence_rejected_ids = {
                str(item.get("record_id"))
                for item in account.get("persistence_rejections") or []
                if item.get("record_id")
            }
            duplicate_ids = {
                str(item.get("record_id"))
                for item in account.get("exact_technical_duplicates") or []
                if item.get("record_id")
            }
            outside_ids = {
                str(item.get("record_id"))
                for item in account.get("explicit_out_of_scope") or []
                if item.get("record_id")
            }
            raw_partitions = (parsed_ids, rejected_ids)
            raw_disjoint = all(
                not left.intersection(right)
                for index, left in enumerate(raw_partitions)
                for right in raw_partitions[index + 1 :]
            )
            parsed_partitions = (
                persisted_ids,
                persistence_rejected_ids,
                outside_ids,
                duplicate_ids,
            )
            parsed_disjoint = all(
                not left.intersection(right)
                for index, left in enumerate(parsed_partitions)
                for right in parsed_partitions[index + 1 :]
            )
            raw_accounted_ids = set().union(*raw_partitions)
            parsed_accounted_ids = set().union(*parsed_partitions)
            account["persisted_count"] = len(persisted_ids)
            account["technically_rejected_count"] = len(rejected_ids)
            account["explicit_out_of_scope_count"] = len(outside_ids)
            account["persistence_rejected_count"] = len(
                persistence_rejected_ids
            )
            account["exact_technical_duplicate_count"] = len(duplicate_ids)
            account["unaccounted_record_ids"] = sorted(
                raw_ids - raw_accounted_ids
            )
            account["unexpected_accounted_record_ids"] = sorted(
                raw_accounted_ids - raw_ids
            )
            account["parsed_but_not_accounted_ids"] = sorted(
                parsed_ids - parsed_accounted_ids
            )
            account["accounted_without_parsed_identity_ids"] = sorted(
                parsed_accounted_ids - parsed_ids
            )
            account["persisted_without_raw_lineage_ids"] = sorted(
                persisted_ids - raw_ids
            )
            account["raw_capture_missing_ids"] = sorted(
                raw_ids - raw_capture_ids
            )
            account["raw_capture_unexpected_ids"] = sorted(
                raw_capture_ids - raw_ids
            )
            reasoned_partitions = (
                account.get("technical_rejections") or [],
                account.get("persistence_rejections") or [],
                account.get("explicit_out_of_scope") or [],
                account.get("exact_technical_duplicates") or [],
            )
            account["partition_entries_missing_identity_or_reason"] = [
                {
                    "partition_index": partition_index,
                    "entry_index": entry_index,
                }
                for partition_index, partition in enumerate(
                    reasoned_partitions
                )
                for entry_index, item in enumerate(partition)
                if not item.get("record_id") or not item.get("reason_code")
            ]
            account["raw_capture_contract_violations"] = [
                str(item.get("record_id") or f"capture:{index}")
                for index, item in enumerate(account.get("raw_capture") or [])
                if not item.get("record_id")
                or item.get("captured_before_parsing") is not True
                or not item.get("payload_sha256")
                or item.get("payload_size_bytes") is None
                or "raw_payload" not in item
            ]
            account["accounting_disjoint"] = (
                raw_disjoint and parsed_disjoint
            )
            account["raw_identity_equation_valid"] = (
                raw_ids == raw_accounted_ids
            )
            account["parsed_identity_equation_valid"] = (
                parsed_ids == parsed_accounted_ids
            )
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
                raw_disjoint
                and parsed_disjoint
                and raw_ids == raw_capture_ids
                and not account["unaccounted_record_ids"]
                and not account["unexpected_accounted_record_ids"]
                and not account["parsed_but_not_accounted_ids"]
                and not account["accounted_without_parsed_identity_ids"]
                and not account["persisted_without_raw_lineage_ids"]
                and not account[
                    "partition_entries_missing_identity_or_reason"
                ]
                and not account["raw_capture_contract_violations"]
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
        aggregate_accounting = _aggregate_provider_accounting(accounts)
        accounting_valid = (
            accounting_valid and aggregate_accounting["valid"]
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
            account.get("status")
            in {"FAILED", "PARTIAL", "TEMPORARILY_UNAVAILABLE"}
            or account.get("metadata_enrichment_status") == "PARTIAL"
            for account in accounts
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
                "provider_accounting_aggregate": aggregate_accounting,
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
        execution_evidence: _NewsRequestCallEvidence | None = None,
    ) -> dict[str, Any]:
        provider = "Alpha Vantage NEWS_SENTIMENT"
        try:
            if execution_evidence is not None:
                execution_evidence.record_call(provider)
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
            raw_capture = _capture_raw_records(
                provider,
                list(payload.get("feed") or []),
            )
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
                raw_capture=raw_capture,
                articles=articles,
                technical_rejections=rejected,
                explicit_out_of_scope=[*outside, *recency_outside],
                pagination_complete=len(raw_ids) < limit,
                coverage_reason=(
                    "PROVIDER_PAGE_LIMIT_REACHED_NO_CURSOR"
                    if len(raw_ids) >= limit
                    else None
                ),
                records_not_acquired=(
                    "UNKNOWN" if len(raw_ids) >= limit else 0
                ),
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
        execution_evidence: _NewsRequestCallEvidence | None = None,
    ) -> dict[str, Any]:
        provider = "GDELT Doc API"
        attempts = 0
        try:
            while True:
                attempts += 1
                try:
                    if execution_evidence is not None:
                        execution_evidence.record_call(provider)
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
                        timeout=min(
                            float(self.settings.http_timeout_seconds),
                            float(self.settings.news_gdelt_timeout_seconds),
                        ),
                    )
                    response.raise_for_status()
                    break
                except Exception as exc:
                    retryable = _gdelt_retryable(exc)
                    if (
                        not retryable
                        or attempts >= self.settings.news_gdelt_max_attempts
                    ):
                        raise
                    await asyncio.sleep(
                        self.settings.news_gdelt_retry_backoff_seconds
                        * (2 ** (attempts - 1))
                    )
            payload = response.json()
            raw_capture = _capture_raw_records(
                provider,
                list(payload.get("articles") or []),
            )
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
                raw_capture=raw_capture,
                articles=articles,
                technical_rejections=rejected,
                explicit_out_of_scope=[*outside, *recency_outside],
                pagination_supported=True,
                pagination_complete=len(raw_ids) < limit,
                next_cursor=(
                    {"startrecord": len(raw_ids) + 1}
                    if len(raw_ids) >= limit
                    else None
                ),
                coverage_reason=(
                    "PROVIDER_PAGE_LIMIT_REACHED"
                    if len(raw_ids) >= limit
                    else None
                ),
                records_not_acquired=(
                    "UNKNOWN" if len(raw_ids) >= limit else 0
                ),
                calls=attempts,
                retry_count=max(attempts - 1, 0),
            )
        except Exception as exc:
            temporarily_unavailable = _gdelt_retryable(exc)
            return _failed_provider_batch(
                provider=provider,
                provider_type=ProviderType.API,
                reliability=0.66,
                limit=limit,
                error=exc,
                calls=max(attempts, 1),
                retry_count=max(attempts - 1, 0),
                status=(
                    "TEMPORARILY_UNAVAILABLE"
                    if temporarily_unavailable
                    else "FAILED"
                ),
                reason_code=_provider_failure_reason_code(
                    provider,
                    exc,
                    retry_exhausted=temporarily_unavailable,
                ),
            )

    async def _enrich_missing_metadata(
        self,
        client: httpx.AsyncClient,
        articles: list[dict[str, object]],
        *,
        batches: list[dict[str, Any]],
        timeout_seconds: float | None = None,
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
                requested_url = str(article.get("source_url"))
                current_url = requested_url
                redirect_count = 0
                while True:
                    response = await client.get(
                        current_url,
                        headers=REQUEST_HEADERS,
                        follow_redirects=False,
                        timeout=min(
                            float(self.settings.http_timeout_seconds),
                            2.5,
                        ),
                    )
                    if response.status_code not in {301, 302, 303, 307, 308}:
                        break
                    location = response.headers.get("location")
                    if not location:
                        raise MetadataRedirectError(
                            "METADATA_REDIRECT_LOCATION_MISSING",
                            f"{provider} redirect response has no Location",
                        )
                    next_url = urljoin(str(response.url), location)
                    if not _metadata_redirect_allowed(
                        provider=provider,
                        original_url=requested_url,
                        redirect_url=next_url,
                    ):
                        raise MetadataRedirectError(
                            "METADATA_REDIRECT_NOT_ALLOWLISTED",
                            f"{provider} redirect target is not allowlisted",
                        )
                    if self.network_observer is not None:
                        self.network_observer.register_metadata_redirect(
                            provider=provider,
                            record_id=record_id,
                            original_url=requested_url,
                            redirect_url=next_url,
                        )
                    redirect_count += 1
                    if redirect_count > 5:
                        raise MetadataRedirectError(
                            "METADATA_REDIRECT_LIMIT_EXCEEDED",
                            f"{provider} exceeded five metadata redirects",
                        )
                    current_url = next_url
                response.raise_for_status()
                metadata = extract_page_metadata(response.text, page_url=str(response.url))
            except Exception as exc:
                if batch is not None:
                    error_message = _redact_provider_error(
                        str(exc) or type(exc).__name__
                    )
                    batch["metadata_enrichment_status"] = "PARTIAL"
                    batch.setdefault("warnings", []).append(
                        f"{provider} metadata_enrichment_failed: {error_message}"
                    )
                    batch.setdefault("metadata_enrichment_results", []).append(
                        {
                            "record_id": record_id,
                            "status": "FAILED",
                            "reason_code": (
                                exc.reason_code
                                if isinstance(exc, MetadataRedirectError)
                                else "METADATA_ENRICHMENT_TIMEOUT"
                                if isinstance(exc, httpx.TimeoutException)
                                else "METADATA_ENRICHMENT_FAILED"
                            ),
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
                        "redirect_count": redirect_count,
                        "final_url": str(response.url),
                    }
                )
                if batch.get("metadata_enrichment_status") != "PARTIAL":
                    batch["metadata_enrichment_status"] = "COMPLETE"

        tasks = {
            asyncio.create_task(enrich(article)): article
            for article in candidates
        }
        if not tasks:
            return articles
        if timeout_seconds is None:
            await asyncio.gather(*tasks)
            return articles

        _, pending = await asyncio.wait(
            tasks,
            timeout=max(float(timeout_seconds), 0.0),
        )
        pending_articles = [tasks[task] for task in pending]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for article in pending_articles:
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
            if batch is None:
                continue
            record_id = str(
                article.get("raw_record_id")
                or article.get("provider_record_id")
                or ""
            )
            existing = {
                str(item.get("record_id"))
                for item in batch.get("metadata_enrichment_results") or []
            }
            if record_id in existing:
                continue
            batch["metadata_enrichment_status"] = "PARTIAL"
            batch.setdefault("warnings", []).append(
                f"{provider} metadata_enrichment_deadline"
            )
            batch.setdefault("metadata_enrichment_results", []).append(
                {
                    "record_id": record_id,
                    "status": "FAILED",
                    "reason_code": "METADATA_ENRICHMENT_DEADLINE",
                    "error_type": "TimeoutError",
                    "error": "request-scoped metadata deadline reached",
                }
            )
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
    execution_evidence: _NewsRequestCallEvidence | None = None,
) -> dict[str, Any]:
    try:
        if execution_evidence is not None:
            execution_evidence.record_call(source)
        response = await client.get(
            url,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        raw_capture = _capture_rss_records(source, response.text)
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
            raw_capture=raw_capture,
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
            rejection["source_identity"] = _raw_source_identity(
                provider_record_id=provider_record_id,
                title=title,
                timestamp=provider_seen_at,
                url=url,
            )
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
            rejection["source_identity"] = _raw_source_identity(
                provider_record_id=provider_record_id,
                title=title,
                timestamp=published_at,
                url=url,
            )
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
            rejection["source_identity"] = _raw_source_identity(
                provider_record_id=provider_record_id,
                title=title,
                timestamp=published_at or editorial_updated_at,
                url=url,
                guid=guid or None,
            )
            rejected.append(rejection)
            continue
        article_reliability = reliability
        if not published_at:
            article_reliability = max(reliability - 0.12, 0.0)
            warnings.append(f"{source_name} article missing published_at: {title[:80]}")
        declared_publisher = (
            _node_text(item, "source")
            or _node_text(item, "author")
            or DIRECT_RSS_PUBLISHERS.get(source_name)
        )
        source = declared_publisher or source_name
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
                "original_publisher": declared_publisher,
                "_source_is_declared_publisher": bool(declared_publisher),
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
    serialized = _canonical_raw_payload(payload)
    digest = hashlib.sha256(
        f"{provider}:{index}:{serialized}".encode("utf-8")
    ).hexdigest()
    return f"raw:{digest}"


def _canonical_raw_payload(payload: Any) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def _capture_raw_records(
    provider: str,
    payloads: list[Any],
) -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []
    for index, payload in enumerate(payloads):
        serialized = _canonical_raw_payload(payload)
        captured.append(
            {
                "record_id": _raw_record_id(provider, index, payload),
                "provider": provider,
                "sequence_index": index,
                "captured_before_parsing": True,
                "payload_sha256": hashlib.sha256(
                    serialized.encode("utf-8")
                ).hexdigest(),
                "payload_size_bytes": len(serialized.encode("utf-8")),
                "raw_payload": payload,
            }
        )
    return captured


def _capture_rss_records(
    provider: str,
    text: str,
) -> list[dict[str, Any]]:
    root = ET.fromstring(text)
    rss_items = list(root.findall(".//item"))
    atom_items = [
        node for node in root.iter() if node.tag.split("}")[-1] == "entry"
    ]
    payloads = [
        ET.tostring(item, encoding="unicode")
        for item in [*rss_items, *atom_items]
    ]
    return _capture_raw_records(provider, payloads)


def _gdelt_retryable(exc: Exception) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503, 504}
    return False


def _metadata_redirect_allowed(
    *,
    provider: str,
    original_url: str,
    redirect_url: str,
) -> bool:
    original = urlparse(original_url)
    redirect = urlparse(redirect_url)
    if redirect.scheme != "https" or not redirect.hostname:
        return False
    if provider == "Yahoo Finance RSS":
        return (
            original.scheme == "https"
            and original.hostname in YAHOO_METADATA_REDIRECT_HOSTS
            and redirect.hostname in YAHOO_METADATA_REDIRECT_HOSTS
        )
    return (
        original.scheme == "https"
        and redirect.hostname == original.hostname
    )


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


def _raw_source_identity(
    *,
    provider_record_id: str | None,
    title: str,
    timestamp: str | None,
    url: str | None,
    guid: str | None = None,
) -> dict[str, object]:
    return {
        "provider_record_id": provider_record_id,
        "guid": guid,
        "title": title or None,
        "timestamp": timestamp,
        "url": url,
        "content_identity": hashlib.sha256(
            re.sub(r"\s+", " ", title).strip().casefold().encode("utf-8")
        ).hexdigest()
        if title
        else None,
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
        exclusion = _out_of_scope(
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
            )
        )
        exclusion["source_identity"] = _raw_source_identity(
            provider_record_id=(
                str(article.get("provider_record_id"))
                if article.get("provider_record_id")
                else None
            ),
            title=str(article.get("title") or ""),
            timestamp=str(value),
            url=(
                str(
                    article.get("canonical_url")
                    or article.get("source_url")
                    or article.get("url")
                )
                if (
                    article.get("canonical_url")
                    or article.get("source_url")
                    or article.get("url")
                )
                else None
            ),
        )
        outside.append(exclusion)
    return filtered, outside


_NEWS_PROVIDER_RUNTIME_SPECS = {
    "ALPHA_VANTAGE_NEWS_SENTIMENT": _NewsProviderRuntimeSpec(
        provider_id="ALPHA_VANTAGE_NEWS_SENTIMENT",
        provider="Alpha Vantage NEWS_SENTIMENT",
        provider_type=ProviderType.API,
        reliability=0.74,
        enabled_field="alpha_vantage_api_key",
        url_field=None,
        dispatch="ALPHA_VANTAGE",
        limit_field="news_alpha_vantage_limit",
    ),
    "GDELT_DOC_API": _NewsProviderRuntimeSpec(
        provider_id="GDELT_DOC_API",
        provider="GDELT Doc API",
        provider_type=ProviderType.API,
        reliability=0.66,
        enabled_field="news_gdelt_enabled",
        url_field=None,
        dispatch="GDELT",
        limit_field="news_gdelt_limit",
    ),
    "FEDERAL_RESERVE_RSS": _NewsProviderRuntimeSpec(
        provider_id="FEDERAL_RESERVE_RSS",
        provider="Federal Reserve RSS",
        provider_type=ProviderType.RSS,
        reliability=0.76,
        enabled_field="news_rss_enabled",
        url_field="federal_reserve_rss_url",
        dispatch="RSS",
        limit_field="news_rss_limit_per_feed",
    ),
    "BLS_RSS": _NewsProviderRuntimeSpec(
        provider_id="BLS_RSS",
        provider="BLS RSS",
        provider_type=ProviderType.RSS,
        reliability=0.86,
        enabled_field="news_rss_enabled",
        url_field="bls_rss_url",
        dispatch="RSS",
        limit_field="news_rss_limit_per_feed",
    ),
    "BEA_RSS": _NewsProviderRuntimeSpec(
        provider_id="BEA_RSS",
        provider="BEA RSS",
        provider_type=ProviderType.RSS,
        reliability=0.86,
        enabled_field="news_rss_enabled",
        url_field="bea_rss_url",
        dispatch="RSS",
        limit_field="news_rss_limit_per_feed",
    ),
    "YAHOO_FINANCE_RSS": _NewsProviderRuntimeSpec(
        provider_id="YAHOO_FINANCE_RSS",
        provider="Yahoo Finance RSS",
        provider_type=ProviderType.RSS,
        reliability=0.58,
        enabled_field="news_rss_enabled",
        url_field="yahoo_finance_rss_url",
        dispatch="RSS",
        limit_field="news_rss_limit_per_feed",
    ),
    "MARKETWATCH_RSS": _NewsProviderRuntimeSpec(
        provider_id="MARKETWATCH_RSS",
        provider="MarketWatch RSS",
        provider_type=ProviderType.RSS,
        reliability=0.56,
        enabled_field="news_rss_enabled",
        url_field="marketwatch_rss_url",
        dispatch="RSS",
        limit_field="news_rss_limit_per_feed",
    ),
    "GOOGLE_NEWS_RSS": _NewsProviderRuntimeSpec(
        provider_id="GOOGLE_NEWS_RSS",
        provider="Google News RSS",
        provider_type=ProviderType.RSS,
        reliability=0.64,
        enabled_field="news_rss_enabled",
        url_field="google_news_rss_url",
        dispatch="RSS",
        limit_field="news_rss_limit_per_feed",
        query_mode="GOOGLE_NEWS",
    ),
}


def _news_runtime_provider_specs() -> tuple[
    _NewsProviderRuntimeSpec,
    ...,
]:
    provider_ids = dataset_runtime_provider_order(
        "current_news",
        _NEWS_PROVIDER_RUNTIME_SPECS,
    )
    return tuple(
        _NEWS_PROVIDER_RUNTIME_SPECS[provider_id]
        for provider_id in provider_ids
    )


class _NewsProviderSpecsView:
    """Legacy tuple view resolved from the current policy on every access."""

    @staticmethod
    def _values() -> tuple[
        tuple[str, ProviderType, float, str, str | None],
        ...,
    ]:
        return tuple(
            (
                spec.provider,
                spec.provider_type,
                spec.reliability,
                spec.enabled_field,
                spec.url_field,
            )
            for spec in _news_runtime_provider_specs()
        )

    def __iter__(self):
        return iter(self._values())

    def __len__(self) -> int:
        return len(self._values())

    def __getitem__(self, index):
        return self._values()[index]


# Imported by NasdaqDataService for cache-accounting validation. This is a
# dynamic compatibility view, not a module-import policy snapshot.
NEWS_PROVIDER_SPECS = _NewsProviderSpecsView()


def _news_request_budgets(settings: Settings) -> tuple[float, float]:
    wrapper_budget = max(
        float(settings.timeout_news_seconds),
        1.0,
    )
    wrapper_grace = min(
        1.0,
        max(0.1, wrapper_budget * 0.1),
    )
    total_budget = max(wrapper_budget - wrapper_grace, 0.1)
    acquisition_budget = max(total_budget * 0.7, 0.05)
    return total_budget, min(acquisition_budget, total_budget)


async def _bounded_news_provider_batches(
    task_specs: list[tuple[str, Any]],
    *,
    provider_specs: tuple[_NewsProviderRuntimeSpec, ...],
    execution_evidence: _NewsRequestCallEvidence,
    limit: int,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    if not task_specs:
        return []

    task_by_provider = {
        provider: asyncio.create_task(awaitable)
        for provider, awaitable in task_specs
    }
    _, pending = await asyncio.wait(
        task_by_provider.values(),
        timeout=max(float(timeout_seconds), 0.0),
    )
    pending_providers = {
        provider
        for provider, task in task_by_provider.items()
        if task in pending
    }
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    batches: list[dict[str, Any]] = []
    for provider, task in task_by_provider.items():
        provider_type, reliability = _news_provider_identity(
            provider,
            provider_specs=provider_specs,
        )
        calls = execution_evidence.calls(provider)
        if provider in pending_providers:
            if calls:
                batches.append(
                    _failed_provider_batch(
                        provider=provider,
                        provider_type=provider_type,
                        reliability=reliability,
                        limit=limit,
                        error=TimeoutError(
                            "request-scoped news fan-in deadline reached"
                        ),
                        calls=calls,
                        status="TEMPORARILY_UNAVAILABLE",
                        reason_code=(
                            f"{_provider_reason_prefix(provider)}"
                            "_FAN_IN_DEADLINE"
                        ),
                    )
                )
            else:
                batches.append(
                    _observed_skip_provider_batch(
                        provider=provider,
                        provider_type=provider_type,
                        reliability=reliability,
                        limit=limit,
                        reason_code=(
                            f"{_provider_reason_prefix(provider)}"
                            "_FAN_IN_DEADLINE_BEFORE_ATTEMPT"
                        ),
                    )
                )
            continue
        try:
            batches.append(task.result())
        except Exception as exc:
            if calls:
                batches.append(
                    _failed_provider_batch(
                        provider=provider,
                        provider_type=provider_type,
                        reliability=reliability,
                        limit=limit,
                        error=exc,
                        calls=calls,
                    )
                )
            else:
                batches.append(
                    _observed_skip_provider_batch(
                        provider=provider,
                        provider_type=provider_type,
                        reliability=reliability,
                        limit=limit,
                        reason_code=(
                            f"{_provider_reason_prefix(provider)}"
                            "_EXECUTION_FAILED_BEFORE_ATTEMPT"
                        ),
                    )
                )
    return batches


def _news_provider_identity(
    provider: str,
    *,
    provider_specs: tuple[_NewsProviderRuntimeSpec, ...] | None = None,
) -> tuple[ProviderType, float]:
    for spec in provider_specs or _news_runtime_provider_specs():
        if spec.provider == provider:
            return spec.provider_type, spec.reliability
    raise ValueError(f"unknown news provider: {provider}")


def _provider_reason_prefix(provider: str) -> str:
    return re.sub(
        r"[^A-Z0-9]+",
        "_",
        provider.upper(),
    ).strip("_")


def _complete_news_provider_batches(
    batches: list[dict[str, Any]],
    *,
    settings: Settings,
    limit: int,
    provider_specs: tuple[_NewsProviderRuntimeSpec, ...] | None = None,
) -> list[dict[str, Any]]:
    specs = provider_specs or _news_runtime_provider_specs()
    output = list(batches)
    observed = {
        str(batch.get("provider"))
        for batch in batches
        if batch.get("provider")
    }
    for spec in specs:
        if spec.provider in observed:
            continue
        enabled = bool(
            getattr(settings, spec.enabled_field, False)
        )
        url_configured = bool(
            getattr(settings, spec.url_field, None)
        ) if spec.url_field else True
        reason = (
            "PROVIDER_URL_NOT_CONFIGURED"
            if enabled and not url_configured
            else "PROVIDER_CREDENTIAL_NOT_CONFIGURED"
            if spec.provider_id == "ALPHA_VANTAGE_NEWS_SENTIMENT"
            else "PROVIDER_DISABLED_BY_CONFIGURATION"
        )
        output.append(
            _not_called_provider_batch(
                provider=spec.provider,
                provider_type=spec.provider_type,
                reliability=spec.reliability,
                limit=limit,
                reason_code=reason,
            )
        )
    return output


def _provider_batch(
    *,
    provider: str,
    provider_type: ProviderType,
    reliability: float,
    limit: int,
    raw_record_ids: list[str],
    raw_capture: list[dict[str, Any]],
    articles: list[dict[str, object]],
    technical_rejections: list[dict[str, object]],
    explicit_out_of_scope: list[dict[str, object]],
    warnings: list[str] | None = None,
    pagination_supported: bool = False,
    pagination_complete: bool = True,
    next_cursor: dict[str, object] | None = None,
    coverage_reason: str | None = None,
    records_not_acquired: int | str = 0,
    calls: int = 1,
    retry_count: int = 0,
) -> dict[str, Any]:
    captured_ids = [
        str(item.get("record_id"))
        for item in raw_capture
        if item.get("record_id")
    ]
    if captured_ids != raw_record_ids:
        raise AssertionError(
            f"{provider} raw capture identity differs from parser input"
        )
    parsed_record_ids = [
        str(article.get("raw_record_id"))
        for article in articles
        if article.get("raw_record_id")
    ]
    parsed_record_ids.extend(
        str(item.get("record_id"))
        for item in explicit_out_of_scope
        if item.get("record_id")
    )
    status = (
        "PARTIAL"
        if technical_rejections or not pagination_complete
        else "COMPLETE"
    )
    return {
        "provider": provider,
        "provider_type": provider_type.value,
        "status": status,
        "coverage_status": status,
        "reliability": reliability,
        "calls": calls,
        "pages": 1,
        "retry_count": retry_count,
        "metadata_enrichment_calls": 0,
        "metadata_enrichment_required": False,
        "metadata_enrichment_status": "NOT_REQUIRED",
        "metadata_enrichment_results": [],
        "per_provider_limit": limit,
        "post_fetch_limit_applied": False,
        "received_records_fully_processed": True,
        "pagination_supported": pagination_supported,
        "pagination_complete": pagination_complete,
        "next_cursor": next_cursor,
        "coverage_reason": coverage_reason,
        "records_not_acquired": records_not_acquired,
        "raw_capture_status": "COMPLETE",
        "raw_capture": raw_capture,
        "raw_record_ids": raw_record_ids,
        "raw_count": len(raw_record_ids),
        "parsed_record_ids": sorted(set(parsed_record_ids)),
        "parsed_count": len(set(parsed_record_ids)),
        "technical_rejections": list(technical_rejections),
        "explicit_out_of_scope": list(explicit_out_of_scope),
        "persistence_rejections": [],
        "exact_technical_duplicates": [],
        "persisted_record_ids": [],
        "warnings": _dedupe_errors(warnings or []),
        "errors": [],
        "_articles": articles,
    }


def _not_called_provider_batch(
    *,
    provider: str,
    provider_type: ProviderType,
    reliability: float,
    limit: int,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "provider": provider,
        "provider_type": provider_type.value,
        "status": "NOT_CONFIGURED",
        "availability_status": "NOT_CONFIGURED",
        "coverage_status": "NOT_CONFIGURED",
        "reason_code": reason_code,
        "temporary": False,
        "reliability": reliability,
        "calls": 0,
        "pages": 0,
        "retry_count": 0,
        "metadata_enrichment_calls": 0,
        "metadata_enrichment_required": False,
        "metadata_enrichment_status": "NOT_REQUIRED",
        "metadata_enrichment_results": [],
        "per_provider_limit": limit,
        "pagination_supported": False,
        "pagination_complete": True,
        "coverage_reason": reason_code,
        "raw_capture_status": "NOT_ACQUIRED",
        "raw_capture": [],
        "raw_record_ids": [],
        "raw_count": 0,
        "parsed_record_ids": [],
        "parsed_count": 0,
        "technical_rejections": [],
        "explicit_out_of_scope": [],
        "persistence_rejections": [],
        "exact_technical_duplicates": [],
        "persisted_record_ids": [],
        "warnings": [],
        "errors": [],
        "_articles": [],
    }


def _observed_skip_provider_batch(
    *,
    provider: str,
    provider_type: ProviderType,
    reliability: float,
    limit: int,
    reason_code: str,
) -> dict[str, Any]:
    batch = _not_called_provider_batch(
        provider=provider,
        provider_type=provider_type,
        reliability=reliability,
        limit=limit,
        reason_code=reason_code,
    )
    batch.update(
        {
            "status": "NOT_CALLED",
            "availability_status": "NOT_CALLED",
            "coverage_status": "NOT_CALLED",
        }
    )
    return batch


def _failed_provider_batch(
    *,
    provider: str,
    provider_type: ProviderType,
    reliability: float,
    limit: int,
    error: Exception,
    calls: int = 1,
    retry_count: int = 0,
    status: str = "FAILED",
    reason_code: str | None = None,
) -> dict[str, Any]:
    message = _redact_provider_error(str(error) or type(error).__name__)
    reason_code = reason_code or _provider_failure_reason_code(
        provider,
        error,
        retry_exhausted=retry_count > 0,
    )
    return {
        "provider": provider,
        "provider_type": provider_type.value,
        "status": status,
        "availability_status": status,
        "coverage_status": "FAILED",
        "reason_code": reason_code,
        "temporary": status == "TEMPORARILY_UNAVAILABLE",
        "reliability": reliability,
        "calls": calls,
        "pages": 0,
        "retry_count": retry_count,
        "metadata_enrichment_calls": 0,
        "metadata_enrichment_required": False,
        "metadata_enrichment_status": "NOT_REQUIRED",
        "metadata_enrichment_results": [],
        "per_provider_limit": limit,
        "pagination_supported": False,
        "pagination_complete": False,
        "coverage_reason": reason_code,
        "raw_capture_status": "NOT_ACQUIRED",
        "raw_capture": [],
        "raw_record_ids": [],
        "raw_count": 0,
        "parsed_record_ids": [],
        "parsed_count": 0,
        "technical_rejections": [],
        "explicit_out_of_scope": [],
        "persistence_rejections": [],
        "exact_technical_duplicates": [],
        "persisted_record_ids": [],
        "warnings": [],
        "errors": [
            f"{provider} {_category(message)}: {message}"
        ],
        "_articles": [],
    }


def _aggregate_provider_accounting(
    accounts: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_list = [
        str(record_id)
        for account in accounts
        for record_id in account.get("raw_record_ids") or []
    ]
    raw_ids = set(raw_list)
    parsed_ids = {
        str(record_id)
        for account in accounts
        for record_id in account.get("parsed_record_ids") or []
    }
    persisted_ids = {
        str(record_id)
        for account in accounts
        for record_id in account.get("persisted_record_ids") or []
    }
    technical_rejected_ids = {
        str(item.get("record_id"))
        for account in accounts
        for item in account.get("technical_rejections") or []
        if item.get("record_id")
    }
    persistence_rejected_ids = {
        str(item.get("record_id"))
        for account in accounts
        for item in account.get("persistence_rejections") or []
        if item.get("record_id")
    }
    outside_ids = {
        str(item.get("record_id"))
        for account in accounts
        for item in account.get("explicit_out_of_scope") or []
        if item.get("record_id")
    }
    duplicate_ids = {
        str(item.get("record_id"))
        for account in accounts
        for item in account.get("exact_technical_duplicates") or []
        if item.get("record_id")
    }
    raw_partitions = (parsed_ids, technical_rejected_ids)
    parsed_partitions = (
        persisted_ids,
        persistence_rejected_ids,
        outside_ids,
        duplicate_ids,
    )
    raw_disjoint = all(
        left.isdisjoint(right)
        for index, left in enumerate(raw_partitions)
        for right in raw_partitions[index + 1 :]
    )
    parsed_disjoint = all(
        left.isdisjoint(right)
        for index, left in enumerate(parsed_partitions)
        for right in parsed_partitions[index + 1 :]
    )
    cross_provider_identity_collisions = sorted(
        record_id
        for record_id, count in Counter(raw_list).items()
        if count > 1
    )
    raw_accounted = set().union(*raw_partitions)
    parsed_accounted = set().union(*parsed_partitions)
    return {
        "valid": (
            bool(accounts)
            and raw_disjoint
            and parsed_disjoint
            and not cross_provider_identity_collisions
            and raw_ids == raw_accounted
            and parsed_ids == parsed_accounted
        ),
        "raw_count": len(raw_ids),
        "parsed_count": len(parsed_ids),
        "persisted_count": len(persisted_ids),
        "technically_rejected_count": len(technical_rejected_ids),
        "persistence_rejected_count": len(persistence_rejected_ids),
        "explicit_out_of_scope_count": len(outside_ids),
        "exact_technical_duplicate_count": len(duplicate_ids),
        "raw_identity_equation_valid": raw_ids == raw_accounted,
        "parsed_identity_equation_valid": parsed_ids == parsed_accounted,
        "raw_partitions_disjoint": raw_disjoint,
        "parsed_partitions_disjoint": parsed_disjoint,
        "cross_provider_identity_collision_ids": (
            cross_provider_identity_collisions
        ),
        "unaccounted_raw_ids": sorted(raw_ids - raw_accounted),
        "unexpected_raw_partition_ids": sorted(raw_accounted - raw_ids),
        "parsed_but_not_accounted_ids": sorted(
            parsed_ids - parsed_accounted
        ),
        "accounted_without_parsed_identity_ids": sorted(
            parsed_accounted - parsed_ids
        ),
    }


def _provider_failure_reason_code(
    provider: str,
    error: Exception,
    *,
    retry_exhausted: bool,
) -> str:
    prefix = re.sub(r"[^A-Z0-9]+", "_", provider.upper()).strip("_")
    if isinstance(error, httpx.ConnectTimeout):
        suffix = "CONNECT_TIMEOUT"
    elif isinstance(error, httpx.TimeoutException):
        suffix = "TIMEOUT"
    elif isinstance(error, httpx.HTTPStatusError):
        suffix = f"HTTP_{error.response.status_code}"
    elif isinstance(error, httpx.NetworkError):
        suffix = "NETWORK_ERROR"
    else:
        suffix = re.sub(
            r"[^A-Z0-9]+",
            "_",
            type(error).__name__.upper(),
        ).strip("_")
    if retry_exhausted:
        suffix = f"{suffix}_RETRY_EXHAUSTED"
    return f"{prefix}_{suffix}"


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
