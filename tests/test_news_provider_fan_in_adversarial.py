from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
import respx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.news_provider import (
    NewsProvider,
    _article_key,
    _redact_provider_error,
    parse_gdelt_articles,
    parse_rss_articles,
)
from app.services.news_intelligence_service import (
    build_news_context,
    normalize_news_article,
)
from app.services.market_news_repository import MarketNewsRepository


NOW = datetime(2026, 7, 29, 13, 0, tzinfo=UTC)


class RecordingNewsRepository:
    def __init__(self, *, fail_provider_record_ids: set[str] | None = None) -> None:
        self.fail_provider_record_ids = fail_provider_record_ids or set()
        self.stored: list[dict] = []

    def upsert_news(self, article: dict) -> dict:
        if article.get("provider_record_id") in self.fail_provider_record_ids:
            raise RuntimeError("fixture persistence failure")
        self.stored.append(dict(article))
        return {"news_key": article["news_key"]}


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "_env_file": None,
        "environment": "test",
        "database_path": tmp_path / "market.sqlite",
        "alpha_vantage_api_key": "unit-key",
        "alpha_vantage_base_url": "https://alpha.test/query",
        "gdelt_doc_api_url": "https://gdelt.test/api",
        "federal_reserve_rss_url": "https://fed.test/rss",
        "bls_rss_url": "https://bls.test/rss",
        "bea_rss_url": "https://bea.test/rss",
        "yahoo_finance_rss_url": "https://yahoo.test/rss",
        "marketwatch_rss_url": "https://marketwatch.test/rss",
        "google_news_rss_url": "https://google.test/rss",
        "news_metadata_enrichment_limit_per_provider": 0,
    }
    values.update(overrides)
    return Settings(**values)


def _rss(provider: str, record_id: str) -> str:
    published = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")
    return f"""<rss><channel><item>
      <guid>{record_id}</guid>
      <title>{provider} reports a material Nasdaq update</title>
      <link>https://publisher.test/{record_id}</link>
      <pubDate>{published}</pubDate>
      <source>{provider} Publisher</source>
      <description>A complete fixture summary for the controlled provider record.</description>
    </item></channel></rss>"""


def _assert_exact_provider_accounting(account: dict) -> None:
    raw = set(account["raw_record_ids"])
    persisted = set(account["persisted_record_ids"])
    rejected = {
        item["record_id"] for item in account["technical_rejections"]
    }
    outside = {
        item["record_id"] for item in account["explicit_out_of_scope"]
    }
    assert raw == persisted | rejected | outside
    assert persisted.isdisjoint(rejected)
    assert persisted.isdisjoint(outside)
    assert rejected.isdisjoint(outside)
    assert account["accounting_valid"] is True


@pytest.mark.asyncio
async def test_fan_in_calls_every_enabled_provider_and_persists_union(tmp_path) -> None:
    repository = RecordingNewsRepository()
    settings = _settings(tmp_path)
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        market_news_repository=repository,
    )
    gdelt_seen = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    alpha_published = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    rss_urls = {
        "https://fed.test/rss": ("Federal Reserve RSS", "fed-1"),
        "https://bls.test/rss": ("BLS RSS", "bls-1"),
        "https://bea.test/rss": ("BEA RSS", "bea-1"),
        "https://yahoo.test/rss": ("Yahoo Finance RSS", "yahoo-1"),
        "https://marketwatch.test/rss": ("MarketWatch RSS", "marketwatch-1"),
        "https://google.test/rss": ("Google News RSS", "google-1"),
    }
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://alpha.test/query").mock(
            return_value=httpx.Response(
                200,
                json={
                    "feed": [
                        {
                            "id": "alpha-1",
                            "title": "Alpha reports a material Nasdaq update",
                            "url": "https://publisher.test/alpha-1",
                            "time_published": alpha_published,
                            "source": "Alpha Publisher",
                            "summary": "A complete Alpha fixture summary for persistence.",
                        }
                    ]
                },
            )
        )
        router.get("https://gdelt.test/api").mock(
            return_value=httpx.Response(
                200,
                json={
                    "articles": [
                        {
                            "id": "gdelt-1",
                            "title": "GDELT reports a material Nasdaq update",
                            "url": "https://publisher.test/gdelt-1",
                            "seendate": gdelt_seen,
                            "domain": "GDELT Publisher",
                            "description": "A complete GDELT fixture summary.",
                        }
                    ]
                },
            )
        )
        for url, (name, record_id) in rss_urls.items():
            router.get(url).mock(
                return_value=httpx.Response(200, text=_rss(name, record_id))
            )

        result = await provider.fetch_for_symbols(
            ["NVDA", "QQQ"],
            limit=1,
            recency_days=14,
        )

    accounts = result.data["provider_accounting"]
    assert len(accounts) == 8
    assert len(repository.stored) == 8
    assert len(result.data["articles"]) == 8
    assert result.data["data_quality"]["global_limit_applied"] is False
    assert result.data["data_quality"]["requested_limit_semantics"] == "PER_PROVIDER"
    assert result.data["data_quality"]["provider_accounting_valid"] is True
    assert result.data["data_quality"]["persistence_status"] == "COMPLETE"
    assert result.data["data_quality"]["readiness"] == "AVAILABLE"
    for account in accounts:
        assert account["calls"] == 1
        assert account["per_provider_limit"] == 1
        _assert_exact_provider_accounting(account)


@pytest.mark.asyncio
async def test_one_provider_failure_does_not_block_other_providers(tmp_path) -> None:
    repository = RecordingNewsRepository()
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_rss_limit_per_feed=1,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        market_news_repository=repository,
    )
    gdelt_seen = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://gdelt.test/api").mock(
            return_value=httpx.Response(
                200,
                json={
                    "articles": [
                        {
                            "id": "gdelt-ok",
                            "title": "GDELT remains available",
                            "url": "https://publisher.test/gdelt-ok",
                            "seendate": gdelt_seen,
                            "description": "A complete GDELT fixture summary.",
                        }
                    ]
                },
            )
        )
        router.get("https://fed.test/rss").mock(
            return_value=httpx.Response(503, text="unavailable")
        )
        for url, name in (
            ("https://bls.test/rss", "BLS RSS"),
            ("https://bea.test/rss", "BEA RSS"),
            ("https://yahoo.test/rss", "Yahoo Finance RSS"),
            ("https://marketwatch.test/rss", "MarketWatch RSS"),
            ("https://google.test/rss", "Google News RSS"),
        ):
            router.get(url).mock(
                return_value=httpx.Response(
                    200,
                    text=_rss(name, name.lower().replace(" ", "-")),
                )
            )

        result = await provider.fetch_for_symbols(
            ["NVDA"],
            limit=1,
            recency_days=14,
        )

    failed = [
        account
        for account in result.data["provider_accounting"]
        if account["status"] == "FAILED"
    ]
    assert [account["provider"] for account in failed] == ["Federal Reserve RSS"]
    assert len(result.data["articles"]) == 6
    assert result.data["data_quality"]["provider_failure_count"] == 1
    assert result.data["data_quality"]["readiness"] == "DEGRADED"


def test_url_less_rss_guid_is_deliverable_with_explicit_statuses() -> None:
    rss = """<rss><channel><item>
      <guid>publisher-native-guid-1</guid>
      <title>URL-less official release</title>
      <pubDate>Wed, 29 Jul 2026 12:00:00 GMT</pubDate>
      <source>Federal Reserve</source>
      <description>Policy content remains useful without a canonical URL.</description>
    </item></channel></rss>"""
    articles, warnings = parse_rss_articles(
        rss,
        symbols=["QQQ"],
        limit=10,
        source_name="Federal Reserve RSS",
        reliability=0.76,
    )
    assert warnings == []
    assert len(articles) == 1
    parsed = articles[0]
    assert parsed["url"] is None
    assert parsed["provider_record_id"] == "publisher-native-guid-1"
    assert parsed["source_identity_status"] == "PROVIDER_NATIVE"
    assert parsed["canonical_url_status"] == "ABSENT_SOURCE_IDENTITY_SUFFICIENT"
    normalized = normalize_news_article(parsed, now=NOW)
    assert normalized["accepted"] is True
    assert normalized["content_availability_status"] == "SUMMARY_ONLY"


def test_url_less_rss_without_guid_builds_stable_source_identity() -> None:
    rss = """<rss><channel><item>
      <title>URL-less derived identity release</title>
      <pubDate>Wed, 29 Jul 2026 12:00:00 GMT</pubDate>
      <source>Fixture Publisher</source>
      <description>Stable content and timestamp provide sufficient identity.</description>
    </item></channel></rss>"""
    first, _ = parse_rss_articles(
        rss,
        symbols=["QQQ"],
        limit=10,
        source_name="Fixture RSS",
        reliability=0.64,
    )
    second, _ = parse_rss_articles(
        rss,
        symbols=["QQQ"],
        limit=10,
        source_name="Fixture RSS",
        reliability=0.64,
    )
    assert first[0]["provider_record_id"] == second[0]["provider_record_id"]
    assert str(first[0]["provider_record_id"]).startswith("derived:")
    assert first[0]["source_identity_status"] == "DERIVED_STABLE"
    assert normalize_news_article(first[0], now=NOW)["accepted"] is True


def test_missing_title_with_content_and_native_identity_is_deliverable() -> None:
    rss = """<rss><channel><item>
      <guid>content-only-guid</guid>
      <pubDate>Wed, 29 Jul 2026 12:00:00 GMT</pubDate>
      <source>Fixture Publisher</source>
      <description>Content-only records remain useful and auditable.</description>
    </item></channel></rss>"""
    articles, _ = parse_rss_articles(
        rss,
        symbols=["QQQ"],
        limit=10,
        source_name="Fixture RSS",
        reliability=0.64,
    )
    normalized = normalize_news_article(articles[0], now=NOW)
    assert normalized["title"] == ""
    assert normalized["accepted"] is True
    assert normalized["content_availability_status"] == "SUMMARY_ONLY"


def test_url_less_gdelt_native_identity_is_deliverable() -> None:
    articles = parse_gdelt_articles(
        {
            "articles": [
                {
                    "id": "gdelt-native-url-less",
                    "title": "GDELT URL-less record",
                    "seendate": "20260729T120000Z",
                    "description": "Useful GDELT content is retained without a URL.",
                }
            ]
        },
        ["QQQ"],
        10,
    )
    assert len(articles) == 1
    normalized = normalize_news_article(articles[0], now=NOW)
    assert normalized["accepted"] is True
    assert normalized["canonical_url_status"] == "ABSENT_SOURCE_IDENTITY_SUFFICIENT"


@pytest.mark.asyncio
async def test_persistence_exception_is_explicit_and_not_delivered(tmp_path) -> None:
    repository = RecordingNewsRepository(
        fail_provider_record_ids={"gdelt-reject"},
    )
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_rss_enabled=False,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        market_news_repository=repository,
    )
    seen = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://gdelt.test/api").mock(
            return_value=httpx.Response(
                200,
                json={
                    "articles": [
                        {
                            "id": "gdelt-ok",
                            "title": "Persisted record",
                            "url": "https://publisher.test/ok",
                            "seendate": seen,
                            "description": "A complete persisted fixture summary.",
                        },
                        {
                            "id": "gdelt-reject",
                            "title": "Rejected record",
                            "url": "https://publisher.test/reject",
                            "seendate": seen,
                            "description": "A complete rejected fixture summary.",
                        },
                    ]
                },
            )
        )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=2,
            recency_days=14,
        )

    assert [item["provider_record_id"] for item in result.data["articles"]] == [
        "gdelt-ok"
    ]
    assert result.data["data_quality"]["persistence_status"] == "PARTIAL"
    assert result.data["data_quality"]["persistence_failed_count"] == 1
    rejected = [
        item
        for item in result.data["data_quality"]["persistence_results"]
        if item["result"] == "PERSISTENCE_REJECTED"
    ]
    assert rejected[0]["record_id"]
    assert rejected[0]["error_type"] == "RuntimeError"
    assert rejected[0]["reason_code"] == "PERSISTENCE_RUNTIMEERROR"
    assert rejected[0]["retryable"] is False
    _assert_exact_provider_accounting(result.data["provider_accounting"][0])


@pytest.mark.asyncio
async def test_metadata_enrichment_call_is_accounted_and_can_move_record_outside_scope(
    tmp_path,
) -> None:
    repository = RecordingNewsRepository()
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_rss_enabled=False,
        news_metadata_enrichment_limit_per_provider=1,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        market_news_repository=repository,
    )
    seen = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://gdelt.test/api").mock(
            return_value=httpx.Response(
                200,
                json={
                    "articles": [
                        {
                            "id": "gdelt-old-after-enrichment",
                            "title": "Timestamp recovered from article metadata",
                            "url": "https://publisher.test/old-after-enrichment",
                            "seendate": seen,
                            "description": "A complete fixture summary.",
                        }
                    ]
                },
            )
        )
        router.get("https://publisher.test/old-after-enrichment").mock(
            return_value=httpx.Response(
                200,
                text=(
                    '<html><head><meta property="article:published_time" '
                    'content="2020-01-01T12:00:00Z"></head></html>'
                ),
            )
        )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=1,
            recency_days=14,
        )

    account = result.data["provider_accounting"][0]
    assert repository.stored == []
    assert result.data["articles"] == []
    assert account["calls"] == 2
    assert account["metadata_enrichment_calls"] == 1
    assert account["metadata_enrichment_status"] == "COMPLETE"
    assert account["explicit_out_of_scope_count"] == 1
    assert (
        account["explicit_out_of_scope"][0]["reason_code"]
        == "OUTSIDE_RECENCY_WINDOW"
    )
    _assert_exact_provider_accounting(account)


@pytest.mark.asyncio
async def test_metadata_enrichment_failure_is_provider_partial_not_silent(
    tmp_path,
) -> None:
    repository = RecordingNewsRepository()
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_rss_enabled=False,
        news_metadata_enrichment_limit_per_provider=1,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        market_news_repository=repository,
    )
    seen = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://gdelt.test/api").mock(
            return_value=httpx.Response(
                200,
                json={
                    "articles": [
                        {
                            "id": "gdelt-enrichment-failed",
                            "title": "Provider-seen timestamp remains auditable",
                            "url": "https://publisher.test/enrichment-failed",
                            "seendate": seen,
                            "description": "A complete fixture summary.",
                        }
                    ]
                },
            )
        )
        router.get("https://publisher.test/enrichment-failed").mock(
            return_value=httpx.Response(503, text="unavailable")
        )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=1,
            recency_days=14,
        )

    account = result.data["provider_accounting"][0]
    assert len(result.data["articles"]) == 1
    assert account["status"] == "PARTIAL"
    assert account["coverage_status"] == "PARTIAL"
    assert account["metadata_enrichment_status"] == "PARTIAL"
    assert account["metadata_enrichment_calls"] == 1
    assert (
        account["metadata_enrichment_results"][0]["reason_code"]
        == "METADATA_ENRICHMENT_FAILED"
    )
    assert result.data["data_quality"]["readiness"] == "DEGRADED"
    _assert_exact_provider_accounting(account)


def _occurrence(**overrides) -> dict:
    row = {
        "provider_record_id": "record-1",
        "title": "Occurrence title",
        "summary": "A sufficiently complete occurrence body for identity tests.",
        "source": "Reuters",
        "original_publisher": "Reuters",
        "source_url": "https://www.reuters.com/world/us/occurrence",
        "canonical_url": "https://www.reuters.com/world/us/occurrence",
        "published_at": "2026-07-29T12:00:00Z",
        "retrieved_at": "2026-07-29T12:01:00Z",
        "provider_type": "RSS",
        "acquisition_provider": "Fixture RSS",
    }
    row.update(overrides)
    return row


def test_same_url_with_different_timestamps_remains_two_occurrences() -> None:
    context = build_news_context(
        [
            _occurrence(provider_record_id="first"),
            _occurrence(
                provider_record_id="second",
                published_at="2026-07-29T12:05:00Z",
            ),
        ],
        now=NOW,
    )
    assert len(context["articles"]) == 2


def test_same_title_with_different_content_remains_two_occurrences() -> None:
    context = build_news_context(
        [
            _occurrence(provider_record_id="first"),
            _occurrence(
                provider_record_id="second",
                summary="A different complete body proves this is another occurrence.",
            ),
        ],
        now=NOW,
    )
    assert len(context["articles"]) == 2


def test_same_title_and_url_with_different_editorial_update_remains_two() -> None:
    context = build_news_context(
        [
            _occurrence(editorial_updated_at="2026-07-29T12:02:00Z"),
            _occurrence(editorial_updated_at="2026-07-29T12:03:00Z"),
        ],
        now=NOW,
    )
    assert len(context["articles"]) == 2
    assert len({item["canonical_news_id"] for item in context["articles"]}) == 2


def test_exact_syndication_consolidates_editorial_copy_and_keeps_lineage() -> None:
    first = _occurrence(
        provider_record_id="yahoo-copy",
        source_url="https://finance.yahoo.com/news/exact-copy",
        canonical_url=None,
        distribution_source="Yahoo Finance",
        acquisition_provider="Yahoo Finance RSS",
    )
    second = _occurrence(
        provider_record_id="msn-copy",
        source_url="https://www.msn.com/en-us/money/exact-copy",
        canonical_url=None,
        distribution_source="MSN",
        acquisition_provider="MSN RSS",
    )
    context = build_news_context([first, second], now=NOW)
    assert len(context["articles"]) == 1
    article = context["articles"][0]
    assert article["source_record_count"] == 2
    assert {
        item["distribution_source"] for item in article["source_occurrences"]
    } == {"Yahoo Finance", "MSN"}


def test_exact_technical_duplicate_only_is_consolidated() -> None:
    record = _occurrence()
    context = build_news_context([record, dict(record)], now=NOW)
    assert len(context["articles"]) == 1
    assert len(context["duplicates"]) == 1
    assert (
        context["duplicates"][0]["duplicate_classification"]
        == "TECHNICAL_ACQUISITION_RETRY"
    )


def test_article_key_is_not_url_or_title_only() -> None:
    base = _occurrence()
    assert _article_key(base) != _article_key(
        {**base, "published_at": "2026-07-29T12:05:00Z"}
    )
    assert _article_key(base) != _article_key(
        {**base, "summary": "A different body with the same title and URL."}
    )
    assert _article_key(base) != _article_key(
        {**base, "editorial_updated_at": "2026-07-29T12:03:00Z"}
    )


def test_provider_errors_redact_query_credentials() -> None:
    message = _redact_provider_error(
        "401 https://alpha.test/query?apikey=secret-value&limit=1 "
        "Authorization: Bearer secret-token"
    )
    assert "secret-value" not in message
    assert "secret-token" not in message
    assert "apikey=REDACTED" in message
    assert "Bearer REDACTED" in message


def test_repository_persists_distinct_temporal_and_content_occurrences(
    tmp_path,
) -> None:
    repository = MarketNewsRepository(
        _settings(
            tmp_path,
            alpha_vantage_api_key="",
            news_gdelt_enabled=False,
            news_rss_enabled=False,
        ),
        clock=lambda: NOW,
    )
    records = [
        _occurrence(provider_record_id="timestamp-first"),
        _occurrence(
            provider_record_id="timestamp-second",
            published_at="2026-07-29T12:05:00Z",
        ),
        _occurrence(provider_record_id="content-first"),
        _occurrence(
            provider_record_id="content-second",
            summary="A materially different body remains a separate occurrence.",
        ),
        _occurrence(
            provider_record_id="updated-record",
            editorial_updated_at="2026-07-29T12:02:00Z",
        ),
        _occurrence(
            provider_record_id="updated-record",
            editorial_updated_at="2026-07-29T12:03:00Z",
        ),
    ]
    written = [repository.upsert_news(record) for record in records]
    assert len({item["news_key"] for item in written}) == len(records)
    assert len(repository.stored(days=1, include_quarantined=True)) == len(records)


def test_repository_persists_url_less_content_record(tmp_path) -> None:
    repository = MarketNewsRepository(
        _settings(
            tmp_path,
            alpha_vantage_api_key="",
            news_gdelt_enabled=False,
            news_rss_enabled=False,
        ),
        clock=lambda: NOW,
    )
    stored = repository.upsert_news(
        {
            "provider_record_id": "url-less-content-record",
            "title": "",
            "summary": "Content and native identity make this URL-less record useful.",
            "source": "Fixture Publisher",
            "source_url": None,
            "published_at": "2026-07-29T12:00:00Z",
            "provider_type": "RSS",
            "acquisition_provider": "Fixture RSS",
        }
    )
    assert stored["source_url"] == ""
    rows = repository.stored(days=1, include_quarantined=True)
    assert len(rows) == 1
    assert rows[0]["source_identity_status"] == "PROVIDER_NATIVE"
    assert rows[0]["content_availability_status"] == "SUMMARY_ONLY"
