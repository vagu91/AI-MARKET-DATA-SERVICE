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
    parse_alpha_vantage_news_with_accounting,
    parse_gdelt_articles,
    parse_gdelt_articles_with_accounting,
    parse_rss_articles,
    parse_rss_articles_with_accounting,
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


def _yahoo_rss(record_id: str, article_url: str) -> str:
    published = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")
    return f"""<rss><channel><item>
      <guid>{record_id}</guid>
      <title>Reuters reports a material Nasdaq update</title>
      <link>{article_url}</link>
      <pubDate>{published}</pubDate>
      <source>Reuters</source>
    </item></channel></rss>"""


def _assert_exact_provider_accounting(account: dict) -> None:
    raw = set(account["raw_record_ids"])
    captured = {
        item["record_id"] for item in account["raw_capture"]
    }
    parsed = set(account["parsed_record_ids"])
    persisted = set(account["persisted_record_ids"])
    rejected = {
        item["record_id"] for item in account["technical_rejections"]
    }
    outside = {
        item["record_id"] for item in account["explicit_out_of_scope"]
    }
    persistence_rejected = {
        item["record_id"] for item in account["persistence_rejections"]
    }
    duplicates = {
        item["record_id"]
        for item in account["exact_technical_duplicates"]
    }
    assert captured == raw
    assert raw == parsed | rejected
    assert parsed.isdisjoint(rejected)
    assert parsed == persisted | persistence_rejected | outside | duplicates
    parsed_partitions = (
        persisted,
        persistence_rejected,
        outside,
        duplicates,
    )
    assert all(
        left.isdisjoint(right)
        for index, left in enumerate(parsed_partitions)
        for right in parsed_partitions[index + 1 :]
    )
    for partition in (
        account["technical_rejections"],
        account["persistence_rejections"],
        account["explicit_out_of_scope"],
        account["exact_technical_duplicates"],
    ):
        assert all(item.get("record_id") for item in partition)
        assert all(item.get("reason_code") for item in partition)
    assert account["partition_entries_missing_identity_or_reason"] == []
    assert account["raw_capture_contract_violations"] == []
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
    assert result.data["data_quality"]["readiness"] == "DEGRADED"
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


@pytest.mark.asyncio
async def test_gdelt_timeout_preserves_all_six_primary_feed_records(
    tmp_path,
) -> None:
    repository = RecordingNewsRepository()
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_gdelt_max_attempts=3,
        news_gdelt_retry_backoff_seconds=0,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        market_news_repository=repository,
    )
    rss_urls = {
        "https://fed.test/rss": ("Federal Reserve RSS", "fed-timeout-1"),
        "https://bls.test/rss": ("BLS RSS", "bls-timeout-1"),
        "https://bea.test/rss": ("BEA RSS", "bea-timeout-1"),
        "https://yahoo.test/rss": ("Yahoo Finance RSS", "yahoo-timeout-1"),
        "https://marketwatch.test/rss": (
            "MarketWatch RSS",
            "marketwatch-timeout-1",
        ),
        "https://google.test/rss": ("Google News RSS", "google-timeout-1"),
    }
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        gdelt = router.get("https://gdelt.test/api").mock(
            side_effect=httpx.ConnectTimeout("offline fixture timeout")
        )
        for url, (name, record_id) in rss_urls.items():
            router.get(url).mock(
                return_value=httpx.Response(200, text=_rss(name, record_id))
            )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=25,
            recency_days=14,
        )

    accounts = {
        account["provider"]: account
        for account in result.data["provider_accounting"]
    }
    gdelt_account = accounts["GDELT Doc API"]
    assert gdelt.call_count == 3
    assert gdelt_account["status"] == "TEMPORARILY_UNAVAILABLE"
    assert gdelt_account["coverage_status"] == "FAILED"
    assert gdelt_account["reason_code"] == (
        "GDELT_DOC_API_CONNECT_TIMEOUT_RETRY_EXHAUSTED"
    )
    assert gdelt_account["accounting_valid"] is True
    assert len(repository.stored) == 6
    assert len(result.data["articles"]) == 6
    assert {
        article["acquisition_provider"]
        for article in result.data["articles"]
    } == {name for name, _ in rss_urls.values()}
    assert result.data["data_quality"][
        "provider_temporarily_unavailable_count"
    ] == 1
    assert result.data["data_quality"]["provider_failure_count"] == 0
    assert result.data["data_quality"]["provider_accounting_valid"] is True
    assert result.data["data_quality"]["readiness"] == "DEGRADED"
    aggregate = result.data["data_quality"][
        "provider_accounting_aggregate"
    ]
    assert aggregate["valid"] is True
    assert aggregate["raw_count"] == 6
    assert aggregate["persisted_count"] == 6
    assert aggregate["technically_rejected_count"] == 0
    assert aggregate["explicit_out_of_scope_count"] == 0
    assert aggregate["cross_provider_identity_collision_ids"] == []
    for name, _ in rss_urls.values():
        _assert_exact_provider_accounting(accounts[name])


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


@pytest.mark.parametrize(
    ("received_count", "configured_limit"),
    [(11, 10), (26, 25), (101, 100)],
)
def test_rss_processes_every_received_item_without_post_fetch_cap(
    received_count: int,
    configured_limit: int,
) -> None:
    items = "".join(
        f"""
        <item>
          <guid>rss-{index}</guid>
          <title>RSS record {index}</title>
          <link>https://publisher.test/rss-{index}</link>
          <pubDate>Wed, 29 Jul 2026 12:00:00 GMT</pubDate>
          <source>Fixture Publisher</source>
          <description>Complete fixture content {index}.</description>
        </item>
        """
        for index in range(received_count)
    )
    articles, _, rejected, outside, raw_ids = (
        parse_rss_articles_with_accounting(
            f"<rss><channel>{items}</channel></rss>",
            symbols=["QQQ"],
            limit=configured_limit,
            source_name="Fixture RSS",
            reliability=0.64,
        )
    )
    assert len(raw_ids) == received_count
    assert len(articles) == received_count
    assert rejected == []
    assert outside == []


@pytest.mark.parametrize(
    ("received_count", "configured_limit"),
    [(11, 10), (26, 25), (101, 100)],
)
def test_api_parsers_process_every_record_already_returned(
    received_count: int,
    configured_limit: int,
) -> None:
    gdelt_payload = {
        "articles": [
            {
                "id": f"gdelt-{index}",
                "title": f"GDELT record {index}",
                "url": f"https://publisher.test/gdelt-{index}",
                "seendate": "20260729T120000Z",
                "description": f"Complete GDELT fixture content {index}.",
            }
            for index in range(received_count)
        ]
    }
    gdelt, gdelt_rejected, gdelt_outside, gdelt_raw = (
        parse_gdelt_articles_with_accounting(
            gdelt_payload,
            ["QQQ"],
            configured_limit,
        )
    )
    alpha_payload = {
        "feed": [
            {
                "id": f"alpha-{index}",
                "title": f"Alpha record {index}",
                "url": f"https://publisher.test/alpha-{index}",
                "time_published": "20260729T120000",
                "source": "Fixture Publisher",
                "summary": f"Complete Alpha fixture content {index}.",
            }
            for index in range(received_count)
        ]
    }
    alpha, alpha_rejected, alpha_outside, alpha_raw = (
        parse_alpha_vantage_news_with_accounting(
            alpha_payload,
            ["QQQ"],
            configured_limit,
        )
    )
    assert len(gdelt_raw) == len(gdelt) == received_count
    assert len(alpha_raw) == len(alpha) == received_count
    assert gdelt_rejected == alpha_rejected == []
    assert gdelt_outside == alpha_outside == []


def test_marketwatch_direct_feed_fallback_is_verified_publisher() -> None:
    rss = """<rss><channel><item>
      <guid>WP-MKTW-verified</guid>
      <title>MarketWatch direct-host record</title>
      <link>https://www.marketwatch.com/story/direct-host-record</link>
      <pubDate>Wed, 29 Jul 2026 12:00:00 GMT</pubDate>
      <description>Direct feed content remains valid without a source tag.</description>
    </item></channel></rss>"""
    articles, warnings = parse_rss_articles(
        rss,
        symbols=["QQQ"],
        limit=1,
        source_name="MarketWatch RSS",
        reliability=0.56,
    )
    assert warnings == []
    assert articles[0]["original_publisher"] == "MarketWatch"
    normalized = normalize_news_article(articles[0], now=NOW)
    assert normalized["accepted"] is True
    assert normalized["publisher_status"] == "VERIFIED"
    assert normalized["lineage_status"] == "VERIFIED"


@pytest.mark.parametrize(
    ("source_name", "publisher", "url"),
    [
        (
            "Federal Reserve RSS",
            "Federal Reserve",
            "https://www.federalreserve.gov/newsevents/pressreleases/test.htm",
        ),
        (
            "BLS RSS",
            "U.S. Bureau of Labor Statistics",
            "https://www.bls.gov/news.release/test.htm",
        ),
        (
            "BEA RSS",
            "U.S. Bureau of Economic Analysis",
            "https://www.bea.gov/news/test",
        ),
    ],
)
def test_official_direct_feed_fallback_matches_source_policy(
    source_name: str,
    publisher: str,
    url: str,
) -> None:
    rss = f"""<rss><channel><item>
      <guid>{source_name}-verified</guid>
      <title>Official direct-host record</title>
      <link>{url}</link>
      <pubDate>Wed, 29 Jul 2026 12:00:00 GMT</pubDate>
      <description>Official feed content without a source tag.</description>
    </item></channel></rss>"""
    articles, _ = parse_rss_articles(
        rss,
        symbols=["QQQ"],
        limit=1,
        source_name=source_name,
        reliability=0.86,
    )
    assert articles[0]["original_publisher"] == publisher
    normalized = normalize_news_article(articles[0], now=NOW)
    assert normalized["accepted"] is True
    assert normalized["publisher_status"] == "VERIFIED"
    assert normalized["lineage_status"] == "VERIFIED"


def test_aggregator_feed_without_publisher_does_not_invent_one() -> None:
    rss = """<rss><channel><item>
      <guid>aggregator-unknown-publisher</guid>
      <title>Publisher remains unknown</title>
      <link>https://finance.yahoo.com/news/unknown-publisher</link>
      <pubDate>Wed, 29 Jul 2026 12:00:00 GMT</pubDate>
      <description>Useful content remains deliverable with degraded lineage.</description>
    </item></channel></rss>"""
    articles, _ = parse_rss_articles(
        rss,
        symbols=["QQQ"],
        limit=1,
        source_name="Yahoo Finance RSS",
        reliability=0.58,
    )
    assert articles[0]["original_publisher"] is None
    normalized = normalize_news_article(articles[0], now=NOW)
    assert normalized["accepted"] is True
    assert normalized["publisher"] is None
    assert normalized["original_publisher"] is None
    assert normalized["publisher_status"] == "UNKNOWN"
    assert normalized["distribution_source"] == "Yahoo Finance"


@pytest.mark.asyncio
async def test_rss_fan_in_persists_all_items_in_received_document(
    tmp_path,
) -> None:
    repository = RecordingNewsRepository()
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_gdelt_enabled=False,
        federal_reserve_rss_url="",
        bls_rss_url="",
        bea_rss_url="",
        marketwatch_rss_url="",
        google_news_rss_url="",
        news_rss_limit_per_feed=25,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        market_news_repository=repository,
    )
    published = datetime.now(UTC).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    items = "".join(
        f"""
        <item>
          <guid>yahoo-{index}</guid>
          <title>Yahoo received record {index}</title>
          <link>https://finance.yahoo.com/news/record-{index}</link>
          <pubDate>{published}</pubDate>
          <source>Fixture Publisher</source>
          <description>Complete received fixture record {index}.</description>
        </item>
        """
        for index in range(101)
    )
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://yahoo.test/rss").mock(
            return_value=httpx.Response(
                200,
                text=f"<rss><channel>{items}</channel></rss>",
            )
        )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=25,
            recency_days=14,
        )
    account = result.data["provider_accounting"][0]
    assert account["raw_count"] == 101
    assert account["parsed_count"] == 101
    assert account["persisted_count"] == 101
    assert account["explicit_out_of_scope"] == []
    assert account["post_fetch_limit_applied"] is False
    assert account["received_records_fully_processed"] is True
    assert len(repository.stored) == len(result.data["articles"]) == 101
    _assert_exact_provider_accounting(account)


@pytest.mark.asyncio
async def test_provider_page_limit_is_partial_coverage_not_fake_complete(
    tmp_path,
) -> None:
    repository = RecordingNewsRepository()
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_rss_enabled=False,
        news_gdelt_limit=25,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        market_news_repository=repository,
    )
    seen = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    payload = {
        "articles": [
            {
                "id": f"gdelt-{index}",
                "title": f"GDELT page record {index}",
                "url": f"https://publisher.test/gdelt-page-{index}",
                "seendate": seen,
                "description": f"Complete page fixture content {index}.",
            }
            for index in range(25)
        ]
    }
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://gdelt.test/api").mock(
            return_value=httpx.Response(200, json=payload)
        )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=25,
            recency_days=14,
        )
    account = result.data["provider_accounting"][0]
    assert account["raw_count"] == account["persisted_count"] == 25
    assert account["coverage_status"] == "PARTIAL"
    assert account["pagination_supported"] is True
    assert account["pagination_complete"] is False
    assert account["next_cursor"] == {"startrecord": 26}
    assert account["records_not_acquired"] == "UNKNOWN"
    assert account["coverage_reason"] == "PROVIDER_PAGE_LIMIT_REACHED"
    _assert_exact_provider_accounting(account)


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
async def test_metadata_enrichment_failure_does_not_replace_main_coverage(
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
    assert account["coverage_reason"] == "PROVIDER_PAGE_LIMIT_REACHED"
    assert account["metadata_enrichment_required"] is False
    assert account["metadata_enrichment_status"] == "PARTIAL"
    assert account["metadata_enrichment_calls"] == 1
    assert (
        account["metadata_enrichment_results"][0]["reason_code"]
        == "METADATA_ENRICHMENT_FAILED"
    )
    assert result.data["data_quality"]["readiness"] == "DEGRADED"
    _assert_exact_provider_accounting(account)


def _yahoo_only_settings(tmp_path) -> Settings:
    return _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_gdelt_enabled=False,
        federal_reserve_rss_url="",
        bls_rss_url="",
        bea_rss_url="",
        marketwatch_rss_url="",
        google_news_rss_url="",
        news_metadata_enrichment_limit_per_provider=1,
    )


@pytest.mark.asyncio
async def test_yahoo_rss_200_and_allowlisted_307_delivers_record(
    tmp_path,
) -> None:
    repository = RecordingNewsRepository()
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        _yahoo_only_settings(tmp_path),
        market_news_repository=repository,
    )
    original = "https://finance.yahoo.com/technology/articles/rss-307.html"
    redirected = "https://finance.yahoo.com/news/rss-307.html"
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://yahoo.test/rss").mock(
            return_value=httpx.Response(
                200,
                text=_yahoo_rss("yahoo-main-307", original),
            )
        )
        router.get(original).mock(
            return_value=httpx.Response(
                307,
                headers={"Location": redirected},
            )
        )
        router.get(redirected).mock(
            return_value=httpx.Response(
                200,
                text=(
                    '<html><head><meta name="description" '
                    'content="Preserved metadata summary."></head></html>'
                ),
            )
        )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=25,
            recency_days=14,
        )

    account = result.data["provider_accounting"][0]
    assert len(repository.stored) == 1
    assert len(result.data["articles"]) == 1
    assert result.data["articles"][0]["source_url"] == original
    assert account["status"] == "COMPLETE"
    assert account["coverage_status"] == "COMPLETE"
    assert account["metadata_enrichment_status"] == "COMPLETE"
    assert account["metadata_enrichment_required"] is False
    _assert_exact_provider_accounting(account)


@pytest.mark.asyncio
async def test_yahoo_rss_200_and_enrichment_timeout_delivers_original(
    tmp_path,
) -> None:
    repository = RecordingNewsRepository()
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        _yahoo_only_settings(tmp_path),
        market_news_repository=repository,
    )
    original = "https://finance.yahoo.com/technology/articles/rss-timeout.html"
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get("https://yahoo.test/rss").mock(
            return_value=httpx.Response(
                200,
                text=_yahoo_rss("yahoo-main-timeout", original),
            )
        )
        router.get(original).mock(
            side_effect=httpx.ConnectTimeout("offline metadata timeout")
        )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=25,
            recency_days=14,
        )

    account = result.data["provider_accounting"][0]
    assert len(repository.stored) == 1
    assert len(result.data["articles"]) == 1
    assert result.data["articles"][0]["source_url"] == original
    assert account["status"] == "COMPLETE"
    assert account["coverage_status"] == "COMPLETE"
    assert account["metadata_enrichment_status"] == "PARTIAL"
    assert account["metadata_enrichment_results"][0]["reason_code"] == (
        "METADATA_ENRICHMENT_TIMEOUT"
    )
    assert result.data["data_quality"]["readiness"] == "DEGRADED"
    _assert_exact_provider_accounting(account)


@pytest.mark.asyncio
async def test_yahoo_nonallowlisted_307_is_not_called_and_original_is_delivered(
    tmp_path,
) -> None:
    repository = RecordingNewsRepository()
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        _yahoo_only_settings(tmp_path),
        market_news_repository=repository,
    )
    original = "https://finance.yahoo.com/technology/articles/rss-blocked.html"
    blocked = "https://example.com/not-allowlisted"
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        feed_route = router.get("https://yahoo.test/rss").mock(
            return_value=httpx.Response(
                200,
                text=_yahoo_rss("yahoo-main-blocked", original),
            )
        )
        metadata_route = router.get(original).mock(
            return_value=httpx.Response(
                307,
                headers={"Location": blocked},
            )
        )
        result = await provider.fetch_for_symbols(
            ["QQQ"],
            limit=25,
            recency_days=14,
        )

    account = result.data["provider_accounting"][0]
    assert feed_route.call_count == 1
    assert metadata_route.call_count == 1
    assert len(repository.stored) == 1
    assert len(result.data["articles"]) == 1
    assert result.data["articles"][0]["source_url"] == original
    assert account["status"] == "COMPLETE"
    assert account["coverage_status"] == "COMPLETE"
    assert account["metadata_enrichment_status"] == "PARTIAL"
    assert account["metadata_enrichment_results"][0]["reason_code"] == (
        "METADATA_REDIRECT_NOT_ALLOWLISTED"
    )
    _assert_exact_provider_accounting(account)


@pytest.mark.asyncio
async def test_yahoo_metadata_307_follows_only_allowlisted_exact_lineage(
    tmp_path,
) -> None:
    class Observer:
        def __init__(self) -> None:
            self.redirects: list[dict] = []

        def register_metadata_redirect(self, **payload) -> None:
            self.redirects.append(payload)

    observer = Observer()
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key="",
        news_gdelt_enabled=False,
        news_rss_enabled=False,
        news_metadata_enrichment_limit_per_provider=1,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
        network_observer=observer,
    )
    original = "https://finance.yahoo.com/technology/articles/fixture.html"
    redirected = "https://finance.yahoo.com/news/fixture.html"
    article = {
        "acquisition_provider": "Yahoo Finance RSS",
        "source_url": original,
        "raw_record_id": "raw:yahoo-307",
        "published_at": None,
        "summary": "Preserved provider summary.",
    }
    batch = {
        "provider": "Yahoo Finance RSS",
        "calls": 1,
        "status": "COMPLETE",
        "coverage_status": "COMPLETE",
        "metadata_enrichment_calls": 0,
        "metadata_enrichment_status": "NOT_REQUIRED",
        "metadata_enrichment_results": [],
        "warnings": [],
    }

    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.get(original).mock(
            return_value=httpx.Response(
                307,
                headers={"Location": redirected},
            )
        )
        router.get(redirected).mock(
            return_value=httpx.Response(
                200,
                text=(
                    '<html><head><meta property="article:published_time" '
                    'content="2026-07-29T12:00:00Z"></head></html>'
                ),
            )
        )
        async with httpx.AsyncClient() as client:
            enriched = await provider._enrich_missing_metadata(
                client,
                [article],
                batches=[batch],
            )

    assert enriched[0]["published_at"] == "2026-07-29T12:00:00+00:00"
    assert batch["calls"] == 2
    assert batch["metadata_enrichment_calls"] == 1
    assert batch["metadata_enrichment_status"] == "COMPLETE"
    assert batch["metadata_enrichment_results"] == [
        {
            "record_id": "raw:yahoo-307",
            "status": "COMPLETE",
            "reason_code": None,
            "redirect_count": 1,
            "final_url": redirected,
        }
    ]
    assert observer.redirects == [
        {
            "provider": "Yahoo Finance RSS",
            "record_id": "raw:yahoo-307",
            "original_url": original,
            "redirect_url": redirected,
        }
    ]


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
