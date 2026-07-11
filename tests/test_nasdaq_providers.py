import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.core.config import Settings
from app.models.common import Freshness, ProviderMetadata, ProviderResult, ProviderType
from app.providers.earnings_provider import EarningsProvider
from app.providers.earnings_provider import parse_yahoo_earnings
from app.providers.earnings_provider import parse_alpha_vantage_earnings_calendar
from app.providers.mega_cap_snapshot_provider import (
    MEGA_CAP_TICKERS,
    MegaCapSnapshotProvider,
    parse_alpha_vantage_global_quote,
    parse_stooq_quotes,
    parse_yahoo_chart,
)
from app.providers.news_provider import NewsProvider
from app.providers.news_provider import (
    filter_recent_articles,
    parse_alpha_vantage_news,
    parse_rss_articles,
    relevance,
    tag_topics,
)
from app.providers.qqq_holdings_provider import (
    QQQHoldingsProvider,
    is_alpha_vantage_daily_rate_limited,
    parse_alpha_vantage_etf_profile,
    parse_invesco_holdings_csv,
    parse_nasdaq_constituents,
)


def test_parse_invesco_holdings_csv_fixture() -> None:
    csv_text = """Fund holdings as of,2026-07-08
Ticker,Name,Weight (%),Sector
NVDA,NVIDIA Corp,8.7,Technology
GOOGL,Alphabet Inc Class A,4.1,Communication Services
GOOG,Alphabet Inc Class C,,Communication Services
"""

    holdings, as_of, errors = parse_invesco_holdings_csv(csv_text)

    assert as_of == "2026-07-08"
    assert [item.symbol for item in holdings] == ["NVDA", "GOOGL", "GOOG"]
    assert holdings[0].weight == 8.7
    assert holdings[2].weight is None
    assert errors == []


def test_parse_alpha_vantage_etf_profile_fixture() -> None:
    payload = {
        "holdings": [
            {"symbol": "NVDA", "description": "NVIDIA Corp", "weight": "8.7%", "sector": "Technology"},
            {"symbol": "GOOGL", "description": "Alphabet Inc", "weight": "0.041"},
        ]
    }

    holdings, errors = parse_alpha_vantage_etf_profile(payload)

    assert holdings[0].symbol == "NVDA"
    assert holdings[0].weight == 8.7
    assert holdings[1].weight == 4.1
    assert errors == ["Alpha Vantage ETF_PROFILE missing sector for 1 holdings"]


def test_alpha_vantage_daily_rate_limit_payload_detected() -> None:
    payload = {
        "Information": (
            "Thank you for using Alpha Vantage! Our standard API rate limit is "
            "25 requests per day. Please visit https://www.alphavantage.co/premium/"
        )
    }

    assert is_alpha_vantage_daily_rate_limited(payload) is True


@pytest.mark.asyncio
async def test_qqq_equal_weight_proxy_is_runtime_only_after_upstream_failures(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALPHA_VANTAGE_API_KEY=test-key\n", encoding="utf-8")
    settings = Settings(
        _env_file=env_file,
        alpha_vantage_base_url="https://alpha.test/query",
        invesco_qqq_holdings_url="https://invesco.test/qqq.csv",
        nasdaq_100_constituents_url="https://nasdaq.test/constituents",
    )
    provider = QQQHoldingsProvider(ProviderCacheRepository(tmp_path / "cache.sqlite3"), settings)
    nasdaq_payload = {"data": {"rows": [{"symbol": "MSFT", "companyName": "Microsoft", "sector": "Technology"}]}}

    with respx.mock(assert_all_mocked=True, assert_all_called=False) as router:
        invesco = router.get("https://invesco.test/qqq.csv").mock(return_value=httpx.Response(403, text="Forbidden"))
        alpha = router.get("https://alpha.test/query").mock(
            return_value=httpx.Response(
                200,
                json={
                    "Information": (
                        "Thank you for using Alpha Vantage. Our standard API rate limit is "
                        "25 requests per day."
                    )
                },
            )
        )
        nasdaq = router.get("https://nasdaq.test/constituents").mock(return_value=httpx.Response(200, json=nasdaq_payload))
        result = await provider.fetch_safe()
        second = await provider.fetch_safe()

    quality = result.data["data_quality"]
    assert invesco.call_count == 2
    assert alpha.call_count == 1
    assert nasdaq.call_count == 2
    assert second.metadata.provider_type == "API"
    assert second.data["data_quality"]["actual_network_calls"] == 2
    assert result.data["status"] == "proxy"
    assert result.data["is_proxy"] is True
    assert result.data["official_etf_holdings"] is False
    assert result.data["weight_data_available"] is True
    assert result.data["weight_method"] == "equal_weight_proxy"
    assert result.data["holdings"][0]["weight"] == 100.0
    assert quality["invesco_status"] == "access_restricted"
    assert quality["invesco_http_status"] == 403
    assert quality["alpha_vantage_status"] == "rate_limited"
    assert quality["alpha_vantage_rate_limited"] is True
    assert quality["nasdaq_proxy_used"] is True
    assert len(quality["warnings"]) == 1


@pytest.mark.asyncio
async def test_qqq_holdings_no_retry_when_alpha_negative_cache_is_open(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALPHA_VANTAGE_API_KEY=test-key\n", encoding="utf-8")
    settings = Settings(
        _env_file=env_file,
        alpha_vantage_base_url="https://alpha.test/query",
        invesco_qqq_holdings_url="https://invesco.test/qqq.csv",
        nasdaq_100_constituents_url="https://nasdaq.test/constituents",
    )
    cache = ProviderCacheRepository(tmp_path / "cache.sqlite3")
    provider = QQQHoldingsProvider(cache, settings)
    cache.set(
        provider.alpha_negative_cache_key,
        {
            "status": "rate_limited",
            "negative_cache_reason": "provider_daily_rate_limit",
            "next_retry_at": (datetime.now(UTC) + timedelta(hours=2)).isoformat(),
        },
    )

    with respx.mock(assert_all_mocked=True, assert_all_called=False) as router:
        router.get("https://invesco.test/qqq.csv").mock(return_value=httpx.Response(403, text="Forbidden"))
        alpha = router.get("https://alpha.test/query").mock(return_value=httpx.Response(500, text="should not call"))
        router.get("https://nasdaq.test/constituents").mock(
            return_value=httpx.Response(200, json={"data": {"rows": [{"symbol": "AAPL", "companyName": "Apple"}]}})
        )
        result = await provider.fetch()

    assert alpha.call_count == 0
    assert result.data["data_quality"]["alpha_vantage_status"] == "rate_limited"
    assert "alpha_vantage_negative_cache" in result.data["data_quality"]["provider_attempts"]


@pytest.mark.asyncio
async def test_qqq_holdings_last_known_good_preserved_when_upstreams_fail(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        alpha_vantage_api_key=None,
        invesco_qqq_holdings_url="https://invesco.test/qqq.csv",
        nasdaq_100_constituents_url="https://nasdaq.test/constituents",
        qqq_holdings_ttl_hours=0,
        qqq_holdings_stale_tolerance_hours=24,
    )
    provider = QQQHoldingsProvider(ProviderCacheRepository(tmp_path / "cache.sqlite3"), settings)
    cached = ProviderResult(
        metadata=ProviderMetadata(
            source="Invesco QQQ Holdings",
            provider_type=ProviderType.CSV,
            retrieved_at=datetime.now(UTC),
            freshness=Freshness.RECENT,
            reliability=0.88,
        ),
        data={
            "as_of": "2026-07-08",
            "holdings": [{"symbol": "NVDA", "name": "NVIDIA", "weight": 8.7, "sector": "Technology"}],
            "data_quality": {"count": 1, "holdings_count": 1, "missing_weights": False, "final_data_available": True},
        },
    )
    provider.cache.set(provider.cache_key, cached.model_dump(mode="json"))

    with respx.mock(assert_all_mocked=True) as router:
        router.get("https://invesco.test/qqq.csv").mock(return_value=httpx.Response(403, text="Forbidden"))
        router.get("https://nasdaq.test/constituents").mock(return_value=httpx.Response(500, text="down"))
        result = await provider.fetch_safe()

    assert result.data["holdings"][0]["symbol"] == "NVDA"
    assert result.data["data_quality"]["final_status"] == "stale_acceptable"
    assert result.data["data_quality"]["last_known_good_used"] is True

    stored = provider.cache.get(provider.cache_key)
    assert stored["data"]["holdings"][0]["symbol"] == "NVDA"


def test_parse_nasdaq_constituents_html_fixture() -> None:
    html = """
    <table>
      <tr><th>Symbol</th><th>Company Name</th><th>Sector</th></tr>
      <tr><td>MSFT</td><td>Microsoft Corp</td><td>Technology</td></tr>
      <tr><td>AMZN</td><td>Amazon.com Inc</td><td>Consumer Discretionary</td></tr>
    </table>
    """

    holdings = parse_nasdaq_constituents(html)

    assert [item.symbol for item in holdings] == ["MSFT", "AMZN"]
    assert holdings[0].weight is None
    assert holdings[1].sector == "Consumer Discretionary"


def test_parse_nasdaq_constituents_accepts_data_data_list() -> None:
    payload = {
        "data": {
            "data": [
                {"symbol": "MSFT", "companyName": "Microsoft Corp", "sector": "Technology"},
                {"symbol": "AAPL", "companyName": "Apple Inc", "sector": "Technology"},
            ]
        }
    }

    holdings = parse_nasdaq_constituents(json.dumps(payload))

    assert [item.symbol for item in holdings] == ["MSFT", "AAPL"]
    assert holdings[0].weight is None


def test_parse_yahoo_earnings_fixture() -> None:
    event_ts = int((datetime.now(UTC) + timedelta(days=7)).timestamp())
    payload = {
        "quoteSummary": {
            "result": [
                {
                    "calendarEvents": {
                        "earnings": {
                            "earningsDate": [{"raw": event_ts}],
                            "earningsAverage": {"raw": 3.21},
                            "revenueAverage": {"raw": 123456789.0},
                        }
                    }
                }
            ]
        }
    }

    event = parse_yahoo_earnings("MSFT", payload, datetime.now(UTC))

    assert event is not None
    assert event["symbol"] == "MSFT"
    assert event["eps_estimate"] == 3.21
    assert event["revenue_estimate"] == 123456789.0
    assert event["event_risk_level"] == "HIGH"


def test_parse_alpha_vantage_global_quote_fixture() -> None:
    payload = {
        "Global Quote": {
            "01. symbol": "NVDA",
            "05. price": "123.45",
            "06. volume": "123456",
            "09. change": "1.23",
            "10. change percent": "0.84%",
        }
    }

    stock = parse_alpha_vantage_global_quote("NVDA", payload)

    assert stock is not None
    assert stock["last_price"] == 123.45
    assert stock["change_pct"] == 0.84
    assert stock["market_session"] == "UNKNOWN"


def test_parse_stooq_quotes_batch_fixture() -> None:
    csv_text = """Symbol,Date,Time,Open,High,Low,Close,Volume
nvda.us,2026-07-09,22:00:00,100,103,99,102,1000000
aapl.us,2026-07-09,22:00:00,200,202,198,199,2000000
msft.us,2026-07-09,22:00:00,300,303,299,303,3000000
"""

    stocks, errors = parse_stooq_quotes(csv_text)

    assert [stock["symbol"] for stock in stocks] == ["NVDA", "AAPL", "MSFT"]
    assert stocks[0]["last_price"] == 102.0
    assert stocks[0]["change"] == 2.0
    assert stocks[0]["change_pct"] == 2.0
    assert errors


def test_parse_yahoo_chart_fixture() -> None:
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 102.0,
                        "chartPreviousClose": 100.0,
                        "regularMarketVolume": 12345,
                        "currency": "USD",
                    },
                    "indicators": {"quote": [{"close": [99.0, 100.0, 102.0], "volume": [1, 2, 3]}]},
                }
            ]
        }
    }

    stock = parse_yahoo_chart("NVDA", payload)

    assert stock is not None
    assert stock["symbol"] == "NVDA"
    assert stock["change"] == 2.0
    assert stock["change_pct"] == 2.0


def test_parse_alpha_vantage_earnings_calendar_csv_fixture() -> None:
    csv_text = """symbol,name,reportDate,fiscalDateEnding,estimate,currency
MSFT,Microsoft Corp,2099-07-29,2099-06-30,3.21,USD
IBM,International Business Machines,2099-07-30,2099-06-30,2.00,USD
"""

    events = parse_alpha_vantage_earnings_calendar(csv_text, datetime.now(UTC))

    assert len(events) == 1
    assert events[0]["symbol"] == "MSFT"
    assert events[0]["eps_estimate"] == 3.21
    assert events[0]["timing"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_earnings_no_events_does_not_call_yahoo_fallback(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALPHA_VANTAGE_API_KEY=test-key\n", encoding="utf-8")
    settings = Settings(
        _env_file=env_file,
        alpha_vantage_base_url="https://alpha.test/query",
        yahoo_quote_summary_url="https://yahoo.test/v10/finance/quoteSummary",
    )
    provider = EarningsProvider(ProviderCacheRepository(tmp_path / "cache.sqlite3"), settings)
    csv_text = "symbol,name,reportDate,fiscalDateEnding,estimate,currency\nIBM,IBM,2099-07-30,2099-06-30,2.00,USD\n"

    with respx.mock(base_url="https://alpha.test", assert_all_mocked=True) as router:
        router.get("/query").mock(return_value=httpx.Response(200, text=csv_text))
        result = await provider.fetch()

    assert result.data["events"] == []
    assert result.data["data_quality"]["errors"] == ["No watchlist earnings found in requested window"]
    assert result.data["data_quality"]["fallback_used"] is False


def test_parse_alpha_vantage_news_sentiment_fixture() -> None:
    payload = {
        "feed": [
            {
                "title": "Nvidia AI chips earnings update",
                "url": "https://example.com/nvda",
                "time_published": "20260709T123000",
                "source": "Example",
                "topics": [{"topic": "Earnings"}],
                "ticker_sentiment": [{"ticker": "NVDA", "ticker_sentiment_score": "0.1"}],
            }
        ]
    }

    articles = parse_alpha_vantage_news(payload, ["NVDA", "QQQ"], 20)

    assert len(articles) == 1
    assert articles[0]["symbols"] == ["NVDA"]
    assert "AI chips" in articles[0]["topics"]
    assert articles[0]["relevance"] == "HIGH"


def test_alpha_vantage_note_raises_clear_error() -> None:
    payload = {"Note": "Thank you for using Alpha Vantage. Rate limit reached."}

    try:
        parse_alpha_vantage_global_quote("NVDA", payload)
    except Exception as exc:
        assert "Alpha Vantage Note" in str(exc)
        assert str(exc)
    else:
        raise AssertionError("Expected Alpha Vantage note to raise")


def test_news_recency_filter() -> None:
    now = datetime.now(UTC)
    articles = [
        {"title": "new", "published_at": now.isoformat()},
        {"title": "old", "published_at": (now - timedelta(days=40)).isoformat()},
        {"title": "unknown"},
    ]

    filtered = filter_recent_articles(articles, recency_days=14)

    assert [item["title"] for item in filtered] == ["new", "unknown"]


@pytest.mark.asyncio
async def test_news_rss_fallback_after_alpha_and_gdelt_rate_limits(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALPHA_VANTAGE_API_KEY=test-key\n", encoding="utf-8")
    settings = Settings(
        _env_file=env_file,
        alpha_vantage_base_url="https://alpha.test/query",
        gdelt_doc_api_url="https://gdelt.test/api",
        google_news_rss_url="https://news.test/rss",
        yahoo_finance_rss_url="https://yahoo.test/rss",
        marketwatch_rss_url="https://marketwatch.test/rss",
        federal_reserve_rss_url="https://fed.test/rss",
    )
    provider = NewsProvider(ProviderCacheRepository(tmp_path / "cache.sqlite3"), settings)
    pub_date = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")
    rss = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
  <item>
    <title>Nvidia and Microsoft lift Nasdaq mega-cap technology shares</title>
    <link>https://example.com/nvda-msft-nasdaq</link>
    <pubDate>{pub_date}</pubDate>
    <source>Fixture News</source>
  </item>
</channel></rss>
"""

    with respx.mock(assert_all_mocked=True) as router:
        router.get("https://alpha.test/query").mock(
            return_value=httpx.Response(
                200,
                json={"Note": "Thank you for using Alpha Vantage. Rate limit reached."},
            )
        )
        router.get("https://gdelt.test/api").mock(return_value=httpx.Response(429, text="Too Many Requests"))
        router.get("https://news.test/rss").mock(return_value=httpx.Response(200, text=rss))
        result = await provider.fetch_for_symbols(["NVDA", "AAPL", "MSFT", "QQQ"], limit=20, recency_days=14)

    assert len(result.data["articles"]) == 1
    assert result.metadata.provider_type == "RSS"
    assert result.data["data_quality"]["fallback_used"] is True
    assert result.data["data_quality"]["final_data_available"] is True
    assert result.data["data_quality"]["errors"] == []
    assert any("Alpha Vantage NEWS_SENTIMENT rate_limited" in warning for warning in result.data["data_quality"]["warnings"])
    assert any("GDELT Doc API rate_limited" in warning for warning in result.data["data_quality"]["warnings"])


def test_parse_rss_articles_lowers_reliability_when_pubdate_missing() -> None:
    rss = """<rss><channel>
      <item>
        <title>Apple and Nasdaq earnings update</title>
        <link>https://example.com/aapl</link>
      </item>
    </channel></rss>"""

    articles, warnings = parse_rss_articles(
        rss,
        symbols=["AAPL", "QQQ"],
        limit=20,
        source_name="Fixture RSS",
        reliability=0.64,
    )

    assert len(articles) == 1
    assert articles[0]["published_at"] is None
    assert articles[0]["reliability"] < 0.64
    assert warnings


def test_parse_rss_articles_uses_description_as_real_summary() -> None:
    rss = """<rss><channel>
      <item>
        <title>BLS releases CPI data</title>
        <link>https://www.bls.gov/news.release/cpi.nr0.htm</link>
        <description><![CDATA[The Consumer Price Index increased according to the official release.]]></description>
      </item>
    </channel></rss>"""

    articles, _ = parse_rss_articles(
        rss,
        symbols=["QQQ"],
        limit=20,
        source_name="BLS RSS",
        reliability=0.86,
    )

    assert articles[0]["summary"] == "The Consumer Price Index increased according to the official release."
    assert articles[0]["summary_source_type"] == "rss_description"
    assert articles[0]["is_official"] is True
    assert articles[0]["canonical_url"] == "https://www.bls.gov/news.release/cpi.nr0.htm"


@pytest.mark.asyncio
async def test_snapshot_uses_yahoo_chart_without_stooq_noise(tmp_path) -> None:
    settings = Settings(
        yahoo_chart_url="https://chart.test/v8/finance/chart",
        alpha_vantage_api_key=None,
        yahoo_quote_url="https://quote.test/v7/finance/quote",
    )
    provider = MegaCapSnapshotProvider(ProviderCacheRepository(tmp_path / "cache.sqlite3"), settings)
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 102.0,
                        "chartPreviousClose": 100.0,
                        "regularMarketVolume": 12345,
                        "currency": "USD",
                    },
                    "indicators": {"quote": [{"close": [100.0, 102.0], "volume": [1, 2]}]},
                }
            ]
        }
    }

    with respx.mock(assert_all_mocked=True) as router:
        for symbol in MEGA_CAP_TICKERS:
            router.get(f"https://chart.test/v8/finance/chart/{symbol}").mock(
                return_value=httpx.Response(200, json=payload)
            )
        result = await provider.fetch()

    assert result.metadata.source == "Yahoo Finance Chart"
    assert result.data["data_quality"]["resolved_count"] == 12
    assert result.data["data_quality"]["errors"] == []
    assert result.data["data_quality"]["warnings"] == []
    assert not any("Stooq" in error for error in result.metadata.errors)


def test_news_topic_tagging_is_keyword_based() -> None:
    topics = tag_topics("Nvidia AI chips earnings rise as export controls tighten in China")

    assert "AI chips" in topics
    assert "earnings" in topics
    assert "China" in topics
    assert relevance(["NVDA"], topics) == "HIGH"
