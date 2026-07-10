from datetime import UTC, datetime

import pytest

from app.models.common import Freshness, ProviderMetadata, ProviderResult, ProviderType
from app.services.nasdaq_data_service import NasdaqDataService


class FakeProvider:
    def __init__(self, data, source="fixture", provider_type=ProviderType.API, reliability=0.8):
        self.data = data
        self.cache = None
        self.cache_key = "fixture"
        self.source = source
        self.provider_type = provider_type
        self.reliability = reliability

    async def fetch_safe(self):
        return ProviderResult(
            metadata=ProviderMetadata(
                source=self.source,
                provider_type=self.provider_type,
                retrieved_at=datetime.now(UTC),
                freshness=Freshness.RECENT,
                reliability=self.reliability,
            ),
            data=self.data,
        )


class CountingProvider(FakeProvider):
    def __init__(self, data, source="fixture", provider_type=ProviderType.API, reliability=0.8):
        super().__init__(data, source=source, provider_type=provider_type, reliability=reliability)
        self.calls = 0

    async def fetch_safe(self):
        self.calls += 1
        return await super().fetch_safe()


@pytest.mark.asyncio
async def test_mega_cap_breadth_uses_weights_and_equal_weight_fallback() -> None:
    service = NasdaqDataService(
        qqq_holdings_provider=FakeProvider(
            {
                "as_of": "2026-07-08",
                "holdings": [
                    {"symbol": "NVDA", "name": "NVIDIA", "weight": 10.0, "sector": "Technology"},
                    {"symbol": "AAPL", "name": "Apple", "weight": None, "sector": "Technology"},
                ],
                "data_quality": {"count": 2, "missing_weights": True, "stale": False, "fallback_used": False, "errors": []},
            },
            provider_type=ProviderType.CSV,
        ),
        mega_cap_snapshot_provider=FakeProvider(
            {
                "stocks": [
                    {
                        "symbol": "NVDA",
                        "name": "NVIDIA",
                        "last_price": 100.0,
                        "change": 2.0,
                        "change_pct": 2.0,
                        "volume": 10,
                        "market_session": "REGULAR",
                        "currency": "USD",
                        "source": "fixture",
                        "retrieved_at": datetime.now(UTC).isoformat(),
                    },
                    {
                        "symbol": "AAPL",
                        "name": "Apple",
                        "last_price": 200.0,
                        "change": -2.0,
                        "change_pct": -1.0,
                        "volume": 10,
                        "market_session": "REGULAR",
                        "currency": "USD",
                        "source": "fixture",
                        "retrieved_at": datetime.now(UTC).isoformat(),
                    },
                ],
                "data_quality": {"tracked_count": 2, "resolved_count": 2, "missing_prices": [], "fallback_used": False, "errors": []},
            }
        ),
        earnings_provider=FakeProvider({"events": [], "data_quality": {"errors": [], "fallback_used": False}}),
        news_provider=FakeProvider({"articles": [], "data_quality": {"errors": [], "fallback_used": False}}),
    )

    breadth = await service.mega_cap_breadth()

    assert breadth.tracked_count == 2
    assert breadth.positive_count == 1
    assert breadth.negative_count == 1
    assert "AAPL" in breadth.data_quality.missing_weights
    assert breadth.top_positive_contributors[0].symbol == "NVDA"


@pytest.mark.asyncio
async def test_mega_cap_breadth_with_twelve_tickers() -> None:
    tickers = [
        "NVDA",
        "AAPL",
        "MSFT",
        "AMZN",
        "META",
        "GOOGL",
        "GOOG",
        "AVGO",
        "TSLA",
        "AMD",
        "NFLX",
        "COST",
    ]
    service = NasdaqDataService(
        qqq_holdings_provider=FakeProvider(
            {
                "as_of": "2026-07-08",
                "holdings": [
                    {"symbol": symbol, "name": symbol, "weight": 100 / len(tickers), "sector": None}
                    for symbol in tickers
                ],
                "data_quality": {
                    "count": 12,
                    "missing_weights": False,
                    "stale": False,
                    "fallback_used": False,
                    "errors": [],
                },
            },
            provider_type=ProviderType.CSV,
        ),
        mega_cap_snapshot_provider=FakeProvider(
            {
                "stocks": [
                    {
                        "symbol": symbol,
                        "name": symbol,
                        "last_price": 100.0,
                        "change": 1.0 if idx % 2 == 0 else -1.0,
                        "change_pct": 1.0 if idx % 2 == 0 else -1.0,
                        "volume": 10,
                        "market_session": "REGULAR",
                        "currency": "USD",
                        "source": "fixture",
                        "retrieved_at": datetime.now(UTC).isoformat(),
                    }
                    for idx, symbol in enumerate(tickers)
                ],
                "data_quality": {
                    "tracked_count": 12,
                    "resolved_count": 12,
                    "missing_prices": [],
                    "fallback_used": False,
                    "errors": [],
                },
            }
        ),
        earnings_provider=FakeProvider({"events": [], "data_quality": {"errors": [], "fallback_used": False}}),
        news_provider=FakeProvider({"articles": [], "data_quality": {"errors": [], "fallback_used": False}}),
    )

    breadth = await service.mega_cap_breadth()

    assert breadth.tracked_count == 12
    assert breadth.positive_count == 6
    assert breadth.negative_count == 6
    assert breadth.data_quality.missing_weights == []


@pytest.mark.asyncio
async def test_context_metadata_dedupes_provider_errors() -> None:
    duplicate_quality = {
        "errors": ["provider_failed: sample", "provider_failed: sample"],
        "warnings": [],
        "fallback_used": True,
        "final_data_available": False,
        "no_data_found": False,
        "provider_failed": True,
        "rate_limited": False,
    }
    service = NasdaqDataService(
        qqq_holdings_provider=FakeProvider(
            {"as_of": "2026-07-08", "holdings": [], "data_quality": duplicate_quality},
            provider_type=ProviderType.CSV,
        ),
        mega_cap_snapshot_provider=FakeProvider(
            {
                "stocks": [],
                "data_quality": {
                    "tracked_count": 12,
                    "resolved_count": 0,
                    "missing_prices": [],
                    **duplicate_quality,
                },
            }
        ),
        earnings_provider=FakeProvider({"events": [], "data_quality": duplicate_quality}),
        news_provider=FakeProvider({"articles": [], "data_quality": duplicate_quality}),
    )

    context = await service.context()

    assert len(context.metadata["critical_errors"]) == len(set(context.metadata["critical_errors"]))
    assert "provider_errors" not in context.metadata
    assert context.metadata["fallback_used"] is True


@pytest.mark.asyncio
async def test_context_metadata_separates_warnings_and_fallback_notes() -> None:
    holdings = {
        "as_of": "2026-07-08",
        "holdings": [
            {"symbol": "NVDA", "name": "NVIDIA", "weight": 10.0, "sector": None}
        ],
        "data_quality": {
            "count": 1,
            "missing_weights": False,
            "stale": False,
            "errors": ["Alpha Vantage ETF_PROFILE missing sector for 1 holdings"],
            "warnings": [],
            "fallback_used": False,
            "final_data_available": True,
            "no_data_found": False,
            "provider_failed": False,
            "rate_limited": False,
        },
    }
    snapshot = {
        "stocks": [
            {
                "symbol": "NVDA",
                "name": "NVIDIA",
                "last_price": 100.0,
                "change": 2.0,
                "change_pct": 2.0,
                "volume": 10,
                "market_session": "REGULAR",
                "currency": "USD",
                "source": "fixture",
                "retrieved_at": datetime.now(UTC).isoformat(),
            }
        ],
        "data_quality": {
            "tracked_count": 12,
            "resolved_count": 12,
            "missing_prices": [],
            "errors": [],
            "warnings": ["Stooq quote provider_failed: 404"],
            "fallback_used": True,
            "final_data_available": True,
            "no_data_found": False,
            "provider_failed": False,
            "rate_limited": False,
        },
    }
    service = NasdaqDataService(
        qqq_holdings_provider=FakeProvider(holdings, provider_type=ProviderType.API),
        mega_cap_snapshot_provider=FakeProvider(snapshot),
        earnings_provider=FakeProvider(
            {
                "events": [],
                "data_quality": {
                    "errors": ["No watchlist earnings found in requested window"],
                    "warnings": [],
                    "fallback_used": False,
                    "final_data_available": True,
                    "no_data_found": True,
                    "provider_failed": False,
                    "rate_limited": False,
                },
            }
        ),
        news_provider=FakeProvider(
            {
                "articles": [],
                "data_quality": {
                    "errors": ["Alpha Vantage NEWS_SENTIMENT rate_limited: limit"],
                    "warnings": [],
                    "fallback_used": True,
                    "final_data_available": False,
                    "no_data_found": True,
                    "provider_failed": True,
                    "rate_limited": True,
                },
            }
        ),
    )

    context = await service.context()

    assert any("missing sector" in warning for warning in context.metadata["warnings"])
    assert any("Stooq quote provider_failed" in note for note in context.metadata["fallback_notes"])
    assert any("latest_news" in error for error in context.metadata["critical_errors"])


@pytest.mark.asyncio
async def test_context_run_deduplicates_qqq_holdings_provider_call() -> None:
    qqq_provider = CountingProvider(
        {
            "as_of": "2026-07-08",
            "holdings": [{"symbol": "NVDA", "name": "NVIDIA", "weight": 10.0, "sector": "Technology"}],
            "data_quality": {
                "count": 1,
                "holdings_count": 1,
                "missing_weights": False,
                "final_data_available": True,
                "actual_network_calls": 1,
            },
        },
        provider_type=ProviderType.CSV,
    )
    service = NasdaqDataService(
        qqq_holdings_provider=qqq_provider,
        mega_cap_snapshot_provider=FakeProvider(
            {
                "stocks": [
                    {
                        "symbol": "NVDA",
                        "name": "NVIDIA",
                        "last_price": 100.0,
                        "change": 1.0,
                        "change_pct": 1.0,
                        "volume": 10,
                        "market_session": "REGULAR",
                        "currency": "USD",
                        "source": "fixture",
                        "retrieved_at": datetime.now(UTC).isoformat(),
                    }
                ],
                "data_quality": {"tracked_count": 1, "resolved_count": 1, "missing_prices": [], "final_data_available": True},
            }
        ),
        earnings_provider=FakeProvider({"events": [], "data_quality": {"errors": [], "fallback_used": False, "final_data_available": True}}),
        news_provider=FakeProvider({"articles": [], "data_quality": {"errors": [], "fallback_used": False, "final_data_available": True}}),
    )

    context = await service.context()

    assert qqq_provider.calls == 1
    assert context.qqq_holdings.data_quality.run_cache_used is True
    assert context.qqq_holdings.data_quality.run_deduplicated_calls >= 2
