from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from app.core.config import Settings
from app.models.common import (
    Freshness,
    ProviderMetadata,
    ProviderResult,
    ProviderType,
)
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.mega_cap_snapshot_provider import (
    MEGA_CAP_TICKERS,
    MegaCapSnapshotProvider,
    parse_alpha_vantage_global_quote,
    parse_stooq_quotes,
    parse_yahoo_chart,
    parse_yahoo_quotes,
)
from app.services.data_freshness_service import DataFreshnessService
from app.services.deterministic_market_context_service import (
    compute_market_internals,
    compute_options_positioning,
)
from app.services.deterministic_provider_runtime_service import (
    DeterministicProviderRuntimeService,
)
from app.services.nasdaq_data_service import NasdaqDataService


NOW = datetime(2026, 7, 31, 14, 0, tzinfo=UTC)


class _StaticProvider:
    def __init__(self, result: ProviderResult) -> None:
        self.result = result

    async def fetch_safe(self, *, force: bool = False) -> ProviderResult:
        del force
        return self.result.model_copy(deep=True)


class _FactRecorder:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def upsert_fact(self, row: dict) -> None:
        self.rows.append(row)


def _provider_result(
    data: dict,
    *,
    retrieved_at: datetime = NOW,
) -> ProviderResult:
    return ProviderResult(
        metadata=ProviderMetadata(
            source="fixture",
            provider_type=ProviderType.API,
            retrieved_at=retrieved_at,
            freshness=Freshness.RECENT,
            reliability=0.8,
        ),
        data=data,
    )


def _holdings_result() -> ProviderResult:
    return _provider_result(
        {
            "status": "found",
            "source": "fixture",
            "as_of": NOW.date().isoformat(),
            "holdings": [{"symbol": "NVDA", "weight": 9.0}],
            "data_quality": {},
        }
    )


def _option_chains() -> dict[str, list[dict]]:
    return {
        "2026-08-07": [
            {
                "option_type": "call",
                "strike": 500.0,
                "volume": 10,
                "open_interest": 100,
                "contract_size": 100,
                "gamma": 0.01,
            }
        ]
    }


def test_mega_cap_parsers_preserve_provider_observation_times() -> None:
    alpha = parse_alpha_vantage_global_quote(
        "NVDA",
        {
            "Global Quote": {
                "01. symbol": "NVDA",
                "05. price": "123.45",
                "07. latest trading day": "2026-07-30",
            }
        },
    )
    yahoo, _ = parse_yahoo_quotes(
        {
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "NVDA",
                        "regularMarketPrice": 123.45,
                        "regularMarketTime": 1785434400,
                    }
                ]
            }
        }
    )
    chart = parse_yahoo_chart(
        "NVDA",
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": 123.45,
                            "regularMarketTime": 1785434400,
                        },
                        "timestamp": [1785430800, 1785434400],
                        "indicators": {
                            "quote": [{"close": [122.0, 123.45]}]
                        },
                    }
                ]
            }
        },
    )
    stooq, _ = parse_stooq_quotes(
        "Symbol,Date,Time,Open,Close,Volume\n"
        "nvda.us,2026-07-30,20:00:00,122,123.45,1000\n"
    )

    assert alpha is not None
    assert alpha["data_as_of"] == "2026-07-30T00:00:00+00:00"
    assert yahoo[0]["data_as_of"] == "2026-07-30T18:00:00+00:00"
    assert chart is not None
    assert chart["data_as_of"] == "2026-07-30T18:00:00+00:00"
    assert stooq[0]["data_as_of"] == "2026-07-30T20:00:00+00:00"


@pytest.mark.asyncio
async def test_old_mega_cap_observation_retrieved_now_is_excluded() -> None:
    old_observation = NOW - timedelta(days=3)
    snapshot = _provider_result(
        {
            "stocks": [
                {
                    "symbol": "NVDA",
                    "last_price": 123.45,
                    "change_pct": 1.0,
                    "source": "Yahoo Finance Quote",
                    "data_as_of": old_observation.isoformat(),
                    "retrieved_at": NOW.isoformat(),
                }
            ],
            "data_quality": {"tracked_count": 1},
        },
        retrieved_at=NOW,
    )
    service = NasdaqDataService(
        _StaticProvider(_holdings_result()),
        _StaticProvider(snapshot),
        _StaticProvider(_provider_result({})),
        _StaticProvider(_provider_result({})),
        clock=lambda: NOW,
    )

    result = await service.mega_cap_snapshot(force=True)

    assert result.retrieved_at == NOW
    assert result.data_as_of is None
    assert result.stocks == []
    assert result.data_quality.final_data_available is False
    assert result.data_quality.reason_code == (
        "MEGA_CAP_NO_CURRENT_PROVIDER_OBSERVATIONS"
    )
    assert result.data_quality.rejected_stale_symbols == ["NVDA"]


@pytest.mark.asyncio
async def test_stale_primary_mega_cap_quotes_reach_current_fallback(
    tmp_path,
) -> None:
    settings = Settings(
        database_path=tmp_path / "mega-cap-fallback.sqlite",
        yahoo_chart_url="https://chart.test/v8/finance/chart",
        alpha_vantage_api_key=None,
    )
    provider = MegaCapSnapshotProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
    )
    stale_epoch = int(
        (datetime.now(UTC) - timedelta(days=3)).timestamp()
    )
    chart_payload = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 101.0,
                        "regularMarketTime": stale_epoch,
                        "chartPreviousClose": 100.0,
                    },
                    "indicators": {
                        "quote": [{"close": [100.0, 101.0]}]
                    },
                }
            ]
        }
    }
    current = datetime.now(UTC).replace(microsecond=0)
    csv_rows = [
        "Symbol,Date,Time,Open,High,Low,Close,Volume",
        *(
            f"{symbol.lower()}.us,{current.date().isoformat()},"
            f"{current.time().isoformat()},100,102,99,101,1000"
            for symbol in MEGA_CAP_TICKERS
        ),
    ]

    with respx.mock(assert_all_mocked=True) as router:
        for symbol in MEGA_CAP_TICKERS:
            router.get(
                f"https://chart.test/v8/finance/chart/{symbol}"
            ).mock(return_value=httpx.Response(200, json=chart_payload))
        router.get("https://stooq.com/q/l/").mock(
            return_value=httpx.Response(200, text="\n".join(csv_rows))
        )
        result = await provider.fetch()

    assert result.metadata.source == "Stooq Quote CSV"
    assert result.metadata.data_as_of is not None
    accounting = result.data["data_quality"]["provider_accounting"]
    assert accounting[0]["provider"] == "YAHOO_FINANCE_CHART"
    assert accounting[0]["status"] == "FAILED"
    assert accounting[1]["provider"] == "STOOQ"
    assert accounting[1]["status"] == "SUCCESS"


@pytest.mark.parametrize(
    "freshness",
    ["STALE", "REJECTED_FUTURE", "UNKNOWN"],
)
def test_options_fail_closed_for_non_current_quote(
    freshness: str,
) -> None:
    observed_at = (
        NOW + timedelta(minutes=1)
        if freshness == "REJECTED_FUTURE"
        else NOW - timedelta(hours=3)
        if freshness == "STALE"
        else NOW
    )

    result = compute_options_positioning(
        quote={
            "symbol": "QQQ",
            "last": 500.0,
            "observed_at": observed_at.isoformat(),
            "freshness_state": freshness,
        },
        chains=_option_chains(),
        retrieved_at=NOW,
    )

    assert result["status"] == "NO_DATA"
    assert result["reason_code"]
    assert result["data_as_of"] is None
    assert result["volume"]["calls"] is None
    assert result["open_interest"]["calls"] is None


@pytest.mark.parametrize(
    "freshness",
    ["STALE", "REJECTED_FUTURE", "UNKNOWN"],
)
def test_market_internals_fail_closed_when_all_quotes_rejected(
    freshness: str,
) -> None:
    observed_at = (
        NOW + timedelta(minutes=1)
        if freshness == "REJECTED_FUTURE"
        else NOW - timedelta(hours=3)
        if freshness == "STALE"
        else NOW
    )

    result = compute_market_internals(
        constituents=["NVDA"],
        holdings=[{"symbol": "NVDA", "weight": 9.0}],
        quotes=[
            {
                "symbol": "NVDA",
                "last": 101.0,
                "close": 100.0,
                "observed_at": observed_at.isoformat(),
                "freshness_state": freshness,
            }
        ],
        evaluated_at=NOW,
    )

    assert result["status"] == "NO_DATA"
    assert result["reason_code"] == (
        "MARKET_INTERNALS_NO_CURRENT_QUOTES"
    )
    assert result["data_as_of"] is None
    assert result["advancers"] is None
    assert result["weighted_breadth"] is None


def test_deterministic_persistence_uses_observation_not_clock(
    tmp_path,
) -> None:
    service = object.__new__(DeterministicProviderRuntimeService)
    service.clock = lambda: NOW
    service.freshness = DataFreshnessService(
        Settings(database_path=tmp_path / "observation.sqlite"),
        clock=lambda: NOW,
    )
    service.facts = _FactRecorder()
    observed_at = NOW - timedelta(minutes=10)
    spec = {
        "fact_type": "deterministic_market_internals",
        "max_age": timedelta(hours=2),
        "provider_id": "TRADIER",
    }

    persisted = service._persist_deterministic_section(
        "market_internals",
        {
            "status": "AVAILABLE",
            "provider": "TRADIER",
            "data_as_of": observed_at.isoformat(),
        },
        spec=spec,
    )

    assert persisted is True
    row = service.facts.rows[0]
    assert row["release_at"] == observed_at.isoformat()
    assert row["retrieved_at"] == NOW.isoformat()
    assert row["raw_payload_json"]["data_as_of"] == (
        observed_at.isoformat()
    )

    stale = service._persist_deterministic_section(
        "market_internals",
        {
            "status": "AVAILABLE",
            "provider": "TRADIER",
            "data_as_of": (NOW - timedelta(hours=3)).isoformat(),
        },
        spec=spec,
    )
    assert stale is False
    assert len(service.facts.rows) == 1
