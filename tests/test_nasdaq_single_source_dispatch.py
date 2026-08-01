from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.mega_cap_snapshot_provider import (
    MEGA_CAP_TICKERS,
    MegaCapSnapshotProvider,
)
from app.providers.qqq_holdings_provider import QQQHoldingsProvider
import app.services.provider_capability_registry as provider_registry


def _set_provider_order(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dataset_id: str,
    provider_order: tuple[str, ...],
) -> None:
    monkeypatch.setattr(
        provider_registry,
        "DATASET_SOURCE_POLICIES",
        tuple(
            replace(
                policy,
                primary_provider=provider_order[0],
                fallback_providers=provider_order[1:],
            )
            if policy.dataset_id == dataset_id
            else policy
            for policy in provider_registry.DATASET_SOURCE_POLICIES
        ),
    )


def _no_http_client(*args, **kwargs):
    del args, kwargs
    raise AssertionError("HTTP client instantiated for invalid policy")


@pytest.mark.asyncio
async def test_qqq_dispatch_uses_mutated_policy_order_and_short_circuits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_order = (
        "ALPHA_VANTAGE",
        "INVESCO",
        "NASDAQ",
        "SEC",
    )
    _set_provider_order(
        monkeypatch,
        dataset_id="nasdaq_100",
        provider_order=provider_order,
    )
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        alpha_vantage_api_key="controlled-key",
        alpha_vantage_base_url="https://alpha.test/query",
        invesco_qqq_holdings_url="https://invesco.test/qqq.csv",
        nasdaq_100_constituents_url=(
            "https://nasdaq.test/constituents"
        ),
    )
    provider = QQQHoldingsProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
    )
    alpha_payload = {
        "holdings": [
            {
                "symbol": symbol,
                "description": symbol,
                "weight": "20%",
                "sector": "Technology",
            }
            for symbol in ("AAPL", "MSFT", "NVDA", "AMZN", "META")
        ]
    }

    with respx.mock(
        assert_all_called=True,
        assert_all_mocked=True,
    ) as router:
        alpha = router.get("https://alpha.test/query").mock(
            return_value=httpx.Response(200, json=alpha_payload)
        )
        result = await provider.fetch()

    assert alpha.call_count == 1
    assert result.metadata.source == "Alpha Vantage ETF_PROFILE"
    quality = result.data["data_quality"]
    assert quality["provider_attempts"] == ["alpha_vantage"]
    accounting = quality["provider_accounting"]
    assert [row["provider"] for row in accounting] == list(
        provider_order
    )
    assert accounting[0]["called"] is True
    assert accounting[0]["status"] == "SUCCESS"
    assert all(
        row["called"] is False
        and row["reason_code"] == "PRIOR_PROVIDER_SUCCEEDED"
        for row in accounting[1:]
    )


@pytest.mark.asyncio
async def test_qqq_mutated_fallback_order_matches_calls_and_accounting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_order = (
        "ALPHA_VANTAGE",
        "INVESCO",
        "NASDAQ",
        "SEC",
    )
    _set_provider_order(
        monkeypatch,
        dataset_id="nasdaq_100",
        provider_order=provider_order,
    )
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        alpha_vantage_api_key="controlled-key",
        alpha_vantage_base_url="https://alpha.test/query",
        invesco_qqq_holdings_url="https://invesco.test/qqq.csv",
        nasdaq_100_constituents_url=(
            "https://nasdaq.test/constituents"
        ),
    )
    provider = QQQHoldingsProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
    )
    calls: list[str] = []

    def alpha_failure(_request: httpx.Request) -> httpx.Response:
        calls.append("ALPHA_VANTAGE")
        return httpx.Response(503, text="controlled failure")

    def invesco_success(_request: httpx.Request) -> httpx.Response:
        calls.append("INVESCO")
        rows = [
            "Fund holdings as of,2026-07-31",
            "Ticker,Name,Weight (%),Sector",
            "AAPL,Apple,20,Technology",
            "MSFT,Microsoft,20,Technology",
            "NVDA,Nvidia,20,Technology",
            "AMZN,Amazon,20,Consumer Discretionary",
            "META,Meta,20,Communication Services",
        ]
        return httpx.Response(200, text="\n".join(rows))

    with respx.mock(
        assert_all_called=True,
        assert_all_mocked=True,
    ) as router:
        router.get("https://alpha.test/query").mock(
            side_effect=alpha_failure
        )
        router.get("https://invesco.test/qqq.csv").mock(
            side_effect=invesco_success
        )
        result = await provider.fetch()

    assert calls == ["ALPHA_VANTAGE", "INVESCO"]
    assert result.metadata.source == "Invesco QQQ Holdings"
    accounting = result.data["data_quality"][
        "provider_accounting"
    ]
    assert [row["provider"] for row in accounting] == list(
        provider_order
    )
    assert accounting[0]["status"] == "PROVIDER_FAILED"
    assert accounting[1]["status"] == "SUCCESS"
    assert all(
        row["called"] is False
        and row["reason_code"] == "PRIOR_PROVIDER_SUCCEEDED"
        for row in accounting[2:]
    )


@pytest.mark.asyncio
async def test_qqq_unmapped_policy_fails_closed_before_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_order = (
        "INVESCO",
        "ALPHA_VANTAGE",
        "NASDAQ",
        "SEC",
        "YAHOO_FINANCE_CHART",
    )
    _set_provider_order(
        monkeypatch,
        dataset_id="nasdaq_100",
        provider_order=provider_order,
    )
    monkeypatch.setattr(httpx, "AsyncClient", _no_http_client)
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
    )
    provider = QQQHoldingsProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
    )

    result = await provider.fetch()

    quality = result.data["data_quality"]
    assert quality["final_data_available"] is False
    assert quality["reason_code"] == (
        "RUNTIME_PROVIDER_POLICY_INVALID"
    )
    assert "RUNTIME_POLICY_MAPPING_MISMATCH" in str(
        quality["runtime_policy_error"]
    )
    assert [
        (
            row["provider"],
            row["called"],
            row["reason_code"],
        )
        for row in quality["provider_accounting"]
    ] == [
        (
            provider_id,
            False,
            "RUNTIME_ADAPTER_MAPPING_UNAVAILABLE",
        )
        for provider_id in provider_order
    ]


@pytest.mark.asyncio
async def test_mega_cap_dispatch_uses_mutated_order_and_short_circuits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_order = (
        "STOOQ",
        "YAHOO_FINANCE_CHART",
        "ALPHA_VANTAGE",
        "YAHOO_FINANCE_QUOTE",
    )
    _set_provider_order(
        monkeypatch,
        dataset_id="mega_cap_quotes",
        provider_order=provider_order,
    )
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        alpha_vantage_api_key=None,
        yahoo_chart_url="https://chart.test/v8/finance/chart",
        yahoo_quote_url="https://quote.test/v7/finance/quote",
    )
    provider = MegaCapSnapshotProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
    )
    observed_at = datetime.now(UTC).replace(microsecond=0)
    csv_rows = [
        "Symbol,Date,Time,Open,High,Low,Close,Volume",
        *(
            f"{symbol.lower()}.us,"
            f"{observed_at.date().isoformat()},"
            f"{observed_at.time().isoformat()},"
            "100,102,99,101,1000"
            for symbol in MEGA_CAP_TICKERS
        ),
    ]

    with respx.mock(
        assert_all_called=True,
        assert_all_mocked=True,
    ) as router:
        stooq = router.get(
            "https://stooq.com/q/l/"
        ).mock(
            return_value=httpx.Response(
                200,
                text="\n".join(csv_rows),
            )
        )
        result = await provider.fetch()

    assert stooq.call_count == 1
    assert result.metadata.source == "Stooq Quote CSV"
    assert result.metadata.is_fallback is False
    accounting = result.data["data_quality"][
        "provider_accounting"
    ]
    assert [row["provider"] for row in accounting] == list(
        provider_order
    )
    assert accounting[0]["called"] is True
    assert accounting[0]["status"] == "SUCCESS"
    assert all(
        row["called"] is False
        and row["reason_code"] == "PRIOR_PROVIDER_SUCCEEDED"
        for row in accounting[1:]
    )


@pytest.mark.asyncio
async def test_mega_cap_mutated_fallback_order_matches_calls_and_accounting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_order = (
        "STOOQ",
        "YAHOO_FINANCE_QUOTE",
        "YAHOO_FINANCE_CHART",
        "ALPHA_VANTAGE",
    )
    _set_provider_order(
        monkeypatch,
        dataset_id="mega_cap_quotes",
        provider_order=provider_order,
    )
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        alpha_vantage_api_key=None,
        yahoo_chart_url="https://chart.test/v8/finance/chart",
        yahoo_quote_url="https://quote.test/v7/finance/quote",
    )
    provider = MegaCapSnapshotProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
    )
    calls: list[str] = []

    def stooq_failure(_request: httpx.Request) -> httpx.Response:
        calls.append("STOOQ")
        return httpx.Response(503, text="controlled failure")

    def quote_success(_request: httpx.Request) -> httpx.Response:
        calls.append("YAHOO_FINANCE_QUOTE")
        return httpx.Response(
            200,
            json={
                "quoteResponse": {
                    "result": [
                        {
                            "symbol": symbol,
                            "regularMarketPrice": 101.0,
                            "regularMarketTime": int(
                                datetime.now(UTC).timestamp()
                            ),
                            "regularMarketChange": 1.0,
                            "regularMarketChangePercent": 1.0,
                            "regularMarketVolume": 1000,
                            "currency": "USD",
                        }
                        for symbol in MEGA_CAP_TICKERS
                    ]
                }
            },
        )

    with respx.mock(
        assert_all_called=True,
        assert_all_mocked=True,
    ) as router:
        router.get("https://stooq.com/q/l/").mock(
            side_effect=stooq_failure
        )
        router.get(
            "https://quote.test/v7/finance/quote"
        ).mock(side_effect=quote_success)
        result = await provider.fetch()

    assert calls == ["STOOQ", "YAHOO_FINANCE_QUOTE"]
    assert result.metadata.source == "Yahoo Finance Quote"
    accounting = result.data["data_quality"][
        "provider_accounting"
    ]
    assert [row["provider"] for row in accounting] == list(
        provider_order
    )
    assert accounting[0]["status"] == "FAILED"
    assert accounting[1]["status"] == "SUCCESS"
    assert all(
        row["called"] is False
        and row["reason_code"] == "PRIOR_PROVIDER_SUCCEEDED"
        for row in accounting[2:]
    )


@pytest.mark.asyncio
async def test_mega_cap_unmapped_policy_fails_closed_before_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_order = (
        "YAHOO_FINANCE_CHART",
        "STOOQ",
        "ALPHA_VANTAGE",
        "YAHOO_FINANCE_QUOTE",
        "INVESCO",
    )
    _set_provider_order(
        monkeypatch,
        dataset_id="mega_cap_quotes",
        provider_order=provider_order,
    )
    monkeypatch.setattr(httpx, "AsyncClient", _no_http_client)
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
    )
    provider = MegaCapSnapshotProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
    )

    result = await provider.fetch()

    quality = result.data["data_quality"]
    assert quality["final_data_available"] is False
    assert quality["reason_code"] == (
        "RUNTIME_PROVIDER_POLICY_INVALID"
    )
    assert any(
        "RUNTIME_POLICY_MAPPING_MISMATCH" in error
        for error in quality["errors"]
    )
    assert [
        (
            row["provider"],
            row["called"],
            row["reason_code"],
        )
        for row in quality["provider_accounting"]
    ] == [
        (
            provider_id,
            False,
            "RUNTIME_ADAPTER_MAPPING_UNAVAILABLE",
        )
        for provider_id in provider_order
    ]
