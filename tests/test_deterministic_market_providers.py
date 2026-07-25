from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.provider_observation_repository import ProviderObservationRepository
from app.providers.census import CensusProvider
from app.providers.deterministic import (
    DeterministicHttpClient,
    DeterministicProviderError,
    redact_headers,
    redact_url,
    request_fingerprint,
)
from app.providers.finnhub import FinnhubProvider
from app.providers.tradier import TradierProvider, select_relevant_expirations
from app.services.deterministic_market_context_service import (
    ProviderFirstResolutionPlanner,
    compute_cross_asset_context,
    compute_market_internals,
    compute_options_positioning,
    deterministic_anomalies,
)
from app.services.source_policy_service import SourcePolicyService


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 24, 15, tzinfo=UTC)
SENTINEL = "sentinel-secret-token-123456"


def cfg(tmp_path: Path, **overrides) -> Settings:
    values = {
        "database_path": tmp_path / "providers.sqlite",
        "environment": "test",
        "fred_api_key": SENTINEL,
        "bls_api_key": SENTINEL,
        "bea_api_key": SENTINEL,
        "census_api_key": SENTINEL,
        "finnhub_api_key": SENTINEL,
        "tradier_production_token": SENTINEL,
        "tradier_enabled": True,
        "source_policy_path": ROOT / "config" / "source_policy.json",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_config_separates_provider_domains_from_disabled_agents(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    assert settings.deterministic_options_positioning_enabled is True
    assert settings.deterministic_market_internals_enabled is True
    assert settings.deterministic_cross_asset_context_enabled is True
    assert settings.deterministic_earnings_intelligence_enabled is True
    assert settings.research_agent_options_positioning_enabled is False
    assert settings.research_agent_market_internals_enabled is False
    assert settings.research_agent_cross_asset_context_enabled is False
    assert settings.research_agent_earnings_intelligence_enabled is False
    assert SENTINEL not in repr(settings)


@pytest.mark.parametrize(
    "url",
    [
        "https://evilbls.gov/data",
        "https://api.bls.gov.evil.com/data",
        "https://fakebea.gov/data",
        "https://finnhub.io.evil.com/api",
        "https://tradier.com.evil.com/v1",
        "https://localhost/data",
        "https://127.0.0.1/data",
        "https://provider.test/data",
        "http://api.bls.gov/data",
    ],
)
def test_source_policy_rejects_dns_impersonation_and_non_public_urls(url: str) -> None:
    assert SourcePolicyService().validate_url(url).accepted is False


def test_redaction_covers_query_body_headers_and_fingerprint() -> None:
    url = f"https://api.census.gov/data?get=time&key={SENTINEL}"
    assert SENTINEL not in redact_url(url)
    assert redact_headers(
        {
            "Authorization": f"Bearer {SENTINEL}",
            "X-Finnhub-Token": SENTINEL,
        }
    ) == {
        "Authorization": "<redacted>",
        "X-Finnhub-Token": "<redacted>",
    }
    fingerprint = request_fingerprint(
        "POST",
        "https://api.bls.gov/publicAPI/v2/timeseries/data",
        json_body={"registrationkey": SENTINEL, "seriesid": ["CUSR0000SA0"]},
    )
    assert SENTINEL not in fingerprint and len(fingerprint) == 64


@pytest.mark.asyncio
async def test_transport_retries_timeout_and_rejects_redirect() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503, json={"error": "temporary"})
        return httpx.Response(200, json={"data": [{"value": 0}]})

    client = DeterministicHttpClient(
        allowed_hosts={"api.bls.gov"},
        timeout_seconds=1,
        retry_attempts=2,
        transport=httpx.MockTransport(handler),
        sleeper=_no_sleep,
    )
    payload, telemetry, _ = await client.request(
        "GET",
        "https://api.bls.gov/data",
        endpoint_category="test",
        provider="BLS",
    )
    assert payload["data"][0]["value"] == 0
    assert telemetry.actual_provider_requests == 2
    assert telemetry.retries == 1

    redirecting = DeterministicHttpClient(
        allowed_hosts={"api.bls.gov"},
        timeout_seconds=1,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                302,
                headers={"location": "https://api.bls.gov.evil.com"},
            )
        ),
    )
    with pytest.raises(DeterministicProviderError, match="redirect rejected"):
        await redirecting.request(
            "GET",
            "https://api.bls.gov/data",
            endpoint_category="test",
            provider="BLS",
        )


async def _no_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_census_all_explicit_datasets_exact_period_and_compaction(
    tmp_path: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        period = request.url.params["time"]
        path = request.url.path
        if path.endswith("/marts"):
            rows = [
                {
                    "time": period,
                    "time_slot_id": "0",
                    "time_slot_date": "2026-06-01 00:00:00.0",
                    "time_slot_name": "June2026",
                    "program_code": "MARTS",
                    "category_code": "44X72",
                    "data_type_code": "SM",
                    "error_data": "no",
                    "cell_value": "0.00",
                    "seasonally_adj": "yes",
                }
            ]
        elif path.endswith("/advm3"):
            rows = [
                {
                    "time": period,
                    "time_slot_id": "0",
                    "time_slot_date": "2026-06-01 00:00:00.0",
                    "time_slot_name": "June2026",
                    "program_code": "M3ADV",
                    "category_code": "MDM",
                    "data_type_code": "NO",
                    "error_data": "no",
                    "cell_value": "300123.45",
                    "seasonally_adj": "yes",
                }
            ]
        elif path.endswith("/resconst"):
            rows = [
                {
                    "time": period,
                    "time_slot_id": "0",
                    "time_slot_date": "2026-06-01 00:00:00.0",
                    "time_slot_name": "June2026",
                    "program_code": "RESCONST",
                    "category_code": "ASTARTS",
                    "data_type_code": "TOTAL",
                    "error_data": "no",
                    "cell_value": "1400",
                    "seasonally_adj": "yes",
                },
                {
                    "time": period,
                    "time_slot_id": "0",
                    "time_slot_date": "2026-06-01 00:00:00.0",
                    "time_slot_name": "June2026",
                    "program_code": "RESCONST",
                    "category_code": "APERMITS",
                    "data_type_code": "TOTAL",
                    "error_data": "no",
                    "cell_value": "1450",
                    "seasonally_adj": "yes",
                },
            ]
        else:
            rows = [
                {
                    "time": period,
                    "time_slot_id": "0",
                    "time_slot_date": "2026-06-01 00:00:00.0",
                    "time_slot_name": "June2026",
                    "program_code": "FTD",
                    "category_code": "BOPGS",
                    "data_type_code": "BAL",
                    "error_data": "no",
                    "cell_value": "-71234.5",
                    "seasonally_adj": "yes",
                }
            ]
        return httpx.Response(200, json={"data": rows})

    settings = cfg(tmp_path)
    provider = CensusProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=httpx.MockTransport(handler),
    )
    result = await provider.fetch(
        period="2026-06",
        datasets=["MARTS", "ADVM3", "RESCONST", "FTD"],
    )
    assert set(result.data) == {
        "CENSUS:MARTS:RETAIL_SALES",
        "CENSUS:ADVM3:DURABLE_GOODS",
        "CENSUS:RESCONST:HOUSING_STARTS",
        "CENSUS:RESCONST:BUILDING_PERMITS",
        "CENSUS:FTD:TRADE_BALANCE",
    }
    assert result.data["CENSUS:MARTS:RETAIL_SALES"]["value"] == 0
    encoded = json.dumps(result.model_dump(mode="json"))
    assert SENTINEL not in encoded
    assert "2012" not in encoded


@pytest.mark.asyncio
async def test_census_requires_exact_period_and_rejects_unknown_mapping(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    provider = CensusProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": []})
        ),
    )
    with pytest.raises(Exception, match="exact lifecycle period"):
        await provider.fetch()
    with pytest.raises(Exception, match="unknown Census dataset"):
        await provider.fetch(period="2026-06", datasets=["UNKNOWN"])


@pytest.mark.asyncio
async def test_finnhub_earnings_zero_null_sessions_and_news_candidates(
    tmp_path: Path,
) -> None:
    published = int((NOW - timedelta(hours=1)).timestamp())

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/calendar/earnings"):
            return httpx.Response(
                200,
                json={
                    "earningsCalendar": [
                        {
                            "symbol": "NVDA",
                            "date": "2026-07-24",
                            "hour": "bmo",
                            "epsActual": 0,
                            "epsEstimate": 0,
                            "revenueActual": None,
                            "revenueEstimate": 100,
                        },
                        {
                            "symbol": "AMD",
                            "date": "2026-07-24",
                            "hour": "amc",
                            "epsActual": 1.2,
                            "epsEstimate": 1,
                            "revenueActual": 120,
                            "revenueEstimate": 100,
                        },
                    ]
                },
            )
        return httpx.Response(
            200,
            json=[
                {
                    "id": 1,
                    "datetime": published,
                    "headline": "Unicode – résumé",
                    "url": "https://news.example.org/story",
                    "source": "Publisher",
                    "summary": "candidate",
                    "category": "company",
                },
                {
                    "id": 1,
                    "datetime": published,
                    "headline": "Unicode – résumé",
                    "url": "https://news.example.org/story",
                    "source": "Publisher",
                },
            ],
        )

    settings = cfg(tmp_path)
    provider = FinnhubProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=httpx.MockTransport(handler),
        clock=lambda: NOW,
    )
    earnings = await provider.earnings_calendar(
        start=date(2026, 7, 24),
        end=date(2026, 7, 24),
        symbols=["NVDA", "AMD"],
    )
    assert [item["session"] for item in earnings] == ["BMO", "AMC"]
    assert earnings[0]["eps_actual"] == 0
    assert earnings[0]["eps_surprise"] is None
    assert earnings[1]["eps_surprise"] == 20
    news = await provider.company_news(
        "NVDA",
        start=date(2026, 7, 24),
        end=date(2026, 7, 24),
    )
    assert len(news) == 1
    assert news[0]["candidate_only"] is True
    assert news[0]["accepted_for_current_news"] is False
    assert news[0]["independent_source_group"] == "news.example.org"
    assert news[0]["discovery_provider"] == "FINNHUB"
    assert SENTINEL not in json.dumps(news)


def _tradier_transport(request: httpx.Request) -> httpx.Response:
    if request.url.path.endswith("/markets/quotes"):
        return httpx.Response(
            200,
            json={
                "quotes": {
                    "quote": {
                        "symbol": "QQQ",
                        "last": 500,
                        "prevclose": 495,
                        "bid": 499,
                        "ask": 501,
                        "volume": 0,
                        "trade_date": int(NOW.timestamp() * 1000),
                    }
                }
            },
        )
    if request.url.path.endswith("/markets/options/expirations"):
        return httpx.Response(
            200,
            json={"expirations": {"date": ["2026-07-24", "2026-07-31", "2026-08-21"]}},
        )
    return httpx.Response(
        200,
        json={
            "options": {
                "option": [
                    {
                        "symbol": "QQQ260724C00500000",
                        "strike": 500,
                        "option_type": "call",
                        "bid": 2,
                        "ask": 2.2,
                        "volume": 0,
                        "open_interest": 100,
                        "contract_size": 100,
                        "greeks": {
                            "mid_iv": 0.2,
                            "delta": 0.5,
                            "gamma": 0.01,
                            "theta": -0.1,
                            "vega": 0.2,
                            "rho": 0.01,
                        },
                    },
                    {
                        "symbol": "QQQ260724P00500000",
                        "strike": 500,
                        "option_type": "put",
                        "bid": 1.9,
                        "ask": 2.1,
                        "volume": 10,
                        "open_interest": 0,
                        "contract_size": None,
                        "greeks": {
                            "mid_iv": 0.22,
                            "delta": -0.5,
                            "gamma": 0.01,
                            "theta": -0.1,
                            "vega": 0.2,
                            "rho": -0.01,
                        },
                    },
                ]
            }
        },
    )


@pytest.mark.asyncio
async def test_tradier_is_read_only_normalizes_object_and_chain(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    provider = TradierProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=httpx.MockTransport(_tradier_transport),
        clock=lambda: NOW,
    )
    quotes = await provider.quotes(["QQQ"])
    assert quotes[0]["volume"] == 0
    assert quotes[0]["instrument_type"] == "ETF"
    expirations = await provider.expirations("QQQ")
    assert len(expirations) == 3
    chain = await provider.option_chain("QQQ", expirations[0])
    assert len(chain) == 2 and chain[0]["gamma"] == 0.01
    with pytest.raises(DeterministicProviderError, match="only permits GET"):
        await provider.request(
            "/markets/quotes",
            params={},
            method="POST",
            endpoint_category="quotes",
        )
    with pytest.raises(DeterministicProviderError, match="not allowlisted"):
        await provider.request(
            "/accounts/secret/orders",
            params={},
            endpoint_category="forbidden",
        )
    assert not hasattr(provider, "place_order")
    assert SENTINEL not in json.dumps([quotes, chain])


def test_expiration_selection_is_deduplicated_and_bounded() -> None:
    as_of = date(2026, 7, 24)
    selected = select_relevant_expirations(
        [
            as_of,
            as_of,
            as_of + timedelta(days=7),
            as_of + timedelta(days=28),
            as_of + timedelta(days=35),
        ],
        as_of=as_of,
    )
    assert selected == [
        as_of,
        as_of + timedelta(days=7),
        as_of + timedelta(days=28),
    ]


def test_options_metrics_division_zero_and_gamma_contract_size_policy() -> None:
    payload = compute_options_positioning(
        quote={
            "last": 500,
            "environment": "production",
            "observed_at": NOW.isoformat(),
            "freshness_state": "FRESH",
        },
        chains={
            "2026-07-24": [
                {
                    "option_type": "call",
                    "strike": 500,
                    "volume": 0,
                    "open_interest": 100,
                    "contract_size": 100,
                    "iv": 0.2,
                    "delta": 0.5,
                    "gamma": 0.01,
                    "theta": -0.1,
                    "vega": 0.2,
                    "rho": 0.01,
                    "bid": 2,
                    "ask": 2.2,
                },
                {
                    "option_type": "put",
                    "strike": 500,
                    "volume": 10,
                    "open_interest": 0,
                    "contract_size": None,
                    "iv": 0.22,
                    "delta": -0.5,
                    "gamma": 0.01,
                    "theta": -0.1,
                    "vega": 0.2,
                    "rho": -0.01,
                    "bid": 2,
                    "ask": 2.2,
                },
            ]
        },
        retrieved_at=NOW,
    )
    assert payload["volume"]["put_call_ratio"] is None
    assert payload["volume"]["ratio_reason_code"] == "DENOMINATOR_ZERO"
    gamma = payload["gamma_proxy"]
    assert gamma["dealer_positioning_confirmed"] is False
    assert gamma["signed_convention"] == "calls_positive_puts_negative"
    assert gamma["excluded_missing_contract_size"] == 1


def test_market_internals_and_cross_asset_proxies() -> None:
    holdings = [
        {"symbol": "AAPL", "weight_pct": 60},
        {"symbol": "MSFT", "weight_pct": 40},
    ]
    quotes = [
        {"symbol": "AAPL", "last": 102, "close": 100, "volume": 100},
        {"symbol": "MSFT", "last": 99, "close": 100, "volume": 200},
    ]
    internals = compute_market_internals(
        constituents=["AAPL", "AAPL", "MSFT", "NVDA"],
        holdings=holdings,
        quotes=quotes,
        minimum_coverage=0.8,
    )
    assert internals["advancers"] == 1
    assert internals["decliners"] == 1
    assert internals["coverage"] == pytest.approx(2 / 3, rel=1e-6)
    assert "constituent_coverage_insufficient" in internals["warnings"]
    assert internals["label"] == "Nasdaq-100 constituent breadth proxy"

    cross = compute_cross_asset_context(
        quotes=[
            {"symbol": "QQQ", "change_percentage": 1},
            {"symbol": "SPY", "change_percentage": 0.5},
            {"symbol": "HYG", "change_percentage": -0.2},
            {"symbol": "LQD", "change_percentage": 0.2},
            {"symbol": "TLT", "change_percentage": 0.4},
            {"symbol": "UUP", "change_percentage": 0.3},
        ],
        fred_series={
            "DGS2": {"value": 4, "freshness": "STALE"},
            "DGS10": {"value": 4.5, "freshness": "FRESH"},
        },
    )
    assert cross["credit_risk_proxy_hyg_minus_lqd"] == -0.4
    assert cross["relative_strength_vs_qqq"]["SPY"] == -0.5
    assert "equity_credit_divergence" in cross["warnings"]
    assert "stale_rate_series" in cross["warnings"]


def test_provider_first_planner_never_routes_numeric_gaps_to_ai() -> None:
    planner = ProviderFirstResolutionPlanner()
    covered = planner.plan(
        requested_fields=["eps_actual", "official_guidance_summary"],
        provider_values={"eps_actual": 0},
        agent_enabled=True,
    )
    assert covered["fields_resolved_by_provider"] == ["eps_actual"]
    assert covered["ai_fields"] == ["official_guidance_summary"]

    numeric = planner.plan(
        requested_fields=["revenue_actual", "option_volume"],
        agent_enabled=True,
    )
    assert numeric["ai_fields"] == []
    assert numeric["ai_avoided_reason"] == "numeric_gaps_cannot_use_ai"
    assert numeric["actual_ai_invocations"] == 0


def test_anomaly_detection_rejects_non_finite_crossed_and_negative() -> None:
    findings = deterministic_anomalies(
        {
            "contract": {
                "bid": 2,
                "ask": 1,
                "strike": -1,
                "gamma": float("nan"),
            }
        }
    )
    assert any(item.startswith("bid_exceeds_ask") for item in findings)
    assert any(item.startswith("negative_strike") for item in findings)
    assert any(item.startswith("non_finite_numeric") for item in findings)


def test_sentinel_secrets_never_reach_database_snapshot_or_consumer(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    ProviderObservationRepository(settings).record(
        run_id="redaction-test",
        provider_name="CENSUS",
        provider_type="API",
        status="FAILED",
        url=f"https://api.census.gov/data?key={SENTINEL}",
        error=f"token={SENTINEL}",
        raw_payload_json={
            "registrationkey": SENTINEL,
            "headers": {"Authorization": f"Bearer {SENTINEL}"},
        },
    )
    snapshot = MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="fixture",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "data_as_of": NOW.isoformat(),
            "provider_diagnostics": {
                "source_url": f"https://api.census.gov/data?key={SENTINEL}",
                "authorization": f"Bearer {SENTINEL}",
            },
            "readiness": {"status": "PARTIAL", "section_status": {}},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    encoded = json.dumps(snapshot, default=str)
    assert SENTINEL not in encoded
    consumer = snapshot["consumer_payload"]
    assert {
        "macro_actuals",
        "rates_context",
        "options_positioning",
        "market_internals",
        "cross_asset_context",
        "earnings_intelligence",
        "current_company_news",
        "deterministic_domains",
    }.issubset(consumer)
    assert len(json.dumps(consumer, default=str).encode("utf-8")) < 90_000
    assert SENTINEL.encode() not in settings.database_path.read_bytes()
