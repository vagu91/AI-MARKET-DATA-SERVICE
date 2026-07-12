from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.earnings_provider import EarningsProvider
from app.providers.fmp_earnings_calendar_provider import FmpEarningsCalendarProvider
from app.providers.xtb_economic_calendar_provider import (
    XtbEconomicCalendarProvider,
    classify_xtb_event,
    normalize_xtb_events,
    parse_xtb_payload,
)
from app.services.ai_trader_consumer_v2_service import _earnings, _event_risk
from app.services.data_freshness_service import DataFreshnessService
from app.services.diagnostics_service import DiagnosticsService
from app.services.market_fact_repository import MarketFactRepository
from app.services.multi_source_runtime_service import MultiSourceRuntimeService


def cfg(tmp_path, **values) -> Settings:
    return Settings(_env_file=None, database_path=tmp_path / "market.sqlite", **values)


def fmp_row(symbol: str, event_date: str, **values) -> dict:
    return {
        "symbol": symbol,
        "date": event_date,
        "epsActual": values.get("epsActual"),
        "epsEstimated": values.get("epsEstimated", 1.25),
        "revenueActual": values.get("revenueActual"),
        "revenueEstimated": values.get("revenueEstimated", 10_000_000),
        "lastUpdated": "2026-07-12T10:00:00Z",
    }


def xtb_row(**values) -> dict:
    today = datetime.now(UTC).date().isoformat()
    return {
        "countryCode": "US",
        "country": "Stati Uniti",
        "language": "it",
        "title": "CPI M/M",
        "period": "Jun",
        "forecastString": "0,3%",
        "forecast": 0.3,
        "previousString": "0,1%",
        "previous": 0.1,
        "currentString": None,
        "current": None,
        "effect": None,
        "numericalEvent": True,
        "modifications": [],
        "status": "scheduled",
        "impact": 3,
        "year": datetime.now(UTC).year,
        "id": 1001,
        "indicatorId": 2001,
        "date": today,
        "time": "2099-01-01T14:30:00",
        "timeShortFormat": "14:30",
        "timezoneOffset": 7200,
        "evaluationMethod": "monthly",
        "unit": "%",
        "orderOfMagnitude": 0,
        "currency": "USD",
        **values,
    }


@pytest.mark.asyncio
async def test_fmp_missing_key_does_not_call_network(tmp_path) -> None:
    provider = FmpEarningsCalendarProvider(ProviderCacheRepository(tmp_path / "cache.sqlite"), cfg(tmp_path, fmp_api_key=None))
    with respx.mock(assert_all_called=False) as router:
        result = await provider.fetch()
    assert result.data["status"] == "not_configured"
    assert result.data["data_quality"]["actual_network_calls"] == 0
    assert len(router.calls) == 0


@pytest.mark.asyncio
async def test_fmp_200_filters_14d_watchlist_deduplicates_and_preserves_nulls(tmp_path) -> None:
    today = datetime.now(UTC).date()
    settings = cfg(tmp_path, fmp_api_key="secret", fmp_earnings_calendar_url="https://fmp.test/stable/earnings-calendar")
    rows = [
        fmp_row("NFLX", (today + timedelta(days=4)).isoformat(), epsEstimated=None, revenueEstimated=None),
        fmp_row("NFLX", (today + timedelta(days=4)).isoformat(), epsEstimated=None, revenueEstimated=None),
        fmp_row("GOOG", (today + timedelta(days=8)).isoformat()),
        fmp_row("GOOGL", (today + timedelta(days=8)).isoformat()),
        fmp_row("SMALL", (today + timedelta(days=3)).isoformat()),
        fmp_row("TSLA", (today + timedelta(days=15)).isoformat()),
    ]
    with respx.mock(assert_all_called=True) as router:
        route = router.get(settings.fmp_earnings_calendar_url).mock(return_value=httpx.Response(200, json=rows))
        result = await FmpEarningsCalendarProvider(ProviderCacheRepository(tmp_path / "cache.sqlite"), settings).fetch()
    events = result.data["events"]
    assert route.calls[0].request.headers["apikey"] == "secret"
    assert route.calls[0].request.url.params["from"] == today.isoformat()
    assert route.calls[0].request.url.params["to"] == (today + timedelta(days=14)).isoformat()
    assert [(event["symbol"], event["date"]) for event in events] == [
        ("NFLX", (today + timedelta(days=4)).isoformat()),
        ("GOOG", (today + timedelta(days=8)).isoformat()),
        ("GOOGL", (today + timedelta(days=8)).isoformat()),
    ]
    assert events[0]["eps_estimate"] is None
    assert events[0]["revenue_estimate"] is None
    assert events[0]["eps_actual"] is None
    assert events[0]["lineage"]["eps_estimate"]["source_field"] == "epsEstimated"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("http_status", "status"),
    [(400, "bad_request"), (401, "auth_failed"), (402, "plan_restricted"), (403, "access_denied"), (404, "endpoint_not_found"), (429, "rate_limited")],
)
async def test_fmp_http_statuses_are_explicit(tmp_path, http_status, status) -> None:
    settings = cfg(tmp_path, fmp_api_key="secret", fmp_earnings_calendar_url="https://fmp.test/calendar")
    with respx.mock(assert_all_called=True) as router:
        router.get(settings.fmp_earnings_calendar_url).mock(return_value=httpx.Response(http_status))
        result = await FmpEarningsCalendarProvider(ProviderCacheRepository(tmp_path / "cache.sqlite"), settings).fetch()
    assert result.data["status"] == status
    assert result.data["data_quality"]["http_status"] == http_status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "status"),
    [
        (httpx.Response(200, json=[]), "not_found"),
        (httpx.Response(200, text="not-json"), "parse_failed"),
        (httpx.Response(200, json={"items": []}), "parse_failed"),
        (httpx.Response(200, json=[{"symbol": "AAPL"}]), "not_found"),
    ],
)
async def test_fmp_empty_invalid_and_partial_payloads(tmp_path, response, status) -> None:
    settings = cfg(tmp_path, fmp_api_key="secret", fmp_earnings_calendar_url="https://fmp.test/calendar")
    with respx.mock(assert_all_called=True) as router:
        router.get(settings.fmp_earnings_calendar_url).mock(return_value=response)
        result = await FmpEarningsCalendarProvider(ProviderCacheRepository(tmp_path / "cache.sqlite"), settings).fetch()
    assert result.data["status"] == status


@pytest.mark.asyncio
async def test_fmp_timeout_is_explicit(tmp_path) -> None:
    settings = cfg(tmp_path, fmp_api_key="secret", fmp_earnings_calendar_url="https://fmp.test/calendar")
    with respx.mock(assert_all_called=True) as router:
        router.get(settings.fmp_earnings_calendar_url).mock(side_effect=httpx.ReadTimeout("timeout"))
        result = await FmpEarningsCalendarProvider(ProviderCacheRepository(tmp_path / "cache.sqlite"), settings).fetch()
    assert result.data["status"] == "provider_timeout"


@pytest.mark.asyncio
async def test_ranked_earnings_provider_selects_fmp_before_existing_fallback(tmp_path) -> None:
    today = datetime.now(UTC).date().isoformat()
    settings = cfg(
        tmp_path,
        fmp_api_key="secret",
        alpha_vantage_api_key="alpha",
        fmp_earnings_calendar_url="https://fmp.test/calendar",
        alpha_vantage_base_url="https://alpha.test/query",
    )
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as router:
        router.get(settings.fmp_earnings_calendar_url).mock(return_value=httpx.Response(200, json=[fmp_row("AAPL", today)]))
        alpha = router.get(settings.alpha_vantage_base_url).mock(return_value=httpx.Response(500))
        result = await EarningsProvider(ProviderCacheRepository(tmp_path / "cache.sqlite"), settings).fetch_safe()
    assert result.data["events"][0]["source"] == "Financial Modeling Prep Earnings Calendar"
    assert alpha.call_count == 0


@pytest.mark.asyncio
async def test_fmp_earnings_persist_read_back_and_refresh_false(tmp_path) -> None:
    settings = cfg(tmp_path)
    service = object.__new__(DiagnosticsService)
    service.settings = settings
    service.facts = MarketFactRepository(settings)
    service.freshness = DataFreshnessService(settings)
    service.news = type("NoNews", (), {"upsert_news": lambda self, article: None})()
    retrieved_at = datetime.now(UTC).isoformat()
    raw = {
        "retrieved_at": retrieved_at,
        "days": 14,
        "events": [{
            "symbol": "NFLX",
            "date": datetime.now(UTC).date().isoformat(),
            "source": "Financial Modeling Prep Earnings Calendar",
            "source_url": "https://financialmodelingprep.com/stable/earnings-calendar",
            "reliability": 0.82,
            "lineage": {"date": {"source_field": "date"}},
        }],
        "data_quality": {"final_data_available": True},
    }
    context = type("Context", (), {"model_dump": lambda self, mode: {"upcoming_earnings": raw, "latest_news": {"articles": []}}})()
    assert service._save_nasdaq_context(context) == 1
    fact = service.facts.get_valid_facts_by_type("earnings_event")[0]
    assert fact["raw_payload"]["events"][0]["source"] == "Financial Modeling Prep Earnings Calendar"
    materialized, quality = await service._nasdaq_db_first(symbol="MNQ", fetch_missing=False)
    assert materialized["earnings"]["upcoming"][0]["symbol"] == "NFLX"
    assert quality["provider_calls"] == 0
    assert quality["actual_network_calls"] == 0


def test_xtb_payload_accepts_bytes_and_string() -> None:
    assert parse_xtb_payload(b'{"items": []}')["items"] == []
    assert parse_xtb_payload('{"items": []}')["items"] == []


def test_xtb_mapping_actual_forecast_previous_timezone_and_all_day() -> None:
    now = datetime.now(UTC)
    released_date = (now.date() - timedelta(days=1)).isoformat()
    rows = [
        xtb_row(date=released_date, current=0.4, currentString="0,4%"),
        xtb_row(id=1002, title="Fed Chair Testifies", numericalEvent=False, forecast=None, forecastString=None, timeShortFormat=None),
    ]
    events, rejected = normalize_xtb_events(rows, retrieved_at=now, minimum_impact=2, lookahead_days=7)
    numeric = next(event for event in events if event["source_event_id"] == "1001")
    speech = next(event for event in events if event["source_event_id"] == "1002")
    assert rejected == 0
    assert numeric["actual"] == 0.4
    assert numeric["actual_display"] == "0,4%"
    assert numeric["consensus"] == 0.3
    assert numeric["previous"] == 0.1
    assert numeric["release_at"].endswith("12:30:00Z")
    assert numeric["metric_id"] == "headline_cpi_mom"
    assert speech["all_day"] is True
    assert speech["release_at"] is None
    assert speech["normalized_event_type"] == "FED_COMMUNICATION"


@pytest.mark.parametrize(
    ("title", "metric_id"),
    [
        ("Core CPI A/A", "core_cpi_yoy"),
        ("Indice dei prezzi alla produzione M/M", "headline_ppi_mom"),
        ("Richieste iniziali di sussidi di disoccupazione", "initial_jobless_claims"),
    ],
)
def test_xtb_multilingual_mapping(title, metric_id) -> None:
    assert classify_xtb_event(title, 42, "monthly", "%")[1] == metric_id


def test_xtb_indicator_id_preserves_incoherent_title() -> None:
    event_type, metric_id = classify_xtb_event("Titolo non coerente", 98765, "index", "pts")
    assert event_type == "XTB_INDICATOR_98765"
    assert metric_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("http_status", [403, 406, 429])
async def test_xtb_http_statuses(tmp_path, http_status) -> None:
    settings = cfg(tmp_path, xtb_economic_calendar_url="https://xtb.test/calendar")
    with respx.mock(assert_all_called=True) as router:
        router.get(settings.xtb_economic_calendar_url).mock(return_value=httpx.Response(http_status))
        result = await XtbEconomicCalendarProvider(settings).fetch()
    assert result["diagnostics"]["http_status"] == http_status
    assert result["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "status"),
    [
        (httpx.Response(200, content=b""), "not_found"),
        (httpx.Response(200, text="bad"), "parse_failed"),
        (httpx.Response(200, json={}), "parse_failed"),
        (httpx.Response(200, json={"items": []}), "not_found"),
        (httpx.Response(200, json={"items": [{"countryCode": "US"}]}), "not_found"),
    ],
)
async def test_xtb_empty_invalid_missing_and_partial_payloads(tmp_path, response, status) -> None:
    settings = cfg(tmp_path, xtb_economic_calendar_url="https://xtb.test/calendar")
    with respx.mock(assert_all_called=True) as router:
        router.get(settings.xtb_economic_calendar_url).mock(return_value=response)
        result = await XtbEconomicCalendarProvider(settings).fetch()
    assert result["status"] == status


@pytest.mark.asyncio
async def test_xtb_timeout(tmp_path) -> None:
    settings = cfg(tmp_path, xtb_economic_calendar_url="https://xtb.test/calendar")
    with respx.mock(assert_all_called=True) as router:
        router.get(settings.xtb_economic_calendar_url).mock(side_effect=httpx.ReadTimeout("timeout"))
        result = await XtbEconomicCalendarProvider(settings).fetch()
    assert result["status"] == "provider_timeout"


def test_xtb_filters_country_impact_and_preserves_impact_two() -> None:
    now = datetime.now(UTC)
    events, rejected = normalize_xtb_events(
        [xtb_row(id=1, impact=2), xtb_row(id=2, impact=1), xtb_row(id=3, countryCode="DE")],
        retrieved_at=now,
        minimum_impact=2,
        lookahead_days=7,
    )
    assert [event["source_event_id"] for event in events] == ["1"]
    assert events[0]["impact"] == "MEDIUM"
    assert rejected == 2


@pytest.mark.asyncio
async def test_xtb_persistence_read_back_cache_and_refresh_false(tmp_path) -> None:
    settings = cfg(tmp_path, xtb_economic_calendar_url="https://xtb.test/calendar")
    with respx.mock(assert_all_called=True) as router:
        route = router.get(settings.xtb_economic_calendar_url).mock(return_value=httpx.Response(200, json={"items": [xtb_row()]}))
        forced = await MultiSourceRuntimeService(settings).provider("xtb_economic_calendar", refresh="force")
    cached = await MultiSourceRuntimeService(settings).provider("xtb_economic_calendar", refresh="false")
    assert route.call_count == 1
    assert forced["persisted_count"] == 1
    assert forced["read_back_count"] == 1
    assert cached["cache_used"] is True
    assert cached["provider_calls"] == 0
    assert cached["actual_network_calls"] == 0
    assert cached["items"][0]["source"] == "XTB Economic Calendar"


@pytest.mark.asyncio
async def test_xtb_refresh_false_missing_cache_has_no_network_or_write(tmp_path) -> None:
    settings = cfg(tmp_path, xtb_economic_calendar_url="https://xtb.test/calendar")
    result = await MultiSourceRuntimeService(settings).provider("xtb_economic_calendar", refresh="false")
    assert result["provider_calls"] == 0
    assert result["actual_network_calls"] == 0
    assert result["persisted_count"] == 0


def test_consumer_projects_compact_xtb_calendar() -> None:
    event = xtb_row(date=(datetime.now(UTC).date() - timedelta(days=1)).isoformat(), current=0.4)
    normalized, _ = normalize_xtb_events([event], retrieved_at=datetime.now(UTC), minimum_impact=2, lookahead_days=7)
    projected = _event_risk({
        "event_calendar": {},
        "event_windows": {},
        "economic_calendar_enrichment": {"xtb": {"status": "found", "source": "XTB Economic Calendar", "items": normalized}},
    })
    item = projected["xtb_us_macro_calendar"]["events"][0]
    assert item["actual"] == 0.4
    assert item["consensus"] == 0.3
    assert item["lineage"]["actual"]["source_field"] == "current"


def test_consumer_populates_upcoming_earnings_from_fmp() -> None:
    event = {
        "symbol": "TSLA",
        "date": datetime.now(UTC).date().isoformat(),
        "eps_estimate": None,
        "revenue_estimate": None,
        "source": "Financial Modeling Prep Earnings Calendar",
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
        "lineage": {"date": {"source_field": "date"}},
    }
    block = _earnings({"nasdaq_context": {"earnings": {"upcoming": [event]}}, "market_schedule": {}})
    assert block["issuer_event_count"] == 1
    assert block["upcoming_mega_cap_earnings_14d"][0]["source"] == "Financial Modeling Prep Earnings Calendar"
    assert block["upcoming_mega_cap_earnings_14d"][0]["eps_estimate"] is None
