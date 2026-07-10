from datetime import UTC, datetime, timedelta

import pytest

from app.core.cache import SQLiteCache
from app.models.common import Freshness, Impact, ProviderMetadata, ProviderResult, ProviderType
from app.models.events import EconomicEvent
from app.core.config import Settings
from app.providers.event_enrichment import (
    EnrichmentItem,
    FXStreetEconomicCalendarProvider,
    MarketWatchEconomicCalendarProvider,
    ManualEventEnrichmentProvider,
    OpenAIEventEnrichmentProvider,
    PlaywrightDailyFXProvider,
    TargetedSearchEventEnrichmentProvider,
    extract_enrichment_values,
    parse_calendar_payload,
    parse_targeted_search_rss,
)
from app.services.event_enrichment_service import EventEnrichmentService
from app.services.event_service import EventService


class FakeEnrichmentProvider:
    def __init__(self, items=None, errors=None, source="fixture") -> None:
        self.items = items or []
        self.errors = errors or []
        self.source = source

    async def fetch(self, country: str, start: datetime, end: datetime):
        return self.items, self.errors


class FakeEventProvider:
    def __init__(self, events: list[dict]) -> None:
        self.events = events

    async def fetch_safe(self):
        return ProviderResult(
            metadata=ProviderMetadata(
                source="fixture",
                provider_type=ProviderType.API,
                retrieved_at=datetime.now(UTC),
                freshness=Freshness.RECENT,
                reliability=0.9,
            ),
            data=self.events,
        )


def make_event(**overrides) -> EconomicEvent:
    event_time = datetime(2099, 7, 14, 12, 30, tzinfo=UTC)
    values = {
        "event_id": "evt-cpi",
        "name": "Consumer Price Index",
        "country": "US",
        "category": "CPI",
        "date": event_time.date().isoformat(),
        "time_utc": event_time,
        "time_local": event_time,
        "impact": Impact.HIGH,
        "source": "BLS",
        "source_url": "https://bls.test",
        "reliability": 0.9,
        "event_risk_level": Impact.HIGH,
        "default_risk_window_before_minutes": 30,
        "default_risk_window_after_minutes": 30,
    }
    values.update(overrides)
    return EconomicEvent(**values)


def rss_fixture(title: str, description: str, link: str = "https://www.reuters.com/markets/example") -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
  <item>
    <title>{title}</title>
    <link>{link}</link>
    <pubDate>Tue, 14 Jul 2099 10:00:00 GMT</pubDate>
    <source>Reuters</source>
    <description>{description}</description>
  </item>
</channel></rss>
"""


def make_item(**overrides) -> EnrichmentItem:
    item_time = datetime(2099, 7, 14, 12, 30, tzinfo=UTC)
    values = {
        "name": "US CPI",
        "country": "US",
        "category": "CPI",
        "date": item_time.date().isoformat(),
        "time_utc": item_time,
        "forecast": "0.3%",
        "previous": "0.2%",
        "consensus": "0.3%",
        "actual": None,
        "source": "DailyFX Economic Calendar",
        "source_url": "https://dailyfx.test/calendar",
        "provider_type": ProviderType.SCRAPER,
        "reliability": 0.56,
    }
    values.update(overrides)
    return EnrichmentItem(**values)


def test_dailyfx_fixture_parser_extracts_cpi_ppi_nfp() -> None:
    html = """
    <table>
      <tr><th>Date</th><th>Time</th><th>Currency</th><th>Event</th><th>Actual</th><th>Forecast</th><th>Previous</th></tr>
      <tr><td>2099-07-14</td><td>12:30</td><td>USD</td><td>Consumer Price Index</td><td></td><td>0.3%</td><td>0.2%</td></tr>
      <tr><td>2099-07-15</td><td>12:30</td><td>USD</td><td>Producer Price Index</td><td></td><td>0.2%</td><td>0.1%</td></tr>
      <tr><td>2099-07-16</td><td>12:30</td><td>USD</td><td>Nonfarm Payrolls</td><td></td><td>180K</td><td>175K</td></tr>
    </table>
    """

    items, errors = parse_calendar_payload(
        html,
        source="DailyFX Economic Calendar",
        source_url="https://dailyfx.test/calendar",
        reliability=0.56,
    )

    assert errors == []
    assert [item.category for item in items] == ["CPI", "PPI", "NFP"]
    assert items[0].forecast == "0.3%"
    assert items[1].previous == "0.1%"


def test_json_embedded_parser_fixture_extracts_values() -> None:
    html = """
    <html><script id="__NEXT_DATA__" type="application/json">
    {"props":{"events":[{"country":"US","date":"2099-07-14","time":"12:30","event":"Core CPI","forecast":"0.2%","previous":"0.1%","actual":null}]}}
    </script></html>
    """

    items, errors = parse_calendar_payload(
        html,
        source="Fixture Calendar",
        source_url="https://fixture.test/calendar",
        reliability=0.5,
    )

    assert errors == []
    assert len(items) == 1
    assert items[0].category == "Core CPI"
    assert items[0].forecast == "0.2%"


def test_fxstreet_fixture_uses_common_parser(tmp_path) -> None:
    provider = FXStreetEconomicCalendarProvider(Settings(fxstreet_calendar_url="https://fxstreet.test"))
    html = """
    <table>
      <tr><th>Date</th><th>Time</th><th>Country</th><th>Event</th><th>Consensus</th><th>Previous</th></tr>
      <tr><td>2099-07-14</td><td>12:30</td><td>US</td><td>Consumer Price Index</td><td>0.3%</td><td>0.2%</td></tr>
    </table>
    """

    items, errors = provider.parse(html)

    assert errors == []
    assert items[0].source == "FXStreet Economic Calendar"
    assert items[0].consensus == "0.3%"


def test_marketwatch_fixture_uses_common_parser() -> None:
    provider = MarketWatchEconomicCalendarProvider(Settings(marketwatch_calendar_url="https://mw.test"))
    html = """
    <table>
      <tr><th>Date</th><th>Time</th><th>Country</th><th>Name</th><th>Forecast</th><th>Previous</th></tr>
      <tr><td>2099-07-15</td><td>12:30</td><td>United States</td><td>Producer Price Index</td><td>0.2%</td><td>0.1%</td></tr>
    </table>
    """

    items, errors = provider.parse(html)

    assert errors == []
    assert items[0].source == "MarketWatch Economic Calendar"
    assert items[0].category == "PPI"


def test_targeted_search_extracts_cpi_forecast_previous() -> None:
    event = make_event(name="Consumer Price Index (June 2026)", date="2026-07-14")
    rss = rss_fixture(
        "US CPI June 2026 forecast of 0.3%",
        "Economists see previous 0.2% and consensus at 0.3% for June 2026.",
    )

    items = parse_targeted_search_rss(
        rss,
        event=event,
        query="US CPI June 2026 forecast previous consensus",
        require_source_url=True,
        recency_days=36500,
    )

    assert len(items) == 1
    assert items[0].forecast == "0.3%"
    assert items[0].previous == "0.2%"
    assert items[0].consensus == "0.3%"
    assert items[0].provider_type == ProviderType.SEARCH_SNIPPET


def test_targeted_search_extracts_ppi_forecast() -> None:
    event = make_event(name="Producer Price Index (June 2026)", category="PPI", date="2026-07-15")
    rss = rss_fixture(
        "US PPI June 2026 expected to rise 0.2%",
        "The previous 0.1% reading was reported for June 2026.",
    )

    items = parse_targeted_search_rss(
        rss,
        event=event,
        query="US PPI June 2026 forecast previous consensus",
        require_source_url=True,
        recency_days=36500,
    )

    assert items[0].forecast == "0.2%"
    assert items[0].previous == "0.1%"


def test_targeted_search_extracts_nfp_k_values() -> None:
    event = make_event(name="Employment Situation (July 2026)", category="NFP", date="2026-08-07")
    rss = rss_fixture(
        "US nonfarm payrolls July 2026 consensus at 180K",
        "Payrolls forecast 180K; previous 147K for July 2026.",
    )

    items = parse_targeted_search_rss(
        rss,
        event=event,
        query="US nonfarm payrolls July 2026 forecast previous consensus",
        require_source_url=True,
        recency_days=36500,
    )

    assert items[0].forecast == "180K"
    assert items[0].previous == "147K"
    assert items[0].consensus == "180K"


def test_targeted_search_gdp_quarter_matching() -> None:
    event = make_event(
        name="GDP (Advance Estimate), 2nd Quarter 2026",
        category="GDP",
        date="2026-07-30",
    )
    rss = rss_fixture(
        "United States GDP second quarter 2026 forecast 2.1%",
        "Consensus at 2.0% and previous 1.8% for second quarter 2026.",
    )

    items = parse_targeted_search_rss(
        rss,
        event=event,
        query="US GDP Q2 2026 advance estimate forecast previous consensus",
        require_source_url=True,
        recency_days=36500,
    )

    assert items[0].forecast == "2.1%"
    assert items[0].previous == "1.8%"


def test_targeted_search_core_pce_matching() -> None:
    event = make_event(name="Personal Income and Outlays (June 2026)", category="PCE", date="2026-07-31")
    rss = rss_fixture(
        "US core PCE June 2026 forecast 0.2%",
        "Prior reading was 0.1% and consensus at 0.2% for June 2026.",
    )

    items = parse_targeted_search_rss(
        rss,
        event=event,
        query="US core PCE June 2026 forecast previous consensus",
        require_source_url=True,
        recency_days=36500,
    )

    assert items[0].forecast == "0.2%"
    assert items[0].previous == "0.1%"


def test_targeted_search_ambiguous_snippet_has_no_extraction() -> None:
    values = extract_enrichment_values("US CPI June 2026 may be watched by economists.")

    assert values["forecast"] is None
    assert values["previous"] is None
    assert values["consensus"] is None


def test_targeted_search_wrong_month_has_no_match() -> None:
    event = make_event(name="Consumer Price Index (June 2026)", date="2026-07-14")
    rss = rss_fixture(
        "US CPI May 2026 forecast of 0.3%",
        "Previous 0.2% and consensus at 0.3% for May 2026.",
    )

    items = parse_targeted_search_rss(
        rss,
        event=event,
        query="US CPI June 2026 forecast previous consensus",
        require_source_url=True,
        recency_days=36500,
    )

    assert items == []


def test_targeted_search_requires_source_url() -> None:
    event = make_event(name="Consumer Price Index (June 2026)", date="2026-07-14")
    rss = rss_fixture(
        "US CPI June 2026 forecast of 0.3%",
        "Previous 0.2% for June 2026.",
        link="",
    )

    items = parse_targeted_search_rss(
        rss,
        event=event,
        query="US CPI June 2026 forecast previous consensus",
        require_source_url=True,
        recency_days=36500,
    )

    assert items == []


@pytest.mark.asyncio
async def test_playwright_disabled_provider_skipped_no_error(tmp_path) -> None:
    settings = Settings(enable_browser_scraping=False)
    provider = PlaywrightDailyFXProvider(settings)
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[provider],
    )
    event = make_event()

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.source is None
    assert metadata["provider_statuses"][0]["status"] == "skipped"
    assert metadata["provider_errors"] == []
    assert metadata["browser_scraping_enabled"] is False
    assert metadata["browser_scraping_used"] is False


@pytest.mark.asyncio
async def test_playwright_unavailable_is_provider_unavailable(tmp_path) -> None:
    settings = Settings(
        enable_browser_scraping=True,
        browser_scraping_timeout_seconds=0.1,
        dailyfx_calendar_url="http://127.0.0.1:9/unavailable",
    )
    provider = PlaywrightDailyFXProvider(settings)
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[provider],
    )
    event = make_event()

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.source is None
    assert metadata["provider_statuses"][0]["status"] == "provider_unavailable"
    assert metadata["browser_scraping_enabled"] is True


@pytest.mark.asyncio
async def test_provider_blocked_captcha_is_unavailable(respx_mock, tmp_path) -> None:
    settings = Settings(fxstreet_calendar_url="https://fxstreet.test/calendar")
    provider = FXStreetEconomicCalendarProvider(settings)
    respx_mock.get("https://fxstreet.test/calendar").respond(200, text="<html>captcha access denied</html>")

    items, errors = await provider.fetch(
        country="US",
        start=datetime(2099, 7, 14, tzinfo=UTC),
        end=datetime(2099, 7, 15, tzinfo=UTC),
    )

    assert items == []
    assert errors == ["FXStreet Economic Calendar provider_unavailable: blocked page or challenge detected"]


@pytest.mark.asyncio
async def test_matching_cpi_official_event_to_dailyfx_enrichment(tmp_path) -> None:
    event = make_event()
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(items=[make_item()])],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert metadata["enriched_count"] == 1
    assert enriched[0].enrichment.forecast == "0.3%"
    assert enriched[0].enrichment.previous == "0.2%"
    assert enriched[0].enrichment.source == "DailyFX Economic Calendar"


@pytest.mark.asyncio
async def test_matching_ppi_official_event(tmp_path) -> None:
    event = make_event(event_id="evt-ppi", name="Producer Price Index", category="PPI")
    item = make_item(name="US PPI", category="PPI", forecast="0.2%", previous="0.1%")
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(items=[item])],
    )

    enriched, _ = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.forecast == "0.2%"
    assert enriched[0].enrichment.matched_by is not None


@pytest.mark.asyncio
async def test_matching_nfp_official_event(tmp_path) -> None:
    event = make_event(event_id="evt-nfp", name="Nonfarm Payrolls", category="NFP")
    item = make_item(name="US Nonfarm Payrolls", category="NFP", forecast="180K", previous="175K")
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(items=[item])],
    )

    enriched, _ = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.forecast == "180K"
    assert enriched[0].enrichment.previous == "175K"


@pytest.mark.asyncio
async def test_event_without_match_has_null_enrichment_warning(tmp_path) -> None:
    event = make_event(name="Retail Sales", category="Retail Sales")
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(items=[make_item()])],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert metadata["missing_enrichment_count"] == 1
    assert enriched[0].enrichment.forecast is None
    assert enriched[0].enrichment.warnings == ["no_match_found: No enrichment match found among provider results"]


@pytest.mark.asyncio
async def test_time_mismatch_inside_window_matches_with_warning(tmp_path) -> None:
    event = make_event()
    item = make_item(time_utc=event.time_utc + timedelta(minutes=10))
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(items=[item])],
    )

    enriched, _ = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.source == "DailyFX Economic Calendar"
    assert any("differs from official time" in warning for warning in enriched[0].enrichment.warnings)


@pytest.mark.asyncio
async def test_provider_failure_uses_next_provider(tmp_path) -> None:
    event = make_event()
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[
            FakeEnrichmentProvider(errors=["DailyFX provider_failed: timeout"]),
            FakeEnrichmentProvider(items=[make_item(source="ForexFactory Calendar")]),
        ],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.source == "ForexFactory Calendar"
    assert metadata["fallback_used"] is True
    assert metadata["providers_attempted"] == 2
    assert metadata["providers_succeeded"] == 1
    assert metadata["providers_failed"] == 1
    assert metadata["provider_errors"] == ["DailyFX provider_failed: timeout"]


@pytest.mark.asyncio
async def test_all_providers_fail_with_cache_uses_cached_enrichment(tmp_path) -> None:
    cache = SQLiteCache(tmp_path / "cache.sqlite3")
    event = make_event()
    cache.set("macro_event_enrichment:v1:US:2099-07-14", [make_item().model_dump(mode="json")])
    service = EventEnrichmentService(
        cache,
        providers=[FakeEnrichmentProvider(errors=["provider_failed: timeout"])],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.provider_type == ProviderType.CACHE
    assert enriched[0].enrichment.forecast == "0.3%"
    assert metadata["cache_used"] is True


@pytest.mark.asyncio
async def test_all_providers_fail_without_cache_does_not_break(tmp_path) -> None:
    event = make_event()
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(errors=["provider_failed: timeout"])],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.source is None
    assert metadata["provider_errors"] == ["provider_failed: timeout"]
    assert enriched[0].enrichment.warnings == ["no_data_available: No enrichment data available"]


@pytest.mark.asyncio
async def test_structured_providers_403_429_surface_provider_unavailable(tmp_path) -> None:
    event = make_event()
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[
            FakeEnrichmentProvider(
                errors=["DailyFX Economic Calendar provider_failed: 403 Forbidden"],
                source="DailyFX Economic Calendar",
            ),
            FakeEnrichmentProvider(
                errors=["ForexFactory Calendar provider_failed: 403 Forbidden"],
                source="ForexFactory Calendar",
            ),
            FakeEnrichmentProvider(
                errors=["Investing Economic Calendar provider_failed: 429 Too Many Requests"],
                source="Investing Economic Calendar",
            ),
        ],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    warnings = enriched[0].enrichment.warnings
    assert warnings[0] == "provider_unavailable: Structured enrichment providers unavailable"
    assert "DailyFX 403" in warnings[1]
    assert "ForexFactory 403" in warnings[1]
    assert "Investing 429" in warnings[1]
    assert metadata["providers_attempted"] == 3
    assert metadata["providers_succeeded"] == 0
    assert metadata["providers_failed"] == 3
    assert metadata["provider_statuses"][0]["status"] == "provider_unavailable"


@pytest.mark.asyncio
async def test_empty_cache_does_not_mark_cache_used(tmp_path) -> None:
    event = make_event()
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(errors=["provider_failed: timeout"])],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.source is None
    assert metadata["cache_used"] is False


@pytest.mark.asyncio
async def test_cache_with_non_matching_item_does_not_mark_cache_used(tmp_path) -> None:
    cache = SQLiteCache(tmp_path / "cache.sqlite3")
    event = make_event()
    cache.set(
        "macro_event_enrichment:v1:US:2099-07-14",
        [make_item(category="PPI", name="US PPI").model_dump(mode="json")],
    )
    service = EventEnrichmentService(
        cache,
        providers=[FakeEnrichmentProvider(errors=["provider_failed: timeout"])],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.source is None
    assert metadata["cache_used"] is False
    assert not any("cached enrichment" in warning for warning in enriched[0].enrichment.warnings)


@pytest.mark.asyncio
async def test_v2_cache_with_useful_enrichment_sets_cache_used(tmp_path) -> None:
    cache = SQLiteCache(tmp_path / "cache.sqlite3")
    event = make_event()
    cache.set("macro_event_enrichment_merged:v2:US:2099-07-14", [make_item().model_dump(mode="json")])
    service = EventEnrichmentService(
        cache,
        providers=[FakeEnrichmentProvider(errors=["provider_failed: timeout"])],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.forecast == "0.3%"
    assert enriched[0].enrichment.provider_type == ProviderType.CACHE
    assert metadata["cache_used"] is True


@pytest.mark.asyncio
async def test_high_impact_only_filter_skips_low_impact_event(tmp_path) -> None:
    low_event = make_event(impact=Impact.LOW)
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(items=[make_item()])],
    )

    enriched, metadata = await service.enrich_events(
        [low_event],
        country="US",
        start=low_event.time_utc - timedelta(days=1),
        end=low_event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.forecast is None
    assert enriched[0].enrichment.warnings == ["no_data_available: enrichment skipped by high-impact filter"]
    assert metadata["enriched_count"] == 0


@pytest.mark.asyncio
async def test_targeted_search_provider_enriches_cpi_and_metadata(respx_mock, tmp_path) -> None:
    event = make_event(
        name="Consumer Price Index (June 2026)",
        category="CPI",
        date="2026-07-14",
        time_utc=datetime(2026, 7, 14, 12, 30, tzinfo=UTC),
    )
    settings = Settings(
        google_news_rss_url="https://news.test/rss/search",
        targeted_search_recency_days=36500,
    )
    respx_mock.get("https://news.test/rss/search").respond(
        200,
        text=rss_fixture(
            "US CPI June 2026 forecast of 0.3%",
            "The previous 0.2% reading and consensus at 0.3% were cited for June 2026.",
        ),
    )
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[TargetedSearchEventEnrichmentProvider(settings)],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.forecast == "0.3%"
    assert enriched[0].enrichment.previous == "0.2%"
    assert enriched[0].enrichment.provider_type == ProviderType.SEARCH_SNIPPET
    assert enriched[0].enrichment.source_url == "https://www.reuters.com/markets/example"
    assert enriched[0].enrichment.matched_by == "targeted_search:Reuters"
    assert metadata["targeted_search_enabled"] is True
    assert metadata["targeted_search_used"] is True
    assert metadata["targeted_search_matches"] == 1
    assert metadata["targeted_search_no_match_count"] == 0
    assert metadata["targeted_search_queries"]


@pytest.mark.asyncio
async def test_manual_event_enrichment_file_matches_cpi(tmp_path) -> None:
    manual_path = tmp_path / "manual_event_enrichment.json"
    manual_path.write_text(
        """
        {
          "events": [
            {
              "country": "US",
              "date": "2099-07-14",
              "category": "CPI",
              "forecast": "0.4%",
              "previous": "0.3%",
              "consensus": "0.4%",
              "actual": null,
              "source": "manual",
              "source_url": "https://example.com/calendar",
              "reliability": 0.6
            }
          ]
        }
        """,
        encoding="utf-8",
    )
    settings = Settings(manual_event_enrichment_path=manual_path)
    event = make_event()
    service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[
            FakeEnrichmentProvider(errors=["DailyFX provider_failed: 403"], source="DailyFX Economic Calendar"),
            ManualEventEnrichmentProvider(settings),
        ],
    )

    enriched, metadata = await service.enrich_events(
        [event],
        country="US",
        start=event.time_utc - timedelta(days=1),
        end=event.time_utc + timedelta(days=1),
    )

    assert enriched[0].enrichment.forecast == "0.4%"
    assert enriched[0].enrichment.previous == "0.3%"
    assert enriched[0].enrichment.source == "manual"
    assert enriched[0].enrichment.provider_type == ProviderType.CACHE
    assert metadata["providers_succeeded"] == 1


@pytest.mark.asyncio
async def test_openai_event_enrichment_disabled_has_no_error(tmp_path) -> None:
    settings = Settings(enable_openai_event_enrichment=False, openai_api_key="test-key")
    provider = OpenAIEventEnrichmentProvider(settings)

    items, errors = await provider.fetch(
        country="US",
        start=datetime(2099, 7, 14, tzinfo=UTC),
        end=datetime(2099, 7, 15, tzinfo=UTC),
    )

    assert items == []
    assert errors == []


@pytest.mark.asyncio
async def test_events_upcoming_service_includes_enrichment(tmp_path) -> None:
    event = make_event()
    enrichment_service = EventEnrichmentService(
        SQLiteCache(tmp_path / "cache.sqlite3"),
        providers=[FakeEnrichmentProvider(items=[make_item()])],
    )
    service = EventService(
        providers=[FakeEventProvider([event.model_dump(mode="json")])],
        enrichment_service=enrichment_service,
    )

    events = await service.upcoming(country="US", days=30000)

    assert events[0].enrichment.forecast == "0.3%"
    assert service.last_enrichment_metadata["enriched_count"] == 1


def test_payload_has_no_disallowed_action_terms() -> None:
    forbidden = {
        "_".join(("no", "trade")),
        "_".join(("blocks", "trading")),
        "_".join(("blocking", "events")),
        "/".join(("", "risk", "-".join(("no", "trade", "now")))),
        "/".join(("ent" + "ry", "st" + "op", "tar" + "get")),
    }
    payload = make_event().model_dump(mode="json")

    assert _has_no_terms(payload, forbidden)


def _has_no_terms(value, forbidden: set[str]) -> bool:
    if isinstance(value, dict):
        return all(key not in forbidden and _has_no_terms(item, forbidden) for key, item in value.items())
    if isinstance(value, list):
        return all(_has_no_terms(item, forbidden) for item in value)
    if isinstance(value, str):
        return all(term not in value for term in forbidden)
    return True
