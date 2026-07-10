from __future__ import annotations

import httpx
import pytest
import respx

from app.api.routes import router
from app.core.config import Settings
from app.providers.investing_fed_rate_monitor_provider import (
    InvestingFedRateMonitorProvider,
    parse_investing_fed_rate_monitor_html,
)
from app.providers.marketbeat_holidays_provider import (
    MarketBeatHolidaysProvider,
    deduplicate_marketbeat_events,
    parse_marketbeat_holidays_html,
    parse_marketbeat_holidays_json_ld,
)
from app.services.multi_source_runtime_service import MultiSourceRuntimeService, build_multi_source_context_blocks


def settings(tmp_path) -> Settings:
    return Settings(_env_file=None, market_db_path=tmp_path / "market.sqlite", database_path=tmp_path / "cache.sqlite")


def test_marketbeat_table_parser_reads_closed_and_early_close_rows() -> None:
    html = """
    <table><thead><tr><th><strong>NASDAQ and NYSE Holidays</strong></th><th>2026</th><th>2027</th></tr></thead>
    <tbody>
      <tr><td><strong>Independence Day</strong></td><td>July 3rd</td><td>July 5th</td></tr>
      <tr><td><strong>Christmas</strong></td><td>December 25th</td><td>December 24th</td></tr>
    </tbody></table>
    <table><thead><tr><th><strong>NASDAQ and NYSE Partial Holidays</strong><br/><strong>(1:00 p.m. Eastern Close)</strong></th><th>2026</th><th>2027</th></tr></thead>
    <tbody>
      <tr><td><strong>Day before Independence Day</strong></td><td>July 3rd</td><td></td></tr>
      <tr><td><strong>The Day Following Thanksgiving</strong></td><td>November 27th</td><td>November 26th</td></tr>
    </tbody></table>
    """

    parsed = parse_marketbeat_holidays_html(html)
    events = parsed["events"]

    assert parsed["tables_seen"] == 2
    assert parsed["closed_rows"] == 2
    assert parsed["early_close_rows"] == 2
    assert any(item["date"] == "2026-07-03" and item["session_status"] == "closed" for item in events)
    assert any(item["date"] == "2026-07-03" and item["session_status"] == "early_close" for item in events)
    assert all(item["source_type"] == "secondary_calendar" for item in events)
    assert all(item["official_exchange_source"] is False for item in events)
    deduped, duplicates, conflicts = deduplicate_marketbeat_events(events)
    july_3 = [item for item in deduped if item["date"] == "2026-07-03"]
    assert len(july_3) == 1
    assert july_3[0]["session_status"] == "closed"
    assert duplicates == 1
    assert conflicts == 1


def test_marketbeat_json_ld_fallback_extracts_dates() -> None:
    html = """
    <script type="application/ld+json">
    [
      {"name":"New Year's Day","startDate":"2027-01-01"},
      {"name":"Early close after Thanksgiving","date":"2027-11-26","description":"1:00 p.m. early close"}
    ]
    </script>
    """

    events = parse_marketbeat_holidays_json_ld(html)

    assert [item["date"] for item in events] == ["2027-01-01", "2027-11-26"]
    assert events[0]["session_status"] == "closed"
    assert events[1]["session_status"] == "early_close"


def test_investing_fed_rate_monitor_parser_reads_meeting_probabilities() -> None:
    html = """
    <div class="cardWrapper">
      <div class="fedRateDate" id="cardName_0">Jul 29, 2026</div>
      <div class="infoFed">
        <div><span>Meeting Time:</span><i>Jul 29, 2026 02:00PM ET</i></div>
        <div><span>Future Price:</span><i>96.373</i></div>
      </div>
      <table class="genTbl openTbl fedRateTbl">
        <tbody>
          <tr><td class="left">3.50 - 3.75 <span class="chartIcon show-history-chart" eventId="516971" probability="3.50% - 3.75%" calcKey="%3.75"></span></td><td>65.7%</td><td>75.6%</td><td>75.6%</td></tr>
          <tr><td class="left">3.75 - 4.00 <span class="chartIcon show-history-chart" eventId="516971" probability="3.75% - 4.00%" calcKey="%4.00"></span></td><td>34.3%</td><td>&mdash;</td><td>24.4%</td></tr>
        </tbody>
      </table>
      <div class="fedUpdate">Updated: Jul 10, 2026 01:05PM EDT</div>
    </div>
    """

    parsed = parse_investing_fed_rate_monitor_html(html)
    meeting = parsed["meetings"][0]

    assert parsed["cards_seen"] == 1
    assert meeting["meeting_date"] == "2026-07-29"
    assert meeting["meeting_at"] == "2026-07-29"
    assert meeting["future_price"] == 96.373
    assert meeting["updated_at"] == "Jul 10, 2026 01:05PM EDT"
    assert meeting["event_id"] == "516971"
    assert meeting["probability_sum_pct"] == 100.0
    assert meeting["target_rate_probabilities"][1]["previous_day_probability_pct"] is None
    assert meeting["probabilities_normalized"] is False
    assert meeting["source_type"] == "secondary_market_implied_probabilities"


def test_investing_parser_does_not_renormalize_probability_outlier() -> None:
    html = """
    <div class="cardWrapper">
      <div class="fedRateDate">Sep 16, 2026</div>
      <div class="infoFed"><div><span>Future Price:</span><i>96.100</i></div></div>
      <table class="fedRateTbl"><tbody>
        <tr><td>3.50 - 3.75 <span eventId="516972"></span></td><td>80.0%</td><td>--</td><td>--</td></tr>
        <tr><td>3.75 - 4.00 <span eventId="516972"></span></td><td>10.0%</td><td>--</td><td>--</td></tr>
      </tbody></table>
    </div>
    """

    parsed = parse_investing_fed_rate_monitor_html(html)

    assert parsed["probability_sum_outliers"] == 1
    assert parsed["meetings"][0]["probability_sum_pct"] == 90.0
    assert parsed["meetings"][0]["probabilities_normalized"] is False


@pytest.mark.asyncio
async def test_marketbeat_provider_fetches_and_marks_secondary(tmp_path) -> None:
    cfg = settings(tmp_path)
    cfg.marketbeat_holidays_url = "https://marketbeat.test/holidays"
    html = """
    <table><thead><tr><th>NASDAQ and NYSE Holidays</th><th>2026</th></tr></thead>
    <tbody><tr><td>New Year's Day</td><td>January 1st</td></tr></tbody></table>
    """

    with respx.mock(assert_all_called=True) as router_mock:
        router_mock.get("https://marketbeat.test/holidays").mock(return_value=httpx.Response(200, text=html))
        result = await MarketBeatHolidaysProvider(cfg).fetch()

    assert result["status"] == "found"
    assert result["holidays"][0]["date"] == "2026-01-01"
    assert result["source_type"] == "secondary_calendar"
    assert result["official_exchange_source"] is False
    assert result["is_official"] is False
    assert result["parser_strategy"] == "html_table"


@pytest.mark.asyncio
async def test_investing_fed_monitor_provider_fetches_secondary_probabilities(tmp_path) -> None:
    cfg = settings(tmp_path)
    cfg.investing_fed_rate_monitor_url = "https://investing.test/fed-rate-monitor"
    html = """
    <div class="cardWrapper"><div class="fedRateDate">Jul 29, 2026</div>
    <div class="infoFed"><div><span>Future Price:</span><i>96.373</i></div></div>
    <table class="fedRateTbl"><tbody><tr><td>3.50 - 3.75 <span eventId="516971" calcKey="%3.75"></span></td><td>100.0%</td><td>100.0%</td><td>100.0%</td></tr></tbody></table></div>
    """

    with respx.mock(assert_all_called=True) as router_mock:
        router_mock.get("https://investing.test/fed-rate-monitor").mock(return_value=httpx.Response(200, text=html))
        result = await InvestingFedRateMonitorProvider(cfg).fetch()

    assert result["status"] == "found"
    assert result["source"] == "Investing Fed Rate Monitor"
    assert result["dataset_type"] == "market_implied_target_rate_distribution"
    assert result["official_fed_data"] is False
    assert result["official_cme_data"] is False
    assert result["current_meeting"]["event_id"] == "516971"
    assert result["current_meeting"]["meeting_at"] == "2026-07-29"
    assert result["history_endpoint"]["status"] == "not_integrated"
    assert result["history_endpoint_status"] == "not_integrated"
    assert result["official_fed_source"] is False


@pytest.mark.asyncio
async def test_multi_source_refresh_false_uses_db_for_new_providers_without_network(monkeypatch, tmp_path) -> None:
    cfg = settings(tmp_path)
    service = MultiSourceRuntimeService(cfg)

    async def marketbeat_ok():
        return {
            "status": "found",
            "source": "MarketBeat",
            "source_url": "https://marketbeat.test/holidays",
            "retrieved_at": "2026-07-10T12:00:00Z",
            "valid_until": "2099-01-01T00:00:00Z",
            "holidays": [{"date": "2026-07-03", "holiday_name": "Independence Day"}],
            "warnings": [],
            "errors": [],
            "diagnostics": {},
        }

    async def fed_ok():
        return {
            "status": "found",
            "source": "Investing.com",
            "source_url": "https://investing.test/fed",
            "retrieved_at": "2026-07-10T12:00:00Z",
            "valid_until": "2099-01-01T00:00:00Z",
            "meetings": [{"meeting_date": "2026-07-29"}],
            "warnings": [],
            "errors": [],
            "diagnostics": {},
        }

    service.marketbeat_holidays.fetch = marketbeat_ok
    service.investing_fed_rate_monitor.fetch = fed_ok
    await service.provider("marketbeat_holidays", refresh="force")
    await service.provider("investing_fed_rate_monitor", refresh="force")

    async def fail_fetch():
        raise AssertionError("network should not be called on refresh=false cache hit")

    service.marketbeat_holidays.fetch = fail_fetch
    service.investing_fed_rate_monitor.fetch = fail_fetch

    marketbeat = await service.provider("marketbeat_holidays", refresh="false")
    fed = await service.provider("investing_fed_rate_monitor", refresh="false")

    assert marketbeat["cache_used"] is True
    assert fed["cache_used"] is True
    assert marketbeat["provider_calls"] == 0
    assert fed["provider_calls"] == 0


def test_market_context_blocks_include_secondary_calendar_and_fed_monitor() -> None:
    blocks = build_multi_source_context_blocks(
        {
            "investing_holidays": {"status": "found", "relevant_holidays": [{"date": "2026-01-01", "holiday_name": "New Year's Day"}]},
            "marketbeat_holidays": {"status": "found", "relevant_holidays": [{"date": "2026-07-03", "holiday_name": "Independence Day"}]},
            "investing_fed_rate_monitor": {"status": "found", "meetings": [{"meeting_date": "2026-07-29"}]},
        }
    )

    assert blocks["market_calendar"]["market_holidays"]["secondary_sources"]["marketbeat"]["status"] == "found"
    assert len(blocks["market_calendar"]["market_holidays"]["merged_relevant_holidays"]) == 2
    assert blocks["market_schedule"]["holidays"][0]["holiday_name"] == "New Year's Day"
    assert blocks["rates_expectations"]["fed_funds_futures"]["investing_fed_rate_monitor"]["status"] == "found"


def test_market_schedule_uses_marketbeat_when_primary_holiday_source_is_empty() -> None:
    blocks = build_multi_source_context_blocks(
        {
            "investing_holidays": {"status": "not_found", "relevant_holidays": []},
            "marketbeat_holidays": {"status": "found", "relevant_holidays": [{"date": "2026-07-03", "holiday_name": "Independence Day"}]},
        }
    )

    assert blocks["market_schedule"]["holidays"][0]["holiday_name"] == "Independence Day"
    assert blocks["market_schedule"]["holiday_source"].get("source") is None or blocks["market_schedule"]["holiday_source"].get("status") == "found"


def test_new_provider_routes_are_registered() -> None:
    paths = {route.path for route in router.routes}

    assert "/providers/marketbeat/holidays" in paths
    assert "/providers/investing/fed-rate-monitor" in paths
