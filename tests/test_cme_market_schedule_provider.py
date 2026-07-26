from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from app.core.config import Settings
from app.providers.cme_market_schedule_provider import (
    CmeMarketScheduleProvider,
    parse_cme_equity_index_schedule,
    parse_cme_trading_hours_page,
)
from app.services.market_session_service import build_session_aware_schedule


HTML = """
<html><body>
  <h1>CME Group Holiday and Trading Hours</h1>
  <p>2026 CME Globex Trading holiday schedules and Regular Trading Hours.</p>
  <a href="/tools-information/holiday-calendar/files/2026-globex-holiday-schedule.pdf">
    2026 CME Globex holiday schedule
  </a>
</body></html>
"""
FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "cme"
    / "equity-index-holiday-schedule-redacted.html"
)


def test_cme_official_page_parser_discovers_schedule_documents() -> None:
    parsed = parse_cme_trading_hours_page(HTML, base_url="https://www.cmegroup.com/trading-hours.html")
    assert parsed["calendar_verified"] is True
    assert parsed["globex_schedule_present"] is True
    assert parsed["regular_trading_hours_present"] is True
    assert parsed["documents"][0]["url"].startswith("https://www.cmegroup.com/")


@pytest.mark.asyncio
async def test_cme_provider_disabled_has_zero_network(tmp_path) -> None:
    cfg = Settings(_env_file=None, database_path=tmp_path / "market.sqlite", enable_cme_market_schedule=False)
    result = await CmeMarketScheduleProvider(cfg).fetch()
    assert result["status"] == "disabled"
    assert result["provider_calls"] == 0
    assert result["actual_network_calls"] == 0


@pytest.mark.asyncio
async def test_cme_provider_fetches_official_page_once(tmp_path) -> None:
    url = "https://cme.test/trading-hours"
    cfg = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        cme_market_schedule_url=url,
    )
    with respx.mock(assert_all_called=True) as router:
        router.get(url).mock(return_value=httpx.Response(200, text=HTML))
        result = await CmeMarketScheduleProvider(cfg).fetch()
    assert result["status"] == "found"
    assert result["calendar_verified"] is True
    assert result["is_official_source"] is True
    assert result["actual_network_calls"] == 1


@pytest.mark.asyncio
async def test_cme_provider_separates_discovery_parsing_and_session_verification(
    tmp_path,
) -> None:
    url = "https://cme.test/trading-hours"
    cfg = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        cme_market_schedule_url=url,
    )
    fixture = FIXTURE.read_text(encoding="utf-8")
    with respx.mock(assert_all_called=True) as router:
        router.get(url).mock(return_value=httpx.Response(200, text=fixture))
        result = await CmeMarketScheduleProvider(cfg).fetch()

    assert result["official_document_discovered"] is True
    assert result["official_schedule_parsed"] is True
    assert result["session_state_verified"] is False
    assert result["data_origin_is_official"] is True


def test_document_discovery_without_parsed_schedule_never_claims_official_state() -> None:
    schedule = build_session_aware_schedule(
        {
            "cme_calendar": {
                "status": "found",
                "calendar_verified": True,
                "source": "CME Group Trading Hours",
                "source_url": "https://www.cmegroup.com/trading-hours.html",
            }
        },
        now=datetime(2026, 7, 11, 12, tzinfo=UTC),
    )
    mnq = schedule["mnq_session"]
    assert mnq["source_classification"] == "versioned_static_last_known_good"
    assert mnq["calendar_crosscheck_status"] == "found"
    assert mnq["session_state_verified"] is False
    assert mnq["is_official_source"] is False


def test_redacted_official_fixture_parses_equity_index_overrides() -> None:
    parsed = parse_cme_equity_index_schedule(
        FIXTURE.read_text(encoding="utf-8")
    )

    assert parsed is not None
    assert parsed["schema"] == "cme_equity_index_schedule_v1"
    assert len(parsed["overrides"]) == 3
    assert parsed["overrides"][1]["session_status"] == "early_close"


def test_official_cme_holiday_override_closes_mnq_without_overclaim() -> None:
    parsed = parse_cme_equity_index_schedule(
        FIXTURE.read_text(encoding="utf-8")
    )
    schedule = build_session_aware_schedule(
        {
            "holidays": [
                {
                    "date": "2026-01-01",
                    "session_status": "closed",
                    "holiday_name": "New Year's Day",
                }
            ],
            "cme_calendar": {
                "status": "found",
                "source": "CME Group Trading Hours",
                "source_url": "https://www.cmegroup.com/trading-hours.html",
                "official_document_discovered": True,
                "official_schedule_parsed": True,
                "equity_index_schedule": parsed,
            },
        },
        now=datetime(2026, 1, 1, 15, tzinfo=UTC),
    )

    cash = schedule["nasdaq_cash_session"]
    mnq = schedule["mnq_futures_session"]
    assert cash["closed_reason"] == "HOLIDAY"
    assert mnq["status"] == "holiday"
    assert mnq["holiday_name"] == "New Year's Day"
    assert mnq["official_document_discovered"] is True
    assert mnq["official_schedule_parsed"] is True
    assert mnq["session_state_verified"] is True
    assert mnq["data_origin_is_official"] is True


def test_observed_cash_holiday_can_leave_verified_mnq_open_before_early_close() -> None:
    parsed = parse_cme_equity_index_schedule(
        FIXTURE.read_text(encoding="utf-8")
    )
    schedule = build_session_aware_schedule(
        {
            "holidays": [
                {
                    "date": "2026-07-03",
                    "session_status": "closed",
                    "holiday_name": "Independence Day observed",
                }
            ],
            "cme_calendar": {
                "status": "found",
                "official_document_discovered": True,
                "official_schedule_parsed": True,
                "equity_index_schedule": parsed,
            },
        },
        now=datetime(2026, 7, 3, 16, tzinfo=UTC),
    )

    assert schedule["nasdaq_cash_session"]["is_open"] is False
    assert schedule["mnq_futures_session"]["is_open"] is True
    assert schedule["mnq_futures_session"]["is_early_close"] is True
    assert schedule["mnq_futures_session"]["session_state_verified"] is True


def test_verified_cme_early_close_stops_mnq_after_official_close() -> None:
    parsed = parse_cme_equity_index_schedule(
        FIXTURE.read_text(encoding="utf-8")
    )
    schedule = build_session_aware_schedule(
        {
            "cme_calendar": {
                "status": "found",
                "official_document_discovered": True,
                "official_schedule_parsed": True,
                "equity_index_schedule": parsed,
            }
        },
        now=datetime(2026, 11, 27, 19, tzinfo=UTC),
    )

    mnq = schedule["mnq_futures_session"]
    assert mnq["status"] == "holiday_closed"
    assert mnq["is_open"] is False
    assert mnq["closed_reason"] == "EARLY_CLOSE"
    assert mnq["is_early_close"] is True
    assert mnq["session_state_verified"] is True


def test_unavailable_cme_calendar_is_unknown_on_holiday_sensitive_date() -> None:
    schedule = build_session_aware_schedule(
        {
            "holidays": [
                {
                    "date": "2026-07-03",
                    "session_status": "closed",
                    "holiday_name": "Independence Day observed",
                }
            ],
            "cme_calendar": {
                "status": "provider_failed",
                "official_document_discovered": False,
                "official_schedule_parsed": False,
            },
        },
        now=datetime(2026, 7, 3, 16, tzinfo=UTC),
    )

    mnq = schedule["mnq_futures_session"]
    assert mnq["status"] == "unknown"
    assert mnq["is_open"] is None
    assert mnq["session_reason"] == "UNVERIFIED_HOLIDAY_SCHEDULE"
    assert mnq["closed_reason"] == "UNVERIFIED_HOLIDAY_SCHEDULE"
    assert mnq["session_state_verified"] is False
    assert mnq["data_origin_is_official"] is False


def test_malformed_structured_cme_schedule_fails_closed() -> None:
    malformed = """
    <script data-cme-equity-index-schedule type="application/json">
      {"coverage_start":"2026-01-01","overrides":[{"date":"bad"}]}
    </script>
    """
    assert parse_cme_equity_index_schedule(malformed) is None
