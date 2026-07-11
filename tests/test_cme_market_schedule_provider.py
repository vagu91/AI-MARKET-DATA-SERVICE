from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
import respx

from app.core.config import Settings
from app.providers.cme_market_schedule_provider import (
    CmeMarketScheduleProvider,
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


def test_official_cme_crosscheck_replaces_static_source() -> None:
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
    assert mnq["source_classification"] == "official_cme_calendar"
    assert mnq["calendar_crosscheck_status"] == "verified"
    assert mnq["is_official_source"] is True
    assert schedule["warnings"] == []
