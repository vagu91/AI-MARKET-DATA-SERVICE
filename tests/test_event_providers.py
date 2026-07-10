from pathlib import Path

from app.core.cache import SQLiteCache
from app.core.config import Settings
from app.models.common import Impact
from app.models.events import EconomicEvent
from app.providers.bea_calendar import BeaReleaseScheduleProvider
from app.providers.bls_calendar import BlsReleaseCalendarProvider
from app.providers.fed_calendar import FederalReserveCalendarProvider


def settings(tmp_path: Path) -> Settings:
    return Settings(database_path=tmp_path / "cache.sqlite3")


def test_bls_upcoming_parser_uses_fixture_content_not_silent_mock(tmp_path) -> None:
    html = """
    <h1>July 2026</h1>
    <p>14</p>
    <p>Consumer Price Index</p>
    <p>June 2026</p>
    <p>08:30 AM</p>
    """
    provider = BlsReleaseCalendarProvider(SQLiteCache(tmp_path / "cache.sqlite3"), settings(tmp_path))

    events = [EconomicEvent.model_validate(item) for item in provider._parse_month(html, "https://bls.test", 2026, 7)]

    assert len(events) == 1
    assert events[0].name == "Consumer Price Index (June 2026)"
    assert events[0].source_url == "https://bls.test"
    assert events[0].impact == Impact.HIGH
    assert events[0].event_risk_level == Impact.HIGH
    assert events[0].default_risk_window_before_minutes == 30
    assert events[0].default_risk_window_after_minutes == 30
    assert events[0].time_utc is not None


def test_bea_release_schedule_parser_marks_high_impact_timed_events(tmp_path) -> None:
    html = """
    <p>Year 2026 Release</p>
    <p>July 30</p><p>8:30 AM</p><p>N ews</p>
    <p>GDP (Advance Estimate), 2nd Quarter 2026</p>
    """
    provider = BeaReleaseScheduleProvider(SQLiteCache(tmp_path / "cache.sqlite3"), settings(tmp_path))

    events = [EconomicEvent.model_validate(item) for item in provider._parse(html)]

    assert len(events) == 1
    assert events[0].category == "GDP"
    assert events[0].event_risk_level == Impact.HIGH
    assert events[0].default_risk_window_before_minutes == 30
    assert events[0].time_utc is not None


def test_fed_calendar_parser_reads_fomc_from_fixture(tmp_path) -> None:
    html = """
    <h4>FOMC Meetings</h4>
    <h6>Time:</h6><h6>Release Date(s):</h6>
    <p>2:00 p.m.</p><p>FOMC Meeting</p><p>Two-day meeting, July 28 - 29</p><p>29</p>
    """
    provider = FederalReserveCalendarProvider(SQLiteCache(tmp_path / "cache.sqlite3"), settings(tmp_path))

    events = [EconomicEvent.model_validate(item) for item in provider._parse_month(html, "https://fed.test", 2026, 7)]

    assert len(events) == 1
    assert events[0].category == "FOMC"
    assert events[0].impact == Impact.HIGH
    assert events[0].source_url == "https://fed.test"
    assert events[0].event_risk_level == Impact.HIGH
    assert events[0].default_risk_window_after_minutes == 30
