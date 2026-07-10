from datetime import UTC, datetime, timedelta

import pytest

from app.models.common import Impact
from app.models.events import EconomicEvent
from app.services.event_window_service import EventWindowService


class FakeEventService:
    def __init__(self, events: list[EconomicEvent]) -> None:
        self.events = events

    async def upcoming(self, country: str = "US", days: int = 1) -> list[EconomicEvent]:
        return self.events


def make_event(**overrides) -> EconomicEvent:
    now = datetime.now(UTC)
    values = {
        "event_id": "evt-1",
        "name": "FOMC",
        "country": "US",
        "category": "FOMC",
        "date": now.date().isoformat(),
        "time_utc": now,
        "time_local": now,
        "impact": Impact.HIGH,
        "source": "test",
        "source_url": "https://example.com",
        "reliability": 0.9,
        "event_risk_level": Impact.HIGH,
        "default_risk_window_before_minutes": 30,
        "default_risk_window_after_minutes": 30,
    }
    values.update(overrides)
    return EconomicEvent(**values)


@pytest.mark.asyncio
async def test_event_windows_detects_active_high_impact_window() -> None:
    service = EventWindowService(FakeEventService([make_event()]))

    response = await service.event_windows("MNQ")

    assert response.symbol == "MNQ"
    assert len(response.active_event_windows) == 1
    assert response.upcoming_event_windows == []
    assert response.note == "Data only. Trading decisions are delegated to AI-TRADER."


@pytest.mark.asyncio
async def test_event_windows_ignores_events_without_time() -> None:
    event = make_event(time_utc=None, time_local=None)
    service = EventWindowService(FakeEventService([event]))

    response = await service.event_windows("MNQ")

    assert response.active_event_windows == []
    assert response.upcoming_event_windows == []


@pytest.mark.asyncio
async def test_event_windows_ignores_old_window() -> None:
    old_event = make_event(time_utc=datetime.now(UTC) - timedelta(hours=3))
    service = EventWindowService(FakeEventService([old_event]))

    response = await service.event_windows("MNQ")

    assert response.active_event_windows == []

