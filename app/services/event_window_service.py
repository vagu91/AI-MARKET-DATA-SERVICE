from datetime import UTC, datetime, timedelta

from app.models.common import Impact
from app.models.macro import EventWindow, EventWindowsResponse
from app.services.event_service import EventService


class EventWindowService:
    def __init__(self, event_service: EventService) -> None:
        self.event_service = event_service

    async def event_windows(self, symbol: str) -> EventWindowsResponse:
        now = datetime.now(UTC)
        events = await self.event_service.upcoming(country="US", days=1)
        active_windows: list[EventWindow] = []
        upcoming_windows: list[EventWindow] = []
        for event in events:
            if (
                event.incomplete_time
                or event.impact != Impact.HIGH
                or not event.time_utc
                or event.default_risk_window_before_minutes <= 0
                or event.default_risk_window_after_minutes <= 0
            ):
                continue
            start = event.time_utc.astimezone(UTC) - timedelta(
                minutes=event.default_risk_window_before_minutes
            )
            end = event.time_utc.astimezone(UTC) + timedelta(
                minutes=event.default_risk_window_after_minutes
            )
            window = EventWindow(
                event=event,
                window_start_utc=start.isoformat(),
                window_end_utc=end.isoformat(),
            )
            if start <= now <= end:
                active_windows.append(window)
            elif now < start:
                upcoming_windows.append(window)
        return EventWindowsResponse(
            symbol=symbol.upper(),
            checked_at_utc=now.isoformat(),
            active_event_windows=active_windows,
            upcoming_event_windows=upcoming_windows,
        )

