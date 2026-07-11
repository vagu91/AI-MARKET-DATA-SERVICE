from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo


NEW_YORK = ZoneInfo("America/New_York")
CASH_OPEN = time(9, 30)
CASH_CLOSE = time(16, 0)
FUTURES_OPEN_SUNDAY = time(18, 0)
FUTURES_DAILY_CLOSE = time(17, 0)
FUTURES_DAILY_REOPEN = time(18, 0)
SCHEDULE_VERSION = "us_equities_cme_globex_v1"


def build_session_aware_schedule(
    schedule: dict[str, Any] | None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = _aware(now or datetime.now(UTC))
    local = now.astimezone(NEW_YORK)
    existing = dict(schedule or {})
    holidays = list(existing.get("holidays") or [])
    closed_dates = {
        str(item.get("date"))
        for item in holidays
        if isinstance(item, dict) and str(item.get("session_status") or "").lower() == "closed"
    }
    early_closes = {
        str(item.get("date")): item
        for item in holidays
        if isinstance(item, dict) and str(item.get("session_status") or "").lower() == "early_close"
    }
    cash = _cash_session(local, closed_dates, early_closes)
    futures = _futures_session(local)
    last_session = _previous_cash_session(local.date(), closed_dates)
    next_holiday = next(
        (item for item in sorted(holidays, key=lambda row: str(row.get("date") or "")) if str(item.get("date") or "") >= local.date().isoformat()),
        None,
    )
    next_early_close = next(
        (item for key, item in sorted(early_closes.items()) if key >= local.date().isoformat()),
        None,
    )
    existing_cash = existing.get("nasdaq_cash_session") or {}
    official_cash = str(existing_cash.get("status") or "").lower() in {"found", "available"} and "nasdaq" in str(
        f"{existing_cash.get('source') or ''} {existing_cash.get('provider') or ''}"
    ).lower()
    cash_view = {**existing_cash, **cash}
    if official_cash:
        cash_view.update(
            {
                "source": existing_cash.get("source") or existing_cash.get("provider"),
                "data_origin_is_official": True,
                "distribution_source_is_official": True,
                "source_is_primary_originator": True,
                "source_is_official_redistributor": False,
                "is_official_source": True,
            }
        )
    return {
        **existing,
        "status": "AVAILABLE",
        "market_session_status": cash["status"],
        "last_market_session_date": last_session.isoformat(),
        "nasdaq_cash_session": cash_view,
        "cme_equity_futures_session": futures,
        "mnq_session": {**futures, "instrument": "MNQ", "venue": "CME Globex"},
        "next_holiday": next_holiday,
        "next_early_close": next_early_close,
        "calendar_source_ranking": [
            "official_cme_calendar",
            "official_cme_globex_calendar",
            "exchange_distributed_calendar",
            "secondary_calendar",
            "versioned_static_last_known_good",
        ],
        "schedule_version": SCHEDULE_VERSION,
        "data_origin_is_official": False,
        "distribution_source_is_official": False,
        "source_is_primary_originator": False,
        "source_is_official_redistributor": False,
        "source": _schedule_source(existing),
        "warnings": _schedule_warnings(existing),
    }


def last_market_session_date(schedule: dict[str, Any], *, now: datetime | None = None) -> str:
    if value := schedule.get("last_market_session_date"):
        return str(value)
    local = _aware(now or datetime.now(UTC)).astimezone(NEW_YORK)
    return _previous_cash_session(local.date(), set()).isoformat()


def is_market_closed(status: Any) -> bool:
    return str(status or "").lower() in {
        "weekend",
        "holiday",
        "market_closed",
        "closed",
        "maintenance_break",
    }


def _cash_session(
    local: datetime,
    closed_dates: set[str],
    early_closes: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    day = local.date()
    day_key = day.isoformat()
    if local.weekday() >= 5:
        status = "weekend"
    elif day_key in closed_dates:
        status = "holiday"
    else:
        close_at = _early_close_time(early_closes.get(day_key)) or CASH_CLOSE
        status = "open" if CASH_OPEN <= local.timetz().replace(tzinfo=None) < close_at else "market_closed"
    next_open_day = _next_cash_day(day, closed_dates, include_today=status == "market_closed" and local.time() < CASH_OPEN)
    close_time = _early_close_time(early_closes.get(next_open_day.isoformat())) or CASH_CLOSE
    current_close_time = _early_close_time(early_closes.get(day_key)) or CASH_CLOSE
    current_close = datetime.combine(day, current_close_time, NEW_YORK)
    return {
        "status": status,
        "market": "NASDAQ cash",
        "timezone": "America/New_York",
        "regular_trading_hours": {"open": "09:30:00", "close": "16:00:00"},
        "extended_trading_hours": {"pre_market_open": "04:00:00", "after_hours_close": "20:00:00"},
        "maintenance_break": None,
        "early_close": day_key in early_closes,
        "next_open": datetime.combine(next_open_day, CASH_OPEN, NEW_YORK).astimezone(UTC).isoformat(),
        "next_close": (
            current_close if status == "open" else datetime.combine(next_open_day, close_time, NEW_YORK)
        ).astimezone(UTC).isoformat(),
        "source": "exchange calendar with deterministic session rules",
        "freshness": "LIVE" if status == "open" else "CURRENT_SESSION",
    }


def _futures_session(local: datetime) -> dict[str, Any]:
    weekday = local.weekday()
    local_time = local.timetz().replace(tzinfo=None)
    if weekday == 5 or (weekday == 4 and local_time >= FUTURES_DAILY_CLOSE) or (weekday == 6 and local_time < FUTURES_OPEN_SUNDAY):
        status = "weekend"
    elif weekday in {0, 1, 2, 3} and FUTURES_DAILY_CLOSE <= local_time < FUTURES_DAILY_REOPEN:
        status = "maintenance_break"
    else:
        status = "open"
    next_open = _next_futures_open(local, status)
    next_close = _next_futures_close(local, status)
    return {
        "status": status,
        "market": "CME equity index futures",
        "timezone": "America/New_York",
        "regular_trading_hours": "Sunday 18:00 through Friday 17:00 ET",
        "extended_trading_hours": "Globex electronic session",
        "maintenance_break": {"start": "17:00:00", "end": "18:00:00", "days": "Monday-Thursday"},
        "holiday_schedule": "calendar-specific overrides required",
        "early_close": False,
        "next_open": next_open.astimezone(UTC).isoformat(),
        "next_close": next_close.astimezone(UTC).isoformat(),
        "source": "versioned CME Globex schedule fallback",
        "source_classification": "versioned_static_last_known_good",
        "freshness": "LIVE" if status == "open" else "CURRENT_SESSION",
    }


def _next_cash_day(day: date, closed_dates: set[str], *, include_today: bool = False) -> date:
    candidate = day if include_today else day + timedelta(days=1)
    for _ in range(14):
        if candidate.weekday() < 5 and candidate.isoformat() not in closed_dates:
            return candidate
        candidate += timedelta(days=1)
    return candidate


def _previous_cash_session(day: date, closed_dates: set[str]) -> date:
    candidate = day - timedelta(days=1)
    for _ in range(14):
        if candidate.weekday() < 5 and candidate.isoformat() not in closed_dates:
            return candidate
        candidate -= timedelta(days=1)
    return candidate


def _next_futures_open(local: datetime, status: str) -> datetime:
    if status == "maintenance_break":
        return datetime.combine(local.date(), FUTURES_DAILY_REOPEN, NEW_YORK)
    if status == "weekend":
        candidate = local.date()
        while candidate.weekday() != 6:
            candidate += timedelta(days=1)
        if candidate == local.date() and local.time() >= FUTURES_OPEN_SUNDAY:
            candidate += timedelta(days=7)
        return datetime.combine(candidate, FUTURES_OPEN_SUNDAY, NEW_YORK)
    return local


def _next_futures_close(local: datetime, status: str) -> datetime:
    if status != "open":
        opened = _next_futures_open(local, status)
        return datetime.combine(opened.date() + timedelta(days=1), FUTURES_DAILY_CLOSE, NEW_YORK)
    if local.time() < FUTURES_DAILY_CLOSE:
        return datetime.combine(local.date(), FUTURES_DAILY_CLOSE, NEW_YORK)
    return datetime.combine(local.date() + timedelta(days=1), FUTURES_DAILY_CLOSE, NEW_YORK)


def _early_close_time(item: dict[str, Any] | None) -> time | None:
    value = str((item or {}).get("early_close_time_local") or "")
    try:
        return time.fromisoformat(value) if value else None
    except ValueError:
        return None


def _schedule_source(schedule: dict[str, Any]) -> str:
    source = (schedule.get("holiday_source") or {}).get("source")
    return str(source or "versioned session rules")


def _schedule_warnings(schedule: dict[str, Any]) -> list[str]:
    source = str((schedule.get("holiday_source") or {}).get("source") or "").lower()
    return [] if any(token in source for token in ("cme", "nasdaq", "nyse")) else ["official_cme_calendar_crosscheck_unavailable"]


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value
