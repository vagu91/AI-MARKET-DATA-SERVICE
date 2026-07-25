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
    holidays_by_date = {
        str(item.get("date")): item
        for item in holidays
        if isinstance(item, dict) and item.get("date")
    }
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
    cash_holiday = holidays_by_date.get(local.date().isoformat())
    cash.update(
        {
            "holiday_name": (
                cash_holiday.get("holiday_name")
                or cash_holiday.get("name")
                or cash_holiday.get("title")
                if cash_holiday
                else None
            ),
        }
    )
    cme_calendar = existing.get("cme_calendar") or {}
    structured_cme = (
        cme_calendar.get("equity_index_schedule")
        if isinstance(cme_calendar.get("equity_index_schedule"), dict)
        else {}
    )
    official_document_discovered = bool(
        cme_calendar.get("official_document_discovered")
    )
    official_schedule_parsed = bool(
        cme_calendar.get("official_schedule_parsed") and structured_cme
    )
    schedule_covered = (
        official_schedule_parsed
        and str(structured_cme.get("coverage_start") or "")
        <= local.date().isoformat()
        <= str(structured_cme.get("coverage_end") or "")
    )
    futures_overrides = {
        str(item.get("date")): item
        for item in structured_cme.get("overrides") or []
        if isinstance(item, dict) and item.get("date")
    }
    futures = _futures_session(
        local,
        override=futures_overrides.get(local.date().isoformat()),
        schedule_verified=schedule_covered,
        holiday_sensitive=bool(cash_holiday),
    )
    if schedule_covered:
        futures.update(
            {
                "source": cme_calendar.get("source") or "CME Group Trading Hours",
                "source_url": cme_calendar.get("source_url"),
                "source_classification": "official_cme_calendar",
                "calendar_crosscheck_status": "verified",
                "official_document_discovered": official_document_discovered,
                "official_schedule_parsed": True,
                "session_state_verified": True,
                "data_origin_is_official": True,
                "distribution_source_is_official": True,
                "source_is_primary_originator": True,
                "source_is_official_redistributor": False,
                "is_official_source": True,
            }
        )
    else:
        futures.update(
            {
                "calendar_crosscheck_status": str(
                    cme_calendar.get("status") or "not_available"
                ),
                "official_document_discovered": official_document_discovered,
                "official_schedule_parsed": official_schedule_parsed,
                "session_state_verified": False,
                "data_origin_is_official": False,
                "distribution_source_is_official": False,
                "source_is_primary_originator": False,
                "source_is_official_redistributor": False,
                "is_official_source": False,
            }
        )
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
        "context_date": local.date().isoformat(),
        "market_session_status": cash["status"],
        "last_market_session_date": last_session.isoformat(),
        "nasdaq_cash_session": cash_view,
        "cme_equity_futures_session": futures,
        "mnq_session": {**futures, "instrument": "MNQ", "venue": "CME Globex"},
        "mnq_futures_session": {
            **futures,
            "instrument": "MNQ",
            "venue": "CME Globex",
        },
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
        "official_document_discovered": official_document_discovered,
        "official_schedule_parsed": official_schedule_parsed,
        "session_state_verified": bool(futures["session_state_verified"]),
        "data_origin_is_official": False,
        "distribution_source_is_official": False,
        "source_is_primary_originator": False,
        "source_is_official_redistributor": False,
        "source": _schedule_source(existing),
        "warnings": _schedule_warnings(
            existing,
            official_cme=bool(futures["session_state_verified"]),
        ),
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
    is_open = status == "open"
    return {
        "status": status,
        "is_open": is_open,
        "closed_reason": (
            None
            if is_open
            else "WEEKEND"
            if status == "weekend"
            else "HOLIDAY"
            if status == "holiday"
            else "OUTSIDE_REGULAR_HOURS"
        ),
        "holiday_name": None,
        "is_early_close": day_key in early_closes,
        "market": "NASDAQ cash",
        "timezone": "America/New_York",
        "regular_trading_hours": {"open": "09:30:00", "close": "16:00:00"},
        "extended_trading_hours": {"pre_market_open": "04:00:00", "after_hours_close": "20:00:00"},
        "maintenance_break": None,
        "early_close": day_key in early_closes,
        "next_open": datetime.combine(next_open_day, CASH_OPEN, NEW_YORK).astimezone(UTC).isoformat(),
        "next_open_at": datetime.combine(next_open_day, CASH_OPEN, NEW_YORK).astimezone(UTC).isoformat(),
        "next_close": (
            current_close if status == "open" else datetime.combine(next_open_day, close_time, NEW_YORK)
        ).astimezone(UTC).isoformat(),
        "source": "exchange calendar with deterministic session rules",
        "freshness": "LIVE" if status == "open" else "CURRENT_SESSION",
    }


def _futures_session(
    local: datetime,
    *,
    override: dict[str, Any] | None = None,
    schedule_verified: bool = False,
    holiday_sensitive: bool = False,
) -> dict[str, Any]:
    weekday = local.weekday()
    local_time = local.timetz().replace(tzinfo=None)
    if weekday == 5 or (weekday == 4 and local_time >= FUTURES_DAILY_CLOSE) or (weekday == 6 and local_time < FUTURES_OPEN_SUNDAY):
        status = "weekend"
    elif weekday in {0, 1, 2, 3} and FUTURES_DAILY_CLOSE <= local_time < FUTURES_DAILY_REOPEN:
        status = "maintenance_break"
    else:
        status = "open"
    holiday_name = None
    is_early_close = False
    closed_reason = None
    if schedule_verified and override:
        holiday_name = override.get("holiday_name")
        override_status = str(override.get("session_status") or "").lower()
        open_at = _override_time(override, "open_time_local")
        close_at = _override_time(override, "close_time_local")
        if override_status == "closed":
            status = "holiday"
            closed_reason = "HOLIDAY"
        elif override_status == "early_close":
            is_early_close = True
            if close_at is not None and local_time >= close_at:
                status = "holiday_closed"
                closed_reason = "EARLY_CLOSE"
        elif override_status == "late_open" and open_at is not None:
            if local_time < open_at:
                status = "late_open"
                closed_reason = "LATE_OPEN"
            elif weekday != 5:
                status = "open"
        elif override_status == "modified":
            maintenance_start = _override_time(
                override,
                "maintenance_start_local",
            )
            maintenance_end = _override_time(
                override,
                "maintenance_end_local",
            )
            if (
                maintenance_start is not None
                and maintenance_end is not None
                and maintenance_start <= local_time < maintenance_end
            ):
                status = "maintenance_break"
                closed_reason = "MAINTENANCE_BREAK"
    elif holiday_sensitive and status == "open":
        status = "unknown"
        closed_reason = "UNVERIFIED_HOLIDAY_SCHEDULE"
    is_open: bool | None = status == "open" if status != "unknown" else None
    if closed_reason is None and is_open is False:
        closed_reason = (
            "MAINTENANCE_BREAK"
            if status == "maintenance_break"
            else "WEEKEND"
            if status == "weekend"
            else "HOLIDAY"
            if status in {"holiday", "holiday_closed"}
            else "UNVERIFIED_HOLIDAY_SCHEDULE"
            if status == "unknown"
            else "SESSION_CLOSED"
        )
    next_open = (
        _next_futures_open(local, status)
        if status not in {"unknown", "holiday", "holiday_closed", "late_open"}
        else None
    )
    next_close = (
        _next_futures_close(local, status)
        if next_open is not None
        else None
    )
    return {
        "status": status,
        "is_open": is_open,
        "closed_reason": closed_reason,
        "holiday_name": holiday_name,
        "is_early_close": is_early_close,
        "market": "CME equity index futures",
        "timezone": "America/New_York",
        "regular_trading_hours": "Sunday 18:00 through Friday 17:00 ET",
        "extended_trading_hours": "Globex electronic session",
        "maintenance_break": {"start": "17:00:00", "end": "18:00:00", "days": "Monday-Thursday"},
        "holiday_schedule": "calendar-specific overrides required",
        "early_close": is_early_close,
        "next_open": (
            next_open.astimezone(UTC).isoformat() if next_open else None
        ),
        "next_open_at": (
            next_open.astimezone(UTC).isoformat() if next_open else None
        ),
        "next_close": (
            next_close.astimezone(UTC).isoformat() if next_close else None
        ),
        "source": "versioned CME Globex schedule fallback",
        "source_classification": "versioned_static_last_known_good",
        "official_document_discovered": False,
        "official_schedule_parsed": False,
        "session_state_verified": False,
        "data_origin_is_official": False,
        "is_official_source": False,
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


def _override_time(item: dict[str, Any], key: str) -> time | None:
    value = item.get(key)
    try:
        return time.fromisoformat(str(value)) if value else None
    except ValueError:
        return None


def _schedule_source(schedule: dict[str, Any]) -> str:
    source = (schedule.get("holiday_source") or {}).get("source")
    return str(source or "versioned session rules")


def _schedule_warnings(schedule: dict[str, Any], *, official_cme: bool = False) -> list[str]:
    if official_cme:
        return []
    source = str((schedule.get("holiday_source") or {}).get("source") or "").lower()
    return [] if any(token in source for token in ("cme", "nasdaq", "nyse")) else ["official_cme_calendar_crosscheck_unavailable"]


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value
