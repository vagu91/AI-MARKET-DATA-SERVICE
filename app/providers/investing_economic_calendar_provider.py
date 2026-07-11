from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS
from app.services.economic_value_parser import parse_economic_value


class InvestingEconomicCalendarProvider:
    source = "Investing Economic Calendar"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self, *, start: datetime | None = None, end: datetime | None = None) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_investing_calendar:
            return _status("disabled", "investing_calendar_disabled", started)
        start = start or started
        end = end or start + timedelta(days=self.settings.investing_calendar_lookahead_days)
        params: dict[str, Any] = {
            "domain_id": self.settings.investing_domain_id,
            "limit": self.settings.investing_calendar_page_limit,
            "start_date": start.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "end_date": end.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "country_ids": self.settings.investing_country_ids,
        }
        pages: list[dict[str, Any]] = []
        cursor = None
        try:
            async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
                for _ in range(10):
                    request_params = dict(params)
                    if cursor:
                        request_params["cursor"] = cursor
                    response = await asyncio.wait_for(
                        client.get(self.settings.investing_economic_calendar_api_url, params=request_params, headers=_json_headers()),
                        timeout=min(float(self.settings.http_timeout_seconds), 12.0),
                    )
                    response.raise_for_status()
                    payload = response.json()
                    pages.append(payload)
                    cursor = payload.get("next_page_cursor")
                    if not cursor:
                        break
        except TimeoutError:
            return _status("provider_timeout", "Investing calendar request timed out", started)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "Investing calendar request failed", started)

        events_by_id: dict[str, dict[str, Any]] = {}
        normalized: list[dict[str, Any]] = []
        rejected_future_actual = 0
        for page in pages:
            for event in page.get("events") or []:
                events_by_id[str(event.get("event_id"))] = event
            for occurrence in page.get("occurrences") or []:
                event = events_by_id.get(str(occurrence.get("event_id"))) or {}
                item = _normalize(event, occurrence, now=started)
                if item.get("status") == "REJECTED_TEMPORAL":
                    rejected_future_actual += 1
                    continue
                normalized.append(item)
        retrieved_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        valid_until = (datetime.now(UTC) + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        for item in normalized:
            item["consensus_retrieved_at"] = retrieved_at
            item["consensus_valid_until"] = valid_until
        return {
            "status": "found" if normalized else "not_found",
            "provider": self.source,
            "source": self.source,
            "source_url": self.settings.investing_economic_calendar_api_url,
            "retrieved_at": retrieved_at,
            "valid_until": valid_until,
            "items": normalized,
            "diagnostics": {
                "pages_fetched": len(pages),
                "events_seen": len(events_by_id),
                "occurrences_materialized": len(normalized),
                "rejected_future_actual": rejected_future_actual,
                "matched_count": 0,
                "ambiguous_count": 0,
                "unmatched_count": len(normalized),
            },
            "warnings": [] if normalized else ["investing_calendar_no_events"],
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def _normalize(event: dict[str, Any], occurrence: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    release_at = occurrence.get("occurrence_time")
    release_dt = _parse_dt(release_at)
    actual = parse_economic_value(occurrence.get("actual"), default_unit=occurrence.get("unit"))
    forecast = parse_economic_value(occurrence.get("forecast"), default_unit=occurrence.get("unit"))
    previous = parse_economic_value(occurrence.get("previous"), default_unit=occurrence.get("unit"))
    revised = parse_economic_value(occurrence.get("revised"), default_unit=occurrence.get("unit"))
    if release_dt and release_dt > now and actual["parse_status"] == "parsed":
        return {"status": "REJECTED_TEMPORAL", "reason": "future_actual_rejected"}
    return {
        "provider_event_id": event.get("event_id") or occurrence.get("event_id"),
        "occurrence_id": occurrence.get("occurrence_id"),
        "event_name": event.get("event_translated") or event.get("long_name") or event.get("short_name"),
        "category": event.get("category"),
        "country": "US" if str(event.get("country_id")) == "5" else None,
        "country_id": event.get("country_id"),
        "currency": event.get("currency"),
        "importance": event.get("importance"),
        "release_at": release_at,
        "actual": actual["value"] if actual["parse_status"] == "parsed" else None,
        "actual_is_official": False,
        "source_type": "secondary",
        "forecast": None,
        "consensus": forecast["value"] if forecast["parse_status"] == "parsed" else None,
        "consensus_verified": forecast["parse_status"] == "parsed",
        "consensus_origin": "investing_economic_calendar" if forecast["parse_status"] == "parsed" else None,
        "forecast_origin": None,
        "estimate_count": None,
        "estimate_low": None,
        "estimate_high": None,
        "median_estimate": None,
        "average_estimate": None,
        "previous": previous["value"] if previous["parse_status"] == "parsed" else None,
        "revised_previous": revised["value"] if revised["parse_status"] == "parsed" else None,
        "reference_period": occurrence.get("period") or occurrence.get("reference_period"),
        "unit": occurrence.get("unit") or forecast.get("unit") or previous.get("unit"),
        "frequency": _frequency(event.get("event_translated") or event.get("long_name") or event.get("short_name")),
        "source": "Investing Economic Calendar",
        "source_url": "https://www.investing.com/economic-calendar/",
        "consensus_source": "Investing Economic Calendar",
        "consensus_source_url": "https://www.investing.com/economic-calendar/",
        "official_release_source": event.get("source"),
        "official_release_source_url": event.get("source_url"),
        "status": "UNMATCHED",
        "raw_values": {
            "actual": occurrence.get("actual"),
            "forecast": occurrence.get("forecast"),
            "previous": occurrence.get("previous"),
            "revised": occurrence.get("revised"),
        },
    }


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    return {
        "status": status,
        "provider": "Investing Economic Calendar",
        "source": "Investing Economic Calendar",
        "source_url": "https://endpoints.investing.com/pd-instruments/v1/calendars/economic/events/occurrences",
        "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (datetime.now(UTC) + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "items": [],
        "diagnostics": {"pages_fetched": 0, "events_seen": 0, "occurrences_materialized": 0},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC)


def _json_headers() -> dict[str, str]:
    return {**REQUEST_HEADERS, "Accept": "application/json"}


def _frequency(value: Any) -> str | None:
    text = str(value or "").upper()
    for token, frequency in (("MOM", "MoM"), ("YOY", "YoY"), ("QOQ", "QoQ"), ("WOW", "WoW")):
        if token in text.replace("/", "").replace(" ", ""):
            return frequency
    return None
