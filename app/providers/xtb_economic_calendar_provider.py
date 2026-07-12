from __future__ import annotations

import json
import logging
import re
import unicodedata
from datetime import UTC, date, datetime, time, timedelta, timezone
from time import perf_counter
from typing import Any

import httpx

from app.core.config import Settings

logger = logging.getLogger(__name__)


class XtbEconomicCalendarProvider:
    source = "XTB Economic Calendar"
    reliability = 0.80

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = perf_counter()
        retrieved_at = datetime.now(UTC)
        if not self.settings.enable_xtb_calendar:
            return _status("disabled", "xtb_calendar_disabled", retrieved_at, duration_ms=0)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
            "Referer": "https://www.xtb.com/it/calendario-economico",
        }
        try:
            async with httpx.AsyncClient(timeout=self.settings.xtb_calendar_timeout_seconds) as client:
                response = await client.get(self.settings.xtb_economic_calendar_url, headers=headers)
        except (httpx.TimeoutException, TimeoutError):
            return _status("provider_timeout", "xtb_calendar_timeout", retrieved_at, duration_ms=_elapsed(started))
        except httpx.HTTPError as exc:
            return _status("provider_failed", type(exc).__name__, retrieved_at, duration_ms=_elapsed(started), error=True)

        if response.status_code != 200:
            status = {
                400: "bad_request",
                403: "access_denied",
                404: "endpoint_not_found",
                406: "not_acceptable",
                429: "rate_limited",
            }.get(response.status_code, "provider_failed")
            logger.warning("xtb_calendar_http_status", extra={"provider": self.source, "http_status": response.status_code, "status": status})
            return _status(status, f"xtb_http_{response.status_code}", retrieved_at, duration_ms=_elapsed(started), http_status=response.status_code)
        if not response.content:
            return _status("not_found", "xtb_empty_body", retrieved_at, duration_ms=_elapsed(started), http_status=200)
        try:
            payload = parse_xtb_payload(response.content)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            return _status("parse_failed", "xtb_invalid_json", retrieved_at, duration_ms=_elapsed(started), http_status=200, error=True)
        if "items" not in payload:
            return _status("parse_failed", "xtb_items_missing", retrieved_at, duration_ms=_elapsed(started), http_status=200, error=True)
        rows = payload.get("items")
        if not isinstance(rows, list):
            return _status("parse_failed", "xtb_items_not_list", retrieved_at, duration_ms=_elapsed(started), http_status=200, error=True)

        items, rejected = normalize_xtb_events(
            rows,
            retrieved_at=retrieved_at,
            minimum_impact=self.settings.xtb_calendar_min_impact,
            lookahead_days=self.settings.xtb_calendar_lookahead_days,
        )
        valid_until = (retrieved_at + timedelta(minutes=self.settings.xtb_calendar_ttl_minutes)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        logger.info(
            "xtb_calendar_fetched",
            extra={"provider": self.source, "endpoint": "/web-api/v3/languages/it/market-calendars", "http_status": 200, "record_count": len(rows), "filtered_count": len(items), "rejected_count": rejected, "duration_ms": _elapsed(started)},
        )
        return {
            "status": "found" if items else "not_found",
            "provider": self.source,
            "source": self.source,
            "source_url": "https://www.xtb.com/it/calendario-economico",
            "retrieved_at": retrieved_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "valid_until": valid_until,
            "items": items,
            "reliability": self.reliability,
            "source_classification": "secondary_market_calendar",
            "official_source": False,
            "diagnostics": {
                "http_status": 200,
                "content_type": response.headers.get("content-type"),
                "fetched_count": len(rows),
                "filtered_count": len(items),
                "rejected_count": rejected,
                "us_event_count": len(items),
                "impact_2_count": sum(1 for item in items if item.get("importance") == 2),
                "impact_3_count": sum(1 for item in items if item.get("importance") == 3),
                "actual_count": sum(1 for item in items if item.get("actual") is not None),
                "consensus_count": sum(1 for item in items if item.get("consensus") is not None),
                "previous_count": sum(1 for item in items if item.get("previous") is not None),
                "actual_network_calls": 1,
            },
            "warnings": [] if items else ["xtb_no_relevant_us_events"],
            "errors": [],
            "duration_ms": _elapsed(started),
        }


def parse_xtb_payload(payload: bytes | str) -> dict[str, Any]:
    value = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise TypeError("xtb_payload_not_object")
    return parsed


def normalize_xtb_events(
    rows: list[Any],
    *,
    retrieved_at: datetime,
    minimum_impact: int,
    lookahead_days: int,
) -> tuple[list[dict[str, Any]], int]:
    start = retrieved_at.date() - timedelta(days=1)
    end = retrieved_at.date() + timedelta(days=lookahead_days)
    selected: dict[tuple[Any, ...], dict[str, Any]] = {}
    rejected = 0
    for raw in rows:
        if not isinstance(raw, dict):
            rejected += 1
            continue
        country_code = str(raw.get("countryCode") or "").upper()
        event_date = _date(raw.get("date"))
        impact = _integer(raw.get("impact"))
        if country_code != "US" or event_date is None or impact is None or impact < minimum_impact or not start <= event_date <= end:
            rejected += 1
            continue
        title = str(raw.get("title") or "").strip()
        if not title:
            rejected += 1
            continue
        local_dt, utc_dt = xtb_event_datetimes(event_date, raw.get("timeShortFormat"), raw.get("timezoneOffset"))
        event_type, metric_id = classify_xtb_event(title, raw.get("indicatorId"), raw.get("evaluationMethod"), raw.get("unit"))
        forecast = _number(raw.get("forecast"))
        previous = _number(raw.get("previous"))
        observed_actual = _number(raw.get("current"))
        released = bool((utc_dt and utc_dt <= retrieved_at) or (utc_dt is None and event_date < retrieved_at.date()))
        actual = observed_actual if released else None
        source_event_id = str(raw.get("id") or "").strip() or None
        indicator_id = str(raw.get("indicatorId") or "").strip() or None
        event = {
            "occurrence_id": f"xtb:{source_event_id or indicator_id or title}:{event_date.isoformat()}",
            "source_event_id": source_event_id,
            "indicator_id": indicator_id,
            "event_name": title,
            "original_title": title,
            "normalized_event_type": event_type,
            "metric_id": metric_id,
            "country": "US",
            "country_code": country_code,
            "language": raw.get("language"),
            "date": event_date.isoformat(),
            "time_local": local_dt.isoformat() if local_dt else None,
            "release_at": utc_dt.isoformat().replace("+00:00", "Z") if utc_dt else None,
            "all_day": local_dt is None,
            "timezone_offset_seconds": _integer(raw.get("timezoneOffset")),
            "importance": impact,
            "impact": {0: "NONE", 1: "LOW", 2: "MEDIUM", 3: "HIGH"}.get(impact, "UNKNOWN"),
            "numerical_event": bool(raw.get("numericalEvent")),
            "forecast": None,
            "forecast_display": raw.get("forecastString"),
            "consensus": forecast,
            "consensus_verified": forecast is not None,
            "consensus_origin": "xtb_economic_calendar",
            "previous": previous,
            "previous_display": raw.get("previousString"),
            "actual": actual,
            "actual_display": raw.get("currentString") if actual is not None else None,
            "actual_is_official": False,
            "reference_period": raw.get("period"),
            "evaluation_method": raw.get("evaluationMethod"),
            "unit": raw.get("unit"),
            "order_of_magnitude": raw.get("orderOfMagnitude"),
            "currency": raw.get("currency"),
            "modifications": raw.get("modifications"),
            "status": raw.get("status") or "UNMATCHED",
            "source": "XTB Economic Calendar",
            "source_url": "https://www.xtb.com/it/calendario-economico",
            "consensus_source": "XTB Economic Calendar",
            "consensus_source_url": "https://www.xtb.com/it/calendario-economico",
            "provider_type": "PUBLIC_HTTP",
            "reliability": 0.80,
            "retrieved_at": retrieved_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "lineage": {
                "actual": {"source": "XTB Economic Calendar", "source_field": "current"},
                "consensus": {"source": "XTB Economic Calendar", "source_field": "forecast"},
                "previous": {"source": "XTB Economic Calendar", "source_field": "previous"},
                "release_at": {"source": "XTB Economic Calendar", "source_fields": ["date", "timeShortFormat", "timezoneOffset"]},
            },
            "warnings": ["future_actual_rejected"] if observed_actual is not None and actual is None else [],
        }
        key = (source_event_id or indicator_id or _normalized(title), event["date"], event["release_at"])
        selected[key] = event
    return sorted(selected.values(), key=lambda item: (item["date"], item.get("release_at") or "", item["event_name"])), rejected


def xtb_event_datetimes(event_date: date, short_time: Any, offset_value: Any) -> tuple[datetime | None, datetime | None]:
    if short_time in (None, ""):
        return None, None
    parsed_time = _time(short_time)
    if parsed_time is None:
        return None, None
    offset_seconds = _integer(offset_value) or 0
    local_tz = timezone(timedelta(seconds=offset_seconds))
    local_dt = datetime.combine(event_date, parsed_time, tzinfo=local_tz)
    return local_dt, local_dt.astimezone(UTC)


def classify_xtb_event(title: str, indicator_id: Any, evaluation_method: Any, unit: Any) -> tuple[str, str | None]:
    text = _normalized(title)
    frequency = "mom" if any(token in text for token in ("m m", "mensile", "month over month")) else "yoy" if any(token in text for token in ("a a", "y y", "annuale", "year over year")) else None
    core = any(token in text for token in ("core", "di fondo", "base"))
    if "cpi" in text or "consumer price index" in text or "prezzi al consumo" in text:
        metric = f"{'core' if core else 'headline'}_cpi_{frequency}" if frequency else None
        return ("CORE_CPI" if core else "CPI") + (f"_{frequency.upper()}" if frequency else ""), metric
    if "ppi" in text or "producer price index" in text or "prezzi alla produzione" in text:
        metric = f"{'core' if core else 'headline'}_ppi_{frequency}" if frequency else None
        return ("CORE_PPI" if core else "PPI") + (f"_{frequency.upper()}" if frequency else ""), metric
    if "initial jobless claims" in text or "richieste iniziali" in text or "sussidi di disoccupazione" in text:
        return "INITIAL_JOBLESS_CLAIMS", "initial_jobless_claims"
    mappings = (
        (("retail sales", "vendite al dettaglio"), "RETAIL_SALES"),
        (("philadelphia fed", "fed di philadelphia"), "PHILADELPHIA_FED"),
        (("empire state",), "NY_EMPIRE_STATE"),
        (("housing starts", "nuovi cantieri"), "HOUSING_STARTS"),
        (("building permits", "permessi di costruzione"), "BUILDING_PERMITS"),
        (("industrial production", "produzione industriale"), "INDUSTRIAL_PRODUCTION"),
        (("capacity utilization", "utilizzo della capacita"), "CAPACITY_UTILIZATION"),
        (("university of michigan", "universita del michigan"), "UNIVERSITY_OF_MICHIGAN"),
        (("beige book", "beige book"), "BEIGE_BOOK"),
        (("fomc",), "FOMC"),
        (("fed chair", "presidente fed", "membro fomc", "fomc member"), "FED_COMMUNICATION"),
    )
    for tokens, event_type in mappings:
        if any(token in text for token in tokens):
            return event_type, None
    indicator = str(indicator_id or "").strip()
    method = _normalized(evaluation_method)
    unit_text = _normalized(unit)
    suffix = indicator or method or unit_text or "unclassified"
    return f"XTB_INDICATOR_{suffix}", None


def _status(
    status: str,
    reason: str,
    retrieved_at: datetime,
    *,
    duration_ms: int,
    http_status: int | None = None,
    error: bool = False,
) -> dict[str, Any]:
    log = logger.info if status in {"disabled", "not_found"} else logger.warning
    log(
        "xtb_calendar_status",
        extra={"provider": "XTB Economic Calendar", "endpoint": "/web-api/v3/languages/it/market-calendars", "status": status, "http_status": http_status, "duration_ms": duration_ms, "reason": reason},
    )
    return {
        "status": status,
        "provider": "XTB Economic Calendar",
        "source": "XTB Economic Calendar",
        "source_url": "https://www.xtb.com/it/calendario-economico",
        "retrieved_at": retrieved_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (retrieved_at + timedelta(minutes=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "items": [],
        "diagnostics": {"http_status": http_status, "actual_network_calls": 0 if status == "disabled" else 1, "reason": reason},
        "warnings": [] if error else [reason],
        "errors": [reason] if error else [],
        "duration_ms": duration_ms,
        "reliability": 0.0,
        "official_source": False,
    }


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value or "")[:10])
    except ValueError:
        return None


def _time(value: Any) -> time | None:
    text = str(value or "").strip()
    for pattern in ("%H:%M", "%H:%M:%S", "%I:%M %p"):
        try:
            return datetime.strptime(text, pattern).time()
        except ValueError:
            continue
    return None


def _integer(value: Any) -> int | None:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(str(value).replace(" ", "").replace(",", ".").replace("%", ""))
    except ValueError:
        return None


def _normalized(value: Any) -> str:
    ascii_value = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", ascii_value.lower()).strip()


def _elapsed(started: float) -> int:
    return int((perf_counter() - started) * 1000)
