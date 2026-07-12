from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from time import perf_counter
from typing import Any

import httpx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.models.common import Freshness, Impact, ProviderResult, ProviderType
from app.models.nasdaq import EarningsTiming
from app.providers.base import BaseProvider, metadata
from app.providers.mega_cap_snapshot_provider import MEGA_CAP_TICKERS

logger = logging.getLogger(__name__)


class FmpEarningsCalendarProvider(BaseProvider):
    source = "Financial Modeling Prep Earnings Calendar"
    provider_type = ProviderType.API
    reliability = 0.82
    cache_key = "provider:fmp_earnings_calendar:v1"

    def __init__(self, cache: ProviderCacheProtocol, settings: Settings) -> None:
        super().__init__(cache)
        self.settings = settings

    async def fetch(self) -> ProviderResult:
        started = perf_counter()
        retrieved_at = datetime.now(UTC)
        if not self.settings.enable_fmp_earnings:
            return self._result("disabled", "fmp_earnings_disabled", retrieved_at, duration_ms=0)
        if not self.settings.fmp_api_key:
            logger.info("fmp_earnings_key_missing", extra={"provider": self.source, "actual_network_calls": 0})
            return self._result("not_configured", "fmp_api_key_missing", retrieved_at, duration_ms=0)

        headers = {
            "apikey": self.settings.fmp_api_key,
            "User-Agent": "AI-MARKET-DATA-SERVICE/1.0",
            "Accept": "application/json",
        }
        request_params = {
            "from": retrieved_at.date().isoformat(),
            "to": (retrieved_at.date() + timedelta(days=14)).isoformat(),
        }
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout_earnings_seconds) as client:
                response = await client.get(self.settings.fmp_earnings_calendar_url, headers=headers, params=request_params)
        except (httpx.TimeoutException, TimeoutError):
            return self._result("provider_timeout", "fmp_earnings_timeout", retrieved_at, duration_ms=_elapsed(started))
        except httpx.HTTPError as exc:
            return self._result("provider_failed", type(exc).__name__, retrieved_at, duration_ms=_elapsed(started), error=True)

        status = _http_status(response.status_code)
        if response.status_code != 200:
            logger.warning("fmp_earnings_http_status", extra={"provider": self.source, "http_status": response.status_code, "status": status})
            return self._result(status, f"fmp_http_{response.status_code}", retrieved_at, duration_ms=_elapsed(started), http_status=response.status_code)
        if not response.content:
            return self._result("not_found", "fmp_empty_body", retrieved_at, duration_ms=_elapsed(started), http_status=200)
        try:
            payload = json.loads(response.content)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return self._result("parse_failed", "fmp_invalid_json", retrieved_at, duration_ms=_elapsed(started), http_status=200, error=True)
        if not isinstance(payload, list):
            return self._result("parse_failed", "fmp_payload_not_list", retrieved_at, duration_ms=_elapsed(started), http_status=200, error=True)

        events, rejected = normalize_fmp_earnings(payload, retrieved_at=retrieved_at, days=14)
        logger.info(
            "fmp_earnings_fetched",
            extra={"provider": self.source, "endpoint": "/stable/earnings-calendar", "http_status": 200, "record_count": len(payload), "filtered_count": len(events), "rejected_count": rejected, "duration_ms": _elapsed(started)},
        )
        return ProviderResult(
            metadata=metadata(self.source, self.provider_type, self.reliability, freshness=Freshness.RECENT),
            data={
                "status": "found" if events else "not_found",
                "events": events,
                "data_quality": {
                    "errors": [],
                    "warnings": [] if events else ["fmp_no_relevant_earnings_in_14d"],
                    "fallback_used": False,
                    "final_data_available": bool(events),
                    "no_data_found": not events,
                    "provider_failed": False,
                    "rate_limited": False,
                    "provider_calls": 1,
                    "actual_network_calls": 1,
                    "cache_used": False,
                    "fetched_count": len(payload),
                    "validated_count": len(events),
                    "rejected_count": rejected,
                    "http_status": 200,
                    "provider_status": "found" if events else "not_found",
                    "duration_ms": _elapsed(started),
                },
            },
        )

    def _result(
        self,
        status: str,
        reason: str,
        retrieved_at: datetime,
        *,
        duration_ms: int,
        http_status: int | None = None,
        error: bool = False,
    ) -> ProviderResult:
        log = logger.info if status in {"not_configured", "disabled", "not_found"} else logger.warning
        log(
            "fmp_earnings_status",
            extra={"provider": self.source, "endpoint": "/stable/earnings-calendar", "status": status, "http_status": http_status, "duration_ms": duration_ms, "reason": reason},
        )
        return ProviderResult(
            metadata=metadata(
                self.source,
                self.provider_type,
                0.0,
                freshness=Freshness.UNKNOWN,
                errors=[reason] if error else [],
            ),
            data={
                "status": status,
                "events": [],
                "data_quality": {
                    "errors": [reason] if error else [],
                    "warnings": [] if error else [reason],
                    "fallback_used": False,
                    "final_data_available": False,
                    "no_data_found": status in {"not_found", "not_configured", "disabled"},
                    "provider_failed": status in {"provider_failed", "parse_failed", "auth_failed", "access_denied"},
                    "rate_limited": status == "rate_limited",
                    "provider_calls": 0 if status in {"not_configured", "disabled"} else 1,
                    "actual_network_calls": 0 if status in {"not_configured", "disabled"} else 1,
                    "cache_used": False,
                    "http_status": http_status,
                    "provider_status": status,
                    "duration_ms": duration_ms,
                    "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
                },
            },
        )


def normalize_fmp_earnings(rows: list[Any], *, retrieved_at: datetime, days: int) -> tuple[list[dict[str, Any]], int]:
    start = retrieved_at.date()
    end = start + timedelta(days=days)
    watchlist = set(MEGA_CAP_TICKERS)
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    rejected = 0
    for raw in rows:
        if not isinstance(raw, dict):
            rejected += 1
            continue
        symbol = str(raw.get("symbol") or "").strip().upper()
        event_date = _date(raw.get("date"))
        if symbol not in watchlist or event_date is None or not start <= event_date <= end:
            rejected += 1
            continue
        event = {
            "symbol": symbol,
            "company": raw.get("name") or raw.get("company"),
            "date": event_date.isoformat(),
            "timing": EarningsTiming.UNKNOWN.value,
            "eps_estimate": _number(raw.get("epsEstimated")),
            "eps_actual": _number(raw.get("epsActual")),
            "revenue_estimate": _number(raw.get("revenueEstimated")),
            "revenue_actual": _number(raw.get("revenueActual")),
            "provider_last_updated": raw.get("lastUpdated"),
            "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
            "source": "Financial Modeling Prep Earnings Calendar",
            "source_url": "https://financialmodelingprep.com/stable/earnings-calendar",
            "event_risk_level": Impact.HIGH.value if symbol in {"NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG", "TSLA"} else Impact.MEDIUM.value,
            "reliability": 0.82,
            "lineage": {
                field: {"source": "Financial Modeling Prep Earnings Calendar", "source_field": source_field}
                for field, source_field in {
                    "date": "date",
                    "eps_estimate": "epsEstimated",
                    "eps_actual": "epsActual",
                    "revenue_estimate": "revenueEstimated",
                    "revenue_actual": "revenueActual",
                }.items()
            },
        }
        selected[(symbol, event["date"])] = event
    return sorted(selected.values(), key=lambda item: (item["date"], item["symbol"])), rejected


def _http_status(code: int) -> str:
    return {
        400: "bad_request",
        401: "auth_failed",
        402: "plan_restricted",
        403: "access_denied",
        404: "endpoint_not_found",
        429: "rate_limited",
    }.get(code, "provider_failed")


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value or "")[:10])
    except ValueError:
        return None


def _number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _elapsed(started: float) -> int:
    return int((perf_counter() - started) * 1000)
