from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Iterable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx

from app.core.config import Settings
from app.core.senior_analyst_policy import (
    MNQ_EARNINGS_SELECTION_POLICY,
    MNQ_PRIMARY_SYMBOLS,
)
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, ProviderDisabled, ProviderError, metadata
from app.providers.deterministic import (
    DeterministicHttpClient,
    DeterministicProviderError,
    as_list,
    safe_payload_hash,
)


NEW_YORK = ZoneInfo("America/New_York")
EARNINGS_SYMBOLS = MNQ_PRIMARY_SYMBOLS
SESSION_CODES = {"bmo": "BMO", "amc": "AMC", "dmh": "DMH"}


class FinnhubProvider(BaseProvider):
    source = "FINNHUB"
    provider_type = ProviderType.API
    reliability = 0.86
    cache_key = "provider:finnhub:earnings_and_news"

    def __init__(
        self,
        cache: ProviderCacheProtocol,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock=lambda: datetime.now(UTC),
    ) -> None:
        super().__init__(cache)
        self.settings = settings
        self.clock = clock
        self.http = DeterministicHttpClient(
            allowed_hosts={"finnhub.io"},
            timeout_seconds=settings.finnhub_timeout_seconds,
            retry_attempts=settings.finnhub_retry_attempts,
            allowed_methods={"GET"},
            transport=transport,
            clock=clock,
        )

    async def fetch(self) -> ProviderResult:
        now = self.clock()
        start = now.date()
        end = start + timedelta(
            days=MNQ_EARNINGS_SELECTION_POLICY.lookahead_days
        )
        earnings = await self.earnings_calendar(start=start, end=end)
        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=now,
                freshness=Freshness.RECENT,
            ),
            data={
                "earnings": earnings,
                "company_news_candidates": [],
                "lineage": {
                    "provider": "FINNHUB",
                    "authority_tier": 3,
                    "news_is_candidate_only": True,
                    "trigger_class": "CACHE_UNTIL_DUE",
                },
            },
        )

    def _ready(self) -> None:
        if not self.settings.finnhub_enabled:
            raise ProviderDisabled("Finnhub provider is disabled")
        if not self.settings.finnhub_api_key:
            raise ProviderError("Finnhub API key is not configured")

    async def company_profile(self, symbol: str) -> dict[str, Any]:
        self._ready()
        normalized = _symbol(symbol)
        payload, telemetry, request = await self.http.request(
            "GET",
            f"{self.settings.finnhub_base_url.rstrip('/')}/stock/profile2",
            endpoint_category="company_profile",
            provider=self.source,
            params={"symbol": normalized},
            headers={"X-Finnhub-Token": str(self.settings.finnhub_api_key)},
        )
        if not isinstance(payload, dict) or not payload.get("ticker"):
            raise DeterministicProviderError("Finnhub profile is empty or malformed")
        ticker = _symbol(payload.get("ticker"))
        if ticker != normalized:
            raise DeterministicProviderError("Finnhub profile symbol mismatch")
        return {
            "symbol": ticker,
            "name": payload.get("name"),
            "exchange": payload.get("exchange"),
            "currency": payload.get("currency"),
            "country": payload.get("country"),
            "industry": payload.get("finnhubIndustry"),
            "ipo": payload.get("ipo"),
            "logo": payload.get("logo"),
            "weburl": payload.get("weburl"),
            "retrieved_at": self.clock().isoformat(),
            "source": self.source,
            "source_url": request["source_url"],
            "request_fingerprint": request["request_fingerprint"],
            "raw_payload_hash": safe_payload_hash(payload),
            "telemetry": telemetry.as_dict(),
        }

    async def earnings_calendar(
        self,
        *,
        start: date,
        end: date,
        symbols: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        self._ready()
        if end < start:
            raise ValueError("earnings calendar end precedes start")
        requested_symbols = {_symbol(item) for item in (symbols or EARNINGS_SYMBOLS)}
        payload, telemetry, request = await self.http.request(
            "GET",
            f"{self.settings.finnhub_base_url.rstrip('/')}/calendar/earnings",
            endpoint_category="earnings_calendar",
            provider=self.source,
            params={"from": start.isoformat(), "to": end.isoformat()},
            headers={"X-Finnhub-Token": str(self.settings.finnhub_api_key)},
        )
        rows = as_list(payload, field_name="earningsCalendar")
        output: list[dict[str, Any]] = []
        retrieved_at = self.clock()
        for row in rows:
            symbol = _symbol(row.get("symbol"))
            if symbol not in requested_symbols:
                continue
            event_date = _date(row.get("date"))
            if event_date is None or not start <= event_date <= end:
                continue
            session = SESSION_CODES.get(
                str(row.get("hour") or "").lower(),
                "UNKNOWN",
            )
            occurrence_id = f"FINNHUB:EARNINGS:{symbol}:{event_date.isoformat()}:{session}"
            refresh_due_at = _earnings_refresh_due(event_date, session)
            eps_actual = _nullable_number(row.get("epsActual"))
            eps_estimate = _nullable_number(row.get("epsEstimate"))
            revenue_actual = _nullable_number(row.get("revenueActual"))
            revenue_estimate = _nullable_number(row.get("revenueEstimate"))
            content_valid_until = (
                refresh_due_at + timedelta(hours=48)
                if eps_actual is not None or revenue_actual is not None
                else refresh_due_at
            )
            output.append(
                {
                    "occurrence_id": occurrence_id,
                    "symbol": symbol,
                    "scheduled_date": event_date.isoformat(),
                    "event_date": event_date.isoformat(),
                    "session": session,
                    "timing": session,
                    "event_at": None,
                    "temporal_precision": (
                        "SESSION_ONLY"
                        if session in {"BMO", "AMC", "DMH"}
                        else MNQ_EARNINGS_SELECTION_POLICY.date_only_temporal_precision
                    ),
                    "eps_actual": eps_actual,
                    "eps_estimate": eps_estimate,
                    "revenue_actual": revenue_actual,
                    "revenue_estimate": revenue_estimate,
                    "eps_surprise": _surprise(eps_actual, eps_estimate),
                    "revenue_surprise": _surprise(revenue_actual, revenue_estimate),
                    "actual_status": (
                        "PUBLISHED"
                        if eps_actual is not None or revenue_actual is not None
                        else "SCHEDULED"
                    ),
                    "source": self.source,
                    "publisher": "Finnhub",
                    "distributor": "Finnhub",
                    "acquisition_provider": "FINNHUB",
                    "source_url": request["source_url"],
                    "source_domain": "finnhub.io",
                    "authority_tier": 3,
                    "retrieved_at": retrieved_at.isoformat(),
                    "data_as_of": retrieved_at.isoformat(),
                    "valid_until": content_valid_until.isoformat(),
                    "content_valid_until": (
                        content_valid_until.isoformat()
                    ),
                    "next_refresh_at": refresh_due_at.isoformat(),
                    "refresh_due_at": refresh_due_at.isoformat(),
                    "freshness_state": "CURRENT",
                    "trigger_class": (
                        "TRIGGER"
                        if eps_actual is not None or revenue_actual is not None
                        else "CACHE_UNTIL_DUE"
                    ),
                    "request_fingerprint": request["request_fingerprint"],
                    "raw_payload_hash": safe_payload_hash(row),
                    "lineage": [
                        {
                            "field": field,
                            "source": self.source,
                            "source_field": source_field,
                            "publisher": "Finnhub",
                            "distributor": "Finnhub",
                            "acquisition_provider": "FINNHUB",
                            "source_url": request["source_url"],
                        }
                        for field, source_field in {
                            "symbol": "symbol",
                            "event_date": "date",
                            "timing": "hour",
                            "eps_actual": "epsActual",
                            "eps_estimate": "epsEstimate",
                            "revenue_actual": "revenueActual",
                            "revenue_estimate": "revenueEstimate",
                        }.items()
                    ],
                }
            )
        telemetry.accepted = len(output)
        telemetry.rejected = len(rows) - len(output)
        for item in output:
            item["telemetry"] = telemetry.as_dict()
        deduplicated = _deduplicate(
            output,
            lambda item: str(item["occurrence_id"]),
        )
        if symbols is None:
            deduplicated.sort(
                key=lambda item: tuple(
                    str(item.get(field) or "")
                    for field in MNQ_EARNINGS_SELECTION_POLICY.sort_fields
                )
            )
            return deduplicated
        return deduplicated

    async def company_news(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
    ) -> list[dict[str, Any]]:
        self._ready()
        normalized_symbol = _symbol(symbol)
        if end < start:
            raise ValueError("company news end precedes start")
        payload, telemetry, request = await self.http.request(
            "GET",
            f"{self.settings.finnhub_base_url.rstrip('/')}/company-news",
            endpoint_category="company_news_discovery",
            provider=self.source,
            params={
                "symbol": normalized_symbol,
                "from": start.isoformat(),
                "to": end.isoformat(),
            },
            headers={"X-Finnhub-Token": str(self.settings.finnhub_api_key)},
        )
        rows = as_list(payload)
        if len(rows) >= self.settings.finnhub_news_cap:
            telemetry.anomalies.append("company_news_cap_reached")
        candidates: list[dict[str, Any]] = []
        for row in rows[: self.settings.finnhub_news_cap]:
            published = _unix_timestamp(row.get("datetime"))
            if published and published > self.clock() + timedelta(minutes=5):
                raise DeterministicProviderError(
                    "Finnhub timestamp is in the future"
                )
            original_url = str(row.get("url") or "")
            domain = (urlparse(original_url).hostname or "").lower().rstrip(".")
            if not original_url.startswith("https://") or not domain:
                telemetry.rejected += 1
                continue
            headline = str(row.get("headline") or "").strip()
            if not headline:
                telemetry.rejected += 1
                continue
            source_id = str(row.get("id") or "").strip()
            candidate_id = source_id or hashlib.sha256(
                f"{original_url}|{headline}".encode("utf-8")
            ).hexdigest()
            candidates.append(
                {
                    "candidate_id": candidate_id,
                    "symbol": normalized_symbol,
                    "headline": headline,
                    "summary": row.get("summary"),
                    "category": row.get("category"),
                    "source": row.get("source"),
                    "source_url": original_url,
                    "original_domain": domain,
                    "independent_source_group": domain,
                    "published_at": published.isoformat() if published else None,
                    "retrieved_at": self.clock().isoformat(),
                    "candidate_only": True,
                    "verification_status": "PENDING_SOURCE_GATEWAY",
                    "materiality_status": "UNASSESSED",
                    "accepted_for_current_news": False,
                    "discovery_provider": self.source,
                    "discovery_source_url": request["source_url"],
                    "request_fingerprint": request["request_fingerprint"],
                    "raw_payload_hash": safe_payload_hash(row),
                }
            )
        output = _deduplicate(
            candidates,
            lambda item: "|".join(
                (
                    str(item["candidate_id"]),
                    str(item["source_url"]).lower(),
                    str(item["headline"]).casefold(),
                )
            ),
        )
        telemetry.accepted = len(output)
        telemetry.rejected += len(candidates) - len(output)
        for item in output:
            item["telemetry"] = telemetry.as_dict()
        return output


def _symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or not symbol.replace(".", "").replace("-", "").isalnum():
        raise ValueError("invalid company symbol")
    return symbol


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _earnings_refresh_due(event_date: date, session: str) -> datetime:
    local_time = {
        "BMO": time(8),
        "AMC": time(16, 15),
        "DMH": time(12),
        "UNKNOWN": time(23, 59, 59),
    }[session]
    return datetime.combine(event_date, local_time, NEW_YORK).astimezone(UTC)


def _unix_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromtimestamp(int(value), tz=UTC)
    except (TypeError, ValueError, OSError):
        return None
    return parsed


def _nullable_number(value: Any) -> int | float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DeterministicProviderError("Finnhub impossible numeric value") from exc
    if number != number or number in {float("inf"), float("-inf")}:
        raise DeterministicProviderError("Finnhub non-finite numeric value")
    return int(number) if number.is_integer() else number


def _surprise(actual: int | float | None, estimate: int | float | None) -> float | None:
    if actual is None or estimate in (None, 0):
        return None
    return round((float(actual) - float(estimate)) / abs(float(estimate)) * 100, 6)


def _deduplicate(
    values: Iterable[dict[str, Any]],
    identity,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in values:
        key = identity(item)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output
