from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, ProviderDisabled, ProviderError, metadata
from app.providers.calendar_utils import REQUEST_HEADERS
from app.providers.sp_global_pmi import SERIES_ID
from app.services.official_actual_semantics import normalize_reference_period


EVENT_ID = 1062
SOURCE = "INVESTING_EVENT_1062"
ALLOWED_HOST = "endpoints.investing.com"
EXPECTED_PATH = "/pd-instruments/v1/calendars/economic/events/1062/occurrences"
INVESTING_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
INVESTING_API_HEADERS = {
    **REQUEST_HEADERS,
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.investing.com",
    "Referer": "https://www.investing.com/",
    "User-Agent": INVESTING_BROWSER_USER_AGENT,
}


class InvestingFlashServicesPmiProvider(BaseProvider):
    """Exact, occurrence-matched Investing fallback for Flash Services PMI only."""

    source = SOURCE
    provider_type = ProviderType.API
    reliability = 0.8
    cache_key = "provider:investing:flash_services_pmi:event_1062"

    def __init__(
        self,
        cache: ProviderCacheProtocol,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(cache)
        self.settings = settings
        self.transport = transport

    async def fetch(
        self,
        *,
        expected_period: str,
        release_date: str,
        expected_release_at: str | None = None,
    ) -> ProviderResult:
        if not self.settings.investing_flash_services_pmi_enabled:
            raise ProviderDisabled("Investing Flash Services PMI fallback is disabled")
        url = self.settings.investing_flash_services_pmi_url
        _assert_allowed_url(url)
        try:
            async with httpx.AsyncClient(
                timeout=self.settings.investing_flash_services_pmi_timeout_seconds,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                response = await client.get(
                    url,
                    headers=INVESTING_API_HEADERS,
                )
                response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise ProviderError("investing_flash_services_pmi_timeout") from exc
        except httpx.HTTPStatusError as exc:
            raise ProviderError(
                f"investing_flash_services_pmi_http_{exc.response.status_code}"
            ) from exc
        except httpx.HTTPError as exc:
            raise ProviderError("investing_flash_services_pmi_unavailable") from exc

        payload = response.json()
        occurrence = _select_occurrence(
            payload,
            expected_period=expected_period,
            release_date=release_date,
            expected_release_at=expected_release_at,
        )
        actual = _number(occurrence.get("actual"))
        previous = _number(occurrence.get("previous"))
        forecast = _number(occurrence.get("forecast"))
        if actual is None:
            raise ProviderError("investing_flash_services_pmi_actual_missing")
        if previous is None:
            raise ProviderError("investing_flash_services_pmi_previous_missing")
        release_at = str(occurrence.get("occurrence_time"))
        period = normalize_reference_period(
            occurrence.get("reference_period") or occurrence.get("period"),
            frequency="monthly",
            release_date=datetime.fromisoformat(release_date).date(),
        )
        retrieved_at = datetime.now(UTC)
        series = {
            "series_id": SERIES_ID,
            "event_id": EVENT_ID,
            "occurrence_id": occurrence.get("occurrence_id"),
            "occurrence_time": release_at,
            "period": period,
            "reference_period": period,
            "observations": [
                {
                    "period": _previous_month(period),
                    "value": previous,
                    "release_vintage": release_at,
                },
                {
                    "period": period,
                    "value": actual,
                    "release_vintage": release_at,
                },
            ],
            "actual": actual,
            "forecast": forecast,
            "previous": previous,
            "preliminary": occurrence.get("preliminary"),
            "precision": occurrence.get("precision"),
            "publisher": "S&P Global",
            "source_originator": "S&P Global",
            "distribution_source": "Investing.com",
            "acquisition_provider": SOURCE,
            "source": SOURCE,
            "source_url": url,
            "canonical_url": "https://www.pmi.spglobal.com/Public/Home/PressRelease",
            "source_domain": ALLOWED_HOST,
            "provider_adapter": SOURCE,
            "official_adapter": False,
            "frequency": "monthly",
            "units": "index_points",
            "seasonal_adjustment": "SA",
            "release_timestamp": release_at,
            "retrieved_at": retrieved_at.isoformat(),
            "field_lineage": {
                "actual": {
                    "event_id": EVENT_ID,
                    "occurrence_id": occurrence.get("occurrence_id"),
                    "reference_period": period,
                    "publisher": "S&P Global",
                    "distributor": "Investing.com",
                    "acquisition_provider": SOURCE,
                    "source_field": "actual",
                },
                "forecast": {
                    "event_id": EVENT_ID,
                    "occurrence_id": occurrence.get("occurrence_id"),
                    "reference_period": period,
                    "publisher": "S&P Global",
                    "distributor": "Investing.com",
                    "acquisition_provider": SOURCE,
                    "source_field": "forecast",
                },
                "previous": {
                    "event_id": EVENT_ID,
                    "occurrence_id": occurrence.get("occurrence_id"),
                    "reference_period": _previous_month(period),
                    "publisher": "S&P Global",
                    "distributor": "Investing.com",
                    "acquisition_provider": SOURCE,
                    "source_field": "previous",
                },
            },
            "raw_lineage_redacted": {
                "content_sha256": hashlib.sha256(response.content).hexdigest().upper(),
                "event_id": EVENT_ID,
                "occurrence_id": occurrence.get("occurrence_id"),
            },
        }
        return ProviderResult(
            metadata=metadata(
                source=SOURCE,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=_parse_datetime(release_at),
                freshness=Freshness.RECENT,
            ),
            data={SERIES_ID: series},
        )


def _select_occurrence(
    payload: Any,
    *,
    expected_period: str,
    release_date: str,
    expected_release_at: str | None,
) -> dict[str, Any]:
    occurrences = (
        payload.get("occurrences")
        if isinstance(payload, dict)
        else payload
        if isinstance(payload, list)
        else []
    )
    if not isinstance(occurrences, list):
        raise ProviderError("investing_flash_services_pmi_occurrences_missing")
    expected_date = datetime.fromisoformat(release_date).date()
    expected_period = normalize_reference_period(
        expected_period,
        frequency="monthly",
        release_date=expected_date,
    )
    expected_release = _parse_datetime(expected_release_at)
    matches: list[dict[str, Any]] = []
    for item in occurrences:
        if not isinstance(item, dict) or int(item.get("event_id") or 0) != EVENT_ID:
            continue
        release = _parse_datetime(item.get("occurrence_time"))
        period = normalize_reference_period(
            item.get("reference_period") or item.get("period"),
            frequency="monthly",
            release_date=release.date() if release else expected_date,
        )
        if release is None or release.date() != expected_date or period != expected_period:
            continue
        if expected_release and release != expected_release:
            continue
        matches.append(item)
    if len(matches) != 1:
        reason = (
            "investing_flash_services_pmi_occurrence_not_found"
            if not matches
            else "investing_flash_services_pmi_occurrence_ambiguous"
        )
        raise ProviderError(reason)
    if not matches[0].get("occurrence_id"):
        raise ProviderError("investing_flash_services_pmi_occurrence_id_missing")
    return matches[0]


def _assert_allowed_url(url: str) -> None:
    parsed = urlparse(str(url))
    query = parse_qs(parsed.query)
    if (
        parsed.scheme != "https"
        or parsed.hostname != ALLOWED_HOST
        or parsed.path != EXPECTED_PATH
        or query.get("domain_id") != ["1"]
        or query.get("limit") != ["1000"]
        or parsed.username
        or parsed.password
    ):
        raise ProviderError("investing_flash_services_pmi_url_not_allowed")


def _previous_month(period: str | None) -> str:
    if not period:
        raise ProviderError("investing_flash_services_pmi_period_missing")
    year, month = (int(item) for item in period.split("-", 1))
    if month == 1:
        return f"{year - 1:04d}-12"
    return f"{year:04d}-{month - 1:02d}"


def _number(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
