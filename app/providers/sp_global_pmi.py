from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, ProviderDisabled, ProviderError, metadata
from app.services.official_actual_semantics import normalize_reference_period


SERIES_ID = "SPGLOBAL:US:FLASH_SERVICES_PMI"
ALLOWED_HOST = "pmi.spglobal.com"


class SpGlobalPmiProvider(BaseProvider):
    """Read an exact S&P Global public PMI release without guessing values."""

    source = "SPGLOBAL"
    provider_type = ProviderType.SCRAPER
    reliability = 0.98
    cache_key = "provider:spglobal:flash_services_pmi"

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
        self, *, expected_period: str, release_date: str
    ) -> ProviderResult:
        if not self.settings.sp_global_pmi_enabled:
            raise ProviderDisabled("S&P Global PMI provider is disabled")
        url = self.settings.sp_global_pmi_release_url or self.settings.sp_global_pmi_index_url
        _assert_allowed_url(url)
        async with httpx.AsyncClient(
            timeout=self.settings.sp_global_pmi_timeout_seconds,
            follow_redirects=False,
            transport=self.transport,
        ) as client:
            response = await client.get(url)
            if response.status_code in {401, 403}:
                raise ProviderError("sp_global_public_release_access_restricted")
            response.raise_for_status()
            if self.settings.sp_global_pmi_release_url is None:
                release_url = _find_release_url(response.text, base_url=url)
                if release_url is None:
                    raise ProviderError("sp_global_exact_release_link_not_found")
                _assert_allowed_url(release_url)
                response = await client.get(release_url)
                if response.status_code in {401, 403}:
                    raise ProviderError("sp_global_public_release_access_restricted")
                response.raise_for_status()
                url = release_url

        parsed = _parse_release(
            response.text,
            expected_period=expected_period,
            release_date=release_date,
        )
        retrieved_at = datetime.now(UTC)
        parsed.update(
            {
                "series_id": SERIES_ID,
                "source": self.source,
                "source_url": url,
                "canonical_url": url,
                "source_domain": ALLOWED_HOST,
                "provider_adapter": "SPGLOBAL_OFFICIAL_API",
                "official_adapter": True,
                "frequency": "monthly",
                "units": "index_points",
                "seasonal_adjustment": "SA",
                "retrieved_at": retrieved_at.isoformat(),
                "raw_lineage_redacted": {
                    "content_sha256": hashlib.sha256(response.content).hexdigest().upper(),
                    "source_url": url,
                    "release_date": release_date,
                },
            }
        )
        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability,
                data_as_of=datetime.fromisoformat(release_date).replace(tzinfo=UTC),
                freshness=Freshness.RECENT,
            ),
            data={SERIES_ID: parsed},
        )


def _find_release_url(document: str, *, base_url: str) -> str | None:
    for href in re.findall(r"""href\s*=\s*["']([^"']+)["']""", document, re.I):
        if "pressrelease" in href.casefold():
            return urljoin(base_url, unescape(href))
    return None


def _parse_release(
    document: str, *, expected_period: str, release_date: str
) -> dict[str, Any]:
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(document))).strip()
    match = re.search(
        r"Flash\s+US\s+Services\s+PMI\s+Business\s+Activity\s+Index"
        r"\s*[:\-]\s*(?P<actual>\d+(?:\.\d+)?)"
        r"\s*\((?P<previous_month>[A-Za-z]+)\s*:\s*"
        r"(?P<previous>\d+(?:\.\d+)?)\)",
        text,
        re.I,
    )
    if match is None:
        raise ProviderError("sp_global_flash_services_value_not_found")
    release = datetime.fromisoformat(release_date)
    normalized = normalize_reference_period(
        expected_period, frequency="monthly", release_date=release
    )
    if normalized != expected_period:
        raise ProviderError("sp_global_reference_period_mismatch")
    previous_period = normalize_reference_period(
        match.group("previous_month"), frequency="monthly", release_date=release
    )
    return {
        "observations": [
            {"period": previous_period, "value": match.group("previous"), "release_vintage": release_date},
            {"period": normalized, "value": match.group("actual"), "release_vintage": release_date},
        ],
        "period": normalized,
        "release_timestamp": release_date,
    }


def _assert_allowed_url(url: str) -> None:
    parsed = urlparse(str(url))
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or not (host == ALLOWED_HOST or host.endswith(f".{ALLOWED_HOST}"))
        or parsed.username
        or parsed.password
    ):
        raise ProviderError("sp_global_source_url_not_allowed")
