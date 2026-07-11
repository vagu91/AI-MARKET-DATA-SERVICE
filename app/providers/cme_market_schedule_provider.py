from __future__ import annotations

import asyncio
import html
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS


class CmeMarketScheduleProvider:
    source = "CME Group Trading Hours"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_cme_market_schedule:
            return _status("disabled", "cme_market_schedule_disabled", started, provider_calls=0, source_url=self.settings.cme_market_schedule_url)
        try:
            async with httpx.AsyncClient(timeout=self.settings.cme_market_schedule_timeout_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(self.settings.cme_market_schedule_url, headers=REQUEST_HEADERS),
                    timeout=max(float(self.settings.cme_market_schedule_timeout_seconds), 1.0),
                )
            if response.status_code in {401, 403}:
                return _status(
                    "access_restricted",
                    f"cme_market_schedule_http_{response.status_code}",
                    started,
                    provider_calls=1,
                    http_status=response.status_code,
                    source_url=self.settings.cme_market_schedule_url,
                )
            response.raise_for_status()
        except TimeoutError:
            return _status("provider_timeout", "cme_market_schedule_timeout", started, provider_calls=1, source_url=self.settings.cme_market_schedule_url)
        except httpx.HTTPStatusError as exc:
            return _status(
                "provider_failed",
                f"cme_market_schedule_http_{exc.response.status_code}",
                started,
                provider_calls=1,
                http_status=exc.response.status_code,
                source_url=self.settings.cme_market_schedule_url,
            )
        except Exception as exc:
            return _status("provider_failed", str(exc) or "cme_market_schedule_failed", started, provider_calls=1, source_url=self.settings.cme_market_schedule_url)

        parsed = parse_cme_trading_hours_page(response.text, base_url=self.settings.cme_market_schedule_url)
        now = datetime.now(UTC)
        if not parsed["calendar_verified"]:
            return _status(
                "schema_changed",
                "cme_market_schedule_markers_not_found",
                started,
                provider_calls=1,
                http_status=response.status_code,
                source_url=self.settings.cme_market_schedule_url,
            )
        return {
            "status": "found",
            "source": self.source,
            "source_url": self.settings.cme_market_schedule_url,
            "provider_type": "OFFICIAL_EXCHANGE_WEB",
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(hours=self.settings.cme_market_schedule_ttl_hours)),
            "calendar_verified": True,
            "is_official_source": True,
            "data_origin_is_official": True,
            "distribution_source_is_official": True,
            "source_is_primary_originator": True,
            "source_is_official_redistributor": False,
            "documents": parsed["documents"],
            "document_count": len(parsed["documents"]),
            "regular_trading_hours_present": parsed["regular_trading_hours_present"],
            "globex_schedule_present": parsed["globex_schedule_present"],
            "warnings": [] if parsed["documents"] else ["cme_official_page_found_without_downloadable_schedule_links"],
            "errors": [],
            "diagnostics": {
                "http_status": response.status_code,
                "actual_network_calls": 1,
                "calendar_verified": True,
                "document_count": len(parsed["documents"]),
            },
            "provider_calls": 1,
            "actual_network_calls": 1,
            "cache_used": False,
            "AI_called": False,
            "duration_ms": int((now - started).total_seconds() * 1000),
        }


def parse_cme_trading_hours_page(text: str, *, base_url: str) -> dict[str, Any]:
    decoded = html.unescape(text or "")
    lowered = re.sub(r"\s+", " ", decoded).lower()
    globex = "cme globex" in lowered or "globex trading" in lowered
    holiday = "holiday" in lowered and ("trading hours" in lowered or "trading schedule" in lowered)
    regular = "regular trading hours" in lowered or "trading hours" in lowered
    documents: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", decoded, re.IGNORECASE | re.DOTALL):
        href = urljoin(base_url, match.group(1).strip())
        label = re.sub(r"<[^>]+>", " ", match.group(2))
        label = re.sub(r"\s+", " ", html.unescape(label)).strip()
        candidate = f"{href} {label}".lower()
        if not any(token in candidate for token in ("holiday", "trading-hours", "trading hours", "globex")):
            continue
        if href in seen:
            continue
        seen.add(href)
        documents.append({"label": label[:160] or "CME schedule document", "url": href})
    return {
        "calendar_verified": bool(globex and holiday),
        "globex_schedule_present": globex,
        "regular_trading_hours_present": regular,
        "documents": documents[:30],
    }


def _status(
    status: str,
    reason: str,
    started: datetime,
    *,
    provider_calls: int,
    http_status: int | None = None,
    source_url: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "source": "CME Group Trading Hours",
        "source_url": source_url,
        "retrieved_at": _iso(now),
        "valid_until": None,
        "calendar_verified": False,
        "is_official_source": False,
        "documents": [],
        "document_count": 0,
        "warnings": [reason],
        "errors": [] if status in {"disabled", "access_restricted"} else [reason],
        "diagnostics": {"http_status": http_status, "actual_network_calls": provider_calls, "calendar_verified": False},
        "provider_calls": provider_calls,
        "actual_network_calls": provider_calls,
        "cache_used": False,
        "AI_called": False,
        "duration_ms": int((now - started).total_seconds() * 1000),
    }


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
