from __future__ import annotations

import hashlib
import html
import re
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx

from app.core.config import Settings


class EvidenceVerifierProtocol(Protocol):
    def verify(self, evidence: dict[str, Any]) -> dict[str, Any] | None: ...


class DeterministicEvidenceVerifier:
    """Verify a cited HTTPS page without assigning source trust or interpreting facts."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def verify(self, evidence: dict[str, Any]) -> dict[str, Any] | None:
        source_url = str(evidence.get("source_url") or evidence.get("canonical_url") or "")
        expected = _normalized_text(str(evidence.get("evidence_text") or ""))
        if not source_url.startswith("https://") or not expected:
            return None
        try:
            with httpx.Client(
                timeout=min(float(self.settings.http_timeout_seconds), 15.0),
                follow_redirects=True,
                headers={"User-Agent": "AI-MARKET-DATA-SERVICE/1.0 evidence-verifier"},
            ) as client:
                response = client.get(source_url)
                response.raise_for_status()
        except (httpx.HTTPError, OSError):
            return None
        page_text = _normalized_text(_visible_text(response.text))
        if expected not in page_text:
            return None
        final_url = _canonical_url(str(response.url))
        declared_canonical = _html_canonical(response.text, final_url)
        return {
            "event_type": "server_source_verified",
            "source_url": source_url,
            "canonical_url": declared_canonical or final_url,
            "redirect_url": final_url if final_url != _canonical_url(source_url) else None,
            "observed_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "content_hash": hashlib.sha256(response.content).hexdigest(),
            "http_status": response.status_code,
            "evidence_text_verified": True,
            "published_at": response.headers.get("last-modified"),
            "verification_backend": "deterministic_http",
        }


def _visible_text(value: str) -> str:
    without_scripts = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value)
    return html.unescape(re.sub(r"(?s)<[^>]+>", " ", without_scripts))


def _normalized_text(value: str) -> str:
    return " ".join(value.casefold().split())


def _html_canonical(value: str, base_url: str) -> str | None:
    match = re.search(
        r"(?is)<link[^>]+rel=[\"'][^\"']*canonical[^\"']*[\"'][^>]+href=[\"']([^\"']+)",
        value,
    ) or re.search(
        r"(?is)<link[^>]+href=[\"']([^\"']+)[\"'][^>]+rel=[\"'][^\"']*canonical",
        value,
    )
    return _canonical_url(urljoin(base_url, match.group(1))) if match else None


def _canonical_url(value: str) -> str:
    split = urlsplit(value)
    return urlunsplit((split.scheme.lower(), split.netloc.lower(), split.path or "/", "", ""))
