from __future__ import annotations

import hashlib
import html
import ipaddress
import json
import socket
import unicodedata
import urllib.robotparser
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from html.parser import HTMLParser
from io import BytesIO
from time import perf_counter
from typing import Any, Callable, Iterable, Protocol
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

import httpx

from app.core.config import Settings
from app.core.text_normalization import (
    contains_mojibake,
    normalize_payload_text,
    normalize_text,
)
from app.services.source_policy_service import SourcePolicyService

try:  # Installed as an application dependency; kept lazy for migration-only tooling.
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - exercised only in incomplete runtime installs.
    PdfReader = None  # type: ignore[assignment]


SUPPORTED_CONTENT_TYPES = {
    "application/json",
    "application/pdf",
    "application/xhtml+xml",
    "text/html",
    "text/json",
    "text/plain",
}
REDIRECT_STATUSES = {301, 302, 303, 307, 308}


class ResearchSourceStore(Protocol):
    def persist_research_source(
        self,
        run_id: str,
        source: dict[str, Any],
    ) -> dict[str, Any]: ...

    def research_sources(self, run_id: str) -> list[dict[str, Any]]: ...

    def research_source_for_url(
        self,
        run_id: str,
        url: str,
    ) -> dict[str, Any] | None: ...

    def research_source_for_hash(
        self,
        run_id: str,
        content_hash: str,
    ) -> dict[str, Any] | None: ...

    def record_evidence_verification(
        self,
        run_id: str,
        verification: dict[str, Any],
    ) -> dict[str, Any]: ...

    def mark_research_source_verified(
        self,
        source_id: str,
        verification: dict[str, Any],
    ) -> None: ...


@dataclass(frozen=True)
class EvidenceMatch:
    accepted: bool
    reason: str
    method: str | None
    score: float
    anchor: str
    evidence_token_count: int
    matched_token_count: int


class ResearchSourceGateway:
    """Policy-aware, SSRF-safe acquisition and deterministic evidence verification."""

    def __init__(
        self,
        settings: Settings,
        *,
        repository: ResearchSourceStore,
        policy: SourcePolicyService | None = None,
        transport: httpx.BaseTransport | None = None,
        resolver: Callable[[str], Iterable[str]] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.policy = policy or SourcePolicyService(settings.source_policy_path)
        self.transport = transport
        self.resolver = resolver or _resolve_host
        self.now = now or (lambda: datetime.now(UTC))

    def acquire_many(
        self,
        run_id: str,
        requests: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        unique: list[dict[str, Any]] = []
        seen: set[str] = set()
        for request in requests:
            if not isinstance(request, dict):
                continue
            url = _canonical_url(str(request.get("source_url") or request.get("url") or ""))
            if not url or url in seen:
                continue
            seen.add(url)
            unique.append({**request, "source_url": url})
            if len(unique) >= self.settings.research_gateway_max_sources_per_run:
                break
        return [self.acquire(run_id, request) for request in unique]

    def acquire(
        self,
        run_id: str,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        started = perf_counter()
        original_url = _canonical_url(str(request.get("source_url") or ""))
        existing = self.repository.research_source_for_url(run_id, original_url)
        if existing is not None:
            return existing
        source_id = _source_id(run_id, original_url or str(request.get("source_url") or ""))
        base = {
            "source_id": source_id,
            "requested_url": original_url or str(request.get("source_url") or "")[:2048],
            "final_url": None,
            "canonical_url": None,
            "redirect_chain": [],
            "publisher": str(request.get("publisher") or "")[:200] or None,
            "title": str(request.get("title") or "")[:500] or None,
            "source_domain": self.policy.domain(original_url),
            "source_tier": None,
            "fetch_status": "REJECTED",
            "verification_status": "UNVERIFIED",
            "rejection_reason": None,
            "http_status": None,
            "content_type": None,
            "retrieved_at": _iso(self.now()),
            "content_sha256": None,
            "content_bytes": 0,
            "content_text": None,
            "duplicate_of_source_id": None,
            "acquisition_backend": "service_http_gateway",
        }
        valid, reason, rule = self._validate_url(original_url, request.get("publisher"))
        if not valid:
            return self._persist_failure(run_id, base, reason, started)
        base["source_tier"] = int(rule["tier"]) if rule else None
        base["publisher"] = base.get("publisher") or (
            str(rule.get("publisher") or "")[:200] if rule else None
        )
        current_url = original_url
        redirect_chain: list[str] = []
        try:
            timeout = httpx.Timeout(
                min(
                    float(self.settings.research_gateway_timeout_seconds),
                    float(self.settings.http_timeout_seconds),
                )
            )
            with httpx.Client(
                timeout=timeout,
                follow_redirects=False,
                transport=self.transport,
                headers={
                    "User-Agent": "AI-MARKET-DATA-SERVICE/1.0 research-source-gateway",
                    "Accept": (
                        "text/html,application/xhtml+xml,application/json,"
                        "text/plain,application/pdf;q=0.9"
                    ),
                },
            ) as client:
                if self.settings.research_gateway_respect_robots:
                    robots_allowed, robots_reason = _robots_allowed(
                        client,
                        current_url,
                    )
                    if not robots_allowed:
                        return self._persist_failure(run_id, base, robots_reason, started)
                response: httpx.Response | None = None
                for _ in range(self.settings.research_gateway_max_redirects + 1):
                    with client.stream("GET", current_url) as streamed:
                        response = streamed
                        status = int(response.status_code)
                        if status in REDIRECT_STATUSES:
                            location = response.headers.get("location")
                            if not location:
                                return self._persist_failure(
                                    run_id,
                                    {**base, "http_status": status},
                                    "redirect_without_location",
                                    started,
                                )
                            target = _canonical_url(urljoin(current_url, location))
                            valid, reason, _target_rule = self._validate_url(target, None)
                            if not valid:
                                return self._persist_failure(
                                    run_id,
                                    {**base, "http_status": status},
                                    f"unsafe_redirect:{reason}",
                                    started,
                                )
                            redirect_chain.append(target)
                            current_url = target
                            continue
                        raw = _bounded_response_bytes(
                            response,
                            self.settings.research_gateway_max_content_bytes,
                        )
                        break
                else:  # pragma: no cover - loop always exits or returns.
                    response = None
                    raw = b""
                if response is None:
                    return self._persist_failure(run_id, base, "redirect_limit_exceeded", started)
                if response.status_code in REDIRECT_STATUSES:
                    return self._persist_failure(
                        run_id,
                        {
                            **base,
                            "http_status": int(response.status_code),
                            "redirect_chain": redirect_chain,
                        },
                        "redirect_limit_exceeded",
                        started,
                    )
                final_url = _canonical_url(str(response.url))
                final_rule = self.policy.rule_for(
                    final_url,
                    str(request.get("publisher") or "") or None,
                )
                content_type = _content_type(response.headers.get("content-type"))
                base.update(
                    {
                        "final_url": final_url,
                        "canonical_url": final_url,
                        "source_domain": self.policy.domain(final_url),
                        "source_tier": (
                            int(final_rule["tier"])
                            if final_rule is not None
                            else base.get("source_tier")
                        ),
                        "redirect_chain": redirect_chain,
                        "http_status": int(response.status_code),
                        "content_type": content_type,
                        "content_bytes": len(raw),
                        "content_sha256": hashlib.sha256(raw).hexdigest() if raw else None,
                    }
                )
                if not 200 <= response.status_code < 300:
                    return self._persist_failure(
                        run_id, base, f"http_status_{response.status_code}", started
                    )
                if content_type not in SUPPORTED_CONTENT_TYPES:
                    return self._persist_failure(
                        run_id,
                        base,
                        f"unsupported_content_type:{content_type or 'missing'}",
                        started,
                    )
                text, declared_canonical, extraction_reason = _extract_content(
                    raw,
                    content_type,
                    response.encoding,
                )
                if declared_canonical:
                    candidate = _canonical_url(urljoin(final_url, declared_canonical))
                    canonical_valid, _canonical_reason, _ = self._validate_url(
                        candidate, request.get("publisher")
                    )
                    if canonical_valid:
                        base["canonical_url"] = candidate
                normalized = normalize_document_text(text)
                base["content_text"] = normalized[: self.settings.research_gateway_max_text_chars]
                if extraction_reason:
                    return self._persist_failure(run_id, base, extraction_reason, started)
                if contains_mojibake(base["content_text"]):
                    return self._persist_failure(
                        run_id,
                        base,
                        "mojibake_content_rejected",
                        started,
                    )
                if len(base["content_text"] or "") < self.settings.research_gateway_min_text_chars:
                    return self._persist_failure(run_id, base, "insufficient_static_text", started)
        except SourceGatewayLimitError as exc:
            return self._persist_failure(run_id, base, str(exc), started)
        except httpx.TimeoutException:
            return self._persist_failure(run_id, base, "fetch_timeout", started)
        except (httpx.HTTPError, OSError, ValueError) as exc:
            return self._persist_failure(run_id, base, f"fetch_error:{type(exc).__name__}", started)
        duplicate = (
            self.repository.research_source_for_hash(run_id, str(base.get("content_sha256") or ""))
            if base.get("content_sha256")
            else None
        )
        if duplicate and duplicate.get("source_id") != source_id:
            base["duplicate_of_source_id"] = duplicate.get("source_id")
        base.update(
            {
                "fetch_status": "FETCHED",
                "rejection_reason": None,
                "fetch_duration_ms": int((perf_counter() - started) * 1000),
            }
        )
        return self.repository.persist_research_source(run_id, base)

    def verify_claims(
        self,
        run_id: str,
        claims: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        verified_claims: list[dict[str, Any]] = []
        for claim_index, raw_claim in enumerate(claims):
            if not isinstance(raw_claim, dict):
                continue
            claim = normalize_payload_text(dict(raw_claim))
            claim_ref = str(
                claim.get("claim_ref") or claim.get("claim_id") or f"candidate-{claim_index + 1}"
            )[:120]
            evidence_items: list[dict[str, Any]] = []
            for evidence_index, raw_evidence in enumerate(claim.get("evidence") or []):
                if not isinstance(raw_evidence, dict):
                    continue
                started = perf_counter()
                evidence = normalize_payload_text(dict(raw_evidence))
                url = str(evidence.get("canonical_url") or evidence.get("source_url") or "")
                source = self.repository.research_source_for_url(run_id, url)
                if source is None or source.get("fetch_status") != "FETCHED":
                    match = EvidenceMatch(
                        False,
                        "source_not_fetched",
                        None,
                        0.0,
                        _bounded_anchor(evidence.get("evidence_text")),
                        len(_tokens(evidence.get("evidence_text"))),
                        0,
                    )
                else:
                    match = match_evidence(
                        str(evidence.get("evidence_text") or ""),
                        str(source.get("content_text") or ""),
                        threshold=self.settings.research_evidence_match_threshold,
                        minimum_tokens=self.settings.research_evidence_min_tokens,
                    )
                verification_id = (
                    "rverify-"
                    f"{hashlib.sha256(f'{run_id}|{claim_ref}|{evidence_index}|{url}'.encode()).hexdigest()[:24]}"
                )
                verification = {
                    "verification_id": verification_id,
                    "claim_ref": claim_ref,
                    "source_id": source.get("source_id") if source else None,
                    "evidence_url": _canonical_url(url) or url[:2048],
                    "status": "VERIFIED" if match.accepted else "REJECTED",
                    "reason": match.reason,
                    "match_method": match.method,
                    "match_score": match.score,
                    "evidence_anchor": match.anchor,
                    "evidence_token_count": match.evidence_token_count,
                    "matched_token_count": match.matched_token_count,
                    "verification_duration_ms": int((perf_counter() - started) * 1000),
                }
                persisted = self.repository.record_evidence_verification(run_id, verification)
                evidence["_service_verification"] = {
                    "verification_id": persisted["verification_id"],
                    "accepted": match.accepted,
                    "reason": match.reason,
                    "match_method": match.method,
                    "match_score": match.score,
                    "source_id": source.get("source_id") if source else None,
                    "content_sha256": (source.get("content_sha256") if source else None),
                    "canonical_url": (source.get("canonical_url") if source else None),
                    "retrieved_at": source.get("retrieved_at") if source else None,
                }
                if source is not None:
                    evidence["canonical_url"] = source.get("canonical_url")
                    evidence["retrieved_at"] = source.get("retrieved_at")
                if match.accepted and source is not None:
                    self.repository.mark_research_source_verified(
                        str(source["source_id"]), persisted
                    )
                evidence_items.append(evidence)
            claim["claim_ref"] = claim_ref
            claim["evidence"] = evidence_items
            verified_claims.append(claim)
        return verified_claims

    def _persist_failure(
        self,
        run_id: str,
        base: dict[str, Any],
        reason: str,
        started: float,
    ) -> dict[str, Any]:
        return self.repository.persist_research_source(
            run_id,
            {
                **base,
                "fetch_status": "REJECTED",
                "verification_status": "UNVERIFIED",
                "rejection_reason": str(reason)[:300],
                "fetch_duration_ms": int((perf_counter() - started) * 1000),
            },
        )

    def _validate_url(
        self,
        value: str,
        publisher: Any,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        try:
            parts = urlsplit(value)
        except ValueError:
            return False, "invalid_url", None
        if parts.scheme.lower() != "https":
            return False, "https_required", None
        if not parts.hostname or parts.username or parts.password:
            return False, "invalid_authority", None
        try:
            if parts.port not in (None, 443):
                return False, "nonstandard_port_rejected", None
        except ValueError:
            return False, "invalid_port", None
        decoded_path = unquote(parts.path).replace("\\", "/")
        if any(segment == ".." for segment in decoded_path.split("/")):
            return False, "path_traversal_rejected", None
        rule = self.policy.rule_for(value, str(publisher or "") or None)
        if rule is None:
            return False, "source_policy_rejected", None
        try:
            addresses = list(self.resolver(parts.hostname))
        except OSError:
            return False, "dns_resolution_failed", rule
        if not addresses:
            return False, "dns_resolution_empty", rule
        for address in addresses:
            try:
                parsed = ipaddress.ip_address(address)
            except ValueError:
                return False, "dns_invalid_address", rule
            if not parsed.is_global:
                return False, "ssrf_non_global_address", rule
        return True, "", rule


class SourceGatewayLimitError(RuntimeError):
    pass


def match_evidence(
    evidence_text: str,
    document_text: str,
    *,
    threshold: float = 0.88,
    minimum_tokens: int = 5,
) -> EvidenceMatch:
    anchor = _bounded_anchor(evidence_text)
    evidence_normalized = normalize_match_text(anchor)
    document_normalized = normalize_match_text(document_text)
    evidence_tokens = evidence_normalized.split()
    document_tokens = document_normalized.split()
    if len(evidence_tokens) < minimum_tokens:
        return EvidenceMatch(
            False,
            "evidence_anchor_too_short",
            None,
            0.0,
            anchor,
            len(evidence_tokens),
            0,
        )
    if not document_tokens:
        return EvidenceMatch(
            False,
            "document_text_unavailable",
            None,
            0.0,
            anchor,
            len(evidence_tokens),
            0,
        )
    if evidence_normalized in document_normalized:
        return EvidenceMatch(
            True,
            "verified_exact_normalized_match",
            "exact_normalized",
            1.0,
            anchor,
            len(evidence_tokens),
            len(evidence_tokens),
        )
    evidence_counts = Counter(evidence_tokens)
    document_counts = Counter(document_tokens)
    matched = sum((evidence_counts & document_counts).values())
    coverage = matched / max(len(evidence_tokens), 1)
    candidate_starts = _candidate_window_starts(evidence_tokens, document_tokens)
    best_ratio = 0.0
    for start in candidate_starts:
        for delta in (-2, -1, 0, 1, 2):
            length = max(len(evidence_tokens) + delta, minimum_tokens)
            window = document_tokens[start : start + length]
            if len(window) < minimum_tokens:
                continue
            best_ratio = max(
                best_ratio,
                SequenceMatcher(
                    None,
                    evidence_tokens,
                    window,
                    autojunk=False,
                ).ratio(),
            )
    score = round(min(coverage, best_ratio), 6)
    accepted = coverage >= threshold and best_ratio >= threshold
    return EvidenceMatch(
        accepted,
        "verified_rigorous_token_match" if accepted else "evidence_mismatch",
        "token_window" if accepted else None,
        score,
        anchor,
        len(evidence_tokens),
        matched,
    )


def normalize_match_text(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", normalize_text(value)).casefold()
    characters = [
        " " if unicodedata.category(character)[0] in {"P", "S", "Z"} else character
        for character in normalized
    ]
    return " ".join("".join(characters).split())


def normalize_document_text(value: Any) -> str:
    return " ".join(
        unicodedata.normalize("NFKC", normalize_text(html.unescape(str(value or "")))).split()
    )


def _extract_content(
    raw: bytes,
    content_type: str,
    response_encoding: str | None,
) -> tuple[str, str | None, str | None]:
    if content_type in {"text/html", "application/xhtml+xml"}:
        decoded = _decode(raw, response_encoding)
        parser = _VisibleHTMLParser()
        parser.feed(decoded)
        return parser.text(), parser.canonical_url, None
    if content_type in {"application/json", "text/json"}:
        try:
            payload = json.loads(_decode(raw, response_encoding))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return "", None, "invalid_json_content"
        return _json_text(payload), None, None
    if content_type == "application/pdf":
        if PdfReader is None:
            return "", None, "pdf_parser_unavailable"
        try:
            reader = PdfReader(BytesIO(raw))
            text = "\n".join(str(page.extract_text() or "") for page in reader.pages)
        except Exception:
            return "", None, "invalid_pdf_content"
        return text, None, None
    return _decode(raw, response_encoding), None, None


class _VisibleHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden_depth = 0
        self.canonical_url: str | None = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript", "svg", "template"}:
            self.hidden_depth += 1
        values = {key.lower(): str(value or "") for key, value in attrs}
        if lowered == "link" and "canonical" in values.get("rel", "").lower():
            self.canonical_url = values.get("href") or self.canonical_url

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript", "svg", "template"}:
            self.hidden_depth = max(self.hidden_depth - 1, 0)

    def handle_data(self, data: str) -> None:
        if not self.hidden_depth and data.strip():
            self.parts.append(data)

    def text(self) -> str:
        return " ".join(self.parts)


def _bounded_response_bytes(response: httpx.Response, maximum: int) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > maximum:
                raise SourceGatewayLimitError("content_length_limit_exceeded")
        except ValueError:
            pass
    chunks: list[bytes] = []
    size = 0
    for chunk in response.iter_bytes():
        size += len(chunk)
        if size > maximum:
            raise SourceGatewayLimitError("content_size_limit_exceeded")
        chunks.append(chunk)
    return b"".join(chunks)


def _robots_allowed(
    client: httpx.Client,
    url: str,
) -> tuple[bool, str]:
    parts = urlsplit(url)
    robots_url = urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))
    try:
        with client.stream("GET", robots_url) as response:
            if response.status_code in {401, 403}:
                return False, "robots_access_blocked"
            if response.status_code != 200:
                return True, ""
            raw = _bounded_response_bytes(response, 65_536)
    except (httpx.HTTPError, OSError, SourceGatewayLimitError):
        return True, ""
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(robots_url)
    parser.parse(raw.decode("utf-8", errors="replace").splitlines())
    if not parser.can_fetch(
        "AI-MARKET-DATA-SERVICE/1.0 research-source-gateway",
        url,
    ):
        return False, "robots_denied"
    return True, ""


def _json_text(value: Any, *, depth: int = 0) -> str:
    if depth > 12:
        return ""
    if isinstance(value, dict):
        return " ".join(
            f"{key} {_json_text(item, depth=depth + 1)}" for key, item in list(value.items())[:500]
        )
    if isinstance(value, list):
        return " ".join(_json_text(item, depth=depth + 1) for item in value[:500])
    if value is None:
        return ""
    return str(value)


def _decode(raw: bytes, encoding: str | None) -> str:
    return raw.decode(encoding or "utf-8", errors="replace")


def _content_type(value: str | None) -> str:
    return str(value or "").split(";", 1)[0].strip().lower()


def _canonical_url(value: str) -> str:
    try:
        parts = urlsplit(value.strip())
    except ValueError:
        return ""
    if not parts.scheme or not parts.netloc:
        return ""
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path or "/",
            parts.query,
            "",
        )
    )


def _resolve_host(host: str) -> list[str]:
    return sorted(
        {str(item[4][0]) for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    )


def _source_id(run_id: str, url: str) -> str:
    digest = hashlib.sha256(f"{run_id}|{url}".encode()).hexdigest()[:24]
    return f"rsource-{digest}"


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def _bounded_anchor(value: Any) -> str:
    return normalize_document_text(value)[:1000]


def _tokens(value: Any) -> list[str]:
    return normalize_match_text(value).split()


def _candidate_window_starts(
    evidence_tokens: list[str],
    document_tokens: list[str],
) -> list[int]:
    anchors = set(evidence_tokens[: min(3, len(evidence_tokens))])
    starts = {
        max(index - offset, 0)
        for index, token in enumerate(document_tokens)
        if token in anchors
        for offset in range(min(3, len(evidence_tokens)))
    }
    if starts:
        return sorted(starts)[:5000]
    step = max(len(evidence_tokens) // 2, 1)
    return list(range(0, len(document_tokens), step))[:5000]
