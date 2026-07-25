from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Awaitable, Callable, Iterable, Mapping
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx
from pydantic import BaseModel, Field, field_validator

from app.core.redaction import redact_payload, redact_sensitive


SECRET_QUERY_KEYS = {
    "api_key",
    "apikey",
    "key",
    "registrationkey",
    "token",
    "userid",
}
AUTH_HEADER_KEYS = {
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-api-key",
    "x-finnhub-token",
}
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}


class ProviderKind(StrEnum):
    OFFICIAL_GOVERNMENT = "official_government"
    LICENSED_MARKET_DATA = "licensed_market_data"
    STRUCTURED_VENDOR = "structured_vendor"


class CacheStatus(StrEnum):
    MISS = "MISS"
    HIT = "HIT"
    NEGATIVE_HIT = "NEGATIVE_HIT"
    REFRESHED = "REFRESHED"
    STALE_GRACE = "STALE_GRACE"


class RetryClassification(StrEnum):
    NONE = "NONE"
    RETRYABLE = "RETRYABLE"
    TERMINAL = "TERMINAL"
    RATE_LIMITED = "RATE_LIMITED"
    AUTHENTICATION = "AUTHENTICATION"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"


class DeterministicProviderError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        classification: RetryClassification = RetryClassification.TERMINAL,
        status_code: int | None = None,
    ) -> None:
        super().__init__(redact_sensitive(message))
        self.classification = classification
        self.status_code = status_code


class NormalizedObservation(BaseModel):
    observation_id: str
    semantic_field: str
    value: int | float | str | None = None
    unit: str | None = None
    frequency: str | None = None
    seasonal_adjustment: str | None = None
    reference_period: str | None = None
    occurrence_id: str | None = None
    observed_at: datetime | None = None
    release_at: datetime | None = None
    retrieved_at: datetime
    provider_timestamp: datetime | None = None
    valid_until: datetime | None = None
    next_refresh_at: datetime | None = None
    freshness_state: str = "FRESH"
    lifecycle_state: str = "CURRENT"
    source_program: str | None = None
    revision: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("value")
    @classmethod
    def finite_number(cls, value: Any) -> Any:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("non_finite_numeric_value")
        return value


class ProviderEnvelope(BaseModel):
    provider_id: str
    provider_kind: ProviderKind
    authority_tier: int = Field(ge=1, le=5)
    requested_domain: str
    retrieved_at: datetime
    provider_timestamp: datetime | None = None
    source_url: str
    request_fingerprint: str
    cache_status: CacheStatus = CacheStatus.MISS
    freshness: str = "UNKNOWN"
    exact_occurrence_identity: str | None = None
    observations: list[NormalizedObservation] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    rejection_reasons: list[str] = Field(default_factory=list)
    rate_limit: dict[str, Any] = Field(default_factory=dict)
    retry_classification: RetryClassification = RetryClassification.NONE
    raw_payload_hash: str | None = None
    lineage: dict[str, Any] = Field(default_factory=dict)
    telemetry: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source_url")
    @classmethod
    def source_url_must_be_redacted(cls, value: str) -> str:
        return redact_url(value)


@dataclass
class ProviderTelemetry:
    provider: str
    endpoint_category: str
    dataset_or_series: str | None = None
    request_count: int = 0
    actual_provider_requests: int = 0
    cache_hits: int = 0
    negative_cache_hits: int = 0
    retries: int = 0
    status_code: int | None = None
    payload_count: int = 0
    accepted: int = 0
    rejected: int = 0
    quarantined: int = 0
    duration_ms: int = 0
    computed_metrics: list[str] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)
    secret_redaction_status: str = "PASSED"

    def as_dict(self) -> dict[str, Any]:
        return redact_payload(
            {
                "provider": self.provider,
                "endpoint_category": self.endpoint_category,
                "dataset_or_series": self.dataset_or_series,
                "request_count": self.request_count,
                "actual_provider_requests": self.actual_provider_requests,
                "cache_hit": self.cache_hits,
                "negative_cache_hit": self.negative_cache_hits,
                "retry": self.retries,
                "status_code": self.status_code,
                "payload_count": self.payload_count,
                "accepted": self.accepted,
                "rejected": self.rejected,
                "quarantined": self.quarantined,
                "duration_ms": self.duration_ms,
                "computed_metrics": list(self.computed_metrics),
                "anomalies": list(dict.fromkeys(self.anomalies)),
                "secret_redaction_status": self.secret_redaction_status,
            }
        )


@dataclass
class CircuitBreaker:
    failure_threshold: int = 3
    recovery_seconds: int = 60
    consecutive_failures: int = 0
    opened_at: datetime | None = None

    def check(self, now: datetime) -> None:
        if self.opened_at is None:
            return
        if now >= self.opened_at + timedelta(seconds=self.recovery_seconds):
            self.consecutive_failures = 0
            self.opened_at = None
            return
        raise DeterministicProviderError(
            "provider circuit is open",
            classification=RetryClassification.CIRCUIT_OPEN,
        )

    def succeeded(self) -> None:
        self.consecutive_failures = 0
        self.opened_at = None

    def failed(self, now: datetime) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.opened_at = now


class DeterministicHttpClient:
    """Small fail-closed transport shared by deterministic provider adapters."""

    def __init__(
        self,
        *,
        allowed_hosts: Iterable[str],
        timeout_seconds: float,
        retry_attempts: int = 3,
        allowed_methods: Iterable[str] = ("GET", "POST"),
        transport: httpx.AsyncBaseTransport | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self.allowed_hosts = frozenset(str(host).lower().rstrip(".") for host in allowed_hosts)
        self.timeout_seconds = float(timeout_seconds)
        self.retry_attempts = max(1, int(retry_attempts))
        self.allowed_methods = frozenset(str(method).upper() for method in allowed_methods)
        self.transport = transport
        self.sleeper = sleeper
        self.clock = clock
        self.circuit_breaker = circuit_breaker or CircuitBreaker()

    async def request(
        self,
        method: str,
        url: str,
        *,
        endpoint_category: str,
        provider: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        json_body: Any = None,
    ) -> tuple[Any, ProviderTelemetry, dict[str, str]]:
        normalized_method = str(method).upper()
        if normalized_method not in self.allowed_methods:
            raise DeterministicProviderError(
                f"HTTP method {normalized_method} is not allowed",
                classification=RetryClassification.TERMINAL,
            )
        validate_endpoint_url(url, self.allowed_hosts)
        telemetry = ProviderTelemetry(provider=provider, endpoint_category=endpoint_category)
        telemetry.request_count = 1
        safe_headers = redact_headers(headers or {})
        last_error: Exception | None = None
        for attempt in range(self.retry_attempts):
            self.circuit_breaker.check(self.clock())
            telemetry.actual_provider_requests += 1
            started = self.clock()
            try:
                async with httpx.AsyncClient(
                    timeout=self.timeout_seconds,
                    follow_redirects=False,
                    transport=self.transport,
                ) as client:
                    response = await client.request(
                        normalized_method,
                        url,
                        params=dict(params or {}),
                        headers=dict(headers or {}),
                        json=json_body,
                    )
                telemetry.duration_ms += max(
                    0,
                    int((self.clock() - started).total_seconds() * 1000),
                )
                telemetry.status_code = response.status_code
                if response.status_code in REDIRECT_STATUS_CODES:
                    raise DeterministicProviderError(
                        "provider redirect rejected",
                        classification=RetryClassification.TERMINAL,
                        status_code=response.status_code,
                    )
                if response.status_code in {401, 403}:
                    raise DeterministicProviderError(
                        "provider authentication failed",
                        classification=RetryClassification.AUTHENTICATION,
                        status_code=response.status_code,
                    )
                if response.status_code == 429:
                    telemetry.anomalies.append("rate_limit_exhausted")
                    raise DeterministicProviderError(
                        "provider rate limited request",
                        classification=RetryClassification.RATE_LIMITED,
                        status_code=response.status_code,
                    )
                if response.status_code in RETRYABLE_STATUS_CODES:
                    raise DeterministicProviderError(
                        f"retryable provider response HTTP {response.status_code}",
                        classification=RetryClassification.RETRYABLE,
                        status_code=response.status_code,
                    )
                if response.status_code >= 400:
                    raise DeterministicProviderError(
                        f"terminal provider response HTTP {response.status_code}",
                        classification=RetryClassification.TERMINAL,
                        status_code=response.status_code,
                    )
                try:
                    payload = response.json()
                except (ValueError, json.JSONDecodeError) as exc:
                    raise DeterministicProviderError(
                        "malformed provider JSON",
                        classification=RetryClassification.TERMINAL,
                        status_code=response.status_code,
                    ) from exc
                self.circuit_breaker.succeeded()
                telemetry.payload_count = payload_count(payload)
                telemetry.accepted = telemetry.payload_count
                telemetry.secret_redaction_status = "PASSED"
                return (
                    payload,
                    telemetry,
                    {
                        "source_url": redact_url(str(response.request.url)),
                        "request_fingerprint": request_fingerprint(
                            normalized_method,
                            url,
                            params=params,
                            json_body=json_body,
                        ),
                        "safe_headers": json.dumps(safe_headers, sort_keys=True),
                    },
                )
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = DeterministicProviderError(
                    f"provider transport failed: {type(exc).__name__}",
                    classification=RetryClassification.RETRYABLE,
                )
            except DeterministicProviderError as exc:
                last_error = exc
            retryable = isinstance(last_error, DeterministicProviderError) and (
                last_error.classification
                in {RetryClassification.RETRYABLE, RetryClassification.RATE_LIMITED}
            )
            if retryable and attempt + 1 < self.retry_attempts:
                telemetry.retries += 1
                await self.sleeper(min(0.25 * (2**attempt), 2.0))
                continue
            self.circuit_breaker.failed(self.clock())
            raise last_error
        raise last_error or DeterministicProviderError("provider request failed")


def validate_endpoint_url(url: str, allowed_hosts: Iterable[str]) -> str:
    parsed = urlparse(str(url))
    if parsed.scheme.lower() != "https":
        raise DeterministicProviderError("provider URL must use HTTPS")
    if parsed.username or parsed.password:
        raise DeterministicProviderError("provider URL userinfo is forbidden")
    host = (parsed.hostname or "").lower().rstrip(".")
    allowed = {str(item).lower().rstrip(".") for item in allowed_hosts}
    if host not in allowed:
        raise DeterministicProviderError("provider host is not allowlisted")
    if _is_forbidden_host(host):
        raise DeterministicProviderError("provider host is not public")
    return host


def _is_forbidden_host(host: str) -> bool:
    if host == "localhost" or host.endswith((".test", ".invalid", ".example")):
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return not address.is_global


def redact_url(url: str) -> str:
    parsed = urlparse(str(url))
    query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        query.append((key, "<redacted>" if key.lower() in SECRET_QUERY_KEYS else value))
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query),
            "",
        )
    )


def redact_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(key): (
            "<redacted>"
            if str(key).lower() in AUTH_HEADER_KEYS
            else redact_sensitive(str(value))
        )
        for key, value in headers.items()
    }


def request_fingerprint(
    method: str,
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
) -> str:
    safe_params = {
        str(key): "<redacted>" if str(key).lower() in SECRET_QUERY_KEYS else value
        for key, value in (params or {}).items()
    }
    safe_body = redact_payload(json_body)
    canonical = json.dumps(
        {
            "method": str(method).upper(),
            "url": redact_url(url),
            "params": safe_params,
            "body": safe_body,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def safe_payload_hash(payload: Any) -> str:
    canonical = json.dumps(
        redact_payload(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def payload_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("data", "results", "observations", "news", "earningsCalendar"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
        return 1 if payload else 0
    return 0


def as_list(value: Any, *, field_name: str | None = None) -> list[dict[str, Any]]:
    if field_name and isinstance(value, dict):
        value = value.get(field_name)
    if value in (None, ""):
        return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    raise DeterministicProviderError("provider payload shape changed")
