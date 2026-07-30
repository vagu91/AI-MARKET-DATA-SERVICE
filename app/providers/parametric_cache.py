from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.providers.deterministic import RetryClassification


@dataclass(frozen=True)
class CacheResolution:
    value: Any
    cache_key: str
    cache_status: str
    telemetry: dict[str, Any]


class ParametricProviderCache:
    """TTL/LKG/negative-cache policy for parameterized deterministic calls."""

    def __init__(
        self,
        cache: ProviderCacheProtocol,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.cache = cache
        self.clock = clock

    @staticmethod
    def key(
        provider: str,
        endpoint: str,
        *,
        environment: str,
        parameters: dict[str, Any],
    ) -> str:
        identity = {
            "provider": str(provider).upper(),
            "endpoint": str(endpoint),
            "environment": str(environment).lower(),
            "parameters": parameters,
        }
        encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"), default=str)
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        return (
            f"deterministic:{identity['provider'].lower()}:{endpoint}:"
            f"{identity['environment']}:{digest}"
        )

    async def resolve(
        self,
        *,
        provider: str,
        endpoint: str,
        environment: str,
        parameters: dict[str, Any],
        ttl_seconds: int,
        loader: Callable[[], Awaitable[Any]],
        force_refresh: bool = False,
    ) -> CacheResolution:
        key = self.key(
            provider,
            endpoint,
            environment=environment,
            parameters=parameters,
        )
        now = _aware(self.clock())
        entry = (
            None
            if force_refresh
            else self.cache.get_entry(key)
        )
        if entry and _future(entry.get("valid_until"), now):
            negative = entry.get("status") == "negative_cache"
            return CacheResolution(
                value=entry.get("payload"),
                cache_key=key,
                cache_status="NEGATIVE_HIT" if negative else "HIT",
                telemetry=_telemetry(
                    provider,
                    endpoint,
                    cache_hit=0 if negative else 1,
                    negative_cache_hit=1 if negative else 0,
                ),
            )

        stale = (
            entry
            if entry
            and entry.get("status") in {"valid_cache", "last_known_good"}
            and _future(entry.get("stale_until"), now)
            else None
        )
        try:
            value = await loader()
        except Exception as exc:
            classification = getattr(exc, "classification", None)
            terminal = classification in {
                RetryClassification.TERMINAL,
                RetryClassification.AUTHENTICATION,
            }
            if terminal:
                negative_ttl = min(max(int(ttl_seconds), 30), 300)
                until = now + timedelta(seconds=negative_ttl)
                self.cache.set(
                    key,
                    [],
                    provider_name=provider,
                    valid_until=until.isoformat(),
                    stale_until=until.isoformat(),
                    status="negative_cache",
                    last_error=str(exc),
                    endpoint=endpoint,
                    parameters=parameters,
                    environment=environment,
                )
            if stale is not None:
                return CacheResolution(
                    value=stale.get("payload"),
                    cache_key=key,
                    cache_status="STALE_GRACE",
                    telemetry={
                        **_telemetry(provider, endpoint, actual_provider_requests=1),
                        "stale_grace": True,
                        "failure_classification": str(
                            classification.value
                            if hasattr(classification, "value")
                            else classification or type(exc).__name__
                        ),
                    },
                )
            raise

        ttl = max(0, int(ttl_seconds))
        if _empty(value):
            negative_ttl = min(max(ttl, 30), 300)
            valid_until = now + timedelta(seconds=negative_ttl)
            status = "negative_cache"
            cache_status = "REFRESHED_NO_DATA"
        else:
            valid_until = now + timedelta(seconds=ttl)
            status = "valid_cache"
            cache_status = "REFRESHED"
        stale_until = valid_until + timedelta(seconds=max(ttl, 60))
        self.cache.set(
            key,
            value,
            provider_name=provider,
            valid_until=valid_until.isoformat(),
            stale_until=stale_until.isoformat(),
            status=status,
            endpoint=endpoint,
            parameters=parameters,
            environment=environment,
        )
        return CacheResolution(
            value=value,
            cache_key=key,
            cache_status=cache_status,
            telemetry=_telemetry(provider, endpoint, actual_provider_requests=1),
        )


def _telemetry(
    provider: str,
    endpoint: str,
    *,
    actual_provider_requests: int = 0,
    cache_hit: int = 0,
    negative_cache_hit: int = 0,
) -> dict[str, Any]:
    return {
        "provider": provider,
        "endpoint_category": endpoint,
        "request_count": 1,
        "actual_provider_requests": actual_provider_requests,
        "cache_hit": cache_hit,
        "negative_cache_hit": negative_cache_hit,
        "retry": 0,
    }


def _empty(value: Any) -> bool:
    return value is None or value == [] or value == {}


def _future(value: Any, now: datetime) -> bool:
    if not value:
        return False
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return False
    return _aware(parsed) > now


def _aware(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
