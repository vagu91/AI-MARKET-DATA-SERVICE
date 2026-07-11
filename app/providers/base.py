import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.redaction import redact_sensitive
from app.models.common import Freshness, ProviderMetadata, ProviderResult, ProviderType

logger = logging.getLogger(__name__)


class ProviderError(RuntimeError):
    pass


class BaseProvider(ABC):
    source: str
    provider_type: ProviderType
    reliability: float
    cache_key: str

    def __init__(self, cache: ProviderCacheProtocol) -> None:
        self.cache = cache

    @abstractmethod
    async def fetch(self) -> ProviderResult:
        raise NotImplementedError

    async def fetch_safe(self) -> ProviderResult:
        try:
            result = await self.fetch()
            self.cache.set(self.cache_key, result.model_dump(mode="json"))
            return result
        except Exception as exc:
            detail = str(exc) or f"{type(exc).__name__}"
            error = redact_sensitive(f"{self.source} failed: {detail}")
            logger.warning(
                "provider_failed",
                extra={"_provider": self.source, "_error": error},
            )
            cached = self.cache.get(self.cache_key)
            if cached:
                result = ProviderResult.model_validate(cached)
                result.metadata.provider_type = ProviderType.CACHE
                result.metadata.is_fallback = True
                result.metadata.freshness = Freshness.STALE
                result.metadata.errors.append(error)
                result.metadata.retrieved_at = datetime.now(UTC)
                return result
            return ProviderResult(
                metadata=ProviderMetadata(
                    source=self.source,
                    provider_type=self.provider_type,
                    retrieved_at=datetime.now(UTC),
                    freshness=Freshness.UNKNOWN,
                    reliability=0.0,
                    is_fallback=True,
                    errors=[error],
                ),
                data={},
            )


def metadata(
    source: str,
    provider_type: ProviderType,
    reliability: float,
    data_as_of: datetime | None = None,
    freshness: Freshness = Freshness.RECENT,
    is_fallback: bool = False,
    errors: list[str] | None = None,
) -> ProviderMetadata:
    return ProviderMetadata(
        source=source,
        provider_type=provider_type,
        retrieved_at=datetime.now(UTC),
        data_as_of=data_as_of,
        freshness=freshness,
        reliability=reliability,
        is_fallback=is_fallback,
        errors=[redact_sensitive(str(error)) for error in (errors or [])],
    )


def latest_observation(observations: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid = [item for item in observations if item.get("value") not in (None, ".")]
    return valid[-1] if valid else None
