import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.core.redaction import redact_sensitive
from app.models.common import Freshness, ProviderMetadata, ProviderResult, ProviderType
from app.providers.deterministic import (
    ProviderKind,
    redact_url,
    request_fingerprint,
    safe_payload_hash,
)

logger = logging.getLogger(__name__)


class ProviderError(RuntimeError):
    pass


class ProviderDisabled(ProviderError):
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
        except ProviderDisabled as exc:
            detail = redact_sensitive(str(exc) or f"{self.source} disabled")
            return ProviderResult(
                metadata=ProviderMetadata(
                    source=self.source,
                    provider_type=self.provider_type,
                    retrieved_at=datetime.now(UTC),
                    freshness=Freshness.UNKNOWN,
                    reliability=0.0,
                    is_fallback=True,
                    errors=[],
                ),
                data={"status": "disabled", "warning": detail},
            )
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

    def provider_contract(
        self,
        *,
        requested_domain: str,
        source_url: str,
        result: ProviderResult | None = None,
        exact_occurrence_identity: str | None = None,
        cache_status: str = "MISS",
        request_method: str = "GET",
        request_params: dict[str, Any] | None = None,
        telemetry: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Project any legacy or new adapter into the common deterministic contract."""
        source = str(self.source).upper()
        if source in {"FRED", "BLS", "BEA", "CENSUS"}:
            kind = ProviderKind.OFFICIAL_GOVERNMENT
            authority_tier = 1
        elif source == "TRADIER":
            kind = ProviderKind.LICENSED_MARKET_DATA
            authority_tier = 2
        else:
            kind = ProviderKind.STRUCTURED_VENDOR
            authority_tier = 3
        retrieved_at = (
            result.metadata.retrieved_at
            if result is not None
            else datetime.now(UTC)
        )
        errors = list(result.metadata.errors) if result is not None else []
        payload = result.data if result is not None else {}
        return {
            "provider_id": source.lower(),
            "provider_kind": kind.value,
            "authority_tier": authority_tier,
            "requested_domain": requested_domain,
            "retrieved_at": retrieved_at.isoformat(),
            "provider_timestamp": (
                result.metadata.data_as_of.isoformat()
                if result is not None and result.metadata.data_as_of
                else None
            ),
            "source_url": redact_url(source_url),
            "request_fingerprint": request_fingerprint(
                request_method,
                source_url,
                params=request_params,
            ),
            "cache_status": cache_status,
            "freshness": (
                result.metadata.freshness.value
                if result is not None
                else Freshness.UNKNOWN.value
            ),
            "exact_occurrence_identity": exact_occurrence_identity,
            "normalized_observation_count": (
                len(payload) if isinstance(payload, (dict, list)) else 0
            ),
            "warnings": [redact_sensitive(item) for item in errors],
            "rejection_reasons": [],
            "rate_limit": {},
            "retry_classification": "NONE",
            "raw_payload_hash": safe_payload_hash(payload),
            "lineage": {
                "provider": source,
                "requested_domain": requested_domain,
                "exact_occurrence_identity": exact_occurrence_identity,
            },
            "telemetry": telemetry or {},
        }


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
    return max(valid, key=lambda item: str(item.get("date") or ""), default=None)
