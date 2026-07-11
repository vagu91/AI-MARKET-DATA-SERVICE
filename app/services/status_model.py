from __future__ import annotations

from enum import StrEnum


class AcquisitionStatus(StrEnum):
    FOUND = "found"
    PARTIAL = "partial"
    NOT_FOUND = "not_found"
    DISABLED = "disabled"
    ACCESS_RESTRICTED = "access_restricted"
    RATE_LIMITED = "rate_limited"
    PROVIDER_FAILED = "provider_failed"
    PROXY = "proxy"
    INVALID_DATA = "invalid_data"


class CacheStatus(StrEnum):
    VALID_CACHE = "valid_cache"
    STALE_CACHE = "stale_cache"
    LAST_KNOWN_GOOD = "last_known_good"
    NEGATIVE_CACHE = "negative_cache"


class DataUsability(StrEnum):
    USABLE = "usable"
    DEGRADED = "degraded"
    BLOCKED = "blocked"


class FreshnessStatus(StrEnum):
    FRESH = "fresh"
    STALE_ACCEPTABLE = "stale_acceptable"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


class ConsumerReadiness(StrEnum):
    READY = "ready"
    DEGRADED_READY = "degraded_ready"
    NOT_READY = "not_ready"


LEGACY_STATUS_MAP = {
    "valid": AcquisitionStatus.FOUND,
    "anomalous": AcquisitionStatus.PARTIAL,
    "restricted": AcquisitionStatus.ACCESS_RESTRICTED,
}


def normalize_status(value: str | None) -> str | None:
    if value is None:
        return None
    return str(LEGACY_STATUS_MAP.get(value, value))
