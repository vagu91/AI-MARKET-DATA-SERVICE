from __future__ import annotations

from enum import StrEnum


class RefreshMode(StrEnum):
    FALSE = "false"
    AUTO = "auto"
    FORCE = "force"


class RefreshPolicyService:
    @staticmethod
    def normalize(value: str | None) -> RefreshMode:
        if value in (None, "", "auto"):
            return RefreshMode.AUTO
        if value == "false":
            return RefreshMode.FALSE
        if value == "force":
            return RefreshMode.FORCE
        raise ValueError(f"unsupported refresh mode: {value}")

    @staticmethod
    def allow_network(value: str | None) -> bool:
        return RefreshPolicyService.normalize(value) in {RefreshMode.AUTO, RefreshMode.FORCE}

    @staticmethod
    def require_cache_only(value: str | None) -> bool:
        return RefreshPolicyService.normalize(value) == RefreshMode.FALSE

    @staticmethod
    def bypass_valid_cache(value: str | None) -> bool:
        return RefreshPolicyService.normalize(value) == RefreshMode.FORCE
