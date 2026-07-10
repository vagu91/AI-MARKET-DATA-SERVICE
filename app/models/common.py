from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class ProviderType(StrEnum):
    API = "API"
    RSS = "RSS"
    CSV = "CSV"
    SCRAPER = "SCRAPER"
    CACHE = "CACHE"
    DB = "DB"
    OPENAI = "OPENAI"
    AI_RESEARCHER = "AI_RESEARCHER"
    AI_RESEARCHER_CODEX_CLI = "AI_RESEARCHER_CODEX_CLI"
    AI_WEB_FALLBACK = "AI_WEB_FALLBACK"
    SEARCH_SNIPPET = "SEARCH_SNIPPET"


class Freshness(StrEnum):
    LIVE = "LIVE"
    RECENT = "RECENT"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


class Impact(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ProviderMetadata(BaseModel):
    source: str
    provider_type: ProviderType
    retrieved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    data_as_of: datetime | None = None
    freshness: Freshness = Freshness.UNKNOWN
    reliability: float = Field(ge=0.0, le=1.0)
    is_fallback: bool = False
    errors: list[str] = Field(default_factory=list)


class ProviderResult(BaseModel):
    metadata: ProviderMetadata
    data: dict[str, Any] | list[dict[str, Any]]
