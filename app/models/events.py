from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models.common import Impact, ProviderType


class EventEnrichment(BaseModel):
    forecast: Any | None = None
    previous: Any | None = None
    consensus: Any | None = None
    actual: Any | None = None
    estimate_count: int | None = None
    estimate_low: Any | None = None
    estimate_high: Any | None = None
    median_estimate: Any | None = None
    average_estimate: Any | None = None
    forecast_origin: str | None = None
    consensus_source: str | None = None
    consensus_source_url: str | None = None
    consensus_retrieved_at: datetime | None = None
    consensus_valid_until: datetime | None = None
    consensus_verified: bool = False
    metrics: list[dict[str, Any]] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    fomc_context: dict[str, Any] | None = None
    source: str | None = None
    source_url: str | None = None
    provider_type: ProviderType | None = None
    retrieved_at: datetime | None = None
    valid_until: datetime | None = None
    next_refresh_at: datetime | None = None
    reliability: float = Field(default=0.0, ge=0.0, le=1.0)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    evidence: str | None = None
    evidence_text: str | None = None
    validation: dict[str, Any] = Field(default_factory=dict)
    field_lineage: dict[str, dict[str, Any]] = Field(default_factory=dict)
    cache_status: str | None = None
    matched_by: str | None = None
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class EconomicEvent(BaseModel):
    event_id: str
    name: str
    country: str = "US"
    category: str
    metric_id: str | None = None
    normalized_event_family: str | None = None
    reference_period: str | None = None
    frequency: str | None = None
    date: str
    time_utc: datetime | None = None
    time_local: datetime | None = None
    impact: Impact
    actual: Any | None = None
    forecast: Any | None = None
    previous: Any | None = None
    source: str
    source_url: str
    reliability: float = Field(ge=0.0, le=1.0)
    incomplete_time: bool = False
    event_risk_level: Impact = Impact.LOW
    default_risk_window_before_minutes: int = 0
    default_risk_window_after_minutes: int = 0
    enrichment: EventEnrichment = Field(default_factory=EventEnrichment)
