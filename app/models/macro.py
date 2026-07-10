from typing import Any

from pydantic import BaseModel, Field

from app.models.common import ProviderMetadata
from app.models.events import EconomicEvent


class MacroSeries(BaseModel):
    series_id: str
    name: str
    value: float | None = None
    units: str | None = None
    unit: str | None = None
    metric: str | None = None
    frequency: str | None = None
    source_url: str | None = None
    valid_until: str | None = None
    cache_status: str | None = None
    data_as_of: str | None = None
    source: str
    metadata: ProviderMetadata


class MacroLatestResponse(BaseModel):
    series: list[MacroSeries] = Field(default_factory=list)
    provider_results: list[ProviderMetadata] = Field(default_factory=list)


class EventWindow(BaseModel):
    event: EconomicEvent
    window_start_utc: str
    window_end_utc: str


class EventWindowsResponse(BaseModel):
    symbol: str
    checked_at_utc: str
    active_event_windows: list[EventWindow] = Field(default_factory=list)
    upcoming_event_windows: list[EventWindow] = Field(default_factory=list)
    note: str = "Data only. Trading decisions are delegated to AI-TRADER."


class MarketContextResponse(BaseModel):
    symbol: str
    generated_at_utc: str | None = None
    service_role: str = "data provider only"
    macro_snapshot: dict[str, Any] = Field(default_factory=dict)
    event_calendar: dict[str, Any] = Field(default_factory=dict)
    next_24h_events: list[dict[str, Any]] = Field(default_factory=list)
    next_7d_critical_events: list[dict[str, Any]] = Field(default_factory=list)
    fed_communications_today: list[dict[str, Any]] = Field(default_factory=list)
    recently_released_events: list[dict[str, Any]] = Field(default_factory=list)
    positioning: dict[str, Any] = Field(default_factory=dict)
    sentiment_context: dict[str, Any] = Field(default_factory=dict)
    sentiment: dict[str, Any] = Field(default_factory=dict)
    risk_sentiment: dict[str, Any] = Field(default_factory=dict)
    risk_context: dict[str, Any] = Field(default_factory=dict)
    fomc_context: dict[str, Any] = Field(default_factory=dict)
    nasdaq_context: dict[str, Any] | None = None
    economic_calendar_enrichment: dict[str, Any] = Field(default_factory=dict)
    market_schedule: dict[str, Any] = Field(default_factory=dict)
    corporate_events: dict[str, Any] = Field(default_factory=dict)
    source_reviews: dict[str, Any] = Field(default_factory=dict)
    news_context: dict[str, Any] = Field(default_factory=dict)
    news_digest: dict[str, Any] = Field(default_factory=dict)
    macro: MacroLatestResponse
    events_today: list[EconomicEvent]
    upcoming_high_impact_events: list[EconomicEvent]
    latest_news: dict[str, Any] = Field(default_factory=lambda: {"articles": []})
    event_windows: dict[str, Any] = Field(default_factory=dict)
    data_quality: dict[str, Any] = Field(default_factory=dict)
    db_summary: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
