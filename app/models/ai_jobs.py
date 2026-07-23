from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class AIResearchEnqueueRequest(BaseModel):
    job_type: Literal[
        "MISSING_EVENT_RESEARCH", "RELEASE_ACTUAL_REFRESH", "SPEECH_OUTCOME_REFRESH",
        "MNQ_MARKET_RESEARCH", "EARNINGS_CONTEXT", "NEWS_DRIVER_RESEARCH", "CONFLICT_RESOLUTION",
        "MACRO_EVENTS_RESEARCH", "FED_RATES_RESEARCH", "VIX_RISK_RESEARCH",
        "COT_POSITIONING_RESEARCH", "NASDAQ_100_RESEARCH",
        "MEGA_CAP_SEMICONDUCTORS_RESEARCH", "EARNINGS_RESEARCH",
        "NEWS_RESEARCH", "GEOPOLITICAL_REGULATORY_RISK_RESEARCH",
    ]
    symbol: str = Field(default="MNQ", min_length=1, max_length=16)
    correlation_id: str = Field(min_length=1, max_length=160)
    event_key: str | None = Field(default=None, max_length=512)
    pending_fields: list[str] = Field(default_factory=list, max_length=50)
    request_payload: dict[str, Any] = Field(default_factory=dict)
    force_requeue: bool = False


class MarketResearchRunRequest(BaseModel):
    force_requeue: bool = False
    correlation_id: str | None = Field(default=None, max_length=160)
    authorized_live_smoke: bool = False
    wait_for_revision: int | None = Field(default=None, ge=1)
