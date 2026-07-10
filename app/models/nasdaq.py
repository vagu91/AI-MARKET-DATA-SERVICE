from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from app.models.common import Impact, ProviderType


class MarketSession(StrEnum):
    PREMARKET = "PREMARKET"
    REGULAR = "REGULAR"
    AFTER_HOURS = "AFTER_HOURS"
    UNKNOWN = "UNKNOWN"


class EarningsTiming(StrEnum):
    BEFORE_MARKET = "BEFORE_MARKET"
    AFTER_CLOSE = "AFTER_CLOSE"
    UNKNOWN = "UNKNOWN"


class Relevance(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class DataQuality(BaseModel):
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    fallback_used: bool = False
    final_data_available: bool = False
    no_data_found: bool = False
    provider_failed: bool = False
    rate_limited: bool = False


class QQQHolding(BaseModel):
    symbol: str
    name: str | None = None
    weight: float | None = None
    sector: str | None = None


class QQQHoldingsQuality(DataQuality):
    count: int = 0
    missing_weights: bool = False
    stale: bool = False
    provider_attempts: list[str] = Field(default_factory=list)
    actual_network_calls: int = 0
    run_deduplicated_calls: int = 0
    run_cache_used: bool = False
    alpha_vantage_status: str | None = None
    alpha_vantage_rate_limited: bool = False
    alpha_vantage_next_retry_at: str | None = None
    invesco_status: str | None = None
    invesco_http_status: int | None = None
    nasdaq_proxy_used: bool = False
    last_known_good_used: bool = False
    final_source: str | None = None
    final_status: str | None = None
    holdings_count: int = 0
    weights_available: bool = False
    is_proxy: bool = False
    proxy_for: str | None = None
    official_etf_holdings: bool = True
    weight_data_available: bool = True


class QQQHoldingsResponse(BaseModel):
    status: str = "found"
    as_of: str | None = None
    source: str
    provider_type: ProviderType
    retrieved_at: datetime
    reliability: float
    is_fallback: bool = False
    is_proxy: bool = False
    proxy_for: str | None = None
    holdings_count: int = 0
    weight_data_available: bool = True
    official_etf_holdings: bool = True
    holdings: list[QQQHolding] = Field(default_factory=list)
    data_quality: QQQHoldingsQuality


class MegaCapStock(BaseModel):
    symbol: str
    name: str | None = None
    weight: float | None = None
    last_price: float | None = None
    change: float | None = None
    change_pct: float | None = None
    volume: int | None = None
    market_session: MarketSession = MarketSession.UNKNOWN
    currency: str = "USD"
    source: str
    retrieved_at: datetime


class MegaCapSnapshotQuality(DataQuality):
    tracked_count: int
    resolved_count: int = 0
    missing_prices: list[str] = Field(default_factory=list)


class MegaCapSnapshotResponse(BaseModel):
    retrieved_at: datetime
    source: str
    provider_type: ProviderType
    reliability: float
    stocks: list[MegaCapStock] = Field(default_factory=list)
    data_quality: MegaCapSnapshotQuality


class BreadthContributor(BaseModel):
    symbol: str
    weight: float
    change_pct: float
    weighted_contribution: float


class MegaCapBreadthQuality(BaseModel):
    missing_weights: list[str] = Field(default_factory=list)
    missing_prices: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class MegaCapBreadthResponse(BaseModel):
    retrieved_at: datetime
    tracked_count: int
    positive_count: int
    negative_count: int
    neutral_count: int
    weighted_positive_pct: float
    weighted_negative_pct: float
    weighted_neutral_pct: float
    average_change_pct: float
    weighted_average_change_pct: float
    top_positive_contributors: list[BreadthContributor] = Field(default_factory=list)
    top_negative_contributors: list[BreadthContributor] = Field(default_factory=list)
    source: str = "MEGA_CAP_SNAPSHOT+QQQ_HOLDINGS"
    reliability: float
    note: str = "Data only. Trading decisions are delegated to AI-TRADER."
    data_quality: MegaCapBreadthQuality


class EarningsEvent(BaseModel):
    symbol: str
    company: str | None = None
    date: str
    timing: EarningsTiming = EarningsTiming.UNKNOWN
    eps_estimate: float | None = None
    eps_actual: float | None = None
    revenue_estimate: float | None = None
    revenue_actual: float | None = None
    source: str
    source_url: str
    event_risk_level: Impact = Impact.MEDIUM
    reliability: float


class EarningsQuality(DataQuality):
    pass


class EarningsResponse(BaseModel):
    retrieved_at: datetime
    days: int
    events: list[EarningsEvent] = Field(default_factory=list)
    data_quality: EarningsQuality


class NewsArticle(BaseModel):
    title: str
    source: str
    published_at: datetime | None = None
    url: str
    summary: str | None = None
    content_snippet: str | None = None
    source_url: str | None = None
    canonical_url: str | None = None
    aggregator_url: str | None = None
    summary_source_type: str | None = None
    source_text_available: bool = False
    is_official: bool = False
    symbols: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    relevance: Relevance = Relevance.LOW
    provider_type: ProviderType
    reliability: float


class NewsQuality(DataQuality):
    pass


class NewsResponse(BaseModel):
    retrieved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    articles: list[NewsArticle] = Field(default_factory=list)
    data_quality: NewsQuality


class QQQHoldingsSummary(BaseModel):
    as_of: str | None = None
    top_holdings: list[QQQHolding] = Field(default_factory=list)
    source: str
    reliability: float


class NasdaqContextResponse(BaseModel):
    generated_at: datetime
    service_role: str = "data provider only"
    qqq_holdings: QQQHoldingsResponse | None = None
    qqq_holdings_summary: QQQHoldingsSummary
    mega_cap_snapshot: MegaCapSnapshotResponse
    mega_cap_breadth: MegaCapBreadthResponse
    upcoming_earnings: EarningsResponse
    latest_news: NewsResponse
    metadata: dict[str, object] = Field(default_factory=dict)
