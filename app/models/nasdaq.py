from datetime import UTC, datetime
from typing import Any
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
    provider_calls: int = 0
    actual_network_calls: int = 0
    cache_used: bool = False
    run_cache_used: bool = False
    run_deduplicated_calls: int = 0


class QQQHolding(BaseModel):
    symbol: str
    name: str | None = None
    company_name: str | None = None
    share_class: str | None = None
    weight: float | None = None
    weight_pct: float | None = None
    sector: str | None = None
    weight_source: str | None = None
    weight_source_url: str | None = None
    weight_method: str | None = None
    weight_as_of: str | None = None
    weight_retrieved_at: datetime | None = None
    weight_valid_until: datetime | None = None
    weight_verified: bool = False
    weight_is_official: bool = False
    weight_is_reconstructed: bool = False
    weight_confidence: float = 0.0
    market_cap: float | None = None
    market_cap_raw: str | float | None = None
    market_cap_parsed: float | None = None
    security_market_cap: float | None = None
    implied_shares: float | None = None
    issuer_id: str | None = None
    issuer_name: str | None = None
    issuer_group: str | None = None
    issuer_identifier: str | None = None
    cik: str | None = None
    isin: str | None = None
    cusip: str | None = None
    security_type: str | None = None
    market_cap_semantics: str = "unknown"
    market_cap_raw_semantics: str | None = None
    market_cap_source: str | None = None
    market_cap_source_url: str | None = None
    market_cap_verified: bool = False
    market_cap_is_issuer_level: bool = False
    market_cap_is_security_level: bool = False
    multi_class_group: str | None = None
    multi_class_adjustment_applied: bool = False
    multi_class_adjustment_method: str | None = None
    multi_class_confidence: float = 0.0
    class_shares: float | None = None
    class_shares_source: str | None = None
    class_shares_source_url: str | None = None
    class_shares_as_of: str | None = None
    class_shares_retrieved_at: datetime | None = None
    class_shares_valid_until: datetime | None = None
    class_shares_verified: bool = False
    issuer_aggregate_weight_pct: float | None = None
    price: float | None = None
    change_pct: float | None = None
    price_source: str | None = None
    shares_outstanding: float | None = None
    source: str | None = None
    source_url: str | None = None
    as_of: str | None = None
    retrieved_at: datetime | None = None
    valid_until: datetime | None = None
    is_official: bool = False
    is_reconstructed: bool = False
    confidence: float = 0.0
    warnings: list[str] = Field(default_factory=list)


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
    source_attempt_count: int = 0
    source_success_count: int = 0
    source_failure_count: int = 0
    official_source_success: bool = False
    vendor_source_success: bool = False
    reconstruction_used: bool = False
    equal_weight_used: bool = False
    weighted_constituent_count: int = 0
    missing_weight_count: int = 0
    missing_price_count: int = 0
    duplicate_symbol_count: int = 0
    negative_weight_count: int = 0
    zero_weight_count: int = 0
    total_weight_pct: float | None = None
    top_10_weight_pct: float | None = None
    largest_weight_pct: float | None = None
    weight_coverage_pct: float = 0.0
    official_weight_coverage_pct: float = 0.0
    price_coverage_pct: float = 0.0
    weighted_contribution_coverage_pct: float = 0.0
    sector_weight_coverage_pct: float = 0.0
    stale_weight_count: int = 0
    proxy_penalty: float = 0.0
    weight_quality_score: float = 0.0
    weight_method: str | None = None
    weight_freshness: str = "UNKNOWN"
    weight_age_hours: float | None = None
    last_successful_weight_refresh_at: str | None = None
    next_weight_refresh_at: str | None = None
    normalization_applied: bool = False
    fallback_chain: list[dict[str, Any]] = Field(default_factory=list)
    alternative_sources: list[dict[str, Any]] = Field(default_factory=list)
    failure_breakdown: dict[str, int] = Field(default_factory=dict)
    multi_class_issuer_count: int = 0
    multi_class_security_count: int = 0
    verified_security_cap_count: int = 0
    issuer_level_duplicate_count: int = 0
    issuer_level_probable_count: int = 0
    unknown_market_cap_semantics_count: int = 0
    multi_class_adjustment_count: int = 0
    multi_class_weight_coverage_pct: float = 0.0
    issuer_semantics_quality_score: float = 0.0
    multi_class_diagnostics: list[dict[str, Any]] = Field(default_factory=list)


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
    weight_method: str | None = None
    weight_source: str | None = None
    weight_source_url: str | None = None
    weight_as_of: str | None = None
    weight_valid_until: datetime | None = None
    weight_verified: bool = False
    weight_is_official: bool = False
    weight_is_reconstructed: bool = False
    weight_confidence: float = 0.0
    holdings: list[QQQHolding] = Field(default_factory=list)
    data_quality: QQQHoldingsQuality


class MegaCapStock(BaseModel):
    symbol: str
    name: str | None = None
    weight: float | None = None
    weight_method: str | None = None
    weight_source: str | None = None
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
    weight_pct: float | None = None
    change_pct: float
    weighted_contribution: float
    weighted_contribution_pct_points: float | None = None
    contribution_rank: int | None = None
    direction: str | None = None
    price_source: str | None = None
    weight_source: str | None = None


class MegaCapBreadthQuality(BaseModel):
    missing_weights: list[str] = Field(default_factory=list)
    missing_prices: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    covered_weight_pct: float = 0.0
    uncovered_weight_pct: float = 100.0
    price_coverage_pct: float = 0.0
    weight_coverage_pct: float = 0.0


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
    coverage_adjusted_weighted_change_pct: float | None = None
    weighted_positive_contribution: float = 0.0
    weighted_negative_contribution: float = 0.0
    weighted_net_contribution: float = 0.0
    covered_weight_pct: float = 0.0
    uncovered_weight_pct: float = 100.0
    calculation_method: str = "unavailable"
    weight_method: str | None = None
    covered_symbols: list[str] = Field(default_factory=list)
    missing_price_symbols: list[str] = Field(default_factory=list)
    missing_weight_symbols: list[str] = Field(default_factory=list)
    confidence: float = 0.0
    is_proxy: bool = False
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
    provider_last_updated: str | None = None
    retrieved_at_utc: datetime | None = None
    lineage: dict[str, Any] = Field(default_factory=dict)
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
    original_publisher: str | None = None
    source_classification: str | None = None
    author: str | None = None
    summary_source_url: str | None = None
    summary_quality: float | None = None
    summary_is_generated: bool = False
    summary_reliability: float | None = None
    is_official_source: bool = False
    is_primary_source: bool = False
    entities: list[str] = Field(default_factory=list)
    matched_entities: list[str] = Field(default_factory=list)
    topic_classifications: list[dict[str, Any]] = Field(default_factory=list)
    relevance_score: float | None = None
    relevance_reasons: list[str] = Field(default_factory=list)
    relevance_tier: str | None = None
    exclusion_reason: str | None = None
    accepted: bool = True
    article_id: str | None = None
    duplicate_group_id: str | None = None
    duplicate_of: str | None = None
    syndication_group: str | None = None
    independent_source_count: int = 1
    pipeline_version: str | None = None
    warnings: list[str] = Field(default_factory=list)


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
