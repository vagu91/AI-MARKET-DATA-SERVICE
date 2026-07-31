from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


MAX_SENIOR_ANALYST_PAYLOAD_BYTES = 250_000


@dataclass(frozen=True)
class EarningsSelectionPolicy:
    policy_id: str
    primary_symbols: tuple[str, ...]
    include_nasdaq_100_components: bool
    lookback_days: int
    lookahead_days: int
    max_observation_age_hours: int
    max_events: int
    sort_fields: tuple[str, ...]
    minimum_fields: tuple[str, ...]
    date_only_temporal_precision: str
    date_only_timing: str
    exact_temporal_precision: str


MNQ_EARNINGS_SELECTION_POLICY = EarningsSelectionPolicy(
    policy_id="MNQ_PRIMARY_EARNINGS_V1",
    primary_symbols=("AAPL", "NVDA", "AMZN", "META", "TSLA", "AMD"),
    include_nasdaq_100_components=False,
    lookback_days=1,
    lookahead_days=14,
    max_observation_age_hours=24,
    max_events=24,
    sort_fields=("event_date", "symbol"),
    minimum_fields=(
        "symbol",
        "event_date",
        "temporal_precision",
        "timing",
        "data_as_of",
        "content_valid_until",
        "refresh_due_at",
        "freshness",
        "source",
    ),
    date_only_temporal_precision="DATE_ONLY",
    date_only_timing="UNKNOWN",
    exact_temporal_precision="EXACT",
)

MNQ_PRIMARY_SYMBOLS = MNQ_EARNINGS_SELECTION_POLICY.primary_symbols

ProviderAccountingCollectionPath = str | tuple[str, ...]


# Tuple order is contractual: multi-node references concatenate and hash
# the exact consumer lists in this order without copying them into accounting.
PROVIDER_ACCOUNTING_COLLECTION_PATHS: Mapping[
    str,
    ProviderAccountingCollectionPath,
] = MappingProxyType(
    {
        "cpi": "analytics.macro.metrics",
        "ppi": "analytics.macro.metrics",
        "pce": "analytics.macro.metrics",
        "gdp": "analytics.macro.metrics",
        "employment": "analytics.macro.metrics",
        "wages": "analytics.macro.metrics",
        "nfp": "analytics.macro.metrics",
        "jobless_claims": "analytics.macro.metrics",
        "treasury_rates": "analytics.rates.treasury_rates",
        "fed_funds": "analytics.rates.fed_funds",
        "nasdaq_100": "analytics.nasdaq.components",
        "mega_cap_quotes": "analytics.nasdaq.components",
        "macro_calendar": (
            "analytics.calendar.active_event_windows",
            "analytics.calendar.next_24h_events",
            "analytics.calendar.next_7d_high_impact_events",
        ),
        "flash_services_pmi": (
            "analytics.calendar.latest_released_events",
            "analytics.calendar.active_event_windows",
            "analytics.calendar.next_24h_events",
            "analytics.calendar.next_7d_high_impact_events",
        ),
        "earnings": "analytics.earnings.events",
        "current_news": "analytics.news.current_news",
    }
)
