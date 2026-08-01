from __future__ import annotations

from datetime import UTC, datetime

from app.core.senior_analyst_policy import (
    MAX_SENIOR_ANALYST_PAYLOAD_BYTES,
    MNQ_EARNINGS_SELECTION_POLICY,
    MNQ_PRIMARY_SYMBOLS,
    PROVIDER_ACCOUNTING_COLLECTION_PATHS,
)
from app.providers.earnings_provider import (
    parse_alpha_vantage_earnings_calendar,
)
from app.providers.finnhub import EARNINGS_SYMBOLS
from app.providers.fmp_earnings_calendar_provider import normalize_fmp_earnings
from app.providers.mega_cap_snapshot_provider import MEGA_CAP_TICKERS
from app.providers.nasdaq_earnings_provider import _is_relevant
from app.services.multi_source_runtime_service import (
    _with_runtime_fields,
    build_multi_source_context_blocks,
)


NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)
EXPECTED_PRIMARY_SYMBOLS = ("AAPL", "NVDA", "AMZN", "META", "TSLA", "AMD")
EXPECTED_MEGA_CAP_QUOTE_SYMBOLS = (
    "NVDA",
    "AAPL",
    "MSFT",
    "AMZN",
    "META",
    "GOOGL",
    "GOOG",
    "AVGO",
    "TSLA",
    "AMD",
    "NFLX",
    "COST",
)


def test_all_default_earnings_universes_derive_from_shared_policy() -> None:
    assert MNQ_EARNINGS_SELECTION_POLICY.policy_id == "MNQ_PRIMARY_EARNINGS_V1"
    assert MNQ_PRIMARY_SYMBOLS is MNQ_EARNINGS_SELECTION_POLICY.primary_symbols
    assert MNQ_PRIMARY_SYMBOLS == EXPECTED_PRIMARY_SYMBOLS
    assert EARNINGS_SYMBOLS is MNQ_PRIMARY_SYMBOLS

    for symbol in EXPECTED_PRIMARY_SYMBOLS:
        assert _is_relevant({"symbol": symbol}) is True
    for symbol in ("MSFT", "GOOGL", "QQQ", "SMH", "OTHER"):
        assert _is_relevant({"symbol": symbol}) is False


def test_shared_policy_contract_is_explicit_and_stable() -> None:
    policy = MNQ_EARNINGS_SELECTION_POLICY

    assert policy.include_nasdaq_100_components is False
    assert (policy.lookback_days, policy.lookahead_days) == (1, 14)
    assert policy.max_events == 24
    assert policy.sort_fields == ("event_date", "symbol")
    assert policy.minimum_fields == (
        "symbol",
        "event_date",
        "temporal_precision",
        "timing",
        "data_as_of",
        "content_valid_until",
        "refresh_due_at",
        "freshness",
        "source",
    )
    assert policy.date_only_temporal_precision == "DATE_ONLY"
    assert policy.date_only_timing == "UNKNOWN"
    assert PROVIDER_ACCOUNTING_COLLECTION_PATHS["earnings"] == (
        "analytics.earnings.events"
    )
    assert MAX_SENIOR_ANALYST_PAYLOAD_BYTES == 250_000


def test_fmp_and_alpha_vantage_apply_the_same_policy_universe() -> None:
    fmp_rows = [
        {"symbol": "MSFT", "date": "2026-08-01"},
        {"symbol": "AMD", "date": "2026-08-01"},
        {"symbol": "AAPL", "date": "2026-08-02"},
    ]
    fmp_events, _ = normalize_fmp_earnings(
        fmp_rows,
        retrieved_at=NOW,
        days=MNQ_EARNINGS_SELECTION_POLICY.lookahead_days,
    )
    assert [event["symbol"] for event in fmp_events] == ["AMD", "AAPL"]

    alpha_csv = """symbol,name,reportDate,fiscalDateEnding,estimate,currency
MSFT,Microsoft,2026-08-01,2026-06-30,3.21,USD
AMD,AMD,2026-08-01,2026-06-30,1.21,USD
AAPL,Apple,2026-08-02,2026-06-30,2.21,USD
"""
    alpha_events = parse_alpha_vantage_earnings_calendar(alpha_csv, NOW)
    assert [event["symbol"] for event in alpha_events] == ["AMD", "AAPL"]


def test_multi_source_corporate_block_filters_and_bounds_from_policy() -> None:
    events = [
        {
            "symbol": symbol,
            "event_date": f"2026-08-{day:02d}",
        }
        for day in range(1, 7)
        for symbol in (*reversed(MNQ_PRIMARY_SYMBOLS), "MSFT")
    ]
    corporate = build_multi_source_context_blocks(
        {
            "nasdaq_earnings": {
                "status": "found",
                "events": list(reversed(events)),
            }
        }
    )["corporate_events"]["earnings"]

    delivered = corporate["relevant_upcoming"]
    assert corporate["selection_policy"] == MNQ_EARNINGS_SELECTION_POLICY.policy_id
    assert corporate["total_available"] == len(events)
    assert corporate["relevant_count"] == 36
    assert corporate["delivered_count"] == MNQ_EARNINGS_SELECTION_POLICY.max_events
    assert corporate["excluded_count"] == len(events) - len(delivered)
    assert corporate["exclusion_counts"] == {
        "provider_prefiltered_or_invalid": 6,
        "bounded_limit": 12,
    }
    assert [(item["event_date"], item["symbol"]) for item in delivered] == sorted(
        (item["event_date"], item["symbol"]) for item in delivered
    )
    assert {item["symbol"] for item in delivered} <= set(MNQ_PRIMARY_SYMBOLS)
    assert corporate["mega_cap"] == delivered
    assert {item["symbol"] for item in corporate["semiconductors"]} <= {
        "AMD",
        "NVDA",
    }


def test_observed_provider_counts_survive_runtime_and_corporate_projection() -> None:
    relevant_event = {
        "symbol": "AMD",
        "event_date": "2026-08-01",
    }
    provider_payload = {
        "status": "found",
        "events": [relevant_event],
        "relevant_upcoming": [relevant_event],
        "selection_counts": {
            "total_available": 3_001,
            "relevant_count": 1,
            "delivered_count": 1,
            "excluded_count": 3_000,
        },
    }

    runtime = _with_runtime_fields(
        provider_payload,
        enabled=True,
        cache_used=False,
        provider_calls=1,
        attempted=True,
        persisted_count=1,
        read_back_count=1,
        materialized_count=1,
        item_count=1,
    )
    corporate = build_multi_source_context_blocks(
        {"nasdaq_earnings": runtime}
    )["corporate_events"]["earnings"]

    assert runtime["fetched_count"] == 3_001
    assert runtime["validated_count"] == 1
    assert runtime["rejected_count"] == 3_000
    assert runtime["excluded_count"] == 3_000
    assert corporate["total_available"] == 3_001
    assert corporate["relevant_count"] == 1
    assert corporate["delivered_count"] == 1
    assert corporate["excluded_count"] == 3_000
    assert corporate["exclusion_counts"] == {
        "provider_prefiltered_or_invalid": 3_000,
        "bounded_limit": 0,
    }
    assert corporate["relevant_upcoming"] == [relevant_event]


def test_earnings_policy_does_not_change_the_mega_cap_quote_universe() -> None:
    assert tuple(MEGA_CAP_TICKERS) == EXPECTED_MEGA_CAP_QUOTE_SYMBOLS
    assert tuple(MEGA_CAP_TICKERS) != MNQ_PRIMARY_SYMBOLS
