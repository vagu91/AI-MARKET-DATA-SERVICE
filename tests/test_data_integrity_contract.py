from datetime import UTC, datetime, timedelta

from app.main import app
from app.providers.ai_researcher_provider import AIResearcherProvider
from app.core.config import Settings
from app.services.data_integrity_service import (
    classify_source,
    fact_temporal_status,
    next_release_refresh_at,
    reject_future_actual,
    sector_exposure,
    temporal_status,
)
from app.services.market_context_builder import build_event_calendar, build_news_context
from app.models.common import Impact, ProviderType
from app.models.events import EconomicEvent, EventEnrichment


def settings(tmp_path) -> Settings:
    return Settings(_env_file=None, database_path=tmp_path / "market.sqlite")


def event_with_metric(metric: dict, *, release_at: datetime | None = None) -> EconomicEvent:
    release = release_at or datetime(2099, 7, 14, 12, 30, tzinfo=UTC)
    return EconomicEvent(
        event_id="evt-cpi",
        name="Consumer Price Index",
        country="US",
        category="CPI",
        date=release.date().isoformat(),
        time_utc=release,
        impact=Impact.HIGH,
        source="BLS",
        source_url="https://www.bls.gov/schedule/",
        reliability=0.9,
        event_risk_level=Impact.HIGH,
        default_risk_window_before_minutes=30,
        default_risk_window_after_minutes=30,
        enrichment=EventEnrichment(
            metrics=[metric],
            source="BLS",
            source_url="https://www.bls.gov/news.release/cpi.nr0.htm",
            provider_type=ProviderType.API,
            reliability=0.9,
            confidence=0.9,
        ),
    )


def test_future_actual_rejected_before_release():
    item = {
        "fact_key": "future-cpi",
        "time_utc": "2099-07-14T12:30:00+00:00",
        "actual": "0.4%",
        "metrics": [{"metric_id": "headline_cpi_mom", "actual": 0.4}],
    }

    cleaned, rejected = reject_future_actual(item, now=datetime(2099, 7, 14, 12, 0, tzinfo=UTC))

    assert rejected is True
    assert cleaned["actual"] is None
    assert cleaned["metrics"][0]["actual"] is None
    assert "actual_before_release_rejected" in cleaned["warnings"]


def test_actual_after_release_accepted():
    item = {"time_utc": "2099-07-14T12:30:00+00:00", "actual": "0.4%"}
    cleaned, rejected = reject_future_actual(item, now=datetime(2099, 7, 14, 12, 31, tzinfo=UTC))
    assert rejected is False
    assert cleaned["actual"] == "0.4%"


def test_ai_provider_rejects_future_actual_payload(tmp_path):
    provider = AIResearcherProvider(settings(tmp_path))
    payload = {
        "generated_at": "2099-07-10T00:00:00+00:00",
        "results": [
            {
                "fact_key": "future-cpi",
                "country": "US",
                "date": "2099-07-14",
                "time_utc": "2099-07-14T12:30:00+00:00",
                "category": "CPI",
                "event_name": "Consumer Price Index",
                "actual": "0.4%",
                "source": "BLS",
                "source_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                "valid_until": "2099-07-14T12:30:00+00:00",
                "reliability": 0.9,
                "confidence": 0.9,
            }
        ],
    }

    facts, status = provider.load_payload(payload)

    assert status["future_actual_rejected"] == 1
    assert facts[0]["actual"] is None


def test_temporal_state_and_retry_schedule():
    release = datetime(2099, 7, 14, 12, 30, tzinfo=UTC)
    assert temporal_status(release_at=release, actual=None, now=release - timedelta(minutes=1)) == "pre_release"
    assert temporal_status(release_at=release, actual=None, now=release + timedelta(seconds=1)) == "awaiting_actual"
    assert temporal_status(release_at=release, actual="0.4", now=release + timedelta(seconds=1)) == "released"
    assert next_release_refresh_at(release_at=release, attempt_count=2, retry_seconds=[30, 120, 300], now=release) == (
        release + timedelta(seconds=300)
    ).isoformat()


def test_fred_published_macro_series_with_value_is_not_awaiting_actual():
    fact = {
        "fact_key": "FRED:VIXCLS:latest:official_macro_latest",
        "fact_type": "official_macro_latest",
        "value": "16.9",
        "release_at": "2026-07-08",
        "valid_until": "2099-07-08T00:00:00+00:00",
    }

    assert fact_temporal_status(fact, now=datetime(2026, 7, 10, tzinfo=UTC)) == "published"


def test_fred_expired_macro_series_is_refresh_due_not_awaiting_actual():
    fact = {
        "fact_key": "FRED:VIXCLS:latest:official_macro_latest",
        "fact_type": "official_macro_latest",
        "value": "16.9",
        "release_at": "2026-07-08",
        "valid_until": "2026-07-09T00:00:00+00:00",
    }

    assert fact_temporal_status(fact, now=datetime(2026, 7, 10, tzinfo=UTC)) == "refresh_due"


def test_cpi_post_release_without_actual_is_awaiting_actual():
    fact = {
        "fact_key": "US:CPI:2026-07-14:consumer-price-index:macro_event_enrichment",
        "fact_type": "macro_event_enrichment",
        "event_name": "Consumer Price Index",
        "release_at": "2026-07-14T12:30:00+00:00",
        "actual": None,
    }

    assert fact_temporal_status(fact, now=datetime(2026, 7, 14, 12, 31, tzinfo=UTC)) == "awaiting_actual"


def test_expired_news_excluded_and_marketbeat_not_official():
    news = build_news_context(
        [
            {
                "title": "Fresh BLS release",
                "source": "BLS",
                "source_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                "published_at": datetime.now(UTC).isoformat(),
                "summary": "The official BLS release reports current Consumer Price Index data.",
                "valid_until": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            },
            {
                "title": "Old MarketBeat story",
                "source": "MarketBeat",
                "source_url": "https://marketbeat.test/story",
                "valid_until": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
            },
        ]
    )

    assert len(news["latest"]) == 1
    assert news["official_sources"][0]["source"] == "BLS"
    assert classify_source("MarketBeat", "https://marketbeat.test")["is_official_source"] is False


def test_placeholder_news_title_excluded_from_latest():
    news = build_news_context(
        [
            {
                "title": "META_TITLE_QUOTE - Yahoo Finance",
                "source": "Yahoo Finance",
                "source_url": "https://news.test/meta-title-quote",
                "valid_until": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            },
            {
                "title": "Real QQQ market story",
                "source": "Reuters",
                "source_url": "https://news.test/real-story",
                "published_at": datetime.now(UTC).isoformat(),
                "summary": "Reuters reports a material development affecting QQQ and Nasdaq markets.",
                "valid_until": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            },
        ]
    )

    assert [item["title"] for item in news["latest"]] == ["Real QQQ market story"]


def test_source_classification_official_and_market_sources():
    assert classify_source("BLS", "https://www.bls.gov/news.release/cpi.nr0.htm")["source_classification"] == "official_source"
    assert classify_source("BEA", "https://www.bea.gov/news")["is_official_source"] is True
    assert classify_source("Federal Reserve", "https://www.federalreserve.gov/")["is_official_source"] is True
    assert classify_source("Yahoo Finance", "https://finance.yahoo.com/")["source_classification"] == "market_source"


def test_sector_exposure_classifies_top_qqq_holdings_below_unknown_threshold():
    exposure = sector_exposure(
        [
            {"symbol": "NVDA", "weight": 7.0},
            {"symbol": "AAPL", "weight": 7.0},
            {"symbol": "MSFT", "weight": 6.0},
            {"symbol": "UNKNOWN", "weight": 1.0},
        ]
    )
    assert exposure["unknown_weight_pct"] == 80.0
    assert exposure["classified_weight_pct"] == 20.0
    assert exposure["covered_holdings_weight_pct"] == 21.0
    assert exposure["uncovered_holdings_weight_pct"] == 79.0
    assert exposure["complete_portfolio_coverage"] is False


def test_metric_previous_sets_summary_previous_true():
    calendar = build_event_calendar(
        [
            event_with_metric(
                {
                    "metric_id": "headline_cpi_mom",
                    "previous": 0.5,
                    "unit": "percent",
                    "frequency": "MoM",
                    "source_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                }
            )
        ]
    )
    summary = calendar["critical_macro_events"][0].enrichment.summary
    assert summary["has_previous"] is True
    assert summary["has_actual"] is False


def test_fed_speech_quantitative_fields_not_applicable():
    speech = EconomicEvent(
        event_id="fed-speech",
        name="Speech - Chair",
        country="US",
        category="Fed Speech",
        date="2099-07-14",
        time_utc=datetime(2099, 7, 14, 15, tzinfo=UTC),
        impact=Impact.HIGH,
        source="Federal Reserve",
        source_url="https://www.federalreserve.gov/",
        reliability=0.9,
    )
    calendar = build_event_calendar([speech])
    summary = calendar["fed_communications"][0].enrichment.summary
    assert summary["quantitative_fields_applicable"] is False


def test_diagnostic_and_refresh_routes_registered():
    paths = app.openapi()["paths"]
    assert "/diagnostics/temporal-integrity" in paths
    assert "/diagnostics/release-refresh-status" in paths
    assert "/diagnostics/news-freshness" in paths
    assert "/diagnostics/source-classification" in paths
    assert "/market-context/mnq" in paths
