from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from app.core.config import Settings
from app.core.text_normalization import normalize_payload_text, normalize_text
from app.models.common import Freshness, Impact, ProviderMetadata, ProviderResult, ProviderType
from app.models.events import EconomicEvent
from app.models.macro import MacroLatestResponse, MacroSeries
from app.providers.hacker_news_social_sentiment_provider import build_social_sentiment
from app.providers.scraper_calendar import EconomicCalendarScraperProvider
from app.services.ai_trader_contract_service import build_ai_trader_market_context
from app.services.context_extensions_service import enrich_nasdaq_context
from app.services.data_integrity_service import classify_source, sector_exposure
from app.services.diagnostics_service import DiagnosticsService
from app.services.market_context_builder import build_event_calendar, build_news_context


def _metadata(source: str = "FRED") -> ProviderMetadata:
    return ProviderMetadata(
        source=source,
        provider_type=ProviderType.API,
        retrieved_at=datetime.now(UTC),
        freshness=Freshness.RECENT,
        reliability=0.9,
    )


def _series(series_id: str = "DGS10", source: str = "FRED") -> MacroSeries:
    return MacroSeries(
        series_id=series_id,
        name=series_id,
        value=4.1,
        units="percent",
        data_as_of="2026-07-10",
        source=source,
        metadata=_metadata(source),
    )


def _event(name: str, event_id: str, *, category: str = "Fed Speech", time: str = "2099-07-20T14:00:00+00:00") -> EconomicEvent:
    dt = datetime.fromisoformat(time)
    return EconomicEvent(
        event_id=event_id,
        name=name,
        category=category,
        date=dt.date().isoformat(),
        time_utc=dt,
        impact=Impact.HIGH,
        source="Federal Reserve",
        source_url=f"https://example.test/{event_id}",
        reliability=0.9,
        event_risk_level=Impact.HIGH,
    )


class _Cache:
    def __init__(self) -> None:
        self.value = None

    def get(self, key):
        return self.value

    def set(self, key, value, ttl_seconds=None):
        self.value = value


class _Empty:
    pass


def test_macro_save_read_back_materializes_from_db(tmp_path) -> None:
    settings = Settings(_env_file=None, database_path=tmp_path / "market.sqlite")
    service = DiagnosticsService(
        settings,
        macro_service=_Empty(),
        event_service=_Empty(),
        event_window_service=_Empty(),
        nasdaq_data_service=_Empty(),
        enrichment_orchestrator=_Empty(),
    )
    macro = MacroLatestResponse(series=[_series("DGS10", "FRED")], provider_results=[_metadata("FRED")])

    written = service._save_macro(macro)
    read_back = service.facts.get_valid_facts_by_type("official_macro_latest")
    materialized = service._macro_from_facts(read_back)

    assert written == 1
    assert len(read_back) == 1
    assert materialized.series[0].series_id == "DGS10"
    assert materialized.provider_results[0].source == "FRED"


def test_readiness_false_when_blocking_reasons_present() -> None:
    consumer = build_ai_trader_market_context(
        {
            "symbol": "MNQ",
            "data_quality": {
                "critical_errors": [],
                "overall_data_quality": {
                    "blocking_reasons": ["macro_snapshot_incomplete"],
                    "critical_missing_count": 1,
                    "missing_critical_fields": ["DGS10"],
                },
            },
        }
    )

    assert consumer["readiness"]["ready"] is False
    assert consumer["readiness"]["critical_errors"] == 1
    assert consumer["data_quality"]["missing_critical_fields"] == ["DGS10"]


def test_event_dedup_keeps_different_speakers_and_detects_true_duplicate() -> None:
    duplicate = _event("Fed Speech - Governor Waller", "waller-copy", time="2099-07-20T14:00:00+00:00")
    duplicate.source_url = "https://example.test/waller"
    calendar = build_event_calendar(
        [
            _event("Fed Speech - Governor Waller", "waller", time="2099-07-20T14:00:00+00:00"),
            _event("Fed Speech - Governor Bowman", "bowman", time="2099-07-20T14:00:00+00:00"),
            duplicate,
        ]
    )
    rows = [row for values in calendar.values() for row in values]
    by_id = {row.event_id: row for row in rows}

    assert by_id["waller"].enrichment.summary["is_duplicate"] is False
    assert by_id["bowman"].enrichment.summary["is_duplicate"] is False
    assert by_id["waller-copy"].enrichment.summary["is_duplicate"] is True
    assert by_id["waller-copy"].enrichment.summary["duplicate_reason"] != "same_category_date_event_type"


def test_missing_qqq_weights_remain_null_in_derived_context() -> None:
    nasdaq = enrich_nasdaq_context(
        {
            "qqq_holdings": {"top_holdings": [{"symbol": "NVDA", "weight": None}, {"symbol": "MSFT", "weight": None}]},
            "mega_cap_snapshot": {"stocks": [{"symbol": "NVDA", "change_pct": 1.2}, {"symbol": "MSFT", "change_pct": -0.2}]},
            "mega_cap_breadth": {"positive_count": 1, "negative_count": 1, "average_change_pct": 0.5},
        },
        {"latest": []},
    )
    exposure = sector_exposure([{"symbol": "NVDA", "weight": None}], total_holdings_count=104)

    assert nasdaq["concentration"]["top_5_weight_pct"] is None
    assert nasdaq["concentration"]["classification"] == "UNKNOWN"
    assert nasdaq["driver_context"][0]["qqq_weight"] is None
    assert nasdaq["driver_context"][0]["weighted_contribution"] is None
    assert nasdaq["breadth_summary"]["calculation_method"] == "equal_weight_proxy"
    assert exposure["portfolio_weight_pct"] is None
    assert exposure["unknown_weight_pct"] is None


def test_hacker_news_generic_ai_is_insufficient_but_market_items_are_partial() -> None:
    started = datetime.now(UTC)
    generic = build_social_sentiment(
        [{"object_id": "1", "title": "OpenAI launches a new coding model", "url": "https://hn.test/1"}],
        started=started,
        source_url="https://hn.test",
        ttl_minutes=30,
    )
    relevant = build_social_sentiment(
        [{"object_id": "2", "title": "Nasdaq and QQQ rally as Nvidia earnings beat", "url": "https://hn.test/2"}],
        started=started,
        source_url="https://hn.test",
        ttl_minutes=30,
    )

    assert generic["status"] == "insufficient_data"
    assert generic["social_market_sentiment"]["neutral_ratio"] is None
    assert relevant["status"] == "partial"
    assert relevant["direct_symbol_mention_count"] >= 1


def test_news_context_filters_irrelevant_articles_and_classifies_sources() -> None:
    context = build_news_context(
        [
            {
                "title": "Retirement planning tips for savers",
                "source": "Yahoo Finance",
                "source_url": "https://finance.yahoo.com/personal",
                "retrieved_at": "2099-07-10T00:00:00Z",
                "valid_until": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                "relevance": "LOW",
            },
            {
                "title": "Nasdaq futures rise as Nvidia earnings lift chip stocks",
                "summary": "QQQ and mega-cap semiconductors moved higher.",
                "source": "Reuters",
                "source_url": "https://reuters.test/nasdaq",
                "published_at": datetime.now(UTC).isoformat(),
                "retrieved_at": datetime.now(UTC).isoformat(),
                "valid_until": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                "relevance": "HIGH",
                "symbols": ["QQQ", "NVDA"],
                "topics": ["mega-cap"],
            },
        ]
    )

    assert len(context["latest"]) == 1
    assert context["latest"][0]["source"] == "Reuters"
    assert context["latest"][0]["is_official_source"] is False
    assert classify_source("Yahoo Finance", "https://finance.yahoo.com")["is_official_source"] is False


def test_encoding_normalization_covers_quotes_ellipsis_and_payload_materialization() -> None:
    assert "Ã¢" not in normalize_text("ChipmakingÃ¢â‚¬â„¢s rebound")
    payload = normalize_payload_text({"title": "From Blastoff To Ã¢â‚¬Â¦ Boring?", "url": "https://example.test/%C3%A2"})
    assert payload["title"] == "From Blastoff To ... Boring?"
    assert payload["url"] == "https://example.test/%C3%A2"


def test_consumer_compacts_polymarket_ssl_error() -> None:
    consumer = build_ai_trader_market_context(
        {
            "symbol": "MNQ",
            "data_quality": {"overall_data_quality": {"blocking_reasons": []}},
            "sentiment_context": {
                "prediction_markets": {
                    "status": "ssl_error",
                    "errors": ["CERTIFICATE_VERIFY_FAILED hostname mismatch"] * 7,
                    "diagnostics": {"attempt_count": 7},
                }
            },
        }
    )

    prediction = consumer["sentiment_context"]["prediction_markets"]
    assert prediction["failure_type"] == "ssl_error"
    assert prediction["short_reason"] == "ssl_certificate_verification_failed"
    assert "CERTIFICATE_VERIFY_FAILED" not in str(prediction)


def test_disabled_scraper_provider_is_not_failed(tmp_path) -> None:
    settings = Settings(_env_file=None, database_path=tmp_path / "market.sqlite", enable_scraper_fallbacks=False)
    result: ProviderResult = asyncio.run(EconomicCalendarScraperProvider(_Cache(), settings).fetch_safe())

    assert result.data["status"] == "disabled"
    assert result.metadata.errors == []
