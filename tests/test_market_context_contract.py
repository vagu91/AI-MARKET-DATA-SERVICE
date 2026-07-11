from datetime import UTC, datetime

from app.models.common import Freshness, Impact, ProviderMetadata, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.models.macro import EventWindowsResponse, MacroLatestResponse, MacroSeries
from app.services.market_context_builder import (
    build_event_calendar,
    build_macro_snapshot,
    build_market_context_contract,
    build_news_context,
    materialize_nasdaq_context_from_facts,
)


def metadata(source: str) -> ProviderMetadata:
    return ProviderMetadata(
        source=source,
        provider_type=ProviderType.API,
        retrieved_at=datetime.now(UTC),
        freshness=Freshness.RECENT,
        reliability=0.93,
    )


def series(series_id: str, source: str, value: float = 1.0) -> MacroSeries:
    return MacroSeries(
        series_id=series_id,
        name=series_id,
        value=value,
        units="percent",
        data_as_of="2099-07-01",
        source=source,
        metadata=metadata(source),
    )


def event(category: str, name: str, event_id: str, impact=Impact.HIGH, date="2099-08-07") -> EconomicEvent:
    return EconomicEvent(
        event_id=event_id,
        name=name,
        country="US",
        category=category,
        date=date,
        time_utc=datetime.fromisoformat(f"{date}T12:30:00+00:00"),
        impact=impact,
        source="fixture",
        source_url="https://fixture.test",
        reliability=0.9,
        event_risk_level=impact,
        default_risk_window_before_minutes=30,
        default_risk_window_after_minutes=30,
        enrichment=EventEnrichment(
            forecast="0.3%",
            previous="0.2%",
            consensus=None,
            source="fixture",
            source_url="https://fixture.test/enrichment",
            provider_type=ProviderType.API,
            reliability=0.8,
            confidence=0.8,
        ),
    )


def test_macro_snapshot_materializes_fred_bls_bea_sections():
    macro = MacroLatestResponse(
        series=[
            series("FEDFUNDS", "FRED"),
            series("DGS10", "FRED"),
            series("CUSR0000SA0", "BLS"),
            series("WPUFD4", "BLS"),
            series("CES0000000001", "BLS"),
            series("LNS14000000", "BLS"),
            series("BEA:GDP", "BEA"),
            series("BEA:PCE", "BEA"),
        ],
        provider_results=[metadata("FRED"), metadata("BLS"), metadata("BEA")],
    )

    snapshot = build_macro_snapshot(macro)

    assert "CUSR0000SA0" in snapshot["inflation"]
    assert "WPUFD4" in snapshot["inflation"]
    assert "CES0000000001" in snapshot["labor"]
    assert "LNS14000000" in snapshot["labor"]
    assert {item["source"] for item in snapshot["provider_results"]} == {"FRED", "BLS", "BEA"}
    assert snapshot["labor"]["LNS14000000"]["metric"] == "unemployment_rate"


def test_event_calendar_excludes_medium_and_separates_fed_communications_and_invalid_nfp():
    events = [
        event("CPI", "Consumer Price Index", "cpi"),
        event("FOMC", "FOMC Press Conference", "fomc-pc"),
        event("Fed Speech", "Fed Speech - Chair", "speech"),
        event("Retail Sales", "Retail Sales", "retail", impact=Impact.MEDIUM),
        event("NFP / Nonfarm Payrolls", "Employment Situation (August 2099)", "bad-nfp", date="2099-08-04"),
        event("NFP / Nonfarm Payrolls", "Employment Situation (July 2099)", "good-nfp", date="2099-08-07"),
    ]

    calendar = build_event_calendar(events)

    assert {item.event_id for item in calendar["critical_macro_events"]} == {"cpi", "good-nfp"}
    assert all(item.impact == Impact.HIGH for item in calendar["critical_macro_events"])
    assert {item.event_id for item in calendar["fed_communications"]} == {"fomc-pc", "speech"}
    assert any(item.event_id == "retail" for item in calendar["other_economic_events"])
    assert any(
        item.event_id == "bad-nfp" and item.enrichment.summary["invalid_period_mapping"] is True
        for item in calendar["other_economic_events"]
    )


def test_nasdaq_context_materialized_from_db_facts():
    context = materialize_nasdaq_context_from_facts(
        {
            "qqq_holdings": [
                {
                    "raw_payload": {
                        "as_of": "2099-07-01",
                        "source": "Invesco",
                        "reliability": 0.9,
                        "holdings": [{"symbol": "NVDA", "weight": 10.0, "sector": "Technology"}],
                    }
                }
            ],
            "mega_cap_snapshot": [{"raw_payload": {"stocks": [{"symbol": "NVDA"}], "data_quality": {"tracked_count": 1, "resolved_count": 1}}}],
            "mega_cap_breadth": [{"raw_payload": {"positive_count": 1, "negative_count": 0}}],
            "earnings_event": [{"raw_payload": {"events": [{"symbol": "NVDA", "date": "2099-07-20"}]}}],
            "nasdaq_context": [],
        }
    )

    assert context is not None
    assert context["qqq_holdings"]["holdings_count"] == 1
    assert context["mega_cap_breadth"]["positive_count"] == 1
    assert context["earnings"]["upcoming"][0]["symbol"] == "NVDA"


def test_news_context_dedupes_and_groups_articles():
    news = build_news_context(
        [
            {
                "title": "Fed policy and Nasdaq story",
                "source": "MarketWatch",
                "source_url": "https://news.test/1",
                "published_at": datetime.now(UTC).isoformat(),
                "summary": "MarketWatch reports a Federal Reserve development affecting Nasdaq and QQQ.",
                "symbols": ["QQQ"],
                "topics": ["Fed", "mega-cap"],
                "relevance": "HIGH",
                "provider_type": "RSS",
                "reliability": 0.7,
            },
            {"title": "duplicate", "source_url": "https://news.test/1"},
        ]
    )

    assert len(news["latest"]) == 1
    assert news["by_topic"]["fed"][0]["source_url"] == "https://news.test/1"
    assert news["by_symbol"]["QQQ"][0]["title"] == "Fed policy and Nasdaq story"


def test_full_contract_has_quality_legacy_views_and_metric_based_enrichment():
    macro = MacroLatestResponse(
        series=[
            series("FEDFUNDS", "FRED"),
            series("CUSR0000SA0", "BLS"),
            series("WPUFD4", "BLS"),
            series("CES0000000001", "BLS"),
            series("LNS14000000", "BLS"),
            series("BEA:GDP", "BEA"),
            series("BEA:PCE", "BEA"),
        ],
        provider_results=[metadata("FRED"), metadata("BLS"), metadata("BEA")],
    )
    contract = build_market_context_contract(
        symbol="MNQ",
        macro=macro,
        events_today=[],
        upcoming_events=[event("CPI", "Consumer Price Index", "cpi")],
        event_windows=EventWindowsResponse(symbol="MNQ", checked_at_utc=datetime.now(UTC).isoformat()),
        nasdaq_context={"qqq_holdings": {"holdings_count": 1}, "mega_cap_breadth": {}, "earnings": {}},
        news_items=[{"title": "QQQ news", "summary": "Reuters reports a material development affecting QQQ and Nasdaq markets.", "source": "Reuters", "source_url": "https://news.test", "published_at": datetime.now(UTC).isoformat(), "symbols": ["QQQ"], "topics": ["macro"]}],
        data_quality={"missing_critical_fields": []},
        db_summary={"market_facts": {"total": 1}},
    )

    assert contract["macro_snapshot"]["inflation"]["CUSR0000SA0"]["unit"] == "percent"
    assert contract["event_calendar"]["critical_macro_events"][0]["enrichment"]["metrics"][0]["metric_id"] == "headline_cpi_mom"
    assert contract["latest_news"]["articles"]
    assert "overall_data_quality" in contract["data_quality"]
    assert contract["metadata"]["trading_logic"] == "not implemented; data service only"


def test_extended_contract_adds_data_only_context_blocks_and_event_windows():
    cpi = event("CPI", "Consumer Price Index", "cpi")
    cpi.enrichment.metrics = [
        {
            "metric_id": "headline_cpi_mom",
            "frequency": "MoM",
            "unit": "percent",
            "previous": 0.2,
            "forecast": 0.3,
            "consensus": 0.3,
            "actual": None,
            "source_forecast": "structured_provider",
            "source_consensus": "verified_calendar_consensus",
            "forecast_source_url": "https://forecast.test",
            "consensus_source_url": "https://consensus.test",
            "field_semantics": {
                "forecast_is_consensus": False,
                "forecast_origin": "source_forecast",
                "consensus_verified": True,
                "actual_is_official": False,
                "period_match": True,
            },
        }
    ]
    window = EventWindowsResponse(symbol="MNQ", checked_at_utc=datetime.now(UTC).isoformat())
    contract = build_market_context_contract(
        symbol="MNQ",
        macro=MacroLatestResponse(series=[series("VIXCLS", "FRED")], provider_results=[metadata("FRED")]),
        events_today=[cpi],
        upcoming_events=[cpi],
        event_windows=window,
        nasdaq_context={
            "qqq_holdings": {"holdings_count": 2, "top_holdings": [{"symbol": "NVDA", "weight": 8.0}, {"symbol": "MSFT", "weight": 7.0}]},
            "mega_cap_snapshot": {"tracked_count": 2, "resolved_count": 2, "stocks": [{"symbol": "NVDA", "change_pct": 1.0}, {"symbol": "MSFT", "change_pct": -0.2}]},
            "mega_cap_breadth": {"positive_count": 1, "negative_count": 1, "neutral_count": 0, "weighted_average_change_pct": 0.1},
            "earnings": {"upcoming": [{"symbol": "NVDA", "date": "2099-08-01"}], "data_quality": {"final_data_available": True}},
            "sector_exposure": {"unknown_weight_pct": 0.0, "classified_weight_pct": 15.0},
        },
        news_items=[{"title": "Nvidia chip story", "summary": "Reuters reports a material semiconductor development affecting Nvidia.", "source": "Reuters", "source_url": "https://reuters.test/1", "published_at": datetime.now(UTC).isoformat(), "symbols": ["NVDA"], "topics": ["semiconductors"], "relevance": "HIGH", "reliability": 0.8}],
        data_quality={"missing_critical_fields": []},
        db_summary={"market_facts": {"total": 1}},
    )

    metric = contract["event_calendar"]["critical_macro_events"][0]["enrichment"]["metrics"][0]
    assert metric["forecast"] == metric["consensus"]
    assert metric["field_semantics"]["forecast_is_consensus"] is False
    assert metric["field_semantics"]["consensus_verified"] is True
    assert "active" in contract["event_windows"]
    assert "upcoming" in contract["event_windows"]
    assert contract["positioning"]["cot"]["nasdaq_100"]["source"] == "CFTC"
    assert contract["sentiment_context"]["aaii"]["source"] == "AAII"
    assert contract["risk_sentiment"]["vix"]["series_id"] == "VIXCLS"
    assert contract["news_digest"]["drivers"][0]["source_urls"]
    assert contract["nasdaq_context"]["concentration"]["top_5_weight_pct"] == 15.0
    assert contract["nasdaq_context"]["semiconductor_context"]["resolved_count"] == 1


def test_extended_contract_has_no_operational_trading_fields():
    contract = build_market_context_contract(
        symbol="MNQ",
        macro=MacroLatestResponse(series=[series("VIXCLS", "FRED")], provider_results=[metadata("FRED")]),
        events_today=[],
        upcoming_events=[],
        event_windows=EventWindowsResponse(symbol="MNQ", checked_at_utc=datetime.now(UTC).isoformat()),
        nasdaq_context={"qqq_holdings": {"holdings_count": 0}, "mega_cap_breadth": {}, "earnings": {}},
        news_items=[],
        data_quality={"missing_critical_fields": []},
        db_summary={},
    )

    text = str(contract).lower()
    for forbidden in ("blocks_trading", "trading_allowed", "no_trade", "candidate_level", "'entry':", "'stop':", "'target':"):
        assert forbidden not in text
