from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from app.api.routes import router
from app.core.config import Settings
from app.providers import aaii_sentiment_provider as aaii_module
from app.providers.aaii_sentiment_provider import AaiiSentimentProvider, is_aaii_blocked_html, parse_aaii_sentiment
from app.providers.investing_economic_calendar_provider import _normalize as normalize_investing
from app.providers.macromicro_aaii_crosscheck_provider import parse_macromicro_aaii
from app.providers.nasdaq_qqq_option_chain_provider import normalize_option_rows, option_chain_aggregates
from app.providers.polymarket_prediction_provider import group_polymarket_events, normalize_polymarket_market
from app.services.acquisition_status_service import _diagnostic_rejection_reasons
from app.services.economic_value_parser import parse_economic_value
from app.services.multi_source_runtime_service import MultiSourceRuntimeService, _exclusion_reasons, build_multi_source_context_blocks
from app.services.positioning_runtime_service import PositioningRuntimeService


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def settings(tmp_path) -> Settings:
    return Settings(_env_file=None, database_path=tmp_path / "market.sqlite")


def test_economic_value_parser_handles_units_missing_and_parentheses():
    assert parse_economic_value("--")["parse_status"] == "missing"
    assert parse_economic_value("—")["parse_status"] == "missing"
    assert parse_economic_value("(1.2%)")["value"] == -1.2
    assert parse_economic_value("2.5M", default_unit="USD")["value"] == 2_500_000


def test_investing_consensus_mapping_and_future_actual_rejection():
    event = {"event_id": "101", "event_translated": "CPI", "country_id": "5", "currency": "USD", "importance": 3}
    future = {"event_id": "101", "occurrence_id": "abc", "occurrence_time": "2099-07-10T12:30:00Z", "actual": "0.4%", "forecast": "0.3%", "previous": "0.2%"}
    released = {**future, "occurrence_time": "2026-07-01T12:30:00Z", "actual": "0.4%"}

    rejected = normalize_investing(event, future, now=datetime(2026, 7, 10, tzinfo=UTC))
    item = normalize_investing(event, released, now=datetime(2026, 7, 10, tzinfo=UTC))

    assert rejected["status"] == "REJECTED_TEMPORAL"
    assert item["consensus"] == 0.3
    assert item["consensus_verified"] is True
    assert item["consensus_origin"] == "investing_economic_calendar"
    assert item["actual_is_official"] is False


def test_option_rows_inherit_expiry_parse_nulls_and_build_descriptive_aggregates():
    rows = [
        {"expirygroup": "July 10, 2026", "strike": None},
        {"expirygroup": "", "expiryDate": "Jul 10", "strike": "700.00", "c_Openinterest": "10", "p_Openinterest": "--", "c_Volume": "2", "p_Volume": "3"},
        {"expirygroup": "", "expiryDate": "Jul 10", "strike": "705.00", "c_Openinterest": "5", "p_Openinterest": "15", "c_Volume": "--", "p_Volume": "9"},
    ]

    contracts, warnings = normalize_option_rows(rows, retrieved_at="2026-07-10T10:00:00Z")
    aggregates = option_chain_aggregates(contracts)

    assert warnings == []
    assert contracts[0]["expiration_date"] == "2026-07-10"
    assert contracts[0]["put_open_interest"] is None
    assert aggregates["observed_aggregates"]["observed_call_open_interest"] == 15
    assert aggregates["observed_aggregates"]["observed_put_open_interest"] == 15
    assert "total_call_open_interest" not in aggregates["observed_aggregates"]
    assert "dealer_gex" not in str(aggregates).lower()


def test_option_partial_snapshot_exposes_observed_not_global_totals():
    contracts = [
        {"expiration_date": "2026-07-10", "strike": 700.0, "call_open_interest": 10, "put_open_interest": 20, "call_volume": 3, "put_volume": 4},
        {"expiration_date": "2026-07-10", "strike": 705.0, "call_open_interest": 15, "put_open_interest": 5, "call_volume": 2, "put_volume": 1},
    ]

    aggregates = option_chain_aggregates(
        contracts,
        total_records=2359,
        incomplete=True,
        requested_expirations=["2026-07-10"],
        requested_scope_complete=False,
        full_chain_complete=False,
        max_pages_reached=True,
        partial_reason="max_pages_reached",
    )

    scope = aggregates["observed_aggregates"]["scope"]
    assert scope["computed_from_partial_snapshot"] is True
    assert scope["coverage_contract_pct"] == round(2 / 2359 * 100, 4)
    assert scope["covered_expirations"] == ["2026-07-10"]
    assert scope["full_chain_complete"] is False
    assert aggregates["global_aggregates"] is None
    assert aggregates["observed_aggregates"]["observed_call_open_interest"] == 25
    assert "pct_total_open_interest" not in str(aggregates)


def test_option_complete_requested_scope_can_publish_global_aggregates():
    contracts = [{"expiration_date": "2026-07-10", "strike": 700.0, "call_open_interest": 10, "put_open_interest": 20, "call_volume": 3, "put_volume": 4}]

    aggregates = option_chain_aggregates(
        contracts,
        total_records=1,
        incomplete=False,
        requested_expirations=["2026-07-10"],
        requested_scope_complete=True,
        full_chain_complete=True,
    )

    assert aggregates["observed_aggregates"]["scope"]["computed_from_partial_snapshot"] is False
    assert aggregates["global_aggregates"]["call_open_interest"] == 10


def test_aaii_parser_reads_inline_chart_data():
    html = """
    <script>
    window.dataChart5 = [
      {"date_":"2026-07-01","bullish":35.0,"neutral":30.0,"bearish":35.0,"spread":0.0},
      {"date_":"2026-07-08","bullish":36.3,"neutral":26.5,"bearish":37.2,"spread":-0.9}
    ];
    </script>
    """

    parsed = parse_aaii_sentiment(html)

    assert parsed["survey_date"] == "2026-07-08"
    assert parsed["bullish_pct"] == 36.3
    assert len(parsed["latest_four_weeks"]) == 2


def test_aaii_parser_reads_public_results_dom_and_historical_averages():
    html = """
    <section class="results">
      <div class="weekending"><span>July 8, 2026</span><div class="bar bullish">Bullish 36.3%</div><div class="bar neutral">Neutral 26.5%</div><div class="bar bearish">Bearish 37.2%</div></div>
      <div class="weekending"><span>July 1, 2026</span><div>Bullish 35.0%</div><div>Neutral 30.0%</div><div>Bearish 35.0%</div></div>
      <div class="weekending"><span>June 24, 2026</span><div>Bullish 34.0%</div><div>Neutral 29.0%</div><div>Bearish 37.0%</div></div>
      <div class="weekending"><span>June 17, 2026</span><div>Bullish 33.0%</div><div>Neutral 31.0%</div><div>Bearish 36.0%</div></div>
    </section>
    <section>Historical averages Bullish 37.5% Neutral 31.5% Bearish 31.0%</section>
    """

    parsed = parse_aaii_sentiment(html)

    assert parsed["survey_date"] == "2026-07-08"
    assert parsed["bullish_pct"] + parsed["neutral_pct"] + parsed["bearish_pct"] == 100.0
    assert len(parsed["latest_four_weeks"]) == 4
    assert parsed["historical_averages"]["bullish"] == 37.5


async def test_aaii_incapsula_http_uses_browser_fallback_and_closes(monkeypatch, tmp_path):
    blocked_html = "<html>Request unsuccessful. Incapsula incident id 123</html>"
    browser_html = """
    <section class="results">
      <div class="weekending"><span>July 8, 2026</span><div>Bullish 36.3%</div><div>Neutral 26.5%</div><div>Bearish 37.2%</div></div>
    </section>
    """

    class FakeResponse:
        status_code = 200
        text = blocked_html

        def raise_for_status(self):
            return None

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, *args, **kwargs):
            return FakeResponse()

    async def fake_browser(url, *, timeout_seconds):
        return browser_html, {
            "browser_attempted": True,
            "browser_success": True,
            "browser_error": None,
            "browser_closed": True,
            "selector_found": True,
        }

    monkeypatch.setattr(aaii_module.httpx, "AsyncClient", lambda timeout: FakeClient())
    monkeypatch.setattr(aaii_module, "fetch_aaii_with_browser", fake_browser)

    result = await AaiiSentimentProvider(settings(tmp_path)).fetch()

    assert is_aaii_blocked_html(blocked_html) is True
    assert result["status"] == "found"
    assert result["diagnostics"]["http_blocked"] is True
    assert result["diagnostics"]["browser_attempted"] is True
    assert result["diagnostics"]["browser_success"] is True
    assert result["diagnostics"]["browser_closed"] is True


async def test_aaii_browser_fallback_failure_is_motivated(monkeypatch, tmp_path):
    blocked_html = "<html>Request unsuccessful. Incapsula incident id 123</html>"

    class FakeResponse:
        status_code = 200
        text = blocked_html

        def raise_for_status(self):
            return None

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, *args, **kwargs):
            return FakeResponse()

    async def fake_browser(url, *, timeout_seconds):
        return "<html><body>challenge still active</body></html>", {
            "browser_attempted": True,
            "browser_success": True,
            "browser_error": None,
            "browser_closed": True,
            "selector_found": False,
        }

    monkeypatch.setattr(aaii_module.httpx, "AsyncClient", lambda timeout: FakeClient())
    monkeypatch.setattr(aaii_module, "fetch_aaii_with_browser", fake_browser)

    result = await AaiiSentimentProvider(settings(tmp_path)).fetch()

    assert result["status"] == "access_restricted"
    assert result["diagnostics"]["browser_attempted"] is True
    assert result["diagnostics"]["browser_closed"] is True
    assert result["diagnostics"]["browser_error"] == "browser_page_loaded_but_sentiment_selectors_not_found"


async def test_aaii_refresh_false_does_not_attempt_network_or_browser(tmp_path):
    service = PositioningRuntimeService(settings(tmp_path))

    async def fail_fetch():
        raise AssertionError("network should not be called")

    service.aaii_provider.fetch = fail_fetch
    result = await service.aaii(refresh="false")

    assert result["status"] == "not_found"
    assert result["attempted_sources"] == []


def test_macromicro_parser_extracts_valid_crosscheck():
    payload = {"data": [{"date_": "2026-07-08", "bullish": "36.3", "neutral": "26.5", "bearish": "37.2"}]}

    parsed = parse_macromicro_aaii(payload)

    assert parsed["survey_date"] == "2026-07-08"
    assert parsed["sum_pct"] == 100.0


def test_polymarket_relevance_and_probability_validation():
    event = {"id": "evt-fed", "title": "How many Fed rate cuts in 2026?", "description": "Resolved from FOMC outcomes.", "endDate": "2099-12-31T00:00:00Z"}
    relevant_market = {
        "id": "m1",
        "question": "Will the Federal Reserve cut rates twice in 2026?",
        "slug": "fed-cuts-2026",
        "outcomes": '["Yes","No"]',
        "outcomePrices": '["0.45","0.55"]',
        "volumeNum": 50000,
        "liquidityNum": 20000,
        "endDate": "2099-12-31T00:00:00Z",
        "active": True,
        "closed": False,
        "description": "Resolves based on Federal Reserve target-rate decisions.",
    }
    irrelevant_market = {
        **relevant_market,
        "id": "m2",
        "question": "Will a pop album release before a video game?",
        "description": "Entertainment resolution rules.",
    }

    accepted, accepted_reason = normalize_polymarket_market(relevant_market, event)
    rejected, rejected_reason = normalize_polymarket_market(irrelevant_market, {"title": "Entertainment market"})

    assert accepted_reason is None
    assert accepted["category"] in {"FED", "INTEREST_RATES"}
    assert accepted["probability_label"] == "market_implied"
    assert rejected is None
    assert rejected_reason == "irrelevant"


def test_polymarket_rejects_observed_false_positives_and_rules_only_matches():
    base_market = {
        "id": "m",
        "outcomes": '["Yes","No"]',
        "outcomePrices": '["0.45","0.55"]',
        "volumeNum": 50000,
        "liquidityNum": 20000,
        "endDate": "2099-12-31T00:00:00Z",
        "active": True,
        "closed": False,
        "description": "Rules mention Nvidia, Microsoft, Nasdaq, and the Fed only as examples.",
    }
    cases = [
        ("Will Elon Musk win the Nobel Peace Prize?", {"title": "Nobel Peace Prize winner"}),
        ("Will there be a Ukraine peace deal in 2026?", {"title": "Ukraine peace deal"}),
        ("Will Xi Jinping be removed from office before 2027?", {"title": "Xi Jinping removal"}),
        ("Will a singer win an award?", {"title": "Entertainment"}),
    ]

    for question, event in cases:
        market, reason = normalize_polymarket_market({**base_market, "question": question}, {"id": "event", "endDate": "2099-12-31T00:00:00Z", **event})
        assert market is None
        assert reason in {"irrelevant", "rules_only"}


def test_polymarket_accepts_direct_mega_cap_security_market():
    market = {
        "id": "m-nvda",
        "question": "Will Nvidia market cap close above $5T in 2026?",
        "slug": "nvidia-market-cap-5t-2026",
        "outcomes": '["Yes","No"]',
        "outcomePrices": '["0.40","0.60"]',
        "volumeNum": 50000,
        "liquidityNum": 20000,
        "endDate": "2099-12-31T00:00:00Z",
        "active": True,
        "closed": False,
        "description": "Resolves from Nvidia market capitalization.",
    }

    accepted, reason = normalize_polymarket_market(market, {"id": "event-nvda", "title": "Nvidia market cap", "endDate": "2099-12-31T00:00:00Z"})

    assert reason is None
    assert accepted["category"] == "MEGA_CAP"
    assert accepted["directness"] == "DIRECT"
    assert accepted["matched_entities"] == ["NVDA"]
    assert accepted["matched_category_evidence"] == "question_title_slug"


def test_polymarket_groups_by_event_before_limiting_and_does_not_sum_event_liquidity():
    markets = [
        {"market_id": "m1", "event_id": "fed", "title": "How many Fed rate cuts in 2026?", "question": "One cut?", "category": "FED", "volume": 100.0, "liquidity": 1000.0, "end_date": "2099-12-31T00:00:00Z", "relevance_score": 0.9, "directness": "DIRECT", "relevance_reason": "Fed subject.", "matched_entities": [], "outcomes": ["Yes", "No"], "implied_probabilities": [0.4, 0.6]},
        {"market_id": "m2", "event_id": "fed", "title": "How many Fed rate cuts in 2026?", "question": "Two cuts?", "category": "FED", "volume": 90.0, "liquidity": 2000.0, "end_date": "2099-12-31T00:00:00Z", "relevance_score": 0.9, "directness": "DIRECT", "relevance_reason": "Fed subject.", "matched_entities": [], "outcomes": ["Yes", "No"], "implied_probabilities": [0.5, 0.5]},
        {"market_id": "m3", "event_id": "cpi", "title": "Will CPI exceed 4% in 2026?", "question": "CPI above 4%?", "category": "CPI_INFLATION", "volume": 80.0, "liquidity": 500.0, "end_date": "2099-12-31T00:00:00Z", "relevance_score": 0.9, "directness": "DIRECT", "relevance_reason": "CPI subject.", "matched_entities": [], "outcomes": ["Yes", "No"], "implied_probabilities": [0.3, 0.7]},
    ]

    events = group_polymarket_events(markets, max_events=2)

    assert len(events) == 2
    fed = next(event for event in events if event["event_id"] == "fed")
    assert fed["market_count"] == 2
    assert fed["total_event_volume"] == 190.0
    assert fed["event_liquidity"] == 2000.0
    assert fed["value_scope"]["liquidity"] == "event_level_or_max_market_level_not_summed"


async def test_multi_source_refresh_false_does_not_call_provider(tmp_path):
    cfg = settings(tmp_path)
    service = MultiSourceRuntimeService(cfg)

    async def fail_fetch():
        raise AssertionError("network should not be called")

    service.cboe.fetch = fail_fetch
    result = await service.provider("cboe_risk_indices", refresh="false")

    assert result["status"] == "not_found"
    assert result["provider_calls"] == 0
    assert result["AI_called"] is False


def test_multi_source_context_contract_is_explicit_for_events_options_and_risk():
    blocks = build_multi_source_context_blocks(
        {
            "investing_economic_calendar": {"status": "found", "items": [{"event_name": "CPI", "consensus": 0.2, "previous": 0.1}]},
            "nasdaq_qqq_options": {
                "status": "partial",
                "snapshot": {"incomplete": True},
                "open_interest_matrix": {"by_strike": []},
                "observed_aggregates": {"observed_call_open_interest": 10},
                "global_aggregates": None,
                "diagnostics": {"computed_from_partial_snapshot": True},
            },
            "cboe_risk_indices": {"status": "found", "source": "CBOE", "indices": {"vvix": {"current_price": 90.0}, "skew": {"current_price": 150.0}}},
            "polymarket_prediction_markets": {"status": "found", "events": [{"event_id": "fed"}], "markets": [{"market_id": "m"}]},
        }
    )

    assert blocks["economic_calendar_enrichment"]["investing"]["events"][0]["event_name"] == "CPI"
    qqq_options = blocks["nasdaq_context_additions"]["qqq_options"]
    assert qqq_options["status"] == "partial"
    assert qqq_options["observed_aggregates"]["observed_call_open_interest"] == 10
    assert qqq_options["global_aggregates"] is None
    assert "aggregates" not in qqq_options
    assert blocks["risk_context"]["vvix"]["status"] == "found"
    assert blocks["risk_context"]["vvix"]["value"] == 90.0
    assert blocks["sentiment"]["prediction_markets"]["events"][0]["event_id"] == "fed"


def test_rejection_reason_summaries_ignore_sample_lists():
    diagnostics = {"rejected_irrelevant": 2, "rejected_rules_only": 1, "rejected_samples": [{"question": "sample"}]}

    assert _exclusion_reasons({"diagnostics": diagnostics}) == {"irrelevant": 2, "rules_only": 1}
    assert _diagnostic_rejection_reasons(diagnostics) == {"irrelevant": 2, "rules_only": 1}


def test_new_provider_routes_registered():
    paths = {route.path for route in router.routes}

    for path in {
        "/providers/investing/economic-calendar",
        "/providers/investing/holidays",
        "/providers/cboe/risk-indices",
        "/providers/nasdaq/earnings-calendar",
        "/providers/nasdaq/nasdaq-100",
        "/providers/nasdaq/market-info",
        "/providers/nasdaq/qqq-options",
        "/providers/sentiment/aaii",
        "/providers/sentiment/macromicro-aaii",
        "/providers/polymarket/markets",
        "/diagnostics/data-quality",
    }:
        assert path in paths


def test_new_modules_do_not_introduce_disallowed_action_phrases():
    checked = [
        PROJECT_ROOT / "app" / "providers" / "polymarket_prediction_provider.py",
        PROJECT_ROOT / "app" / "providers" / "nasdaq_qqq_option_chain_provider.py",
        PROJECT_ROOT / "app" / "services" / "multi_source_runtime_service.py",
    ]
    forbidden = ("place order", "cancel order", "wallet signing", "private key", "relayer api key", "trading signal", "gamma wall", "gamma flip", "call wall", "put wall")
    text = "\n".join(path.read_text(encoding="utf-8").lower() for path in checked)

    assert not any(term in text for term in forbidden)
