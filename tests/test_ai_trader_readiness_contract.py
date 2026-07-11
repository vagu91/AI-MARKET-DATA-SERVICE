from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from app.core.config import Settings
from app.core.text_normalization import normalize_text
from app.models.common import Impact
from app.models.events import EconomicEvent, EventEnrichment
from app.providers.hacker_news_social_sentiment_provider import HackerNewsSocialSentimentProvider
from app.providers.investing_fed_rate_monitor_provider import parse_investing_fed_rate_monitor_html
from app.providers.nasdaq_100_constituents_provider import _normalize
from app.services.ai_trader_contract_service import build_ai_trader_market_context
from app.services.data_integrity_service import sector_exposure
from app.services.diagnostics_service import DiagnosticsService, _event_enrichment_metadata
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.enrichment_run_repository import EnrichmentRunRepository


def test_text_normalization_repairs_mojibake_and_preserves_clean_text() -> None:
    assert normalize_text("WashingtonÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢s Birthday") in {"Washington's Birthday", "Washington’s Birthday"}
    assert normalize_text("OpenAIÃ¢â‚¬â„¢s update") == "OpenAI's update"
    assert normalize_text("AT&amp;T") == "AT&T"
    assert normalize_text("NVDA QQQ https://example.com") == "NVDA QQQ https://example.com"


def test_nasdaq_100_sign_consistency_from_delta_indicator() -> None:
    down = _normalize({"symbol": "AAPL", "netChange": "0.2441", "percentageChange": "-0.08%", "deltaIndicator": "down"})
    up = _normalize({"symbol": "MSFT", "netChange": "+1.2", "percentageChange": "0.5%", "deltaIndicator": "up"})
    flat = _normalize({"symbol": "QQQ", "netChange": "0", "percentageChange": "0%", "deltaIndicator": "neutral"})
    conflict = _normalize({"symbol": "INTC", "netChange": "-2.55", "percentageChange": "2.27%", "deltaIndicator": "up"})

    assert down["net_change"] < 0
    assert down["sign_diagnostics"]["sign_normalized"] is True
    assert up["net_change"] > 0
    assert flat["net_change"] == 0
    assert conflict["net_change"] is None
    assert "nasdaq_change_sign_conflict" in conflict["warnings"]


def test_sector_exposure_reports_partial_top_holdings_coverage() -> None:
    exposure = sector_exposure(
        [
            {"symbol": "NVDA", "weight": 10.0, "sector": "Information Technology"},
            {"symbol": "MSFT", "weight": 8.0, "sector": "Information Technology"},
            {"symbol": "AMZN", "weight": 7.0, "sector": "Consumer Discretionary"},
            {"symbol": "AAPL", "weight": 20.04, "sector": "Information Technology"},
        ],
        total_holdings_count=104,
        coverage_scope="top_10_holdings",
    )
    assert exposure["covered_holdings_weight_pct"] == 45.04
    assert exposure["uncovered_holdings_weight_pct"] == 54.96
    assert exposure["portfolio_weight_pct"] == 100.0
    assert exposure["complete_portfolio_coverage"] is False
    assert exposure["unknown_weight_pct"] == 54.96


def test_fed_rate_monitor_timestamps_are_iso_timezone_aware() -> None:
    html = """
    <div class="cardWrapper">
      <div class="fedRateDate">Jul 29, 2026</div>
      <span>Meeting Time:</span><i>Jul 29, 2026 02:00PM ET</i>
      <span>Future Price:</span><i>95.50</i>
      <div class="fedUpdate">Updated: Jul 10, 2026 02:45PM EDT</div>
      <tr><td eventId="1" calcKey="a">4.25 - 4.50%</td><td>85%</td><td>80%</td><td>75%</td></tr>
    </div>
    """
    meeting = parse_investing_fed_rate_monitor_html(html)["meetings"][0]
    assert meeting["meeting_date"] == "2026-07-29"
    assert meeting["meeting_at"] == "2026-07-29T14:00:00-04:00"
    assert meeting["meeting_time_local_text"] == "Jul 29, 2026 02:00PM ET"
    assert meeting["updated_at"] == "2026-07-10T14:45:00-04:00"
    assert meeting["updated_at_text"] == "Jul 10, 2026 02:45PM EDT"


@pytest.mark.asyncio
async def test_hacker_news_social_sentiment_provider_mock(tmp_path: Path) -> None:
    cfg = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        hacker_news_algolia_url="https://hn.test/search",
    )
    with respx.mock(assert_all_called=True) as router:
        router.get("https://hn.test/search").mock(
            return_value=httpx.Response(
                200,
                json={
                    "hits": [
                        {"objectID": "1", "title": "Nvidia AI growth remains strong", "url": "https://news.test/1", "author": "a", "points": 10, "num_comments": 5},
                        {"objectID": "2", "title": "Fed inflation risk worries markets", "url": "https://news.test/2", "author": "b", "points": 3, "num_comments": 2},
                    ]
                },
            )
        )
        result = await HackerNewsSocialSentimentProvider(cfg).fetch()
    assert result["status"] == "found"
    assert result["source_count"] == 1
    assert result["mention_count"] >= 2
    assert result["social_market_sentiment"]["discussion_volume"] == 2


def test_consumer_contract_is_compact_and_excludes_debug_blocks() -> None:
    full = {
        "symbol": "MNQ",
        "generated_at_utc": "2099-01-01T00:00:00Z",
        "macro_snapshot": {"rates_and_yields": {}},
        "event_calendar": {"critical_macro_events": [], "fed_communications": [], "other_economic_events": []},
        "nasdaq_context": {"qqq_options": {"open_interest_matrix": {"by_strike": [{"strike": 500, "total_open_interest": 10}]}}},
        "news_context": {"latest": [{"title": "Clean"}]},
        "data_quality": {"overall_data_quality": {"is_ready_for_market_analysis": True}},
        "metadata": {"raw_provider_attempts": ["debug"]},
    }
    consumer = build_ai_trader_market_context(full)
    encoded = json.dumps(consumer)
    assert consumer["contract"] == "ai_trader_market_context"
    assert "raw_provider_attempts" not in encoded
    assert "by_strike" not in encoded
    assert len(encoded.encode("utf-8")) < 400_000


def test_consumer_aggregates_optional_enrichment_warnings_and_keeps_debug_details() -> None:
    full = {
        "symbol": "MNQ",
        "generated_at_utc": "2099-01-01T00:00:00Z",
        "data_quality": {
            "overall_data_quality": {"freshness_score": 0.9, "reliability_score": 0.8},
            "warnings": ["optional_event_enrichment_timeout_after_1s"],
        },
        "event_calendar": {
            "critical_macro_events": [
                {
                    "event_id": "cpi",
                    "impact": "HIGH",
                    "time_utc": "2099-01-02T12:30:00Z",
                    "enrichment": {"warnings": ["optional_enrichment_timeout"], "provider": "slow"},
                },
                {
                    "event_id": "nfp",
                    "impact": "HIGH",
                    "time_utc": "2099-01-03T12:30:00Z",
                    "enrichment": {"warnings": ["optional_enrichment_timeout"]},
                },
            ],
            "fed_communications": [],
            "other_economic_events": [],
        },
        "news_context": {"latest": [{"title": "Clean"}]},
    }

    consumer = build_ai_trader_market_context(full)

    assert consumer["warnings"] == [{"code": "optional_event_enrichment_partial", "count": 3, "blocking": False}]
    assert all(
        "optional_enrichment_timeout" not in ((event.get("enrichment") or {}).get("warnings") or [])
        for event in consumer["event_calendar"]["critical_macro_events"]
    )
    assert full["event_calendar"]["critical_macro_events"][0]["enrichment"]["warnings"] == ["optional_enrichment_timeout"]


def test_snapshot_summary_is_present_and_data_only() -> None:
    full = {
        "symbol": "MNQ",
        "generated_at_utc": "2099-01-01T00:00:00Z",
        "data_quality": {
            "overall_data_quality": {"freshness_score": 0.91, "reliability_score": 0.88},
            "critical_errors": [],
        },
        "event_calendar": {
            "critical_macro_events": [{"category": "CPI", "impact": "HIGH", "time_utc": "2099-01-02T12:30:00Z"}],
            "fed_communications": [{"category": "FOMC", "impact": "HIGH", "time_utc": "2099-01-29T19:00:00Z"}],
            "other_economic_events": [],
        },
        "nasdaq_context": {"earnings": {"events": [{"symbol": "NVDA"}]}},
        "news_context": {"latest": [{"title": "A"}, {"title": "B"}]},
        "social_sentiment": {"status": "found"},
        "risk_context": {"vvix": {"status": "found"}},
        "market_schedule": {"nasdaq_cash_session": {"status": "closed"}},
    }

    summary = build_ai_trader_market_context(full)["snapshot_summary"]

    assert summary["symbol"] == "MNQ"
    assert summary["ready"] is True
    assert summary["critical_errors"] == 0
    assert summary["critical_event_count"] == 1
    assert summary["high_impact_event_count_next_7d"] == 2
    assert summary["next_critical_event_at"] == "2099-01-02T12:30:00Z"
    assert summary["next_fomc_meeting_at"] == "2099-01-29T19:00:00Z"
    assert summary["next_earnings_count_14d"] == 1
    assert summary["news_article_count"] == 2
    assert "buy" not in json.dumps(summary).lower()


@pytest.mark.asyncio
async def test_event_enrichment_deadline_skips_ai_without_claiming_timeout(tmp_path: Path) -> None:
    cfg = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        timeout_events_seconds=1,
        enable_ai_researcher=True,
    )

    class Macro:
        async def latest(self):
            from app.models.macro import MacroLatestResponse

            return MacroLatestResponse()

    class Events:
        async def list_events(self, country, start, end, enrich=False):
            return [_event()]

    class Windows:
        async def event_windows(self, symbol):
            return {}

    class Nasdaq:
        async def context(self, *args, **kwargs):
            return {}

    class SlowEnrichment:
        async def enrich_events(self, **kwargs):
            await asyncio.sleep(2)
            return kwargs["events"], {}

    orchestrator = EnrichmentOrchestrator(cfg, event_enrichment_service=SlowEnrichment())
    service = DiagnosticsService(
        cfg,
        macro_service=Macro(),
        event_service=Events(),
        event_window_service=Windows(),
        nasdaq_data_service=Nasdaq(),
        enrichment_orchestrator=orchestrator,
    )
    model = await service.full_model(refresh="force", fetch_missing_nasdaq=False)
    quality = model["data_quality"]
    assert "event_enrichment_timeout" not in quality.get("missing_critical_fields", [])
    assert quality["enrichment_timeout"] is False
    enrichment = model["metadata"]["event_enrichment"]
    assert enrichment["status"] == "not_required"
    assert enrichment["AI_called"] is False
    assert enrichment["attempted_event_count"] == 0
    assert enrichment["timeout_event_count"] == 0
    assert all(row["attempted"] is False and row["timeout"] is False for row in enrichment["events"])
    assert model["event_calendar"]["critical_macro_events"]
    latest_run = EnrichmentRunRepository(cfg).latest()
    assert latest_run["finished_at"] is not None
    assert latest_run["status"] in {"failed", "completed"}


@pytest.mark.asyncio
async def test_event_enrichment_deadline_marks_started_ai_as_cancelled(tmp_path: Path) -> None:
    cfg = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        timeout_events_seconds=1,
        enable_ai_researcher=True,
    )

    class Macro:
        async def latest(self):
            from app.models.macro import MacroLatestResponse

            return MacroLatestResponse()

    class Events:
        async def list_events(self, country, start, end, enrich=False):
            return [_event()]

    class Windows:
        async def event_windows(self, symbol):
            return {}

    class Nasdaq:
        async def context(self, *args, **kwargs):
            return {}

    class EmptyEnrichment:
        async def enrich_events(self, **kwargs):
            return kwargs["events"], {}

    class SlowAI:
        async def research_and_save(self, events):
            await asyncio.sleep(2)
            return [], {"status": "success"}

    orchestrator = EnrichmentOrchestrator(
        cfg,
        event_enrichment_service=EmptyEnrichment(),
        ai_researcher_service=SlowAI(),
    )
    service = DiagnosticsService(
        cfg,
        macro_service=Macro(),
        event_service=Events(),
        event_window_service=Windows(),
        nasdaq_data_service=Nasdaq(),
        enrichment_orchestrator=orchestrator,
    )

    model = await service.full_model(refresh="force", fetch_missing_nasdaq=False)
    enrichment = model["metadata"]["event_enrichment"]
    row = enrichment["events"][0]

    assert enrichment["status"] == "cancelled"
    assert enrichment["AI_called"] is True
    assert enrichment["attempted_event_count"] == 1
    assert enrichment["timeout_event_count"] == 0
    assert row["AI_called"] is True
    assert row["attempted"] is True
    assert row["timeout"] is False


@pytest.mark.parametrize(
    ("quality", "expected_status", "expected_called"),
    [
        ({"ai_research_enabled": False}, "disabled", False),
        ({"ai_research_enabled": True, "ai_research_configured": False}, "not_configured", False),
        ({"ai_research_enabled": True, "ai_research_configured": True}, "not_required", False),
        ({"ai_research_enabled": True, "ai_research_configured": True, "ai_not_available": True}, "not_available", False),
        ({"ai_research_enabled": True, "ai_research_configured": True, "ai_research_called": True, "ai_candidate_event_ids": ["evt-cpi"], "ai_research_status": "success"}, "completed", True),
        ({"ai_research_enabled": True, "ai_research_configured": True, "ai_research_called": True, "ai_candidate_event_ids": ["evt-cpi"], "ai_research_status": "provider_failed", "ai_failure_reason": "codex_cli_non_zero_exit"}, "failed", True),
        ({"ai_research_enabled": True, "ai_research_configured": True, "ai_research_called": True, "ai_candidate_event_ids": ["evt-cpi"], "ai_research_status": "provider_failed", "ai_failure_reason": "ai_research_timeout"}, "timeout", True),
        ({"ai_research_enabled": True, "ai_research_configured": True, "ai_research_called": True, "ai_candidate_event_ids": ["evt-cpi"], "ai_research_status": "cancelled"}, "cancelled", True),
        ({"ai_research_enabled": True, "ai_research_configured": True, "ai_research_called": True, "ai_candidate_event_ids": ["evt-cpi"], "ai_research_status": "rejected", "ai_results_rejected": 1}, "rejected", True),
    ],
)
def test_ai_enrichment_state_machine_has_only_coherent_states(quality, expected_status, expected_called) -> None:
    metadata = _event_enrichment_metadata({"data_quality": quality}, [_event()])
    row = metadata["events"][0]

    assert metadata["status"] == expected_status
    assert metadata["AI_called"] is expected_called
    assert row["status"] == expected_status
    assert row["AI_called"] is expected_called
    assert row["attempted"] is expected_called
    assert row["timeout"] is (expected_status == "timeout")
    assert not (row["attempted"] is False and row["timeout"] is True)
    assert not (metadata["attempted_event_count"] == 0 and metadata["timeout_event_count"] > 0)
    assert not (metadata["AI_called"] is False and metadata["completed_event_count"] > 0)


def test_ai_enrichment_reports_same_run_persistence_read_back() -> None:
    event = _event()
    event.enrichment = EventEnrichment(
        previous=0.5,
        source="BLS",
        source_url="https://www.bls.gov/news.release/cpi.nr0.htm",
        cache_status="refreshed",
        summary={"persistence": {"persisted": True, "read_back": True}},
    )

    metadata = _event_enrichment_metadata(
        {
            "data_quality": {
                "ai_research_enabled": True,
                "ai_research_configured": True,
                "ai_research_called": True,
                "ai_candidate_event_ids": [event.event_id],
                "ai_research_status": "success",
            }
        },
        [event],
    )

    assert metadata["persisted_event_count"] == 1
    assert metadata["read_back_event_count"] == 1


def _event() -> EconomicEvent:
    return EconomicEvent(
        event_id="evt-cpi",
        name="Consumer Price Index",
        country="US",
        category="CPI",
        date="2099-07-14",
        time_utc=datetime(2099, 7, 14, 12, 30, tzinfo=UTC),
        impact=Impact.HIGH,
        source="BLS",
        source_url="https://bls.test",
        reliability=0.9,
        event_risk_level=Impact.HIGH,
    )
