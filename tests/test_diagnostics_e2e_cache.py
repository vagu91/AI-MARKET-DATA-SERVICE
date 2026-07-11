from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from app.api.deps import (
    get_enrichment_orchestrator,
    get_event_service,
    get_event_window_service,
    get_macro_service,
    get_nasdaq_data_service,
)
from app.core.config import Settings
from app.main import app
from app.models.common import Freshness, Impact, ProviderMetadata, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.models.macro import EventWindowsResponse, MacroLatestResponse, MacroSeries
from app.models.nasdaq import (
    EarningsQuality,
    EarningsResponse,
    MegaCapBreadthQuality,
    MegaCapBreadthResponse,
    MegaCapSnapshotQuality,
    MegaCapSnapshotResponse,
    NasdaqContextResponse,
    NewsQuality,
    NewsResponse,
    QQQHoldingsSummary,
)
from app.services.diagnostics_service import DiagnosticsService
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.market_fact_repository import MarketFactRepository
from app.services.market_news_repository import MarketNewsRepository


def settings(tmp_path, **overrides) -> Settings:
    return Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        codex_workspace_dir=tmp_path / "ai_workspace",
        **overrides,
    )


def event(category="CPI", name="Consumer Price Index", event_id="evt-cpi") -> EconomicEvent:
    return EconomicEvent(
        event_id=event_id,
        name=name,
        country="US",
        category=category,
        date="2099-07-14",
        time_utc=datetime(2099, 7, 14, 12, 30, tzinfo=UTC),
        time_local=datetime(2099, 7, 14, 14, 30, tzinfo=UTC),
        impact=Impact.HIGH,
        source="BLS",
        source_url="https://bls.test",
        reliability=0.95,
        event_risk_level=Impact.HIGH,
        default_risk_window_before_minutes=30,
        default_risk_window_after_minutes=30,
    )


class FakeMacroService:
    def __init__(self):
        self.calls = 0

    async def latest(self):
        self.calls += 1
        metadata = ProviderMetadata(
            source="FRED",
            provider_type=ProviderType.API,
            retrieved_at=datetime.now(UTC),
            freshness=Freshness.RECENT,
            reliability=0.95,
        )
        return MacroLatestResponse(
            series=[
                MacroSeries(
                    series_id="VIXCLS",
                    name="VIX",
                    value=12.3,
                    units="index",
                    data_as_of="2099-07-13",
                    source="FRED",
                    metadata=metadata,
                )
            ],
            provider_results=[metadata],
        )


class FakeEventService:
    last_enrichment_metadata = {}

    async def list_events(self, country="US", start=None, end=None, enrich=True):
        return [event()]

    async def today(self, country="US"):
        return [event()]


class FakeWindowService:
    async def event_windows(self, symbol):
        return EventWindowsResponse(symbol=symbol, checked_at_utc=datetime.now(UTC).isoformat())


class FakeProviderEnrichment:
    def __init__(self):
        self.calls = 0

    async def enrich_events(self, events, country, start, end):
        self.calls += 1
        output = []
        for item in events:
            updated = item.model_copy(deep=True)
            updated.enrichment = EventEnrichment(
                forecast="0.3%",
                previous="0.2%",
                source="Provider",
                source_url="https://provider.test/cpi",
                provider_type=ProviderType.SCRAPER,
                retrieved_at=datetime.now(UTC),
                reliability=0.7,
                confidence=0.7,
            )
            output.append(updated)
        return output, {"providers_attempted": 1}


class EmptyProviderEnrichment:
    async def enrich_events(self, events, country, start, end):
        return events, {"providers_attempted": 1}


class FakeNasdaqService:
    def __init__(self):
        self.calls = 0

    async def context(self):
        self.calls += 1
        now = datetime.now(UTC)
        return NasdaqContextResponse(
            generated_at=now,
            qqq_holdings_summary=QQQHoldingsSummary(source="QQQ", reliability=0.7, top_holdings=[]),
            mega_cap_snapshot=MegaCapSnapshotResponse(
                retrieved_at=now,
                source="Yahoo",
                provider_type=ProviderType.API,
                reliability=0.7,
                stocks=[],
                data_quality=MegaCapSnapshotQuality(tracked_count=0, final_data_available=True),
            ),
            mega_cap_breadth=MegaCapBreadthResponse(
                retrieved_at=now,
                tracked_count=0,
                positive_count=0,
                negative_count=0,
                neutral_count=0,
                weighted_positive_pct=0,
                weighted_negative_pct=0,
                weighted_neutral_pct=0,
                average_change_pct=0,
                weighted_average_change_pct=0,
                reliability=0.7,
                data_quality=MegaCapBreadthQuality(),
            ),
            upcoming_earnings=EarningsResponse(
                retrieved_at=now,
                days=14,
                events=[],
                data_quality=EarningsQuality(final_data_available=True),
            ),
            latest_news=NewsResponse(
                retrieved_at=now,
                articles=[],
                data_quality=NewsQuality(final_data_available=True),
            ),
            metadata={"service_role": "data provider only", "warnings": [], "critical_errors": []},
        )


class FakeAIService:
    def __init__(self, facts):
        self.calls = 0
        self.facts = facts

    async def research_and_save(self, events):
        self.calls += 1
        return self.facts, {"status": "success"}


def diagnostics(tmp_path, *, enrichment=None, ai=None, enable_ai=False):
    cfg = settings(tmp_path, enable_ai_researcher=enable_ai)
    orchestrator = EnrichmentOrchestrator(
        cfg,
        event_enrichment_service=enrichment or FakeProviderEnrichment(),
        ai_researcher_service=ai,
    )
    return DiagnosticsService(
        cfg,
        macro_service=FakeMacroService(),
        event_service=FakeEventService(),
        event_window_service=FakeWindowService(),
        nasdaq_data_service=FakeNasdaqService(),
        enrichment_orchestrator=orchestrator,
    )


async def test_diagnostics_reset_first_and_second_run_cache(tmp_path):
    service = diagnostics(tmp_path)
    first = await service.e2e_cache_test(reset_db=True, enable_ai=False, run_count=1)
    assert first["runs"][0]["db_misses"] > 0
    assert first["runs"][0]["facts_total_after_run"] > 0

    second = await service.e2e_cache_test(reset_db=False, enable_ai=False, run_count=1)
    assert second["runs"][0]["db_hits"] > 0

    repo = MarketFactRepository(service.settings)
    repo.reset_data_tables()
    assert repo.db_summary()["market_facts"]["total"] == 0
    repo.count()


async def test_diagnostics_ai_mock_saved_then_db_hit_skips_ai(tmp_path):
    cfg = settings(tmp_path, enable_ai_researcher=True)
    fact_key = EnrichmentOrchestrator(cfg).fact_key(event())
    ai = FakeAIService(
        [
            {
                "fact_key": fact_key,
                "fact_type": "ai_research_result",
                "country": "US",
                "category": "CPI",
                "forecast": "0.4%",
                "source": "Research",
                "source_url": "https://research.test/cpi",
                "provider_type": "AI_RESEARCHER",
                "valid_until": (datetime.now(UTC) + timedelta(days=1)).isoformat(),
            }
        ]
    )
    service = diagnostics(tmp_path, enrichment=EmptyProviderEnrichment(), ai=ai, enable_ai=True)
    first = await service.e2e_cache_test(reset_db=True, enable_ai=True, run_count=1)
    assert first["runs"][0]["ai_research_used"] is True
    assert ai.calls == 1

    second = await service.e2e_cache_test(reset_db=False, enable_ai=True, run_count=1)
    assert second["runs"][0]["db_hits"] > 0
    assert ai.calls == 1


def test_fed_speech_excluded_from_ai_default(tmp_path):
    orchestrator = EnrichmentOrchestrator(settings(tmp_path, enable_ai_researcher=True))
    speech = event(category="FOMC", name="Fed Speech - Chair Powell", event_id="fed-speech")
    assert orchestrator._ai_candidates([speech]) == []


def test_diagnostics_endpoints_and_full_model_data_only(tmp_path):
    service = diagnostics(tmp_path)
    app.dependency_overrides[get_macro_service] = lambda: service.macro_service
    app.dependency_overrides[get_event_service] = lambda: service.event_service
    app.dependency_overrides[get_event_window_service] = lambda: service.event_window_service
    app.dependency_overrides[get_nasdaq_data_service] = lambda: service.nasdaq_data_service
    app.dependency_overrides[get_enrichment_orchestrator] = lambda: service.enrichment_orchestrator
    try:
        with TestClient(app) as client:
            e2e = client.post("/diagnostics/e2e-cache-test?reset_db=true&enable_ai=false&run_count=1")
            summary = client.get("/diagnostics/db-summary")
            full = client.get("/diagnostics/full-model?symbol=MNQ&country=US&days=30")
    finally:
        app.dependency_overrides.clear()

    assert e2e.status_code == 200
    assert summary.status_code == 200
    assert full.status_code == 200
    payload = full.json()
    assert payload["service_role"] == "data provider only"
    text = str(payload).lower()
    assert "recommendation" not in text


async def test_full_model_materializes_symbolless_macro_news_from_db(tmp_path):
    service = diagnostics(tmp_path)
    MarketNewsRepository(service.settings).upsert_news(
        {
            "title": "Fed minutes expose deep divide over interest-rate outlook",
            "source": "Reuters",
            "source_url": "https://www.reuters.com/markets/fed-minutes-test",
            "published_at": datetime.now(UTC).isoformat(),
            "retrieved_at": datetime.now(UTC).isoformat(),
            "summary": "Reuters reports a material Federal Reserve policy debate with implications for rates.",
            "valid_until": (datetime.now(UTC) + timedelta(hours=6)).isoformat(),
            "symbols": [],
            "topics": ["Fed", "macro"],
            "provider_type": "RSS",
            "reliability": 0.7,
        }
    )

    model = await service.full_model(country="US", days=30, symbol="MNQ", fetch_missing_nasdaq=True)

    assert model["news_context"]["latest"]
    assert model["news_context"]["latest"][0]["source_url"] == "https://www.reuters.com/markets/fed-minutes-test"
    assert model["data_quality"]["news_pipeline"]["eligible_count"] == 1
    assert model["data_quality"]["news_pipeline"]["materialized_count"] == 1


async def test_full_model_propagates_force_and_refresh_false_skips_orchestrator(tmp_path):
    service = diagnostics(tmp_path)
    original = service.enrichment_orchestrator.enrich_events
    force_values: list[bool] = []

    async def capture_force(**kwargs):
        force_values.append(kwargs["force"])
        return await original(**kwargs)

    service.enrichment_orchestrator.enrich_events = capture_force

    await service.full_model(country="US", days=30, symbol="MNQ", fetch_missing_nasdaq=False, refresh="force")
    assert force_values == [True]

    force_values.clear()
    await service.full_model(country="US", days=30, symbol="MNQ", fetch_missing_nasdaq=False, refresh="false")
    assert force_values == []
