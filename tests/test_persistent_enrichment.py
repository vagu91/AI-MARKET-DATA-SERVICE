from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from app.api.deps import get_enrichment_orchestrator, get_event_service
from app.core.config import Settings
from app.main import app
from app.models.events import EconomicEvent, EventEnrichment
from app.providers.ai_researcher_provider import AIResearcherProvider
from app.services.data_freshness_service import DataFreshnessService
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.fact_key_service import FactKeyService
from app.services.market_fact_repository import MarketFactRepository, init_market_db
from app.services.market_news_repository import MarketNewsRepository


def settings(tmp_path, **overrides) -> Settings:
    return Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        codex_workspace_dir=tmp_path / "ai_workspace",
        **overrides,
    )


def make_event(**overrides) -> EconomicEvent:
    payload = {
        "event_id": "evt-cpi",
        "name": "Consumer Price Index",
        "country": "US",
        "category": "CPI",
        "date": "2099-07-14",
        "time_utc": "2099-07-14T12:30:00+00:00",
        "time_local": "2099-07-14T14:30:00+02:00",
        "impact": "HIGH",
        "source": "BLS",
        "source_url": "https://bls.test",
        "reliability": 0.95,
        "event_risk_level": "HIGH",
        "default_risk_window_before_minutes": 30,
        "default_risk_window_after_minutes": 30,
    }
    payload.update(overrides)
    return EconomicEvent.model_validate(payload)


class CountingEnrichmentService:
    def __init__(self, enrichment: EventEnrichment | None = None) -> None:
        self.calls = 0
        self.enrichment = enrichment

    async def enrich_events(self, events, country, start, end):
        self.calls += 1
        updated = []
        for event in events:
            copy = event.model_copy(deep=True)
            if self.enrichment:
                copy.enrichment = self.enrichment
            updated.append(copy)
        return updated, {"providers_attempted": 1}


class FakeAIService:
    def __init__(self, facts=None) -> None:
        self.calls = 0
        self.facts = facts or []

    async def research_and_save(self, events):
        self.calls += 1
        return self.facts, {"status": "success" if self.facts else "no_data_available"}


def test_db_init_and_fact_freshness(tmp_path):
    cfg = settings(tmp_path)
    init_market_db(cfg)
    repo = MarketFactRepository(cfg)
    key = FactKeyService().macro_event_key(
        country="US",
        category="CPI",
        event_date="2099-07-14",
        event_name="Consumer Price Index",
    )
    fact = repo.upsert_fact(
        {
            "fact_key": key,
            "fact_type": "macro_event_enrichment",
            "country": "US",
            "category": "CPI",
            "forecast": "0.3%",
            "source_url": "https://example.test/cpi",
            "valid_until": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        }
    )
    assert fact["forecast"] == "0.3%"
    assert DataFreshnessService(cfg).evaluate(fact).cache_status == "hit"

    stale = repo.upsert_fact({**fact, "valid_until": (datetime.now(UTC) - timedelta(hours=1)).isoformat()})
    assert DataFreshnessService(cfg).evaluate(stale, allow_stale=False).usable is False
    assert DataFreshnessService(cfg).evaluate(stale, allow_stale=True).usable is True


def test_orchestrator_db_hit_does_not_call_provider(tmp_path):
    cfg = settings(tmp_path)
    event = make_event()
    orchestrator = EnrichmentOrchestrator(cfg, event_enrichment_service=CountingEnrichmentService())
    MarketFactRepository(cfg).upsert_fact(
        {
            "fact_key": orchestrator.fact_key(event),
            "fact_type": "macro_event_enrichment",
            "country": "US",
            "category": "CPI",
            "forecast": "0.3%",
            "source": "Stored",
            "source_url": "https://stored.test/cpi",
            "provider_type": "DB",
            "valid_until": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        }
    )
    enriched, metadata = run_async(
        orchestrator.enrich_events(
            events=[event],
            country="US",
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(days=7),
            trigger="test",
        )
    )
    assert orchestrator.event_enrichment_service.calls == 0
    assert enriched[0].enrichment.forecast == "0.3%"
    assert metadata["data_quality"]["db_hits"] == 1


def test_orchestrator_provider_success_saves_db(tmp_path):
    cfg = settings(tmp_path)
    event = make_event()
    provider = CountingEnrichmentService(
        EventEnrichment(
            forecast="0.3%",
            previous="0.2%",
            source="Provider",
            source_url="https://provider.test/cpi",
            provider_type="SCRAPER",
            reliability=0.7,
        )
    )
    orchestrator = EnrichmentOrchestrator(cfg, event_enrichment_service=provider)
    enriched, metadata = run_async(
        orchestrator.enrich_events(
            events=[event],
            country="US",
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(days=7),
            trigger="test",
        )
    )
    assert provider.calls == 1
    assert enriched[0].enrichment.forecast == "0.3%"
    assert MarketFactRepository(cfg).get_fact(orchestrator.fact_key(event))["forecast"] == "0.3%"
    assert metadata["data_quality"]["provider_hits"] == 1


def test_orchestrator_ai_disabled_and_enabled_paths(tmp_path):
    event = make_event()
    cfg = settings(tmp_path, enable_ai_researcher=False)
    orchestrator = EnrichmentOrchestrator(cfg, event_enrichment_service=CountingEnrichmentService())
    enriched, metadata = run_async(
        orchestrator.enrich_events(
            events=[event],
            country="US",
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(days=7),
            trigger="test",
        )
    )
    assert enriched[0].enrichment.forecast is None
    assert "ai_researcher_disabled" in metadata["data_quality"]["warnings"]

    cfg_enabled = settings(tmp_path / "ai", enable_ai_researcher=True)
    fact_key = FactKeyService().macro_event_key(
        country="US",
        category="CPI",
        event_date="2099-07-14",
        event_name="Consumer Price Index",
    )
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
                "valid_until": "2099-07-14T12:30:00+00:00",
            }
        ]
    )
    orchestrator = EnrichmentOrchestrator(
        cfg_enabled,
        event_enrichment_service=CountingEnrichmentService(),
        ai_researcher_service=ai,
    )
    enriched, metadata = run_async(
        orchestrator.enrich_events(
            events=[event],
            country="US",
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(days=7),
            trigger="test",
        )
    )
    assert ai.calls == 1
    assert enriched[0].enrichment.forecast == "0.4%"
    assert metadata["data_quality"]["ai_research_used"] is True


def test_ai_researcher_output_validation(tmp_path):
    cfg = settings(tmp_path, enable_ai_researcher=True)
    provider = AIResearcherProvider(cfg)
    output = Path(cfg.codex_workspace_dir) / "research_output.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"results": [{"fact_key": "x", "forecast": "0.3%"}]}), encoding="utf-8")
    facts, status = provider.load_output(output)
    assert facts == []
    assert "rejected_missing_source_url:x" in status["warnings"]

    output.write_text(json.dumps({"results": [{"fact_key": "x", "notes": "buy now"}]}), encoding="utf-8")
    facts, status = provider.load_output(output)
    assert facts == []
    assert status["status"] == "provider_failed"


def test_news_storage_deduplicates_and_sets_valid_until(tmp_path):
    cfg = settings(tmp_path)
    repo = MarketNewsRepository(cfg)
    article = {
        "title": "Fed and Nasdaq update",
        "source_url": "https://news.test/fed",
        "symbols": ["QQQ"],
        "topics": ["fed"],
        "provider_type": "RSS",
        "reliability": 0.6,
    }
    repo.upsert_news(article)
    repo.upsert_news({**article, "summary": "updated"})
    stored = repo.stored(symbols=["QQQ"])
    assert len(stored) == 1
    assert stored[0]["valid_until"] is not None


def test_new_endpoints_with_overrides(tmp_path):
    cfg = settings(tmp_path)
    orchestrator = EnrichmentOrchestrator(cfg, event_enrichment_service=CountingEnrichmentService())

    class FakeEventService:
        async def list_events(self, country="US", start=None, end=None, enrich=True):
            return [make_event()]

    app.dependency_overrides[get_event_service] = lambda: FakeEventService()
    app.dependency_overrides[get_enrichment_orchestrator] = lambda: orchestrator
    try:
        with TestClient(app) as client:
            assert client.get("/db/health").status_code == 200
            assert client.get("/facts/coverage?country=US&days=30").status_code == 200
            assert client.get("/facts/stale").status_code == 200
            run_response = client.post("/enrichment/run?country=US&days=30")
    finally:
        app.dependency_overrides.clear()
    assert run_response.status_code == 200
    assert run_response.json()["service_role"] == "data provider only"


def run_async(awaitable):
    import asyncio

    return asyncio.run(awaitable)
