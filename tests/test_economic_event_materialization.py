from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

from app.core.config import Settings
from app.core.logging import JsonFormatter
from app.api.routes import _materialize_market_context, market_context_mnq
from app.infrastructure.persistence.migrations import migrate_database
from app.models.common import Impact
from app.models.events import EconomicEvent, EventEnrichment
from app.models.macro import EventWindowsResponse, MacroLatestResponse
from app.services.ai_trader_contract_service import build_ai_trader_market_context
from app.services.economic_event_materialization_service import EconomicEventMaterializationService
from app.services.diagnostics_service import DiagnosticsService
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.event_window_service import EventWindowService
from app.services.market_context_builder import build_event_calendar, build_market_context_contract, build_section_quality
from app.services.market_fact_repository import MarketFactRepository, connect_market_db, encode, now_iso


def settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        codex_workspace_dir=tmp_path / "ai_workspace",
    )


def event(index: int = 0, *, category: str = "CPI", name: str = "Consumer Price Index") -> EconomicEvent:
    release = datetime.now(UTC).replace(microsecond=0) + timedelta(days=index + 1)
    return EconomicEvent(
        event_id=f"event-{category.lower()}-{index}",
        name=name,
        country="US",
        category=category,
        date=release.date().isoformat(),
        time_utc=release,
        time_local=release,
        impact=Impact.HIGH,
        source="Official calendar",
        source_url="https://official.test/calendar",
        reliability=0.95,
        event_risk_level=Impact.HIGH,
        default_risk_window_before_minutes=30,
        default_risk_window_after_minutes=30,
    )


def five_events() -> list[EconomicEvent]:
    definitions = [
        ("CPI", "Consumer Price Index"),
        ("PPI", "Producer Price Index"),
        ("GDP", "GDP Advance Estimate"),
        ("PCE", "Personal Income and Outlays"),
        ("NFP / Nonfarm Payrolls", "Employment Situation"),
    ]
    return [event(index, category=category, name=name) for index, (category, name) in enumerate(definitions)]


def fact(materializer: EconomicEventMaterializationService, item: EconomicEvent, previous: float) -> dict:
    metric_id = "headline_cpi_mom" if item.category == "CPI" else f"{item.category.lower()}_previous"
    valid_until = (datetime.now(UTC) + timedelta(days=10)).isoformat()
    return {
        "fact_key": materializer.fact_key(item),
        "fact_type": "macro_event_enrichment",
        "country": item.country,
        "category": item.category,
        "event_name": item.name,
        "previous": previous,
        "source": "Research",
        "source_url": "https://research.test/macro",
        "provider_type": "AI_RESEARCHER_CODEX_CLI",
        "reliability": 0.9,
        "confidence": 0.9,
        "valid_until": valid_until,
        "next_refresh_at": valid_until,
        "raw_payload_json": {
            "metrics": [
                {
                    "metric_id": metric_id,
                    "previous": previous,
                    "source": "Research",
                    "source_url": "https://research.test/macro",
                    "reliability": 0.9,
                    "confidence": 0.9,
                }
            ]
        },
    }


def persist_history_and_facts(cfg: Settings, events: list[EconomicEvent]) -> None:
    repository = MarketFactRepository(cfg)
    materializer = EconomicEventMaterializationService(cfg, facts=repository)
    for index, item in enumerate(events):
        repository.upsert_economic_event(
            item,
            event_key=f"{item.country}:{item.date}:{item.event_id}",
            valid_until=(datetime.now(UTC) + timedelta(days=10)).isoformat(),
        )
        repository.upsert_fact(fact(materializer, item, previous=index + 0.5))


def insert_raw_fact(cfg: Settings, payload: dict) -> None:
    timestamp = payload.get("updated_at") or now_iso()
    with connect_market_db(cfg) as conn:
        conn.execute(
            """
            INSERT INTO market_facts (
                fact_key, fact_type, country, category, event_name, previous, source, source_url,
                provider_type, reliability, confidence, retrieved_at, valid_until, status,
                raw_payload_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["fact_key"], payload["fact_type"], "US", payload.get("category", "CPI"),
                payload.get("event_name", "Consumer Price Index"), payload.get("previous"), "Legacy Research",
                "https://legacy.test", "AI_RESEARCHER_CODEX_CLI", 0.8, 0.8, timestamp,
                payload["valid_until"], payload.get("status", "active"),
                encode(payload.get("raw_payload_json") or {}), timestamp, timestamp,
            ),
        )
        conn.commit()


def test_five_events_survive_new_repository_and_materializer_instances(tmp_path) -> None:
    cfg = settings(tmp_path)
    events = five_events()
    persist_history_and_facts(cfg, events)

    restarted = EconomicEventMaterializationService(settings(tmp_path))
    materialized, metrics = restarted.load_from_history(
        country="US",
        start=datetime.now(UTC),
        end=datetime.now(UTC) + timedelta(days=30),
        refresh_mode="false",
    )

    assert len(materialized) == 5
    assert metrics == {
        "history_event_count": 5,
        "enrichment_fact_lookup_count": 5,
        "enrichment_fact_hit_count": 5,
        "enrichment_fact_miss_count": 0,
        "enrichment_fact_stale_count": 0,
        "enrichment_materialized_count": 5,
        "legacy_fact_type_count": 0,
    }
    assert all(item.enrichment.cache_status == "hit" for item in materialized)
    cpi = next(item for item in materialized if item.category == "CPI")
    assert cpi.enrichment.previous == "0.5"
    assert cpi.enrichment.metrics[0]["metric_id"] == "headline_cpi_mom"
    assert cpi.enrichment.metrics[0]["previous"] == 0.5


def test_legacy_fact_type_is_materialized_and_counted(tmp_path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    materializer = EconomicEventMaterializationService(cfg, facts=repository)
    item = event()
    repository.upsert_economic_event(item, event_key=f"US:{item.date}:{item.event_id}")
    legacy = fact(materializer, item, 0.7)
    legacy["fact_type"] = "ai_research_result"
    insert_raw_fact(cfg, legacy)

    output, metrics = materializer.load_from_history(
        country="US", start=datetime.now(UTC), end=datetime.now(UTC) + timedelta(days=5)
    )

    assert output[0].enrichment.previous == "0.7"
    assert metrics["legacy_fact_type_count"] == 1
    assert metrics["enrichment_materialized_count"] == 1


def test_canonical_and_legacy_versions_select_newer_valid_fact_without_duplicate(tmp_path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    materializer = EconomicEventMaterializationService(cfg, facts=repository)
    item = event()
    repository.upsert_economic_event(item, event_key=f"US:{item.date}:{item.event_id}")
    canonical = fact(materializer, item, 0.5)
    canonical["updated_at"] = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
    insert_raw_fact(cfg, canonical)
    legacy = {**fact(materializer, item, 0.8), "fact_key": materializer.fact_key(item).replace(":macro_event_enrichment", ":ai_research_result"), "fact_type": "ai_research_result"}
    legacy["updated_at"] = datetime.now(UTC).isoformat()
    insert_raw_fact(cfg, legacy)

    output, metrics = materializer.load_from_history(
        country="US", start=datetime.now(UTC), end=datetime.now(UTC) + timedelta(days=5)
    )

    assert len(output) == 1
    assert output[0].enrichment.previous == "0.8"
    assert metrics["legacy_fact_type_count"] == 1
    assert repository.count(fact_type="macro_event_enrichment") == 2
    assert len(repository.get_valid_facts_by_type("macro_event_enrichment")) == 1


def test_startup_migration_canonicalizes_existing_ai_fact_type(tmp_path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    materializer = EconomicEventMaterializationService(cfg, facts=repository)
    legacy = fact(materializer, event(), 0.5)
    legacy["fact_type"] = "ai_research_result"
    insert_raw_fact(cfg, legacy)

    result = migrate_database(cfg.database_path)

    assert result["legacy_event_enrichment_facts_migrated"] == 1
    assert repository.get_fact(legacy["fact_key"])["fact_type"] == "macro_event_enrichment"


def test_stale_fact_is_reported_but_not_materialized(tmp_path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    materializer = EconomicEventMaterializationService(cfg, facts=repository)
    item = event()
    repository.upsert_economic_event(item, event_key=f"US:{item.date}:{item.event_id}")
    stale = fact(materializer, item, 0.5)
    stale["valid_until"] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    repository.upsert_fact(stale)

    output, metrics = materializer.load_from_history(
        country="US", start=datetime.now(UTC), end=datetime.now(UTC) + timedelta(days=5)
    )

    assert output[0].enrichment.previous is None
    assert output[0].enrichment.cache_status == "expired"
    assert "stale_fact" in output[0].enrichment.warnings
    assert metrics["enrichment_fact_stale_count"] == 1
    assert metrics["enrichment_materialized_count"] == 0


def test_negative_cache_is_explicit_in_cache_only_materialization(tmp_path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    materializer = EconomicEventMaterializationService(cfg, facts=repository)
    item = event()
    repository.upsert_economic_event(item, event_key=f"US:{item.date}:{item.event_id}")
    negative = fact(materializer, item, 0.5)
    negative.update({"previous": None, "status": "no_data_available", "raw_payload_json": {}, "warnings_json": ["ai_negative_cache:no_data_available"]})
    repository.upsert_fact(negative)

    output, metrics = materializer.load_from_history(
        country="US", start=datetime.now(UTC), end=datetime.now(UTC) + timedelta(days=5)
    )

    assert output[0].enrichment.cache_status == "hit"
    assert output[0].enrichment.previous is None
    assert "no_data_available" in output[0].enrichment.warnings
    assert metrics["enrichment_fact_hit_count"] == 1


async def test_concurrent_cache_only_reads_are_equivalent_and_non_mutating(tmp_path) -> None:
    cfg = settings(tmp_path)
    persist_history_and_facts(cfg, five_events())
    materializer = EconomicEventMaterializationService(settings(tmp_path))

    first, second = await asyncio.gather(
        asyncio.to_thread(materializer.load_from_history, country="US", start=datetime.now(UTC), end=datetime.now(UTC) + timedelta(days=30)),
        asyncio.to_thread(materializer.load_from_history, country="US", start=datetime.now(UTC), end=datetime.now(UTC) + timedelta(days=30)),
    )

    assert [item.model_dump(mode="json") for item in first[0]] == [item.model_dump(mode="json") for item in second[0]]
    assert MarketFactRepository(cfg).count(fact_type="macro_event_enrichment") == 5


def test_empty_critical_events_fail_serialized_contract_invariant(tmp_path) -> None:
    events = five_events()
    full = build_market_context_contract(
        symbol="MNQ",
        macro=MacroLatestResponse(),
        events_today=[],
        upcoming_events=events,
        event_windows=EventWindowsResponse(symbol="MNQ", checked_at_utc=datetime.now(UTC).isoformat()),
        nasdaq_context=None,
        news_items=[],
        data_quality={"refresh_mode": "false"},
        db_summary={},
    )
    consumer = json.loads(json.dumps(build_ai_trader_market_context(full), default=str))

    critical = consumer["data_quality"]["section_quality"]["critical_macro_events"]
    assert critical["completeness_score"] == 0.0
    assert critical["missing_event_count"] == 5
    assert consumer["data_quality"]["critical_missing_count"] >= 5
    assert consumer["data_quality"]["completeness_score"] != 1.0
    assert consumer["readiness"]["ready"] is False


def test_previous_metrics_produce_partial_not_false_complete_quality() -> None:
    events = five_events()
    for index, item in enumerate(events):
        item.enrichment = EventEnrichment(
            previous=index + 0.5,
            metrics=[{"metric_id": f"metric-{index}", "previous": index + 0.5}],
            source="Research",
            source_url="https://research.test",
        )
    calendar = build_event_calendar(events)
    quality = build_section_quality(
        macro_snapshot={},
        event_calendar=calendar,
        nasdaq_context=None,
        news_context={},
        existing_quality={"refresh_mode": "false"},
    )["critical_macro_events"]

    assert quality["missing_event_count"] == 0
    assert quality["partial_event_count"] == 5
    assert 0.0 < quality["completeness_score"] < 1.0


def test_non_quantitative_fed_event_is_not_penalized() -> None:
    speech = event(category="FOMC", name="FOMC Press Conference")
    calendar = build_event_calendar([speech])

    assert calendar["critical_macro_events"] == []
    assert len(calendar["fed_communications"]) == 1


async def test_force_then_new_service_instance_and_cache_only_route_preserve_all_events(tmp_path) -> None:
    cfg = settings(tmp_path)
    cfg.enable_ai_researcher = True
    events = five_events()

    class EmptyProvider:
        calls = 0

        async def enrich_events(self, events, country, start, end):
            self.calls += 1
            return events, {"providers_attempted": 1}

    class AI:
        calls = 0

        async def research_and_save(self, payloads):
            self.calls += 1
            return ai_facts, {"status": "success", "results_valid": 5}

    provider = EmptyProvider()
    ai = AI()
    process_a = EnrichmentOrchestrator(cfg, event_enrichment_service=provider, ai_researcher_service=ai)
    ai_facts = [fact(process_a.event_materializer, item, index + 0.5) for index, item in enumerate(events)]

    forced, force_metadata = await process_a.enrich_events(
        events=events,
        country="US",
        start=datetime.now(UTC),
        end=datetime.now(UTC) + timedelta(days=30),
        trigger="test_force_restart",
        force=True,
    )
    assert force_metadata["data_quality"]["ai_research_requests"] == 5
    assert force_metadata["data_quality"]["ai_events_requested"] == 5
    assert ai.calls == 0
    assert all("ai_enrichment_pending" in item.enrichment.warnings for item in forced)

    class NoNetworkService:
        calls = 0

        async def latest(self, *args, **kwargs):
            self.calls += 1
            raise AssertionError("network-capable macro service called during refresh=false")

        async def context(self, *args, **kwargs):
            self.calls += 1
            raise AssertionError("network-capable Nasdaq service called during refresh=false")

        async def list_events(self, *args, **kwargs):
            self.calls += 1
            raise AssertionError("event provider called during refresh=false")

        async def upcoming(self, *args, **kwargs):
            self.calls += 1
            raise AssertionError("event provider called during refresh=false")

    no_network = NoNetworkService()
    process_b = EnrichmentOrchestrator(cfg, event_enrichment_service=None)
    diagnostics = DiagnosticsService(
        cfg,
        macro_service=no_network,
        event_service=no_network,
        event_window_service=EventWindowService(no_network),
        nasdaq_data_service=no_network,
        enrichment_orchestrator=process_b,
    )
    full = await diagnostics.full_model(
        country="US", days=30, symbol="MNQ", fetch_missing_nasdaq=False, refresh="false"
    )
    cache_only = full["event_calendar"]["critical_macro_events"]
    assert len(cache_only) == 5
    assert no_network.calls == 0
    assert full["data_quality"]["enrichment_materialized_count"] == 0
    assert full["data_quality"]["enrichment_fact_hit_count"] == 0
    _materialize_market_context(
        full,
        refresh="test_seed",
        view="consumer",
        settings=cfg,
    )

    consumer = await market_context_mnq(
        refresh="false",
        view="consumer",
        macro_service=no_network,
        event_service=no_network,
        event_window_service=EventWindowService(no_network),
        nasdaq_service=no_network,
        enrichment_orchestrator=process_b,
    )
    serialized = json.loads(json.dumps(consumer, default=str))
    critical = serialized["event_risk"]["critical_events"]
    assert len(critical) == 5
    temporal_statuses = [item.get("temporal_status") for item in critical]
    assert all(status in {"PRE_RELEASE", "AWAITING_ACTUAL"} for status in temporal_statuses), temporal_statuses
    assert serialized["ai_enrichment"]["status"] == "PENDING"
    assert no_network.calls == 0


def test_json_formatter_keeps_structured_materialization_context() -> None:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="event_enrichment_materialized",
        args=(),
        exc_info=None,
    )
    record.event_id = "evt-cpi"
    record.fact_key = "US:CPI:key:macro_event_enrichment"
    record.refresh_mode = "false"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["event_id"] == "evt-cpi"
    assert payload["fact_key"].endswith("macro_event_enrichment")
    assert payload["refresh_mode"] == "false"
