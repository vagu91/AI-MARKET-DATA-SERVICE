from __future__ import annotations

import json
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.core.config import Settings
from app.core.text_normalization import normalize_payload_text
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.services.agentic_research_runtime import ALL_STEPS
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_trader_consumer_v2_service import build_ai_trader_consumer_v2
from app.services.db_only_market_context_materializer import (
    DBOnlyMarketContextMaterializer,
)
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_fact_repository import MarketFactRepository
from app.services.market_session_service import build_session_aware_schedule
from app.services.news_research_policy import NewsResearchPolicy
from app.services.parallel_research_coordinator import (
    ParallelResearchCoordinator,
    _parent_status,
)
from app.services.research_backend import (
    OpenAIResponsesResearchBackend,
    ResearchBackendResult,
    normalized_backend_input,
    select_research_backend,
)
from app.services.research_gap_manifest import ResearchGapManifestBuilder
from app.services.research_profiles import PROFILES
from app.services.research_runtime_repository import (
    ResearchRuntimeRepository,
    _tier_reliability,
)
from app.services.research_source_gateway import ResearchSourceGateway
from app.services.research_tool_telemetry import normalize_codex_event
from app.services.temporal_validation_service import (
    TemporalValidationService,
    normalize_event_semantics,
)


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "source_policy.json"
NOW = datetime(2026, 7, 23, 15, 25, 40, tzinfo=UTC)


def cfg(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "research_backend": "codex_cli",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def full_context(*, missing: str | None = None) -> dict[str, Any]:
    stamp = "2026-07-23T15:00:00+00:00"
    valid = "2026-07-24T15:00:00+00:00"
    context = {
        "symbol": "MNQ",
        "event_calendar": {
            "status": "AVAILABLE",
            "data_as_of": stamp,
            "valid_until": valid,
            "critical_macro_events": [{"event_id": "cpi"}],
        },
        "rates_expectations": {
            "status": "AVAILABLE",
            "data_as_of": stamp,
            "valid_until": valid,
            "probabilities": [{"target": "525-550"}],
        },
        "risk_context": {
            "status": "AVAILABLE",
            "data_as_of": stamp,
            "valid_until": valid,
            "vix": {"value": 17},
            "vvix": {"value": 92},
            "skew": {"value": 145},
            "term_structure": [{"tenor": "M1"}],
            "put_call": {"latest": 0.8},
        },
        "positioning": {
            "status": "AVAILABLE",
            "data_as_of": stamp,
            "valid_until": valid,
            "report_date": "2026-07-21",
            "net_position": 10,
        },
        "nasdaq_context": {
            "status": "AVAILABLE",
            "data_as_of": stamp,
            "valid_until": valid,
            "constituents": [{"symbol": "NVDA"}],
            "mega_cap_semiconductors": [{"symbol": "NVDA"}],
            "earnings": {"upcoming": [{"symbol": "MSFT"}]},
        },
        "news_context": {
            "status": "AVAILABLE",
            "data_as_of": stamp,
            "valid_until": valid,
            "articles": [{"canonical_url": "https://www.reuters.com/a"}],
        },
        "geopolitical_regulatory_risk": {
            "status": "AVAILABLE",
            "data_as_of": stamp,
            "valid_until": valid,
            "items": [{"authority": "Commerce"}],
        },
        "market_schedule": {"status": "AVAILABLE", "data_as_of": stamp},
    }
    mapping = {
        "macro_events": "event_calendar",
        "fed_rates": "rates_expectations",
        "vix_risk": "risk_context",
        "cot_positioning": "positioning",
        "nasdaq_100": "nasdaq_context",
        "news": "news_context",
        "geopolitical_regulatory_risk": "geopolitical_regulatory_risk",
    }
    if missing in mapping:
        context[mapping[missing]] = {}
    if missing == "earnings":
        context["nasdaq_context"]["earnings"] = {}
    if missing == "mega_cap_semiconductors":
        context["nasdaq_context"]["mega_cap_semiconductors"] = []
    return context


def base_snapshot(settings: Settings) -> dict[str, Any]:
    context = full_context()
    context["generated_at_utc"] = "2026-07-23T12:00:00+00:00"
    return MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="fixture",
        debug_payload=context,
        ai_enrichment={"status": "NOT_REQUIRED"},
    )


def make_run(
    settings: Settings,
    identity: str,
    *,
    profile_id: str = "VIX_RISK_RESEARCH",
) -> tuple[dict[str, Any], dict[str, Any], ResearchRuntimeRepository]:
    service = AIResearchJobService(settings)
    job, created = service.enqueue_explicit(
        job_type=profile_id,
        symbol="MNQ",
        correlation_id=identity,
        request_payload={"gap": {"topic": "vix_risk"}},
        force=True,
    )
    assert created
    repository = ResearchRuntimeRepository(settings, now=lambda: NOW)
    profile = PROFILES[profile_id]
    run = repository.ensure_run(job, profile.profile_id, profile.prompt_version)
    return job, run, repository


def finish_run(
    settings: Settings,
    job: dict[str, Any],
    run: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    result = {
        "run_id": run["run_id"],
        "status": status,
        "accepted_count": 0,
        "persisted_count": 0,
        "read_back_count": 0,
    }
    with connect_sqlite(settings.database_path) as conn:
        conn.execute(
            """
            UPDATE research_runs SET status=?,result_json=?,coverage_score=?,
                completed_at=?,data_as_of=?,updated_at=? WHERE run_id=?
            """,
            (
                status,
                json.dumps(result),
                0.5 if status == "PARTIAL" else 1.0,
                NOW.isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
                run["run_id"],
            ),
        )
        conn.execute(
            """
            UPDATE ai_research_jobs SET status=?,result_payload_json=?,
                completed_at=?,updated_at=? WHERE job_id=?
            """,
            (
                status,
                json.dumps(result),
                NOW.isoformat(),
                NOW.isoformat(),
                job["job_id"],
            ),
        )
        conn.commit()
    restored = AIResearchJobService(settings).repository.get(str(job["job_id"]))
    assert restored is not None
    return restored


def test_01_old_failed_new_partial_snapshot_uses_new_run(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    base_snapshot(settings)
    old_job, old_run, _ = make_run(settings, "old")
    finish_run(settings, old_job, old_run, "FAILED")
    new_job, new_run, _ = make_run(settings, "new")
    completed = finish_run(settings, new_job, new_run, "PARTIAL")
    snapshot = DBOnlyMarketContextMaterializer(
        settings,
        clock=lambda: NOW,
    ).materialize_for_job(
        job=completed,
        ai_enrichment={"status": "PARTIAL", "job_ids": [new_job["job_id"]]},
    )
    assert snapshot is not None
    research = snapshot["consumer_payload"]["research"]
    assert research["run_id"] == new_run["run_id"]
    assert research["run_id"] != old_run["run_id"]
    assert research["status"] == "PARTIAL"


def test_02_snapshot_research_job_and_link_are_coherent(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    base_snapshot(settings)
    job, run, _ = make_run(settings, "link")
    completed = finish_run(settings, job, run, "PARTIAL")
    snapshot = DBOnlyMarketContextMaterializer(
        settings,
        clock=lambda: NOW,
    ).materialize_for_job(
        job=completed,
        ai_enrichment={"status": "PARTIAL"},
    )
    assert snapshot is not None
    assert snapshot["source_job_id"] == job["job_id"]
    assert snapshot["research_run_id"] == run["run_id"]
    assert snapshot["research_link_status"] == "LINKED"
    assert snapshot["consumer_payload"]["research"]["job_id"] == job["job_id"]


def test_03_reference_clock_has_nasdaq_and_mnq_open() -> None:
    schedule = build_session_aware_schedule({}, now=NOW)
    assert schedule["nasdaq_cash_session"]["status"] == "open"
    assert schedule["mnq_session"]["status"] == "open"
    assert schedule["market_session_status"] == "open"


def test_04_cpi_2099_is_quarantined_and_not_exposed(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    facts = MarketFactRepository(settings)
    facts.upsert_economic_event(
        {
            "event_id": "evt-cpi",
            "name": "CPI",
            "country": "US",
            "category": "CPI",
            "date": "2099-07-14",
            "time_utc": "2099-07-14T12:30:00+00:00",
            "impact": "HIGH",
            "source": "BLS",
            "source_url": "https://www.bls.gov/",
            "reliability": 0.9,
        },
        "evt-cpi",
    )
    records = facts.economic_event_records(country="US")
    valid, quarantined = TemporalValidationService(
        settings,
        clock=lambda: NOW,
    ).audit_economic_events(records)
    assert not valid
    assert quarantined[0]["reason_code"] == "EVENT_BEYOND_CONFIGURED_HORIZON"
    with connect_sqlite(settings.database_path) as conn:
        row = conn.execute(
            "SELECT temporal_audit_status FROM economic_events_history"
        ).fetchone()
        audit_count = conn.execute("SELECT COUNT(*) FROM temporal_quarantine").fetchone()[0]
    assert row["temporal_audit_status"] == "QUARANTINED"
    assert audit_count == 1


def test_05_fresh_db_earnings_creates_no_earnings_agent(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    snapshot = base_snapshot(settings)
    manifest = ResearchGapManifestBuilder(
        settings,
        clock=lambda: NOW,
    ).build(snapshot=snapshot, components=full_context())
    assert "earnings" not in manifest["agent_topics"]
    parent = ParallelResearchCoordinator(settings).create_parent(
        manifest,
        correlation_id="earnings-fresh",
    )
    assert all(
        child.get("specialized_topic") != "earnings"
        for child in parent["child_jobs"]
    )


def test_06_missing_vix_creates_only_risk_child(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    snapshot = base_snapshot(settings)
    manifest = ResearchGapManifestBuilder(
        settings,
        clock=lambda: NOW,
    ).build(snapshot=snapshot, components=full_context(missing="vix_risk"))
    assert manifest["agent_topics"] == ["vix_risk"]
    parent = ParallelResearchCoordinator(settings).create_parent(
        manifest,
        correlation_id="vix-only",
    )
    assert [job["profile_id"] for job in parent["child_jobs"]] == [
        "VIX_RISK_RESEARCH"
    ]


def test_07_missing_cot_creates_only_cot_child(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    manifest = ResearchGapManifestBuilder(
        settings,
        clock=lambda: NOW,
    ).build(snapshot=None, components=full_context(missing="cot_positioning"))
    assert manifest["agent_topics"] == ["cot_positioning"]
    parent = ParallelResearchCoordinator(settings).create_parent(
        manifest,
        correlation_id="cot-only",
    )
    assert [job["profile_id"] for job in parent["child_jobs"]] == [
        "COT_POSITIONING_RESEARCH"
    ]


def test_08_independent_children_execute_concurrently(tmp_path: Path) -> None:
    coordinator = ParallelResearchCoordinator(cfg(tmp_path, research_parallelism=2))
    started = time.perf_counter()
    result = coordinator.execute_children(
        [{"job_id": "a"}, {"job_id": "b"}],
        lambda job: (time.sleep(0.12), job["job_id"])[1],
    )
    elapsed = time.perf_counter() - started
    assert result == ["a", "b"]
    assert elapsed < 0.22


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        (["SUCCEEDED", "SUCCEEDED"], "SUCCEEDED"),
        (["SUCCEEDED", "FAILED"], "PARTIAL"),
        (["NO_DATA", "NO_DATA"], "NO_DATA"),
        (["FAILED", "FAILED"], "FAILED"),
        (["SUCCEEDED", "RUNNING"], "RUNNING"),
    ],
)
def test_09_parent_aggregation_is_deterministic(
    statuses: list[str],
    expected: str,
) -> None:
    assert _parent_status(statuses) == expected


def test_10_cli_and_api_share_normalized_contract(tmp_path: Path) -> None:
    settings = cfg(tmp_path, research_backend="openai_api")
    job = {"job_id": "j", "symbol": "MNQ", "request_payload": {"gap": {"topic": "news"}}}
    run = {"run_id": "r"}
    profile = PROFILES["NEWS_RESEARCH"]
    normalized = normalized_backend_input(
        job=job,
        run=run,
        profile=profile,
        effective_budget={"max_searches": 3},
    )
    payload = {"status": "NO_DATA", "claims": [], "searches": []}
    api = OpenAIResponsesResearchBackend(
        settings,
        request_sender=lambda request: {
            "id": "resp-1",
            "model": request["model"],
            "output_json": payload,
            "usage": {"total_tokens": 5},
        },
    )
    api_result = api.execute_research(
        job=job,
        run=run,
        profile=normalized,
        workspace=tmp_path,
        watchdog_seconds=10,
        effective_budget={"max_searches": 3},
    )
    cli_result = ResearchBackendResult(
        "cli-1",
        "codex_cli",
        "AGENTIC_RESEARCH",
        payload,
        {"total_tokens": 5},
    )
    assert api_result.payload == cli_result.payload
    assert normalized["contract_version"] == "research_backend_v1"
    assert select_research_backend(settings, openai_request_sender=lambda _: {})\
        .backend_name == "openai_api"


def test_11_fomc_multi_day_is_normalized() -> None:
    event = normalize_event_semantics(
        {
            "category": "FOMC",
            "event_start_at": "2026-07-28T13:00:00Z",
            "event_end_at": "2026-07-29T18:00:00Z",
            "release_at": "2026-07-29T18:00:00Z",
        }
    )
    assert event["event_type"] == "FOMC_MEETING"
    assert event["event_start_at"] < event["event_end_at"]
    assert event["decision_at"] == event["release_at"]


def test_12_closed_board_meeting_uses_outcome_not_official_actual() -> None:
    event = normalize_event_semantics(
        {"category": "FED BOARD CLOSED MEETING", "event_kind": "closed_board_meeting"}
    )
    assert event["event_type"] == "FED_BOARD_MEETING"
    assert event["post_event_semantics"] == "outcome"
    assert event["post_event_semantics"] != "official_actual"


def test_13_no_current_item_is_not_not_applicable(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    context = full_context()
    context["news_context"] = {"status": "NO_RELEVANT_NEWS"}
    manifest = ResearchGapManifestBuilder(
        settings,
        clock=lambda: NOW,
    ).build(snapshot=None, components=context)
    news = next(item for item in manifest["items"] if item["topic"] == "news")
    assert news["deterministic_status"] == "NO_CURRENT_ITEM"
    assert news["applicability"] == "APPLICABLE"
    assert all(item["deterministic_status"] != "NOT_APPLICABLE" for item in manifest["items"])


def test_14_news_single_source_and_confirmed_are_distinct(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    _job, run, _repository = make_run(settings, "news-policy", profile_id="NEWS_RESEARCH")
    article = {
        "canonical_url": "https://www.reuters.com/technology/a",
        "publisher": "Reuters",
        "published_at": "2026-07-23T14:00:00Z",
        "mnq_relevance": 0.9,
        "content_acquired": True,
    }
    policy = NewsResearchPolicy(settings)
    single = policy.decide(
        run_id=run["run_id"],
        article=article,
        claim_verified=True,
        independent_domains={"reuters.com"},
        now=NOW,
    )
    confirmed = policy.decide(
        run_id=run["run_id"],
        article={**article, "canonical_url": "https://apnews.com/article/a"},
        claim_verified=True,
        independent_domains={"reuters.com", "apnews.com"},
        now=NOW,
    )
    assert single["confirmation_status"] == "SINGLE_SOURCE_REPORT"
    assert confirmed["confirmation_status"] == "CONFIRMED"


def test_15_pdf_http_200_records_extraction_failure(tmp_path: Path) -> None:
    settings = cfg(
        tmp_path,
        research_gateway_min_text_chars=20,
        research_gateway_respect_robots=False,
    )
    _job, run, repository = make_run(settings, "pdf")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            content=b"%PDF-1.4 broken",
            headers={"content-type": "application/pdf"},
            request=request,
        )
    )
    gateway = ResearchSourceGateway(
        settings,
        repository=repository,
        transport=transport,
        resolver=lambda _host: ["8.8.8.8"],
        now=lambda: NOW,
    )
    source = gateway.acquire(
        run["run_id"],
        {
            "source_url": "https://www.nvidia.com/content/dam/report.pdf",
            "publisher": "NVIDIA",
        },
    )
    assert source["http_status"] == 200
    assert source["http_fetched_at"]
    assert source["stage_status"] == "EXTRACTION_FAILED"


def test_16_tier_one_reliability_is_service_owned_and_nonzero() -> None:
    assert _tier_reliability(1) == pytest.approx(0.95)
    assert _tier_reliability(1) > _tier_reliability(3) > 0


def test_17_unicode_survives_json_sqlite_and_consumer(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    value = "July 28–29 — NVIDIA®’s déjà vu"
    normalized = normalize_payload_text({"title": value})
    assert normalized["title"] == value
    snapshot = MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="unicode",
        debug_payload={
            **full_context(),
            "generated_at_utc": NOW.isoformat(),
            "news_context": {
                "status": "AVAILABLE",
                "articles": [{
                    "title": value,
                    "published_at": "2026-07-23T14:00:00Z",
                }],
            },
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    encoded = json.dumps(snapshot["consumer_payload"], ensure_ascii=False)
    assert "July 28–29" in encoded
    assert "NVIDIA®’s" in encoded


def test_18_planned_search_count_differs_from_executed(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    _job, run, repository = make_run(settings, "query-counts")
    step, _ = repository.begin_step(
        run["run_id"],
        "SEARCH",
        2,
        {},
        backend="fake",
        tool="fake",
    )
    repository.record_query_plan(run["run_id"], ["q1", "q2", "q3", "q4"])
    event = normalize_codex_event(
        {
            "type": "item.completed",
            "item": {"id": "s1", "type": "web_search", "query": "q1"},
        },
        step_name="SEARCH",
    )[0]
    repository.record_tool_events(run["run_id"], step["step_id"], [event])
    restored = repository.get_run(run["run_id"])
    assert restored is not None
    assert restored["planned_query_count"] == 4
    assert restored["search_count"] == 1


def test_19_accepted_persisted_and_read_back_contract_is_exact() -> None:
    result = json.loads(
        (
            ROOT
            / "tests"
            / "fixtures"
            / "atomicfix_run_20260723_offline_replay.json"
        ).read_text(encoding="utf-8")
    )
    assert result["accepted_count"] == result["persisted_count"] == result["read_back_count"]
    assert ALL_STEPS[-2:] == ("MATERIALIZE", "COMPLETE")


def test_20_materializer_reads_committed_components_only(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    original = full_context()
    original["generated_at_utc"] = "2026-07-23T12:00:00Z"
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="base",
        debug_payload=original,
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    original["uncommitted_mutation"] = "must-not-appear"
    job, run, _ = make_run(settings, "committed-only")
    completed = finish_run(settings, job, run, "PARTIAL")
    snapshot = DBOnlyMarketContextMaterializer(
        settings,
        clock=lambda: NOW,
    ).materialize_for_job(job=completed, ai_enrichment={"status": "PARTIAL"})
    assert snapshot is not None
    assert "uncommitted_mutation" not in snapshot["debug_payload"]
    assert snapshot["debug_payload"]["metadata"]["worker_materialization"][
        "committed_component_count"
    ] > 0


def test_21_materialize_retry_does_not_duplicate_claim_evidence_fact(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    base_snapshot(settings)
    job, run, _ = make_run(settings, "materialize-retry")
    completed = finish_run(settings, job, run, "PARTIAL")
    materializer = DBOnlyMarketContextMaterializer(settings, clock=lambda: NOW)
    materializer.materialize_for_job(job=completed, ai_enrichment={"status": "PARTIAL"})
    before = _research_counts(settings, run["run_id"])
    materializer.materialize_for_job(job=completed, ai_enrichment={"status": "PARTIAL"})
    after = _research_counts(settings, run["run_id"])
    assert before == after


def test_22_refresh_false_read_is_byte_stable_and_zero_write(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    base_snapshot(settings)
    repository = MarketContextSnapshotRepository(settings)
    before = settings.database_path.read_bytes()
    first = json.dumps(
        repository.latest("MNQ")["consumer_payload"],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    second = json.dumps(
        repository.latest("MNQ")["consumer_payload"],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    after = settings.database_path.read_bytes()
    assert first == second
    assert before == after


def test_23_consumer_21_is_under_90kb(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    snapshot = base_snapshot(settings)
    payload = build_ai_trader_consumer_v2(
        snapshot["debug_payload"],
        settings=settings,
    )
    size = len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode())
    assert payload["schema_version"] == "2.1"
    assert size < 90_000


def test_24_no_trading_or_order_api_surface() -> None:
    routes = (ROOT / "app" / "api" / "routes.py").read_text(encoding="utf-8")
    decorators = [
        line.strip().lower()
        for line in routes.splitlines()
        if line.strip().startswith("@router.")
    ]
    assert not any("/order" in line or "/trade" in line for line in decorators)
    assert "trading_actions\": \"not_supported" in routes


def test_25_schema_15_migrates_to_16_with_data_preserved(tmp_path: Path) -> None:
    path = tmp_path / "schema15.sqlite"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT,applied_at TEXT)"
    )
    for index, (name, sql) in enumerate(MIGRATIONS[:15], start=1):
        for statement in _split_sql(sql):
            conn.execute(statement)
        conn.execute(
            "INSERT INTO schema_migrations VALUES (?,?,?)",
            (index, name, NOW.isoformat()),
        )
    conn.execute(
        """
        INSERT INTO market_context_snapshots(
          snapshot_id,symbol,revision,generated_at,data_as_of,refresh_mode,
          debug_payload_json,consumer_payload_json,ai_status,source_job_id,
          checksum,created_at,audit_status
        ) VALUES ('legacy','MNQ',1,?,?,?,'{"symbol":"MNQ"}','{}',
                  'NOT_REQUIRED',NULL,'hash',?,'ACTIVE')
        """,
        (NOW.isoformat(), NOW.isoformat(), "legacy", NOW.isoformat()),
    )
    conn.commit()
    conn.close()
    result = migrate_database(path)
    with connect_sqlite(path) as migrated:
        row = migrated.execute(
            "SELECT snapshot_id,research_link_status FROM market_context_snapshots"
        ).fetchone()
    assert result["schema_version"] == 16
    assert row["snapshot_id"] == "legacy"
    assert row["research_link_status"] == "NOT_REQUIRED"


def test_26_temporal_quarantine_is_non_destructive(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    facts = MarketFactRepository(settings)
    facts.upsert_economic_event(
        {
            "event_id": "future",
            "name": "Future CPI",
            "country": "US",
            "category": "CPI",
            "date": "2099-01-01",
            "time_utc": "2099-01-01T13:30:00Z",
        },
        "future",
    )
    records = facts.economic_event_records()
    TemporalValidationService(settings, clock=lambda: NOW).audit_economic_events(records)
    with connect_sqlite(settings.database_path) as conn:
        source_count = conn.execute(
            "SELECT COUNT(*) FROM economic_events_history"
        ).fetchone()[0]
        quarantine_count = conn.execute(
            "SELECT COUNT(*) FROM temporal_quarantine"
        ).fetchone()[0]
    assert source_count == 1
    assert quarantine_count == 1


def test_27_old_snapshots_remain_immutable_after_quarantine(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    snapshot = base_snapshot(settings)
    before = snapshot["checksum"]
    TemporalValidationService(settings, clock=lambda: NOW).audit_economic_events([])
    restored = MarketContextSnapshotRepository(settings).get(snapshot["snapshot_id"])
    assert restored is not None
    assert restored["checksum"] == before
    assert restored["debug_payload"] == snapshot["debug_payload"]


def _research_counts(settings: Settings, run_id: str) -> tuple[int, int, int]:
    with connect_sqlite(settings.database_path) as conn:
        claims = conn.execute(
            "SELECT COUNT(*) FROM research_claims WHERE research_run_id=?",
            (run_id,),
        ).fetchone()[0]
        evidence = conn.execute(
            """
            SELECT COUNT(*) FROM research_evidence WHERE claim_id IN(
              SELECT claim_id FROM research_claims WHERE research_run_id=?
            )
            """,
            (run_id,),
        ).fetchone()[0]
        facts = conn.execute(
            """
            SELECT COUNT(*) FROM market_facts WHERE fact_key IN(
              SELECT 'research:' || claim_id FROM research_claims
              WHERE research_run_id=?
            )
            """,
            (run_id,),
        ).fetchone()[0]
    return int(claims), int(evidence), int(facts)
