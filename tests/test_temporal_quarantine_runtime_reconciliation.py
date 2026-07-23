from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from app.api import routes
from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.services.agentic_research_runtime import AgenticResearchRuntime
from app.services.ai_research_job_service import AIResearchJobService
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_fact_repository import MarketFactRepository
from app.services.research_gap_manifest import ResearchGapManifestBuilder
from app.services.temporal_domain_service import canonical_event_key
from app.services.temporal_validation_service import TemporalValidationService


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "source_policy.json"
IMPOSSIBLE_DATE = "2099-07-14"
IMPOSSIBLE_RELEASE = "2099-07-14T12:30:00+00:00"
REAL_KEYS = (
    "event:bab517f0abd336898429f573",
    "event:e2fcd0a2f5062f15d58612de",
)


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "economic_event_max_future_days": 550,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def impossible_event(*, event_key: str = REAL_KEYS[0]) -> dict[str, Any]:
    return {
        "event_id": "evt-cpi",
        "event_key": event_key,
        "canonical_event_key": event_key,
        "name": "Consumer Price Index",
        "country": "US",
        "category": "CPI",
        "date": IMPOSSIBLE_DATE,
        "time_utc": IMPOSSIBLE_RELEASE,
        "release_at": IMPOSSIBLE_RELEASE,
        "impact": "HIGH",
        "status": "PRE_RELEASE",
        "temporal_status": "PRE_RELEASE",
        "source": "BLS",
        "source_url": "https://www.bls.gov/cpi/",
        "reliability": 0.99,
        "enrichment": {
            "field_lineage": {
                "forecast": {
                    "source": "BLS",
                    "source_url": "https://www.bls.gov/cpi/",
                }
            }
        },
    }


def realistic_event(release: datetime, *, event_id: str) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "name": "Consumer Price Index",
        "country": "US",
        "category": "CPI",
        "date": release.date().isoformat(),
        "time_utc": release.isoformat(),
        "release_at": release.isoformat(),
        "impact": "HIGH",
        "source": "BLS",
        "source_url": "https://www.bls.gov/cpi/",
        "reliability": 0.99,
    }


def context_with_impossible_event() -> dict[str, Any]:
    item = impossible_event()
    return {
        "symbol": "MNQ",
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "market_schedule": {
            "status": "AVAILABLE",
            "context_date": datetime.now(UTC).date().isoformat(),
            "market_session_status": "open",
        },
        "event_calendar": {
            "status": "AVAILABLE",
            "data_as_of": datetime.now(UTC).isoformat(),
            "critical_macro_events": [item],
            "fed_communications": [],
            "other_economic_events": [],
        },
        "events_today": [item],
        "event_windows": {
            "active": [item],
            "upcoming": [item],
            "event_risk_window_status": "PRE_EVENT",
        },
        "macro_snapshot": {},
        "risk_context": {},
        "nasdaq_context": {},
        "news_context": {},
        "rates_expectations": {},
        "positioning": {},
        "sentiment_context": {},
        "data_quality": {},
    }


def _create_schema(path: Path, version: int) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT,applied_at TEXT)"
        )
        for index, (name, sql) in enumerate(MIGRATIONS[:version], start=1):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)",
                (index, name, "2026-07-23T16:32:04+00:00"),
            )
        conn.execute(f"PRAGMA user_version={version}")
        conn.commit()


def _insert_legacy_impossible_rows(path: Path, keys: tuple[str, ...]) -> None:
    with sqlite3.connect(path) as conn:
        available = {
            row[1] for row in conn.execute("PRAGMA table_info(economic_events_history)")
        }
        base = impossible_event()
        for index, key in enumerate(keys):
            values: dict[str, Any] = {
                "event_id": "evt-cpi",
                "event_key": key,
                "country": "US",
                "category": "CPI",
                "name": "Consumer Price Index",
                "date": IMPOSSIBLE_DATE,
                "time_utc": IMPOSSIBLE_RELEASE,
                "impact": "HIGH",
                "source": "BLS",
                "source_url": "https://www.bls.gov/cpi/",
                "release_at": IMPOSSIBLE_RELEASE,
                "status": "PRE_RELEASE",
                "raw_payload_json": json.dumps(
                    {
                        **base,
                        "event_key": key,
                        "canonical_event_key": key,
                    },
                    sort_keys=True,
                ),
                "created_at": "2026-07-23T15:25:40+00:00",
                "updated_at": (
                    "2026-07-23T16:32:05+00:00"
                    if index
                    else "2026-07-23T15:25:40+00:00"
                ),
                "canonical_event_key": key,
                "event_kind": "scheduled_event",
                "temporal_status": "PRE_RELEASE",
                "field_lineage_json": json.dumps(
                    {"forecast": {"source": "BLS", "source_url": "https://www.bls.gov/cpi/"}}
                ),
                "temporal_audit_status": "ACTIVE",
                "temporal_invalid_reason": None,
            }
            columns = [column for column in values if column in available]
            conn.execute(
                f"INSERT INTO economic_events_history({','.join(columns)}) "
                f"VALUES ({','.join('?' for _ in columns)})",
                [values[column] for column in columns],
            )
        conn.commit()


def test_new_impossible_event_is_quarantined_immediately_and_idempotently(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    event = impossible_event()
    repository.upsert_economic_event(event, event_key=REAL_KEYS[0])
    repository.upsert_economic_event(event, event_key=REAL_KEYS[0])

    with connect_sqlite(cfg.database_path) as conn:
        row = conn.execute(
            "SELECT * FROM economic_events_history WHERE event_key=?",
            (REAL_KEYS[0],),
        ).fetchone()
        quarantine = conn.execute(
            "SELECT * FROM temporal_quarantine WHERE entity_key=?",
            (REAL_KEYS[0],),
        ).fetchall()
    assert row["status"] == "QUARANTINED"
    assert row["temporal_status"] == "QUARANTINED"
    assert row["temporal_audit_status"] == "QUARANTINED"
    assert row["temporal_invalid_reason"] == "EVENT_BEYOND_CONFIGURED_HORIZON"
    assert row["actual"] is None
    assert len(quarantine) == 1
    details = json.loads(quarantine[0]["details_json"])
    assert details["original_state"]["release_at"] == IMPOSSIBLE_RELEASE
    assert details["source"] == "BLS"
    assert details["lineage"]["forecast"]["source"] == "BLS"
    assert repository.economic_event_records(country="US") == []


def test_migration_15_to_17_backfills_without_deleting_history(tmp_path: Path) -> None:
    path = tmp_path / "schema15.sqlite"
    _create_schema(path, 15)
    _insert_legacy_impossible_rows(path, (REAL_KEYS[0],))
    result = migrate_database(path)
    with connect_sqlite(path) as conn:
        row = conn.execute("SELECT * FROM economic_events_history").fetchone()
        versions = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    assert result["schema_version"] == 18
    assert [item["version"] for item in versions][-2:] == [17, 18]
    assert row["event_key"] == REAL_KEYS[0]
    assert row["temporal_status"] == "QUARANTINED"


def test_migration_16_to_17_quarantines_both_real_keys_and_reopen_is_idempotent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "schema16.sqlite"
    _create_schema(path, 16)
    _insert_legacy_impossible_rows(path, REAL_KEYS)
    first = migrate_database(path)
    with connect_sqlite(path) as conn:
        rows = conn.execute(
            "SELECT event_key,temporal_status,temporal_invalid_reason "
            "FROM economic_events_history ORDER BY event_key"
        ).fetchall()
        audit_before = conn.execute(
            "SELECT quarantine_id,entity_key FROM temporal_quarantine ORDER BY entity_key"
        ).fetchall()
        runs_before = conn.execute(
            "SELECT COUNT(*) FROM temporal_reconciliation_runs"
        ).fetchone()[0]
    second = migrate_database(path)
    with connect_sqlite(path) as conn:
        audit_after = conn.execute(
            "SELECT quarantine_id,entity_key FROM temporal_quarantine ORDER BY entity_key"
        ).fetchall()
        runs_after = conn.execute(
            "SELECT COUNT(*) FROM temporal_reconciliation_runs"
        ).fetchone()[0]
    assert first["applied"] == [
        "017_temporal_quarantine_runtime_reconciliation",
        "018_invalid_source_quarantine_and_reconciliation",
    ]
    assert len(rows) == 2
    assert {row["event_key"] for row in rows} == set(REAL_KEYS)
    assert all(row["temporal_status"] == "QUARANTINED" for row in rows)
    assert all(
        row["temporal_invalid_reason"] == "EVENT_BEYOND_CONFIGURED_HORIZON"
        for row in rows
    )
    assert [tuple(row) for row in audit_before] == [tuple(row) for row in audit_after]
    assert runs_before == runs_after == 1
    assert second["applied"] == []


def test_quarantined_events_are_excluded_from_snapshot_consumer_and_gap_manifest(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.sqlite"
    _create_schema(path, 16)
    _insert_legacy_impossible_rows(path, REAL_KEYS)
    migrate_database(path)
    cfg = settings(tmp_path)

    snapshots = MarketContextSnapshotRepository(cfg)
    stored = snapshots.save_next(
        symbol="MNQ",
        refresh_mode="offline_test",
        debug_payload=context_with_impossible_event(),
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    debug_operational = {
        key: stored["debug_payload"].get(key)
        for key in (
            "event_calendar",
            "events_today",
            "events_today_context",
            "event_windows",
        )
    }
    consumer_operational = stored["consumer_payload"].get("event_risk")
    assert IMPOSSIBLE_DATE not in json.dumps(debug_operational, sort_keys=True)
    assert IMPOSSIBLE_DATE not in json.dumps(consumer_operational, sort_keys=True)
    assert stored["consumer_payload"]["event_risk"]["next_critical_event"] is None
    assert stored["consumer_payload"]["schema_version"] == "2.1"
    assert len(json.dumps(stored["consumer_payload"]).encode("utf-8")) < 90 * 1024
    audit_items = (
        stored["debug_payload"]["audit"]["temporal_quarantine"]["items"]
    )
    assert set(REAL_KEYS) <= {item["entity_key"] for item in audit_items}

    with connect_sqlite(cfg.database_path) as conn:
        row = conn.execute(
            "SELECT debug_payload_json,consumer_payload_json FROM market_context_snapshots"
        ).fetchone()
    persisted_debug = json.loads(row["debug_payload_json"])
    persisted_consumer = json.loads(row["consumer_payload_json"])
    assert persisted_debug == stored["debug_payload"]
    assert persisted_consumer == stored["consumer_payload"]
    assert IMPOSSIBLE_DATE not in json.dumps(
        persisted_debug["event_calendar"],
        sort_keys=True,
    )
    assert IMPOSSIBLE_DATE not in json.dumps(
        persisted_consumer["event_risk"],
        sort_keys=True,
    )

    manifest = ResearchGapManifestBuilder(cfg).build(
        snapshot=stored,
        components={"event_calendar": context_with_impossible_event()["event_calendar"]},
    )
    macro = next(item for item in manifest["items"] if item["topic"] == "macro_events")
    assert macro["required_action"] == "AGENT_RESEARCH"
    assert macro["deterministic_status"] in {"MISSING", "NEEDS_AGENT_RESEARCH"}


class _CapturingExecutor:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def execute_step(self, **kwargs: Any) -> dict[str, Any]:
        self.payloads.append(
            {
                "job": kwargs["job"],
                "context": kwargs["context"],
            }
        )
        if kwargs["step_name"] == "VALIDATE":
            return {"status": "NO_DATA", "claims": [], "_tool_events": []}
        return {"status": "COMPLETED", "_tool_events": []}


def test_child_agent_prompt_context_never_receives_impossible_event(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, research_budget_mode="observe")
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MACRO_EVENTS_RESEARCH",
        symbol="MNQ",
        correlation_id="temporal-prompt",
        request_payload={
            "database_context": {
                "critical_macro_events": [impossible_event()],
            },
            "missing_fields": ["current_events"],
        },
    )
    assert created
    executor = _CapturingExecutor()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    AgenticResearchRuntime(cfg).run(job, workspace, executor, 30)
    assert executor.payloads
    assert IMPOSSIBLE_DATE not in json.dumps(executor.payloads, default=str)


def test_realistic_future_and_past_events_are_not_quarantined(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    now = datetime.now(UTC).replace(microsecond=0)
    future = realistic_event(now + timedelta(days=30), event_id="evt-future")
    past = {
        **realistic_event(now - timedelta(days=2), event_id="evt-past"),
        "actual": "0.2",
    }
    repository.upsert_economic_event(future, canonical_event_key(future))
    repository.upsert_economic_event(past, canonical_event_key(past))
    records = repository.economic_event_records(country="US")
    assert {record["event_id"] for record in records} == {"evt-future", "evt-past"}
    assert {record["temporal_status"] for record in records} == {
        "PRE_RELEASE",
        "RELEASED",
    }
    assert TemporalValidationService(cfg).quarantine_summary()["total"] == 0


def test_refresh_false_is_zero_call_zero_enqueue_and_zero_write(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    MarketContextSnapshotRepository(cfg).save_next(
        symbol="MNQ",
        refresh_mode="seed",
        debug_payload={
            **context_with_impossible_event(),
            "event_calendar": {
                "critical_macro_events": [],
                "fed_communications": [],
                "other_economic_events": [],
            },
            "events_today": [],
            "event_windows": {},
        },
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    before = cfg.database_path.read_bytes()
    result = asyncio.run(
        routes.market_context_mnq(
            refresh="false",
            view="consumer",
            macro_service=None,
            event_service=None,
            event_window_service=None,
            nasdaq_service=None,
            enrichment_orchestrator=SimpleNamespace(settings=cfg),
        )
    )
    after = cfg.database_path.read_bytes()
    with connect_sqlite(cfg.database_path) as conn:
        jobs = conn.execute("SELECT COUNT(*) FROM ai_research_jobs").fetchone()[0]
    assert result["schema_version"] == "2.1"
    assert before == after
    assert jobs == 0


def test_quarantine_diagnostics_are_aggregate_and_non_mutating(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    repository.upsert_economic_event(impossible_event(), REAL_KEYS[0])
    before = cfg.database_path.read_bytes()
    summary = repository.db_summary()["temporal_quarantine"]
    after = cfg.database_path.read_bytes()
    assert summary["total"] == 1
    assert summary["by_domain_reason"] == [
        {
            "domain": "macro_calendar",
            "reason_code": "EVENT_BEYOND_CONFIGURED_HORIZON",
            "count": 1,
        }
    ]
    assert summary["last_detected_at"]
    assert summary["reconciliation_errors"] == []
    assert before == after
