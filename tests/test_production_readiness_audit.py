from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.database_maintenance import run_database_maintenance
from app.infrastructure.persistence.migrations import migrate_database
from app.services.market_fact_repository import MarketFactRepository
from app.services.multi_source_runtime_service import MultiSourceRuntimeService


def settings(tmp_path, **overrides) -> Settings:
    return Settings(_env_file=None, database_path=tmp_path / "market.sqlite", **overrides)


def macro_fact(*, source: str, value: str, retrieved_at: str, valid_until: str) -> dict:
    return {
        "fact_key": f"{source}:CUSR0000SA0:latest:official_macro_latest",
        "fact_type": "official_macro_latest",
        "country": "US",
        "category": "CUSR0000SA0",
        "event_name": "Consumer Price Index",
        "value": value,
        "source": source,
        "provider_type": "API",
        "reliability": 0.93,
        "confidence": 0.93,
        "retrieved_at": retrieved_at,
        "release_at": "2026-06-01T00:00:00Z",
        "valid_until": valid_until,
        "next_refresh_at": valid_until,
    }


def test_official_macro_latest_has_one_canonical_row_and_prefers_direct_source(tmp_path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    future = "2099-01-02T00:00:00Z"
    repository.upsert_fact(
        macro_fact(source="BLS", value="333.9", retrieved_at="2099-01-01T00:00:00Z", valid_until=future)
    )
    repository.upsert_fact(
        macro_fact(source="BLS via FRED", value="333.8", retrieved_at="2099-01-01T01:00:00Z", valid_until=future)
    )

    rows = repository.search_facts(category="CUSR0000SA0")
    assert len(rows) == 1
    assert rows[0]["fact_key"] == "US:CUSR0000SA0:latest:official_macro_latest"
    assert rows[0]["source"] == "BLS"
    assert rows[0]["value"] == "333.9"

    restarted = MarketFactRepository(cfg)
    assert restarted.get_valid_facts_by_type("official_macro_latest")[0]["value"] == "333.9"


def test_expired_persisted_fact_is_not_counted_as_active(tmp_path) -> None:
    cfg = settings(tmp_path)
    repository = MarketFactRepository(cfg)
    now = datetime.now(UTC)
    repository.upsert_fact(
        {
            "fact_key": "fresh",
            "fact_type": "test",
            "country": "US",
            "retrieved_at": now.isoformat(),
            "valid_until": (now + timedelta(hours=1)).isoformat(),
        }
    )
    repository.upsert_fact(
        {
            "fact_key": "expired",
            "fact_type": "test",
            "country": "US",
            "retrieved_at": (now - timedelta(hours=2)).isoformat(),
            "valid_until": (now - timedelta(hours=1)).isoformat(),
        }
    )

    summary = repository.db_summary()["market_facts"]
    assert summary == {
        "total": 2,
        "active": 1,
        "usable_active": 1,
        "persisted_active": 2,
        "stale": 1,
    }
    assert repository.active_count() == 1
    assert repository.coverage()["active_facts"] == 1


def test_database_retention_removes_only_rows_beyond_category_policy(tmp_path) -> None:
    cfg = settings(
        tmp_path,
        market_facts_retention_days=1,
        economic_events_history_retention_days=1,
        snapshot_history_retention_days=1,
    )
    MarketFactRepository(cfg)
    old = (datetime.now(UTC) - timedelta(days=10)).replace(microsecond=0).isoformat()
    recent = datetime.now(UTC).replace(microsecond=0).isoformat()
    with connect_sqlite(cfg.database_path) as conn:
        conn.execute(
            "INSERT INTO market_facts(fact_key,fact_type,retrieved_at,valid_until,status,created_at,updated_at) VALUES ('old','test',?,?,'active',?,?)",
            (old, old, old, old),
        )
        conn.execute(
            "INSERT INTO market_facts(fact_key,fact_type,retrieved_at,valid_until,status,created_at,updated_at) VALUES ('recent','test',?,?,'active',?,?)",
            (recent, recent, recent, recent),
        )
        conn.execute(
            "INSERT INTO economic_events_history(event_key,date,created_at,updated_at) VALUES ('old-event',?,?,?)",
            (old[:10], old, old),
        )
        conn.execute(
            "INSERT INTO fed_expectation_snapshots(snapshot_key,retrieved_at,payload_json,checksum,created_at) VALUES ('old-fed',?,'{}','x',?)",
            (old, old),
        )
        conn.execute(
            "INSERT INTO risk_context_snapshots(snapshot_key,retrieved_at,status,payload_json,checksum,created_at) VALUES ('old-risk',?,'old','{}','x',?)",
            (old, old),
        )
        conn.commit()

    report = run_database_maintenance(cfg, dry_run=False)
    assert report["purges"]["market_facts"]["deleted_rows"] == 1
    assert report["purges"]["economic_events_history"]["deleted_rows"] == 1
    assert report["purges"]["snapshot_history"]["deleted_rows"] == 2
    with connect_sqlite(cfg.database_path) as conn:
        assert conn.execute("SELECT fact_key FROM market_facts").fetchall()[0]["fact_key"] == "recent"


def test_repeated_migration_check_is_read_only_after_schema_is_current(tmp_path, monkeypatch) -> None:
    cfg = settings(tmp_path)
    migrate_database(cfg.database_path)
    writes: list[str] = []
    original_connect = sqlite3.connect

    def traced_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connection.set_trace_callback(
            lambda statement: writes.append(statement)
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))
            else None
        )
        return connection

    monkeypatch.setattr(sqlite3, "connect", traced_connect)
    migrate_database(cfg.database_path)
    assert writes == []


@pytest.mark.asyncio
async def test_multi_source_refresh_false_is_read_only(tmp_path, monkeypatch) -> None:
    cfg = settings(tmp_path)
    service = MultiSourceRuntimeService(cfg)
    writes: list[str] = []
    original_connect = sqlite3.connect

    def traced_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connection.set_trace_callback(
            lambda statement: writes.append(statement)
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))
            else None
        )
        return connection

    monkeypatch.setattr(sqlite3, "connect", traced_connect)
    snapshot = await service.snapshot(refresh="false")
    assert snapshot["data_quality"]["provider_calls"] == 0
    assert writes == []
