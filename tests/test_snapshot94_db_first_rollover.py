from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.providers.xtb_economic_calendar_provider import normalize_xtb_events
from app.services.event_calendar_coverage_repository import (
    EventCalendarCoverageRepository,
)
from app.services.market_session_service import build_session_aware_schedule
from app.services.research_scheduler_service import ResearchSchedulerService


NOW = datetime(2026, 7, 27, 9, 40, tzinfo=UTC)
FIXTURE = Path(__file__).parent / "fixtures" / (
    "snapshot_94_monday_live_blockers_redacted.json"
)


def cfg(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "snapshot94.sqlite",
    )


@pytest.mark.parametrize("source_version", [1, 20, 21, 22])
def test_schema_22_upgrade_matrix_is_additive_and_idempotent(
    tmp_path: Path,
    source_version: int,
) -> None:
    database = tmp_path / f"schema-{source_version}.sqlite"
    with connect_sqlite(database) as conn:
        conn.execute(
            """
            CREATE TABLE schema_migrations(
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              applied_at TEXT NOT NULL
            )
            """
        )
        for version, (name, sql) in enumerate(
            MIGRATIONS[:source_version],
            start=1,
        ):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)",
                (version, name, NOW.isoformat()),
            )
        conn.execute(f"PRAGMA user_version={source_version}")
        conn.commit()

    first = migrate_database(database)
    second = migrate_database(database)

    assert first["schema_version"] == second["schema_version"] == 22
    with connect_sqlite(database) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 22
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(economic_events_history)"
            ).fetchall()
        }
        assert {
            "completeness_status",
            "outcome_contract_json",
            "publication_grace_until",
            "next_revision_check_at",
            "removal_status",
            "removal_lineage_json",
        } <= columns


def test_verified_empty_requires_successful_scoped_provider_call(
    tmp_path: Path,
) -> None:
    repo = EventCalendarCoverageRepository(cfg(tmp_path), clock=lambda: NOW)
    with pytest.raises(ValueError, match="successful_scoped_call"):
        repo.record_day(
            date(2026, 7, 26),
            provider_name="fixture",
            query_scope="country=US",
            window_start=NOW - timedelta(days=1),
            window_end=NOW,
            status="VERIFIED_EMPTY",
            record_count=0,
            provider_called=False,
            scope_verified=False,
        )


def test_coverage_noop_preserves_updated_at(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    repo = EventCalendarCoverageRepository(settings, clock=lambda: NOW)
    kwargs = {
        "provider_name": "fixture",
        "query_scope": "country=US",
        "window_start": datetime(2026, 7, 26, tzinfo=UTC),
        "window_end": datetime(2026, 7, 27, tzinfo=UTC),
        "status": "VERIFIED_EMPTY",
        "record_count": 0,
        "provider_called": True,
        "scope_verified": True,
        "valid_until": NOW + timedelta(days=2),
        "next_revision_check_at": NOW + timedelta(days=7),
        "lineage": {"request": "redacted"},
    }
    assert repo.record_day(date(2026, 7, 26), **kwargs) is True
    with connect_sqlite(settings.database_path) as conn:
        before = conn.execute(
            "SELECT updated_at FROM event_calendar_coverage"
        ).fetchone()["updated_at"]
    assert repo.record_day(date(2026, 7, 26), **kwargs) is False
    with connect_sqlite(settings.database_path) as conn:
        after = conn.execute(
            "SELECT updated_at FROM event_calendar_coverage"
        ).fetchone()["updated_at"]
    assert after == before


@pytest.mark.parametrize("shutdown_days", [1, 30, 365])
def test_restart_gap_inventory_spans_arbitrary_shutdowns(
    tmp_path: Path,
    shutdown_days: int,
) -> None:
    repo = EventCalendarCoverageRepository(cfg(tmp_path), clock=lambda: NOW)
    start = NOW.date() - timedelta(days=shutdown_days)
    missing = repo.missing_dates(
        [start, NOW.date()],
        provider_name="fixture",
        query_scope="country=US",
        now=NOW,
    )
    assert missing == [start, NOW.date()]


def test_valid_daily_coverage_suppresses_provider_calls(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    calls: list[dict[str, object]] = []

    class Acquire:
        last_provider_results: list[object] = []

        def __call__(self, **kwargs: object) -> list[object]:
            calls.append(kwargs)
            return []

    first = scheduler._seed_canonical_schedule_gaps_unleased(
        schedule_acquire=Acquire(),
        now=NOW,
    )
    second = scheduler._seed_canonical_schedule_gaps_unleased(
        schedule_acquire=Acquire(),
        now=NOW,
    )
    assert first["provider_calls"] == 1
    assert second["provider_calls"] == 0
    assert len(calls) == 1
    assert second["targeted_gap_dates"] == []


def test_only_unknown_day_is_reacquired(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    repo = EventCalendarCoverageRepository(settings, clock=lambda: NOW)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    previous_start = date(2026, 7, 20)
    for offset in range(8):
        day = previous_start + timedelta(days=offset)
        if day == date(2026, 7, 24):
            continue
        day_start = datetime.combine(day, datetime.min.time(), UTC)
        repo.record_day(
            day,
            provider_name="economic_calendar_composite",
            query_scope="country=US",
            window_start=day_start,
            window_end=min(day_start + timedelta(days=1), NOW),
            status="VERIFIED_EMPTY",
            record_count=0,
            provider_called=True,
            scope_verified=True,
            valid_until=NOW + timedelta(days=1),
            next_revision_check_at=NOW + timedelta(days=1),
        )
    calls: list[dict[str, object]] = []

    def acquire(**kwargs: object) -> list[object]:
        calls.append(kwargs)
        return []

    result = scheduler._seed_canonical_schedule_gaps_unleased(
        schedule_acquire=acquire,
        now=NOW,
    )
    assert result["targeted_gap_dates"] == ["2026-07-24"]
    assert len(calls) == 1
    assert calls[0]["start"] == datetime(2026, 7, 24, 4, tzinfo=UTC)
    assert calls[0]["end"] == datetime(2026, 7, 25, 4, tzinfo=UTC)


def test_xtb_normalization_preserves_exact_actual_semantics() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    raw = [
        {
            "id": "146392",
            "countryCode": "US",
            "date": "2026-07-24",
            "timeShortFormat": "10:00",
            "timezoneOffset": -14400,
            "impact": 3,
            "title": "Vendita case nuove",
            "period": "Giugno",
            "forecast": 610,
            "previous": 618,
            "current": 628,
            "numericalEvent": True,
        },
        {
            "id": "146945",
            "countryCode": "US",
            "date": "2026-07-24",
            "timeShortFormat": "09:45",
            "timezoneOffset": -14400,
            "impact": 3,
            "title": "Indice PMI dei servizi",
            "period": "Luglio",
            "forecast": 52,
            "previous": 51.2,
            "current": 53.6,
            "numericalEvent": True,
        },
    ]
    rows, rejected = normalize_xtb_events(
        raw,
        retrieved_at=NOW,
        minimum_impact=0,
        lookahead_days=14,
        lookback_days=7,
    )
    expected = {
        item["occurrence_id"]: item for item in fixture["actuals"]
    }
    assert rejected == 0
    assert len(rows) == 2
    for row in rows:
        values = expected[row["occurrence_id"]]
        assert row["actual"] == values["actual"]
        assert row["reference_period"] == values["reference_period"]
        assert row["forecast"] == values["forecast"]
        assert row["previous"] == values["previous"]
        assert row["lineage"]["actual"]["source_field"] == "current"


def test_monday_0540_et_schedule_is_partial_not_quarantined() -> None:
    schedule = build_session_aware_schedule(
        {
            "cme_calendar": {
                "status": "timeout",
                "official_document_discovered": False,
                "official_schedule_parsed": False,
            }
        },
        now=NOW,
    )
    assert schedule["status"] == "PARTIAL"
    assert schedule["validation"]["status"] == "partial"
    assert schedule["nasdaq_cash_session"]["session_state"] == "CLOSED"
    assert schedule["nasdaq_cash_session"]["phase"] == "PREMARKET"
    assert schedule["mnq_session"]["session_state"] == "GLOBEX_OPEN"
    assert schedule["mnq_session"]["calendar_crosscheck_status"] == "timeout"


def test_snapshot94_fixture_records_lossless_forensic_baseline() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    before = fixture["forensic_before"]
    assert before["snapshot_revision"] == 94
    assert before["news_candidates"] == 100
    assert before["full_sync_sections"] == 17
    assert before["full_sync_bytes"] == 1_007_063
    assert fixture["expected_after"]["calendar_counts"] == {
        "previous": 14,
        "current": 3,
        "next": 25,
    }
