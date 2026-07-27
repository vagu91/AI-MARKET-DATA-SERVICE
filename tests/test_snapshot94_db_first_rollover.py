from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.infrastructure.persistence.schema import DB_FIRST_EVENT_COVERAGE_SCHEMA
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
COMPLETE_EMPTY_PROOF = {
    "request_succeeded": True,
    "scope_match": True,
    "pagination_complete": True,
    "parsing_succeeded": True,
    "records_valid": True,
    "expected_sources_complete": True,
    "authentic_empty": True,
}


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
    with pytest.raises(ValueError, match="complete_positive_proof"):
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
        "proof": COMPLETE_EMPTY_PROOF,
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
        coverage_proof = COMPLETE_EMPTY_PROOF

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
            proof=COMPLETE_EMPTY_PROOF,
            valid_until=NOW + timedelta(days=1),
            next_revision_check_at=NOW + timedelta(days=1),
        )
    calls: list[dict[str, object]] = []

    def acquire(**kwargs: object) -> list[object]:
        calls.append(kwargs)
        return []

    acquire.coverage_proof = COMPLETE_EMPTY_PROOF  # type: ignore[attr-defined]

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
    news = fixture["news_expectations"]
    assert sum(news["live_exclusion_reasons"].values()) == 100
    assert sum(news["live_sources"].values()) == 100


@pytest.mark.parametrize(
    "missing_proof",
    [
        "request_succeeded",
        "scope_match",
        "pagination_complete",
        "parsing_succeeded",
        "records_valid",
        "expected_sources_complete",
        "authentic_empty",
    ],
)
def test_verified_empty_fails_closed_for_every_missing_proof(
    tmp_path: Path,
    missing_proof: str,
) -> None:
    proof = {**COMPLETE_EMPTY_PROOF, missing_proof: False}
    repo = EventCalendarCoverageRepository(cfg(tmp_path), clock=lambda: NOW)
    with pytest.raises(ValueError):
        repo.record_day(
            date(2026, 7, 27),
            provider_name="fixture",
            query_scope="country=US",
            window_start=NOW - timedelta(hours=6),
            window_end=NOW,
            status="VERIFIED_EMPTY",
            record_count=0,
            provider_called=True,
            scope_verified=proof["scope_match"],
            proof=proof,
            valid_until=NOW + timedelta(hours=1),
        )


def test_logical_coverage_key_deduplicates_changing_window_end(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    repo = EventCalendarCoverageRepository(settings, clock=lambda: NOW)
    base = {
        "provider_name": "fixture",
        "query_scope": "country=US",
        "status": "VERIFIED_EMPTY",
        "record_count": 0,
        "provider_called": True,
        "scope_verified": True,
        "proof": COMPLETE_EMPTY_PROOF,
        "valid_until": NOW + timedelta(hours=2),
    }
    repo.record_day(
        date(2026, 7, 27),
        window_start=NOW - timedelta(hours=5),
        window_end=NOW - timedelta(minutes=1),
        **base,
    )
    repo.record_day(
        date(2026, 7, 27),
        window_start=NOW - timedelta(hours=5),
        window_end=NOW,
        **base,
    )
    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM event_calendar_coverage"
        ).fetchone()[0] == 1


def test_policy_or_contract_change_invalidates_prior_coverage(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    repo = EventCalendarCoverageRepository(settings, clock=lambda: NOW)
    repo.record_day(
        date(2026, 7, 27),
        provider_name="fixture",
        query_scope="country=US",
        window_start=NOW - timedelta(hours=5),
        window_end=NOW,
        status="VERIFIED_EMPTY",
        record_count=0,
        provider_called=True,
        scope_verified=True,
        proof=COMPLETE_EMPTY_PROOF,
        valid_until=NOW + timedelta(hours=2),
        contract_version="contract-v1",
        policy_version="policy-v1",
    )
    assert repo.missing_dates(
        [date(2026, 7, 27)],
        provider_name="fixture",
        query_scope="country=US",
        now=NOW,
        contract_version="contract-v2",
        policy_version="policy-v1",
    ) == [date(2026, 7, 27)]
    assert repo.missing_dates(
        [date(2026, 7, 27)],
        provider_name="fixture",
        query_scope="country=US",
        now=NOW,
        contract_version="contract-v1",
        policy_version="policy-v2",
    ) == [date(2026, 7, 27)]


def test_two_coverage_writers_cannot_create_logical_duplicates(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)

    def write() -> bool:
        return EventCalendarCoverageRepository(
            settings,
            clock=lambda: NOW,
        ).record_day(
            date(2026, 7, 27),
            provider_name="fixture",
            query_scope="country=US",
            window_start=NOW - timedelta(hours=5),
            window_end=NOW,
            status="VERIFIED_EMPTY",
            record_count=0,
            provider_called=True,
            scope_verified=True,
            proof=COMPLETE_EMPTY_PROOF,
            valid_until=NOW + timedelta(hours=2),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: write(), range(2)))
    with connect_sqlite(settings.database_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM event_calendar_coverage"
        ).fetchone()[0]
    assert count == 1
    assert sorted(results) == [False, True]


def test_populated_schema21_migrates_with_empty_conservative_ledger(
    tmp_path: Path,
) -> None:
    database = tmp_path / "schema21-populated.sqlite"
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
        for version, (name, sql) in enumerate(MIGRATIONS[:21], start=1):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)",
                (version, name, NOW.isoformat()),
            )
        conn.execute(
            """
            INSERT INTO economic_events_history(
              event_id,event_key,country,name,date,status,created_at,updated_at
            ) VALUES ('existing','existing','US','Existing','2026-07-24',
                      'AWAITING_ACTUAL',?,?)
            """,
            (NOW.isoformat(), NOW.isoformat()),
        )
        conn.commit()
    migrate_database(database)
    with connect_sqlite(database) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM economic_events_history"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM event_calendar_coverage"
        ).fetchone()[0] == 0


def test_schema22_crash_mid_migration_rolls_back_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.infrastructure.persistence import migrations

    database = tmp_path / "schema21-crash.sqlite"
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
        for version, (name, sql) in enumerate(MIGRATIONS[:21], start=1):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)",
                (version, name, NOW.isoformat()),
            )
        conn.execute("PRAGMA user_version=21")
        conn.commit()
    broken = DB_FIRST_EVENT_COVERAGE_SCHEMA + (
        "\nCREATE TABLE crash_probe(id INTEGER);\n"
        "THIS IS NOT VALID SQL;\n"
    )
    monkeypatch.setattr(
        migrations,
        "MIGRATIONS",
        (*MIGRATIONS[:21], ("022_crash_probe", broken)),
    )
    with pytest.raises(Exception):
        migrations.migrate_database(database)
    with connect_sqlite(database) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 21
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "event_calendar_coverage" not in tables
        assert "crash_probe" not in tables


@pytest.mark.parametrize(
    "mutation",
    [
        {"scope_match": False},
        {"pagination_complete": False},
        {"parsing_succeeded": False},
        {"records_valid": False},
        {"expected_sources_complete": False},
        {"request_succeeded": False},
        {"authentic_empty": False},
    ],
    ids=[
        "wrong-scope",
        "next-page",
        "parse-dropped-all",
        "all-quarantined-or-missing-fields",
        "source-not-called",
        "timeout-rate-limit-disabled-or-credential",
        "zero-not-authentic",
    ],
)
def test_unproven_empty_stays_partial_and_is_not_suppressed(
    tmp_path: Path,
    mutation: dict[str, bool],
) -> None:
    settings = cfg(tmp_path)
    proof = {**COMPLETE_EMPTY_PROOF, **mutation}

    class Acquire:
        coverage_proof = proof
        last_provider_results: list[object] = []

        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, **_: object) -> list[object]:
            self.calls += 1
            return []

    acquire = Acquire()
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    first = scheduler._seed_canonical_schedule_gaps_unleased(
        schedule_acquire=acquire,
        now=NOW,
    )
    second = scheduler._seed_canonical_schedule_gaps_unleased(
        schedule_acquire=acquire,
        now=NOW,
    )
    assert first["status"] == second["status"] == "PARTIAL"
    assert acquire.calls == 2
    assert first["provider_calls_executed"] == 1
    assert first["coverage_metadata_writes"] > 0
