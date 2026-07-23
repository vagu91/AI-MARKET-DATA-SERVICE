from __future__ import annotations

import hashlib
import json
import socket
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.services.ai_research_job_service import AIResearchJobService
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.research_runtime_repository import (
    ResearchPersistenceError,
    ResearchRuntimeRepository,
)


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "source_policy.json"
LIVE_FIXTURE = ROOT / "tests" / "fixtures" / "persist_live_run_20260723.json"
REFERENCE_NOW = datetime(2026, 7, 23, 12, 29, 51, tzinfo=UTC)


def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        source_policy_path=POLICY,
        ai_job_workspace_root=tmp_path / "jobs",
    )


class OfflinePersistRepository(ResearchRuntimeRepository):
    """PERSIST-only harness: validated evidence is replayed without a gateway."""

    def _validated_evidence(
        self,
        semantics: str,
        items: list[dict[str, Any]],
        _observed_sources: list[dict[str, Any]],
        _acquired_sources: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        for item in items:
            if item.get("accepted") is not True:
                warnings.append("evidence_mismatch")
                continue
            url = str(item["canonical_url"])
            text = str(item["evidence_text"])
            domain = (urlsplit(url).hostname or "").lower().removeprefix("www.")
            checksum = hashlib.sha256(text.encode("utf-8")).hexdigest()
            rows.append(
                {
                    "query": item.get("query"),
                    "source_url": url,
                    "canonical_url": url,
                    "publisher": item.get("publisher"),
                    "source_domain": domain,
                    "source_tier": 1,
                    "evidence_text": text,
                    "published_at": item.get("published_at"),
                    "retrieved_at": REFERENCE_NOW.isoformat(),
                    "redirect_url": None,
                    "source_status": "VERIFIED",
                    "independent_source_group": f"domain:{domain}",
                    "content_checksum": checksum,
                    "source_content_hash": checksum,
                    "tool_event_id": None,
                    "source_id": None,
                    "verification_id": None,
                    "verification_method": "offline_replay",
                    "verification_reason": "validated_live_fixture",
                    "verification_score": 1.0,
                }
            )
        return rows, warnings


def make_run(
    cfg: Settings,
    identity: str,
    *,
    repository_type: type[ResearchRuntimeRepository] = OfflinePersistRepository,
) -> tuple[ResearchRuntimeRepository, dict[str, Any]]:
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id=identity,
        request_payload={"database_context": {"data_as_of": REFERENCE_NOW.isoformat()}},
        force=True,
    )
    assert created
    repository = repository_type(cfg, now=lambda: REFERENCE_NOW)
    run = repository.ensure_run(
        job,
        "MNQ_MARKET_RESEARCH",
        "mnq_market_research_v2",
    )
    return repository, run


def supported_claim(index: int) -> dict[str, Any]:
    return {
        "claim_ref": f"supported-{index}",
        "topic": "macro" if index == 1 else "events",
        "field_semantics": "official_calendar_event",
        "value": f"Official future event {index}",
        "event_at": f"2026-07-{28 + index:02d}T14:00:00+00:00",
        "confidence": 0.95,
        "topic_status": "SUPPORTED",
        "evidence": [
            {
                "canonical_url": (
                    "https://www.bea.gov/news/schedule"
                    if index == 1
                    else "https://www.bls.gov/schedule/2026/home.htm"
                ),
                "publisher": "Official publisher",
                "evidence_text": f"Official future event {index}",
                "accepted": True,
            }
        ],
    }


def begin_persist(
    repository: ResearchRuntimeRepository,
    run: dict[str, Any],
    count: int,
) -> str:
    step, execute = repository.begin_step(
        str(run["run_id"]),
        "PERSIST",
        7,
        {"claim_count": count},
        backend="service",
        tool="sqlite",
    )
    assert execute
    return str(step["step_id"])


def atomic_counts(cfg: Settings, run_id: str) -> dict[str, int]:
    with connect_sqlite(cfg.database_path) as conn:
        return {
            "claims": int(
                conn.execute(
                    "SELECT COUNT(*) FROM research_claims WHERE research_run_id=?",
                    (run_id,),
                ).fetchone()[0]
            ),
            "evidence": int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM research_evidence
                    WHERE claim_id IN (
                      SELECT claim_id FROM research_claims WHERE research_run_id=?
                    )
                    """,
                    (run_id,),
                ).fetchone()[0]
            ),
            "facts": int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM market_facts
                    WHERE fact_key IN (
                      SELECT 'research:' || claim_id
                      FROM research_claims WHERE research_run_id=?
                    )
                    """,
                    (run_id,),
                ).fetchone()[0]
            ),
        }


def assert_rolled_back(
    cfg: Settings,
    repository: ResearchRuntimeRepository,
    run_id: str,
) -> None:
    assert atomic_counts(cfg, run_id) == {"claims": 0, "evidence": 0, "facts": 0}
    restored = repository.get_run(run_id)
    assert restored is not None and restored["status"] == "FAILED"
    assert restored["completed_at"]
    assert "accepted_count" not in restored["result"]
    persist = next(step for step in restored["steps"] if step["step_name"] == "PERSIST")
    assert persist["status"] == "FAILED"
    assert persist["completed_at"]
    assert persist["diagnostic"]["transaction_outcome"] == "ROLLED_BACK"
    assert persist["attempt_history"][-1]["status"] == "FAILED"


def test_exact_live_persist_replay_is_partial_and_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = json.loads(LIVE_FIXTURE.read_text(encoding="utf-8"))
    assert fixture["run_id"] == "rrun-f88713ec-3e4e-4655-a117-e3274fa53cb7"
    assert fixture["job_id"] == "airj-783324ef-c0ed-4fb4-ba11-6c064f1946bf"
    assert fixture["validate_status"] == "PARTIAL"
    assert len(fixture["claims"]) == 12
    monkeypatch.setattr(
        socket,
        "socket",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("network forbidden during PERSIST replay")
        ),
    )
    cfg = settings(tmp_path)
    repository, run = make_run(cfg, "exact-live-persist-replay")
    step_id = begin_persist(repository, run, len(fixture["claims"]))

    result = repository.persist_claims(
        repository.get_run(str(run["run_id"])) or run,
        fixture["claims"],
        step_id=step_id,
    )

    assert result["status"] == "PARTIAL"
    assert result["candidate_count"] == 12
    assert result["accepted_count"] == 4
    assert result["persisted_count"] == result["read_back_count"] == 4
    assert result["valid_not_applicable_topics"] == []
    assert {item["claim_ref"] for item in result["accepted_claims"]} >= {
        "candidate-1",
        "candidate-2",
        "candidate-4",
        "candidate-7",
    }
    assert atomic_counts(cfg, str(run["run_id"])) == {
        "claims": 12,
        "evidence": 5,
        "facts": 4,
    }


def test_persist_success_commits_counters_and_terminal_step(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, run = make_run(cfg, "persist-success")
    claims = [supported_claim(1), supported_claim(2)]
    step_id = begin_persist(repository, run, len(claims))

    result = repository.persist_claims(run, claims, step_id=step_id)

    assert result["persisted_count"] == result["read_back_count"] == 2
    restored = repository.get_run(str(run["run_id"]))
    assert restored is not None and restored["status"] == "PARTIAL"
    assert restored["completed_at"]
    persist = next(step for step in restored["steps"] if step["step_name"] == "PERSIST")
    assert persist["status"] == "COMPLETED"
    assert persist["output"]["accepted_count"] == 2


@pytest.mark.parametrize("failing_index", [0, 1])
def test_claim_exception_rolls_back_first_or_intermediate_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_index: int,
) -> None:
    cfg = settings(tmp_path)
    repository, run = make_run(cfg, f"claim-failure-{failing_index}")
    claims = [supported_claim(1), supported_claim(2)]
    step_id = begin_persist(repository, run, len(claims))
    original = repository._persist_claim
    calls = 0

    def injected(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        index = calls
        calls += 1
        if index == failing_index:
            raise ValueError(f"injected_claim_{index}")
        return original(*args, **kwargs)

    monkeypatch.setattr(repository, "_persist_claim", injected)
    with pytest.raises(ResearchPersistenceError) as raised:
        repository.persist_claims(run, claims, step_id=step_id)

    assert raised.value.diagnostic["claim_ref"] == claims[failing_index]["claim_ref"]
    assert raised.value.diagnostic["exception_type"] == "ValueError"
    assert raised.value.diagnostic["retry_classification"] == "NON_RETRYABLE"
    assert_rolled_back(cfg, repository, str(run["run_id"]))


def test_evidence_insert_exception_rolls_back_everything(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, run = make_run(cfg, "evidence-failure")
    claim = supported_claim(1)
    step_id = begin_persist(repository, run, 1)
    with connect_sqlite(cfg.database_path) as conn:
        conn.execute(
            """
            CREATE TRIGGER inject_evidence_failure
            BEFORE INSERT ON research_evidence
            BEGIN
              SELECT RAISE(ABORT, 'injected_evidence_insert');
            END
            """
        )
        conn.commit()

    with pytest.raises(ResearchPersistenceError, match="research_persist"):
        repository.persist_claims(run, [claim], step_id=step_id)

    assert_rolled_back(cfg, repository, str(run["run_id"]))


@pytest.mark.parametrize("failure_point", ["projection", "read_back"])
def test_projection_or_read_back_exception_rolls_back_everything(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    cfg = settings(tmp_path)
    repository, run = make_run(cfg, f"{failure_point}-failure")
    claim = supported_claim(1)
    step_id = begin_persist(repository, run, 1)
    if failure_point == "projection":
        monkeypatch.setattr(
            repository.facts,
            "upsert_fact_in_transaction",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                ValueError("injected_projection")
            ),
        )
    else:
        monkeypatch.setattr(
            repository,
            "_claim_result_from_evidence",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                ValueError("injected_read_back")
            ),
        )

    with pytest.raises(ResearchPersistenceError):
        repository.persist_claims(run, [claim], step_id=step_id)

    assert_rolled_back(cfg, repository, str(run["run_id"]))


def test_retry_is_idempotent_after_rollback_and_after_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = settings(tmp_path)
    repository, run = make_run(cfg, "idempotent-retry")
    claim = supported_claim(1)
    first_step = begin_persist(repository, run, 1)
    original = repository._project_and_read_back
    monkeypatch.setattr(
        repository,
        "_project_and_read_back",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("injected_once")
        ),
    )
    with pytest.raises(ResearchPersistenceError):
        repository.persist_claims(run, [claim], step_id=first_step)
    assert_rolled_back(cfg, repository, str(run["run_id"]))

    monkeypatch.setattr(repository, "_project_and_read_back", original)
    retry_step = begin_persist(repository, run, 1)
    result = repository.persist_claims(
        repository.get_run(str(run["run_id"])) or run,
        [claim],
        step_id=retry_step,
    )
    duplicate = repository.persist_claims(
        repository.get_run(str(run["run_id"])) or run,
        [claim],
    )

    assert result["persisted_count"] == duplicate["persisted_count"] == 1
    assert atomic_counts(cfg, str(run["run_id"])) == {
        "claims": 1,
        "evidence": 1,
        "facts": 1,
    }


def test_concurrent_persist_has_no_duplicates(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, run = make_run(cfg, "concurrent-persist")
    claim = supported_claim(1)

    def persist() -> dict[str, Any]:
        local = OfflinePersistRepository(cfg, now=lambda: REFERENCE_NOW)
        return local.persist_claims(
            local.get_run(str(run["run_id"])) or run,
            [claim],
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _item: persist(), range(2)))

    assert [item["persisted_count"] for item in results] == [1, 1]
    assert atomic_counts(cfg, str(run["run_id"])) == {
        "claims": 1,
        "evidence": 1,
        "facts": 1,
    }


def test_reconciliation_closes_step_and_quarantines_without_deleting(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository, run = make_run(cfg, "reconcile-failed")
    claim = supported_claim(1)
    step_id = begin_persist(repository, run, 1)
    repository.persist_claims(run, [claim], step_id=step_id)
    snapshot = MarketContextSnapshotRepository(cfg).save_next(
        symbol="MNQ",
        refresh_mode="offline",
        debug_payload={"symbol": "MNQ", "generated_at_utc": REFERENCE_NOW.isoformat()},
        ai_enrichment={"status": "PARTIAL"},
        source_job_id=str(run["job_id"]),
    )
    with connect_sqlite(cfg.database_path) as conn:
        conn.execute(
            """
            UPDATE research_run_steps
            SET status='RUNNING',completed_at=NULL,error=NULL,diagnostic_json=NULL
            WHERE step_id=?
            """,
            (step_id,),
        )
        conn.execute(
            """
            UPDATE research_step_attempts
            SET status='RUNNING',completed_at=NULL,error=NULL,diagnostic_json=NULL
            WHERE step_id=? AND attempt=1
            """,
            (step_id,),
        )
        conn.execute(
            """
            UPDATE ai_research_jobs
            SET status='FAILED',completed_at=?,last_error='worker:unknown:ValueError'
            WHERE job_id=?
            """,
            (REFERENCE_NOW.isoformat(), run["job_id"]),
        )
        conn.commit()
    before = atomic_counts(cfg, str(run["run_id"]))

    assert repository.reconcile_terminal_jobs() == 1
    assert repository.reconcile_terminal_jobs() == 0
    assert atomic_counts(cfg, str(run["run_id"])) == before
    restored = repository.get_run(str(run["run_id"]))
    assert restored is not None and restored["status"] == "FAILED"
    assert "accepted_claims" not in restored["result"]
    persist = next(step for step in restored["steps"] if step["step_name"] == "PERSIST")
    assert persist["status"] == "FAILED"
    assert persist["diagnostic"]["reconciled"] is True
    assert persist["diagnostic"]["transaction_outcome"] == "PARTIALLY_COMMITTED"
    with connect_sqlite(cfg.database_path) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM research_claims
            WHERE research_run_id=? AND materialization_status='ORPHANED'
            """,
            (run["run_id"],),
        ).fetchone()[0] == before["claims"]
        assert conn.execute(
            """
            SELECT COUNT(*) FROM research_evidence
            WHERE claim_id IN (
              SELECT claim_id FROM research_claims WHERE research_run_id=?
            ) AND audit_status='ORPHANED'
            """,
            (run["run_id"],),
        ).fetchone()[0] == before["evidence"]
        assert conn.execute(
            """
            SELECT COUNT(*) FROM market_facts
            WHERE fact_key IN (
              SELECT 'research:' || claim_id
              FROM research_claims WHERE research_run_id=?
            ) AND status='orphaned'
            """,
            (run["run_id"],),
        ).fetchone()[0] == before["facts"]
    snapshots = MarketContextSnapshotRepository(cfg)
    assert snapshots.get(str(snapshot["snapshot_id"]))["audit_status"] == "ORPHANED"
    assert snapshots.latest("MNQ") is None


@pytest.mark.parametrize("source_version", [13, 14])
def test_migration_from_schema_13_or_14_preserves_data(
    tmp_path: Path,
    source_version: int,
) -> None:
    database = tmp_path / f"schema-{source_version}.sqlite"
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE schema_migrations(
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              applied_at TEXT NOT NULL
            )
            """
        )
        for index, (name, sql) in enumerate(MIGRATIONS[:source_version], start=1):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations(version,name,applied_at) VALUES (?,?,?)",
                (index, name, REFERENCE_NOW.isoformat()),
            )
        conn.execute(f"PRAGMA user_version={source_version}")
        conn.execute(
            """
            INSERT INTO market_facts(
              fact_key,fact_type,retrieved_at,status,created_at,updated_at
            ) VALUES ('migration-marker','test',?,'active',?,?)
            """,
            (REFERENCE_NOW.isoformat(),) * 3,
        )
        conn.commit()

    result = migrate_database(database)

    assert result["schema_version"] == len(MIGRATIONS)
    with connect_sqlite(database) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM market_facts WHERE fact_key='migration-marker'"
        ).fetchone()[0] == 1
        assert conn.execute("PRAGMA user_version").fetchone()[0] == len(MIGRATIONS)
        assert {
            row["name"] for row in conn.execute("PRAGMA table_info(research_claims)")
        } >= {"materialization_status"}
        assert {
            row["name"] for row in conn.execute("PRAGMA table_info(research_evidence)")
        } >= {"audit_status"}
