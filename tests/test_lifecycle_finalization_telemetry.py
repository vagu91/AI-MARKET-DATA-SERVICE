from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
    persist_lifecycle_in_transaction,
)
from app.services.execution_context import ExecutionContext
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.lifecycle_due_resolver import (
    DeterministicLifecycleDueResolver,
    TemporaryLifecycleProviderError,
)
from app.services.market_context_outbox_service import (
    MarketContextOutboxRepository,
)
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.research_scheduler_service import ResearchSchedulerService
from scripts.reconcile_terminal_lifecycle_leases import (
    _open_connection,
    reconcile_terminal_leases,
)
from scripts.replay_provider_only_lifecycle_forensics import replay


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 24, 17, 6, 50, tzinfo=UTC)
FRESH_SOURCE = {
    "source": "Cboe",
    "source_url": "https://www.cboe.com/us/indices/dashboard/vix/",
    "source_lineage": [
        {
            "source": "Cboe",
            "source_url": "https://www.cboe.com/us/indices/dashboard/vix/",
            "verification_status": "VERIFIED",
        }
    ],
}


def cfg(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "model_pricing_path": ROOT / "config" / "model_pricing.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "environment": "test",
        "lifecycle_due_scanner_enabled": True,
        "research_agent_macro_events_enabled": True,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def seed_due(
    settings: Settings,
    *,
    entity_type: str = "vix",
    entity_key: str = "VIX",
    fields: list[str] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value = dict(
        payload
        or {
            "value": 18.0,
            "observed_at": (NOW - timedelta(hours=2)).isoformat(),
            "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
        }
    )
    lifecycle = compute_datum_lifecycle(
        entity_type,
        entity_key,
        value,
        settings=settings,
        now=NOW,
        fields_attempted=fields or ["value"],
    )
    return LifecycleRepository(settings, clock=lambda: NOW).upsert(
        lifecycle,
        payload=value,
        work_status="READY",
    )


def seed_expired_no_data(
    settings: Settings,
    *,
    entity_type: str = "vix",
    entity_key: str = "VIX",
    fields: list[str] | None = None,
) -> dict[str, Any]:
    seeded_at = NOW - timedelta(hours=2)
    attempted = fields or ["value"]
    payload = {
        "status": "NO_DATA",
        "value": None,
        "reason": "no_fresh_verified_source",
        "valid_until": (NOW + timedelta(hours=4)).isoformat(),
        "sources_attempted": [
            {
                "source_domain": "cboe.com",
                "fetch_status": "FETCHED",
                "verification_status": "VERIFIED",
            }
        ],
    }
    lifecycle = compute_datum_lifecycle(
        entity_type,
        entity_key,
        payload,
        settings=settings,
        now=seeded_at,
        no_data=True,
        fields_attempted=attempted,
        retry_class="NO_DATA",
        refresh_reason="no_fresh_verified_source",
    )
    return LifecycleRepository(
        settings,
        clock=lambda: seeded_at,
    ).upsert(
        lifecycle,
        payload=payload,
        work_status="BACKOFF",
    )


class FakeProvider:
    performs_io = True

    def __init__(
        self,
        *,
        status: str = "RESOLVED",
        datum: dict[str, Any] | None = None,
        missing_fields: list[str] | None = None,
        temporary_failure: bool = False,
    ) -> None:
        self.status = status
        self.datum = dict(
            datum
            or {
                "value": 19.0,
                "data_as_of": NOW.isoformat(),
                "valid_until": (NOW + timedelta(hours=1)).isoformat(),
                **FRESH_SOURCE,
            }
        )
        self.missing_fields = list(missing_fields or [])
        self.temporary_failure = temporary_failure
        self.calls = 0

    def resolve(self, _: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        if self.temporary_failure:
            raise TemporaryLifecycleProviderError("controlled_timeout")
        return {
            "status": self.status,
            "datum": self.datum,
            "missing_fields": self.missing_fields,
            "reason": "controlled_provider_result",
        }


def telemetry_names(settings: Settings) -> list[str]:
    with connect_sqlite(settings.database_path) as conn:
        return [
            str(row["event_name"])
            for row in conn.execute(
                """
                SELECT event_name FROM service_telemetry_events
                ORDER BY occurred_at,telemetry_id
                """
            ).fetchall()
        ]


def stored_item(settings: Settings, item_id: str) -> dict[str, Any]:
    return next(
        item
        for item in LifecycleRepository(settings).list_items()
        if item["item_id"] == item_id
    )


def test_committed_operational_payload_skips_provider_io(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    item = seed_due(settings)
    fresh_payload = {
        "value": 18.5,
        "data_as_of": NOW.isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        **FRESH_SOURCE,
    }
    with connect_sqlite(settings.database_path) as conn:
        conn.execute(
            """
            UPDATE datum_lifecycle_items SET payload_json=?
            WHERE item_id=?
            """,
            (
                json.dumps(fresh_payload, sort_keys=True),
                item["item_id"],
            ),
        )
        conn.commit()
    provider = FakeProvider()
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"vix": provider},
    )
    result = ResearchSchedulerService(
        settings,
        clock=lambda: NOW,
    ).scan_due_items(
        owner="committed-hit",
        resolver=resolver.resolve,
        ai_enqueue=None,
        trigger_type="macro_actual",
    )

    assert provider.calls == 0
    assert result["resolver_evaluations"] == 1
    assert result["committed_payload_hits"] == 1
    assert result["actual_provider_requests"] == 0
    assert "committed_payload_hit" in telemetry_names(settings)
    final = stored_item(settings, item["item_id"])
    assert final["refresh_reason"] == (
        "deterministic_committed_payload_revalidated"
    )
    assert final["lease_owner"] is final["lease_expires_at"] is None
    assert final["heartbeat_at"] is None


def test_temporally_valid_no_data_payload_calls_provider(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    item = seed_expired_no_data(settings)
    provider = FakeProvider()
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"vix": provider},
    )
    result = ResearchSchedulerService(
        settings,
        clock=lambda: NOW,
    ).scan_due_items(
        owner="non-operational",
        resolver=resolver.resolve,
        ai_enqueue=None,
        trigger_type="macro_actual",
    )

    assert provider.calls == 1
    assert result["committed_payload_hits"] == 1
    assert result["actual_provider_requests"] == 1
    assert result["successful_provider_requests"] == 1
    final = stored_item(settings, item["item_id"])
    assert final["freshness_state"] == "FRESH"
    assert final["refresh_reason"] == "deterministic_provider_resolved"


@pytest.mark.parametrize(
    ("payload", "expected_reason"),
    [
        (
            {
                "status": "NO_DATA",
                "value": None,
                "reason": "no_fresh_verified_source",
                "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            },
            "committed_payload_no_data_envelope",
        ),
        (
            {
                "status": "AVAILABLE",
                "value": None,
                "valid_until": (NOW + timedelta(hours=1)).isoformat(),
                **FRESH_SOURCE,
            },
            "committed_payload_temporally_valid_but_not_operational",
        ),
        (
            {
                "status": "AVAILABLE",
                "value": 18.0,
                "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            },
            "committed_payload_source_unverified",
        ),
    ],
)
def test_committed_payload_rejection_reason_codes_are_deterministic(
    tmp_path: Path,
    payload: dict[str, Any],
    expected_reason: str,
) -> None:
    settings = cfg(tmp_path)
    result = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={},
    ).resolve(
        {
            "entity_type": "vix",
            "entity_key": "VIX",
            "fields_attempted": ["value"],
            "payload": payload,
        }
    )

    assert result["committed_payload_hit"] is True
    assert result["committed_payload_reason"] == expected_reason
    assert result["status"] == "EXHAUSTED"


def test_provider_request_has_attempted_and_completed_telemetry(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    seed_due(settings)
    provider = FakeProvider()
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"vix": provider},
    )
    result = ResearchSchedulerService(
        settings,
        clock=lambda: NOW,
    ).scan_due_items(
        owner="provider-completed",
        resolver=resolver.resolve,
        ai_enqueue=None,
        trigger_type="macro_actual",
    )

    assert result["actual_provider_requests"] == 1
    assert result["successful_provider_requests"] == 1
    assert result["failed_provider_requests"] == 0
    names = telemetry_names(settings)
    assert names.count("provider_request_attempted") == 1
    assert names.count("provider_request_completed") == 1
    assert "provider_call" not in names


def test_temporary_provider_failure_backoff_clears_lease(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    item = seed_expired_no_data(settings)
    provider = FakeProvider(temporary_failure=True)
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"vix": provider},
    )
    result = ResearchSchedulerService(
        settings,
        clock=lambda: NOW,
    ).scan_due_items(
        owner="provider-failed",
        resolver=resolver.resolve,
        ai_enqueue=lambda _: pytest.fail("AI must not run"),
        trigger_type="macro_actual",
    )

    assert result["failed_provider_requests"] == 1
    assert result["successful_provider_requests"] == 0
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0
    final = stored_item(settings, item["item_id"])
    assert final["work_status"] == "BACKOFF"
    assert final["negative_cache_expires_at"] == final["next_retry_at"]
    assert final["lease_owner"] is final["lease_expires_at"] is None
    assert final["heartbeat_at"] is None
    assert telemetry_names(settings).count("provider_request_failed") == 1


def test_partial_result_exposes_only_missing_fields_without_enqueue(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    item = seed_due(
        settings,
        entity_type="macro_actual",
        entity_key="US:CPI:2026-07",
        fields=["actual", "consensus"],
        payload={
            "actual": None,
            "consensus": None,
            "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
        },
    )
    provider = FakeProvider(
        status="PARTIAL",
        datum={
            "actual": 2.7,
            "consensus": None,
            "data_as_of": NOW.isoformat(),
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            **FRESH_SOURCE,
        },
        missing_fields=["consensus"],
    )
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"macro_actual": provider},
    )
    direct = resolver.resolve(
        {
            **item,
            "payload": item["payload"],
            "fields_attempted": ["actual", "consensus"],
        }
    )
    assert direct["missing_fields"] == ["consensus"]
    assert direct["ai_eligible"] is False
    result = ResearchSchedulerService(
        settings,
        clock=lambda: NOW,
    ).scan_due_items(
        owner="partial-no-enqueue",
        resolver=resolver.resolve,
        ai_enqueue=None,
    )

    assert result["ai_eligible_count"] == 0
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0
    final = stored_item(settings, item["item_id"])
    assert final["work_status"] == "DISABLED"
    assert final["payload"]["actual"] == 2.7
    assert final["payload"]["consensus"] is None
    assert final["lease_owner"] is final["lease_expires_at"] is None
    assert final["heartbeat_at"] is None


def test_mixed_ai_eligible_and_disabled_residuals_finalize_every_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.services.lifecycle_due_resolver."
        "automatic_ai_delivery_authorized",
        lambda **_: True,
    )
    monkeypatch.setattr(
        "app.services.research_scheduler_service."
        "automatic_ai_delivery_authorized",
        lambda **_: True,
    )
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
        research_agent_macro_events_enabled=True,
        research_agent_vix_risk_enabled=False,
    )
    eligible = seed_due(
        settings,
        entity_type="macro_actual",
        entity_key="US:CPI:2026-07",
        fields=["actual"],
        payload={
            "actual": None,
            "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
        },
    )
    disabled = seed_due(
        settings,
        entity_type="vix",
        entity_key="VIX",
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={},
    )
    context = ExecutionContext.explicit_ai(
        correlation_id="mixed-ai-disabled",
        request_origin="research_scheduler",
        allow_live_providers=True,
    )

    first = scheduler.scan_due_items(
        owner="mixed-ai-disabled-first",
        resolver=resolver.resolve,
        ai_enqueue=lambda items: scheduler.enqueue_due_residuals(
            items,
            execution_context=context,
        ),
        execution_context=context,
    )
    jobs = AIResearchJobRepository(settings).latest(limit=10)
    outcomes = {
        item["item_id"]: item["status"]
        for item in first["item_outcomes"]
    }

    assert first["claimed"] == 2
    assert first["ai_invocations"] == 1
    assert first["ai_jobs_created"] == 1
    assert len(jobs) == 1
    assert jobs[0]["request_payload"]["lifecycle_item_id"] == eligible["item_id"]
    assert len(first["item_outcomes"]) == 2
    assert len(outcomes) == 2
    assert outcomes == {
        eligible["item_id"]: "AI_QUEUED",
        disabled["item_id"]: "DISABLED",
    }

    eligible_stored = stored_item(settings, eligible["item_id"])
    disabled_stored = stored_item(settings, disabled["item_id"])
    assert eligible_stored["work_status"] == "QUEUED"
    assert disabled_stored["work_status"] == "DISABLED"
    assert disabled_stored["lease_owner"] is None
    assert disabled_stored["lease_expires_at"] is None
    assert disabled_stored["heartbeat_at"] is None
    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM datum_lifecycle_items
            WHERE work_status='LEASED'
            """
        ).fetchone()[0] == 0

    second = scheduler.scan_due_items(
        owner="mixed-ai-disabled-second",
        resolver=resolver.resolve,
        ai_enqueue=lambda items: scheduler.enqueue_due_residuals(
            items,
            execution_context=context,
        ),
        execution_context=context,
    )
    assert second["claimed"] == 0
    assert second["ai_invocations"] == 0
    assert second["item_outcomes"] == []
    assert len(AIResearchJobRepository(settings).latest(limit=10)) == 1


@pytest.mark.parametrize(
    "work_status",
    [
        "RESOLVED",
        "COMPLETED",
        "PARTIAL",
        "BACKOFF",
        "IDLE",
        "DISABLED",
        "NO_DATA",
        "QUEUED",
        "FAILED",
    ],
)
def test_upsert_final_states_clear_owned_lease(
    tmp_path: Path,
    work_status: str,
) -> None:
    settings = cfg(tmp_path)
    item = seed_due(settings)
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    leased = repository.claim_due(owner="lease-owner", now=NOW)[0]
    lifecycle = compute_datum_lifecycle(
        "vix",
        "VIX",
        {
            "value": 19.0,
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            **FRESH_SOURCE,
        },
        settings=settings,
        now=NOW,
        fields_attempted=["value"],
    )
    repository.upsert(
        lifecycle,
        payload={
            "value": 19.0,
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            **FRESH_SOURCE,
        },
        work_status=work_status,
    )
    final = stored_item(settings, leased["item_id"])
    assert final["work_status"] == work_status
    assert final["lease_owner"] is final["lease_expires_at"] is None
    assert final["heartbeat_at"] is None
    assert item["item_id"] == leased["item_id"]


@pytest.mark.parametrize("persistence_path", ["repository", "transaction"])
def test_leased_upsert_preserves_existing_lease(
    tmp_path: Path,
    persistence_path: str,
) -> None:
    settings = cfg(tmp_path)
    seed_due(settings)
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    leased = repository.claim_due(owner="lease-owner", now=NOW)[0]
    lifecycle = compute_datum_lifecycle(
        "vix",
        "VIX",
        {
            "value": 19.0,
            "observed_at": NOW.isoformat(),
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            **FRESH_SOURCE,
        },
        settings=settings,
        now=NOW,
        fields_attempted=["value"],
        refresh_reason="leased_payload_refresh",
    )
    payload = {
        "value": 19.0,
        "observed_at": NOW.isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        **FRESH_SOURCE,
    }

    if persistence_path == "repository":
        repository.upsert(
            lifecycle,
            payload=payload,
            work_status="LEASED",
        )
    else:
        with connect_sqlite(settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            persist_lifecycle_in_transaction(
                conn,
                lifecycle,
                payload=payload,
                work_status="LEASED",
                timestamp=NOW.isoformat(),
            )
            conn.commit()

    final = stored_item(settings, leased["item_id"])
    assert final["work_status"] == "LEASED"
    assert final["lease_owner"] == leased["lease_owner"]
    assert final["lease_expires_at"] == leased["lease_expires_at"]
    assert final["heartbeat_at"] == leased["heartbeat_at"]


def test_complete_and_transition_clear_owned_leases(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    first = seed_due(settings, entity_key="VIX")
    second = seed_due(
        settings,
        entity_type="macro_actual",
        entity_key="US:CPI:2026-07",
    )
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    claimed = {
        item["item_id"]: item
        for item in repository.claim_due(
            owner="finalizer",
            now=NOW,
            limit=2,
        )
    }
    assert set(claimed) == {first["item_id"], second["item_id"]}
    assert repository.complete(
        first["item_id"],
        owner="finalizer",
        now=NOW,
    )
    assert repository.transition(
        second["item_id"],
        owner="finalizer",
        work_status="NO_DATA",
        refresh_reason="terminal_no_data",
        now=NOW,
    )

    for item_id in claimed:
        final = stored_item(settings, item_id)
        assert final["lease_owner"] is final["lease_expires_at"] is None
        assert final["heartbeat_at"] is None


def test_snapshot_rematerialization_does_not_touch_unrelated_lifecycle(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    earnings: list[dict[str, Any]] = []
    unrelated_ids: list[str] = []
    for index in range(7):
        event = {
            "ticker": f"T{index}",
            "actual_eps": index + 1,
            "event_at": (NOW + timedelta(days=index + 1)).isoformat(),
            "valid_until": (NOW + timedelta(days=10)).isoformat(),
            **FRESH_SOURCE,
        }
        lifecycle = compute_datum_lifecycle(
            "earnings_schedule",
            f"T{index}:2026-07-{25 + index:02d}",
            event,
            settings=settings,
            now=NOW,
            fields_attempted=["actual_eps"],
        )
        stored = repository.upsert(
            lifecycle,
            payload=event,
            work_status="IDLE",
        )
        unrelated_ids.append(str(stored["item_id"]))
        earnings.append({**event, "lifecycle": lifecycle.as_dict()})
    snapshots = MarketContextSnapshotRepository(settings)
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="baseline",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "nasdaq_context": {
                "status": "AVAILABLE",
                "qqq_holdings": {"status": "NOT_AVAILABLE"},
                "earnings": {"events": earnings},
            },
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    before = _raw_rows(settings, unrelated_ids)
    target = seed_due(
        settings,
        entity_type="nasdaq_100",
        entity_key="MNQ:nasdaq_100",
        fields=["holdings"],
        payload={
            "holdings": [],
            "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
        },
    )
    provider = FakeProvider(
        datum={
            "holdings": [{"symbol": "NVDA", "weight": 0.09}],
            "as_of": NOW.date().isoformat(),
            "data_as_of": NOW.isoformat(),
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            **FRESH_SOURCE,
        }
    )
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"nasdaq_100": provider},
    )
    result = ResearchSchedulerService(
        settings,
        clock=lambda: NOW,
    ).scan_due_items(
        owner="snapshot-conditional-upsert",
        resolver=resolver.resolve,
        ai_enqueue=None,
    )
    after = _raw_rows(settings, unrelated_ids)

    assert len(result["rematerialized_snapshot_ids"]) == 1
    assert before == after
    assert len(MarketContextOutboxRepository(settings).list_events()) == 0
    final = stored_item(settings, target["item_id"])
    assert final["lease_owner"] is final["lease_expires_at"] is None
    assert final["heartbeat_at"] is None


def test_reconciliation_is_scoped_idempotent_and_auditable(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    item = seed_due(settings)
    with connect_sqlite(settings.database_path) as conn:
        conn.execute(
            """
            UPDATE datum_lifecycle_items
            SET work_status='COMPLETED',
                lease_owner='stale-owner',
                lease_expires_at=?,
                heartbeat_at=?
            WHERE item_id=?
            """,
            (NOW.isoformat(), NOW.isoformat(), item["item_id"]),
        )
        conn.commit()

    dry_run = reconcile_terminal_leases(
        settings.database_path,
        apply=False,
    )
    first = reconcile_terminal_leases(settings.database_path, apply=True)
    second = reconcile_terminal_leases(settings.database_path, apply=True)

    assert dry_run["candidate_count"] == 1
    assert dry_run["changed_count"] == 0
    assert first["changed_count"] == 1
    assert first["remaining_candidate_count"] == 0
    assert second["candidate_count"] == second["changed_count"] == 0


def test_reconciliation_dry_run_connection_is_sqlite_read_only(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    seed_due(settings)

    connection = _open_connection(
        settings.database_path.resolve(),
        apply=False,
    )
    try:
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute(
                """
                UPDATE datum_lifecycle_items
                SET refresh_reason='must-not-write'
                """
            )
    finally:
        connection.close()


def test_repository_noop_upsert_preserves_created_and_updated_at(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    lifecycle = compute_datum_lifecycle(
        "vix",
        "VIX",
        {
            "value": 18.0,
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            **FRESH_SOURCE,
        },
        settings=settings,
        now=NOW,
        fields_attempted=["value"],
    )
    first = LifecycleRepository(
        settings,
        clock=lambda: NOW,
    ).upsert(
        lifecycle,
        payload={
            "value": 18.0,
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            "searched_at": NOW.isoformat(),
            **FRESH_SOURCE,
        },
        work_status="COMPLETED",
    )
    second = LifecycleRepository(
        settings,
        clock=lambda: NOW + timedelta(hours=1),
    ).upsert(
        lifecycle,
        payload={
            "value": 18.0,
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
            "searched_at": (NOW + timedelta(hours=1)).isoformat(),
            **FRESH_SOURCE,
        },
        work_status="COMPLETED",
    )

    assert second["created_at"] == first["created_at"]
    assert second["updated_at"] == first["updated_at"]
    assert second["payload"]["searched_at"] == first["payload"]["searched_at"]


def test_redacted_provider_only_forensic_replay(tmp_path: Path) -> None:
    result = replay(workspace=tmp_path / "replay")

    assert result["claimed_count"] == 2
    assert result["changed_items_are_targets_only"] is True
    assert result["unrelated_count"] == 7
    assert result["unrelated_unchanged_count"] == 7
    assert result["lease_violations"] == 0
    assert result["snapshot_delta"] <= 1
    assert result["outbox_delta"] == 0
    assert result["ai_job_delta"] == 0
    assert result["artifact_unchanged"] is True
    assert result["provider_call_event_count"] == 0
    assert result["resolver_evaluations"] == 2
    assert result["committed_payload_hits"] == 2
    assert result["actual_provider_requests"] == 0
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0


def _raw_rows(
    settings: Settings,
    item_ids: list[str],
) -> dict[str, dict[str, Any]]:
    placeholders = ",".join("?" for _ in item_ids)
    with connect_sqlite(settings.database_path) as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM datum_lifecycle_items
            WHERE item_id IN ({placeholders})
            ORDER BY item_id
            """,
            tuple(item_ids),
        ).fetchall()
    return {str(row["item_id"]): dict(row) for row in rows}
