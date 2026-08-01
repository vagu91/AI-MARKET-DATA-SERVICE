from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.main import run_lifecycle_due_scan
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_worker import AIResearchWorker
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
)
from app.services.lifecycle_due_resolver import (
    DeterministicLifecycleDueResolver,
    StaticLifecycleProviderAdapter,
    TemporaryLifecycleProviderError,
)
from app.services.market_context_outbox_service import MarketContextOutboxRepository
from app.services.market_context_outbox_service import TriggerEnvelope
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.execution_context import ExecutionContext
from app.services.research_agent_enablement import research_agent_enablement
from app.services.research_profiles import JOB_PROFILE
from app.services.research_scheduler_service import (
    ResearchSchedulerService as BaseResearchSchedulerService,
)


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 24, 12, tzinfo=UTC)


def _allow_certified_ai(monkeypatch: pytest.MonkeyPatch) -> None:
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


class ResearchSchedulerService(BaseResearchSchedulerService):
    @staticmethod
    def _context() -> ExecutionContext:
        return ExecutionContext.explicit_ai(
            correlation_id="review16-scheduler-test",
            request_origin="research_scheduler",
            allow_live_providers=True,
        )

    def scan_due_items(self, **kwargs):
        kwargs.setdefault("execution_context", self._context())
        return super().scan_due_items(**kwargs)

    def enqueue_due_residuals(self, items, **kwargs):
        kwargs.setdefault("execution_context", self._context())
        return super().enqueue_due_residuals(items, **kwargs)


def cfg(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "model_pricing_path": ROOT / "config" / "model_pricing.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "environment": "test",
        "enable_scheduler": True,
        "research_scheduler_enabled": True,
        "lifecycle_due_scanner_enabled": True,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def seed_due_vix(settings: Settings) -> dict[str, Any]:
    lifecycle = compute_datum_lifecycle(
        "vix",
        "VIX",
        {
            "value": 20.0,
            "observed_at": (NOW - timedelta(hours=2)).isoformat(),
            "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
        },
        settings=settings,
        now=NOW,
        fields_attempted=["value"],
    )
    return LifecycleRepository(settings, clock=lambda: NOW).upsert(
        lifecycle,
        payload={
            "value": 20.0,
            "observed_at": (NOW - timedelta(hours=2)).isoformat(),
            "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
        },
    )


def seed_snapshot(settings: Settings) -> dict[str, Any]:
    return MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="test_baseline",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": (NOW - timedelta(minutes=2)).isoformat(),
            "risk_context": {
                "status": "AVAILABLE",
                "vix": {
                    "value": 20.0,
                    "data_as_of": (NOW - timedelta(hours=2)).isoformat(),
                },
            },
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )


def seed_due_trigger(
    settings: Settings,
    *,
    entity_type: str,
    entity_key: str,
) -> dict[str, Any]:
    datum = {
        "value": 1,
        "observed_at": (NOW - timedelta(hours=2)).isoformat(),
        "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
    }
    lifecycle = compute_datum_lifecycle(
        entity_type,
        entity_key,
        datum,
        settings=settings,
        now=NOW,
        fields_attempted=["value"],
    )
    assert lifecycle.trigger_class == "TRIGGER"
    return LifecycleRepository(settings, clock=lambda: NOW).upsert(
        lifecycle,
        payload=datum,
    )


class _OfflineTriggerMaterializer:
    def __init__(
        self,
        settings: Settings,
        *,
        snapshots: MarketContextSnapshotRepository,
    ) -> None:
        self.settings = settings
        self.snapshots = snapshots
        self.calls = 0
        self.trigger_envelopes: list[dict[str, Any]] = []

    def materialize_for_job(
        self,
        *,
        job: dict[str, Any],
        ai_enrichment: dict[str, Any],
    ) -> dict[str, Any]:
        self.calls += 1
        envelope_value = dict(
            (job.get("request_payload") or {}).get("trigger_envelope") or {}
        )
        self.trigger_envelopes.append(envelope_value)
        trigger = TriggerEnvelope.from_mapping(envelope_value)
        assert trigger is not None
        previous = self.snapshots.latest("MNQ")
        assert previous is not None
        debug = dict(previous["debug_payload"])
        risk = dict(debug.get("risk_context") or {})
        risk["status"] = "AVAILABLE"
        risk["vix"] = {
            "value": 19.0,
            "data_as_of": NOW.isoformat(),
            "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        }
        debug["risk_context"] = risk
        debug["generated_at_utc"] = NOW.isoformat()
        return self.snapshots.save_next(
            symbol="MNQ",
            refresh_mode="offline_ai_completion",
            debug_payload=debug,
            ai_enrichment=ai_enrichment,
            source_job_id=str(job["job_id"]),
            job_ids=[str(job["job_id"])],
            **trigger.snapshot_arguments(),
        )


async def test_real_due_scan_preserves_trigger_through_offline_ai_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_certified_ai(monkeypatch)
    settings = cfg(tmp_path)
    snapshots = MarketContextSnapshotRepository(settings)
    seed_snapshot(settings)
    seed_due_trigger(
        settings,
        entity_type="macro_actual",
        entity_key="CPI:2026-07",
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    state = {
        "research_scheduler": scheduler,
        "lifecycle_due_resolver": DeterministicLifecycleDueResolver(
            settings,
            clock=lambda: NOW,
        ),
    }

    first = await run_lifecycle_due_scan(state)
    jobs = AIResearchJobRepository(settings, clock=lambda: NOW).latest(limit=10)
    assert first["ai_invocations"] == 1
    assert len(jobs) == 1
    assert jobs[0]["request_payload"]["trigger_envelope"] == {
        "trigger_type": "macro_actual",
        "trigger_entity": "CPI:2026-07",
        "correlation_id": first["effective_triggers"][0]["correlation_id"],
    }

    backend_calls: list[str] = []
    materializer = _OfflineTriggerMaterializer(settings, snapshots=snapshots)
    worker = AIResearchWorker(
        settings,
        repository=AIResearchJobRepository(settings, clock=lambda: NOW),
        executor=lambda *_: backend_calls.append("offline") or {
            "status": "NO_DATA",
            "results": [],
        },
        snapshots=snapshots,
        worker_id="offline-trigger-worker",
    )
    worker.materializer = materializer
    assert worker.process_once() is True
    assert backend_calls == ["offline"]
    assert materializer.calls == 1

    second = await run_lifecycle_due_scan(state)
    assert second["claimed"] == 0
    assert second["ai_invocations"] == 0
    assert worker.process_once() is False
    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_outbox"
        ).fetchone()[0] == 1
    event = MarketContextOutboxRepository(settings).list_events(status=None)[0]
    assert event["trigger_type"] == "macro_actual"
    assert event["trigger_entity"] == "CPI:2026-07"


def test_coalesced_residuals_preserve_per_item_trigger_and_suppress_nontriggering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_certified_ai(monkeypatch)
    settings = cfg(tmp_path, lifecycle_due_max_concurrency=5)
    macro = seed_due_trigger(
        settings,
        entity_type="macro_actual",
        entity_key="CPI:2026-07",
    )
    news = seed_due_trigger(
        settings,
        entity_type="breaking_news",
        entity_key="news-1",
    )
    nontrigger_datum = {
        "value": "risk",
        "observed_at": (NOW - timedelta(hours=2)).isoformat(),
        "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
    }
    nontrigger_lifecycle = compute_datum_lifecycle(
        "geopolitical_regulatory_risk",
        "risk-1",
        nontrigger_datum,
        settings=settings,
        now=NOW,
        fields_attempted=["value"],
    )
    assert nontrigger_lifecycle.trigger_class == "NON_TRIGGERING"
    nontrigger = LifecycleRepository(settings, clock=lambda: NOW).upsert(
        nontrigger_lifecycle,
        payload=nontrigger_datum,
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    resolver = DeterministicLifecycleDueResolver(settings, clock=lambda: NOW)

    result = scheduler.scan_due_items(
        owner="per-item-triggers",
        resolver=resolver.resolve,
        ai_enqueue=scheduler.enqueue_due_residuals,
    )
    assert result["ai_invocations"] == 1
    assert result["coalesced"] is True
    jobs = AIResearchJobRepository(settings).latest(limit=10)
    assert len(jobs) == 3, (result, jobs)
    by_item = {
        job["request_payload"]["lifecycle_item_id"]: job
        for job in jobs
    }
    assert by_item[macro["item_id"]]["request_payload"]["trigger_envelope"][
        "trigger_type"
    ] == "macro_actual"
    assert by_item[news["item_id"]]["request_payload"]["trigger_envelope"][
        "trigger_type"
    ] == "breaking_news"
    assert (
        by_item[nontrigger["item_id"]]["request_payload"]["trigger_envelope"]
        is None
    )


def test_provider_absent_enabled_enqueues_once_and_second_tick_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_certified_ai(monkeypatch)
    settings = cfg(tmp_path)
    seed_due_vix(settings)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    resolver = DeterministicLifecycleDueResolver(settings, clock=lambda: NOW)

    first = scheduler.scan_due_items(
        owner="review-a-1",
        resolver=resolver.resolve,
        ai_enqueue=scheduler.enqueue_due_residuals,
        trigger_type="macro_actual",
    )
    second = scheduler.scan_due_items(
        owner="review-a-2",
        resolver=resolver.resolve,
        ai_enqueue=scheduler.enqueue_due_residuals,
        trigger_type="macro_actual",
    )

    assert first["ai_invocations"] == 1
    assert first["ai_eligible_count"] == 1
    assert second["claimed"] == 0
    with connect_sqlite(settings.database_path) as conn:
        rows = conn.execute(
            "SELECT status,job_type FROM ai_research_jobs"
        ).fetchall()
    assert [tuple(row) for row in rows] == [("PENDING", "VIX_RISK_RESEARCH")]


def test_provider_absent_disabled_is_explicitly_not_requested(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path, research_agent_vix_risk_enabled=False)
    seed_due_vix(settings)
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    resolver = DeterministicLifecycleDueResolver(settings, clock=lambda: NOW)
    backend_calls: list[str] = []

    result = scheduler.scan_due_items(
        owner="review-b",
        resolver=resolver.resolve,
        ai_enqueue=lambda _: backend_calls.append("enqueue"),
        trigger_type="macro_actual",
    )

    assert result["ai_invocations"] == 0
    assert result["ai_decisions"] == [
        {
            "item_id": result["ai_decisions"][0]["item_id"],
            "agent_status": "DISABLED",
            "execution_status": "NOT_REQUESTED",
        }
    ]
    assert backend_calls == []
    stored = LifecycleRepository(settings).list_items()[0]
    assert stored["work_status"] == "DISABLED"
    assert stored["refresh_reason"] == "agent_disabled_ai_not_requested"
    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM ai_research_jobs").fetchone()[0] == 0


def test_deterministic_provider_projects_canonically_and_atomically(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    seed_snapshot(settings)
    seed_due_vix(settings)
    datum = {
        "status": "AVAILABLE",
        "value": 18.25,
        "data_as_of": NOW.isoformat(),
        "observed_at": NOW.isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        "source": "TEST_DETERMINISTIC_PROVIDER",
        "provider_type": "API",
    }
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={
            "vix": StaticLifecycleProviderAdapter(
                status="RESOLVED",
                datum=datum,
            )
        },
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)

    result = scheduler.scan_due_items(
        owner="review-c",
        resolver=resolver.resolve,
        ai_enqueue=lambda _: pytest.fail("AI must not be enqueued"),
        trigger_type="macro_actual",
    )

    assert result["ai_invocations"] == 0
    latest = MarketContextSnapshotRepository(settings).latest("MNQ")
    assert latest is not None
    assert latest["debug_payload"]["risk_context"]["vix"]["value"] == 18.25
    assert latest["consumer_payload"]["risk"]["VIX"]["value"] == 18.25
    with connect_sqlite(settings.database_path) as conn:
        lifecycle_payload = conn.execute(
            "SELECT payload_json FROM datum_lifecycle_items "
            "WHERE entity_type='vix' AND entity_key='VIX'"
        ).fetchone()[0]
        assert '"value":18.25' in lifecycle_payload
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_outbox"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0] == 2


def test_diagnostic_only_lifecycle_resolution_emits_no_outbox(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    repository = MarketContextSnapshotRepository(settings)
    seed_snapshot(settings)
    previous = repository.latest("MNQ")
    assert previous is not None
    debug = dict(previous["debug_payload"])
    debug["lifecycle_resolutions"] = {
        "VIX": {"reason": "diagnostic_refresh", "attempt_count": 7}
    }
    repository.save_next(
        symbol="MNQ",
        refresh_mode="diagnostic_only",
        debug_payload=debug,
        ai_enrichment={"status": "NOT_REQUIRED"},
        trigger_type="macro_actual",
        trigger_entity="VIX",
    )
    assert MarketContextOutboxRepository(settings).list_events(status=None) == []


def test_provider_projection_rolls_back_snapshot_lifecycle_and_outbox_together(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    seed_snapshot(settings)
    seed_due_vix(settings)
    repository = MarketContextSnapshotRepository(settings)
    datum = {
        "value": 17.5,
        "data_as_of": NOW.isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
    }
    lifecycle = compute_datum_lifecycle(
        "vix",
        "VIX",
        datum,
        settings=settings,
        now=NOW,
    )

    def fail_outbox(*_: Any, **__: Any) -> None:
        raise RuntimeError("fault_injected_before_commit")

    repository.outbox.emit_in_transaction = fail_outbox  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="fault_injected_before_commit"):
        repository.save_next(
            symbol="MNQ",
            refresh_mode="provider_resolution",
            debug_payload={
                "symbol": "MNQ",
                "generated_at_utc": NOW.isoformat(),
                "risk_context": {"status": "AVAILABLE", "vix": datum},
            },
            ai_enrichment={"status": "NOT_REQUIRED"},
            trigger_type="macro_actual",
            trigger_entity="VIX",
            resolved_lifecycle=lifecycle,
            resolved_datum=datum,
        )

    with connect_sqlite(settings.database_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM market_context_outbox"
        ).fetchone()[0] == 0
        payload = conn.execute(
            "SELECT payload_json FROM datum_lifecycle_items "
            "WHERE entity_type='vix' AND entity_key='VIX'"
        ).fetchone()[0]
    assert '"value":20.0' in payload


class _FailingProvider:
    def __init__(self) -> None:
        self.calls = 0

    def resolve(self, _: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        raise TemporaryLifecycleProviderError("provider_timeout")


def test_mixed_deferred_and_ai_eligible_batch_reports_backoff_consistently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_certified_ai(monkeypatch)
    settings = cfg(tmp_path, lifecycle_no_data_retry_seconds="60")
    deferred_item = seed_due_trigger(
        settings,
        entity_type="macro_actual",
        entity_key="CPI:2026-07",
    )
    eligible_item = seed_due_trigger(
        settings,
        entity_type="breaking_news",
        entity_key="news-1",
    )
    provider = _FailingProvider()
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"macro_actual": provider},
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)

    first = scheduler.scan_due_items(
        owner="mixed-review-1",
        resolver=resolver.resolve,
        ai_enqueue=scheduler.enqueue_due_residuals,
    )
    assert first["backoff"] == [deferred_item["item_id"]]
    assert first["ai_eligible_count"] == 1
    assert first["ai_invocations"] == 1
    assert first["residual_count"] == 1
    assert {
        item["item_id"]: item["status"]
        for item in first["item_outcomes"]
    } == {
        deferred_item["item_id"]: "BACKOFF",
        eligible_item["item_id"]: "AI_QUEUED",
    }

    stored = {
        item["item_id"]: item
        for item in LifecycleRepository(settings).list_items()
    }
    deferred = stored[deferred_item["item_id"]]
    assert deferred["work_status"] == "BACKOFF"
    assert deferred["next_retry_at"] == deferred["negative_cache_expires_at"]
    with connect_sqlite(settings.database_path) as conn:
        telemetry = [
            json.loads(row[0])
            for row in conn.execute(
                """
                SELECT payload_json FROM service_telemetry_events
                WHERE event_name='retry_backoff'
                ORDER BY occurred_at,telemetry_id
                """
            ).fetchall()
        ]
    assert telemetry[-1]["payload"]["status"] == "BACKOFF"

    second = scheduler.scan_due_items(
        owner="mixed-review-2",
        resolver=resolver.resolve,
        ai_enqueue=scheduler.enqueue_due_residuals,
    )
    assert second["claimed"] == 0
    assert second["provider_calls"] == 0
    assert second["ai_invocations"] == 0
    assert provider.calls == 1


def test_temporary_provider_error_sets_backoff_and_negative_cache(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path, lifecycle_no_data_retry_seconds="60")
    seed_due_vix(settings)
    provider = _FailingProvider()
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"vix": provider},
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    ai_calls: list[str] = []

    first = scheduler.scan_due_items(
        owner="review-f-1",
        resolver=resolver.resolve,
        ai_enqueue=lambda _: ai_calls.append("ai"),
        trigger_type="macro_actual",
    )
    second = scheduler.scan_due_items(
        owner="review-f-2",
        resolver=resolver.resolve,
        ai_enqueue=lambda _: ai_calls.append("ai"),
        trigger_type="macro_actual",
    )

    stored = LifecycleRepository(settings).list_items()[0]
    assert first["ai_invocations"] == second["ai_invocations"] == 0
    assert provider.calls == 1
    assert ai_calls == []
    assert stored["work_status"] == "BACKOFF"
    assert stored["negative_cache_key"]
    assert stored["negative_cache_expires_at"] == stored["next_retry_at"]


def _enqueue_news(settings: Settings, suffix: str) -> dict[str, Any]:
    job, created = AIResearchJobRepository(settings, clock=lambda: NOW).enqueue(
        idempotency_key=f"review-e-{suffix}",
        job_type="NEWS_RESEARCH",
        symbol="MNQ",
        correlation_id=f"review-e-{suffix}",
        request_payload={
            "execution_context": ExecutionContext.explicit_ai(
                correlation_id=f"review-e-{suffix}",
            ).as_payload()
        },
        policy_version="test",
        prompt_version="test",
        profile_id="NEWS_RESEARCH",
        specialized_topic="news",
    )
    assert created is True
    return job


@pytest.mark.parametrize("status", ["PENDING", "RETRY_SCHEDULED", "RUNNING"])
def test_disabled_queued_or_abandoned_job_is_rejected_without_attempt_increment(
    tmp_path: Path,
    status: str,
) -> None:
    enabled = cfg(tmp_path)
    job = _enqueue_news(enabled, status.lower())
    attempts = 1 if status == "RUNNING" else 0
    with connect_sqlite(enabled.database_path) as conn:
        conn.execute(
            """
            UPDATE ai_research_jobs
            SET status=?,attempts=?,worker_id=?,lease_expires_at=?,next_retry_at=?
            WHERE job_id=?
            """,
            (
                status,
                attempts,
                "old-worker" if status == "RUNNING" else None,
                (NOW - timedelta(seconds=1)).isoformat()
                if status == "RUNNING"
                else None,
                NOW.isoformat() if status == "RETRY_SCHEDULED" else None,
                job["job_id"],
            ),
        )
        if status == "RUNNING":
            conn.execute(
                """
                INSERT INTO ai_research_job_attempts(
                  job_id,attempt_number,worker_id,status,started_at
                ) VALUES (?,1,'old-worker','RUNNING',?)
                """,
                (job["job_id"], (NOW - timedelta(minutes=5)).isoformat()),
            )
        conn.commit()
    disabled = cfg(tmp_path, research_agent_news_enabled=False)
    repository = AIResearchJobRepository(disabled, clock=lambda: NOW)

    assert repository.acquire_next("new-worker") is None
    stored = repository.get(job["job_id"])
    assert stored is not None
    assert stored["status"] == "REJECTED"
    assert stored["last_error"] == "AGENT_DISABLED"
    assert stored["retry_class"] == "NON_RETRYABLE"
    assert stored["attempts"] == attempts


class _BackendGuard:
    def __init__(self, backend_name: str) -> None:
        self.backend_name = backend_name
        self.calls = 0

    def __call__(self, *_: Any) -> dict[str, Any]:
        self.calls += 1
        raise AssertionError("disabled job reached backend")


@pytest.mark.parametrize("backend_name", ["codex_cli", "openai_api"])
def test_disabled_agent_is_rejected_before_each_backend(
    tmp_path: Path,
    backend_name: str,
) -> None:
    enabled = cfg(tmp_path, research_backend=backend_name)
    job = _enqueue_news(enabled, backend_name)
    disabled = cfg(
        tmp_path,
        research_backend=backend_name,
        research_agent_news_enabled=False,
    )
    backend = _BackendGuard(backend_name)
    worker = AIResearchWorker(
        disabled,
        repository=AIResearchJobRepository(disabled, clock=lambda: NOW),
        executor=backend,
        worker_id=f"{backend_name}-worker",
    )

    assert worker.process_once() is False
    assert backend.calls == 0
    stored = AIResearchJobRepository(disabled).get(job["job_id"])
    assert stored is not None
    assert stored["status"] == "REJECTED"
    assert stored["attempts"] == 0


def test_unknown_job_type_is_fail_closed(tmp_path: Path) -> None:
    decision = research_agent_enablement(cfg(tmp_path), job_type="UNKNOWN_RESEARCH")
    assert decision["agent_enabled"] is False
    assert decision["configured_enabled"] is False
    assert decision["reason"] == "unmapped_research_job_type"
    disguised = research_agent_enablement(
        cfg(tmp_path),
        topic="news",
        profile_id="NEWS_RESEARCH",
        job_type="UNKNOWN_RESEARCH",
    )
    assert disguised["agent_enabled"] is False
    assert disguised["reason"] == "unmapped_research_job_type"


def test_every_executable_job_type_has_an_explicit_enablement_flag(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    for job_type in JOB_PROFILE:
        decision = research_agent_enablement(settings, job_type=job_type)
        assert decision["reason"] != "unmapped_research_job_type", job_type
        assert decision["settings_field"], job_type
        assert decision["env_name"], job_type
