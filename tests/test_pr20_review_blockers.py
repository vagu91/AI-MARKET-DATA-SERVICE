from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.core.config import Settings
from app.main import run_lifecycle_due_scan
from app.models.common import Impact
from app.models.events import EconomicEvent, EventEnrichment
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_trader_consumer_v2_service import _enforce_payload_limit
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
)
from app.services.execution_context import ExecutionContext
from app.services.parallel_research_coordinator import ParallelResearchCoordinator
from app.services.research_scheduler_service import ResearchSchedulerService


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 25, 12, tzinfo=UTC)


def cfg(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "environment": "test",
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "model_pricing_path": ROOT / "config" / "model_pricing.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "enable_ai_researcher": True,
        "research_agent_macro_events_enabled": True,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def table_count(settings: Settings, table: str) -> int:
    with sqlite3.connect(settings.database_path) as connection:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def contains_key(value: Any, target: str) -> bool:
    if isinstance(value, dict):
        return target in value or any(
            contains_key(item, target) for item in value.values()
        )
    if isinstance(value, list):
        return any(contains_key(item, target) for item in value)
    return False


def latest_authorization(
    settings: Settings,
    *,
    event_name: str = "ai_authorization",
) -> str:
    with sqlite3.connect(settings.database_path) as connection:
        row = connection.execute(
            """
            SELECT payload_json FROM service_telemetry_events
            WHERE event_name=?
            ORDER BY rowid DESC LIMIT 1
            """,
            (event_name,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def event(*, released: bool = False) -> EconomicEvent:
    release = NOW - timedelta(hours=1) if released else NOW + timedelta(days=2)
    return EconomicEvent(
        event_id="cpi-review",
        name="Consumer Price Index",
        country="US",
        category="CPI",
        date=release.date().isoformat(),
        time_utc=release,
        impact=Impact.HIGH,
        source="BLS",
        source_url="https://bls.gov/cpi",
        reliability=0.99,
        enrichment=EventEnrichment(
            forecast=None,
            consensus=None,
            previous="0.2",
            actual=None,
        ),
    )


def invalid_context(case: str) -> tuple[bool, ExecutionContext | None]:
    if case == "omitted":
        return False, None
    if case == "none":
        return True, None
    if case == "incomplete":
        return True, ExecutionContext.from_payload(
            {
                "allow_ai": True,
                "request_origin": "explicit_ai_api",
                "correlation_id": "incomplete",
            }
        )
    if case == "allow_ai_false":
        return True, ExecutionContext(
            allow_live_providers=True,
            allow_ai=False,
            request_origin="explicit_ai_api",
            correlation_id="allow-ai-false",
        )
    if case == "unknown_origin":
        return True, ExecutionContext.from_payload(
            {
                "allow_live_providers": True,
                "allow_ai": True,
                "request_origin": "untrusted_callback",
                "correlation_id": "unknown-origin",
            }
        )
    raise AssertionError(case)


def test_execution_context_rejects_unknown_origin_at_construction() -> None:
    with pytest.raises(
        ValueError,
        match="execution_context_request_origin_invalid",
    ):
        ExecutionContext(
            allow_live_providers=True,
            allow_ai=True,
            request_origin="untrusted_callback",  # type: ignore[arg-type]
            correlation_id="unknown-origin",
        )


@pytest.mark.parametrize(
    ("method", "case"),
    [
        *[
            (method, case)
            for method in ("enqueue_missing_events", "enqueue_explicit")
            for case in (
                "omitted",
                "none",
                "incomplete",
                "allow_ai_false",
                "unknown_origin",
            )
        ],
        *[
            ("enqueue_temporal_refreshes", case)
            for case in ("omitted", "none", "incomplete", "unknown_origin")
        ],
    ],
)
def test_job_service_public_methods_fail_closed_for_invalid_context(
    tmp_path: Path,
    method: str,
    case: str,
) -> None:
    settings = cfg(tmp_path)
    service = AIResearchJobService(settings)
    include_context, context = invalid_context(case)
    kwargs = {"execution_context": context} if include_context else {}
    if method == "enqueue_missing_events":
        result = service.enqueue_missing_events(
            [event()],
            correlation_id=f"{method}-{case}",
            **kwargs,
        )
        assert result == []
    elif method == "enqueue_temporal_refreshes":
        result = service.enqueue_temporal_refreshes(
            [event(released=True)],
            correlation_id=f"{method}-{case}",
            now=NOW,
            **kwargs,
        )
        assert result == []
    else:
        job, created = service.enqueue_explicit(
            job_type="MISSING_EVENT_RESEARCH",
            symbol="MNQ",
            correlation_id=f"{method}-{case}",
            request_payload={"pending_fields": ["forecast"]},
            **kwargs,
        )
        assert created is False
        assert job["last_error"] == "AI_NOT_AUTHORIZED"
    assert table_count(settings, "ai_research_jobs") == 0
    assert table_count(settings, "research_runs") == 0
    assert table_count(settings, "research_backend_invocations") == 0
    if method == "enqueue_temporal_refreshes":
        assert "AI_SUPPRESSED" in latest_authorization(settings)
    else:
        assert "AI_SUPPRESSED" in latest_authorization(settings)


@pytest.mark.parametrize(
    "case",
    ["omitted", "none", "incomplete", "allow_ai_false", "unknown_origin"],
)
def test_parallel_coordinator_fails_closed_before_parent_or_child_runs(
    tmp_path: Path,
    case: str,
) -> None:
    settings = cfg(tmp_path)
    include_context, context = invalid_context(case)
    kwargs = {"execution_context": context} if include_context else {}
    result = ParallelResearchCoordinator(settings).create_parent(
        {
            "manifest_id": f"manifest-{case}",
            "items": [
                {
                    "topic": "macro_events",
                    "required_action": "AGENT_RESEARCH",
                    "ai_eligible": True,
                    "agent_enabled": True,
                    "missing_fields": ["forecast"],
                }
            ],
        },
        correlation_id=f"parent-{case}",
        **kwargs,
    )
    assert result["status"] == "AI_SUPPRESSED"
    assert result["child_job_ids"] == []
    assert table_count(settings, "ai_research_jobs") == 0
    assert table_count(settings, "research_runs") == 0
    assert table_count(settings, "research_parent_runs") == 0
    assert table_count(settings, "research_backend_invocations") == 0
    assert "AI_SUPPRESSED" in latest_authorization(settings)


def seed_due(settings: Settings, key: str) -> None:
    payload = {
        "actual": None,
        "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
    }
    lifecycle = compute_datum_lifecycle(
        "macro_actual",
        key,
        payload,
        settings=settings,
        now=NOW,
        fields_attempted=["actual"],
    )
    LifecycleRepository(settings, clock=lambda: NOW).upsert(
        lifecycle,
        payload=payload,
        work_status="READY",
    )


def test_scheduler_direct_scans_and_callback_do_not_create_authority(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    callback_calls: list[list[dict[str, Any]]] = []
    results = []
    for index in range(2):
        seed_due(settings, f"US:CPI:2026-0{index + 7}")
        results.append(
            scheduler.scan_due_items(
                owner=f"direct-scan-{index}",
                resolver=lambda _: {"status": "EXHAUSTED"},
                ai_enqueue=lambda items: callback_calls.append(items),
            )
        )
    assert [result["ai_invocations"] for result in results] == [0, 0]
    assert callback_calls == []
    assert table_count(settings, "ai_research_jobs") == 0
    assert table_count(settings, "research_runs") == 0
    assert table_count(settings, "research_backend_invocations") == 0
    assert "AI_SUPPRESSED" in latest_authorization(settings)


def test_disabled_scheduler_suppresses_ai_even_with_explicit_context(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=False,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
    )
    seed_due(settings, "US:CPI:2026-09")
    callback_calls: list[list[dict[str, Any]]] = []
    context = ExecutionContext.explicit_ai(
        correlation_id="disabled-scheduler",
        request_origin="research_scheduler",
        allow_live_providers=True,
    )
    result = ResearchSchedulerService(settings, clock=lambda: NOW).scan_due_items(
        owner="disabled-scheduler",
        resolver=lambda _: {"status": "EXHAUSTED"},
        ai_enqueue=lambda items: callback_calls.append(items),
        force=True,
        execution_context=context,
    )
    assert result["ai_invocations"] == 0
    assert callback_calls == []
    assert table_count(settings, "ai_research_jobs") == 0
    assert table_count(settings, "research_runs") == 0
    assert table_count(settings, "research_backend_invocations") == 0


def test_trusted_enabled_scheduler_entrypoint_passes_explicit_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    )
    seed_due(settings, "US:CPI:2026-10")
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    state = {
        "research_scheduler": scheduler,
        "lifecycle_due_resolver": SimpleNamespace(
            resolve=lambda _: {
                "status": "EXHAUSTED",
                "ai_eligible": True,
            }
        ),
    }
    result = asyncio.run(run_lifecycle_due_scan(state))
    assert result["ai_invocations"] == 1
    assert result["ai_jobs_created"] == 1
    assert table_count(settings, "ai_research_jobs") == 1
    assert table_count(settings, "research_backend_invocations") == 0


@pytest.mark.parametrize(
    "forbidden_key",
    [
        "chains",
        "contracts",
        "raw_chain",
        "raw_contracts",
        "raw_payload",
        "raw_payload_json",
        "request_headers",
        "response_headers",
        "authorization",
        "api_key",
        "token",
    ],
)
@pytest.mark.parametrize("nested", [False, True])
def test_small_consumer_sections_are_always_recursively_sanitized(
    forbidden_key: str,
    nested: bool,
) -> None:
    forbidden_value = "Bearer review-secret-token"
    unsafe = (
        {"safe": [{"safe": {forbidden_key: forbidden_value}}]}
        if nested
        else {forbidden_key: forbidden_value}
    )
    consumer = {
        "contract": "ai_trader_market_context_consumer",
        "schema_version": "2.1",
        "options_positioning": {
            "status": "NO_DATA",
            "provider": "TRADIER",
            "as_of": NOW.isoformat(),
            "freshness": "CURRENT",
            "quality": {"status": "VERIFIED"},
            "no_data_reason": "no_verified_contracts",
            **unsafe,
        },
        "lineage_note": "authorization Bearer another-review-secret",
    }
    assert len(json.dumps(consumer).encode("utf-8")) < 2_500
    _enforce_payload_limit(consumer)
    encoded = json.dumps(consumer, sort_keys=True).lower()
    assert not contains_key(consumer, forbidden_key)
    assert "review-secret-token" not in encoded
    assert "another-review-secret" not in encoded
    section = consumer["options_positioning"]
    assert section["provider"] == "TRADIER"
    assert section["as_of"] == NOW.isoformat()
    assert section["freshness"] == "CURRENT"
    assert section["quality"]["status"] == "VERIFIED"
    assert section["no_data_reason"] == "no_verified_contracts"
