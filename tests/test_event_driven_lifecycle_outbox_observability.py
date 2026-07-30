from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.core.redaction import redact_payload
from app.core.text_normalization import normalize_text
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.ai_trader_consumer_v2_service import _research
from app.services.data_lifecycle_service import attach_lifecycle_metadata
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
    material_changes,
    materiality_fingerprint,
    next_cftc_publication,
)
from app.services.execution_context import ExecutionContext
from app.services.market_context_hardening_service import _event_window_status
from app.services.market_context_outbox_service import MarketContextOutboxRepository
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.observability_contract_service import (
    DeterministicAnomalyDetector,
    ModelPricingService,
    TelemetryRepository,
)
from app.services.research_runtime_repository import _minimum_topic_coverage
from app.services.research_scheduler_service import ResearchSchedulerService
from scripts.replay_event_driven_forensics import replay


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 24, 12, tzinfo=UTC)


def cfg(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "model_pricing_path": ROOT / "config" / "model_pricing.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "environment": "test",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def lifecycle(
    settings: Settings,
    entity_type: str,
    key: str,
    value: dict[str, Any],
    *,
    now: datetime = NOW,
    **kwargs: Any,
):
    return compute_datum_lifecycle(
        entity_type,
        key,
        value,
        settings=settings,
        now=now,
        **kwargs,
    )


def _due_item(settings: Settings, key: str, entity_type: str = "vix") -> dict[str, Any]:
    contract = lifecycle(
        settings,
        entity_type,
        key,
        {
            "value": 20,
            "observed_at": (NOW - timedelta(hours=2)).isoformat(),
            "valid_until": (NOW - timedelta(minutes=1)).isoformat(),
        },
    )
    return LifecycleRepository(settings, clock=lambda: NOW).upsert(contract)


def _snapshot_row(conn: Any, snapshot_id: str, revision: int) -> None:
    stamp = NOW.isoformat()
    conn.execute(
        """
        INSERT INTO market_context_snapshots(
          snapshot_id,symbol,revision,generated_at,data_as_of,refresh_mode,
          debug_payload_json,consumer_payload_json,ai_status,source_job_id,
          checksum,created_at,audit_status
        ) VALUES (?,?,?,?,?,'test','{}','{}','NOT_REQUIRED',NULL,'hash',?,'ACTIVE')
        """,
        (snapshot_id, "MNQ", revision, stamp, stamp, stamp),
    )


def test_valid_data_is_fresh_and_requires_no_due_work(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    item = lifecycle(
        settings,
        "vix",
        "VIX",
        {"value": 17, "valid_until": (NOW + timedelta(minutes=5)).isoformat()},
    )
    assert item.freshness_state == "FRESH"
    assert item.trigger_class == "REFRESH_ON_TRIGGER"


def test_expired_data_becomes_due_before_ai(tmp_path: Path) -> None:
    item = _due_item(cfg(tmp_path), "VIX")
    assert item["work_status"] == "READY"
    assert item["freshness_state"] == "DUE"


def test_provider_resolution_results_in_zero_ai(tmp_path: Path) -> None:
    settings = cfg(tmp_path, lifecycle_due_scanner_enabled=True)
    _due_item(settings, "VIX")
    calls = {"provider": 0, "ai": 0}

    def resolver(_: dict[str, Any]) -> dict[str, Any]:
        calls["provider"] += 1
        return {"status": "RESOLVED", "next_refresh_at": (NOW + timedelta(hours=1)).isoformat()}

    result = ResearchSchedulerService(settings, clock=lambda: NOW).scan_due_items(
        owner="test",
        resolver=resolver,
        ai_enqueue=lambda _: calls.__setitem__("ai", calls["ai"] + 1),
        trigger_type="macro_actual",
    )
    assert result["resolver_evaluations"] == 1
    assert result["actual_provider_requests"] == result["provider_calls"] == 0
    assert (result["ai_invocations"], calls["ai"]) == (0, 0)


def test_unresolved_eligible_gap_invokes_ai_once(tmp_path: Path) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
    )
    _due_item(settings, "VIX")
    calls: list[list[dict[str, Any]]] = []
    result = ResearchSchedulerService(settings, clock=lambda: NOW).scan_due_items(
        owner="test",
        resolver=lambda _: {"status": "EXHAUSTED"},
        ai_enqueue=lambda items: calls.append(items),
        trigger_type="macro_actual",
        execution_context=ExecutionContext.explicit_ai(
            correlation_id="test",
            request_origin="research_scheduler",
        ),
    )
    assert result["ai_invocations"] == len(calls) == 1


def test_missing_resolver_never_falls_through_to_ai(tmp_path: Path) -> None:
    settings = cfg(tmp_path, lifecycle_due_scanner_enabled=True)
    _due_item(settings, "VIX")
    calls: list[Any] = []
    result = ResearchSchedulerService(settings, clock=lambda: NOW).scan_due_items(
        owner="test",
        ai_enqueue=lambda items: calls.append(items),
        trigger_type="macro_actual",
    )
    assert result["ai_invocations"] == 0
    assert calls == []


def test_no_data_negative_cache_blocks_repeat_attempt(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    stored = repository.record_no_data(
        "options_positioning",
        "MNQ:options_positioning",
        fields_attempted=["put_call"],
        sources_attempted=[],
        reason="no_fresh_verified_source",
        session_state="open",
        triggering_event="macro_actual",
    )
    cached = repository.negative_cache(
        "options_positioning",
        "MNQ:options_positioning",
        ["put_call"],
        session_state="open",
        now=NOW,
    )
    assert cached is not None
    assert cached["next_retry_at"] == stored["next_retry_at"]


def test_expired_negative_cache_allows_new_attempt(tmp_path: Path) -> None:
    settings = cfg(tmp_path, lifecycle_no_data_retry_seconds="60")
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    repository.record_no_data(
        "market_internals",
        "MNQ:market_internals",
        fields_attempted=["breadth"],
        sources_attempted=[],
        reason="no_data",
        session_state="open",
        triggering_event="macro_actual",
    )
    assert repository.negative_cache(
        "market_internals",
        "MNQ:market_internals",
        ["breadth"],
        session_state="open",
        now=NOW + timedelta(seconds=61),
    ) is None


def test_two_due_items_are_coalesced_into_one_ai_enqueue(tmp_path: Path) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
    )
    _due_item(settings, "VIX")
    _due_item(settings, "VVIX", "vvix")
    calls: list[Any] = []
    result = ResearchSchedulerService(settings, clock=lambda: NOW).scan_due_items(
        owner="test",
        resolver=lambda _: {"status": "EXHAUSTED"},
        ai_enqueue=lambda items: calls.append(items),
        trigger_type="macro_actual",
        execution_context=ExecutionContext.explicit_ai(
            correlation_id="test",
            request_origin="research_scheduler",
        ),
    )
    assert result["coalesced"] is True
    assert len(calls) == 1 and len(calls[0]) == 2


def test_vix_without_trigger_is_deferred(tmp_path: Path) -> None:
    settings = cfg(tmp_path, lifecycle_due_scanner_enabled=True)
    _due_item(settings, "VIX")
    result = ResearchSchedulerService(settings, clock=lambda: NOW).scan_due_items(
        owner="test",
        resolver=lambda _: {"status": "EXHAUSTED"},
    )
    assert result["deferred"] and result["ai_invocations"] == 0


def test_earnings_pre_release_is_fresh(tmp_path: Path) -> None:
    item = lifecycle(
        cfg(tmp_path),
        "earnings_schedule",
        "AMD:2026-08-04",
        {"ticker": "AMD", "event_at": "2026-08-04T21:00:00Z"},
    )
    assert item.freshness_state == "FRESH"


def test_past_earnings_without_actual_awaits_actual(tmp_path: Path) -> None:
    item = lifecycle(
        cfg(tmp_path),
        "earnings",
        "GOOGL:2026-07-22",
        {"ticker": "GOOGL", "event_at": "2026-07-22T20:00:00Z"},
    )
    assert item.freshness_state == "AWAITING_ACTUAL"
    assert item.next_retry_at is not None


def test_earnings_actual_is_triggering(tmp_path: Path) -> None:
    item = lifecycle(
        cfg(tmp_path),
        "earnings_actual",
        "AMD:2026-08-04",
        {"actual_eps": 1.0, "event_at": "2026-08-04T21:00:00Z"},
        now=datetime(2026, 8, 4, 22, tzinfo=UTC),
    )
    assert item.trigger_class == "TRIGGER"


def test_unknown_earnings_time_uses_configured_window(tmp_path: Path) -> None:
    settings = cfg(tmp_path, earnings_unknown_time_window_hours=18)
    item = lifecycle(
        settings,
        "earnings_schedule",
        "AMD:2026-08-04",
        {"ticker": "AMD", "date": "2026-08-04"},
    )
    assert item.event_at == "2026-08-04T22:00:00+00:00"


def test_pending_earnings_create_distinct_issuer_lifecycles(tmp_path: Path) -> None:
    output = attach_lifecycle_metadata(
        {
            "nasdaq_context": {
                "earnings": {
                    "released_earnings": [
                        {"ticker": "GOOGL", "event_at": "2026-07-22T20:00:00Z"},
                        {"ticker": "TSLA", "event_at": "2026-07-22T21:00:00Z"},
                    ]
                }
            }
        },
        settings=cfg(tmp_path),
        now=NOW,
    )
    items = output["nasdaq_context"]["earnings"]["released_earnings"]
    assert {item["lifecycle"]["entity_key"] for item in items} == {
        "GOOGL:2026-07-22",
        "TSLA:2026-07-22",
    }
    assert all(
        item["lifecycle"]["freshness_state"] == "AWAITING_ACTUAL"
        for item in items
    )


def test_cot_valid_until_next_configured_publication(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    item = lifecycle(
        settings,
        "cot",
        "NASDAQ",
        {"report_date": "2026-07-14", "open_interest": 1},
    )
    assert item.valid_until == next_cftc_publication(
        datetime(2026, 7, 14).date(),
        settings=settings,
        now=NOW,
    ).isoformat()


def test_monthly_actual_deadline_is_anchored_to_release_cadence(
    tmp_path: Path,
) -> None:
    release = NOW - timedelta(days=4)
    item = lifecycle(
        cfg(tmp_path),
        "macro_actual",
        "flash-services-pmi:2026-07",
        {
            "actual": 53.6,
            "release_at": release.isoformat(),
            "retrieved_at": NOW.isoformat(),
            "frequency": "monthly",
            "source": "SPGLOBAL",
        },
    )

    expected_deadline = (release + timedelta(days=45)).isoformat()
    assert item.freshness_state == "FRESH"
    assert item.valid_until == expected_deadline
    assert item.next_refresh_at == expected_deadline


def test_old_monthly_actual_is_not_refreshed_by_recent_retrieval(
    tmp_path: Path,
) -> None:
    release = NOW - timedelta(days=60)
    item = lifecycle(
        cfg(tmp_path),
        "macro_actual",
        "flash-services-pmi:old-release",
        {
            "actual": 49.2,
            "release_at": release.isoformat(),
            "retrieved_at": NOW.isoformat(),
            "frequency": "monthly",
            "source": "SPGLOBAL",
        },
    )

    assert item.freshness_state == "DUE"
    assert item.valid_until == (
        release + timedelta(days=45)
    ).isoformat()
    assert item.next_refresh_at == item.valid_until


def test_cftc_holiday_delay_is_configurable(tmp_path: Path) -> None:
    base = cfg(tmp_path)
    delayed = cfg(
        tmp_path,
        cftc_release_holidays="2026-07-24",
        cftc_release_delay_days=1,
    )
    regular = next_cftc_publication(
        datetime(2026, 7, 14).date(), settings=base, now=NOW
    )
    shifted = next_cftc_publication(
        datetime(2026, 7, 14).date(), settings=delayed, now=NOW
    )
    assert shifted > regular


def test_cot_metadata_only_is_not_minimum_coverage() -> None:
    claims = [
        {"metric_id": "cot_report_date"},
        {"metric_id": "cot_contract"},
    ]
    assert _minimum_topic_coverage("cot_positioning", claims) is False


def test_complete_cot_requires_open_interest_and_group_positions() -> None:
    claims = [
        {"metric_id": "cot_report_date"},
        {"metric_id": "cot_contract"},
        {"metric_id": "cot_open_interest"},
        {"metric_id": "asset_manager_long"},
        {"metric_id": "asset_manager_short"},
    ]
    assert _minimum_topic_coverage("cot_positioning", claims) is True


def test_complete_cot_claims_project_into_positioning(tmp_path: Path) -> None:
    repository = MarketContextSnapshotRepository(cfg(tmp_path))
    debug: dict[str, Any] = {}
    claims = [
        {"topic": "cot_positioning", "metric_id": "cot_report_date", "value": "2026-07-14", "lineage": []},
        {"topic": "cot_positioning", "metric_id": "cot_contract", "value": "209742", "lineage": []},
        {"topic": "cot_positioning", "metric_id": "cot_open_interest", "value": 1000, "lineage": []},
        {"topic": "cot_positioning", "metric_id": "asset_manager_long", "value": 600, "lineage": []},
        {"topic": "cot_positioning", "metric_id": "asset_manager_short", "value": 400, "lineage": []},
    ]
    repository._reconcile_cot(debug, claims)
    assert debug["positioning"]["status"] == "AVAILABLE"
    assert debug["positioning"]["asset_managers"] == {"long": 600, "short": 400}


def test_amd_confirmation_enriches_existing_issuer_event(tmp_path: Path) -> None:
    repository = MarketContextSnapshotRepository(cfg(tmp_path))
    debug = {
        "nasdaq_context": {
            "earnings": {
                "upcoming": [
                    {"ticker": "AMD", "date": "2026-08-04", "reliability": 0}
                ]
            }
        }
    }
    repository._reconcile_earnings(
        debug,
        [
            {
                "claim_id": "claim-amd",
                "field_semantics": "earnings_schedule",
                "symbol": "AMD",
                "issuer": "Advanced Micro Devices, Inc.",
                "event_at": "2026-08-04T21:00:00Z",
                "valid_until": "2026-08-05T21:00:00Z",
                "next_refresh_at": "2026-08-04T21:00:00Z",
                "lineage": [
                    {
                        "canonical_url": "https://ir.amd.com/news-events/ir-calendar",
                        "source_domain": "ir.amd.com",
                        "source_tier": 1,
                        "publisher": "AMD",
                    }
                ],
            }
        ],
    )
    item = debug["nasdaq_context"]["earnings"]["upcoming"][0]
    assert item["event_at"] == "2026-08-04T21:00:00Z"
    assert item["source_tier"] == 1 and item["reliability"] == 0.97
    assert item["confirmation_status"] == "VERIFIED"


def test_material_fingerprint_ignores_volatile_timestamps() -> None:
    first = {"value": 1, "generated_at": "2026-01-01T00:00:00Z"}
    second = {"value": 1, "generated_at": "2026-02-01T00:00:00Z"}
    assert materiality_fingerprint(first) == materiality_fingerprint(second)


def test_material_diff_detects_only_changed_sections() -> None:
    sections, changes = material_changes(
        {"risk": {"VIX": 17}, "macro": {"CPI": 2}},
        {"risk": {"VIX": 18}, "macro": {"CPI": 2}},
    )
    assert sections == ["risk"]
    assert changes[0]["section"] == "risk"


def test_non_triggering_change_emits_no_outbox_event(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    repository = MarketContextOutboxRepository(settings, clock=lambda: NOW)
    with connect_sqlite(settings.database_path) as conn:
        _snapshot_row(conn, "snapshot-1", 1)
        event = repository.emit_in_transaction(
            conn,
            trigger_type="vix",
            trigger_entity="VIX",
            snapshot_id="snapshot-1",
            snapshot_revision=1,
            current_payload={"risk": {"VIX": 18}},
            previous_snapshot_id=None,
            previous_payload={"risk": {"VIX": 17}},
            trace_id=None,
            correlation_id=None,
            data_as_of=NOW.isoformat(),
            created_at=NOW.isoformat(),
        )
        conn.commit()
    assert event is None and repository.list_events() == []


def test_material_trigger_emits_one_idempotent_outbox_event(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    repository = MarketContextOutboxRepository(settings, clock=lambda: NOW)
    with connect_sqlite(settings.database_path) as conn:
        _snapshot_row(conn, "snapshot-1", 1)
        kwargs = {
            "trigger_type": "macro_actual",
            "trigger_entity": "CPI",
            "snapshot_id": "snapshot-1",
            "snapshot_revision": 1,
            "current_payload": {"macro": {"CPI": 2.1}},
            "previous_snapshot_id": "snapshot-0",
            "previous_payload": {"macro": {"CPI": 2.0}},
            "trace_id": "trace-1",
            "correlation_id": "corr-1",
            "data_as_of": NOW.isoformat(),
            "created_at": NOW.isoformat(),
        }
        first = repository.emit_in_transaction(conn, **kwargs)
        second = repository.emit_in_transaction(
            conn,
            **{
                **kwargs,
                "created_at": (NOW + timedelta(minutes=1)).isoformat(),
            },
        )
        conn.commit()
    assert first and second and first["event_id"] == second["event_id"]
    assert len(repository.list_events()) == 1


def test_outbox_acknowledgement_requires_matching_key(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    repository = MarketContextOutboxRepository(settings, clock=lambda: NOW)
    with connect_sqlite(settings.database_path) as conn:
        _snapshot_row(conn, "snapshot-1", 1)
        event = repository.emit_in_transaction(
            conn,
            trigger_type="official_correction",
            trigger_entity="CPI",
            snapshot_id="snapshot-1",
            snapshot_revision=1,
            current_payload={"macro": {"CPI": 2.1}},
            previous_snapshot_id="snapshot-0",
            previous_payload={"macro": {"CPI": 2.0}},
            trace_id=None,
            correlation_id=None,
            data_as_of=NOW.isoformat(),
            created_at=NOW.isoformat(),
        )
        conn.commit()
    assert event
    acknowledged = repository.acknowledge(
        event["event_id"],
        consumer_id="future-ai-trader",
        idempotency_key=event["idempotency_key"],
    )
    assert acknowledged["delivery_status"] == "ACKNOWLEDGED"


def test_lease_recovery_claims_expired_lease(tmp_path: Path) -> None:
    settings = cfg(tmp_path, lifecycle_due_lease_seconds=30)
    item = _due_item(settings, "VIX")
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    assert repository.claim_due(owner="first", now=NOW)
    recovered = repository.claim_due(
        owner="second", now=NOW + timedelta(seconds=31)
    )
    assert recovered[0]["item_id"] == item["item_id"]
    assert recovered[0]["lease_owner"] == "second"


def test_heartbeat_extends_owned_lease(tmp_path: Path) -> None:
    settings = cfg(tmp_path, lifecycle_due_lease_seconds=30)
    _due_item(settings, "VIX")
    repository = LifecycleRepository(settings, clock=lambda: NOW)
    item = repository.claim_due(owner="worker", now=NOW)[0]
    assert repository.heartbeat(
        item["item_id"], owner="worker", now=NOW + timedelta(seconds=10)
    )


def test_scheduler_disabled_by_default(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    assert settings.lifecycle_due_scanner_enabled is False
    result = ResearchSchedulerService(settings, clock=lambda: NOW).scan_due_items(
        owner="test"
    )
    assert result["status"] == "DISABLED"


def test_cli_cost_is_explicitly_unavailable() -> None:
    cost = ModelPricingService(ROOT / "config" / "model_pricing.json").estimate(
        backend="codex_cli",
        model="codex",
        input_tokens=10,
        cached_tokens=2,
        output_tokens=3,
    )
    assert cost["cost"] is None
    assert cost["billing_basis"] == "tokens_observed_codex_cli_pricing_unavailable"


def test_api_cost_uses_versioned_config(tmp_path: Path) -> None:
    path = tmp_path / "pricing.json"
    path.write_text(
        json.dumps(
            {
                "version": "test-v1",
                "currency": "USD",
                "models": [
                    {
                        "backend": "openai_api",
                        "model": "fixture",
                        "input_per_million": 1,
                        "cached_input_per_million": 0.5,
                        "output_per_million": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    result = ModelPricingService(path).estimate(
        backend="openai_api",
        model="fixture",
        input_tokens=1_000_000,
        cached_tokens=500_000,
        output_tokens=1_000_000,
    )
    assert result["cost"] == 2.75 and result["cost_status"] == "estimated"


def test_secret_redaction_is_key_aware() -> None:
    assert redact_payload(
        {"Authorization": "Bearer secret-value", "nested": {"api_key": "abcdefgh"}}
    ) == {"Authorization": "<redacted>", "nested": {"api_key": "<redacted>"}}


def test_structured_telemetry_propagates_identifiers(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    event = TelemetryRepository(settings, clock=lambda: NOW).emit(
        "provider_call",
        identifiers={"trace_id": "trace", "child_run_id": "run"},
        decision_summary="provider attempted\nsafely",
        payload={"status": "EXHAUSTED"},
    )
    assert event["trace_id"] == "trace" and event["child_run_id"] == "run"
    assert "\n" not in str(event["decision_summary"])


def test_anomaly_fingerprint_is_deduplicated(tmp_path: Path) -> None:
    detector = DeterministicAnomalyDetector(cfg(tmp_path), clock=lambda: NOW)
    first = detector.detect({"job_status": "RUNNING", "stalled": True, "job_id": "j"})[0]
    second = detector.detect({"job_status": "RUNNING", "stalled": True, "job_id": "j"})[0]
    assert first["fingerprint"] == second["fingerprint"]
    assert second["occurrence_count"] == 2


def test_anomaly_detector_covers_accounting_and_outbox_lag(tmp_path: Path) -> None:
    incidents = DeterministicAnomalyDetector(
        cfg(tmp_path), clock=lambda: NOW
    ).detect(
        {
            "rejected_claims": 19,
            "rejection_reason_count": 11,
            "outbox_lag_seconds": 301,
        }
    )
    assert {item["category"] for item in incidents} == {
        "outbox_lag",
        "rejection_accounting_mismatch",
    }


def test_execution_complete_does_not_imply_coverage_complete() -> None:
    research = _research(
        {
            "status": "SUCCEEDED",
            "coverage_score": 0.2,
            "missing_topics": ["news"],
            "blocking_gaps": ["missing_topic:news"],
        }
    )
    assert research["execution_complete"] is True
    assert research["coverage_complete"] is False
    assert research["research_complete"] is False


def test_future_legacy_timestamp_moves_to_audit() -> None:
    output = _event_window_status(
        {"legacy": {"checked_at_utc": "2099-01-01T00:00:00Z"}},
        events_today={"status": "NO_EVENTS_SCHEDULED"},
        now=NOW,
    )
    assert "checked_at_utc" not in output["legacy"]
    assert output["audit"]["quarantined_future_timestamps"]


def test_smoke_mojibake_sequence_is_repaired() -> None:
    assert normalize_text("Companyā\u0080\u0099s results") == "Company's results"


def test_valid_unicode_is_not_corrupted() -> None:
    value = "Mercati – società 日本語"
    assert normalize_text(value) == value


def test_migration_21_is_idempotent_and_preserves_rows(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    first = migrate_database(settings.database_path)
    with connect_sqlite(settings.database_path) as conn:
        conn.execute(
            """
            INSERT INTO anomaly_incidents(
              incident_id,fingerprint,severity,category,first_seen_at,
              last_seen_at,occurrence_count,trace_ids_json,entity_ids_json,
              evidence_json,status,resolution_json
            ) VALUES ('i','f','LOW','fixture',?,?,1,'[]','[]','{}','OPEN',NULL)
            """,
            (NOW.isoformat(), NOW.isoformat()),
        )
        conn.commit()
    second = migrate_database(settings.database_path)
    with connect_sqlite(settings.database_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM anomaly_incidents").fetchone()[0]
    assert first["schema_version"] == second["schema_version"] == 22
    assert count == 1


def test_consumer_artifact_is_byte_identical() -> None:
    path = ROOT / "ai-trader-consumer-payload.json"
    digest = hashlib.sha256(path.read_bytes()).hexdigest().upper()
    assert digest == "BCED28DECDF98D65AF9843C3CF3FF23DAB0A164C721B8BEF9B3E0D7697699DD4"


def test_backup_artifact_is_byte_identical() -> None:
    path = ROOT / "data" / "market_data_service-pre-agentic-domains-20260724.sqlite"
    digest = hashlib.sha256(path.read_bytes()).hexdigest().upper()
    assert digest == "98F3DF7D08B15980C5EEBA029466FA6AEAF3ED4F4B44CA60E75F698E44F42059"


def test_no_trading_or_order_surface_added() -> None:
    source = (ROOT / "app" / "api" / "routes.py").read_text(encoding="utf-8")
    route_lines = [line.lower() for line in source.splitlines() if line.lstrip().startswith("@router.")]
    assert all("/trade" not in line and "/order" not in line for line in route_lines)


def test_shared_telemetry_schema_is_valid_json() -> None:
    schema = json.loads(
        (ROOT / "config" / "service_telemetry_event.schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert schema["$schema"].endswith("schema")
    assert "trace_id" in schema["properties"]


def test_offline_smoke_replay_has_no_live_calls() -> None:
    result = replay(ROOT / "data" / "market-research-smoke-agentic-domains-20260724")
    assert result["passed"] is True
    assert result["findings"]["live_calls_executed"] == 0
