from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.deps import get_enrichment_orchestrator
from app.api.routes import router
from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.event_calendar_window_service import build_event_calendar_window
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import (
    SECTION_NAMES,
    MarketContextSyncService,
    SyncContractError,
    canonical_json,
    material_fingerprint,
)
from app.services.market_context_sync_refresh_worker import (
    MarketContextSyncRefreshWorker,
)


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 25, 20, 6, 20, tzinfo=UTC)


def cfg(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        source_policy_path=ROOT / "config" / "source_policy.json",
        model_pricing_path=ROOT / "config" / "model_pricing.json",
        ai_job_workspace_root=tmp_path / "jobs",
        codex_workspace_dir=tmp_path / "codex",
        environment="test",
    )


def record(record_id: str, value: Any) -> dict[str, Any]:
    return {
        "record_id": record_id,
        "provider_record_id": record_id,
        "related_occurrence_id": None,
        "version": 1,
        "provider": "BLS",
        "source": "BLS",
        "source_url": "https://www.bls.gov/news.release/",
        "source_classification": "official_source",
        "reliability": 0.98,
        "published_at": NOW.isoformat(),
        "event_at": NOW.isoformat(),
        "retrieved_at": NOW.isoformat(),
        "reference_period": "2026-07",
        "valid_from": NOW.isoformat(),
        "valid_until": (NOW + timedelta(days=7)).isoformat(),
        "freshness": "CURRENT",
        "lifecycle_status": "PUBLISHED",
        "validation_status": "accepted",
        "warnings": [],
        "rejection_reasons": [],
        "payload": {"value": value},
    }


def test_record_count_keeps_same_provider_id_from_distinct_sources() -> None:
    from app.services.market_context_sync_service import record_count_for

    first = record("shared-id", 1)
    second = {
        **record("shared-id", 2),
        "provider": "FRED",
        "source": "Federal Reserve Bank of St. Louis",
    }

    assert record_count_for([first, second]) == 2


def debug_payload(
    *,
    news: list[dict[str, Any]] | None = None,
    telemetry_marker: str = "first",
) -> dict[str, Any]:
    calendar_event = {
        **record("event-1", 1),
        "occurrence_id": "event-1",
        "event_id": "event-1",
        "name": "Employment Situation",
        "scheduled_at": "2026-07-24T08:30:00-04:00",
        "release_status": "PUBLISHED",
        "impact": "HIGH",
        "actual": "1",
    }
    return {
        "symbol": "MNQ",
        "generated_at_utc": NOW.isoformat(),
        "data_as_of": NOW.isoformat(),
        "macro_snapshot": {"status": "AVAILABLE", "series": [record("macro-1", 1)]},
        "macro_actuals": {"status": "AVAILABLE", "items": [calendar_event]},
        "event_calendar": {
            "critical_macro_events": [calendar_event],
            "fed_communications": [],
            "other_economic_events": [],
        },
        "fomc_context": {"status": "AVAILABLE", "items": [record("fed-1", 1)]},
        "rates_context": {"status": "AVAILABLE", "items": [record("rate-1", 1)]},
        "rates_expectations": {"status": "AVAILABLE", "meetings": [record("meeting-1", 1)]},
        "risk_context": {
            "status": "AVAILABLE",
            "vix": {**record("vix-1", 18.2), "status": "AVAILABLE"},
        },
        "positioning": {"status": "AVAILABLE", "items": [record("cot-1", 1)]},
        "nasdaq_context": {
            "status": "AVAILABLE",
            "qqq_holdings": {"status": "AVAILABLE", "holdings": [record("qqq-1", 1)]},
        },
        "news_context": {
            "status": "AVAILABLE",
            "articles": news if news is not None else [record("news-1", "alpha")],
            "telemetry": {"marker": telemetry_marker},
        },
        "market_schedule": {
            "status": "AVAILABLE",
            "timezone": "America/New_York",
            "nasdaq_cash_session": {
                "status": "weekend",
                "source": "Nasdaq",
                "validation": {"status": "accepted"},
            },
            "mnq_futures_session": {
                "status": "weekend",
                "source": "CME Group",
                "validation": {"status": "accepted"},
            },
        },
        "options_positioning": {"status": "AVAILABLE", "items": [record("option-1", 1)]},
        "market_internals": {"status": "AVAILABLE", "items": [record("internal-1", 1)]},
        "cross_asset_context": {"status": "AVAILABLE", "items": [record("cross-1", 1)]},
        "corporate_events": {"status": "AVAILABLE", "items": [record("earning-1", 1)]},
        "earnings_intelligence": {
            "status": "AVAILABLE",
            "items": [record("earning-intel-1", 1)],
        },
        "geopolitical_regulatory_risk": {
            "status": "AVAILABLE",
            "items": [record("geo-1", 1)],
        },
    }


def save_snapshot(
    settings: Settings,
    *,
    revision: int,
    payload: dict[str, Any],
) -> None:
    snapshot_id = f"mcs-sync-{revision}"
    payload = {
        **payload,
        "snapshot_id": snapshot_id,
        "snapshot_revision": revision,
    }
    MarketContextSnapshotRepository(settings).save(
        snapshot_id=snapshot_id,
        revision=revision,
        symbol="MNQ",
        refresh_mode="offline-test",
        debug_payload=payload,
        consumer_payload={
            "contract": "ai_trader_market_context_consumer",
            "schema_version": "2.1",
            "snapshot_id": snapshot_id,
            "snapshot_revision": revision,
            "data_as_of": NOW.isoformat(),
        },
        ai_status="NOT_REQUIRED",
    )


def test_manifest_full_and_section_revisions_are_consistent(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    save_snapshot(settings, revision=1, payload=debug_payload())
    save_snapshot(
        settings,
        revision=2,
        payload=debug_payload(telemetry_marker="telemetry-only-change"),
    )
    changed_news = [record("news-1", "materially changed")]
    save_snapshot(settings, revision=3, payload=debug_payload(news=changed_news))
    service = MarketContextSyncService(settings)

    first = service.manifest(snapshot_revision=1)
    second = service.manifest(snapshot_revision=2)
    third = service.manifest(snapshot_revision=3)
    full = service.full(snapshot_revision=3)

    assert tuple(sorted(first["sections"])) == tuple(sorted(SECTION_NAMES))
    assert first["sections"]["news"]["fingerprint"] == second["sections"]["news"]["fingerprint"]
    assert first["sections"]["news"]["section_revision"] == second["sections"]["news"]["section_revision"]
    assert third["sections"]["news"]["section_revision"] == second["sections"]["news"]["section_revision"] + 1
    assert third["sections"]["news"]["fingerprint"] != second["sections"]["news"]["fingerprint"]
    assert full["manifest"] == third
    assert full["snapshot_revision"] == 3
    assert full["readiness"]["calculated_from_delivered_payload"] is True
    assert full["sections"]["news"]["sync"]["fingerprint"] == third["sections"]["news"]["fingerprint"]
    assert full["checksum"]
    assert full["payload_size_bytes"] == len(canonical_json(full).encode("utf-8"))


def test_order_independent_record_fingerprint_and_exact_record_count(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    rows = [record("news-a", "a"), record("news-b", "b")]
    save_snapshot(settings, revision=1, payload=debug_payload(news=rows))
    save_snapshot(settings, revision=2, payload=debug_payload(news=list(reversed(rows))))
    service = MarketContextSyncService(settings)
    one = service.manifest(snapshot_revision=1)["sections"]["news"]
    two = service.manifest(snapshot_revision=2)["sections"]["news"]
    assert one["fingerprint"] == two["fingerprint"]
    assert one["section_revision"] == two["section_revision"]
    assert one["record_count"] == two["record_count"] == 2
    assert material_fingerprint(rows) == material_fingerprint(list(reversed(rows)))


def test_sync_plan_full_none_selective_gap_and_producer_unavailable(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    save_snapshot(settings, revision=1, payload=debug_payload())
    changed = debug_payload(news=[record("news-2", "new")])
    changed["market_internals"] = {"status": "UNAVAILABLE", "reason": "provider_down"}
    save_snapshot(settings, revision=2, payload=changed)
    service = MarketContextSyncService(settings)
    manifest = service.manifest()

    full = service.plan(
        {
            "consumer_id": "ai-trader",
            "request_id": "full",
            "required_sections": ["news"],
            "known_snapshot_revision": None,
            "known_sections": {},
        }
    )
    assert full["sync_mode"] == "FULL"
    current = manifest["sections"]["news"]
    none = service.plan(
        {
            "consumer_id": "ai-trader",
            "request_id": "none",
            "required_sections": ["news"],
            "known_snapshot_revision": 2,
            "known_sections": {
                "news": {
                    "section_revision": current["section_revision"],
                    "fingerprint": current["fingerprint"],
                }
            },
        }
    )
    assert none["sync_mode"] == "NONE"
    selective = service.plan(
        {
            "consumer_id": "ai-trader",
            "request_id": "selective",
            "required_sections": ["news", "rates", "market_internals"],
            "known_snapshot_revision": 1,
            "known_sections": {
                "news": {"section_revision": 1, "fingerprint": "old"},
                "rates": {
                    "section_revision": manifest["sections"]["rates"]["section_revision"],
                    "fingerprint": manifest["sections"]["rates"]["fingerprint"],
                },
            },
        }
    )
    assert selective["sync_mode"] == "SELECTIVE"
    assert selective["sections_to_fetch"][0]["classification"] == "MISSING_AT_CONSUMER"
    assert selective["unavailable_at_producer"][0]["section"] == "market_internals"
    gap = service.plan(
        {
            "consumer_id": "ai-trader",
            "request_id": "gap",
            "required_sections": ["news"],
            "known_snapshot_revision": 999,
            "known_sections": {"news": {"section_revision": 1}},
        }
    )
    assert gap["sync_mode"] == "FULL"
    assert gap["requires_full_resync"] is True


def test_full_and_selective_preserve_payload_over_one_megabyte(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    rows = [
        record(f"news-{index}", {"body": "x" * 1_100, "ordinal": index})
        for index in range(1_000)
    ]
    save_snapshot(settings, revision=1, payload=debug_payload(news=rows))
    service = MarketContextSyncService(settings)
    full = service.full()
    selective = service.sections(
        consumer_id="ai-trader",
        target_snapshot_revision=1,
        sections=["news"],
        include_lineage=True,
    )
    encoded = json.dumps(full, ensure_ascii=False, separators=(",", ":")).encode()
    delivered = selective["sections"]["news"]["context"]["articles"]

    assert len(encoded) > 1_000_000
    assert len(delivered) == len(rows)
    assert [item["record_id"] for item in delivered] == [
        item["record_id"] for item in rows
    ]
    assert "compacted_item_count" not in encoded.decode()
    assert selective["snapshot_revision"] == 1
    unavailable = service.sections(
        consumer_id="ai-trader",
        target_snapshot_revision=999,
        sections=["news"],
    )
    assert unavailable["status"] == "RESYNC_REQUIRED"


def test_section_whitelist_rejects_unknown_queries(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    save_snapshot(settings, revision=1, payload=debug_payload())
    with pytest.raises(SyncContractError, match="unknown_sections"):
        MarketContextSyncService(settings).sections(
            consumer_id="ai-trader",
            target_snapshot_revision=1,
            sections=["sqlite_master"],
        )


def test_refresh_single_flight_concurrency_overlap_and_persistent_fanout(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    save_snapshot(settings, revision=1, payload=debug_payload())
    service = MarketContextSyncService(settings)

    def request(index: int) -> tuple[int, dict[str, Any]]:
        return service.request_refresh(
            {
                "consumer_id": f"consumer-{index}",
                "request_id": f"request-{index}",
                "reason": "MARKET_TRIGGER",
                "required_sections": ["news", "vix"],
            }
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(request, range(10)))
    work_ids = {result["work_id"] for _, result in results}
    assert len(work_ids) == 1

    _, overlap = service.request_refresh(
        {
            "consumer_id": "ai-trader",
            "request_id": "overlap",
            "reason": "MARKET_TRIGGER",
            "required_sections": ["news", "vix", "market_internals"],
        }
    )
    assert overlap["work_id"] in work_ids
    assert overlap["attached_to_existing_work"] is True
    status = service.work_status(overlap["work_id"])
    assert set(status["sections"]) == {"news", "vix", "market_internals"}
    assert len(status["waiters"]) == 11
    with connect_sqlite(settings.database_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM market_context_sync_refresh_work"
            ).fetchone()[0]
            == 1
        )


def test_refresh_ready_backoff_and_restart_recovery(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    save_snapshot(settings, revision=1, payload=debug_payload())
    service = MarketContextSyncService(settings)
    status_code, ready = service.request_refresh(
        {
            "consumer_id": "ai-trader",
            "request_id": "ready",
            "reason": "CONTEXT_REQUIRED",
            "required_sections": ["news"],
        }
    )
    assert status_code == 200
    assert ready["status"] == "READY"

    _, pending = service.request_refresh(
        {
            "consumer_id": "ai-trader",
            "request_id": "backoff-owner",
            "reason": "MARKET_TRIGGER",
            "required_sections": ["news"],
        }
    )
    next_retry = (NOW + timedelta(minutes=5)).isoformat()
    with connect_sqlite(settings.database_path) as conn:
        conn.execute(
            """
            UPDATE market_context_sync_refresh_work
            SET status='WAITING_BACKOFF',next_retry_at=?
            WHERE work_id=?
            """,
            (next_retry, pending["work_id"]),
        )
        conn.commit()
    restarted = MarketContextSyncService(settings)
    _, attached = restarted.request_refresh(
        {
            "consumer_id": "other",
            "request_id": "backoff-waiter",
            "reason": "MARKET_TRIGGER",
            "required_sections": ["news"],
        }
    )
    assert attached["status"] == "WAITING_BACKOFF"
    assert attached["work_id"] == pending["work_id"]
    assert attached["new_job_created"] is False


def test_scheduler_worker_commits_and_completes_durable_refresh(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="test_seed",
        debug_payload=debug_payload(),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    service = MarketContextSyncService(settings)
    status_code, queued = service.request_refresh(
        {
            "consumer_id": "ai-trader",
            "request_id": "worker-request",
            "reason": "MARKET_TRIGGER",
            "required_sections": ["news", "vix", "market_internals"],
        }
    )

    class Runtime:
        def enrich_market_context_sync(
            self,
            contract: dict[str, Any],
            *,
            refresh: str,
            trigger_type: str | None,
        ) -> dict[str, Any]:
            assert refresh == "auto"
            assert trigger_type == "breaking_news"
            return {
                **contract,
                "news_context": {
                    **dict(contract["news_context"]),
                    "articles": [
                        *list(contract["news_context"]["articles"]),
                        record("news-2", "new material news"),
                    ],
                },
            }

    assert status_code == 202
    completed = MarketContextSyncRefreshWorker(
        settings,
        deterministic_runtime=Runtime(),
    ).run_once(owner="test-sync-worker")
    assert completed["status"] == "COMPLETED"
    assert completed["work_id"] == queued["work_id"]
    assert completed["snapshot_revision"] == 2
    assert service.work_status(queued["work_id"])["status"] == "COMPLETED"
    with connect_sqlite(settings.database_path) as conn:
        outbox = conn.execute(
            "SELECT changed_sections_json FROM market_context_outbox"
        ).fetchone()
    assert "news" in json.loads(outbox["changed_sections_json"])


def test_scheduler_worker_failure_enters_persistent_backoff(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="test_seed",
        debug_payload=debug_payload(),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    service = MarketContextSyncService(settings)
    _, queued = service.request_refresh(
        {
            "consumer_id": "ai-trader",
            "request_id": "worker-backoff",
            "reason": "MARKET_TRIGGER",
            "required_sections": ["news"],
        }
    )

    class Runtime:
        def enrich_market_context_sync(self, *_args, **_kwargs):
            raise RuntimeError("provider unavailable")

    waiting = MarketContextSyncRefreshWorker(
        settings,
        deterministic_runtime=Runtime(),
        clock=lambda: NOW,
    ).run_once(owner="test-sync-worker")
    assert waiting["status"] == "WAITING_BACKOFF"
    persisted = service.work_status(queued["work_id"])
    assert persisted["status"] == "WAITING_BACKOFF"
    assert persisted["error"] == {
        "attempt": 1,
        "code": "SYNC_REFRESH_WORK_FAILED",
        "error_type": "RuntimeError",
    }


def test_running_generation_is_immutable_and_expired_lease_recovers(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    save_snapshot(settings, revision=1, payload=debug_payload())
    service = MarketContextSyncService(settings)
    _, pending = service.request_refresh(
        {
            "consumer_id": "first",
            "request_id": "generation-2",
            "reason": "MARKET_TRIGGER",
            "required_sections": ["news", "vix"],
        }
    )
    claimed = service.claim_next_refresh_work(owner="worker-a")
    assert claimed["work_id"] == pending["work_id"]
    assert claimed["status"] == "RUNNING"

    _, next_generation = service.request_refresh(
        {
            "consumer_id": "second",
            "request_id": "generation-3",
            "reason": "MARKET_TRIGGER",
            "required_sections": ["news", "vix", "market_internals"],
        }
    )
    assert next_generation["work_id"] != pending["work_id"]
    assert next_generation["target_generation"] == 3
    assert next_generation["sections"] == ["market_internals"]

    save_snapshot(
        settings,
        revision=2,
        payload=debug_payload(news=[record("news-2", "generation-2")]),
    )
    completed = service.complete_refresh_work(
        pending["work_id"],
        owner="worker-a",
        snapshot_id="mcs-sync-2",
    )
    assert completed["status"] == "COMPLETED"
    assert completed["target_snapshot_revision"] == 2

    claimed_next = service.claim_next_refresh_work(
        owner="crashed-worker",
        lease_seconds=1,
    )
    assert claimed_next["work_id"] == next_generation["work_id"]
    with connect_sqlite(settings.database_path) as conn:
        conn.execute(
            """
            UPDATE market_context_sync_refresh_work
            SET lease_expires_at='2000-01-01T00:00:00+00:00'
            WHERE work_id=?
            """,
            (next_generation["work_id"],),
        )
        conn.commit()
    recovered = MarketContextSyncService(settings).claim_next_refresh_work(
        owner="worker-after-restart"
    )
    assert recovered["work_id"] == next_generation["work_id"]
    assert recovered["lease_owner"] == "worker-after-restart"


def test_changes_notification_and_idempotent_ack_survive_restart(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    save_snapshot(settings, revision=1, payload=debug_payload())
    save_snapshot(
        settings,
        revision=2,
        payload=debug_payload(news=[record("news-2", "changed")]),
    )
    service = MarketContextSyncService(settings)
    manifest = service.manifest()
    changes = service.changes(since_revision=1)
    assert changes["requires_full_resync"] is False
    assert {item["section"] for item in changes["changed_sections"]} == {"news"}

    delivery_id = "delivery-sync-2"
    with connect_sqlite(settings.database_path) as conn:
        conn.execute(
            """
            INSERT INTO market_context_outbox(
              event_id,event_type,trigger_type,snapshot_id,snapshot_revision,
              previous_snapshot_id,changed_sections_json,material_changes_json,
              created_at,delivery_status,attempt_count,idempotency_key,
              payload_hash,base_revision,triggers_json,manifest_url,changes_url
            ) VALUES (?,?,?,?,?,?,'["news"]','[]',?,'PENDING',0,?,?,1,?,?,?)
            """,
            (
                delivery_id,
                "MARKET_CONTEXT_UPDATED",
                "breaking_news",
                "mcs-sync-2",
                2,
                "mcs-sync-1",
                NOW.isoformat(),
                "delivery-idempotency",
                "payload-hash",
                json.dumps(
                    [
                        {
                            "type": "NEW_MATERIAL_NEWS",
                            "entity_id": "news-2",
                            "occurred_at": NOW.isoformat(),
                        }
                    ]
                ),
                "/market-context/mnq/sync/manifest",
                "/market-context/mnq/sync/changes?since_revision=1",
            ),
        )
        conn.commit()
    notification = service.notification(delivery_id)
    assert notification["event_type"] == "MARKET_CONTEXT_UPDATED"
    assert notification["base_revision"] == 1
    assert notification["target_revision"] == 2
    failed_attempt = service.record_delivery_attempt(
        delivery_id=delivery_id,
        consumer_id="ai-trader",
        status="FAILED",
        next_retry_at=(NOW + timedelta(minutes=1)).isoformat(),
        error={"code": "temporary"},
    )
    notified_attempt = service.record_delivery_attempt(
        delivery_id=delivery_id,
        consumer_id="ai-trader",
        status="NOTIFIED",
    )
    assert failed_attempt["attempt_number"] == 1
    assert notified_attempt["attempt_number"] == 2
    section_revisions = {
        name: details["section_revision"]
        for name, details in manifest["sections"].items()
    }
    ack_payload = {
        "consumer_id": "ai-trader",
        "delivery_id": delivery_id,
        "snapshot_revision": 2,
        "status": "PERSISTED",
        "section_revisions": section_revisions,
        "acknowledged_at": NOW.isoformat(),
    }
    first = service.acknowledge(ack_payload)
    second = MarketContextSyncService(settings).acknowledge(ack_payload)
    assert first["idempotent_replay"] is False
    assert second["idempotent_replay"] is True
    state = service.consumer_state("ai-trader")
    assert state["last_snapshot_revision_acknowledged"] == 2
    assert state["pending_delivery_count"] == 0

    with pytest.raises(SyncContractError, match="delivery_not_found"):
        service.acknowledge({**ack_payload, "delivery_id": "unknown"})
    with pytest.raises(SyncContractError, match="ack_snapshot_revision_mismatch"):
        service.acknowledge(
            {
                **ack_payload,
                "consumer_id": "other",
                "snapshot_revision": 1,
            }
        )


def test_calendar_retains_every_event_and_counts_match(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    events = []
    for index in range(120):
        events.append(
            {
                "occurrence_id": f"event-{index}",
                "event_id": f"event-{index}",
                "name": f"Event {index}",
                "scheduled_at": (
                    datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
                    + timedelta(hours=index % 120)
                ).isoformat(),
                "impact": "LOW",
                "source": "BLS",
                "source_url": "https://www.bls.gov/schedule/",
                "validation_status": "accepted",
            }
        )
    window = build_event_calendar_window(
        {
            "event_calendar": {
                "critical_macro_events": events,
                "fed_communications": [],
                "other_economic_events": [],
            }
        },
        settings=settings,
        now=NOW,
    )
    bucket_events = sum(
        len(window[name]["events"])
        for name in ("previous_week", "current_week", "next_week")
    )
    bucket_counts = sum(
        window[name]["event_count"]
        for name in ("previous_week", "current_week", "next_week")
    )
    assert window["coverage"]["status"] == "COMPLETE"
    assert window["coverage"]["omitted_count"] == 0
    assert window["counts"]["total"] == window["coverage"]["retained_count"]
    assert window["counts"]["total"] == bucket_counts == bucket_events


def test_migration_21_is_additive_and_idempotent(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    first = migrate_database(settings.database_path)
    second = migrate_database(settings.database_path)
    assert first["schema_version"] == second["schema_version"] == 21
    assert first["applied"][-1] == "021_market_context_sync_producer_protocol"
    assert second["applied"] == []


def test_versioned_sync_http_contract(tmp_path: Path) -> None:
    settings = cfg(tmp_path)
    save_snapshot(settings, revision=1, payload=debug_payload())
    api = FastAPI()
    api.include_router(router)
    api.dependency_overrides[get_enrichment_orchestrator] = lambda: (
        SimpleNamespace(settings=settings)
    )
    with TestClient(api) as client:
        manifest = client.get("/market-context/mnq/sync/manifest")
        full = client.get("/market-context/mnq/sync/full")
        plan = client.post(
            "/market-context/mnq/sync/plan",
            json={
                "consumer_id": "ai-trader",
                "request_id": "http-plan",
                "required_sections": ["news"],
                "known_snapshot_revision": None,
                "known_sections": {},
            },
        )
        selective = client.post(
            "/market-context/mnq/sync/sections",
            json={
                "consumer_id": "ai-trader",
                "target_snapshot_revision": 1,
                "sections": ["news"],
                "include_lineage": True,
            },
        )
        changes = client.get(
            "/market-context/mnq/sync/changes?since_revision=1"
        )
        refresh = client.post(
            "/market-context/mnq/sync/refresh",
            json={
                "consumer_id": "ai-trader",
                "request_id": "http-refresh",
                "reason": "MARKET_TRIGGER",
                "required_sections": ["news"],
            },
        )
        work = client.get(
            refresh.json()["status_url"]
        )
        rejected = client.post(
            "/market-context/mnq/sync/sections",
            json={
                "consumer_id": "ai-trader",
                "target_snapshot_revision": 1,
                "sections": ["arbitrary_sql"],
            },
        )

    assert manifest.status_code == 200
    assert manifest.json()["contract"] == "ai_trader_market_context_sync"
    assert full.status_code == 200
    assert full.json()["delivery_type"] == "FULL_SNAPSHOT"
    assert plan.json()["sync_mode"] == "FULL"
    assert selective.json()["snapshot_revision"] == 1
    assert changes.json()["changed_sections"] == []
    assert refresh.status_code == 202
    assert work.json()["status"] == "PENDING"
    assert rejected.status_code == 422
