from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.event_calendar_window_service import (
    build_event_calendar_window,
)
from app.services.execution_context import ExecutionContext
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import (
    MarketContextSyncService,
    canonical_json,
    extract_sync_sections,
    reconcile_delivered_section,
)
from app.services.market_session_service import build_session_aware_schedule
from app.services.news_intelligence_service import build_news_context
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_domain_service import canonical_event_key


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "snapshot_92_live_blockers_redacted.json"
)


def fixture() -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def cfg(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "environment": "test",
        "database_path": tmp_path / "snapshot92.sqlite",
        "event_calendar_catchup_enabled": True,
        "event_calendar_catchup_batch_size": 7,
        "event_calendar_catchup_max_per_tick": 40,
        "event_calendar_catchup_lookback_days": 730,
    }
    values.update(overrides)
    return Settings(**values)


def next_week_rows(count: int = 25) -> list[dict[str, object]]:
    return [
        {
            "provider": "BLS",
            "provider_event_id": f"redacted-next-{index:02d}",
            "name": f"Redacted next-week occurrence {index:02d}",
            "country": "US",
            "currency": "USD",
            "impact": "HIGH" if index < 3 else "MEDIUM",
            "event_type": "MACRO",
            "reference_period": "2026-07",
            "frequency": "monthly",
            "release_at": (
                datetime(2026, 7, 27, 12, 30, tzinfo=UTC)
                + timedelta(hours=index)
            ).isoformat(),
            "actual": None,
            "source": "BLS",
            "source_url": "https://www.bls.gov/",
            "retrieved_at": "2026-07-26T15:55:00+00:00",
        }
        for index in range(count)
    ]


def baseline_payload(now: datetime) -> dict[str, object]:
    return {
        "symbol": "MNQ",
        "generated_at_utc": now.isoformat(),
        "event_calendar": {
            "critical_macro_events": next_week_rows(),
            "fed_communications": [],
            "other_economic_events": [],
            "source_coverage": {
                "by_bucket": {
                    "PREVIOUS_WEEK": {"status": "UNVERIFIED_EMPTY"},
                    "CURRENT_WEEK": {"status": "UNVERIFIED_EMPTY"},
                    "NEXT_WEEK": {"status": "VERIFIED_COMPLETE"},
                }
            },
        },
        "macro_snapshot": {},
        "market_schedule": {},
        "nasdaq_context": {"earnings": {}},
        "news_context": {},
        "risk_context": {},
    }


def full_window_input(payload: dict[str, object]) -> dict[str, object]:
    discovery = list(payload["discovery"])  # type: ignore[arg-type]
    discovered_ids = [
        canonical_event_key(item)
        for item in discovery
        if isinstance(item, dict)
    ]
    return {
        **baseline_payload(
            datetime.fromisoformat(str(payload["reference_now"]))
        ),
        "event_calendar": {
            "critical_macro_events": [*discovery, *next_week_rows()],
            "fed_communications": [],
            "other_economic_events": [],
            "source_coverage": {
                "discovered_occurrence_ids": discovered_ids,
                "by_bucket": {
                    bucket: {"status": "VERIFIED_COMPLETE"}
                    for bucket in (
                        "PREVIOUS_WEEK",
                        "CURRENT_WEEK",
                        "NEXT_WEEK",
                    )
                },
            },
        },
        "event_calendar_window": {
            "audit": {
                "comparison": {
                    "removals": payload["unconfirmed_removals"],
                }
            }
        },
    }


def test_three_week_projection_retains_discovery_and_unconfirmed_actuals(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    window = build_event_calendar_window(
        full_window_input(payload),
        settings=cfg(tmp_path),
        now=now,
    )

    assert window["previous_week"]["event_count"] == 14
    assert window["current_week"]["event_count"] == 5
    assert window["next_week"]["event_count"] == 25
    missing = set(window["actual_missing_ids"])
    assert set(payload["expectations"]["actual_missing_ids"]) <= missing  # type: ignore[index]
    retained_removals = {
        item["occurrence_id"]: item
        for item in window["current_week"]["events"]
        if item.get("removal_status") == "UNCONFIRMED_REMOVAL"
    }
    assert set(retained_removals) == set(
        payload["expectations"]["actual_missing_ids"]  # type: ignore[index]
    )
    assert all(
        item["comparison_lineage"]["previous_source"] == "XTB"
        for item in retained_removals.values()
    )
    reconciliation = window["coverage"]["cross_stage_reconciliation"]
    assert reconciliation["discovered_occurrence_count"] == 17
    assert reconciliation["delivered_occurrence_count"] == 17
    assert reconciliation["unexplained_loss"] == 0


def test_discovery_gaps_are_processed_same_tick_and_wait_in_backoff(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="snapshot92-redacted-baseline",
        debug_payload=baseline_payload(now),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )

    class ScheduleAcquire:
        last_provider_results = [
            SimpleNamespace(errors=[]) for _ in range(5)
        ]

        def __call__(self, **_: object) -> list[dict[str, object]]:
            return list(payload["discovery"])  # type: ignore[arg-type]

    resolver_calls: list[str] = []

    def no_data(item: dict[str, object]) -> dict[str, object]:
        resolver_calls.append(str(item["entity_key"]))
        return {
            "status": "NO_DATA",
            "reason": "redacted_provider_envelope_no_data",
            "provider_request_attempted": True,
            "provider_request_completed": True,
            "ai_eligible": True,
        }

    scheduler = ResearchSchedulerService(settings, clock=lambda: now)
    context = ExecutionContext.provider_only(
        correlation_id="snapshot92-redacted-catchup",
        allow_live_providers=True,
    )
    first = scheduler.startup_catch_up(
        resolver=no_data,
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI enqueue must remain unreachable")
        ),
        schedule_acquire=ScheduleAcquire(),
        execution_context=context,
    )
    with connect_sqlite(settings.database_path) as conn:
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0]
        ai_jobs = conn.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0]
        ai_backends = conn.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0]
    second = scheduler.startup_catch_up(
        resolver=lambda _: (_ for _ in ()).throw(
            AssertionError("backoff must suppress the resolver")
        ),
        ai_enqueue=lambda _: (_ for _ in ()).throw(
            AssertionError("AI enqueue must remain unreachable")
        ),
        schedule_acquire=ScheduleAcquire(),
        execution_context=context,
    )

    assert len(resolver_calls) == 17
    assert first["catch_up_backlog_before"] == 17
    assert first["claimed"] == 17
    assert first["catch_up_backlog_after"] == 0
    assert first["residual_count"] == 17
    assert first["status"] == "WAITING_BACKOFF"
    assert first["catch_up_pending_backoff"] == 17
    assert first["source_coverage"]["persisted_gap_count"] == 17
    assert first["source_coverage"]["by_bucket"]["PREVIOUS_WEEK"][
        "candidate_count"
    ] == 14
    assert first["source_coverage"]["by_bucket"]["CURRENT_WEEK"][
        "candidate_count"
    ] == 3
    assert first["source_coverage"]["provider_result_count"] == 5
    assert first["source_coverage"]["provider_success_count"] == 5
    assert first["writes"] == 1
    assert len(first["rematerialized_snapshot_ids"]) == 1
    assert first["ai_invocations"] == first["ai_jobs_created"] == 0
    assert ai_jobs == ai_backends == 0
    assert second["status"] == "WAITING_BACKOFF"
    assert second["claimed"] == second["writes"] == 0
    with connect_sqlite(settings.database_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM market_context_snapshots"
            ).fetchone()[0]
            == snapshot_count
        )

    sync = MarketContextSyncService(settings)
    full = sync.full()
    revision = int(full["snapshot_revision"])
    selective = sync.sections(
        consumer_id="snapshot92-fixture",
        target_snapshot_revision=revision,
        sections=["event_calendar"],
        include_lineage=True,
    )
    assert (
        selective["sections"]["event_calendar"]["sync"]["fingerprint"]
        == full["sections"]["event_calendar"]["sync"]["fingerprint"]
    )
    selective_payload = dict(selective["sections"]["event_calendar"])
    full_payload = dict(full["sections"]["event_calendar"])
    selective_payload.pop("sync")
    full_payload.pop("sync")
    assert selective_payload == full_payload


def test_retry_exhaustion_is_not_reported_as_completed(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    scheduler = ResearchSchedulerService(settings, clock=lambda: now)
    lifecycle = scheduler.lifecycle
    row = dict(payload["discovery"][0])  # type: ignore[index]
    from app.services.event_driven_lifecycle_service import (
        compute_datum_lifecycle,
    )

    lifecycle.upsert(
        compute_datum_lifecycle(
            "macro_actual",
            "fixture:exhausted",
            row,
            settings=settings,
            now=now,
            fields_attempted=["actual"],
        ),
        payload=row,
        work_status="READY",
    )
    result = scheduler.scan_due_items(
        owner="snapshot92-exhaustion",
        resolver=lambda _: {
            "status": "NO_DATA",
            "reason": "redacted_retry_deadline",
            "retry_deadline_exhausted": True,
        },
        ai_enqueue=None,
        force=True,
        allow_ai_residual=False,
    )

    assert result["status"] == "EXHAUSTED_NO_DATA"
    assert result["exhausted_no_data"]
    assert result["item_outcomes"][0]["status"] == "EXHAUSTED_NO_DATA"
    assert result["ai_invocations"] == result["ai_jobs_created"] == 0


def test_news_policy_and_sync_are_lossless_and_count_coherent() -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    context = build_news_context(
        list(payload["news"]),  # type: ignore[arg-type]
        now=now,
    )
    by_title = {item["title"]: item for item in context["latest"]}
    ibd = by_title[
        "Nvidia earnings outlook lifts semiconductor shares"
    ]
    reuters = by_title[
        "Reuters: US expands Nvidia chip export controls"
    ]
    rejected = by_title[
        "Unknown publisher claims Nvidia development"
    ]

    assert ibd["validation"]["status"] == "accepted"
    assert ibd["confirmation"]["confirmed"] is False
    assert ibd["confirmed_by_multiple_sources"] is False
    assert reuters["validation"]["status"] == "accepted"
    assert reuters["original_publisher"] == "Reuters"
    assert reuters["distribution_source"] == "Yahoo Finance"
    assert reuters["distribution_url"].startswith(
        "https://finance.yahoo.com/"
    )
    assert rejected["validation"]["status"] == "rejected"
    policy = SourcePolicyService()
    assert policy.policy_version == "source-policy-v5"

    news = extract_sync_sections(
        {
            "news_context": context,
            "latest_news": context["latest"],
            "news_digest": context["digest"],
        }
    )["news"]
    delivered = news["context"]["articles"]
    assert len(delivered) == 2
    assert {item["original_publisher"] for item in delivered} == {
        "Investor's Business Daily",
        "Reuters",
    }
    assert news["context"]["accepted_article_count"] == 2
    assert news["context"]["delivered_raw_article_count"] == 2
    assert news["context"]["historical_article_count"] == 0
    assert news["context"]["diagnostics"]["excluded_count"] == 1
    assert news["context"]["rejected_article_count"] == 1
    assert news["context"]["usable_for_analysis"] is True
    assert news["context"]["status"] == "PARTIAL"
    assert news["digest"]["status"] == "PARTIAL"
    assert news["digest"]["accepted_article_count"] == 2
    assert news["producer_disclosures"]["quarantine"][
        "record_count"
    ] >= 1


def test_multi_megabyte_news_reconciliation_does_not_cap_or_deduplicate() -> None:
    rows = [
        {
            "article_id": f"large-{index}",
            "source": "Reuters",
            "summary": f"{index}:" + ("x" * 180_000),
        }
        for index in range(13)
    ]
    section = reconcile_delivered_section(
        "news",
        {
            "context": {
                "articles": rows,
                "latest": rows,
                "historical_articles": [],
                "search_completed": True,
            },
            "latest": rows,
            "digest": {},
        },
    )

    assert len(canonical_json(section).encode("utf-8")) > 2_000_000
    assert len(section["context"]["articles"]) == 13
    assert section["context"]["delivered_raw_article_count"] == 13


def test_weekend_closure_survives_override_timeout_without_inventing_holiday() -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    schedule = build_session_aware_schedule(
        dict(payload["market_schedule"]),  # type: ignore[arg-type]
        now=now,
    )

    assert schedule["status"] == "PARTIAL"
    for key in ("nasdaq_cash_session", "mnq_futures_session"):
        session = schedule[key]
        assert session["status"] == "weekend"
        assert session["is_open"] is False
        assert session["closed_reason"] == "WEEKEND"
        assert session["verification_scope"] == "BASE_WEEKLY_RULE"
        assert session["holiday_override_status"] == "UNVERIFIED"
        assert session["holiday_name"] is None
        assert session["is_early_close"] is False


def test_rates_temporal_invariant_uses_delivered_observation(
    tmp_path: Path,
) -> None:
    payload = fixture()
    now = datetime.fromisoformat(str(payload["reference_now"]))
    settings = cfg(tmp_path)
    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="snapshot92-rates-temporal-fixture",
        debug_payload={
            **baseline_payload(now),
            "rates_context": payload["rates"],
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    rates = MarketContextSyncService(settings).manifest()["sections"][
        "rates"
    ]

    assert datetime.fromisoformat(rates["valid_until"]) >= datetime.fromisoformat(
        str(payload["rates"]["retrieved_at"])  # type: ignore[index]
    )
    assert rates["freshness"] == "CURRENT"
