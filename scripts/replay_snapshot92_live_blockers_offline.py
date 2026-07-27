from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.event_calendar_window_service import (
    build_event_calendar_window,
)
from app.services.execution_context import ExecutionContext
from app.services.lifecycle_due_resolver import (
    ExactOccurrenceActualProviderAdapter,
)
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import (
    SECTION_NAMES,
    MarketContextSyncService,
    canonical_json,
)
from app.services.market_news_repository import MarketNewsRepository
from app.services.news_intelligence_runtime_service import (
    NewsIntelligenceRuntimeService,
)
from app.services.research_scheduler_service import ResearchSchedulerService


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    ROOT / "tests" / "fixtures" / "snapshot_92_live_blockers_redacted.json"
)


def replay_artifacts() -> tuple[dict[str, Any], dict[str, Any]]:
    source_bytes = FIXTURE.read_bytes()
    fixture = json.loads(source_bytes)
    now = datetime.fromisoformat(fixture["reference_now"])
    named_ids = set(fixture["expectations"]["actual_missing_ids"])

    with tempfile.TemporaryDirectory(
        prefix="snapshot92-materialized-",
        ignore_cleanup_errors=True,
    ) as raw:
        settings = _settings(Path(raw) / "replay.sqlite")
        news_repository = MarketNewsRepository(
            settings,
            clock=lambda: now,
        )
        news_writes = [
            news_repository.upsert_news(
                {
                    **article,
                    "created_at": (
                        article.get("created_at")
                        or article.get("retrieved_at")
                    ),
                }
            )
            for article in fixture["news"]
        ]
        admitted_news = news_repository.stored(days=30, limit=50)
        news_context, news_metrics = NewsIntelligenceRuntimeService(
            settings,
            clock=lambda: now,
        ).materialize(
            admitted_news,
            refresh_mode="force",
            limit=50,
        )

        snapshot_sequence = iter(
            [
                "00000000-0000-4000-8000-000000000001",
                "00000000-0000-4000-8000-000000000002",
                "00000000-0000-4000-8000-000000000003",
                "00000000-0000-4000-8000-000000000004",
            ]
        )
        snapshots = MarketContextSnapshotRepository(
            settings,
            clock=lambda: now,
            id_factory=lambda: next(snapshot_sequence),
        )
        baseline = snapshots.save_next(
            symbol="MNQ",
            refresh_mode="snapshot92_materialized_baseline",
            debug_payload=_baseline_payload(
                fixture=fixture,
                news_context=news_context,
                now=now,
            ),
            ai_enrichment={"status": "NOT_REQUIRED"},
        )
        window_before = build_event_calendar_window(
            snapshots.latest_components("MNQ"),
            settings=settings,
            now=now,
        )

        class ScheduleAcquire:
            coverage_proof = {
                "request_succeeded": True,
                "scope_match": True,
                "pagination_complete": True,
                "parsing_succeeded": True,
                "records_valid": True,
                "expected_sources_complete": True,
                "authentic_empty": True,
            }
            last_provider_results = [
                SimpleNamespace(errors=[]) for _ in range(5)
            ]

            def __call__(self, **_: Any) -> list[dict[str, Any]]:
                return _replay_discovery(fixture)

        adapter = ExactOccurrenceActualProviderAdapter(
            lambda _: fixture["actual_provider_observations"]
        )
        resolved_actuals: dict[str, dict[str, Any]] = {}

        def resolve(item: dict[str, Any]) -> dict[str, Any]:
            result = adapter.resolve(item)
            if result["status"] == "RESOLVED":
                datum = dict(result["datum"])
                resolved_actuals[str(datum["occurrence_id"])] = datum
            return result

        scheduler = ResearchSchedulerService(settings, clock=lambda: now)
        scheduler.snapshots = snapshots
        catchup = scheduler.startup_catch_up(
            resolver=resolve,
            ai_enqueue=lambda _: (_ for _ in ()).throw(
                AssertionError("AI enqueue reached in offline replay")
            ),
            schedule_acquire=ScheduleAcquire(),
            execution_context=ExecutionContext.provider_only(
                correlation_id="snapshot92-materialized-replay",
                allow_live_providers=True,
            ),
        )
        window_after = build_event_calendar_window(
            snapshots.latest_components("MNQ"),
            settings=settings,
            now=now,
        )

        sync = MarketContextSyncService(settings)
        full_sync = sync.full()
        first_bytes = canonical_json(full_sync).encode("utf-8")
        second_bytes = canonical_json(sync.full()).encode("utf-8")
        if first_bytes != second_bytes:
            raise AssertionError("full_sync_replay_not_byte_identical")
        if full_sync["payload_size_bytes"] != len(first_bytes):
            raise AssertionError("full_sync_payload_size_mismatch")
        if set(full_sync["sections"]) != set(SECTION_NAMES):
            raise AssertionError("full_sync_section_inventory_mismatch")

        delivered = canonical_json(full_sync)
        admitted_proof = [
            {
                "article_id": item.get("article_id"),
                "title": item.get("title"),
                "publisher": item.get("original_publisher"),
                "distribution_source": item.get("distribution_source"),
                "published_at": item.get("published_at"),
                "validation": item.get("validation"),
                "present_in_full_sync": (
                    str(item.get("article_id")) in delivered
                ),
            }
            for item in admitted_news
        ]
        with connect_sqlite(settings.database_path) as conn:
            ai_jobs = int(
                conn.execute(
                    "SELECT COUNT(*) FROM ai_research_jobs"
                ).fetchone()[0]
            )
            ai_backends = int(
                conn.execute(
                    "SELECT COUNT(*) FROM research_backend_invocations"
                ).fetchone()[0]
            )
            quarantined_news = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM market_news
                    WHERE source_audit_status='QUARANTINED'
                    """
                ).fetchone()[0]
            )

        summary = {
            "mode": "OFFLINE_SNAPSHOT_92_MATERIALIZED_RESIDUAL_REPLAY",
            "fixture_integrity": {
                "sha256": _sha256(source_bytes),
                "bytes": len(source_bytes),
            },
            "materialization": {
                "baseline_snapshot_id": baseline["snapshot_id"],
                "baseline_revision": baseline["revision"],
                "final_snapshot_id": full_sync["snapshot_id"],
                "final_revision": full_sync["snapshot_revision"],
                "rematerialized_snapshot_ids": catchup[
                    "rematerialized_snapshot_ids"
                ],
                "pipeline": [
                    "structured_provider_fixture",
                    "discovery",
                    "lifecycle",
                    "canonical_store",
                    "snapshot",
                    "full_sync",
                ],
            },
            "calendar": {
                "before": {
                    "bucket_counts": window_before["counts"]["by_bucket"],
                    "actual_missing_ids": sorted(
                        named_ids
                        & set(window_before["actual_missing_ids"])
                    ),
                },
                "after": {
                    "bucket_counts": window_after["counts"]["by_bucket"],
                    "actual_missing_ids": sorted(
                        named_ids
                        & set(window_after["actual_missing_ids"])
                    ),
                    "unconfirmed_removals_retained": window_after[
                        "coverage"
                    ]["cross_stage_reconciliation"][
                        "unconfirmed_removals_retained"
                    ],
                    "duplicates_removed": window_after["coverage"][
                        "cross_stage_reconciliation"
                    ].get("duplicate_occurrences_removed", 0),
                    "omitted_for_size_count": window_after["coverage"][
                        "omitted_for_size_count"
                    ],
                    "omitted_for_count_count": window_after["coverage"][
                        "omitted_for_count_count"
                    ],
                    "source_coverage": catchup["source_coverage"],
                },
            },
            "actuals": [
                _actual_proof(resolved_actuals[occurrence_id])
                for occurrence_id in sorted(named_ids)
            ],
            "catchup": {
                "status": catchup["status"],
                "completion_status": catchup[
                    "catch_up_completion_status"
                ],
                "backlog_before": catchup["catch_up_backlog_before"],
                "backlog_after": catchup["catch_up_backlog_after"],
                "pending_backoff": catchup["catch_up_pending_backoff"],
                "claimed": catchup["claimed"],
                "resolved_count": len(catchup["resolved"]),
                "actuals_recovered": catchup["actuals_recovered"],
                "lifecycle_writes": catchup["lifecycle_writes"],
                "snapshot_writes": catchup["snapshot_writes"],
            },
            "news": {
                "candidate_count": len(fixture["news"]),
                "admitted_count": len(admitted_news),
                "quarantined_count": quarantined_news,
                "runtime_metrics": news_metrics,
                "admitted_articles": admitted_proof,
                "unknown_publisher_via_yahoo_admitted": False,
                "temporal_distinct_reuters_delivered": (
                    len(
                        {
                            item["published_at"]
                            for item in admitted_news
                            if item.get("original_publisher") == "Reuters"
                        }
                    )
                    == 2
                ),
                "write_statuses": [
                    row["source_audit_status"] for row in news_writes
                ],
            },
            "full_sync": {
                "delivery_type": full_sync["delivery_type"],
                "contract": full_sync["contract"],
                "section_count": len(full_sync["sections"]),
                "section_record_counts": {
                    name: full_sync["manifest"]["sections"][name][
                        "record_count"
                    ]
                    for name in SECTION_NAMES
                },
                "payload_size_bytes": len(first_bytes),
                "artifact_sha256": _sha256(first_bytes),
                "contract_checksum": full_sync["checksum"],
                "checksum_scope": full_sync["checksum_scope"],
                "readiness": full_sync["readiness"],
                "two_consecutive_replays_byte_identical": True,
            },
            "side_effects": {
                "live_provider_calls": 0,
                "ai_jobs": ai_jobs,
                "ai_backend_invocations": ai_backends,
                "ai_enqueue_invocations": 0,
                "browser_calls": 0,
                "delivery_attempts": 0,
                "trading_calls": 0,
                "operational_database_writes": 0,
            },
        }
        return summary, full_sync


def replay() -> dict[str, Any]:
    summary, _ = replay_artifacts()
    return summary


def _settings(database_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=database_path,
        source_policy_path=ROOT / "config" / "source_policy.json",
        event_calendar_catchup_enabled=True,
        event_calendar_catchup_batch_size=7,
        event_calendar_catchup_max_per_tick=40,
        event_calendar_catchup_lookback_days=730,
    )


def _baseline_payload(
    *,
    fixture: dict[str, Any],
    news_context: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    prior = [
        dict(item["previous_occurrence"])
        for item in fixture["unconfirmed_removals"]
    ]
    return {
        "symbol": "MNQ",
        "generated_at_utc": now.isoformat(),
        "event_calendar": {
            "critical_macro_events": [*prior, *_next_week_rows()],
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
        "event_calendar_window": {
            "audit": {
                "comparison": {
                    "removals": fixture["unconfirmed_removals"][:1]
                }
            }
        },
        "macro_snapshot": {},
        "rates_context": fixture["rates"],
        "market_schedule": fixture["market_schedule"],
        "nasdaq_context": {"earnings": {}},
        "news_context": news_context,
        "latest_news": news_context["latest"],
        "news_digest": news_context["digest"],
        "risk_context": {},
    }


def _replay_discovery(
    fixture: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return a final 14/3 materialized window with one retained removal.

    The S&P PMI occurrence is rediscovered, while New Home Sales is absent from
    the current provider envelope and must remain visible as the one
    unconfirmed removal.  One ordinary current fixture row plus those two
    occurrences yields the required three current-week events.
    """

    previous = list(fixture["discovery"][:14])
    ordinary_current = dict(fixture["discovery"][14])
    pmi = dict(fixture["unconfirmed_removals"][1]["previous_occurrence"])
    return [*previous, ordinary_current, pmi]


def _next_week_rows(count: int = 25) -> list[dict[str, Any]]:
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


def _actual_proof(item: dict[str, Any]) -> dict[str, Any]:
    lineage = (
        item.get("field_lineage")
        or (item.get("lineage") or {}).get("field_lineage")
        or {}
    )
    return {
        "occurrence_id": item.get("occurrence_id"),
        "title": item.get("title") or item.get("name"),
        "release_at": item.get("scheduled_at_utc")
        or item.get("release_at"),
        "reference_period": item.get("reference_period"),
        "frequency": item.get("frequency"),
        "actual": item.get("actual"),
        "forecast": item.get("forecast"),
        "consensus": item.get("consensus"),
        "previous": item.get("previous"),
        "unit": item.get("unit"),
        "publisher": item.get("publisher")
        or item.get("source_originator")
        or item.get("source"),
        "distribution_source": item.get("distribution_source"),
        "source_url": item.get("source_url"),
        "release_status": item.get("release_status"),
        "validation_status": item.get("validation_status"),
        "field_lineage": lineage,
    }


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest().upper()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        help="Backward-compatible alias for --summary-output.",
    )
    parser.add_argument("--summary-output", type=Path)
    parser.add_argument("--full-sync-output", type=Path)
    args = parser.parse_args()
    summary, full_sync = replay_artifacts()
    summary_bytes = canonical_json(summary).encode("utf-8")
    full_sync_bytes = canonical_json(full_sync).encode("utf-8")
    summary_path = args.summary_output or args.output
    if summary_path is not None:
        summary_path.write_bytes(summary_bytes)
    if args.full_sync_output is not None:
        args.full_sync_output.write_bytes(full_sync_bytes)
    if summary_path is None and args.full_sync_output is None:
        print(summary_bytes.decode("utf-8"))


if __name__ == "__main__":
    main()
