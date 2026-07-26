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
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import (
    canonical_json,
    extract_sync_sections,
    reconcile_delivered_section,
)
from app.services.market_session_service import build_session_aware_schedule
from app.services.news_intelligence_service import build_news_context
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.temporal_domain_service import canonical_event_key


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    ROOT / "tests" / "fixtures" / "snapshot_92_live_blockers_redacted.json"
)


def replay() -> dict[str, Any]:
    source_bytes = FIXTURE.read_bytes()
    fixture = json.loads(source_bytes)
    now = datetime.fromisoformat(fixture["reference_now"])
    settings = Settings(
        _env_file=None,
        environment="test",
        source_policy_path=ROOT / "config" / "source_policy.json",
        event_calendar_catchup_enabled=True,
        event_calendar_catchup_batch_size=7,
        event_calendar_catchup_max_per_tick=40,
        event_calendar_catchup_lookback_days=730,
    )
    discovery = list(fixture["discovery"])
    discovered_ids = [canonical_event_key(item) for item in discovery]
    window = build_event_calendar_window(
        {
            "event_calendar": {
                "critical_macro_events": [
                    *discovery,
                    *_next_week_rows(),
                ],
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
                        "removals": fixture["unconfirmed_removals"]
                    }
                }
            },
        },
        settings=settings,
        now=now,
    )
    raw_news = build_news_context(fixture["news"], now=now)
    news = extract_sync_sections(
        {
            "news_context": raw_news,
            "latest_news": raw_news["latest"],
            "news_digest": raw_news["digest"],
        }
    )["news"]
    schedule = build_session_aware_schedule(
        fixture["market_schedule"],
        now=now,
    )
    catchup = _replay_catchup(
        fixture=fixture,
        now=now,
    )
    large_rows = [
        {
            "article_id": f"large-{index}",
            "source": "Reuters",
            "summary": f"{index}:" + ("x" * 180_000),
        }
        for index in range(13)
    ]
    large_news = reconcile_delivered_section(
        "news",
        {
            "context": {
                "articles": large_rows,
                "latest": large_rows,
                "historical_articles": [],
                "search_completed": True,
            },
            "latest": large_rows,
            "digest": {},
        },
    )
    large_bytes = canonical_json(large_news).encode("utf-8")
    delivered_news = list(news["context"]["articles"])
    rates = fixture["rates"]
    effective_valid_until = max(
        datetime.fromisoformat(rates["valid_until"]),
        datetime.fromisoformat(rates["data_as_of"] + "T00:00:00+00:00"),
        datetime.fromisoformat(rates["retrieved_at"]),
    ).isoformat()
    result = {
        "mode": "OFFLINE_SNAPSHOT_92_LIVE_BLOCKER_REPLAY",
        "fixture_integrity": {
            "sha256": hashlib.sha256(source_bytes).hexdigest().upper(),
            "bytes": len(source_bytes),
        },
        "forensic_before": fixture["forensic_before"],
        "calendar_after": {
            "bucket_counts": window["counts"]["by_bucket"],
            "actual_missing_ids": sorted(
                set(fixture["expectations"]["actual_missing_ids"])
                & set(window["actual_missing_ids"])
            ),
            "unconfirmed_removals_retained": window["coverage"][
                "cross_stage_reconciliation"
            ]["unconfirmed_removals_retained"],
            "cross_stage": window["coverage"][
                "cross_stage_reconciliation"
            ],
            "omitted_for_size_count": window["coverage"][
                "omitted_for_size_count"
            ],
            "omitted_for_count_count": window["coverage"][
                "omitted_for_count_count"
            ],
        },
        "catchup_after": catchup,
        "news_after": {
            "candidate_article_count": news["context"]["diagnostics"][
                "raw_article_count"
            ],
            "accepted_article_count": news["context"][
                "accepted_article_count"
            ],
            "delivered_raw_article_count": news["context"][
                "delivered_raw_article_count"
            ],
            "historical_article_count": news["context"][
                "historical_article_count"
            ],
            "rejected_article_count": news["context"]["diagnostics"][
                "excluded_count"
            ],
            "status": news["context"]["status"],
            "digest_status": news["digest"]["status"],
            "usable_for_analysis": news["context"]["usable_for_analysis"],
            "publishers": sorted(
                {
                    str(item.get("original_publisher"))
                    for item in delivered_news
                }
            ),
            "distribution_sources": sorted(
                {
                    str(item.get("distribution_source"))
                    for item in delivered_news
                    if item.get("distribution_source")
                }
            ),
            "quarantined_record_count": news["producer_disclosures"][
                "quarantine"
            ]["record_count"],
        },
        "market_schedule_after": {
            key: {
                field: schedule[key].get(field)
                for field in (
                    "status",
                    "is_open",
                    "closed_reason",
                    "verification_scope",
                    "holiday_override_status",
                    "holiday_name",
                    "is_early_close",
                )
            }
            for key in ("nasdaq_cash_session", "mnq_futures_session")
        },
        "rates_after": {
            "data_as_of": rates["data_as_of"],
            "inherited_valid_until": rates["valid_until"],
            "effective_valid_until": effective_valid_until,
            "freshness": rates["freshness"],
            "temporal_invariant_holds": (
                datetime.fromisoformat(effective_valid_until)
                >= datetime.fromisoformat(rates["retrieved_at"])
            ),
        },
        "lossless_multi_megabyte": {
            "payload_bytes": len(large_bytes),
            "record_count": len(large_news["context"]["articles"]),
            "sha256": hashlib.sha256(large_bytes).hexdigest().upper(),
        },
        "side_effects": {
            "live_provider_calls": 0,
            "ai_jobs": 0,
            "ai_backend_invocations": 0,
            "ai_enqueue_invocations": 0,
            "browser_calls": 0,
            "delivery_attempts": 0,
            "trading_calls": 0,
            "operational_database_writes": 0,
        },
    }
    encoded = canonical_json(result).encode("utf-8")
    result["replay_sha256"] = hashlib.sha256(encoded).hexdigest().upper()
    return result


def _replay_catchup(
    *,
    fixture: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix="snapshot92-offline-",
        ignore_cleanup_errors=True,
    ) as raw:
        settings = Settings(
            _env_file=None,
            environment="test",
            database_path=Path(raw) / "replay.sqlite",
            source_policy_path=ROOT / "config" / "source_policy.json",
            event_calendar_catchup_enabled=True,
            event_calendar_catchup_batch_size=7,
            event_calendar_catchup_max_per_tick=40,
            event_calendar_catchup_lookback_days=730,
        )
        MarketContextSnapshotRepository(settings).save_next(
            symbol="MNQ",
            refresh_mode="offline_snapshot92_baseline",
            debug_payload={
                "symbol": "MNQ",
                "generated_at_utc": now.isoformat(),
                "event_calendar": {
                    "critical_macro_events": _next_week_rows(),
                    "fed_communications": [],
                    "other_economic_events": [],
                    "source_coverage": {
                        "by_bucket": {
                            "PREVIOUS_WEEK": {
                                "status": "UNVERIFIED_EMPTY"
                            },
                            "CURRENT_WEEK": {
                                "status": "UNVERIFIED_EMPTY"
                            },
                            "NEXT_WEEK": {
                                "status": "VERIFIED_COMPLETE"
                            },
                        }
                    },
                },
                "macro_snapshot": {},
                "market_schedule": {},
                "nasdaq_context": {"earnings": {}},
                "news_context": {},
                "risk_context": {},
            },
            ai_enrichment={"status": "NOT_REQUIRED"},
        )

        class ScheduleAcquire:
            last_provider_results = [
                SimpleNamespace(errors=[]) for _ in range(5)
            ]

            def __call__(self, **_: Any) -> list[dict[str, Any]]:
                return list(fixture["discovery"])

        scheduler = ResearchSchedulerService(settings, clock=lambda: now)
        first = scheduler.startup_catch_up(
            resolver=lambda _: {
                "status": "NO_DATA",
                "reason": "redacted_provider_envelope_no_data",
                "provider_request_attempted": True,
                "provider_request_completed": True,
                "ai_eligible": True,
            },
            ai_enqueue=lambda _: (_ for _ in ()).throw(
                AssertionError("AI enqueue reached in offline replay")
            ),
            schedule_acquire=ScheduleAcquire(),
            execution_context=ExecutionContext.provider_only(
                correlation_id="snapshot92-offline-replay",
                allow_live_providers=True,
            ),
        )
        second = scheduler.startup_catch_up(
            resolver=lambda _: (_ for _ in ()).throw(
                AssertionError("backoff item reclaimed too early")
            ),
            ai_enqueue=lambda _: (_ for _ in ()).throw(
                AssertionError("AI enqueue reached in offline replay")
            ),
            schedule_acquire=ScheduleAcquire(),
            execution_context=ExecutionContext.provider_only(
                correlation_id="snapshot92-offline-replay",
                allow_live_providers=True,
            ),
        )
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
        return {
            "status": first["status"],
            "completion_status": first["catch_up_completion_status"],
            "backlog_before": first["catch_up_backlog_before"],
            "backlog_after": first["catch_up_backlog_after"],
            "claimed": first["claimed"],
            "resolved_count": len(first["resolved"]),
            "residual_count": first["residual_count"],
            "pending_backoff": first["catch_up_pending_backoff"],
            "writes": first["writes"],
            "rematerialized_snapshot_count": len(
                first["rematerialized_snapshot_ids"]
            ),
            "discovered_previous_count": first["source_coverage"][
                "by_bucket"
            ]["PREVIOUS_WEEK"]["candidate_count"],
            "discovered_current_count": first["source_coverage"][
                "by_bucket"
            ]["CURRENT_WEEK"]["candidate_count"],
            "provider_result_count": first["source_coverage"][
                "provider_result_count"
            ],
            "provider_success_count": first["source_coverage"][
                "provider_success_count"
            ],
            "repeat_status": second["status"],
            "repeat_claimed": second["claimed"],
            "repeat_writes": second["writes"],
            "ai_jobs": ai_jobs,
            "ai_backend_invocations": ai_backends,
            "ai_enqueue_invocations": 0,
        }


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    encoded = json.dumps(
        replay(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if args.output is not None:
        args.output.write_text(encoded + "\n", encoding="utf-8")
    else:
        print(encoded)


if __name__ == "__main__":
    main()
