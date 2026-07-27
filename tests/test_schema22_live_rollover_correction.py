from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.services.lifecycle_due_resolver import (
    ExactOccurrenceActualProviderAdapter,
)
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import delivery_readiness
from app.services.market_news_repository import MarketNewsRepository
from app.services.news_intelligence_service import build_news_context
from app.services.research_scheduler_service import ResearchSchedulerService
from scripts.replay_schema22_live_rollover_offline import replay


ROOT = Path(__file__).resolve().parents[1]
LIVE_FIXTURE = (
    ROOT / "tests" / "fixtures" / "schema22_live_rollover_redacted.json"
)
ACTUAL_FIXTURE = (
    ROOT / "tests" / "fixtures" / "snapshot_92_live_blockers_redacted.json"
)
NOW = datetime(2026, 7, 27, 13, 28, tzinfo=UTC)


def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "schema22-correction.sqlite",
    )


def actual_fixture() -> dict[str, Any]:
    return json.loads(ACTUAL_FIXTURE.read_text(encoding="utf-8"))


def actual_item(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_at": event["release_at"],
        "fields_attempted": ["actual"],
        "payload": event,
    }


def test_provider_scope_cannot_remove_previous_week_without_authority(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    snapshots = MarketContextSnapshotRepository(cfg, clock=lambda: NOW)
    previous = {
        "event_id": "previous",
        "occurrence_id": "previous",
        "name": "Previous scoped event",
        "country": "US",
        "event_type": "MACRO",
        "impact": "HIGH",
        "release_at": "2026-07-24T12:30:00+00:00",
        "source": "BLS",
        "source_url": "https://www.bls.gov/",
    }
    current = {
        **previous,
        "event_id": "current",
        "occurrence_id": "current",
        "name": "Current scoped event",
        "release_at": "2026-07-27T12:30:00+00:00",
    }
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="scope-baseline",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "event_calendar": {
                "critical_macro_events": [previous],
                "fed_communications": [],
                "other_economic_events": [],
            },
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    scheduler = ResearchSchedulerService(cfg, clock=lambda: NOW)
    scheduler.snapshots = snapshots
    common_coverage = {
        "status": "VERIFIED_COMPLETE",
        "window_start": "2026-07-20T04:00:00+00:00",
        "window_end": "2026-08-10T04:00:00+00:00",
    }

    scheduler._rematerialize_schedule_discovery(
        rows=[current],
        lifecycles=[],
        source_coverage={
            **common_coverage,
            "authoritative_dates": ["2026-07-27"],
        },
        now=NOW,
    )
    retained = snapshots.latest_components("MNQ")["event_calendar"][
        "critical_macro_events"
    ]
    prior = next(item for item in retained if item["occurrence_id"] == "previous")
    assert prior.get("removal_status") is None

    scheduler._rematerialize_schedule_discovery(
        rows=[current],
        lifecycles=[],
        source_coverage={
            **common_coverage,
            "authoritative_dates": ["2026-07-24", "2026-07-27"],
        },
        now=NOW,
    )
    retained = snapshots.latest_components("MNQ")["event_calendar"][
        "critical_macro_events"
    ]
    prior = next(item for item in retained if item["occurrence_id"] == "previous")
    assert prior["removal_status"] == "UNCONFIRMED_REMOVAL"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            {"forecast": 51.2, "previous": 52.0},
            "forecast_previous_fields_swapped",
        ),
        (
            {"frequency": "quarterly"},
            "exact_occurrence_frequency_mismatch",
        ),
        (
            {"unit": "percent"},
            "exact_occurrence_unit_mismatch",
        ),
        (
            {"retrieved_at": "2026-07-24T13:00:00+00:00"},
            "exact_occurrence_validation_stale",
        ),
        (
            {"field_lineage": {"actual": {"source_field": "forecast"}}},
            "actual_field_lineage_invalid",
        ),
    ],
)
def test_actual_semantics_fail_closed(
    mutation: dict[str, Any],
    reason: str,
) -> None:
    fixture = actual_fixture()
    previous = dict(
        fixture["unconfirmed_removals"][1]["previous_occurrence"]
    )
    previous["unit"] = "index"
    observation = dict(fixture["actual_provider_observations"][1])
    observation.update(mutation)
    result = ExactOccurrenceActualProviderAdapter(
        lambda _: [observation]
    ).resolve(actual_item(previous))
    assert result["status"] == "NO_DATA"
    assert reason in result["reason"]


def test_ambiguous_actual_observations_fail_closed() -> None:
    fixture = actual_fixture()
    previous = fixture["unconfirmed_removals"][1]["previous_occurrence"]
    first = dict(fixture["actual_provider_observations"][1])
    second = {**first, "actual": 99.0}
    result = ExactOccurrenceActualProviderAdapter(
        lambda _: [first, second]
    ).resolve(actual_item(previous))
    assert result["status"] == "NO_DATA"
    assert "exact_occurrence_observation_ambiguous" in result["reason"]


def test_news_projection_has_no_sql_top_n_and_explains_rejections(
    tmp_path: Path,
) -> None:
    repository = MarketNewsRepository(settings(tmp_path), clock=lambda: NOW)
    for index in range(116):
        repository.upsert_news(
            {
                "title": f"Nvidia material Reuters update {index:03d}",
                "summary": "Reuters reports a material Nvidia market update.",
                "content": f"Lossless provider content {index:03d}",
                "source": "Yahoo Finance",
                "publisher": "Reuters",
                "distribution_source": "Yahoo Finance",
                "source_url": (
                    "https://finance.yahoo.com/news/"
                    f"redacted-reuters-{index:03d}.html"
                ),
                "published_at": (
                    NOW.replace(hour=12, minute=0, second=0)
                ).isoformat(),
                "retrieved_at": NOW.isoformat(),
                "lineage": {"publisher": "Reuters", "sequence": index},
            }
        )
    unknown = repository.upsert_news(
        {
            "title": "Unknown publisher claims Nvidia development",
            "summary": "A material claim with unverifiable originator.",
            "content": "Unknown publisher content remains quarantined.",
            "source": "Yahoo Finance",
            "publisher": "Unknown Publisher",
            "distribution_source": "Yahoo Finance",
            "source_url": "https://finance.yahoo.com/news/redacted-unknown.html",
            "published_at": NOW.isoformat(),
            "retrieved_at": NOW.isoformat(),
        }
    )
    rows = repository.stored(
        days=30,
        limit=None,
        include_quarantined=True,
    )
    context = build_news_context(rows, now=NOW)

    assert len(rows) == 117
    assert len(context["latest"]) == 116
    assert unknown["source_audit_status"] == "QUARANTINED"
    assert len(context["excluded"]) == 1
    rejected = context["excluded"][0]
    assert rejected["publisher"] == "Unknown Publisher"
    assert rejected["distribution_source"] == "Yahoo Finance"
    assert rejected["policy_outcome"]["status"] == "rejected"
    assert rejected["reason"]
    assert rejected["content"] == "Unknown publisher content remains quarantined."


def test_readiness_distinguishes_usable_degraded_from_unavailable() -> None:
    readiness = delivery_readiness(
        {
            "event_calendar": {
                "sync": {
                    "status": "PARTIAL",
                    "freshness": "CURRENT",
                    "record_count": 42,
                }
            },
            "rates": {
                "sync": {
                    "status": "AVAILABLE",
                    "freshness": "STALE",
                    "record_count": 1,
                }
            },
            "risk": {
                "sync": {
                    "status": "NO_DATA",
                    "freshness": "NO_DATA",
                    "record_count": 0,
                }
            },
        }
    )
    assert readiness["sections_available"] == ["event_calendar", "rates"]
    assert readiness["sections_degraded"] == ["event_calendar", "rates"]
    assert readiness["sections_unavailable"] == ["risk"]
    assert readiness["available_section_count"] == 2
    assert readiness["unavailable_section_count"] == 1
    assert readiness["status"] == "PARTIAL"


def test_schema22_forensic_replay_closes_every_observed_blocker() -> None:
    summary, full = replay()
    accounting = summary["after"]["candidate_accounting"]
    assert summary["after"]["calendar_counts"] == {
        "PREVIOUS_WEEK": 14,
        "CURRENT_WEEK": 3,
        "NEXT_WEEK": 25,
    }
    assert summary["after"]["actual_missing_ids"] == []
    assert summary["after"]["news_admitted"] == 3
    assert summary["after"]["news_quarantined"] == 1
    assert summary["after"]["market_schedule"] == "PARTIAL"
    assert summary["after"]["ledger_dates"] == 21
    assert accounting["source_candidate_count"] == (
        accounting["delivered_occurrence_count"]
        + accounting["quarantined_occurrence_count"]
        + accounting["exact_duplicate_count"]
    )
    assert accounting["unexplained_loss"] == 0
    assert len(full["sections"]) == 17
    assert summary["full_sync"]["two_independent_replays_byte_identical"]
    assert set(summary["side_effects"].values()) == {0}
    assert set(summary["second_replay_side_effects"].values()) == {0}


def test_redacted_evidence_records_snapshot95_and_96() -> None:
    fixture = json.loads(LIVE_FIXTURE.read_text(encoding="utf-8"))
    assert fixture["snapshot_95"]["calendar_counts"] == {
        "previous": 21,
        "current": 53,
        "next": 9,
    }
    assert fixture["snapshot_96"]["calendar_counts"] == {
        "previous": 0,
        "current": 25,
        "next": 7,
    }
    assert fixture["snapshot_96"]["news_rows_added"] == 16
