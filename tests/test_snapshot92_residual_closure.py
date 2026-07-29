from __future__ import annotations

import json
from datetime import datetime
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
from app.services.market_context_sync_service import (
    SECTION_NAMES,
    MarketContextSyncService,
    canonical_json,
)
from app.services.market_news_repository import MarketNewsRepository
from app.services.news_intelligence_runtime_service import (
    NewsIntelligenceRuntimeService,
)
from scripts.replay_snapshot92_live_blockers_offline import replay_artifacts


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = (
    ROOT / "tests" / "fixtures" / "snapshot_92_live_blockers_redacted.json"
)


def _fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "snapshot92-residual.sqlite",
        source_policy_path=ROOT / "config" / "source_policy.json",
        event_calendar_catchup_enabled=True,
        event_calendar_catchup_lookback_days=730,
    )


def _actual_item(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_type": "macro_actual",
        "entity_key": f"event:{event['canonical_event_key']}",
        "event_at": event["release_at"],
        "fields_attempted": ["actual"],
        "payload": dict(event),
    }


def test_exact_occurrence_actuals_preserve_semantics_and_lineage() -> None:
    fixture = _fixture()
    previous = {
        item["occurrence_id"]: item["previous_occurrence"]
        for item in fixture["unconfirmed_removals"]
    }
    observations = list(fixture["actual_provider_observations"])
    adapter = ExactOccurrenceActualProviderAdapter(lambda _: observations)

    home = adapter.resolve(_actual_item(previous["xtb:146392:2026-07-24"]))
    pmi = adapter.resolve(_actual_item(previous["xtb:146945:2026-07-24"]))

    assert home["status"] == "RESOLVED"
    assert home["datum"]["actual"] == 628.0
    assert home["datum"]["forecast"] == 610.0
    assert home["datum"]["previous"] == 618.0
    assert home["datum"]["reference_period"] == "2026-06"
    assert home["datum"]["source_originator"] == "U.S. Census Bureau / HUD"
    assert pmi["status"] == "RESOLVED"
    assert pmi["datum"]["actual"] == 53.6
    assert pmi["datum"]["forecast"] == 52.0
    assert pmi["datum"]["previous"] == 51.2
    assert pmi["datum"]["reference_period"] == "2026-07"
    assert "Q" not in pmi["datum"]["reference_period"]
    assert pmi["datum"]["source_originator"] == "S&P Global"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            {"reference_period": "2026-Q2"},
            "exact_occurrence_reference_period_mismatch",
        ),
        (
            {"release_at": "2026-07-24T14:01:00+00:00"},
            "exact_occurrence_release_mismatch",
        ),
        (
            {
                "field_lineage": {
                    "actual": {"source_field": "forecast"}
                }
            },
            "actual_field_lineage_invalid",
        ),
    ],
)
def test_exact_occurrence_actual_rejects_semantic_mismatch(
    mutation: dict[str, Any],
    reason: str,
) -> None:
    fixture = _fixture()
    pmi = dict(fixture["actual_provider_observations"][1])
    pmi.update(mutation)
    previous = fixture["unconfirmed_removals"][1]["previous_occurrence"]

    result = ExactOccurrenceActualProviderAdapter(
        lambda _: [pmi]
    ).resolve(_actual_item(previous))

    assert result["status"] == "NO_DATA"
    assert reason in result["reason"]


def test_news_productive_path_reaches_full_sync_without_destructive_dedup(
    tmp_path: Path,
) -> None:
    fixture = _fixture()
    settings = _settings(tmp_path)
    repository = MarketNewsRepository(settings)
    writes = [repository.upsert_news(item) for item in fixture["news"]]
    admitted = repository.stored(days=30, limit=20)
    now = datetime.fromisoformat(fixture["reference_now"])
    news, metrics = NewsIntelligenceRuntimeService(
        settings,
        clock=lambda: now,
    ).materialize(admitted, refresh_mode="force", limit=20)

    assert [row["source_audit_status"] for row in writes].count("ACTIVE") == 4
    assert [row["source_audit_status"] for row in writes].count("QUARANTINED") == 0
    assert len(admitted) == 4
    assert metrics["persisted_count"] == 1
    assert metrics["read_back_count"] == 1
    assert len({item["article_id"] for item in admitted}) == 4
    assert {
        item["title"] for item in admitted
    } == {
        "Nvidia earnings outlook lifts semiconductor shares",
        "US expands Nvidia chip export controls",
        "US expands Nvidia chip export controls — update",
        "Unknown publisher claims Nvidia development",
    }
    reuters = [
        item for item in admitted
        if item["original_publisher"] == "Reuters"
    ]
    assert len(reuters) == 2
    assert {item["distribution_source"] for item in reuters} == {
        "Yahoo Finance"
    }
    assert len({item["published_at"] for item in reuters}) == 2

    MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="snapshot92_news_productive_path",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": now.isoformat(),
            "event_calendar": {},
            "macro_snapshot": {},
            "market_schedule": {},
            "nasdaq_context": {"earnings": {}},
            "news_context": news,
            "latest_news": news["latest"],
            "news_digest": news["digest"],
            "risk_context": {},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    full = MarketContextSyncService(settings).full()
    encoded = canonical_json(full)

    assert set(full["sections"]) == set(SECTION_NAMES)
    assert len(full["sections"]) == 17
    assert full["payload_size_bytes"] == len(encoded.encode("utf-8"))
    assert "redacted-nvidia-outlook" in encoded
    assert "redacted-reuters-nvidia-131000000" in encoded
    assert "redacted-reuters-nvidia-134000000" in encoded
    assert "redacted-unknown-publisher" in encoded
    assert "Yahoo Finance" in encoded
    assert "Investor's Business Daily" in encoded


def test_materialized_three_week_replay_and_authentic_full_sync() -> None:
    summary, full = replay_artifacts()
    encoded = canonical_json(full).encode("utf-8")
    repeated = canonical_json(full).encode("utf-8")

    assert summary["calendar"]["after"]["bucket_counts"] == {
        "PREVIOUS_WEEK": 14,
        "CURRENT_WEEK": 3,
        "NEXT_WEEK": 25,
    }
    assert summary["calendar"]["before"]["actual_missing_ids"] == [
        "xtb:146392:2026-07-24",
        "xtb:146945:2026-07-24",
    ]
    assert summary["calendar"]["after"]["actual_missing_ids"] == []
    assert set(
        summary["calendar"]["after"][
            "unconfirmed_removals_retained"
        ]
    ) == set(summary["calendar"]["before"]["actual_missing_ids"])
    assert summary["catchup"]["actuals_recovered"] == 2
    assert summary["materialization"]["final_revision"] > summary[
        "materialization"
    ]["baseline_revision"]
    assert len(
        summary["materialization"]["rematerialized_snapshot_ids"]
    ) == 2

    actuals = {
        item["occurrence_id"]: item for item in summary["actuals"]
    }
    assert actuals["xtb:146392:2026-07-24"]["actual"] == 628.0
    assert actuals["xtb:146392:2026-07-24"]["forecast"] == 610.0
    assert actuals["xtb:146392:2026-07-24"]["previous"] == 618.0
    assert actuals["xtb:146945:2026-07-24"]["actual"] == 53.6
    assert actuals["xtb:146945:2026-07-24"][
        "reference_period"
    ] == "2026-07"
    assert set(full["sections"]) == set(SECTION_NAMES)
    assert len(full["sections"]) == 17
    assert full["payload_size_bytes"] == len(encoded)
    assert encoded == repeated
    assert b'"actual":628.0' in encoded
    assert b'"actual":53.6' in encoded
    assert summary["news"]["admitted_count"] == 4
    assert summary["news"]["quarantined_count"] == 0
    assert all(
        item["present_in_full_sync"]
        for item in summary["news"]["admitted_articles"]
    )
    assert summary["news"]["temporal_distinct_reuters_delivered"] is True
    assert summary["news"]["unknown_publisher_via_yahoo_admitted"] is True
