from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.providers.cboe_put_call_provider import normalize_cboe_put_call
from app.providers.cboe_vix_futures_provider import parse_vix_futures_csv
from app.providers.cftc_cot_provider import parse_cftc_financial_row
from app.services.ai_trader_consumer_v2_service import (
    build_ai_trader_consumer_v2,
)
from app.services.data_lifecycle_service import attach_lifecycle_metadata
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
)
from app.services.lifecycle_due_resolver import (
    DeterministicLifecycleDueResolver,
    StaticLifecycleProviderAdapter,
    TemporaryLifecycleProviderError,
    _select_earnings,
)
from app.services.observability_contract_service import (
    DeterministicAnomalyDetector,
)
from app.services.market_context_outbox_service import (
    MarketContextOutboxRepository,
)
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.research_backend import normalize_backend_payload
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.research_semantics import (
    normalize_research_claim,
    semantic_validation_warnings,
)
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_domain_service import (
    canonical_event_key,
    temporal_event_state,
)
from scripts.replay_snapshot83_forensics import replay


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "snapshot_83_forensic_redacted.json"
NOW = datetime(2026, 7, 24, 15, 1, 42, tzinfo=UTC)


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


def forensic_fixture() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def consumer_input(
    events: list[dict[str, Any]],
    *,
    nasdaq: dict[str, Any] | None = None,
    generated_at: datetime = NOW,
) -> dict[str, Any]:
    return {
        "snapshot_id": "mcs-offline-test",
        "snapshot_revision": 1,
        "symbol": "MNQ",
        "generated_at_utc": generated_at.isoformat(),
        "data_as_of": generated_at.isoformat(),
        "event_calendar": {
            "critical_macro_events": events,
            "fed_communications": [],
            "other_economic_events": [],
        },
        "events_today": events,
        "nasdaq_context": nasdaq or {},
        "research": {"status": "NOT_REQUIRED"},
        "ai_enrichment": {"status": "NOT_REQUIRED"},
        "readiness": {"section_status": {}},
    }


def seed_macro_lifecycle(
    settings: Settings,
    *,
    now: datetime,
    release_at: datetime,
    actual: float | None = None,
) -> dict[str, Any]:
    payload = {
        "event_id": "event-0900",
        "provider": "official-calendar",
        "provider_event_id": "event-0900",
        "country": "US",
        "name": "Offline event",
        "release_at": release_at.isoformat(),
        "actual": actual,
        "valid_until": (release_at + timedelta(minutes=30)).isoformat(),
        "source": "official-calendar",
        "source_url": "https://www.bls.gov/schedule/",
    }
    lifecycle = compute_datum_lifecycle(
        "macro_actual",
        "event-0900",
        payload,
        settings=settings,
        now=now,
        fields_attempted=["actual"],
    )
    return LifecycleRepository(settings, clock=lambda: now).upsert(
        lifecycle,
        payload=payload,
    )


def test_01_snapshot83_offline_replay_closes_all_findings(
    tmp_path: Path,
) -> None:
    result = replay(FIXTURE, settings=cfg(tmp_path))
    assert result["offline"] is True
    assert result["passed"] is True
    assert result["findings"]["live_calls_executed"] == 0


@pytest.mark.parametrize(
    ("offset_minutes", "actual", "audit_status", "expected"),
    [
        (-1, None, None, "PRE_RELEASE"),
        (0, None, None, "AWAITING_ACTUAL"),
        (0, 51.2, None, "RELEASED"),
        (0, 51.2, "QUARANTINED", "QUARANTINED"),
    ],
)
def test_02_temporal_state_truth_table(
    offset_minutes: int,
    actual: float | None,
    audit_status: str | None,
    expected: str,
) -> None:
    event = {
        "provider": "provider",
        "provider_event_id": "evt",
        "country": "US",
        "name": "Event",
        "release_at": NOW.isoformat(),
        "actual": actual,
        "audit_status": audit_status,
    }
    state = temporal_event_state(
        event,
        now=NOW + timedelta(minutes=offset_minutes),
    )
    assert state["temporal_status"] == expected


def test_03_snapshot83_past_events_are_awaiting_actual_not_pre_release(
    tmp_path: Path,
) -> None:
    events = forensic_fixture()["macro_events"]
    consumer = build_ai_trader_consumer_v2(
        consumer_input(events),
        settings=cfg(tmp_path),
    )
    risk = consumer["event_risk"]
    assert len(risk["awaiting_actual_events"]) == 3
    assert risk["upcoming_events"] == []


def test_04_provider_identity_deduplicates_label_and_category_aliases() -> None:
    first, second = forensic_fixture()["macro_events"][:2]
    assert first["name"] != second["name"]
    assert first["category"] != second["category"]
    assert canonical_event_key(first) == canonical_event_key(second)


def test_05_same_provider_event_tomorrow_is_a_distinct_occurrence() -> None:
    event = forensic_fixture()["macro_events"][0]
    tomorrow = {
        **event,
        "date": "2026-07-25",
        "release_at": "2026-07-25T13:45:00+00:00",
        "time_utc": "2026-07-25T13:45:00+00:00",
    }
    assert canonical_event_key(event) != canonical_event_key(tomorrow)


def test_06_next_critical_event_is_strictly_future(tmp_path: Path) -> None:
    past = forensic_fixture()["macro_events"][0]
    future = {
        **past,
        "provider_event_id": "tomorrow",
        "release_at": "2026-07-25T13:45:00+00:00",
        "time_utc": "2026-07-25T13:45:00+00:00",
    }
    risk = build_ai_trader_consumer_v2(
        consumer_input([past, future]),
        settings=cfg(tmp_path),
    )["event_risk"]
    assert datetime.fromisoformat(
        risk["next_critical_event"]["release_at"].replace("Z", "+00:00")
    ) == datetime.fromisoformat(future["release_at"])


def test_07_released_event_is_projected_as_recent(tmp_path: Path) -> None:
    event = {
        **forensic_fixture()["macro_events"][2],
        "actual": 51.2,
        "release_at": (NOW - timedelta(hours=1)).isoformat(),
        "time_utc": (NOW - timedelta(hours=1)).isoformat(),
    }
    risk = build_ai_trader_consumer_v2(
        consumer_input([event]),
        settings=cfg(tmp_path),
    )["event_risk"]
    assert len(risk["recently_released_events"]) == 1
    assert risk["recently_released_events"][0]["actual"] == 51.2


def test_08_published_issuer_announcement_is_current() -> None:
    policy = SourcePolicyService(ROOT / "config" / "source_policy.json")
    claim = normalize_research_claim(
        forensic_fixture()["issuer_announcement"],
        policy=policy,
        now=NOW,
    )
    warnings = semantic_validation_warnings(claim, policy=policy, now=NOW)
    assert claim["lifecycle_status"] == "CURRENT"
    assert claim["content_status"] == "PUBLISHED"
    assert "scheduled_event_elapsed_refresh_required" not in warnings


def test_09_earnings_schedule_and_published_result_are_distinct() -> None:
    policy = SourcePolicyService(ROOT / "config" / "source_policy.json")
    schedule = normalize_research_claim(
        {
            "topic": "earnings",
            "field_semantics": "earnings_schedule",
            "event_key": "AMD:2026-07-30",
            "issuer": "AMD",
            "event_at": "2026-07-30T20:00:00+00:00",
        },
        policy=policy,
        now=NOW,
    )
    result = normalize_research_claim(
        {
            "topic": "earnings",
            "field_semantics": "verified_corporate_metric",
            "symbol": "AMD",
            "metric_id": "eps_actual",
            "value": 1.23,
            "published_at": "2026-07-24T14:00:00+00:00",
        },
        policy=policy,
        now=NOW,
    )
    assert schedule["lifecycle_status"] == "UPCOMING"
    assert result["lifecycle_status"] == "CURRENT"


def test_10_cftc_structured_parser_uses_exact_mnq_contract() -> None:
    row = next(csv.reader(StringIO(forensic_fixture()["cftc_row"])))
    parsed = parse_cftc_financial_row(row)
    assert parsed["cftc_contract_market_code"] == "209747"
    assert parsed["open_interest"] == 278558
    assert parsed["validation"]["valid"] is True
    assert parsed["asset_managers"]["long"] == 102755
    assert parsed["leveraged_funds"]["short"] == 105413


def test_11_cboe_put_call_is_numeric_not_fuzzy() -> None:
    payload = {
        **forensic_fixture()["cboe"]["put_call"],
        "selectedDate": "2026-07-24",
    }
    ratios, rejected = normalize_cboe_put_call(
        payload,
        retrieved_at=NOW.isoformat(),
        valid_until=(NOW + timedelta(hours=18)).isoformat(),
    )
    assert rejected == 0
    assert ratios[0]["ratio"] == 0.82
    assert ratios[0]["valid_from"] < ratios[0]["valid_until"]


def test_12_cboe_invalid_validity_interval_is_rejected() -> None:
    payload = {
        **forensic_fixture()["cboe"]["put_call"],
        "selectedDate": "2026-07-24",
    }
    ratios, rejected = normalize_cboe_put_call(
        payload,
        retrieved_at=NOW.isoformat(),
        valid_until=(NOW - timedelta(seconds=1)).isoformat(),
    )
    assert ratios == []
    assert rejected == 1


def test_13_cboe_future_settlement_date_is_quarantined() -> None:
    contracts, diagnostics = parse_vix_futures_csv(
        forensic_fixture()["cboe"]["vx_csv"],
        data_as_of="2099-07-24",
    )
    assert contracts == []
    assert diagnostics["future_timestamp_quarantined_count"] == 1


def test_14_invalid_nasdaq_source_fails_closed(tmp_path: Path) -> None:
    invalid = forensic_fixture()["nasdaq_invalid"]
    nasdaq = {"status": "AVAILABLE", "qqq_holdings": invalid}
    projected = build_ai_trader_consumer_v2(
        consumer_input([], nasdaq=nasdaq),
        settings=cfg(tmp_path),
    )["nasdaq"]
    assert projected["status"] == "NOT_AVAILABLE"
    assert projected["holdings_count"] == 0
    assert projected["concentration"] == {}


def test_15_explicit_last_known_good_preserves_audited_holdings(
    tmp_path: Path,
) -> None:
    qqq = {
        "status": "LAST_KNOWN_GOOD",
        "last_known_good": True,
        "holdings": [{"symbol": "AMD", "weight_pct": 8.5}],
        "holdings_count": 1,
        "data_as_of": "2026-07-24T13:00:00+00:00",
        "valid_until": "2026-07-24T16:00:00+00:00",
        "age_minutes": 121,
        "reliability": 0.72,
        "quality_penalty": "stale_source",
    }
    projected = build_ai_trader_consumer_v2(
        consumer_input([], nasdaq={"qqq_holdings": qqq}),
        settings=cfg(tmp_path),
    )["nasdaq"]
    assert projected["status"] == "LAST_KNOWN_GOOD"
    assert projected["holdings_count"] == 1


def test_16_unqualified_last_known_good_is_not_operational(
    tmp_path: Path,
) -> None:
    qqq = {
        "status": "LAST_KNOWN_GOOD",
        "holdings": [{"symbol": "AMD", "weight_pct": 8.5}],
        "holdings_count": 1,
    }
    projected = build_ai_trader_consumer_v2(
        consumer_input([], nasdaq={"qqq_holdings": qqq}),
        settings=cfg(tmp_path),
    )["nasdaq"]
    assert projected["status"] == "NOT_AVAILABLE"
    assert projected["holdings_count"] == 0


def test_17_disabled_domains_are_compact_and_listed(tmp_path: Path) -> None:
    consumer = build_ai_trader_consumer_v2(
        consumer_input([]),
        settings=cfg(tmp_path),
    )
    expected = sorted(forensic_fixture()["disabled_topics"])
    assert consumer["research"]["disabled_optional_topics"] == expected
    for topic in expected:
        assert consumer["agentic_domains"][topic] == {
            "status": "DISABLED",
            "enabled": False,
            "reason": "agent_disabled_by_configuration",
        }


def test_18_consumer_measures_real_utf8_bytes_without_reduction(tmp_path: Path) -> None:
    full = consumer_input([])
    full["quality"] = {"verbose": "é" * 120_000}
    full["news_context"] = {
        "status": "AVAILABLE",
        "articles": [
            {
                "article_id": f"article-{index}",
                "title": "é" * 4_000,
                "published_at": NOW.isoformat(),
                "source_url": f"https://www.reuters.com/article/{index}",
            }
            for index in range(40)
        ],
    }
    consumer = build_ai_trader_consumer_v2(full, settings=cfg(tmp_path))
    encoded = json.dumps(
        consumer,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert len(encoded) > 500_000
    assert len(consumer["news"]["articles"]) == 40
    assert consumer["payload_measurement"]["size_limit_applied"] is False
    assert consumer["payload_measurement"]["records_removed_for_size"] == 0


def test_19_empty_domain_payload_is_not_data_present(tmp_path: Path) -> None:
    projected = attach_lifecycle_metadata(
        {
            "news_context": {"status": "NO_DATA", "articles": []},
            "positioning": {"status": "NO_DATA", "report_date": None},
            "sentiment_context": {
                "aaii": {"status": "NOT_CONFIGURED"},
                "prediction_markets": {"status": "NOT_CONFIGURED"},
            },
        },
        settings=cfg(tmp_path),
        now=NOW,
    )
    lifecycle = projected["metadata"]["data_lifecycle"]
    assert lifecycle["news"]["data_present"] is False
    assert lifecycle["cot"]["data_present"] is False
    assert lifecycle["aaii"]["freshness_state"] == "NOT_CONFIGURED"
    assert lifecycle["prediction_markets"]["currently_valid"] is False


def test_20_startup_before_event_has_zero_provider_and_ai(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
    )
    start = datetime(2026, 7, 24, 8, tzinfo=UTC)
    seed_macro_lifecycle(
        settings,
        now=start,
        release_at=datetime(2026, 7, 24, 9, tzinfo=UTC),
    )
    calls = {"provider": 0, "ai": 0}
    result = ResearchSchedulerService(
        settings,
        clock=lambda: start,
    ).startup_catch_up(
        resolver=lambda _: calls.__setitem__(
            "provider",
            calls["provider"] + 1,
        )
        or {"status": "NO_DATA"},
        ai_enqueue=lambda _: calls.__setitem__("ai", calls["ai"] + 1),
    )
    assert result["claimed"] == 0
    assert calls == {"provider": 0, "ai": 0}


def test_21_startup_one_hour_after_event_resolves_same_occurrence(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
    )
    release = datetime(2026, 7, 24, 9, tzinfo=UTC)
    start = release + timedelta(hours=1)
    baseline_event = {
        "event_id": "event-0900",
        "provider": "official-calendar",
        "provider_event_id": "event-0900",
        "country": "US",
        "name": "Offline event",
        "release_at": release.isoformat(),
        "time_utc": release.isoformat(),
        "actual": None,
        "impact": "HIGH",
        "source": "official-calendar",
        "source_url": "https://www.bls.gov/schedule/",
    }
    snapshots = MarketContextSnapshotRepository(settings)
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="offline_baseline",
        debug_payload=consumer_input(
            [baseline_event],
            generated_at=start,
        ),
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    seed_macro_lifecycle(settings, now=start, release_at=release)
    calls: list[str] = []
    scheduler = ResearchSchedulerService(
        settings,
        clock=lambda: start,
    )
    result = scheduler.startup_catch_up(
        resolver=lambda item: calls.append(str(item["entity_key"]))
        or {
            "status": "RESOLVED",
            "datum": {
                "event_id": "event-0900",
                "provider_event_id": "event-0900",
                "country": "US",
                "release_at": release.isoformat(),
                "actual": 52.1,
                "valid_until": (start + timedelta(hours=1)).isoformat(),
                "source": "official-provider",
                "source_url": "https://www.bls.gov/schedule/",
            },
        },
        ai_enqueue=lambda _: pytest.fail("AI must not be invoked"),
    )
    assert calls == ["event-0900"]
    assert result["resolved"]
    assert result["ai_invocations"] == 0
    latest = snapshots.latest("MNQ")
    assert latest is not None
    assert latest["revision"] == 2
    assert (
        latest["consumer_payload"]["event_risk"]["recently_released_events"][0][
            "actual"
        ]
        == 52.1
    )
    assert len(
        MarketContextOutboxRepository(settings).list_events(status=None)
    ) == 1
    repeated = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("provider must not run twice"),
        ai_enqueue=lambda _: pytest.fail("AI must not run twice"),
    )
    assert repeated["claimed"] == 0
    assert snapshots.latest("MNQ")["revision"] == 2
    assert len(
        MarketContextOutboxRepository(settings).list_events(status=None)
    ) == 1


@pytest.mark.parametrize(
    ("enable_scheduler", "research_scheduler_enabled", "due_enabled"),
    [(False, True, True), (True, False, True), (True, True, False)],
)
def test_22_disabled_startup_catchup_is_zero_work_and_zero_write(
    tmp_path: Path,
    enable_scheduler: bool,
    research_scheduler_enabled: bool,
    due_enabled: bool,
) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=enable_scheduler,
        research_scheduler_enabled=research_scheduler_enabled,
        lifecycle_due_scanner_enabled=due_enabled,
    )
    scheduler = ResearchSchedulerService(settings, clock=lambda: NOW)
    with connect_sqlite(settings.database_path) as conn:
        before = conn.execute(
            "SELECT COUNT(*) FROM service_telemetry_events"
        ).fetchone()[0]
    result = scheduler.startup_catch_up(
        resolver=lambda _: pytest.fail("provider must not be called"),
        ai_enqueue=lambda _: pytest.fail("AI must not be called"),
    )
    with connect_sqlite(settings.database_path) as conn:
        after = conn.execute(
            "SELECT COUNT(*) FROM service_telemetry_events"
        ).fetchone()[0]
    assert result["status"] == "DISABLED"
    assert result["writes"] == 0
    assert before == after


def test_23_provider_full_resolution_is_zero_ai() -> None:
    settings = Settings(_env_file=None)
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={
            "vix": StaticLifecycleProviderAdapter(
                "RESOLVED",
                {
                    "value": 18.5,
                    "data_as_of": NOW.isoformat(),
                    "valid_until": (NOW + timedelta(hours=1)).isoformat(),
                    "source": "offline-provider",
                },
            )
        },
    )
    result = resolver.resolve(
        {
            "entity_type": "vix",
            "entity_key": "VIX",
            "fields_attempted": ["value"],
        }
    )
    assert result["status"] == "RESOLVED"
    assert result["ai_eligible"] is False


def test_24_partial_provider_only_exposes_missing_fields_to_ai() -> None:
    settings = Settings(_env_file=None)
    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={
            "vix": StaticLifecycleProviderAdapter(
                "PARTIAL",
                {
                    "value": 18.5,
                    "data_as_of": NOW.isoformat(),
                    "valid_until": (NOW + timedelta(hours=1)).isoformat(),
                },
            )
        },
    )
    result = resolver.resolve(
        {
            "entity_type": "vix",
            "entity_key": "VIX",
            "fields_attempted": ["value", "previous_close"],
        }
    )
    assert result["status"] == "PARTIAL"
    assert result["missing_fields"] == ["previous_close"]
    assert result["ai_eligible"] is True


def test_25_temporary_provider_failure_creates_backoff_and_negative_cache() -> None:
    settings = Settings(_env_file=None)

    def temporary(_: dict[str, Any]) -> dict[str, Any]:
        raise TemporaryLifecycleProviderError("offline_timeout")

    resolver = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
        adapters={"vix": temporary},
    )
    result = resolver.resolve(
        {
            "entity_type": "vix",
            "entity_key": "VIX",
            "fields_attempted": ["value"],
        }
    )
    lifecycle = result["lifecycle"]
    assert result["status"] == "DEFERRED"
    assert result["ai_eligible"] is False
    assert lifecycle.freshness_state == "NO_DATA_BACKOFF"
    assert lifecycle.next_retry_at == lifecycle.negative_cache_expires_at


def test_26_retry_deadline_terminates_no_data_without_ai() -> None:
    settings = Settings(
        _env_file=None,
        lifecycle_retry_deadline_hours=24,
    )
    result = DeterministicLifecycleDueResolver(
        settings,
        clock=lambda: NOW,
    ).resolve(
        {
            "entity_type": "macro_actual",
            "entity_key": "old-event",
            "event_at": (NOW - timedelta(hours=25)).isoformat(),
            "fields_attempted": ["actual"],
        }
    )
    assert result["retry_deadline_exhausted"] is True
    assert result["data_outcome"] == "NO_DATA"
    assert result["ai_eligible"] is False


def test_27_earnings_adapter_selects_exact_occurrence() -> None:
    selected = _select_earnings(
        {
            "events": [
                {"symbol": "AMD", "event_at": "2026-07-30T20:00:00Z"},
                {"symbol": "NVDA", "event_at": "2026-08-15T20:00:00Z"},
            ]
        },
        {"entity_key": "AMD:2026-07-30"},
    )
    assert selected is not None
    assert selected["symbol"] == "AMD"


@pytest.mark.parametrize("backend", ["codex_cli", "openai_api"])
def test_28_cli_api_normalized_contract_parity(backend: str) -> None:
    normalized = normalize_backend_payload(
        {
            "status": "PARTIAL",
            "plan": {},
            "searches": [],
            "acquisition_requests": [],
            "claims": [{"metric_id": "vix", "value": 18.5}],
            "topic_statuses": {"vix_risk": "PARTIAL"},
            "warnings": ["residual_field_missing"],
            "backend": backend,
        }
    )
    comparable = {key: value for key, value in normalized.items() if key != "backend"}
    assert comparable["contract"]["version"] == "research_backend_v1"
    assert comparable["status"] == "PARTIAL"
    assert comparable["claims"][0]["value"] == 18.5


def test_29_anomaly_detector_reports_impossible_combinations(
    tmp_path: Path,
) -> None:
    categories = {
        item["category"]
        for item in DeterministicAnomalyDetector(
            cfg(tmp_path),
            clock=lambda: NOW,
        ).detect(
            {
                "events": [
                    {
                        "event_id": "past",
                        "release_at": (NOW - timedelta(hours=1)).isoformat(),
                        "temporal_status": "PRE_RELEASE",
                    }
                ],
                "next_critical_event": {
                    "release_at": (NOW - timedelta(minutes=1)).isoformat()
                },
                "duplicate_occurrence_count": 1,
                "lifecycle_items": [
                    {
                        "entity_key": "bad",
                        "valid_from": NOW.isoformat(),
                        "valid_until": (NOW - timedelta(seconds=1)).isoformat(),
                        "data_present": True,
                        "operational_value_present": False,
                        "status": "AVAILABLE",
                        "source_classification": "invalid_source",
                    }
                ],
                "deterministic_provider_available": True,
                "agent_invocation_attempted": True,
            }
        )
    }
    assert {
        "past_event_pre_release",
        "next_critical_event_in_past",
        "duplicate_event_occurrence",
        "invalid_validity_interval",
        "data_present_without_operational_value",
        "available_from_invalid_source",
        "provider_available_ai_invoked_without_reason",
    } <= categories


def test_30_catchup_window_is_bounded_to_configured_hours(
    tmp_path: Path,
) -> None:
    settings = cfg(
        tmp_path,
        enable_scheduler=True,
        research_scheduler_enabled=True,
        lifecycle_due_scanner_enabled=True,
        lifecycle_startup_catchup_hours=12,
    )
    result = ResearchSchedulerService(
        settings,
        clock=lambda: NOW,
    ).startup_catch_up(
        resolver=lambda _: {"status": "NO_DATA", "ai_eligible": False},
        ai_enqueue=lambda _: None,
    )
    assert result["catch_up_window_hours"] == 12
    assert result["catch_up_window_start"] == (
        NOW - timedelta(hours=12)
    ).isoformat()
