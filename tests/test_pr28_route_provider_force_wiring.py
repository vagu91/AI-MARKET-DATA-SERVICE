from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

import app.main as main_module
from app.api.deps import get_lifecycle_due_resolver
from app.bootstrap.application import build_application_state
from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.models.events import EconomicEvent
from app.providers.fred import FredProvider
from app.providers.sp_global_pmi import SpGlobalPmiProvider
from app.services.event_calendar_coverage_repository import (
    EventCalendarCoverageRepository,
)
from app.services.fed_expectations_repository import (
    FedExpectationsRepository,
)
from app.services.market_fact_repository import MarketFactRepository
from app.services.market_news_repository import MarketNewsRepository
from app.services.risk_context_repository import (
    RiskContextHistoryRepository,
)
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
)
from app.services.temporal_domain_service import exact_occurrence_key
from app.services.senior_analyst_projection_v1 import (
    DATASET_POLICIES,
    validate_senior_analyst_payload_v1,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    ROOT
    / "tests"
    / "fixtures"
    / "pr28_live_actual_reconciliation_redacted.json"
)
R3_FIXTURE = (
    ROOT
    / "tests"
    / "fixtures"
    / "pr28_r3_force_generation_redacted.json"
)
HOME_ID = "xtb:146392:2026-07-24"
PMI_ID = "xtb:146945:2026-07-24"


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "route-provider-force.sqlite",
        source_policy_path=ROOT / "config" / "source_policy.json",
        ai_job_workspace_root=tmp_path / "jobs",
        temp_dir=tmp_path / "temp",
        diagnostics_dir=tmp_path / "diagnostics",
        backups_dir=tmp_path / "backups",
        logs_dir=tmp_path / "logs",
        **{"fred_api_key": "test"},
        fred_enabled=True,
        bls_enabled=False,
        bea_enabled=False,
        census_enabled=False,
        sp_global_pmi_enabled=True,
        sp_global_pmi_release_url=(
            "https://pmi.spglobal.com/Public/Release/PressReleases"
        ),
        finnhub_enabled=False,
        tradier_enabled=False,
        enable_scheduler=False,
        event_calendar_catchup_enabled=False,
        ai_worker_enabled=False,
        enable_ai_researcher=False,
        research_agents_enabled=False,
        enable_openai_event_enrichment=False,
        enable_openai_fallback=False,
        enable_browser_scraping=False,
        enable_event_enrichment_scrapers=False,
        enable_targeted_search_enrichment=False,
        enable_investing_calendar=False,
        enable_investing_holidays=False,
        enable_marketbeat_holidays=False,
        enable_investing_fed_rate_monitor=False,
        enable_cboe_risk_indices=False,
        enable_cboe_vix_futures=False,
        enable_cboe_put_call=False,
        enable_nasdaq_earnings=False,
        enable_fmp_earnings=False,
        enable_xtb_calendar=False,
        enable_nasdaq_100=False,
        enable_nasdaq_market_info=False,
        enable_cme_market_schedule=False,
        enable_nasdaq_qqq_options=False,
        enable_aaii_sentiment=False,
        enable_social_sentiment=False,
        timeout_macro_seconds=2,
        timeout_events_seconds=2,
        timeout_nasdaq_seconds=2,
        timeout_cot_seconds=2,
        timeout_sentiment_seconds=2,
        http_timeout_seconds=1,
    )


def _seed_route_state(
    cfg: Settings,
    event_service: Any,
    *,
    seed_stale_lifecycle: bool = False,
) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    r3_stale = json.loads(
        R3_FIXTURE.read_text(encoding="utf-8")
    )["stale_no_data"]
    facts = MarketFactRepository(cfg)
    for raw in fixture["initial_events"]:
        event = EconomicEvent.model_validate(
            {
                **raw,
                "metric_id": (
                    "new_home_sales"
                    if raw["occurrence_id"] == HOME_ID
                    else "flash_services_pmi"
                ),
                "enrichment": {
                    **raw["enrichment"],
                    "field_lineage": {
                        "forecast": {
                            "source": "XTB Economic Calendar",
                            "source_url": raw["source_url"],
                            "source_field": "forecast",
                            "field_semantics": "forecast",
                        },
                        "previous": {
                            "source": "XTB Economic Calendar",
                            "source_url": raw["source_url"],
                            "source_field": "previous",
                            "field_semantics": "previous",
                        },
                    },
                },
            }
        )
        assert facts.upsert_economic_event(
            event,
            event_key=exact_occurrence_key(event),
        )
        if seed_stale_lifecycle:
            now = datetime.now(UTC)
            payload = event.model_dump(mode="json")
            payload["valid_until"] = (
                now
                + timedelta(
                    seconds=int(
                        r3_stale["valid_until_offset_seconds"]
                    )
                )
            ).isoformat()
            lifecycle = compute_datum_lifecycle(
                "macro_actual",
                str(event.occurrence_id),
                payload,
                settings=cfg,
                now=now,
                attempt_count=int(r3_stale["attempt_count"]),
                fields_attempted=["actual"],
                refresh_reason=str(r3_stale["refresh_reason"]),
            )
            LifecycleRepository(cfg).upsert(
                replace(
                    lifecycle,
                    valid_until=payload["valid_until"],
                    next_refresh_at=payload["valid_until"],
                    next_retry_at=None,
                    freshness_state=str(
                        r3_stale["freshness_state"]
                    ),
                ),
                payload=payload,
                work_status=str(r3_stale["work_status"]),
            )

    now = datetime.now(UTC)
    timezone = ZoneInfo(str(cfg.event_calendar_timezone))
    local_now = now.astimezone(timezone)
    current_start = local_now.date() - timedelta(
        days=local_now.weekday()
    )
    requested_days = [
        current_start - timedelta(days=7) + timedelta(days=offset)
        for offset in range(21)
    ]
    window_start = datetime.combine(
        requested_days[0],
        time.min,
        timezone,
    ).astimezone(UTC)
    window_end = datetime.combine(
        requested_days[-1] + timedelta(days=1),
        time.min,
        timezone,
    ).astimezone(UTC)
    coverage = EventCalendarCoverageRepository(cfg)
    targets = event_service.coverage_targets(
        country="US",
        start=window_start,
        end=window_end,
    )
    for target in targets:
        for day in requested_days:
            coverage.record_day(
                day,
                provider_name=str(target["provider_name"]),
                query_scope=str(target["query_scope"]),
                window_start=window_start,
                window_end=window_end,
                status="VERIFIED_EMPTY",
                record_count=0,
                provider_called=True,
                scope_verified=True,
                proof={
                    "request_succeeded": True,
                    "pagination_complete": True,
                    "parsing_succeeded": True,
                    "records_valid": True,
                    "expected_sources_complete": True,
                    "authentic_empty": True,
                },
                lineage={
                    "fixture": "controlled_http_boundary",
                    "redacted": True,
                },
                valid_until=now + timedelta(days=1),
                policy_version=facts.source_policy.policy_version,
            )


def _seed_senior_canonical_facts(cfg: Settings) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    valid_until = now + timedelta(hours=6)
    facts = MarketFactRepository(cfg)
    macro_series = (
        ("DGS2", "FRED", "4.25"),
        ("DFF", "FRED", "4.33"),
        ("DFEDTARL", "FRED", "4.25"),
        ("DFEDTARU", "FRED", "4.50"),
        ("CUSR0000SA0", "BLS", "321.5"),
        ("WPUFD4", "BLS", "258.4"),
        ("BEA:PCE", "BEA", "0.3"),
        ("BEA:GDP", "BEA", "2.8"),
        ("LNS14000000", "BLS", "4.1"),
        ("CES0500000003", "BLS", "0.3"),
        ("CES0000000001", "BLS", "159500"),
        ("ICSA", "FRED", "218000"),
        ("VIXCLS", "FRED", "18.0"),
    )
    for series_id, source, value in macro_series:
        facts.upsert_fact(
            {
                "fact_key": (
                    f"US:{series_id}:latest:official_macro_latest"
                ),
                "fact_type": "official_macro_latest",
                "country": "US",
                "category": series_id,
                "event_name": series_id,
                "value": value,
                "unit": "index",
                "source": source,
                "provider_type": "API",
                "reliability": 0.95,
                "confidence": 0.95,
                "retrieved_at": now.isoformat(),
                "release_at": now.isoformat(),
                "valid_until": valid_until.isoformat(),
                "next_refresh_at": valid_until.isoformat(),
                "raw_payload_json": {
                    "series_id": series_id,
                    "data_as_of": now.isoformat(),
                    "content_valid_until": valid_until.isoformat(),
                    "refresh_due_at": valid_until.isoformat(),
                },
            }
        )

    decision_at = (now + timedelta(days=2)).replace(
        hour=18,
        minute=0,
        second=0,
    )
    fomc_event = EconomicEvent(
        event_id=f"fed:fomc:{decision_at.date().isoformat()}",
        provider="Federal Reserve",
        source_event_id=decision_at.date().isoformat(),
        occurrence_id=(
            f"fed:fomc:{decision_at.date().isoformat()}:decision"
        ),
        name="FOMC Rate Decision",
        country="US",
        category="FOMC",
        metric_id="fomc_rate_decision",
        date=decision_at.date().isoformat(),
        time_utc=decision_at,
        release_at=decision_at,
        impact="HIGH",
        source="Federal Reserve",
        source_url=(
            "https://www.federalreserve.gov/"
            "monetarypolicy/fomccalendars.htm"
        ),
        retrieved_at=now,
        reliability=0.98,
        event_risk_level="HIGH",
        enrichment={
            "source": "Federal Reserve",
            "source_url": (
                "https://www.federalreserve.gov/"
                "monetarypolicy/fomccalendars.htm"
            ),
            "retrieved_at": now,
            "valid_until": valid_until,
            "reliability": 0.98,
            "confidence": 0.98,
            "fomc_context": {
                "meeting_date": decision_at.date().isoformat(),
                "decision_time_utc": decision_at.isoformat(),
                "current_target_range_lower": 4.25,
                "current_target_range_upper": 4.50,
                "expected_action": "hold",
                "expected_change_bps": 0,
                "probability_hold": 0.8,
                "probability_cut_25bps": 0.15,
                "probability_hike_25bps": 0.05,
                "probability_source": (
                    "Investing.com Fed Rate Monitor"
                ),
            },
        },
    )
    assert facts.upsert_economic_event(
        fomc_event,
        event_key=exact_occurrence_key(fomc_event),
    )

    meeting = {
        "meeting_date": decision_at.date().isoformat(),
        "meeting_at": decision_at.isoformat(),
        "expected_action": "hold",
        "expected_change_bps": 0,
        "probability_hold": 0.8,
        "probability_cut_25bps": 0.15,
        "probability_hike_25bps": 0.05,
        "outcomes": [
            {
                "classification": "hold",
                "probability": 0.8,
                "target_lower_bound": 4.25,
                "target_upper_bound": 4.50,
            },
            {
                "classification": "cut",
                "probability": 0.15,
                "target_lower_bound": 4.00,
                "target_upper_bound": 4.25,
            },
            {
                "classification": "hike",
                "probability": 0.05,
                "target_lower_bound": 4.50,
                "target_upper_bound": 4.75,
            },
        ],
    }
    fed_snapshot = {
        "status": "available",
        "data_as_of": now.isoformat(),
        "retrieved_at": now.isoformat(),
        "valid_until": valid_until.isoformat(),
        "refresh_due_at": valid_until.isoformat(),
        "current_fed_state": {
            "current_target_lower_bound": 4.25,
            "current_target_upper_bound": 4.50,
            "current_target_midpoint": 4.375,
            "effective_fed_funds_rate": 4.33,
            "next_fomc_meeting_at": decision_at.isoformat(),
        },
        "next_meeting": dict(meeting),
        "meetings": [dict(meeting)],
        "repricing": {
            "history_available": False,
            "history_status": "history_insufficient",
        },
        "source_summary": {
            "selected_source": (
                "Investing.com Fed Rate Monitor"
            ),
            "selected_source_type": "secondary_monitor",
            "ranking_class": "secondary_monitor",
            "is_official_source": False,
            "is_reconstructed": False,
            "last_known_good_used": False,
        },
        "quality": {
            "quality_score": 0.75,
            "meeting_coverage_pct": 100.0,
            "probability_distribution_coverage_pct": 100.0,
        },
        "diagnostics": {
            "provider_calls": 0,
            "browser_calls": 0,
            "AI_called": False,
            "cache_used": True,
            "materialized_count": 1,
        },
        "warnings": [],
        "errors": [],
    }
    FedExpectationsRepository(cfg).append(fed_snapshot)

    for name, fact_type, source, raw in (
        (
            "investing_fed_rate_monitor",
            "investing_fed_rate_monitor",
            "Investing.com Fed Rate Monitor",
            {
                "status": "found",
                "source": "Investing.com Fed Rate Monitor",
                "source_url": (
                    "https://www.investing.com/"
                    "central-banks/fed-rate-monitor"
                ),
                "data_as_of": now.isoformat(),
                "retrieved_at": now.isoformat(),
                "valid_until": valid_until.isoformat(),
                "next_refresh_at": valid_until.isoformat(),
                "meetings": [dict(meeting)],
                "warnings": [],
                "errors": [],
            },
        ),
        (
            "nasdaq_market_info",
            "nasdaq_market_info",
            "Nasdaq Market Info",
            {
                "status": "found",
                "source": "Nasdaq Market Info",
                "provider": "Nasdaq Market Info",
                "source_url": (
                    "https://www.nasdaq.com/market-activity/"
                    "stock-market-holiday-schedule"
                ),
                "data_as_of": now.isoformat(),
                "retrieved_at": now.isoformat(),
                "valid_until": valid_until.isoformat(),
                "next_refresh_at": valid_until.isoformat(),
                "validation": {
                    "status": "accepted",
                    "policy_version": "source-policy-v5",
                },
                "warnings": [],
                "errors": [],
            },
        ),
    ):
        facts.upsert_fact(
            {
                "fact_key": (
                    f"multi_source:{name}:{fact_type}:latest"
                ),
                "fact_type": fact_type,
                "country": "US",
                "category": fact_type,
                "source": source,
                "provider_type": "API",
                "reliability": 0.9,
                "confidence": 0.9,
                "retrieved_at": now.isoformat(),
                "release_at": now.isoformat(),
                "valid_until": valid_until.isoformat(),
                "next_refresh_at": valid_until.isoformat(),
                "raw_payload_json": raw,
            }
        )

    nasdaq_facts = (
        (
            "nasdaq_context:qqq_holdings",
            "qqq_holdings",
            {
                "status": "AVAILABLE",
                "source": "INVESCO",
                "holdings_count": 2,
                "holdings": [
                    {
                        "symbol": "NVDA",
                        "weight": 9.0,
                        "sector": "Technology",
                    },
                    {
                        "symbol": "MSFT",
                        "weight": 8.0,
                        "sector": "Technology",
                    },
                ],
            },
        ),
        (
            "nasdaq_context:mega_cap_snapshot",
            "mega_cap_snapshot",
            {
                "status": "AVAILABLE",
                "source": "NASDAQ",
                "stocks": [
                    {
                        "symbol": "NVDA",
                        "price": 150.0,
                        "change_pct": 1.2,
                    },
                    {
                        "symbol": "MSFT",
                        "price": 510.0,
                        "change_pct": -0.2,
                    },
                ],
                "data_quality": {
                    "tracked_count": 2,
                    "resolved_count": 2,
                },
            },
        ),
        (
            "nasdaq_context:mega_cap_breadth",
            "mega_cap_breadth",
            {
                "status": "AVAILABLE",
                "source": "NASDAQ",
                "positive_count": 1,
                "negative_count": 1,
                "weighted_average_change_pct": 0.5,
            },
        ),
        (
            "nasdaq_context:earnings",
            "earnings_event",
            {
                "status": "AVAILABLE",
                "source": "NASDAQ",
                "events": [
                    {
                        "symbol": "NVDA",
                        "date": (
                            now.date() + timedelta(days=2)
                        ).isoformat(),
                    }
                ],
            },
        ),
    )
    for fact_key, fact_type, raw in nasdaq_facts:
        facts.upsert_fact(
            {
                "fact_key": fact_key,
                "fact_type": fact_type,
                "symbol": "QQQ",
                "category": fact_type,
                "source": raw["source"],
                "provider_type": "API",
                "reliability": 0.9,
                "confidence": 0.9,
                "retrieved_at": now.isoformat(),
                "release_at": now.isoformat(),
                "valid_until": valid_until.isoformat(),
                "next_refresh_at": valid_until.isoformat(),
                "raw_payload_json": {
                    **raw,
                    "data_as_of": now.isoformat(),
                    "retrieved_at": now.isoformat(),
                    "content_valid_until": valid_until.isoformat(),
                    "refresh_due_at": valid_until.isoformat(),
                },
            }
        )

    deterministic_sections = {
        "market_internals": {
            "status": "AVAILABLE",
            "provider": "TRADIER",
            "advance_decline_ratio": 1.2,
            "advancers": 60,
            "decliners": 40,
            "percent_advancers": 60.0,
            "weighted_breadth": 0.2,
            "stale_quote_count": 0,
        },
        "options_positioning": {
            "status": "AVAILABLE",
            "provider": "TRADIER",
            "iv_atm": 0.2,
            "open_interest": 1000,
            "volume": 250,
            "skew": -0.03,
        },
    }
    for dataset_id, raw in deterministic_sections.items():
        fact_type = f"deterministic_{dataset_id}"
        facts.upsert_fact(
            {
                "fact_key": f"MNQ:{dataset_id}:{fact_type}",
                "fact_type": fact_type,
                "country": "US",
                "symbol": "MNQ",
                "category": dataset_id,
                "event_name": dataset_id,
                "source": "TRADIER",
                "provider_type": "API",
                "reliability": 0.85,
                "confidence": 0.85,
                "retrieved_at": now.isoformat(),
                "release_at": now.isoformat(),
                "valid_until": valid_until.isoformat(),
                "next_refresh_at": valid_until.isoformat(),
                "raw_payload_json": {
                    **raw,
                    "data_as_of": now.isoformat(),
                    "retrieved_at": now.isoformat(),
                    "content_valid_until": valid_until.isoformat(),
                    "refresh_due_at": valid_until.isoformat(),
                },
            }
        )

    facts.upsert_fact(
        {
            "fact_key": "cot:nasdaq_100",
            "fact_type": "cot_positioning",
            "country": "US",
            "symbol": "NQ",
            "category": "cot_positioning",
            "event_name": "CFTC",
            "source": "CFTC",
            "provider_type": "OFFICIAL_WEB",
            "reliability": 0.95,
            "confidence": 0.95,
            "retrieved_at": now.isoformat(),
            "release_at": now.isoformat(),
            "valid_until": valid_until.isoformat(),
            "next_refresh_at": valid_until.isoformat(),
            "status": "active",
            "raw_payload_json": {
                "status": "found",
                "report_date": now.date().isoformat(),
                "publication_date": now.date().isoformat(),
                "market_name": "NASDAQ-100 Consolidated",
                "cftc_contract_market_code": "209742",
                "report_type": "TFF",
                "asset_managers": {
                    "long": 100,
                    "short": 80,
                    "spreading": 10,
                    "net": 20,
                    "net_change_week": 2,
                },
                "leveraged_funds": {
                    "long": 90,
                    "short": 110,
                    "spreading": 5,
                    "net": -20,
                    "net_change_week": -3,
                },
                "dealers": {
                    "long": 70,
                    "short": 65,
                    "net": 5,
                },
                "open_interest": 500,
                "source": "CFTC",
                "data_as_of": now.isoformat(),
                "retrieved_at": now.isoformat(),
                "content_valid_until": valid_until.isoformat(),
                "refresh_due_at": valid_until.isoformat(),
                "reliability": 0.95,
                "warnings": [],
                "errors": [],
            },
        }
    )

    risk_metric_metadata = {
        "status": "found",
        "data_as_of": now.isoformat(),
        "retrieved_at": now.isoformat(),
        "content_valid_until": valid_until.isoformat(),
        "refresh_due_at": valid_until.isoformat(),
        "freshness": "CURRENT",
        "source": "CBOE",
    }
    RiskContextHistoryRepository(cfg).append(
        {
            "status": "available",
            "data_as_of": now.isoformat(),
            "retrieved_at": now.isoformat(),
            "valid_until": valid_until.isoformat(),
            "content_valid_until": valid_until.isoformat(),
            "next_refresh_at": valid_until.isoformat(),
            "refresh_due_at": valid_until.isoformat(),
            "vix": {
                **risk_metric_metadata,
                "value": 18.0,
            },
            "vvix": {
                **risk_metric_metadata,
                "value": 90.0,
            },
            "skew": {
                **risk_metric_metadata,
                "value": 125.0,
            },
            "derived_context": {
                "risk_regime": "NEUTRAL",
                "risk_score": 0.5,
            },
            "source_summary": {
                "selected_sources": {
                    "vix": "CBOE",
                    "vvix": "CBOE",
                    "risk": "CBOE",
                },
                "last_known_good_used": False,
            },
            "quality": {
                "quality_score": 0.9,
                "vix_available": True,
                "vvix_available": True,
                "skew_available": True,
            },
            "diagnostics": {
                "provider_calls": 0,
                "actual_network_calls": 0,
                "cache_used": True,
            },
            "warnings": [],
            "errors": [],
        }
    )


def _counts(cfg: Settings) -> dict[str, int]:
    tables = (
        "market_context_snapshots",
        "market_context_outbox",
        "economic_events_history",
        "datum_lifecycle_items",
        "event_value_candidates",
        "event_calendar_coverage",
        "provider_state",
        "ai_research_jobs",
        "research_backend_invocations",
    )
    with sqlite3.connect(cfg.database_path) as connection:
        return {
            table: int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
            )
            for table in tables
        }


def _install_write_audit(cfg: Settings) -> None:
    tables = (
        "market_context_snapshots",
        "market_context_outbox",
        "economic_events_history",
        "datum_lifecycle_items",
        "event_value_candidates",
        "event_calendar_coverage",
        "provider_state",
        "ai_research_jobs",
        "research_backend_invocations",
    )
    with sqlite3.connect(cfg.database_path) as connection:
        connection.execute(
            "CREATE TABLE test_route_write_audit("
            "table_name TEXT NOT NULL, operation TEXT NOT NULL,"
            "row_id INTEGER, before_payload TEXT, after_payload TEXT)"
        )
        for table in tables:
            for operation in ("INSERT", "UPDATE", "DELETE"):
                row_reference = "OLD.rowid" if operation == "DELETE" else "NEW.rowid"
                before_payload = (
                    "OLD.raw_payload_json"
                    if table == "economic_events_history"
                    and operation == "UPDATE"
                    else "NULL"
                )
                after_payload = (
                    "NEW.raw_payload_json"
                    if table == "economic_events_history"
                    and operation == "UPDATE"
                    else "NULL"
                )
                connection.execute(
                    f"CREATE TRIGGER test_audit_{table}_{operation.lower()} "
                    f"AFTER {operation} ON {table} BEGIN "
                    "INSERT INTO test_route_write_audit("
                    "table_name,operation,row_id,before_payload,after_payload) "
                    f"VALUES ('{table}','{operation}',{row_reference},"
                    f"{before_payload},{after_payload}); END"
                )
        connection.commit()


def _write_audit(cfg: Settings) -> dict[str, int]:
    with sqlite3.connect(cfg.database_path) as connection:
        return {
            str(row[0]): int(row[1])
            for row in connection.execute(
                "SELECT table_name,COUNT(*) FROM test_route_write_audit "
                "GROUP BY table_name"
            )
        }


def _events(value: Any) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    if isinstance(value, dict):
        occurrence_id = value.get("occurrence_id") or value.get(
            "event_id"
        )
        if occurrence_id in {HOME_ID, PMI_ID}:
            selected.append(value)
        for child in value.values():
            selected.extend(_events(child))
    elif isinstance(value, list):
        for child in value:
            selected.extend(_events(child))
    return selected


def _install_controlled_application(
    monkeypatch: pytest.MonkeyPatch,
    cfg: Settings,
    *,
    fred_calls: list[str],
    sp_calls: list[str],
    sp_success: bool = False,
) -> None:
    controlled = json.loads(FIXTURE.read_text(encoding="utf-8"))[
        "controlled_http_boundary"
    ]

    def fred_http(request: httpx.Request) -> httpx.Response:
        series_id = str(request.url.params.get("series_id") or "")
        fred_calls.append(series_id)
        return httpx.Response(
            200,
            json={
                "observations": controlled["fred"].get(
                    series_id,
                    controlled["fred"]["default"],
                )
            },
            request=request,
        )

    def sp_http(request: httpx.Request) -> httpx.Response:
        sp_calls.append(str(request.url))
        if sp_success:
            return httpx.Response(
                200,
                text=(
                    "Flash US Services PMI Business Activity Index: "
                    "53.6 (June: 51.2)"
                ),
                request=request,
            )
        return httpx.Response(
            int(controlled["sp_global"]["status_code"]),
            text=str(controlled["sp_global"]["body"]),
            request=request,
        )

    def production_state(settings: Settings) -> dict[str, Any]:
        cache = ProviderCacheRepository(settings.database_path)
        return build_application_state(
            settings,
            deterministic_provider_overrides={
                "fred": FredProvider(
                    cache,
                    settings,
                    transport=httpx.MockTransport(fred_http),
                ),
                "spglobal": SpGlobalPmiProvider(
                    cache,
                    settings,
                    transport=httpx.MockTransport(sp_http),
                ),
            },
        )

    monkeypatch.setattr(main_module, "get_settings", lambda: cfg)
    monkeypatch.setattr(
        main_module,
        "build_application_state",
        production_state,
    )
    monkeypatch.setattr(
        main_module,
        "maybe_run_startup_cleanup",
        lambda _settings: None,
    )


def _baseline_missing_actual_snapshot(
    client: TestClient,
    cfg: Settings,
) -> None:
    main_module.app.dependency_overrides[
        get_lifecycle_due_resolver
    ] = lambda: object()
    try:
        response = client.get(
            "/market-context/mnq?refresh=force&view=debug"
        )
        assert response.status_code == 200, response.text
    finally:
        main_module.app.dependency_overrides.pop(
            get_lifecycle_due_resolver,
            None,
        )


def _seed_no_data_state(
    cfg: Settings,
    state: str,
) -> dict[str, str | None]:
    now = datetime.now(UTC)
    retry_by_id: dict[str, str | None] = {}
    facts = MarketFactRepository(cfg)
    payloads = {
        str(
            item.get("occurrence_id")
            or item.get("event_id")
            or item.get("event_key")
        ): dict(item)
        for item in facts.economic_event_payloads(
            country="US",
            start_date="2026-07-24",
            end_date="2026-07-24",
        )
    }
    lifecycle_repository = LifecycleRepository(cfg)
    for occurrence_id in (HOME_ID, PMI_ID):
        payload = dict(payloads[occurrence_id])
        base = compute_datum_lifecycle(
            "macro_actual",
            occurrence_id,
            payload,
            settings=cfg,
            now=now,
            attempt_count=1,
            fields_attempted=["actual"],
            refresh_reason=f"fixture_{state.lower()}",
        )
        if state == "FRESH_NO_DATA":
            future = (now + timedelta(hours=1)).isoformat()
            lifecycle = replace(
                base,
                freshness_state="NO_DATA",
                valid_until=future,
                next_refresh_at=future,
                next_retry_at=None,
                negative_cache_expires_at=None,
            )
            work_status = "COMPLETED"
        elif state == "BACKOFF":
            future = (now + timedelta(hours=1)).isoformat()
            lifecycle = replace(
                compute_datum_lifecycle(
                    "macro_actual",
                    occurrence_id,
                    payload,
                    settings=cfg,
                    now=now,
                    attempt_count=1,
                    no_data=True,
                    fields_attempted=["actual"],
                    refresh_reason="fixture_backoff",
                ),
                freshness_state="NO_DATA_BACKOFF",
                next_refresh_at=future,
                next_retry_at=future,
                negative_cache_expires_at=future,
            )
            work_status = "BACKOFF"
        else:
            lifecycle = replace(
                base,
                freshness_state="EXHAUSTED_NO_DATA",
                next_refresh_at=None,
                next_retry_at=None,
                negative_cache_key=None,
                negative_cache_expires_at=None,
            )
            work_status = "EXHAUSTED_NO_DATA"
        lifecycle_repository.upsert(
            lifecycle,
            payload=payload,
            work_status=work_status,
        )
        retry_by_id[occurrence_id] = lifecycle.next_retry_at
    return retry_by_id


def _canonical_state(cfg: Settings) -> dict[str, Any]:
    tables = (
        "event_calendar_coverage",
        "economic_events_history",
        "datum_lifecycle_items",
        "market_context_snapshots",
        "market_context_outbox",
        "ai_research_jobs",
    )
    with sqlite3.connect(cfg.database_path) as connection:
        return {
            table: connection.execute(
                f"SELECT * FROM {table} ORDER BY rowid"
            ).fetchall()
            for table in tables
        }


def test_real_app_route_wires_official_actuals_and_fixed_point(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cfg = _settings(tmp_path)
    provider_fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))[
        "controlled_http_boundary"
    ]
    fred_calls: list[str] = []
    sp_calls: list[str] = []

    def fred_http(request: httpx.Request) -> httpx.Response:
        series_id = str(request.url.params.get("series_id") or "")
        fred_calls.append(series_id)
        observations = provider_fixture["fred"].get(
            series_id,
            provider_fixture["fred"]["default"],
        )
        return httpx.Response(
            200,
            json={"observations": observations},
            request=request,
        )

    def sp_http(request: httpx.Request) -> httpx.Response:
        sp_calls.append(str(request.url))
        return httpx.Response(
            int(provider_fixture["sp_global"]["status_code"]),
            text=str(provider_fixture["sp_global"]["body"]),
            request=request,
        )

    def production_state(settings: Settings) -> dict[str, Any]:
        cache = ProviderCacheRepository(settings.database_path)
        return build_application_state(
            settings,
            deterministic_provider_overrides={
                "fred": FredProvider(
                    cache,
                    settings,
                    transport=httpx.MockTransport(fred_http),
                ),
                "spglobal": SpGlobalPmiProvider(
                    cache,
                    settings,
                    transport=httpx.MockTransport(sp_http),
                ),
            },
        )

    monkeypatch.setattr(main_module, "get_settings", lambda: cfg)
    monkeypatch.setattr(
        main_module,
        "build_application_state",
        production_state,
    )
    monkeypatch.setattr(
        main_module,
        "maybe_run_startup_cleanup",
        lambda _settings: None,
    )
    with respx.mock(assert_all_mocked=False) as network:
        network.route().respond(404)
        with TestClient(main_module.app) as client:
            _seed_route_state(
                cfg,
                main_module.app.state.event_service,
                seed_stale_lifecycle=True,
            )
            before = _counts(cfg)

            response = client.get(
                "/market-context/mnq?refresh=force&view=debug"
            )
            assert response.status_code == 200, response.text
            debug = response.json()
            full_response = client.get("/market-context/mnq/sync/full")
            assert full_response.status_code == 200, full_response.text
            full = full_response.json()
            after_first = _counts(cfg)

            debug_by_id: dict[str, dict[str, Any]] = {}
            for item in _events(debug):
                occurrence_id = str(
                    item.get("occurrence_id") or item.get("event_id")
                )
                current = debug_by_id.get(occurrence_id)
                score = sum(
                    key in item
                    for key in (
                        "actual",
                        "forecast",
                        "previous",
                        "reference_period",
                        "actual_resolution",
                    )
                )
                current_score = sum(
                    key in (current or {})
                    for key in (
                        "actual",
                        "forecast",
                        "previous",
                        "reference_period",
                        "actual_resolution",
                    )
                )
                if current is None or score > current_score:
                    debug_by_id[occurrence_id] = item
            home = debug_by_id[HOME_ID]
            pmi = debug_by_id[PMI_ID]
            assert float(home["actual"]) == 628.0
            assert float(home["forecast"]) == 610.0
            assert float(home["previous"]) == 618.0
            assert home["reference_period"] == "2026-06"
            assert home["release_status"] == "RELEASED"
            lineage = home["enrichment"]["field_lineage"]
            assert lineage["actual"]["source_series_id"] == "HSN1F"
            assert lineage["actual"]["validation_status"] == "accepted"
            assert (
                lineage["previous"]["derivation"]
                == "previous_official_series_observation"
            )
            assert lineage["forecast"]["source"] == (
                "XTB Economic Calendar"
            )

            assert pmi["actual"] is None
            assert float(pmi["forecast"]) == 51.5
            assert float(pmi["previous"]) == 51.2
            assert pmi["reference_period"] == "2026-07"
            assert pmi["release_status"] == "AWAITING_ACTUAL"
            pmi_audit = pmi["actual_resolution"]
            assert pmi_audit["resolver_invoked"] is True
            assert pmi_audit["provider_attempted"] == "SPGLOBAL"
            assert pmi_audit["provider_http_outcome"] == "HTTP_403"
            assert pmi_audit["reason_code"] == (
                "all_flash_services_pmi_providers_failed:"
                "investing_flash_services_pmi_http_404"
            )
            assert pmi_audit["retryable"] is True
            assert pmi_audit["actual_still_missing"] is True

            route_audit = debug["data_quality"][
                "actual_reconciliation"
            ]
            audits = {
                item["occurrence_id"]: item
                for item in route_audit["occurrences"]
            }
            assert audits[HOME_ID]["source_series"] == "HSN1F"
            assert audits[HOME_ID]["eligibility"] == "RECLAIMABLE"
            assert audits[HOME_ID]["reclaim_reason"] == (
                "STALE_NO_DATA_RETRY_DUE"
            )
            assert audits[HOME_ID]["provider_call_count"] == 1
            assert audits[HOME_ID]["candidate_validation"] == "accepted"
            assert audits[HOME_ID]["persistence_outcome"] == (
                "ATOMIC_COMMIT_WITH_SNAPSHOT"
            )
            assert audits[PMI_ID]["provider_http_outcome"] == "HTTP_403"
            assert "HSN1F" in fred_calls
            assert len(sp_calls) == 1
            with sqlite3.connect(cfg.database_path) as connection:
                assert connection.execute(
                    """
                    SELECT COUNT(*) FROM datum_lifecycle_items
                    WHERE entity_type='macro_actual'
                      AND entity_key IN (?,?)
                    """,
                    (HOME_ID, PMI_ID),
                ).fetchone()[0] == 2

            assert len(full["sections"]) == 17
            full_by_id: dict[str, dict[str, Any]] = {}
            for item in _events(full):
                occurrence_id = str(
                    item.get("occurrence_id") or item.get("event_id")
                )
                current = full_by_id.get(occurrence_id)
                if current is None or item.get("actual_resolution"):
                    full_by_id[occurrence_id] = item
            assert float(full_by_id[HOME_ID]["actual"]) == 628.0
            assert full_by_id[HOME_ID]["reference_period"] == "2026-06"
            assert full_by_id[PMI_ID]["actual"] is None
            assert full_by_id[PMI_ID]["reference_period"] == "2026-07"
            assert full_by_id[PMI_ID]["actual_resolution"][
                "reason_code"
            ] == (
                "all_flash_services_pmi_providers_failed:"
                "investing_flash_services_pmi_http_404"
            )

            assert (
                after_first["market_context_snapshots"]
                - before["market_context_snapshots"]
                == 1
            )
            assert (
                after_first["market_context_outbox"]
                - before["market_context_outbox"]
                <= 1
            )
            assert (
                after_first["ai_research_jobs"]
                == before["ai_research_jobs"]
                == 0
            )
            assert (
                after_first["research_backend_invocations"]
                == before["research_backend_invocations"]
                == 0
            )

            fred_before_fixed_point = len(fred_calls)
            sp_before_fixed_point = len(sp_calls)
            _install_write_audit(cfg)
            fixed = client.get(
                "/market-context/mnq?refresh=force&view=debug"
            )
            assert fixed.status_code == 200, fixed.text
            after_fixed = _counts(cfg)

    assert len(sp_calls) == sp_before_fixed_point + 1
    fixed_audits = fixed.json()["data_quality"][
        "actual_reconciliation"
    ]["occurrences"]
    pmi_fixed_audit = next(
        item
        for item in fixed_audits
        if item["occurrence_id"] == PMI_ID
    )
    assert pmi_fixed_audit["provider_call_count"] == 2
    assert pmi_fixed_audit["resolver_invoked"] is True
    assert pmi_fixed_audit["provider_negative_cache_bypassed"] is True
    assert pmi_fixed_audit["reconciliation_outcome"] == "FAIL_CLOSED"
    assert [
        attempt["provider"]
        for attempt in pmi_fixed_audit["provider_attempts"]
    ] == ["SPGLOBAL", "INVESTING_EVENT_1062"]
    assert len(fred_calls) >= fred_before_fixed_point
    assert _write_audit(cfg) == {
        "datum_lifecycle_items": 3,
        "economic_events_history": 1,
        "market_context_snapshots": 1,
    }
    assert (
        after_fixed["market_context_snapshots"]
        == after_first["market_context_snapshots"] + 1
    )
    for table in (
        "market_context_outbox",
        "economic_events_history",
        "datum_lifecycle_items",
        "event_value_candidates",
        "event_calendar_coverage",
        "provider_state",
        "ai_research_jobs",
        "research_backend_invocations",
    ):
        assert after_fixed[table] == after_first[table], table


def test_real_senior_route_emits_request_scoped_accounting_on_two_force_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the production route twice without replacing application services."""

    cfg = _settings(tmp_path)
    fred_calls: list[str] = []
    sp_calls: list[str] = []
    _install_controlled_application(
        monkeypatch,
        cfg,
        fred_calls=fred_calls,
        sp_calls=sp_calls,
        sp_success=True,
    )

    with respx.mock(
        assert_all_mocked=False,
        assert_all_called=False,
    ) as network:
        network.route().respond(404)
        with TestClient(main_module.app) as client:
            _seed_route_state(
                cfg,
                main_module.app.state.event_service,
                seed_stale_lifecycle=True,
            )
            _seed_senior_canonical_facts(cfg)
            now = datetime.now(UTC).replace(microsecond=0)
            MarketNewsRepository(cfg).upsert_news(
                {
                    "title": "Federal Reserve controlled official release",
                    "summary": (
                        "Controlled current-news repository fixture for "
                        "request-scoped accounting."
                    ),
                    "source": "Federal Reserve",
                    "source_url": (
                        "https://www.federalreserve.gov/newsevents/"
                        "pressreleases/test.htm"
                    ),
                    "published_at": (
                        now - timedelta(minutes=1)
                    ).isoformat(),
                    "retrieved_at": now.isoformat(),
                    "valid_until": (
                        now + timedelta(hours=6)
                    ).isoformat(),
                    "next_refresh_at": (
                        now + timedelta(hours=6)
                    ).isoformat(),
                    "topics": ["Federal Reserve", "macro"],
                    "provider_type": "RSS",
                    "is_official": True,
                    "source_verification_status": "VERIFIED",
                    "reliability": 0.9,
                }
            )

            first = client.get(
                "/market-context/mnq"
                "?refresh=force&view=consumer"
                "&audience=senior_analyst_v1"
            )
            assert first.status_code == 200, first.text

            fred_after_first = list(fred_calls)
            sp_after_first = list(sp_calls)
            network_calls_after_first = len(network.calls)
            second = client.get(
                "/market-context/mnq"
                "?refresh=force&view=consumer"
                "&audience=senior_analyst_v1"
            )
            assert second.status_code == 200, second.text
            network_calls_after_second = len(network.calls)
            second_request_urls = [
                str(call.request.url)
                for call in network.calls[
                    network_calls_after_first:
                ]
            ]

    expected = {
        policy.dataset_id
        for policy in DATASET_POLICIES
    }
    assert len(DATASET_POLICIES) == 25
    assert len(expected) == 25
    for response in (first, second):
        payload = response.json()
        rows = payload["provider_accounting"]
        assert {
            row["dataset_id"]
            for row in rows
        } == expected
        assert len(rows) == len(expected)
        assert len(rows) == 25
        assert all(
            row["evidence_status"] == "COMPLETE"
            for row in rows
        )
        missing_database_lookup = [
            row["dataset_id"]
            for row in rows
            if row["database_lookup_performed"] is not True
        ]
        missing_database_lookup_details = [
            (
                row["dataset_id"],
                row.get("reason_code"),
                row.get("evidence_status"),
            )
            for row in rows
            if row["dataset_id"] in missing_database_lookup
        ]
        assert missing_database_lookup == [], (
            "canonical repository lookup missing for "
            f"{missing_database_lookup_details}"
        )
    second_payload = second.json()
    incomplete_rows = [
        (
            row["dataset_id"],
            row.get("reason_code"),
            row.get("database_freshness_evaluation"),
        )
        for row in second_payload["provider_accounting"]
        if row.get("evidence_status") != "COMPLETE"
    ]
    assert incomplete_rows == []
    null_rows = [
        row
        for row in second_payload["provider_accounting"]
        if row["selected_value_present"] is False
    ]
    assert null_rows
    assert all(
        row["delivered_value"] is None
        and row["payload_freshness"] == "UNAVAILABLE"
        and row["reason_code"]
        for row in null_rows
    )
    assert second_payload["request"][
        "same_request_provider_accounting"
    ] is True, incomplete_rows
    validation = validate_senior_analyst_payload_v1(
        second_payload,
        require_recent_response=True,
    )
    assert validation["checks"][
        "provider_accounting_valid"
    ] is True
    missing_one = deepcopy(second_payload)
    missing_one["provider_accounting"] = [
        row
        for row in missing_one["provider_accounting"]
        if row["dataset_id"] != "nasdaq_100"
    ]
    assert len(missing_one["provider_accounting"]) == 24
    missing_validation = validate_senior_analyst_payload_v1(
        missing_one,
        require_recent_response=True,
    )
    assert missing_validation["status"] == "FAIL"
    assert missing_validation["checks"][
        "provider_accounting_valid"
    ] is False

    assert fred_calls == fred_after_first
    assert sp_calls == sp_after_first
    assert (
        network_calls_after_second == network_calls_after_first
    ), second_request_urls


def test_real_senior_route_observes_expired_nasdaq_news_and_earnings_chains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control HTTP only while exercising the production route and services."""

    cfg = _settings(tmp_path)
    cfg.alpha_vantage_api_key = None
    cfg.invesco_qqq_holdings_url = "https://invesco.test/qqq.csv"
    cfg.nasdaq_100_constituents_url = (
        "https://nasdaq.test/constituents"
    )
    cfg.news_gdelt_enabled = True
    cfg.news_rss_enabled = False
    cfg.gdelt_doc_api_url = "https://gdelt.test/api"
    cfg.enable_nasdaq_earnings = True
    cfg.enable_fmp_earnings = True
    cfg.fmp_api_key = "controlled-test-key"
    cfg.nasdaq_earnings_calendar_url = (
        "https://nasdaq.test/earnings"
    )
    cfg.fmp_earnings_calendar_url = "https://fmp.test/earnings"
    fred_calls: list[str] = []
    sp_calls: list[str] = []
    _install_controlled_application(
        monkeypatch,
        cfg,
        fred_calls=fred_calls,
        sp_calls=sp_calls,
        sp_success=True,
    )
    now = datetime.now(UTC).replace(microsecond=0)
    event_date = (now.date() + timedelta(days=2)).isoformat()
    gdelt_seen = (now - timedelta(days=30)).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    with respx.mock(
        assert_all_mocked=False,
        assert_all_called=False,
    ) as network:
        invesco = network.get(
            cfg.invesco_qqq_holdings_url
        ).mock(
            return_value=httpx.Response(403, text="Forbidden")
        )
        nasdaq_holdings = network.get(
            cfg.nasdaq_100_constituents_url
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "rows": [
                            {
                                "symbol": "MSFT",
                                "companyName": "Microsoft",
                                "sector": "Technology",
                            }
                        ]
                    }
                },
            )
        )
        gdelt = network.get(cfg.gdelt_doc_api_url).mock(
            return_value=httpx.Response(
                200,
                json={
                    "articles": [
                        {
                            "id": "gdelt-route-1",
                            "title": (
                                "Microsoft reports a material Nasdaq "
                                "business update"
                            ),
                            "url": (
                                "https://publisher.test/"
                                "gdelt-route-1"
                            ),
                            "seendate": gdelt_seen,
                            "domain": "Reuters",
                            "description": (
                                "Controlled current news for the "
                                "production route."
                            ),
                        }
                    ]
                },
            )
        )
        nasdaq_earnings = network.get(
            cfg.nasdaq_earnings_calendar_url
        ).mock(return_value=httpx.Response(503, text="unavailable"))
        fmp_earnings = network.get(
            cfg.fmp_earnings_calendar_url
        ).mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "symbol": "AAPL",
                        "name": "Apple",
                        "date": event_date,
                        "epsEstimated": 1.25,
                        "revenueEstimated": 100_000,
                    }
                ],
            )
        )
        network.route().respond(404)

        with TestClient(main_module.app) as client:
            _seed_route_state(
                cfg,
                main_module.app.state.event_service,
                seed_stale_lifecycle=True,
            )
            _seed_senior_canonical_facts(cfg)
            expired_at = (
                datetime.now(UTC) - timedelta(minutes=1)
            ).isoformat()
            with sqlite3.connect(cfg.database_path) as connection:
                connection.execute(
                    """
                    UPDATE market_facts
                    SET valid_until=?, next_refresh_at=?
                    WHERE fact_type IN ('qqq_holdings','earnings_event')
                    """,
                    (expired_at, expired_at),
                )
                connection.commit()

            response = client.get(
                "/market-context/mnq"
                "?refresh=force&view=consumer"
                "&audience=senior_analyst_v1"
            )

    assert response.status_code == 200, response.text
    rows = {
        row["dataset_id"]: row
        for row in response.json()["provider_accounting"]
    }

    qqq = rows["nasdaq_100"]
    assert qqq["database_lookup_performed"] is True
    assert qqq["database_record_found"] is True
    assert qqq["database_record_expired"] is True
    assert qqq["database_freshness_evaluation"] != "VALID"
    assert [
        (
            item["provider"],
            item["called"],
            item["execution_origin"],
        )
        for item in [
            qqq["primary_provider"],
            *qqq["fallbacks"],
        ]
    ] == [
        ("INVESCO", True, "PROVIDER_CALL"),
        ("ALPHA_VANTAGE", False, "OBSERVED_SKIP"),
        ("NASDAQ", True, "PROVIDER_CALL"),
        ("SEC", False, "OBSERVED_SKIP"),
    ]
    assert qqq["evidence_status"] == "COMPLETE"

    mega = rows["mega_cap_quotes"]
    assert mega["database_lookup_performed"] is True
    assert mega["database_record_found"] is True
    assert mega["database_record_expired"] is False
    assert all(
        item["called"] is False
        and item["execution_origin"] == "CACHE_DECISION"
        for item in [
            mega["primary_provider"],
            *mega["fallbacks"],
        ]
    )

    news = rows["current_news"]
    assert news["database_lookup_performed"] is True
    assert news["database_record_found"] is False
    news_attempts = [
        news["primary_provider"],
        *news["fallbacks"],
    ]
    assert [
        item["provider"]
        for item in news_attempts
    ] == [
        "ALPHA_VANTAGE_NEWS_SENTIMENT",
        "GDELT_DOC_API",
        "FEDERAL_RESERVE_RSS",
        "BLS_RSS",
        "BEA_RSS",
        "YAHOO_FINANCE_RSS",
        "MARKETWATCH_RSS",
        "GOOGLE_NEWS_RSS",
    ]
    assert news_attempts[0]["called"] is False
    assert news_attempts[1]["called"] is True
    assert all(
        item["called"] is False
        for item in news_attempts[2:]
    )
    assert news["evidence_status"] == "COMPLETE"
    assert news["acquisition_selected_source"] is None
    assert (
        news["acquisition_reason_code"]
        == "NEWS_FAN_IN_COMPLETED_NO_CURRENT_DATA"
    )
    assert news["selected_value_present"] is False
    assert news["delivered_value"] is None
    assert news["payload_freshness"] == "UNAVAILABLE"
    assert news["delivery_missing_reason_codes"] == [
        "NO_CURRENT_NEWS"
    ]

    earnings = rows["earnings"]
    assert earnings["database_lookup_performed"] is True
    assert earnings["database_record_found"] is True
    assert earnings["database_record_expired"] is True
    assert earnings["primary_provider"]["called"] is True
    assert earnings["fallbacks"][0]["called"] is True
    assert (
        earnings["fallbacks"][0]["provider"]
        == "FMP_EARNINGS_CALENDAR"
    )
    assert earnings["acquisition_selected_source"] == (
        "Financial Modeling Prep Earnings Calendar"
    )
    assert earnings["selected_value_present"] is False
    assert earnings["delivered_value"] is None
    assert earnings["reason_code"] == "NO_CURRENT_EARNINGS_EVENTS"
    assert earnings["delivery_missing_reason_codes"] == [
        "NO_CURRENT_EARNINGS_EVENTS"
    ]
    assert earnings["evidence_status"] == "COMPLETE"

    assert invesco.call_count == 1
    assert nasdaq_holdings.call_count == 1
    assert gdelt.call_count == 1
    assert nasdaq_earnings.call_count == 14
    assert fmp_earnings.call_count == 1


@pytest.mark.parametrize(
    "lifecycle_state",
    ("FRESH_NO_DATA", "EXHAUSTED_NO_DATA"),
)
def test_real_route_skips_non_due_no_data_states_at_fixed_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lifecycle_state: str,
) -> None:
    cfg = _settings(tmp_path)
    fred_calls: list[str] = []
    sp_calls: list[str] = []
    _install_controlled_application(
        monkeypatch,
        cfg,
        fred_calls=fred_calls,
        sp_calls=sp_calls,
    )
    with respx.mock(assert_all_mocked=False) as network:
        network.route().respond(404)
        with TestClient(main_module.app) as client:
            _seed_route_state(
                cfg,
                main_module.app.state.event_service,
            )
            _baseline_missing_actual_snapshot(client, cfg)
            _seed_senior_canonical_facts(cfg)
            expected_retry = _seed_no_data_state(
                cfg,
                lifecycle_state,
            )
            fred_calls.clear()
            sp_calls.clear()
            before = _counts(cfg)
            _install_write_audit(cfg)

            response = client.get(
                "/market-context/mnq?refresh=force&view=debug"
            )
            after = _counts(cfg)

    assert response.status_code == 200, response.text
    assert fred_calls.count("HSN1F") == 0
    assert sp_calls == []
    assert _write_audit(cfg) == {}
    assert after == before
    with sqlite3.connect(cfg.database_path) as connection:
        for occurrence_id in (HOME_ID, PMI_ID):
            row = connection.execute(
                """
                SELECT freshness_state,next_retry_at
                FROM datum_lifecycle_items
                WHERE entity_type='macro_actual' AND entity_key=?
                """,
                (occurrence_id,),
            ).fetchone()
            assert row is not None
            assert row[1] == expected_retry[occurrence_id]
            if lifecycle_state == "EXHAUSTED_NO_DATA":
                assert row[0] == "EXHAUSTED_NO_DATA"
        telemetry = [
            json.loads(row[0])["payload"]
            for row in connection.execute(
                """
                SELECT payload_json
                FROM service_telemetry_events
                WHERE event_name='provider_force_actual_reconciliation'
                ORDER BY rowid DESC LIMIT 2
                """
            )
        ]
    expected_eligibility = {
        "FRESH_NO_DATA": "FRESH_NO_DATA",
        "EXHAUSTED_NO_DATA": "EXHAUSTED_NO_DATA",
    }[lifecycle_state]
    assert len(telemetry) == 2
    assert {
        item["eligibility"] for item in telemetry
    } == {expected_eligibility}
    assert all(
        item["resolver_invoked"] is False
        and item["provider_attempted"] is False
        and item["finalization_status"] == "NO_OP"
        for item in telemetry
    )


def test_two_concurrent_force_routes_serialize_writes_and_retry_prior_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _settings(tmp_path)
    fred_calls: list[str] = []
    sp_calls: list[str] = []
    _install_controlled_application(
        monkeypatch,
        cfg,
        fred_calls=fred_calls,
        sp_calls=sp_calls,
    )
    with respx.mock(assert_all_mocked=False) as network:
        network.route().respond(404)
        with TestClient(main_module.app) as client:
            _seed_route_state(
                cfg,
                main_module.app.state.event_service,
                seed_stale_lifecycle=True,
            )
            resolver_calls: list[str] = []
            resolver = main_module.app.state.lifecycle_due_resolver
            original_resolve = resolver.resolve

            def counted_resolve(
                item: dict[str, Any],
            ) -> dict[str, Any]:
                if (
                    item.get("resolution_mode")
                    == "prepare_atomic_provider_force"
                ):
                    resolver_calls.append(str(item["entity_key"]))
                return original_resolve(item)

            monkeypatch.setattr(
                resolver,
                "resolve",
                counted_resolve,
            )
            with sqlite3.connect(cfg.database_path) as connection:
                connection.execute(
                    "DELETE FROM event_calendar_coverage"
                )
                connection.commit()
            before = _counts(cfg)
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(
                        client.get,
                        "/market-context/mnq?refresh=force&view=debug",
                    )
                    for _ in range(2)
                ]
                responses = [future.result() for future in futures]
            after = _counts(cfg)

    assert all(response.status_code == 200 for response in responses)
    assert resolver_calls.count(HOME_ID) == 1
    # The first request resolves HOME permanently. PMI remains unresolved and
    # writes a request-scoped negative-cache decision; the second, distinct
    # force request must bypass that prior-request backoff exactly once.
    assert resolver_calls.count(PMI_ID) == 2
    assert fred_calls.count("HSN1F") >= 1
    assert len(sp_calls) == 2
    assert (
        after["market_context_snapshots"]
        - before["market_context_snapshots"]
        == 2
    )
    assert (
        after["market_context_outbox"]
        - before["market_context_outbox"]
        <= 2
    )
    with sqlite3.connect(cfg.database_path) as connection:
        assert connection.execute(
            """
            SELECT COUNT(*) FROM datum_lifecycle_items
            WHERE entity_type='macro_actual'
              AND entity_key IN (?,?)
            """,
            (HOME_ID, PMI_ID),
        ).fetchone()[0] == 2
        assert connection.execute(
            """
            SELECT COUNT(DISTINCT coverage_date)
            FROM event_calendar_coverage
            """
        ).fetchone()[0] == 21
        assert connection.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT coverage_date,data_domain,entity_type,
                     provider_name,query_scope,symbol,
                     contract_version,policy_version,COUNT(*) AS copies
              FROM event_calendar_coverage
              GROUP BY coverage_date,data_domain,entity_type,
                       provider_name,query_scope,symbol,
                       contract_version,policy_version
              HAVING copies>1
            )
            """
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    "failure_point",
    ("AFTER_COVERAGE", "BEFORE_FINALIZATION"),
)
def test_failed_force_route_publishes_no_partial_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    cfg = _settings(tmp_path)
    fred_calls: list[str] = []
    sp_calls: list[str] = []
    _install_controlled_application(
        monkeypatch,
        cfg,
        fred_calls=fred_calls,
        sp_calls=sp_calls,
    )
    with respx.mock(assert_all_mocked=False) as network:
        network.route().respond(404)
        with TestClient(
            main_module.app,
            raise_server_exceptions=False,
        ) as client:
            _seed_route_state(
                cfg,
                main_module.app.state.event_service,
                seed_stale_lifecycle=True,
            )
            _baseline_missing_actual_snapshot(client, cfg)
            with sqlite3.connect(cfg.database_path) as connection:
                connection.execute(
                    "DELETE FROM event_calendar_coverage"
                )
                connection.commit()
            before = _canonical_state(cfg)
            if failure_point == "AFTER_COVERAGE":
                def fail_after_coverage(
                    _service: Any,
                    _contract: dict[str, Any],
                ) -> dict[str, Any]:
                    raise RuntimeError(
                        "injected_after_coverage_before_reconciliation"
                    )

                monkeypatch.setattr(
                    "app.api.routes."
                    "ProviderForceActualReconciliationService.prepare",
                    fail_after_coverage,
                )
            else:
                def fail_before_finalization(
                    _repository: Any,
                    **_kwargs: Any,
                ) -> dict[str, Any]:
                    raise RuntimeError(
                        "injected_after_reconciliation_before_finalization"
                    )

                monkeypatch.setattr(
                    "app.api.routes."
                    "MarketContextSnapshotRepository.save_next",
                    fail_before_finalization,
                )

            response = client.get(
                "/market-context/mnq?refresh=force&view=debug"
            )
            after = _canonical_state(cfg)

    assert response.status_code == 500
    assert after == before
    assert not list(
        cfg.database_path.parent.glob(
            f".{cfg.database_path.stem}.force-stage-*.sqlite*"
        )
    )
    with sqlite3.connect(cfg.database_path) as connection:
        telemetry_row = connection.execute(
            """
            SELECT payload_json
            FROM service_telemetry_events
            WHERE event_name='provider_force_generation'
            ORDER BY rowid DESC LIMIT 1
            """
        ).fetchone()
    assert telemetry_row is not None
    aborted = json.loads(telemetry_row[0])["payload"]
    assert aborted["finalization_status"] == "ABORTED"
    assert aborted["canonical_write_count"] == 0
    assert aborted["lifecycle_write_count"] == 0
    assert aborted["coverage_write_count"] == 0
    assert aborted["snapshot_write_count"] == 0
