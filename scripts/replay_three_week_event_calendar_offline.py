from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import Settings
from app.services.ai_trader_consumer_v2_service import (
    build_ai_trader_consumer_v2,
)


GENERATED_AT = "2026-07-22T15:00:00+00:00"


def replay(*, workspace: Path | None = None) -> dict[str, Any]:
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if workspace is None:
        temporary = tempfile.TemporaryDirectory(
            prefix="three-week-calendar-replay-"
        )
        workspace = Path(temporary.name)
    workspace.mkdir(parents=True, exist_ok=True)
    settings = Settings(
        _env_file=None,
        environment="test",
        database_path=workspace / "offline-replay.sqlite",
        event_calendar_timezone="America/New_York",
        event_calendar_consumer_max_events=90,
        enable_scheduler=False,
        research_scheduler_enabled=False,
        lifecycle_due_scanner_enabled=False,
        event_calendar_catchup_enabled=False,
    )
    payload = _fixture()
    first = build_ai_trader_consumer_v2(payload, settings=settings)
    second = build_ai_trader_consumer_v2(payload, settings=settings)
    encoded = _canonical(first)
    second_encoded = _canonical(second)
    window = first["event_calendar_window"]
    result = {
        "mode": "offline_three_week_event_calendar_replay",
        "schema_version": first["schema_version"],
        "timezone": window["timezone"],
        "window_start": window["window_start"],
        "window_end": window["window_end"],
        "bucket_counts": window["counts"]["by_bucket"],
        "status_counts": window["counts"]["by_status"],
        "coverage": window["coverage"],
        "consumer_payload_bytes": len(encoded),
        "consumer_payload_sha256": hashlib.sha256(encoded).hexdigest(),
        "idempotent": encoded == second_encoded,
        "provider_calls": 0,
        "ai_invocations": 0,
        "browser_calls": 0,
        "delivery_attempts": 0,
        "trading_calls": 0,
        "under_90kb": len(encoded) < 90_000,
        "cash_session_status": first["market_schedule"][
            "nasdaq_cash_session"
        ]["status"],
        "mnq_futures_session_status": first["market_schedule"][
            "mnq_futures_session"
        ]["status"],
    }
    if temporary is not None:
        temporary.cleanup()
    return result


def _fixture() -> dict[str, Any]:
    return {
        "symbol": "MNQ",
        "generated_at_utc": GENERATED_AT,
        "data_as_of": GENERATED_AT,
        "event_calendar": {
            "critical_macro_events": [
                _event(
                    "bls:cpi:2026-07-14",
                    "Consumer Price Index",
                    "2026-07-14T08:30:00-04:00",
                    actual="2.7",
                    forecast="2.6",
                    previous="2.5",
                ),
                _event(
                    "census:retail:2026-07-21",
                    "Advance Retail Sales",
                    "2026-07-21T08:30:00-04:00",
                    actual="0.4",
                    forecast="0.3",
                    previous="0.2",
                ),
                _event(
                    "bea:gdp:2026-07-23",
                    "Gross Domestic Product",
                    "2026-07-23T08:30:00-04:00",
                    forecast="2.1",
                    previous="2.0",
                ),
                _event(
                    "bls:nfp:2026-07-31",
                    "Employment Situation",
                    "2026-07-31T08:30:00-04:00",
                    forecast="175000",
                    previous="160000",
                ),
            ],
            "fed_communications": [
                _event(
                    "fed:speech:2026-07-24",
                    "Scheduled Federal Reserve remarks",
                    "2026-07-24T13:00:00-04:00",
                    category="FED_SPEECH",
                )
            ],
            "other_economic_events": [],
        },
        "event_windows": {
            "upcoming_unscheduled": [
                {
                    "event_id": "breaking-news-not-a-future-event",
                    "event_kind": "unscheduled_news",
                    "impact": "HIGH",
                }
            ]
        },
        "nasdaq_context": {
            "earnings": {
                "upcoming": [
                    {
                        "issuer_event_id": "earnings:nvda:2026-07-28",
                        "issuer_name": "NVIDIA",
                        "symbol": "NVDA",
                        "date": "2026-07-28",
                        "impact": "HIGH",
                        "eps_estimate": "1.25",
                        "eps_actual": None,
                        "source": "NASDAQ",
                    }
                ]
            }
        },
        "market_schedule": {},
        "macro_snapshot": {},
        "news_context": {},
        "risk_context": {},
        "research": {"status": "NOT_REQUIRED"},
        "ai_enrichment": {"status": "NOT_REQUIRED"},
    }


def _event(
    occurrence_id: str,
    title: str,
    release_at: str,
    *,
    actual: Any = None,
    forecast: Any = None,
    previous: Any = None,
    category: str = "MACRO",
) -> dict[str, Any]:
    return {
        "event_id": occurrence_id,
        "occurrence_id": occurrence_id,
        "name": title,
        "country": "US",
        "currency": "USD",
        "category": category,
        "impact": "HIGH",
        "release_at": release_at,
        "actual": actual,
        "forecast": forecast,
        "previous": previous,
        "source": "official offline fixture",
    }


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


if __name__ == "__main__":
    print(
        json.dumps(
            replay(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
