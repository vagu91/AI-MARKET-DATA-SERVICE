from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from app.bootstrap.application import build_application_state
from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.providers.census import CensusProvider
from app.providers.finnhub import FinnhubProvider
from app.providers.tradier import TradierProvider
from app.services.deterministic_provider_runtime_service import (
    DeterministicProviderRuntimeService,
)
from app.services.event_driven_lifecycle_service import compute_datum_lifecycle
from app.services.market_context_outbox_service import MarketContextOutboxRepository
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)


NOW = datetime(2026, 7, 25, 14, 0, tzinfo=UTC)


def replay(
    database_path: Path,
    *,
    fixture_path: Path | None = None,
) -> dict[str, Any]:
    calls = {"total": 0, "by_endpoint": {}}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls["total"] += 1
        calls["by_endpoint"][path] = calls["by_endpoint"].get(path, 0) + 1
        if path.endswith("/markets/quotes"):
            symbols = str(request.url.params.get("symbols") or "").split(",")
            return httpx.Response(
                200,
                json={
                    "quotes": {
                        "quote": [
                            {
                                "symbol": symbol,
                                "last": 100 + index,
                                "prevclose": 99 + index,
                                "bid": 99.9 + index,
                                "ask": 100.1 + index,
                                "volume": 1_000_000 + index,
                                "trade_date": int(NOW.timestamp()),
                            }
                            for index, symbol in enumerate(symbols)
                        ]
                    }
                },
            )
        if path.endswith("/markets/options/expirations"):
            return httpx.Response(
                200,
                json={"expirations": {"date": ["2026-07-27", "2026-08-03", "2026-08-24"]}},
            )
        if path.endswith("/markets/options/chains"):
            expiration = request.url.params["expiration"]
            return httpx.Response(
                200,
                json={
                    "options": {
                        "option": [
                            _option(expiration, "call", 100, 120, 200, 0.20),
                            _option(expiration, "put", 100, 150, 240, 0.24),
                        ]
                    }
                },
            )
        if path.endswith("/calendar/earnings"):
            return httpx.Response(
                200,
                json={
                    "earningsCalendar": [
                        {
                            "symbol": "NVDA",
                            "date": "2026-07-30",
                            "hour": "amc",
                            "epsEstimate": 1.2,
                            "revenueEstimate": 45_000,
                        }
                    ]
                },
            )
        if path.endswith("/company-news"):
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 7,
                        "datetime": int(NOW.timestamp()),
                        "headline": "Candidate awaiting independent verification",
                        "summary": "Discovery-only candidate.",
                        "source": "Example Wire",
                        "url": "https://example.org/nvda-candidate",
                    }
                ],
            )
        if "/data/timeseries/eits/marts" in path:
            return httpx.Response(
                200,
                json=[
                    [
                        "time",
                        "seasonally_adj",
                        "data_type_code",
                        "cell_value",
                        "category_code",
                    ],
                    ["2026-06", "SA", "SM", "735000", "44X72"],
                ],
            )
        raise AssertionError(f"unexpected mocked endpoint: {request.url}")

    settings = Settings(
        _env_file=None,
        environment="test",
        database_path=database_path,
        fred_api_key="configured-for-offline-replay",
        bea_api_key="configured-for-offline-replay",
        census_api_key="configured-for-offline-replay",
        finnhub_api_key="configured-for-offline-replay",
        tradier_enabled=True,
        tradier_environment="sandbox",
        tradier_sandbox_token="configured-for-offline-replay",
        enable_ai_researcher=False,
    )
    state = build_application_state(settings)
    transport = httpx.MockTransport(handler)
    cache = state["cache"]
    providers = dict(state["deterministic_providers"])
    providers.update(
        {
            "census": CensusProvider(cache, settings, transport=transport),
            "finnhub": FinnhubProvider(
                cache,
                settings,
                transport=transport,
                clock=lambda: NOW,
            ),
            "tradier": TradierProvider(
                cache,
                settings,
                transport=transport,
                clock=lambda: NOW,
            ),
        }
    )
    runtime = DeterministicProviderRuntimeService(
        settings,
        providers=providers,
        cache=cache,
        clock=lambda: NOW,
    )
    contract = _contract()
    first = asyncio.run(
        runtime.enrich_market_context(contract, refresh="auto")
    )
    first_network_calls = calls["total"]
    second = asyncio.run(
        runtime.enrich_market_context(contract, refresh="auto")
    )
    second_network_calls = calls["total"] - first_network_calls

    datum = first["macro_actuals"]["items"][-1]
    lifecycle = compute_datum_lifecycle(
        "macro_actual",
        str(datum["occurrence_id"]),
        datum,
        settings=settings,
        now=NOW,
        triggering_event="macro_actual",
    )
    snapshot = MarketContextSnapshotRepository(settings).save_next(
        symbol="MNQ",
        refresh_mode="deterministic_offline_replay",
        debug_payload=first,
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
        trigger_type="macro_actual",
        trigger_entity=str(datum["occurrence_id"]),
        correlation_id="deterministic-offline-replay",
        resolved_lifecycle=lifecycle,
        resolved_datum=datum,
    )
    consumer = snapshot["consumer_payload"]
    redacted_fixture = _redact_fixture_ids(consumer)
    encoded = json.dumps(
        redacted_fixture,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    if fixture_path is not None:
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_bytes(encoded)
    outbox = MarketContextOutboxRepository(settings).list_events(
        status="PENDING",
        limit=10,
    )
    with connect_sqlite(settings.database_path) as conn:
        persistence_counts = {
            "snapshots": conn.execute(
                "SELECT COUNT(*) FROM market_context_snapshots"
            ).fetchone()[0],
            "components": conn.execute(
                "SELECT COUNT(*) FROM market_context_components"
            ).fetchone()[0],
            "lifecycle_rows": conn.execute(
                "SELECT COUNT(*) FROM datum_lifecycle_items"
            ).fetchone()[0],
            "outbox": conn.execute(
                "SELECT COUNT(*) FROM market_context_outbox"
            ).fetchone()[0],
        }
    return {
        "consumer": consumer,
        "snapshot_id": snapshot["snapshot_id"],
        "outbox_count": len(outbox),
        "first_network_calls": first_network_calls,
        "second_network_calls": second_network_calls,
        "first_cache_hits": first["deterministic_domains"]["telemetry"]["cache_hits"],
        "second_cache_hits": second["deterministic_domains"]["telemetry"]["cache_hits"],
        "ai_invocations": second["deterministic_domains"]["telemetry"]["ai_invocations"],
        "fixture_size_bytes": len(encoded),
        "fixture_sha256": hashlib.sha256(encoded).hexdigest(),
        "calls_by_endpoint": calls["by_endpoint"],
        "persistence_counts": persistence_counts,
    }


def _redact_fixture_ids(value: Any) -> Any:
    output = copy.deepcopy(value)

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if "snapshot_id" in str(key).lower() and child is not None:
                    item[key] = "<redacted-snapshot-id>"
                else:
                    walk(child)
        elif isinstance(item, list):
            for child in item:
                walk(child)

    walk(output)
    return output


def _contract() -> dict[str, Any]:
    return {
        "symbol": "MNQ",
        "generated_at_utc": NOW.isoformat(),
        "data_as_of": NOW.isoformat(),
        "macro_snapshot": {
            "rates_and_yields": {
                "DGS2": _series("DGS2", 4.1, "FRED"),
                "DGS10": _series("DGS10", 4.5, "FRED"),
                "SOFR": _series("SOFR", 4.35, "FRED"),
            },
            "labor": {
                "LNS14000000": _series("LNS14000000", 4.2, "BLS"),
            },
            "growth": {
                "BEA:GDP": _series("BEA:GDP", 2.8, "BEA"),
            },
        },
        "event_calendar": {
            "critical_macro_events": [
                {
                    "event_id": "retail-sales-2026-06",
                    "name": "Advance Retail Sales",
                    "official_provider": "CENSUS",
                    "dataset": "MARTS",
                    "reference_period": "2026-06",
                    "actual": 735000,
                    "temporal_status": "RELEASED",
                    "source": "CENSUS",
                }
            ],
            "fed_communications": [],
            "other_economic_events": [],
        },
        "events_today": [],
        "event_windows": {"active": [], "upcoming": []},
        "nasdaq_context": {
            "status": "available",
            "qqq_holdings": {
                "status": "found",
                "holdings_count": 3,
                "holdings": [
                    {"symbol": "NVDA", "weight_pct": 9.0},
                    {"symbol": "MSFT", "weight_pct": 8.0},
                    {"symbol": "AAPL", "weight_pct": 7.0},
                ],
            },
            "earnings": {"upcoming": [{"symbol": "NVDA", "date": "2026-07-30"}]},
        },
        "news_context": {
            "status": "AVAILABLE",
            "current_drivers": [
                {
                    "article_id": "verified-1",
                    "headline": "Verified material issuer update",
                    "source": "NVIDIA Investor Relations",
                    "source_url": "https://investor.nvidia.com/news/verified-update",
                    "verification_status": "VERIFIED",
                    "materiality_status": "MATERIAL",
                    "published_at": "2026-07-25T12:00:00+00:00",
                }
            ],
        },
        "market_schedule": {"holidays": []},
        "risk_context": {},
        "rates_expectations": {},
        "positioning": {},
        "sentiment_context": {},
        "data_quality": {
            "pipeline_integrity": {"snapshot_built_from_db": True},
            "section_quality": {},
        },
        "metadata": {"request_refresh_mode": "auto"},
    }


def _series(series_id: str, value: float, source: str) -> dict[str, Any]:
    return {
        "series_id": series_id,
        "value": value,
        "units": "percent",
        "data_as_of": "2026-07-24",
        "frequency": "daily",
        "freshness": "FRESH",
        "source": source,
        "source_url": (
            f"https://fred.stlouisfed.org/series/{series_id}"
            if source == "FRED"
            else "https://www.bls.gov/developers/"
            if source == "BLS"
            else "https://www.bea.gov/data"
        ),
        "official_adapter": True,
    }


def _option(
    expiration: str,
    option_type: str,
    strike: float,
    volume: int,
    open_interest: int,
    iv: float,
) -> dict[str, Any]:
    return {
        "symbol": f"QQQ{expiration.replace('-', '')}{option_type[0].upper()}{strike}",
        "expiration_date": expiration,
        "strike": strike,
        "option_type": option_type,
        "bid": 2.0,
        "ask": 2.2,
        "volume": volume,
        "open_interest": open_interest,
        "contract_size": 100,
        "greeks": {
            "mid_iv": iv,
            "delta": 0.5 if option_type == "call" else -0.5,
            "gamma": 0.02,
            "theta": -0.03,
            "vega": 0.1,
            "rho": 0.01,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database")
    parser.add_argument("--write-fixture")
    args = parser.parse_args()
    if args.database:
        database = Path(args.database).resolve()
        result = replay(
            database,
            fixture_path=(
                Path(args.write_fixture).resolve()
                if args.write_fixture
                else None
            ),
        )
    else:
        with tempfile.TemporaryDirectory(
            prefix="deterministic-runtime-replay-",
            ignore_cleanup_errors=True,
        ) as directory:
            result = replay(
                Path(directory) / "replay.db",
                fixture_path=(
                    Path(args.write_fixture).resolve()
                    if args.write_fixture
                    else None
                ),
            )
    print(
        json.dumps(
            {key: value for key, value in result.items() if key != "consumer"},
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
