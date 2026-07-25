from __future__ import annotations

import json
from datetime import UTC, datetime

from app.services.deterministic_market_context_service import (
    ProviderFirstResolutionPlanner,
    compute_cross_asset_context,
    compute_market_internals,
    compute_options_positioning,
    deterministic_anomalies,
)


NOW = datetime(2026, 7, 24, 15, tzinfo=UTC)


def main() -> int:
    option_contracts = [
        {
            "option_type": "call",
            "strike": 500,
            "volume": 20,
            "open_interest": 100,
            "contract_size": 100,
            "iv": 0.2,
            "delta": 0.5,
            "gamma": 0.01,
            "theta": -0.1,
            "vega": 0.2,
            "rho": 0.01,
            "bid": 2,
            "ask": 2.2,
        },
        {
            "option_type": "put",
            "strike": 500,
            "volume": 10,
            "open_interest": 80,
            "contract_size": 100,
            "iv": 0.22,
            "delta": -0.5,
            "gamma": 0.01,
            "theta": -0.1,
            "vega": 0.2,
            "rho": -0.01,
            "bid": 1.9,
            "ask": 2.1,
        },
    ]
    options = compute_options_positioning(
        quote={
            "last": 500,
            "observed_at": NOW.isoformat(),
            "environment": "fixture",
            "freshness_state": "FRESH",
        },
        chains={"2026-07-24": option_contracts},
        retrieved_at=NOW,
    )
    internals = compute_market_internals(
        constituents=["AAPL", "MSFT"],
        holdings=[
            {"symbol": "AAPL", "weight_pct": 55},
            {"symbol": "MSFT", "weight_pct": 45},
        ],
        quotes=[
            {"symbol": "AAPL", "last": 102, "close": 100, "volume": 100},
            {"symbol": "MSFT", "last": 99, "close": 100, "volume": 200},
        ],
    )
    cross_asset = compute_cross_asset_context(
        quotes=[
            {"symbol": "QQQ", "change_percentage": 1},
            {"symbol": "SPY", "change_percentage": 0.5},
            {"symbol": "HYG", "change_percentage": 0.2},
            {"symbol": "LQD", "change_percentage": 0.1},
            {"symbol": "TLT", "change_percentage": -0.2},
            {"symbol": "UUP", "change_percentage": -0.1},
        ],
        fred_series={
            "DGS2": {"value": 4, "freshness": "FRESH"},
            "DGS10": {"value": 4.5, "freshness": "FRESH"},
        },
    )
    planner = ProviderFirstResolutionPlanner().plan(
        requested_fields=["eps_actual", "official_guidance_summary"],
        provider_values={"eps_actual": 0},
        agent_enabled=False,
    )
    aggregate = {
        "options_positioning": options,
        "market_internals": internals,
        "cross_asset_context": cross_asset,
        "provider_first": planner,
    }
    checks = {
        "options_available": options["status"] == "AVAILABLE",
        "gamma_is_proxy": options["gamma_proxy"]["dealer_positioning_confirmed"] is False,
        "market_internals_available": internals["status"] == "AVAILABLE",
        "cross_asset_available": cross_asset["status"] == "AVAILABLE",
        "numeric_zero_resolved": planner["fields_resolved_by_provider"] == ["eps_actual"],
        "ai_not_invoked": planner["actual_ai_invocations"] == 0,
        "no_anomalies": deterministic_anomalies(aggregate) == [],
        "consumer_projection_compact": len(
            json.dumps(aggregate, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        < 90_000,
    }
    print(
        json.dumps(
            {
                "status": "PASSED" if all(checks.values()) else "FAILED",
                "mode": "offline_fixture_replay",
                "network_calls": 0,
                "ai_calls": 0,
                "checks": checks,
            },
            sort_keys=True,
        )
    )
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
