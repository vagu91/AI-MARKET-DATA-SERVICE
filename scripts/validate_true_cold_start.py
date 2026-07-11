from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from fastapi.testclient import TestClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate true cold start using an isolated SQLite database.")
    parser.add_argument("--artifact-dir", type=Path, help="Directory for JSON report artifacts.")
    parser.add_argument("--skip-force", action="store_true", help="Use refresh=false only. Useful for offline diagnostics.")
    args = parser.parse_args()

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    artifact_dir = args.artifact_dir or Path("data/diagnostics") / f"true_cold_start_{timestamp}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="ai_market_cold_start_", ignore_cleanup_errors=True) as tmp:
        db_path = Path(tmp) / "market.sqlite"
        os.environ["AI_MARKET_DATABASE_PATH"] = str(db_path)
        os.environ["AI_MARKET_ENABLE_SCHEDULER"] = "false"
        os.environ["AI_MARKET_TIMEOUT_MACRO_SECONDS"] = "8"
        os.environ["AI_MARKET_TIMEOUT_EVENTS_SECONDS"] = "5"
        os.environ["AI_MARKET_TIMEOUT_NEWS_SECONDS"] = "5"
        os.environ["AI_MARKET_TIMEOUT_NASDAQ_SECONDS"] = "8"
        os.environ["AI_MARKET_TIMEOUT_EARNINGS_SECONDS"] = "5"
        os.environ["AI_MARKET_TIMEOUT_COT_SECONDS"] = "4"
        os.environ["AI_MARKET_TIMEOUT_SENTIMENT_SECONDS"] = "4"
        os.environ["AI_MARKET_MARKETBEAT_TIMEOUT_SECONDS"] = "4"
        os.environ["AI_MARKET_INVESTING_FED_RATE_MONITOR_TIMEOUT_SECONDS"] = "4"
        os.environ["AI_MARKET_SOCIAL_SENTIMENT_TIMEOUT_SECONDS"] = "4"
        os.environ["AI_MARKET_NASDAQ_OPTIONS_MAX_PAGES"] = "1"
        from app.core.config import get_settings

        get_settings.cache_clear()
        from app.main import app
        from app.services.ai_trader_contract_service import build_ai_trader_market_context

        with TestClient(app) as client:
            debug_force = {} if args.skip_force else client.get("/market-context/mnq/debug?refresh=force").json()
            debug_cache = client.get("/market-context/mnq/debug?refresh=false").json()
            force_payload = {} if args.skip_force else build_ai_trader_market_context(debug_force)
            cache_payload = build_ai_trader_market_context(debug_cache)
            acquisition_force = client.get("/diagnostics/acquisition-status").json()
            acquisition_cache = client.get("/diagnostics/acquisition-status").json()
            db_health = client.get("/db/health").json()
            db_health_details = client.get("/db/health/details").json()
            db_schema_version = client.get("/db/schema-version").json()
            db_cache_stats = client.get("/db/cache/stats").json()

    _write(artifact_dir / "consumer_force.json", force_payload)
    _write(artifact_dir / "debug_force.json", debug_force)
    _write(artifact_dir / "consumer_cache.json", cache_payload)
    _write(artifact_dir / "debug_cache.json", debug_cache)
    _write(artifact_dir / "acquisition_status_force.json", acquisition_force)
    _write(artifact_dir / "acquisition_status_cache.json", acquisition_cache)
    _write(artifact_dir / "db_health.json", db_health)
    _write(artifact_dir / "db_health_details.json", db_health_details)
    _write(artifact_dir / "db_schema_version.json", db_schema_version)
    _write(artifact_dir / "db_cache_stats.json", db_cache_stats)
    refresh_false_calls = _runtime_provider_calls(debug_cache) + int(((debug_cache.get("social_sentiment") or {}).get("provider_calls") or 0))
    payload_diff = {
        "consumer_force_keys": sorted(force_payload.keys()) if force_payload else [],
        "consumer_cache_keys": sorted(cache_payload.keys()),
        "debug_force_keys": sorted(debug_force.keys()) if debug_force else [],
        "debug_cache_keys": sorted(debug_cache.keys()),
        "refresh_false_network_calls": refresh_false_calls,
    }
    validation = {
        "artifact_dir": str(artifact_dir),
        "isolated_db_used": True,
        "database_path": db_health.get("database_path"),
        "single_physical_database": db_health.get("single_physical_database") is True,
        "schema_version": db_schema_version.get("schema", {}).get("schema_version"),
        "refresh_false_network_calls": refresh_false_calls,
        "qqq_path": ((debug_force.get("nasdaq_context") or {}).get("qqq_holdings") or {}).get("source") if debug_force else None,
        "qqq_proxy_used": bool(((debug_force.get("nasdaq_context") or {}).get("qqq_holdings") or {}).get("is_proxy")) if debug_force else None,
        "no_weights_invented": not bool(((debug_force.get("nasdaq_context") or {}).get("qqq_holdings") or {}).get("weight_data_available") is True and ((debug_force.get("nasdaq_context") or {}).get("qqq_holdings") or {}).get("is_proxy")) if debug_force else True,
        "lkg_absent_at_start": True,
        "passed": refresh_false_calls == 0 and db_health.get("single_physical_database") is True,
    }
    _write(artifact_dir / "payload_diff.json", payload_diff)
    _write(artifact_dir / "final_validation.json", validation)
    _write(artifact_dir / "final_summary.json", {"passed": validation["passed"], "artifact_dir": str(artifact_dir)})
    print(json.dumps(validation, indent=2, sort_keys=True))
    return 0 if validation["passed"] else 1


def _runtime_provider_calls(payload: dict[str, Any]) -> int:
    runtime = ((payload.get("metadata") or {}).get("multi_source_runtime") or {})
    return int(runtime.get("provider_calls") or 0)


def _write(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
