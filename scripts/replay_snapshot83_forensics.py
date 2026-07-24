from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.providers.cboe_put_call_provider import normalize_cboe_put_call
from app.providers.cboe_vix_futures_provider import parse_vix_futures_csv
from app.providers.cftc_cot_provider import parse_cftc_financial_row
from app.services.ai_trader_consumer_v2_service import (
    build_ai_trader_consumer_v2,
)
from app.services.research_semantics import (
    normalize_research_claim,
    semantic_validation_warnings,
)
from app.services.source_policy_service import SourcePolicyService


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = ROOT / "tests" / "fixtures" / "snapshot_83_forensic_redacted.json"


def replay(
    fixture_path: Path = DEFAULT_FIXTURE,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Replay snapshot 83 entirely in-process without network or backend calls."""

    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    source = fixture["source"]
    now = datetime.fromisoformat(source["generated_at"]).astimezone(UTC)
    settings = settings or Settings(
        _env_file=None,
        source_policy_path=ROOT / "config" / "source_policy.json",
        model_pricing_path=ROOT / "config" / "model_pricing.json",
    )
    events = list(fixture["macro_events"])
    full = {
        "snapshot_id": source["snapshot_id"],
        "snapshot_revision": source["revision"],
        "symbol": "MNQ",
        "generated_at_utc": now.isoformat(),
        "data_as_of": now.isoformat(),
        "event_calendar": {
            "critical_macro_events": events,
            "fed_communications": [],
            "other_economic_events": [],
        },
        "events_today": events,
        "nasdaq_context": {
            "status": "AVAILABLE",
            "qqq_holdings": fixture["nasdaq_invalid"],
            "concentration": {"top_10_weight_pct": 99},
            "sector_exposure": {"technology": 99},
            "semiconductor_context": {"weight_pct": 99},
        },
        "research": {
            "status": "PARTIAL",
            "execution_complete": True,
            "coverage_complete": False,
            "disabled_optional_topics": [],
        },
        "ai_enrichment": {"status": "NOT_REQUIRED"},
        "readiness": {
            "status": "DEGRADED",
            "ready_for_trading_context": False,
            "ready_for_full_analysis": False,
            "section_status": {},
        },
    }
    consumer = build_ai_trader_consumer_v2(full, settings=settings)
    risk = consumer["event_risk"]
    projected_events = [
        *risk["upcoming_events"],
        *risk["awaiting_actual_events"],
        *risk["recently_released_events"],
        *risk["historical_events"],
    ]
    event_keys = [
        item.get("canonical_event_key")
        for item in projected_events
        if item.get("canonical_event_key")
    ]

    policy = SourcePolicyService(settings.source_policy_path)
    issuer = normalize_research_claim(
        fixture["issuer_announcement"],
        policy=policy,
        now=now,
    )
    issuer_warnings = semantic_validation_warnings(
        issuer,
        policy=policy,
        now=now,
    )

    cftc = parse_cftc_financial_row(
        next(csv.reader(StringIO(fixture["cftc_row"])))
    )
    cboe = fixture["cboe"]
    put_call, rejected_put_call = normalize_cboe_put_call(
        {
            **cboe["put_call"],
            "selectedDate": cboe["selected_date"],
        },
        retrieved_at=now.isoformat(),
        valid_until=(now + timedelta(hours=18)).isoformat(),
    )
    vx, vx_diagnostics = parse_vix_futures_csv(
        cboe["vx_csv"],
        data_as_of=cboe["selected_date"],
    )
    payload_bytes = len(
        json.dumps(
            consumer,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    disabled = {
        topic: consumer["agentic_domains"].get(topic)
        for topic in fixture["disabled_topics"]
    }
    findings = {
        "parent_id_verified": source["parent_run_id"]
        == "prun-b67a84fc-29bb-4db0-90c0-42648e087ea1",
        "snapshot_verified": (
            source["snapshot_id"]
            == "mcs-7064e0af-faf1-479d-8855-cdb80ef110fb"
            and source["revision"] == 83
        ),
        "source_metrics_verified": {
            "children": source["child_terminal_count"],
            "partial": source["child_status_counts"]["PARTIAL"],
            "no_data": source["child_status_counts"]["NO_DATA"],
            "tokens": source["total_tokens"],
            "candidates": source["candidate_claim_count"],
            "accepted": source["accepted_claim_count"],
            "rejected": source["rejected_claim_count"],
        }
        == {
            "children": 6,
            "partial": 1,
            "no_data": 5,
            "tokens": 808644,
            "candidates": 19,
            "accepted": 2,
            "rejected": 17,
        },
        "past_pre_release_count": sum(
            item.get("temporal_status") == "PRE_RELEASE"
            for item in projected_events
            if _event_time(item) <= now
        ),
        "awaiting_actual_count": len(risk["awaiting_actual_events"]),
        "duplicate_occurrence_count": len(event_keys) - len(set(event_keys)),
        "next_critical_event": risk["next_critical_event"],
        "issuer_status": issuer.get("lifecycle_status"),
        "issuer_elapsed_warning": (
            "scheduled_event_elapsed_refresh_required" in issuer_warnings
        ),
        "cftc_contract_code": cftc["cftc_contract_market_code"],
        "cftc_open_interest": cftc["open_interest"],
        "cftc_groups_complete": all(
            all(group.get(field) is not None for field in ("long", "short", "spreading"))
            for group in (
                cftc["dealers"],
                cftc["asset_managers"],
                cftc["leveraged_funds"],
            )
        ),
        "cftc_validation_valid": cftc["validation"]["valid"],
        "put_call_ratio": put_call[0]["ratio"] if put_call else None,
        "put_call_rejected": rejected_put_call,
        "vx_contract_count": len(vx),
        "vx_future_quarantine_count": vx_diagnostics[
            "future_timestamp_quarantined_count"
        ],
        "nasdaq_status": consumer["nasdaq"]["status"],
        "nasdaq_holdings_count": consumer["nasdaq"]["holdings_count"],
        "nasdaq_concentration": consumer["nasdaq"]["concentration"],
        "disabled_domains_compact": all(
            value
            == {
                "status": "DISABLED",
                "enabled": False,
                "reason": "agent_disabled_by_configuration",
            }
            for value in disabled.values()
        ),
        "disabled_topics": consumer["research"]["disabled_optional_topics"],
        "consumer_payload_bytes": payload_bytes,
        "live_calls_executed": 0,
    }
    expected_disabled = sorted(fixture["disabled_topics"])
    passed = (
        findings["parent_id_verified"]
        and findings["snapshot_verified"]
        and findings["source_metrics_verified"]
        and findings["past_pre_release_count"] == 0
        and findings["awaiting_actual_count"] == 3
        and findings["duplicate_occurrence_count"] == 0
        and findings["next_critical_event"] is None
        and findings["issuer_status"] == "CURRENT"
        and findings["issuer_elapsed_warning"] is False
        and findings["cftc_contract_code"] == "209747"
        and findings["cftc_open_interest"] == 278558
        and findings["cftc_groups_complete"]
        and findings["cftc_validation_valid"]
        and findings["put_call_ratio"] == 0.82
        and findings["put_call_rejected"] == 0
        and findings["vx_contract_count"] == 2
        and findings["vx_future_quarantine_count"] == 0
        and findings["nasdaq_status"] == "NOT_AVAILABLE"
        and findings["nasdaq_holdings_count"] == 0
        and findings["nasdaq_concentration"] == {}
        and findings["disabled_domains_compact"]
        and findings["disabled_topics"] == expected_disabled
        and findings["consumer_payload_bytes"] <= 90 * 1024
        and findings["live_calls_executed"] == 0
    )
    return {
        "fixture": str(fixture_path),
        "offline": True,
        "findings": findings,
        "passed": passed,
    }


def _event_time(item: dict[str, Any]) -> datetime:
    value = datetime.fromisoformat(
        str(item.get("release_at") or item.get("time_utc")).replace(
            "Z",
            "+00:00",
        )
    )
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("fixture", nargs="?", type=Path, default=DEFAULT_FIXTURE)
    args = parser.parse_args()
    result = replay(args.fixture)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
