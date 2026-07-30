from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.senior_analyst_projection_v1 import (  # noqa: E402
    INVALID_ANALYTIC_STATES,
    build_senior_analyst_payload_v1,
    validate_senior_analyst_payload_v1,
)


DEFAULT_CAPTURE = (
    ROOT
    / "data"
    / "live-ai-trader-capture-20260729T181632Z"
    / "senior-analyst-consumer-full.json"
)
DEFAULT_OUTPUT = ROOT / "docs" / "baselines"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build deterministic SeniorAnalystPayloadV1 offline evidence."
    )
    parser.add_argument("--capture", type=Path, default=DEFAULT_CAPTURE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    source = json.loads(args.capture.read_text(encoding="utf-8"))
    if int(source.get("snapshot_revision") or 0) != 98:
        raise SystemExit("authoritative_capture_revision_must_be_98")
    now = datetime.fromisoformat(str(source["generated_at"]).replace("Z", "+00:00"))
    projection = build_senior_analyst_payload_v1(
        source,
        now=now,
        request_id="offline-snapshot-98",
        request_refresh_mode="replay",
    )
    validation = validate_senior_analyst_payload_v1(projection, now=now)
    if validation["status"] != "PASS_OFFLINE":
        raise SystemExit(json.dumps(validation, indent=2))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    _write(
        args.output_dir / "senior-analyst-snapshot98-projection-v1.json",
        projection,
    )
    _write(
        args.output_dir / "senior-analyst-snapshot98-quality-index.json",
        {
            "contract": projection["contract"],
            "schema_version": projection["schema_version"],
            "snapshot_revision": projection["snapshot_revision"],
            "generated_at": projection["generated_at"],
            "readiness": projection["readiness"],
            "sections": {
                name: {
                    key: section.get(key)
                    for key in (
                        "status",
                        "freshness",
                        "data_as_of",
                        "content_valid_until",
                        "reason_code",
                    )
                }
                for name, section in projection["analytics"].items()
            },
            "missing_data": projection["missing_data"],
            "offline_validation": validation,
            "live_acceptance": "PENDING",
        },
    )
    _write(
        args.output_dir / "senior-analyst-snapshot98-before-after.json",
        _before_after(source, projection, validation),
    )
    _write(
        args.output_dir / "senior-analyst-provider-accounting-example.json",
        {
            "request_id": "offline-snapshot-98",
            "source_snapshot_revision": 98,
            "same_request": True,
            "provider_accounting": projection["provider_accounting"],
            "live_values": False,
            "live_acceptance": "PENDING",
        },
    )
    return 0


def _before_after(
    source: dict[str, Any],
    projection: dict[str, Any],
    validation: dict[str, Any],
) -> dict[str, Any]:
    before_states: Counter[str] = Counter()
    invalid_periods = 0
    awaiting_actual = 0
    exact_duplicates = 0
    contradictory_drivers = _contradictory_driver_count(source)

    def walk(value: Any) -> None:
        nonlocal invalid_periods, awaiting_actual, exact_duplicates
        if isinstance(value, dict):
            for key in ("status", "freshness"):
                state = str(value.get(key) or "").upper()
                if state in INVALID_ANALYTIC_STATES:
                    before_states[state] += 1
            if value.get("invalid_period_mapping") is True:
                invalid_periods += 1
            if str(value.get("release_status") or "").upper() == "AWAITING_ACTUAL":
                awaiting_actual += 1
            coverage = value.get("coverage")
            if isinstance(coverage, dict):
                exact_duplicates += int(coverage.get("exact_duplicate_count") or 0)
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(source.get("sections") or {})
    checks = validation["checks"]
    return {
        "authoritative_capture": {
            "path": (
                "data/live-ai-trader-capture-20260729T181632Z/"
                "senior-analyst-consumer-full.json"
            ),
            "snapshot_revision": 98,
            "generated_at": source.get("generated_at"),
            "full_payload_copy_committed": False,
        },
        "before": {
            "expired_or_stale_state_occurrences": dict(sorted(before_states.items())),
            "invalid_temporal_mapping_occurrences": invalid_periods,
            "past_or_unreconciled_awaiting_actual_occurrences": awaiting_actual,
            "reported_calendar_exact_duplicates": exact_duplicates,
            "contradictory_nasdaq_driver_count": contradictory_drivers,
            "pre_fomc_probabilities_after_release": {
                "count": 0,
                "reason_code": "CAPTURE_PRECEDES_FOMC_RELEASE",
            },
            "semantic_ambiguities": [
                "ICSA unit incorrectly labelled thousands of claims",
                "CUSR0000SA0 key contained CUSR0000SA0L1E core CPI",
                "BEA:GDP transformation omitted",
                "BEA:PCE nominal level not distinguished from price index",
                "CES0000000001 level risked being interpreted as NFP change",
            ],
        },
        "after": {
            "expired_or_stale_values_in_analysis_payload": checks[
                "expired_values_delivered"
            ],
            "invalid_temporal_mappings_in_analysis_payload": checks[
                "invalid_temporal_mappings"
            ],
            "calendar_exact_duplicates": checks["calendar_exact_duplicates"],
            "past_due_awaiting_actual_in_analysis_payload": checks[
                "past_due_awaiting_actual"
            ],
            "post_release_pre_fomc_probabilities_in_current_context": checks[
                "post_release_pre_fomc_probabilities"
            ],
            "contradictory_nasdaq_drivers": checks[
                "contradictory_nasdaq_drivers"
            ],
            "unexplained_omissions": checks["unexplained_omissions"],
            "semantic_mapping_errors_for_required_series": checks[
                "semantic_mapping_errors"
            ],
            "readiness": projection["readiness"],
            "omitted_records": projection["missing_data"],
        },
        "verdict": (
            "IMPLEMENTAZIONE OFFLINE COMPLETATA - ACCETTAZIONE LIVE PENDENTE"
        ),
    }


def _contradictory_driver_count(source: dict[str, Any]) -> int:
    nasdaq = (source.get("sections") or {}).get("nasdaq") or {}
    holdings = {
        str(item.get("symbol") or ""): item
        for item in (nasdaq.get("qqq_holdings") or {}).get("holdings") or []
        if isinstance(item, dict)
    }
    count = 0
    for driver in nasdaq.get("driver_context") or []:
        if not isinstance(driver, dict):
            continue
        quote = holdings.get(str(driver.get("symbol") or ""))
        if quote is None:
            count += 1
            continue
        try:
            difference = abs(
                float(driver.get("change_pct"))
                - float(quote.get("change_pct"))
            )
        except (TypeError, ValueError):
            count += 1
            continue
        if difference > 0.02:
            count += 1
    return count


def _write(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
