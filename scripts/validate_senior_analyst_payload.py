from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.data_freshness_service import parse_datetime  # noqa: E402
from app.services.senior_analyst_projection_v1 import (  # noqa: E402
    validate_senior_analyst_payload_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the exact HTTP bytes received by Senior Analyst."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--headers", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--request-started-at")
    parser.add_argument("--require-live", action="store_true")
    parser.add_argument("--process-cleanup-ok", action="store_true")
    args = parser.parse_args()

    body = args.input.read_bytes()
    payload = json.loads(body.decode("utf-8"))
    now = datetime.now(UTC)
    result = validate_senior_analyst_payload_v1(
        payload,
        now=now,
        require_recent_response=args.require_live,
    )
    started = parse_datetime(args.request_started_at)
    generated = parse_datetime(payload.get("generated_at"))
    same_execution = bool(
        started
        and generated
        and generated >= started
        and generated <= now.replace(microsecond=999999)
    )
    if args.require_live:
        result["checks"]["response_generated_recently"] = bool(
            result["checks"]["response_generated_recently"] and same_execution
        )
    result["checks"]["process_cleanup_ok"] = args.process_cleanup_ok
    required_zero = (
        "expired_values_delivered",
        "stale_values_presented_as_current",
        "invalid_temporal_mappings",
        "semantic_mapping_errors",
        "calendar_exact_duplicates",
        "past_due_awaiting_actual",
        "post_release_pre_fomc_probabilities",
        "contradictory_nasdaq_drivers",
        "expired_current_news",
        "unexplained_omissions",
    )
    passed = (
        result["checks"]["response_generated_recently"]
        and all(result["checks"][key] == 0 for key in required_zero)
        and result["checks"]["provider_accounting_valid"] is True
        and result["checks"]["process_cleanup_ok"] is True
    )
    result["status"] = (
        "PASS"
        if args.require_live and passed
        else "PASS_OFFLINE"
        if not args.require_live
        and all(result["checks"][key] == 0 for key in required_zero)
        else "FAIL"
    )
    result.update(
        {
            "validated_file": str(args.input.resolve()),
            "validated_exact_http_body": True,
            "body_size_bytes": len(body),
            "body_sha256": hashlib.sha256(body).hexdigest(),
            "headers_file": (
                str(args.headers.resolve())
                if args.headers and args.headers.exists()
                else None
            ),
            "request_started_at": (
                started.isoformat() if started else args.request_started_at
            ),
            "response_generated_at": (
                generated.isoformat() if generated else payload.get("generated_at")
            ),
            "same_execution": same_execution,
            "live_acceptance_evaluated": args.require_live,
        }
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result["status"] in {"PASS", "PASS_OFFLINE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
