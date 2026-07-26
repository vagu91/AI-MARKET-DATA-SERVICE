from __future__ import annotations

import argparse
import hashlib
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.services.event_calendar_window_service import (
    build_event_calendar_window,
)
from app.services.market_context_sync_service import (
    canonical_json,
    delivery_readiness,
    finalize_delivery,
    material_fingerprint,
    reconcile_delivered_section,
    record_count_for,
    section_freshness,
    section_status,
)


ROOT = Path(__file__).resolve().parents[1]
FORENSIC_ROOT = (
    ROOT
    / "data"
    / "market-context-sync-live-validation-20260726T083217Z"
)
FULL_EXACT = FORENSIC_ROOT / "ai-trader-full-sync-exact.json"
VALIDATION_SUMMARY = FORENSIC_ROOT / "validation-summary.json"
EXPECTED_FULL_SHA256 = (
    "6521F9B03004DB1D7B52398FABCF46306C26AE604D263580A7475B8DBFE3407F"
)
EXPECTED_SUMMARY_SHA256 = (
    "3F130BF7DA0448FA283E2A185EB041ED28CA26A1A5E4F6E8ABC61BCF95D39605"
)


def replay() -> dict[str, Any]:
    original_bytes = FULL_EXACT.read_bytes()
    summary_bytes = VALIDATION_SUMMARY.read_bytes()
    original = json.loads(original_bytes)
    event_section = deepcopy(original["sections"]["event_calendar"])
    original_window = event_section.get("window") or {}
    source_full = _calendar_source_payload(original, event_section)
    settings = Settings(
        _env_file=None,
        environment="test",
        source_policy_path=ROOT / "config" / "source_policy.json",
        model_pricing_path=ROOT / "config" / "model_pricing.json",
    )
    generated_at = datetime.fromisoformat(
        str(original["generated_at"]).replace("Z", "+00:00")
    )
    replayed_window = build_event_calendar_window(
        source_full,
        settings=settings,
        now=generated_at,
    )
    event_section["window"] = replayed_window

    replayed = deepcopy(original)
    replayed["sections"]["event_calendar"] = _refresh_section(
        event_section,
        original=original["sections"]["event_calendar"],
        generated_at=generated_at,
    )
    replayed["sections"]["news"] = _refresh_section(
        reconcile_delivered_section(
            "news",
            {
                key: value
                for key, value in original["sections"]["news"].items()
                if key != "sync"
            },
        ),
        original=original["sections"]["news"],
        generated_at=generated_at,
    )
    replayed["readiness"] = delivery_readiness(replayed["sections"])
    replayed["context_fingerprint"] = material_fingerprint(
        {
            name: {
                "section_revision": section["sync"].get(
                    "section_revision"
                ),
                "fingerprint": section["sync"]["fingerprint"],
            }
            for name, section in replayed["sections"].items()
        }
    )
    replayed.pop("checksum", None)
    replayed["payload_size_bytes"] = 0
    finalize_delivery(replayed)

    old_ids = _window_ids(original_window)
    new_ids = _window_ids(replayed_window)
    quarantine_ids = {
        str(item.get("occurrence_id"))
        for item in (
            (replayed_window.get("audit") or {}).get(
                "quarantined_occurrences"
            )
            or []
        )
        if item.get("occurrence_id")
    }
    formerly_omitted = sorted(new_ids - old_ids)
    actual_missing = sorted(
        {
            str(item["occurrence_id"])
            for item in _window_events(replayed_window)
            if item.get("is_past")
            and item.get("release_status")
            in {"AWAITING_ACTUAL", "UNAVAILABLE"}
        }
    )
    news = replayed["sections"]["news"]
    news_context = news.get("context") or {}
    schedule_sync = (
        replayed["sections"]["market_schedule"].get("sync") or {}
    )
    coverage = replayed_window["coverage"]
    equation_rhs = (
        int(coverage["delivered_valid_source_record_count"])
        + int(coverage["quarantined_occurrence_count"])
        + int(coverage["exact_duplicate_count"])
    )
    replayed_bytes = canonical_json(replayed).encode("utf-8")
    return {
        "mode": "OFFLINE_SNAPSHOT_91_CONTENT_CLOSURE_REPLAY",
        "snapshot_id": original["snapshot_id"],
        "snapshot_revision": original["snapshot_revision"],
        "input_integrity": {
            "full_exact_sha256": hashlib.sha256(
                original_bytes
            ).hexdigest().upper(),
            "full_exact_hash_verified": (
                hashlib.sha256(original_bytes).hexdigest().upper()
                == EXPECTED_FULL_SHA256
            ),
            "validation_summary_sha256": hashlib.sha256(
                summary_bytes
            ).hexdigest().upper(),
            "validation_summary_hash_verified": (
                hashlib.sha256(summary_bytes).hexdigest().upper()
                == EXPECTED_SUMMARY_SHA256
            ),
            "original_full_sync_bytes": len(original_bytes),
        },
        "calendar": {
            "source_candidate_count": coverage[
                "source_candidate_count"
            ],
            "validated_occurrence_count": coverage[
                "validated_occurrence_count"
            ],
            "delivered_occurrence_count": coverage[
                "delivered_occurrence_count"
            ],
            "delivered_valid_source_record_count": coverage[
                "delivered_valid_source_record_count"
            ],
            "quarantined_occurrence_count": coverage[
                "quarantined_occurrence_count"
            ],
            "invalid_temporal_count": coverage[
                "invalid_temporal_count"
            ],
            "exact_duplicate_count": coverage[
                "exact_duplicate_count"
            ],
            "omitted_for_size_count": coverage[
                "omitted_for_size_count"
            ],
            "omitted_for_count_count": coverage[
                "omitted_for_count_count"
            ],
            "unexplained_loss": coverage["unexplained_loss"],
            "formerly_omitted_ids": formerly_omitted,
            "quarantined_ids": sorted(quarantine_ids),
            "bucket_coverage": coverage["by_bucket"],
            "actual_missing_ids": actual_missing,
        },
        "news": {
            "delivered_current_raw_count": len(
                news_context.get("articles") or []
            ),
            "delivered_historical_raw_count": len(
                news_context.get("historical_articles") or []
            ),
            "declared_historical_article_count": int(
                news_context.get("historical_article_count") or 0
            ),
            "historical_context_available": bool(
                news_context.get("historical_context_available")
            ),
            "coverage_status": news_context.get(
                "historical_coverage_status"
            ),
            "quarantine_disclosure": (
                (news.get("producer_disclosures") or {}).get("quarantine")
                or {}
            ),
        },
        "market_schedule": {
            "status": schedule_sync.get("status"),
            "freshness": schedule_sync.get("freshness"),
            "reason": schedule_sync.get("reason"),
        },
        "replayed_full_sync": {
            "exact_size_bytes": len(replayed_bytes),
            "reported_size_bytes": replayed["payload_size_bytes"],
            "checksum": replayed["checksum"],
            "readiness": replayed["readiness"]["status"],
        },
        "side_effects": {
            "provider_calls": 0,
            "ai_job_count": 0,
            "ai_run_count": 0,
            "ai_backend_invocations": 0,
            "operational_database_writes": 0,
        },
        "invariants": {
            "candidate_equation_holds": (
                int(coverage["source_candidate_count"])
                == equation_rhs
            ),
            "candidate_equation": {
                "candidate": int(coverage["source_candidate_count"]),
                "delivered_valid": int(
                    coverage["delivered_valid_source_record_count"]
                ),
                "quarantined_invalid": int(
                    coverage["quarantined_occurrence_count"]
                ),
                "exact_duplicate_technical": int(
                    coverage["exact_duplicate_count"]
                ),
                "right_hand_side": equation_rhs,
            },
            "omitted_for_size_is_zero": (
                coverage["omitted_for_size_count"] == 0
            ),
            "omitted_for_count_is_zero": (
                coverage["omitted_for_count_count"] == 0
            ),
            "unexplained_loss_is_zero": (
                coverage["unexplained_loss"] == 0
            ),
            "news_historical_state_matches_content": (
                int(news_context.get("historical_article_count") or 0)
                == len(news_context.get("historical_articles") or [])
                and bool(
                    news_context.get("historical_context_available")
                )
                == bool(news_context.get("historical_articles"))
            ),
            "payload_size_exact": (
                len(replayed_bytes) == replayed["payload_size_bytes"]
            ),
        },
    }


def _calendar_source_payload(
    original: dict[str, Any],
    event_section: dict[str, Any],
) -> dict[str, Any]:
    calendar = deepcopy(event_section.get("calendar") or {})
    calendar["source_coverage"] = {
        "by_bucket": {
            "PREVIOUS_WEEK": {"status": "UNVERIFIED_EMPTY"},
            "CURRENT_WEEK": {"status": "PARTIAL"},
            "NEXT_WEEK": {"status": "PARTIAL"},
        }
    }
    return {
        "generated_at_utc": original["generated_at"],
        "event_calendar": calendar,
        "economic_calendar_enrichment": event_section.get(
            "economic_calendar_enrichment"
        )
        or {},
        "events_today": event_section.get("events_today") or [],
        "next_24h_events": event_section.get("next_24h_events") or [],
        "next_7d_critical_events": event_section.get(
            "next_7d_critical_events"
        )
        or [],
        "recently_released_events": event_section.get(
            "recently_released_events"
        )
        or [],
        "upcoming_high_impact_events": event_section.get(
            "upcoming_high_impact_events"
        )
        or [],
    }


def _refresh_section(
    payload: dict[str, Any],
    *,
    original: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    output = {
        key: value for key, value in payload.items() if key != "sync"
    }
    sync = dict(original.get("sync") or {})
    status, reason = section_status(output)
    sync.update(
        {
            "fingerprint": material_fingerprint(output),
            "record_count": record_count_for(output),
            "freshness": section_freshness(
                status=status,
                valid_until=sync.get("valid_until"),
                payload=output,
                reference=generated_at,
            ),
            "status": status,
            "reason": reason,
        }
    )
    output["sync"] = sync
    return output


def _window_events(window: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        item
        for bucket in ("previous_week", "current_week", "next_week")
        for item in (window.get(bucket) or {}).get("events") or []
        if isinstance(item, dict)
    ]


def _window_ids(window: dict[str, Any]) -> set[str]:
    return {
        str(item["occurrence_id"])
        for item in _window_events(window)
        if item.get("occurrence_id")
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay snapshot 91 content closure without live I/O."
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional report path; stdout is always emitted.",
    )
    args = parser.parse_args()
    result = replay()
    text = json.dumps(
        result,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if all(result["invariants"].values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
