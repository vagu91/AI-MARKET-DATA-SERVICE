from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
from pathlib import Path
from typing import Any


MOJIBAKE = ("Ã¢", "Ãƒ", "Ã‚", "\ufffd")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--database-path")
    parser.add_argument("--tests-passed", default="unknown")
    parser.add_argument("--compileall-passed", action="store_true")
    args = parser.parse_args()
    artifact_dir = Path(args.artifact_dir)
    consumer_force = _load(artifact_dir / "consumer_force.json")
    debug_force = _load(artifact_dir / "debug_force.json")
    consumer_cache = _load(artifact_dir / "consumer_cache.json")
    debug_cache = _load(artifact_dir / "debug_cache.json")
    errors: list[str] = []
    warnings: list[str] = []

    readiness = consumer_force.get("readiness") or {}
    blocking = readiness.get("blocking_reasons") or []
    if readiness.get("ready") is True and blocking:
        errors.append("ready_true_with_blocking_reasons")

    quality = consumer_force.get("data_quality") or {}
    critical_missing_count = int(quality.get("critical_missing_count") or 0)
    missing_fields = quality.get("missing_critical_fields") or []
    if critical_missing_count > 0 and not missing_fields:
        errors.append("critical_missing_count_without_missing_fields")

    macro_snapshot = consumer_force.get("macro_snapshot") or {}
    macro_provider_result_count = len(macro_snapshot.get("provider_results") or [])
    macro_non_empty = any(
        isinstance(value, dict) and value
        for key, value in macro_snapshot.items()
        if key != "provider_results"
    )
    macro_pipeline = quality.get("macro_pipeline") or {}
    macro_read_back_count = int(((debug_force.get("data_quality") or {}).get("macro") or {}).get("read_back_count") or macro_pipeline.get("series_count") or 0)
    if macro_provider_result_count and not macro_non_empty:
        errors.append("macro_provider_results_without_snapshot")

    if _contains_zero_unknown_weight(consumer_force):
        errors.append("zero_used_for_unknown_weight")
    if _concentration_low_without_weights(consumer_force):
        errors.append("concentration_low_without_weights")
    if _weighted_contribution_without_weight(consumer_force):
        errors.append("weighted_contribution_without_weight")

    social = consumer_force.get("social_sentiment") or {}
    social_market = social.get("social_market_sentiment") or {}
    if social.get("relevant_item_count") in (0, None) and social_market.get("neutral_ratio") == 1.0:
        errors.append("social_neutral_ratio_masks_absence_of_signal")

    if _contains_disabled_failed(consumer_force):
        errors.append("disabled_provider_classified_failed")
    if _duplicate_ssl_errors(consumer_force):
        errors.append("consumer_contains_duplicate_ssl_errors")
    if _has_mojibake(consumer_force) or _has_mojibake(debug_force):
        errors.append("payload_contains_mojibake")
    if _has_false_duplicate(debug_force):
        errors.append("distinct_events_marked_duplicate")

    event_enrichment = (debug_force.get("metadata") or {}).get("event_enrichment") or {}
    if "AI_called" not in event_enrichment:
        errors.append("ai_called_missing_from_debug_event_enrichment")
    errors.extend(_ai_enrichment_state_errors(event_enrichment))
    if _enrichment_value_without_source(debug_force):
        errors.append("enrichment_value_without_source")

    cache_quality = consumer_cache.get("data_quality") or {}
    refresh_false_provider_calls = int(((cache_quality.get("multi_source_pipeline") or {}).get("provider_calls") or 0))
    refresh_false_network_calls = refresh_false_provider_calls
    if refresh_false_provider_calls:
        errors.append("refresh_false_provider_calls_nonzero")

    consumer_size = len(json.dumps(consumer_force, separators=(",", ":"), default=str).encode("utf-8"))
    debug_size = len(json.dumps(debug_force, separators=(",", ":"), default=str).encode("utf-8"))
    if consumer_size > 200_000:
        warnings.append("consumer_payload_above_200kb")

    single_db = _single_database_ok(args.database_path)
    if args.database_path and not single_db:
        errors.append("database_integrity_failed")

    validation = {
        "passed": not errors,
        "git_head": _git_head(),
        "tests_passed": args.tests_passed,
        "compileall_passed": bool(args.compileall_passed),
        "single_physical_database": single_db,
        "consumer_ready": readiness.get("ready"),
        "consumer_blocking_reasons": blocking,
        "critical_errors": readiness.get("critical_errors"),
        "macro_snapshot_non_empty": macro_non_empty,
        "macro_provider_result_count": macro_provider_result_count,
        "macro_read_back_count": macro_read_back_count,
        "snapshot_materialized": bool((quality.get("pipeline_integrity") or {}).get("snapshot_materialization_completed")),
        "ai_configured": event_enrichment.get("configured"),
        "ai_attempted_count": event_enrichment.get("attempted_event_count"),
        "ai_completed_count": event_enrichment.get("completed_event_count"),
        "ai_timeout_count": event_enrichment.get("timeout_event_count"),
        "ai_failed_count": event_enrichment.get("failed_event_count"),
        "ai_rejected_field_count": event_enrichment.get("rejected_event_count"),
        "ai_accepted_field_count": sum(len(item.get("accepted_fields") or []) for item in event_enrichment.get("events") or []),
        "ai_persisted_event_count": event_enrichment.get("persisted_event_count"),
        "ai_read_back_event_count": event_enrichment.get("read_back_event_count"),
        "event_false_duplicates": 0 if "distinct_events_marked_duplicate" not in errors else 1,
        "qqq_unknown_weights_preserved": "zero_used_for_unknown_weight" not in errors,
        "social_sentiment_semantically_valid": "social_neutral_ratio_masks_absence_of_signal" not in errors,
        "news_relevance_valid": len(((consumer_force.get("news_context") or {}).get("latest") or [])) <= 15,
        "encoding_valid": "payload_contains_mojibake" not in errors,
        "disabled_provider_failures": 0 if "disabled_provider_classified_failed" not in errors else 1,
        "polymarket_error_compacted": "consumer_contains_duplicate_ssl_errors" not in errors,
        "refresh_false_network_calls": refresh_false_network_calls,
        "refresh_false_provider_calls": refresh_false_provider_calls,
        "consumer_payload_size_bytes": consumer_size,
        "debug_payload_size_bytes": debug_size,
        "warnings": warnings,
        "errors": errors,
    }
    (artifact_dir / "final_validation.json").write_text(json.dumps(validation, indent=2, default=str), encoding="utf-8")
    return 0 if validation["passed"] else 1


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _git_head() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None


def _single_database_ok(database_path: str | None) -> bool:
    if not database_path:
        return True
    try:
        with sqlite3.connect(database_path) as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
        return bool(row and row[0] == "ok")
    except sqlite3.Error:
        return False


def _walk(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def _ai_enrichment_state_errors(enrichment: dict[str, Any]) -> list[str]:
    """Validate that aggregate AI telemetry is a lossless summary of event rows."""
    errors: list[str] = []
    rows = enrichment.get("events") or []
    allowed = {"disabled", "not_configured", "not_required", "not_available", "completed", "failed", "timeout", "cancelled", "rejected"}
    global_status = enrichment.get("status")
    if global_status not in allowed:
        errors.append("ai_enrichment_unknown_global_status")
    if global_status in {"completed", "failed", "timeout", "cancelled", "rejected"} and not enrichment.get("AI_called"):
        errors.append("ai_enrichment_terminal_status_without_ai_call")
    if global_status in {"disabled", "not_configured", "not_required", "not_available"} and enrichment.get("AI_called"):
        errors.append("ai_enrichment_passive_status_with_ai_call")
    for row in rows:
        status = row.get("status")
        attempted = bool(row.get("attempted"))
        called = bool(row.get("AI_called"))
        timeout = bool(row.get("timeout"))
        if status not in allowed:
            errors.append("ai_enrichment_unknown_event_status")
        if not attempted and timeout:
            errors.append("ai_enrichment_timeout_without_attempt")
        if not called and status == "timeout":
            errors.append("ai_enrichment_timeout_without_ai_call")
        if attempted != called:
            errors.append("ai_enrichment_attempt_call_mismatch")

    attempted_count = sum(1 for row in rows if row.get("attempted"))
    completed_count = sum(1 for row in rows if row.get("status") == "completed")
    timeout_count = sum(1 for row in rows if row.get("timeout"))
    failed_count = sum(1 for row in rows if row.get("status") == "failed")
    accepted_count = sum(len(row.get("accepted_fields") or []) for row in rows)
    rejected_count = sum(len(row.get("rejected_fields") or []) for row in rows)
    persisted_count = sum(1 for row in rows if row.get("persisted"))
    read_back_count = sum(1 for row in rows if row.get("read_back"))
    if int(enrichment.get("attempted_event_count") or 0) != attempted_count:
        errors.append("ai_enrichment_attempted_count_mismatch")
    if int(enrichment.get("completed_event_count") or 0) != completed_count:
        errors.append("ai_enrichment_completed_count_mismatch")
    if int(enrichment.get("timeout_event_count") or 0) != timeout_count:
        errors.append("ai_enrichment_timeout_count_mismatch")
    if int(enrichment.get("failed_event_count") or 0) != failed_count:
        errors.append("ai_enrichment_failed_count_mismatch")
    if int(enrichment.get("accepted_event_count") or 0) != accepted_count:
        errors.append("ai_enrichment_accepted_field_count_mismatch")
    if int(enrichment.get("rejected_event_count") or 0) != rejected_count:
        errors.append("ai_enrichment_rejected_field_count_mismatch")
    if int(enrichment.get("persisted_event_count") or 0) != persisted_count:
        errors.append("ai_enrichment_persisted_count_mismatch")
    if int(enrichment.get("read_back_event_count") or 0) != read_back_count:
        errors.append("ai_enrichment_read_back_count_mismatch")
    if not enrichment.get("AI_called") and (attempted_count or completed_count or timeout_count):
        errors.append("ai_enrichment_aggregate_call_mismatch")
    if not attempted_count and timeout_count:
        errors.append("ai_enrichment_timeout_count_without_attempt")
    return list(dict.fromkeys(errors))


def _contains_zero_unknown_weight(payload: dict[str, Any]) -> bool:
    for item in _walk(payload):
        if item.get("weight_data_available") is False:
            for key in ("top_5_weight_pct", "top_10_weight_pct", "qqq_weight", "portfolio_weight_pct", "classified_weight_pct", "unknown_weight_pct"):
                if item.get(key) == 0.0:
                    return True
    return False


def _concentration_low_without_weights(payload: dict[str, Any]) -> bool:
    for item in _walk(payload):
        if item.get("classification") == "LOW" and item.get("weight_data_available") is False:
            return True
    return False


def _weighted_contribution_without_weight(payload: dict[str, Any]) -> bool:
    for item in _walk(payload):
        if item.get("qqq_weight") is None and item.get("weighted_contribution") not in (None, ""):
            return True
    return False


def _contains_disabled_failed(payload: dict[str, Any]) -> bool:
    text = json.dumps(payload, default=str).lower()
    return "disabled by config" in text and "provider_failed" in text


def _duplicate_ssl_errors(payload: dict[str, Any]) -> bool:
    text = json.dumps(payload, default=str)
    return text.count("CERTIFICATE_VERIFY_FAILED") > 1


def _has_mojibake(payload: dict[str, Any]) -> bool:
    text = json.dumps(payload, default=str)
    return any(marker in text for marker in MOJIBAKE)


def _has_false_duplicate(debug_payload: dict[str, Any]) -> bool:
    calendar = debug_payload.get("event_calendar") or {}
    for section in calendar.values():
        if not isinstance(section, list):
            continue
        for event in section:
            summary = ((event or {}).get("enrichment") or {}).get("summary") or {}
            if summary.get("duplicate_reason") == "same_category_date_event_type":
                return True
    return False


def _enrichment_value_without_source(debug_payload: dict[str, Any]) -> bool:
    calendar = debug_payload.get("event_calendar") or {}
    for section in calendar.values():
        if not isinstance(section, list):
            continue
        for event in section:
            enrichment = (event or {}).get("enrichment") or {}
            has_value = any(enrichment.get(field) not in (None, "") for field in ("forecast", "previous", "consensus", "actual"))
            if has_value and not enrichment.get("source_url"):
                return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
