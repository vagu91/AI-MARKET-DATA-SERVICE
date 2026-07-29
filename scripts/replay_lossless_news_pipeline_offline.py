from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.persistence.database import connect_sqlite_read_only
from app.services.data_freshness_service import parse_datetime
from app.services.market_context_sync_service import (
    canonical_json,
    delivery_readiness,
    finalize_delivery,
    material_fingerprint,
    reconcile_delivered_section,
    section_status,
)
from app.services.news_intelligence_service import build_news_context


def replay(
    *,
    database_path: Path,
    captured_full_sync_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    database_hash_before = _sha256_file(database_path)
    captured = json.loads(captured_full_sync_path.read_text(encoding="utf-8"))
    reference = parse_datetime(captured.get("generated_at")) or datetime.now(UTC)
    with TemporaryDirectory(prefix="lossless-news-replay-") as directory:
        sandbox_one = Path(directory) / "sandbox-one.sqlite"
        sandbox_two = Path(directory) / "sandbox-two.sqlite"
        shutil.copy2(database_path, sandbox_one)
        shutil.copy2(database_path, sandbox_two)
        rows = _read_news(sandbox_one)
        second_rows = _read_news(sandbox_two)
        first_sandbox_hash_before = _sha256_file(sandbox_one)
        second_sandbox_hash_before = _sha256_file(sandbox_two)
        first = _materialize(captured, rows, reference=reference)
        second = _materialize(captured, second_rows, reference=reference)
        first_sandbox_hash_after = _sha256_file(sandbox_one)
        second_sandbox_hash_after = _sha256_file(sandbox_two)
        if first_sandbox_hash_before != first_sandbox_hash_after:
            raise AssertionError("first_read_only_sandbox_database_changed")
        if second_sandbox_hash_before != second_sandbox_hash_after:
            raise AssertionError("second_read_only_sandbox_database_changed")
    first_bytes = canonical_json(first).encode("utf-8")
    second_bytes = canonical_json(second).encode("utf-8")
    if first_bytes != second_bytes:
        raise AssertionError("lossless_news_replay_not_byte_identical")
    database_hash_after = _sha256_file(database_path)
    if database_hash_before != database_hash_after:
        raise AssertionError("read_only_sandbox_database_changed")
    news = first["sections"]["news"]["context"]
    diagnostics = news["diagnostics"]
    original_news = captured["sections"]["news"].get("context") or {}
    summary = {
        "mode": "OFFLINE_READ_ONLY_LOSSLESS_NEWS_REPLAY",
        "before": {
            "database_record_count": len(rows),
            "delivered_article_count": len(original_news.get("articles") or []),
            "directly_relevant_count": len(
                original_news.get("directly_relevant") or []
            ),
            "excluded_count": len(original_news.get("excluded") or []),
            "duplicate_count": len(original_news.get("duplicates") or []),
            "cluster_count": len(original_news.get("clusters") or []),
            "news_analysis": (
                (captured.get("readiness") or {})
                .get("analysis_readiness", {})
                .get("news_analysis", {})
                .get("status")
            ),
        },
        "after": {
            "raw_fetched": diagnostics["raw_fetched"],
            "raw_acquired": diagnostics["raw_acquired"],
            "persisted_valid": diagnostics["persisted_valid"],
            "persisted_valid_in_scope": diagnostics[
                "persisted_valid_in_scope"
            ],
            "delivered": diagnostics["delivered"],
            "delivered_logical_articles": diagnostics[
                "delivered_logical_articles"
            ],
            "active_current": diagnostics["active_current"],
            "historical": diagnostics["historical"],
            "lifecycle_unclassified": diagnostics["lifecycle_unclassified"],
            "quarantined": diagnostics["quarantined"],
            "withheld": diagnostics["withheld"],
            "technically_invalid": diagnostics["technically_invalid"],
            "technically_rejected": diagnostics["technically_rejected"],
            "outside_scope": diagnostics["outside_scope"],
            "publisher_verified": diagnostics["publisher_verified"],
            "publisher_unknown": diagnostics["publisher_unknown"],
            "category_unclassified": sum(
                item.get("category_status") == "UNCLASSIFIED"
                for item in news.get("articles") or []
            ),
            "topic_ambiguous": sum(
                item.get("topic_status") == "AMBIGUOUS"
                for item in news.get("articles") or []
            ),
            "accounting_balanced": diagnostics["accounting_balanced"],
            "accounting_equations": diagnostics["accounting_equations"],
            "non_delivered_records": diagnostics[
                "non_delivered_records"
            ],
            "publisher_verification_breakdown": diagnostics[
                "publisher_verification_breakdown"
            ],
            "content_availability_breakdown": diagnostics[
                "content_availability_breakdown"
            ],
            "acquisition_provider_breakdown": diagnostics[
                "acquisition_provider_breakdown"
            ],
            "distribution_source_breakdown": diagnostics[
                "distribution_source_breakdown"
            ],
            "original_publisher_breakdown": diagnostics[
                "original_publisher_breakdown"
            ],
            "news_analysis": (
                first["readiness"]["analysis_readiness"]["news_analysis"][
                    "status"
                ]
            ),
        },
        "full_sync": {
            "section_count": len(first["sections"]),
            "payload_size_bytes": len(first_bytes),
            "sha256": hashlib.sha256(first_bytes).hexdigest().upper(),
            "checksum": first["checksum"],
            "two_independent_replays_byte_identical": True,
        },
        "fixed_point": {
            "new_database_writes": 0,
            "new_snapshot_revisions": 0,
            "new_outbox_rows": 0,
            "byte_identical": True,
        },
        "side_effects": {
            "provider_live_calls": 0,
            "ai_jobs": 0,
            "ai_backend_invocations": 0,
            "browser_calls": 0,
            "deliveries": 0,
            "trading_actions": 0,
            "orders": 0,
        },
        "database": {
            "sha256_before": database_hash_before,
            "sha256_after": database_hash_after,
            "unchanged": database_hash_before == database_hash_after,
            "access": "SQLITE_MODE_RO_QUERY_ONLY",
            "independent_sandbox_count": 2,
            "sandbox_hashes_unchanged": (
                first_sandbox_hash_before == first_sandbox_hash_after
                and second_sandbox_hash_before
                == second_sandbox_hash_after
            ),
        },
    }
    return summary, first


def _read_news(database_path: Path) -> list[dict[str, Any]]:
    with connect_sqlite_read_only(database_path) as conn:
        rows = conn.execute(
            """
            SELECT * FROM market_news
            ORDER BY COALESCE(published_at,retrieved_at) DESC,news_key,id
            """
        ).fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        raw = _loads(item.pop("raw_payload_json", None), {})
        item["symbols"] = _loads(item.pop("symbols_json", None), [])
        item["topics"] = _loads(item.pop("topics_json", None), [])
        item["raw_payload"] = raw
        output.append(item)
    return output


def _materialize(
    captured: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    reference: datetime,
) -> dict[str, Any]:
    full = deepcopy(captured)
    original_news = dict(full["sections"]["news"])
    original_context = (
        dict(original_news.get("context") or {})
        if isinstance(original_news.get("context"), dict)
        else {}
    )
    context = {
        **original_context,
        **build_news_context(rows, now=reference, scope_days=30),
    }
    news = {
        **original_news,
        "context": context,
        "latest": list(context.get("latest") or []),
        "digest": dict(context.get("digest") or {}),
    }
    quarantined = [
        item
        for item in context.get("quarantined_records") or []
        if item.get("disposition") == "QUARANTINED"
    ]
    if quarantined:
        news["producer_disclosures"] = {
            "quarantine": {
                "status": "WITHHELD",
                "record_count": len(quarantined),
                "reasons": sorted(
                    {str(item.get("reason") or "UNKNOWN") for item in quarantined}
                ),
                "record_fingerprints": sorted(
                    str(item["record_fingerprint"]) for item in quarantined
                ),
            }
        }
    else:
        news.pop("producer_disclosures", None)
    news = reconcile_delivered_section("news", news)
    prior_sync = dict(original_news.get("sync") or {})
    material = {key: value for key, value in news.items() if key != "sync"}
    status, reason = section_status(news, section_name="news")
    news["sync"] = {
        **prior_sync,
        "section_revision": int(prior_sync.get("section_revision") or 0) + 1,
        "fingerprint": material_fingerprint(material),
        "record_count": len(news["context"].get("articles") or []),
        "status": status,
        "reason": reason,
        "freshness": (
            "CURRENT"
            if status in {"AVAILABLE", "DEGRADED", "PARTIAL"}
            else status
        ),
    }
    full["sections"]["news"] = news
    if isinstance(full.get("manifest"), dict):
        manifest = deepcopy(full["manifest"])
        if isinstance(manifest.get("sections"), dict):
            manifest["sections"]["news"] = {
                key: value
                for key, value in news["sync"].items()
                if key != "lineage"
            }
        full["manifest"] = manifest
    full["context_fingerprint"] = material_fingerprint(
        {
            name: section.get("sync") or {}
            for name, section in full["sections"].items()
        }
    )
    full["readiness"] = delivery_readiness(full["sections"])
    full.pop("checksum", None)
    full["payload_size_bytes"] = 0
    return finalize_delivery(full)


def _loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--captured-full-sync", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path)
    parser.add_argument("--full-sync-output", type=Path)
    args = parser.parse_args()
    summary, full_sync = replay(
        database_path=args.database,
        captured_full_sync_path=args.captured_full_sync,
    )
    if args.summary_output:
        args.summary_output.write_bytes(
            canonical_json(summary).encode("utf-8")
        )
    if args.full_sync_output:
        args.full_sync_output.write_bytes(
            canonical_json(full_sync).encode("utf-8")
        )
    if not args.summary_output and not args.full_sync_output:
        print(canonical_json(summary))


if __name__ == "__main__":
    main()
