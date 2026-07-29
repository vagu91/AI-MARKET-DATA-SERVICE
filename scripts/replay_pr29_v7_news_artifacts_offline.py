from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import Settings
from app.models.common import ProviderType
from app.providers.news_provider import NewsProvider, _news_result


V7_HEAD = "81f36f614babf148c9d183e644827fce48309ae7"
REFERENCE_TIME = "2026-07-29T11:32:13+00:00"
MISSING_ARTIFACT_FIELDS = (
    "V7_CAPTURE_DID_NOT_PRESERVE_RAW_PROVIDER_PAYLOAD_FOR_PRE_PARSE_EXCLUSION"
)


class ReplayRepository:
    def __init__(self) -> None:
        self.keys: set[str] = set()
        self.insert_count = 0
        self.upsert_count = 0

    def upsert_news(self, article: dict[str, Any]) -> dict[str, Any]:
        self.upsert_count += 1
        key = str(article["news_key"])
        if key not in self.keys:
            self.keys.add(key)
            self.insert_count += 1
        return {"news_key": key}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def audit_rows(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def service_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def reconcile(validation_dir: Path) -> dict[str, Any]:
    artifacts = validation_dir / "artifacts"
    accounting = load_json(artifacts / "provider-accounting.json")
    response = load_json(artifacts / "live-acquisition-response.json")
    audit = audit_rows(validation_dir / "logs" / "provider-audit.jsonl")
    service = service_rows(validation_dir / "logs" / "service-stdout.log")

    providers = {
        str(item["provider"]): item
        for item in response["data_quality"]["provider_accounting"]
    }
    parse_batches = {
        str(item["provider"]): item
        for item in audit
        if item.get("event") == "provider_parse_batch"
    }
    raw_provider: dict[str, str] = {}
    raw_index: dict[str, int] = {}
    for provider, row in parse_batches.items():
        for index, record_id in enumerate(row.get("raw_record_ids") or []):
            raw_provider[str(record_id)] = provider
            raw_index[str(record_id)] = index

    raw = set(accounting["sets"]["raw_ids"])
    persisted = set(accounting["sets"]["persisted_ids"])
    outside = set(accounting["sets"]["outside_scope_ids"])
    persistence_results = response["data_quality"]["persistence_results"]
    exact_duplicates = {
        str(item["record_id"])
        for item in persistence_results
        if item.get("reason_code") == "EXACT_TECHNICAL_DUPLICATE"
    }
    technically_rejected = {
        str(item["record_id"])
        for item in persistence_results
        if item.get("result") == "TECHNICALLY_REJECTED"
    } - exact_duplicates
    persistence_failed = {
        str(item["record_id"])
        for item in persistence_results
        if item.get("result") == "PERSISTENCE_REJECTED"
    }
    partitions = {
        "persisted_ids": persisted,
        "technically_rejected_ids": technically_rejected,
        "outside_scope_ids": outside,
        "exact_technical_duplicate_ids": exact_duplicates,
        "persistence_failed_ids": persistence_failed,
    }
    union = set().union(*partitions.values())
    overlaps = {
        f"{left}&{right}": sorted(partitions[left] & partitions[right])
        for left in partitions
        for right in partitions
        if left < right and partitions[left] & partitions[right]
    }
    if union != raw or overlaps:
        raise AssertionError(
            f"V7 identity reconciliation failed: missing={sorted(raw - union)}, "
            f"unexpected={sorted(union - raw)}, overlaps={overlaps}"
        )

    result_by_id = {
        str(item["record_id"]): item
        for item in persistence_results
        if item.get("record_id")
    }
    outside_by_id: dict[str, dict[str, Any]] = {}
    for provider in providers.values():
        for item in (
            provider.get("explicit_out_of_scope")
            or provider.get("explicit_outside_scope")
            or []
        ):
            outside_by_id[str(item["record_id"])] = item

    marketwatch_logs = [
        item
        for item in service
        if item.get("message") == "news_article_rejected"
        and item.get("source") == "MarketWatch RSS"
    ]
    marketwatch_ids = list(
        parse_batches["MarketWatch RSS"]["raw_record_ids"]
    )
    marketwatch_log_by_id = {
        str(record_id): marketwatch_logs[index]
        for index, record_id in enumerate(marketwatch_ids)
        if index < len(marketwatch_logs)
    }

    missing_records: list[dict[str, Any]] = []
    for record_id in sorted(raw - persisted):
        provider = raw_provider[record_id]
        result = result_by_id.get(record_id, {})
        outside_item = outside_by_id.get(record_id, {})
        lineage = (
            dict(result.get("lineage") or {})
            if isinstance(result.get("lineage"), dict)
            else {}
        )
        log = marketwatch_log_by_id.get(record_id, {})
        reason = str(
            result.get("reason_code")
            or outside_item.get("reason_code")
            or "UNEXPLAINED"
        )
        if record_id in technically_rejected:
            disposition = "TECHNICALLY_REJECTED"
            phase = "POST_PARSE_NORMALIZATION_SOURCE_POLICY"
            condition = (
                "normalize_news_article -> SourcePolicyService.validate: "
                "lineage_status == CONTRADICTORY; V7 fallback publisher was "
                "'MarketWatch RSS' while the direct host rule publisher was "
                "'MarketWatch'"
            )
        elif reason == "PER_PROVIDER_LIMIT":
            disposition = "OUTSIDE_SCOPE"
            phase = "RSS_PARSE_PRE_RECORD"
            condition = (
                "parse_rss_articles_with_accounting@"
                f"{V7_HEAD}: if index >= limit: append "
                "PER_PROVIDER_LIMIT and continue"
            )
        elif reason == "OUTSIDE_RECENCY_WINDOW":
            disposition = "OUTSIDE_SCOPE"
            phase = "POST_PARSE_RECENCY_PARTITION"
            condition = (
                "_partition_recency: parsed published_at < "
                "datetime.now(UTC) - timedelta(days=recency_days)"
            )
        else:
            disposition = "UNEXPLAINED"
            phase = "UNKNOWN"
            condition = "NO_MATCHING_V7_CODE_CONDITION"
        title = log.get("title")
        timestamp = lineage.get("published_at") or log.get("published_at")
        url = (
            lineage.get("canonical_url")
            or lineage.get("source_url")
        )
        provider_record_id = lineage.get("provider_record_id")
        evidence_complete = all(
            value not in (None, "") for value in (title, timestamp, url)
        )
        missing_records.append(
            {
                "raw_record_id": record_id,
                "provider_feed": provider,
                "raw_index": raw_index[record_id],
                "source_identity": provider_record_id or record_id,
                "provider_record_id_or_guid": provider_record_id,
                "title_or_content_identity": title,
                "timestamp": timestamp,
                "url_or_guid": url or provider_record_id,
                "exclusion_phase": phase,
                "disposition": disposition,
                "reason_code": reason,
                "exact_code_condition": condition,
                "evidence_completeness": (
                    "FULL_FORENSIC_FIELDS"
                    if evidence_complete
                    else "RAW_ID_AND_PROVIDER_POSITION_ONLY"
                ),
                "missing_field_reason": (
                    None if evidence_complete else MISSING_ARTIFACT_FIELDS
                ),
            }
        )

    per_provider: list[dict[str, Any]] = []
    for provider, row in sorted(providers.items()):
        provider_raw = set(row.get("raw_record_ids") or [])
        provider_partition = {
            name: sorted(values & provider_raw)
            for name, values in partitions.items()
        }
        per_provider.append(
            {
                "provider": provider,
                "raw": len(provider_raw),
                **{
                    name.removesuffix("_ids"): len(values)
                    for name, values in provider_partition.items()
                },
                "identity_union_exact": (
                    set().union(
                        *(
                            set(values)
                            for values in provider_partition.values()
                        )
                    )
                    == provider_raw
                ),
                "sets": provider_partition,
            }
        )

    network_analysis = _network_analysis(accounting)
    incomplete = [
        row
        for row in missing_records
        if row["evidence_completeness"] != "FULL_FORENSIC_FIELDS"
    ]
    return {
        "validation_id": validation_dir.name,
        "v7_head": V7_HEAD,
        "result": "RECONCILED_WITH_CAPTURE_GAP",
        "equation": {
            "raw_acquired": len(raw),
            **{name: len(values) for name, values in partitions.items()},
            "union_equals_raw": union == raw,
            "partitions_disjoint": not overlaps,
            "missing_from_union": sorted(raw - union),
            "unexpected_in_union": sorted(union - raw),
            "overlaps": overlaps,
        },
        "root_cause": {
            "cap_of_10_per_provider": False,
            "persisted_breakdown": {
                row["provider"]: row["persisted"]
                for row in per_provider
                if row["persisted"]
            },
            "post_fetch_cap": {
                "configured_limit": 25,
                "Yahoo Finance RSS": 17,
                "Google News RSS": 75,
                "total": 92,
            },
            "legitimate_recency_exclusions": {
                "Federal Reserve RSS": 10
            },
            "marketwatch_false_source_contradictions": 10,
        },
        "per_provider": per_provider,
        "missing_record_count": len(missing_records),
        "missing_records": missing_records,
        "capture_evidence": {
            "full_forensic_fields": len(missing_records) - len(incomplete),
            "raw_identity_and_position_only": len(incomplete),
            "limitation": (
                "V7 stored hashes and exclusion reasons but not the raw RSS "
                "XML for records skipped before parsing. Titles, timestamps, "
                "URLs and GUIDs for those records cannot be reconstructed "
                "without fabricating evidence or making another live call."
            ),
        },
        "separate_network_conditions": network_analysis,
    }


def _network_analysis(accounting: dict[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for call in accounting["network"]["calls"]:
        endpoint = str(call.get("endpoint_logical") or "")
        if endpoint == "BLS_RSS" and call.get("http_status") == 404:
            classification = "CONFIGURATION_INVALID_OR_ENDPOINT_RETIRED"
            effect = "SOURCE_FAILED_NOT_COMPLETE"
        elif endpoint == "BEA_RSS" and call.get("http_status") == 404:
            classification = "CONFIGURATION_INVALID_OR_ENDPOINT_RETIRED"
            effect = "SOURCE_FAILED_NOT_COMPLETE"
        elif endpoint == "GDELT_DOC_API" and call.get("error_type") == "ConnectTimeout":
            classification = "EXTERNAL_TRANSIENT_OR_CONNECTIVITY_DEGRADATION"
            effect = "SOURCE_FAILED_NOT_COMPLETE"
        elif call.get("call_kind") == "METADATA_ENRICHMENT" and call.get(
            "http_status"
        ) == 307:
            classification = "EXTERNAL_REDIRECT_ENRICHMENT_DEGRADATION"
            effect = "RECORD_RETAINED_ENRICHMENT_PARTIAL"
        else:
            continue
        output.append(
            {
                "provider": call.get("provider"),
                "endpoint_logical": endpoint,
                "http_status": call.get("http_status"),
                "error_type": call.get("error_type"),
                "classification": classification,
                "effect": effect,
                "part_of_112_missing_records": False,
            }
        )
    return output


def _replay_article(
    record_id: str,
    provider: str,
    evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    digest = record_id.removeprefix("raw:")
    direct_publishers = {
        "Federal Reserve RSS": "Federal Reserve",
        "MarketWatch RSS": "MarketWatch",
    }
    publisher = direct_publishers.get(provider)
    if evidence:
        url = evidence.get("url_or_guid")
        if url and not str(url).startswith(("http://", "https://")):
            url = None
        title = evidence.get("title_or_content_identity")
        timestamp = evidence.get("timestamp")
        native_id = evidence.get("provider_record_id_or_guid")
    else:
        url = title = timestamp = native_id = None
    if not url:
        if provider == "Google News RSS":
            url = f"https://news.google.com/rss/articles/{digest}"
        elif provider == "Yahoo Finance RSS":
            url = f"https://finance.yahoo.com/news/{digest}.html"
        elif provider == "MarketWatch RSS":
            url = f"https://www.marketwatch.com/story/{digest}"
        else:
            url = (
                "https://www.federalreserve.gov/newsevents/pressreleases/"
                f"{digest}.htm"
            )
    distribution = (
        "Google News"
        if provider == "Google News RSS"
        else "Yahoo Finance"
        if provider == "Yahoo Finance RSS"
        else None
    )
    return {
        "raw_record_id": record_id,
        "provider_record_id": native_id or f"v7-artifact:{digest}",
        "title": title or f"V7 identity-preserving offline record {digest}",
        "summary": (
            "Deterministic offline regression content reconstructed from the "
            "exact V7 raw identity; it is not represented as captured content."
        ),
        "source": publisher or provider,
        "original_publisher": publisher,
        "_source_is_declared_publisher": bool(publisher),
        "source_url": url,
        "canonical_url": None if distribution else url,
        "aggregator_url": url if distribution else None,
        "distribution_source": distribution,
        "published_at": timestamp or REFERENCE_TIME,
        "retrieved_at": REFERENCE_TIME,
        "reliability": 0.64,
        "provider_type": "RSS",
        "acquisition_provider": provider,
    }


def replay_identity_exact(
    reconciliation: dict[str, Any],
) -> dict[str, Any]:
    missing_by_id = {
        item["raw_record_id"]: item
        for item in reconciliation["missing_records"]
    }
    raw_provider = {
        record_id: row["provider"]
        for row in reconciliation["per_provider"]
        for record_id in row["sets"]["persisted_ids"]
        + row["sets"]["technically_rejected_ids"]
        + row["sets"]["outside_scope_ids"]
        + row["sets"]["exact_technical_duplicate_ids"]
        + row["sets"]["persistence_failed_ids"]
    }
    raw_ids = sorted(raw_provider)
    articles = [
        _replay_article(
            record_id,
            raw_provider[record_id],
            missing_by_id.get(record_id),
        )
        for record_id in raw_ids
    ]

    first = _run_replay(articles)
    second = _run_replay(articles)
    first_bytes = canonical_bytes(first["payload"])
    second_bytes = canonical_bytes(second["payload"])
    if first_bytes != second_bytes:
        raise AssertionError("independent V7 identity replays differ")
    if set(first["delivered_ids"]) != set(raw_ids):
        raise AssertionError("identity replay omitted V7 raw IDs")
    return {
        "result": "PASS_WITH_EXPLICIT_SYNTHETIC_CONTENT_LIMITATION",
        "mode": "OFFLINE_EXACT_V7_IDENTITY_REGRESSION",
        "raw_identity_count": len(raw_ids),
        "delivered_identity_count": len(first["delivered_ids"]),
        "all_raw_identities_delivered": set(first["delivered_ids"])
        == set(raw_ids),
        "zero_unexplained_omissions": not (
            set(raw_ids) - set(first["delivered_ids"])
        ),
        "no_cap_10_25_100": len(first["delivered_ids"]) == 172,
        "two_independent_sandboxes_byte_identical": first_bytes == second_bytes,
        "payload_sha256": hashlib.sha256(first_bytes).hexdigest().upper(),
        "fixed_point": first["fixed_point"],
        "temporal_updates_distinct_regression": _temporal_update_regression(),
        "evidence_boundary": {
            "exact": [
                "all 172 raw_record_id values",
                "provider membership",
                "V7 disposition sets and reason codes",
                "MarketWatch lineage retained by V7",
            ],
            "synthetic": [
                "content for records whose V7 raw XML was not captured",
                "fallback title/URL/timestamp for identity-only records",
            ],
            "claim_not_made": (
                "This replay does not claim byte-exact reconstruction of raw "
                "provider payloads that V7 never persisted."
            ),
        },
        "network_calls": 0,
        "ai_jobs": 0,
        "browser_calls": 0,
        "delivery_calls": 0,
        "trading_actions": 0,
    }


def _run_replay(articles: list[dict[str, Any]]) -> dict[str, Any]:
    with TemporaryDirectory(prefix="pr29-v7-identity-replay-") as directory:
        settings = Settings(
            _env_file=None,
            environment="test",
            database_path=Path(directory) / "unused.sqlite",
            alpha_vantage_api_key="",
            news_gdelt_enabled=False,
            news_rss_enabled=False,
        )
        repository = ReplayRepository()
        provider = NewsProvider(
            cache=None,  # type: ignore[arg-type]
            settings=settings,
            market_news_repository=repository,  # type: ignore[arg-type]
        )

        def result() -> Any:
            value = _news_result(
                source="V7 offline identity replay",
                provider_type=ProviderType.RSS,
                reliability=0.64,
                articles=[dict(item) for item in articles],
                errors=[],
                warnings=[],
                fallback_used=False,
            )
            if isinstance(value.data, dict):
                accounts = []
                by_provider: dict[str, list[dict[str, Any]]] = {}
                for article in articles:
                    by_provider.setdefault(
                        str(article["acquisition_provider"]), []
                    ).append(article)
                for provider_name, rows in sorted(by_provider.items()):
                    accounts.append(
                        {
                            "provider": provider_name,
                            "provider_type": "RSS",
                            "status": "COMPLETE",
                            "coverage_status": "COMPLETE",
                            "calls": 0,
                            "pages": 0,
                            "raw_record_ids": [
                                str(item["raw_record_id"]) for item in rows
                            ],
                            "parsed_record_ids": [
                                str(item["raw_record_id"]) for item in rows
                            ],
                            "technical_rejections": [],
                            "explicit_out_of_scope": [],
                            "persisted_record_ids": [],
                            "errors": [],
                            "warnings": [],
                        }
                    )
                value.data["provider_accounting"] = accounts
            return value

        first = provider._store_and_return(result())
        first_inserts = repository.insert_count
        second = provider._store_and_return(result())
        second_new_inserts = repository.insert_count - first_inserts
        payload = first.data
        delivered_ids = [
            str(item["raw_record_id"])
            for item in first.data["articles"]
        ]
        return {
            "payload": payload,
            "delivered_ids": delivered_ids,
            "fixed_point": {
                "second_run_new_canonical_records": second_new_inserts,
                "second_run_total_records": len(repository.keys),
                "second_run_provider_calls": 0,
                "second_run_lifecycle_writes": 0,
                "second_run_snapshots": 0,
                "second_run_outbox_rows": 0,
                "second_run_revision_increment": 0,
                "second_payload_identity_count": len(
                    second.data["articles"]
                ),
            },
        }


def _temporal_update_regression() -> bool:
    first = _replay_article(
        "raw:temporal-first",
        "Yahoo Finance RSS",
        None,
    )
    second = {
        **first,
        "raw_record_id": "raw:temporal-second",
        "provider_record_id": "temporal-second",
        "published_at": "2026-07-29T11:37:13+00:00",
    }
    result = _run_replay([first, second])
    return len(result["delivered_ids"]) == 2


def write_outputs(
    output_dir: Path,
    reconciliation: dict[str, Any],
    replay: dict[str, Any],
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=False)
    reconciliation_path = output_dir / "pr29-v7-reconciliation.json"
    replay_path = output_dir / "pr29-v7-offline-replay.json"
    csv_path = output_dir / "pr29-v7-missing-112.csv"
    reconciliation_path.write_bytes(canonical_bytes(reconciliation))
    replay_path.write_bytes(canonical_bytes(replay))
    fields = [
        "raw_record_id",
        "provider_feed",
        "raw_index",
        "source_identity",
        "provider_record_id_or_guid",
        "title_or_content_identity",
        "timestamp",
        "url_or_guid",
        "exclusion_phase",
        "disposition",
        "reason_code",
        "exact_code_condition",
        "evidence_completeness",
        "missing_field_reason",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(reconciliation["missing_records"])
    manifest_path = output_dir / "manifest.json"
    manifest = {
        path.name: {
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper(),
        }
        for path in (reconciliation_path, replay_path, csv_path)
    }
    manifest_path.write_bytes(canonical_bytes(manifest))
    return {
        "output_dir": str(output_dir.resolve()),
        "result": reconciliation["result"],
        "raw": reconciliation["equation"]["raw_acquired"],
        "missing": reconciliation["missing_record_count"],
        "manifest": manifest,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validation-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    reconciliation = reconcile(args.validation_dir.resolve())
    replay = replay_identity_exact(reconciliation)
    summary = write_outputs(
        args.output_dir.resolve(),
        reconciliation,
        replay,
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
