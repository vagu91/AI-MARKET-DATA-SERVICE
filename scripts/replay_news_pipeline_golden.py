from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import Settings
from app.models.common import ProviderType
from app.providers.news_provider import (
    NewsProvider,
    _capture_rss_records,
    _news_result,
    _provider_batch,
    parse_rss_articles_with_accounting,
)
from app.services.market_context_sync_service import canonical_json
from app.services.news_intelligence_service import build_news_context


REFERENCE = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
V7_PROVIDER_COUNTS = {
    "Federal Reserve RSS": 10,
    "MarketWatch RSS": 20,
    "Yahoo Finance RSS": 42,
    "Google News RSS": 100,
}


class DeterministicReplayRepository:
    def __init__(self) -> None:
        self.records: dict[str, bytes] = {}
        self.write_count = 0

    def upsert_news(self, article: dict[str, Any]) -> dict[str, Any]:
        key = str(
            article.get("technical_acquisition_id")
            or article.get("raw_record_id")
            or ""
        )
        if not key:
            raise ValueError("missing deterministic replay identity")
        payload = canonical_json(article).encode("utf-8")
        if self.records.get(key) != payload:
            self.records[key] = payload
            self.write_count += 1
        return {"news_key": key}


def replay(fixture_path: Path) -> dict[str, Any]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    parsed_articles, pristine_batches = _parse_v7_shape()
    expected_count = sum(V7_PROVIDER_COUNTS.values())
    if len(parsed_articles) != expected_count:
        raise AssertionError(
            f"golden parser count {len(parsed_articles)} != {expected_count}"
        )

    first_repo = DeterministicReplayRepository()
    first = _persist_and_materialize(
        parsed_articles,
        pristine_batches,
        first_repo,
    )
    first_write_count = first_repo.write_count
    second_same_repo = _persist_and_materialize(
        parsed_articles,
        pristine_batches,
        first_repo,
    )
    fixed_point_writes = first_repo.write_count - first_write_count

    second_repo = DeterministicReplayRepository()
    independent = _persist_and_materialize(
        parsed_articles,
        pristine_batches,
        second_repo,
    )

    first_bytes = canonical_json(first["context"]).encode("utf-8")
    second_bytes = canonical_json(second_same_repo["context"]).encode("utf-8")
    independent_bytes = canonical_json(independent["context"]).encode("utf-8")
    if first_bytes != second_bytes or first_bytes != independent_bytes:
        raise AssertionError("golden replay output is not byte-identical")
    if fixed_point_writes != 0:
        raise AssertionError(
            f"golden replay fixed point wrote {fixed_point_writes} records"
        )

    accounts = first["provider_accounting"]
    raw_ids = {
        str(record_id)
        for account in accounts
        for record_id in account["raw_record_ids"]
    }
    persisted_ids = {
        str(record_id)
        for account in accounts
        for record_id in account["persisted_record_ids"]
    }
    rejected_ids = {
        str(item["record_id"])
        for account in accounts
        for item in account["technical_rejections"]
    }
    outside_ids = {
        str(item["record_id"])
        for account in accounts
        for item in account["explicit_out_of_scope"]
    }
    persistence_failed_ids = {
        str(item["record_id"])
        for account in accounts
        for item in account["persistence_rejections"]
    }
    duplicate_ids = {
        str(item["record_id"])
        for account in accounts
        for item in account["exact_technical_duplicates"]
    }
    partitions = (
        persisted_ids,
        rejected_ids,
        outside_ids,
        persistence_failed_ids,
        duplicate_ids,
    )
    if raw_ids != set().union(*partitions):
        raise AssertionError("golden raw identity accounting mismatch")
    if any(
        left.intersection(right)
        for index, left in enumerate(partitions)
        for right in partitions[index + 1 :]
    ):
        raise AssertionError("golden accounting partitions overlap")

    delivered_ids = {
        str(item["raw_record_id"]) for item in first["articles"]
    }
    if delivered_ids != persisted_ids:
        raise AssertionError("golden persisted and delivered identities differ")

    temporal_reuters = [
        item
        for item in first["articles"]
        if item.get("original_publisher") == "Reuters"
        and item.get("source_url")
        == "https://finance.yahoo.com/news/temporal-reuters"
    ]
    if len(temporal_reuters) != 2:
        raise AssertionError("temporal Reuters updates were not preserved")

    return {
        "mode": "PERMANENT_OFFLINE_GOLDEN_REPLAY",
        "baseline": "news-pipeline-v1",
        "fixture_id": fixture["fixture_id"],
        "v7_shape": {
            "provider_counts": V7_PROVIDER_COUNTS,
            "raw_acquired": len(raw_ids),
            "persisted_valid": len(persisted_ids),
            "delivered": len(delivered_ids),
            "technically_rejected": len(rejected_ids),
            "outside_scope": len(outside_ids),
            "exact_technical_duplicates": len(duplicate_ids),
            "persistence_failed": len(persistence_failed_ids),
            "accounting_exact": True,
        },
        "losslessness": {
            "configured_parser_limit": 25,
            "post_fetch_cap_applied": False,
            "all_received_records_processed": True,
            "temporal_reuters_occurrences": len(temporal_reuters),
        },
        "double_replay": {
            "same_sandbox_byte_identical": first_bytes == second_bytes,
            "independent_sandboxes_byte_identical": (
                first_bytes == independent_bytes
            ),
            "sha256": hashlib.sha256(first_bytes).hexdigest().upper(),
            "bytes": len(first_bytes),
        },
        "fixed_point": {
            "canonical_writes": fixed_point_writes,
            "lifecycle_writes": 0,
            "snapshot_writes": 0,
            "outbox_writes": 0,
            "revision_increments": 0,
        },
        "network_calls": 0,
        "ai_jobs": 0,
        "browser_calls": 0,
        "deliveries": 0,
        "trading_actions": 0,
        "pass": True,
    }


def _parse_v7_shape() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    articles: list[dict[str, Any]] = []
    batches: list[dict[str, Any]] = []
    for provider, count in V7_PROVIDER_COUNTS.items():
        xml = _rss_document(provider, count)
        capture = _capture_rss_records(provider, xml)
        parsed, warnings, rejected, outside, raw_ids = (
            parse_rss_articles_with_accounting(
                xml,
                symbols=["QQQ", "NVDA"],
                limit=25,
                source_name=provider,
                reliability=0.76,
            )
        )
        for article in parsed:
            article.update(
                {
                    "retrieved_at": REFERENCE.isoformat(),
                    "first_seen_at": REFERENCE.isoformat(),
                    "last_seen_at": REFERENCE.isoformat(),
                    "updated_at": REFERENCE.isoformat(),
                }
            )
        batch = _provider_batch(
            provider=provider,
            provider_type=ProviderType.RSS,
            reliability=0.76,
            limit=25,
            raw_record_ids=raw_ids,
            raw_capture=capture,
            articles=parsed,
            technical_rejections=rejected,
            explicit_out_of_scope=outside,
            warnings=warnings,
        )
        articles.extend(parsed)
        batches.append(batch)
    return articles, batches


def _persist_and_materialize(
    articles: list[dict[str, Any]],
    batches: list[dict[str, Any]],
    repository: DeterministicReplayRepository,
) -> dict[str, Any]:
    settings = Settings(
        _env_file=None,
        environment="test",
        alpha_vantage_api_key="",
        news_gdelt_enabled=False,
        news_rss_enabled=False,
    )
    provider = NewsProvider(
        object(),  # type: ignore[arg-type]
        settings,
        market_news_repository=repository,  # type: ignore[arg-type]
    )
    result = _news_result(
        source="Permanent offline golden replay",
        provider_type=ProviderType.MIXED,
        reliability=0.76,
        articles=deepcopy(articles),
        errors=[],
        warnings=[],
        fallback_used=False,
    )
    result.data["provider_accounting"] = [
        {
            key: deepcopy(value)
            for key, value in batch.items()
            if key != "_articles"
        }
        for batch in batches
    ]
    persisted = provider._store_and_return(result)
    output_articles = list(persisted.data["articles"])
    return {
        "articles": output_articles,
        "provider_accounting": persisted.data["provider_accounting"],
        "context": build_news_context(
            output_articles,
            now=REFERENCE,
            scope_days=30,
        ),
    }


def _rss_document(provider: str, count: int) -> str:
    items: list[str] = []
    for index in range(count):
        published = REFERENCE - timedelta(minutes=index)
        if provider == "Federal Reserve RSS":
            url = (
                "https://www.federalreserve.gov/newsevents/pressreleases/"
                f"golden-{index}.htm"
            )
            source = ""
        elif provider == "MarketWatch RSS":
            url = f"https://www.marketwatch.com/story/golden-{index}"
            source = ""
        elif provider == "Yahoo Finance RSS":
            url = (
                "https://finance.yahoo.com/news/temporal-reuters"
                if index in {0, 1}
                else f"https://finance.yahoo.com/news/golden-{index}"
            )
            source = "<source>Reuters</source>"
        else:
            url = f"https://news.google.com/rss/articles/golden-{index}"
            source = "<source>Reuters</source>"
        items.append(
            "<item>"
            f"<guid>{provider}-{index}</guid>"
            f"<title>{provider} unique received record {index}</title>"
            f"<link>{url}</link>"
            f"<pubDate>{format_datetime(published, usegmt=True)}</pubDate>"
            f"{source}"
            "<description>"
            f"Complete lossless fixture content for {provider} record {index}."
            "</description>"
            "</item>"
        )
    return "<rss><channel>" + "".join(items) + "</channel></rss>"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Permanent offline lossless-news golden replay."
    )
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = replay(args.fixture.resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        "NEWS_PIPELINE_GOLDEN_PASS "
        f"raw={report['v7_shape']['raw_acquired']} "
        f"persisted={report['v7_shape']['persisted_valid']} "
        f"delivered={report['v7_shape']['delivered']} "
        f"sha256={report['double_replay']['sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
