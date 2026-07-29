from __future__ import annotations

import ast
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re

import httpx
import pytest
import respx

from app.core.config import Settings
from app.models.common import ProviderType
from app.providers.news_provider import (
    DIRECT_RSS_PUBLISHERS,
    NewsProvider,
    YAHOO_METADATA_REDIRECT_HOSTS,
    _capture_raw_records,
    _capture_rss_records,
    _failed_provider_batch,
    _metadata_redirect_allowed,
    parse_alpha_vantage_news_with_accounting,
    parse_gdelt_articles_with_accounting,
    parse_rss_articles_with_accounting,
)
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.services.market_session_service import build_session_aware_schedule


REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = REPO_ROOT / "docs" / "baselines" / "news-pipeline-v1.json"
GOLDEN_PATH = REPO_ROOT / "tests" / "fixtures" / "news-pipeline-golden-v1.json"
BASELINE_SHA256 = "7142794B166F011B21C242E08B8559B12DF6F1BF5997F734C7DFDB42B2670886"


def _baseline() -> dict:
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def _golden() -> dict:
    return json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))


def test_baseline_change_requires_explicit_test_update() -> None:
    assert hashlib.sha256(BASELINE_PATH.read_bytes()).hexdigest().upper() == (
        BASELINE_SHA256
    )


def test_runtime_endpoints_match_versioned_baseline() -> None:
    endpoints = {
        item["provider"]: item["endpoint"] for item in _baseline()["providers"]
    }
    fields = Settings.model_fields
    assert fields["bls_rss_url"].default == endpoints["BLS RSS"]
    assert fields["bea_rss_url"].default == endpoints["BEA RSS"]
    assert fields["gdelt_doc_api_url"].default == endpoints["GDELT Doc API"]
    assert fields["alpha_vantage_base_url"].default == endpoints[
        "Alpha Vantage NEWS_SENTIMENT"
    ]
    assert fields["federal_reserve_rss_url"].default == endpoints[
        "Federal Reserve RSS"
    ]
    assert fields["yahoo_finance_rss_url"].default == endpoints[
        "Yahoo Finance RSS"
    ]
    assert fields["marketwatch_rss_url"].default == endpoints[
        "MarketWatch RSS"
    ]
    assert fields["google_news_rss_url"].default == endpoints[
        "Google News RSS"
    ]
    network = _baseline()["network"]
    assert fields["news_gdelt_timeout_seconds"].default == (
        network["gdelt"]["timeout_seconds"]
    )
    assert fields["news_gdelt_max_attempts"].default == (
        network["gdelt"]["max_attempts"]
    )
    assert fields["news_gdelt_retry_backoff_seconds"].default == (
        network["gdelt"]["backoff_seconds"]
    )
    assert YAHOO_METADATA_REDIRECT_HOSTS == set(
        network["metadata_redirects"]["yahoo_allowed_hosts"]
    )


def test_baseline_provider_lineage_contract_matches_runtime() -> None:
    providers = {
        item["provider"]: item for item in _baseline()["providers"]
    }
    for provider, publisher in DIRECT_RSS_PUBLISHERS.items():
        assert providers[provider]["publisher_fallback"] == publisher
    assert providers["Yahoo Finance RSS"]["publisher_fallback"] is None
    assert providers["Yahoo Finance RSS"]["distributor"] == "Yahoo Finance"
    assert providers["Google News RSS"]["publisher_fallback"] is None
    assert providers["Google News RSS"]["distributor"] == "Google News"


def test_baseline_has_required_primary_group_and_optional_accessories() -> None:
    baseline = _baseline()
    providers = {
        item["provider"]: item for item in baseline["providers"]
    }
    availability = baseline["availability"]
    assert availability["optional_providers"] == [
        "Alpha Vantage NEWS_SENTIMENT",
        "GDELT Doc API",
    ]
    group = availability["required_provider_groups"][0]
    assert group["group"] == "primary_news_feeds"
    assert group["minimum_usable_providers"] == 1
    assert group["minimum_persisted_records"] == 1
    assert set(group["providers"]) == {
        "Federal Reserve RSS",
        "BLS RSS",
        "BEA RSS",
        "Yahoo Finance RSS",
        "MarketWatch RSS",
        "Google News RSS",
    }
    assert all(
        providers[name]["availability_role"] == "PRIMARY_GROUP_MEMBER"
        for name in group["providers"]
    )
    assert providers["GDELT Doc API"]["availability_role"] == (
        "OPTIONAL_ACCESSORY"
    )
    assert providers["Yahoo Finance RSS"]["metadata_enrichment"] == {
        "required": False,
        "failure_preserves_original_record": True,
        "failure_degrades_enrichment_only": True,
    }
    assert availability["not_all_sources_are_optional"] is True
    assert baseline["accounting"][
        "provider_and_aggregate_equations_required"
    ] is True
    assert baseline["accounting"][
        "cross_provider_raw_identity_collisions_forbidden"
    ] is True


def test_no_post_fetch_record_slicing_or_named_limit_rejection() -> None:
    provider_path = REPO_ROOT / "app" / "providers" / "news_provider.py"
    source = provider_path.read_text(encoding="utf-8")
    assert "PER_PROVIDER_LIMIT" not in source
    assert re.search(r"if\s+index\s*>=\s*limit", source) is None

    tree = ast.parse(source)
    guarded_functions = {
        "fetch_for_symbols",
        "_fetch_alpha_vantage",
        "_fetch_gdelt",
        "_fetch_one_rss_feed",
        "parse_alpha_vantage_news_with_accounting",
        "parse_gdelt_articles_with_accounting",
        "parse_rss_articles_with_accounting",
    }
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name not in guarded_functions:
            continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Subscript):
                continue
            if not isinstance(child.slice, ast.Slice):
                continue
            upper = ast.unparse(child.slice.upper) if child.slice.upper else ""
            if upper and (
                "limit" in upper.casefold()
                or upper.strip() in {"10", "25", "100"}
            ):
                violations.append(f"{node.name}:{child.lineno}:{upper}")
    assert violations == []


def test_golden_bls_and_bea_documents_are_parseable_and_fully_captured() -> None:
    fixture = _golden()
    for provider in ("BLS RSS", "BEA RSS"):
        text = fixture["rss"][provider]
        capture = _capture_rss_records(provider, text)
        articles, warnings, rejected, outside, raw_ids = (
            parse_rss_articles_with_accounting(
                text,
                symbols=fixture["symbols"],
                limit=1,
                source_name=provider,
                reliability=0.86,
            )
        )
        assert warnings == []
        assert rejected == []
        assert outside == []
        assert [item["record_id"] for item in capture] == raw_ids
        assert len(articles) == len(capture)
        assert all(item["captured_before_parsing"] for item in capture)


def test_golden_raw_capture_precedes_parsing_and_exact_accounting_holds() -> None:
    fixture = _golden()
    partitions: list[tuple[set[str], set[str], set[str]]] = []

    alpha_payload = fixture["alpha_vantage"]
    alpha_capture = _capture_raw_records(
        "Alpha Vantage NEWS_SENTIMENT",
        alpha_payload["feed"],
    )
    alpha, alpha_rejected, alpha_outside, alpha_raw = (
        parse_alpha_vantage_news_with_accounting(
            alpha_payload,
            fixture["symbols"],
            limit=1,
        )
    )
    assert [item["record_id"] for item in alpha_capture] == alpha_raw
    partitions.append(
        (
            set(alpha_raw),
            {str(item["raw_record_id"]) for item in alpha},
            {str(item["record_id"]) for item in alpha_rejected},
        )
    )
    assert alpha_outside == []

    gdelt_payload = fixture["gdelt"]
    gdelt_capture = _capture_raw_records(
        "GDELT Doc API",
        gdelt_payload["articles"],
    )
    gdelt, gdelt_rejected, gdelt_outside, gdelt_raw = (
        parse_gdelt_articles_with_accounting(
            gdelt_payload,
            fixture["symbols"],
            limit=1,
        )
    )
    assert [item["record_id"] for item in gdelt_capture] == gdelt_raw
    partitions.append(
        (
            set(gdelt_raw),
            {str(item["raw_record_id"]) for item in gdelt},
            {str(item["record_id"]) for item in gdelt_rejected},
        )
    )
    assert gdelt_outside == []

    for provider, text in fixture["rss"].items():
        capture = _capture_rss_records(provider, text)
        articles, _, rejected, outside, raw_ids = (
            parse_rss_articles_with_accounting(
                text,
                symbols=fixture["symbols"],
                limit=1,
                source_name=provider,
                reliability=0.86,
            )
        )
        assert [item["record_id"] for item in capture] == raw_ids
        partitions.append(
            (
                set(raw_ids),
                {str(item["raw_record_id"]) for item in articles},
                {str(item["record_id"]) for item in rejected},
            )
        )
        assert outside == []

    raw = set().union(*(item[0] for item in partitions))
    parsed = set().union(*(item[1] for item in partitions))
    rejected = set().union(*(item[2] for item in partitions))
    assert raw == parsed | rejected
    assert parsed.isdisjoint(rejected)
    assert len(raw) == fixture["expected"]["raw_record_count"]
    assert len(parsed) == fixture["expected"]["parsed_record_count"]
    assert len(rejected) == fixture["expected"]["technically_rejected_count"]


def test_production_raw_capture_call_precedes_each_semantic_parser() -> None:
    provider_path = REPO_ROOT / "app" / "providers" / "news_provider.py"
    tree = ast.parse(provider_path.read_text(encoding="utf-8"))
    expected = {
        "_fetch_alpha_vantage": (
            "_capture_raw_records",
            "parse_alpha_vantage_news_with_accounting",
        ),
        "_fetch_gdelt": (
            "_capture_raw_records",
            "parse_gdelt_articles_with_accounting",
        ),
        "_fetch_one_rss_feed": (
            "_capture_rss_records",
            "parse_rss_articles_with_accounting",
        ),
    }
    found: dict[str, tuple[int, int]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name not in expected:
            continue
        capture_name, parser_name = expected[node.name]
        capture_lines: list[int] = []
        parser_lines: list[int] = []
        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue
            name = ast.unparse(child.func)
            if name == capture_name:
                capture_lines.append(child.lineno)
            if name == parser_name:
                parser_lines.append(child.lineno)
        assert len(capture_lines) == 1
        assert len(parser_lines) == 1
        found[node.name] = (capture_lines[0], parser_lines[0])
    assert set(found) == set(expected)
    assert all(capture < parser for capture, parser in found.values())


def test_marketwatch_and_yahoo_lineage_remain_distinct() -> None:
    fixture = _golden()
    marketwatch, _ = _parse_rss_fixture(fixture, "MarketWatch RSS")
    assert marketwatch[0]["original_publisher"] == "MarketWatch"
    assert marketwatch[0]["acquisition_provider"] == "MarketWatch RSS"

    yahoo, _ = _parse_rss_fixture(fixture, "Yahoo Finance RSS")
    assert yahoo[0]["original_publisher"] == "Reuters"
    assert yahoo[0]["distribution_source"] == "Yahoo Finance"
    assert yahoo[0]["acquisition_provider"] == "Yahoo Finance RSS"
    assert yahoo[1]["original_publisher"] is None
    assert yahoo[1]["distribution_source"] == "Yahoo Finance"


def test_failed_source_can_never_claim_complete_coverage() -> None:
    batch = _failed_provider_batch(
        provider="BLS RSS",
        provider_type=ProviderType.RSS,
        reliability=0.86,
        limit=250,
        error=httpx.ConnectTimeout("offline fixture timeout"),
    )
    assert batch["status"] == "FAILED"
    assert batch["coverage_status"] == "FAILED"
    assert batch["pagination_complete"] is False
    assert batch["raw_capture_status"] == "NOT_ACQUIRED"


def test_yahoo_metadata_redirect_allowlist_is_fail_closed() -> None:
    assert _metadata_redirect_allowed(
        provider="Yahoo Finance RSS",
        original_url="https://finance.yahoo.com/technology/articles/a.html",
        redirect_url="https://finance.yahoo.com/news/a.html",
    )
    assert not _metadata_redirect_allowed(
        provider="Yahoo Finance RSS",
        original_url="https://finance.yahoo.com/technology/articles/a.html",
        redirect_url="https://example.com/news/a.html",
    )
    assert not _metadata_redirect_allowed(
        provider="Yahoo Finance RSS",
        original_url="https://finance.yahoo.com/technology/articles/a.html",
        redirect_url="http://finance.yahoo.com/news/a.html",
    )


@pytest.mark.asyncio
async def test_gdelt_timeout_retry_policy_is_bounded_and_accounted(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "market.sqlite",
        alpha_vantage_api_key="",
        news_rss_enabled=False,
        gdelt_doc_api_url="https://gdelt.test/api",
        news_gdelt_timeout_seconds=8.0,
        news_gdelt_max_attempts=3,
        news_gdelt_retry_backoff_seconds=0,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
    )
    async with httpx.AsyncClient() as client:
        with respx.mock(
            assert_all_called=True,
            assert_all_mocked=True,
        ) as router:
            route = router.get("https://gdelt.test/api").mock(
                side_effect=[
                    httpx.ConnectTimeout("first"),
                    httpx.ConnectTimeout("second"),
                    httpx.Response(200, json={"articles": []}),
                ]
            )
            batch = await provider._fetch_gdelt(
                client=client,
                symbols=["QQQ"],
                query="QQQ Nasdaq",
                limit=25,
                recency_days=30,
            )
    assert route.call_count == 3
    assert batch["calls"] == 3
    assert batch["retry_count"] == 2
    assert batch["status"] == "COMPLETE"
    assert batch["raw_capture"] == []


@pytest.mark.asyncio
async def test_gdelt_retry_exhaustion_is_temporary_and_never_complete(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "market.sqlite",
        alpha_vantage_api_key="",
        news_rss_enabled=False,
        gdelt_doc_api_url="https://gdelt.test/api",
        news_gdelt_max_attempts=3,
        news_gdelt_retry_backoff_seconds=0,
    )
    provider = NewsProvider(
        ProviderCacheRepository(tmp_path / "cache.sqlite3"),
        settings,
    )
    async with httpx.AsyncClient() as client:
        with respx.mock(
            assert_all_called=True,
            assert_all_mocked=True,
        ) as router:
            route = router.get("https://gdelt.test/api").mock(
                side_effect=httpx.ConnectTimeout("offline timeout")
            )
            batch = await provider._fetch_gdelt(
                client=client,
                symbols=["QQQ"],
                query="QQQ Nasdaq",
                limit=25,
                recency_days=30,
            )
    assert route.call_count == 3
    assert batch["calls"] == 3
    assert batch["retry_count"] == 2
    assert batch["status"] == "TEMPORARILY_UNAVAILABLE"
    assert batch["availability_status"] == "TEMPORARILY_UNAVAILABLE"
    assert batch["coverage_status"] == "FAILED"
    assert batch["pagination_complete"] is False
    assert batch["temporary"] is True
    assert batch["reason_code"] == (
        "GDELT_DOC_API_CONNECT_TIMEOUT_RETRY_EXHAUSTED"
    )
    assert batch["raw_record_ids"] == []


def test_cme_crosscheck_unavailable_is_partial_not_quarantined_offline() -> None:
    contract = _baseline()["related_contract_checks"]
    assert contract["official_cme_crosscheck"] == (
        "OPTIONAL_AUTHORITATIVE_VERIFICATION"
    )
    schedule = build_session_aware_schedule(
        {
            "cme_calendar": {
                "status": "timeout",
                "official_document_discovered": False,
                "official_schedule_parsed": False,
            }
        },
        now=datetime(2026, 7, 27, 9, 40, tzinfo=UTC),
    )
    assert schedule["status"] == "PARTIAL"
    assert schedule["validation"]["status"] == "partial"
    assert schedule["mnq_session"]["session_state"] == "GLOBEX_OPEN"
    assert schedule["mnq_session"]["calendar_crosscheck_status"] == "timeout"
    assert schedule["quarantined_holidays"] == []
    assert schedule["validation"]["status"] != "quarantined"


def test_temporary_stage_scripts_are_not_part_of_permanent_harness() -> None:
    forbidden = re.compile(
        r"(?:stage2-v\d+|controlled-live-news-validation-stage)",
        re.IGNORECASE,
    )
    tracked_surfaces = [
        path
        for root in (REPO_ROOT / "scripts", REPO_ROOT / "tests")
        for path in root.rglob("*")
        if path.is_file()
    ]
    assert [
        str(path.relative_to(REPO_ROOT))
        for path in tracked_surfaces
        if forbidden.search(path.name)
    ] == []


def _parse_rss_fixture(
    fixture: dict,
    provider: str,
) -> tuple[list[dict[str, object]], list[str]]:
    articles, warnings, rejected, outside, _ = (
        parse_rss_articles_with_accounting(
            fixture["rss"][provider],
            symbols=fixture["symbols"],
            limit=1,
            source_name=provider,
            reliability=0.86,
        )
    )
    assert rejected == []
    assert outside == []
    return articles, warnings
