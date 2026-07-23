from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.agentic_research_runtime import AgenticResearchRuntime
from app.services.ai_research_job_executor import build_agentic_research_prompt
from app.services.ai_research_job_service import AIResearchJobService
from app.services.research_backend import ResearchBackend, ResearchBackendResult
from app.services.research_metrics_service import ResearchMetricsService
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.research_source_gateway import (
    ResearchSourceGateway,
    match_evidence,
)
from app.services.research_tool_telemetry import normalize_codex_event
from app.services.source_policy_service import SourcePolicyService


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "source_policy.json"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
PUBLIC_IP = ["93.184.216.34"]


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": True,
        "ai_research_web_access_enabled": True,
        "research_single_invocation_enabled": True,
        "research_gateway_min_text_chars": 20,
        "research_gateway_timeout_seconds": 2,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def ensure_run(
    cfg: Settings,
    identity: str,
) -> tuple[ResearchRuntimeRepository, dict[str, Any], dict[str, Any]]:
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id=identity,
        request_payload={
            "database_context": {
                "data_as_of": "2026-07-23T10:00:00Z",
                "large_rows": [{"raw_payload": "x" * 20_000, "topic": "macro"} for _ in range(5)],
            },
            "context_date": "2026-07-23",
            "identity": identity,
        },
        force=True,
    )
    assert created
    repository = ResearchRuntimeRepository(cfg)
    run = repository.ensure_run(
        job,
        "MNQ_MARKET_RESEARCH",
        "mnq_market_research_v1",
    )
    return repository, job, run


def gateway(
    cfg: Settings,
    repository: ResearchRuntimeRepository,
    handler,
    *,
    resolver=lambda _host: PUBLIC_IP,
) -> ResearchSourceGateway:
    return ResearchSourceGateway(
        cfg,
        repository=repository,
        transport=httpx.MockTransport(handler),
        resolver=resolver,
        now=lambda: datetime(2026, 7, 23, 10, 0, tzinfo=UTC),
    )


def html_response(
    request: httpx.Request,
    text: str,
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(
        status,
        text=text,
        headers={"content-type": "text/html", **(headers or {})},
        request=request,
    )


class FakeResearchBackend:
    backend_name = "fake_contract"

    def __init__(self, payload: dict[str, Any], usage: dict[str, Any] | None = None):
        self.payload = payload
        self.usage = usage or {
            "input_tokens": 100,
            "output_tokens": 20,
            "cached_tokens": 10,
            "reasoning_tokens": 4,
        }
        self.calls = 0

    def execute_research(self, **kwargs: Any) -> ResearchBackendResult:
        self.calls += 1
        raw = {
            "type": "item.completed",
            "item": {
                "id": "search-single-invocation",
                "type": "web_search",
                "query": "site:bls.gov June 2026 unemployment rate",
                "results": [
                    {
                        "url": "https://www.bls.gov/news.release/empsit.nr0.htm",
                        "title": "Employment Situation",
                    }
                ],
            },
        }
        return ResearchBackendResult(
            invocation_id="rinvoke-single",
            backend=self.backend_name,
            purpose="agentic_research",
            payload=self.payload,
            usage=self.usage,
            tool_events=tuple(normalize_codex_event(raw, step_name="SEARCH")),
            duration_ms=1250,
        )


def official_payload(evidence_text: str) -> dict[str, Any]:
    url = "https://www.bls.gov/news.release/empsit.nr0.htm"
    return {
        "status": "COMPLETED",
        "plan": {
            "topics": ["macro"],
            "queries": [
                {
                    "query": "site:bls.gov June 2026 unemployment rate",
                    "purpose": "official labor evidence",
                    "topic": "macro",
                }
            ],
            "stop_conditions": ["official source found"],
        },
        "searches": [
            {
                "query": "site:bls.gov June 2026 unemployment rate",
                "discovered_urls": [url],
            }
        ],
        "acquisition_requests": [
            {
                "source_url": url,
                "title": "Employment Situation",
                "publisher": "U.S. Bureau of Labor Statistics",
                "published_at": None,
            }
        ],
        "claims": [
            {
                "claim_ref": "unemployment-june-2026",
                "topic": "macro",
                "field_semantics": "outcome",
                "value": "4.2",
                "metric_id": "unemployment_rate",
                "period": "2026-06",
                "frequency": "monthly",
                "unit": "percent",
                "event_key": None,
                "symbol": "MNQ",
                "valid_from": None,
                "valid_until": None,
                "published_at": None,
                "retrieved_at": None,
                "confidence": 0.95,
                "topic_status": "SUPPORTED",
                "evidence": [
                    {
                        "query": "site:bls.gov June 2026 unemployment rate",
                        "source_url": url,
                        "canonical_url": url,
                        "publisher": "U.S. Bureau of Labor Statistics",
                        "evidence_text": evidence_text,
                        "published_at": None,
                        "retrieved_at": None,
                    }
                ],
                "warnings": [],
            }
        ],
        "warnings": [],
    }


def test_live_redacted_jsonl_shapes_preserve_bounded_operational_fields() -> None:
    events = []
    for line in (
        (FIXTURES / "codex_mnq_live_redacted.jsonl").read_text(encoding="utf-8").splitlines()
    ):
        events.extend(normalize_codex_event(json.loads(line), step_name="SEARCH"))
    completed_search = next(
        item
        for item in events
        if item["semantic_action"] == "search" and item["lifecycle"] == "completed"
    )
    assert completed_search["raw_event_type"] == "item.completed"
    assert completed_search["raw_shape"]["item_keys"]
    assert completed_search["provider_payload"]["action"]["type"] == "search"
    assert completed_search["discovered_urls"] == []
    usage = next(item["usage"] for item in events if item["usage"])
    assert usage == {
        "input_tokens": 587707,
        "output_tokens": 1646,
        "cached_tokens": 382464,
        "reasoning_tokens": 0,
        "total_tokens": 589353,
    }


def test_single_invocation_prompt_uses_bounded_database_inventory(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    _repository, job, run = ensure_run(cfg, "bounded-agent-prompt")
    profile = {
        "profile_id": "MNQ_MARKET_RESEARCH",
        "required_topics": ["macro"],
        "database_context": {"must_not": "be forwarded"},
    }
    prompt = build_agentic_research_prompt(
        job,
        run,
        profile,
        {
            "budget_mode": "observe",
            "max_searches": 8,
            "max_opened_sources": 12,
        },
    )
    assert len(prompt) < 60_000
    assert "x" * 1_000 not in prompt
    assert "must_not" not in prompt
    assert "service independently acquires and verifies" in prompt


def test_url_fetch_hash_verify_persist_and_partial_uses_one_invocation(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository, job, _run = ensure_run(cfg, "single-agent-partial")
    evidence = "The unemployment rate was 4.2 percent in June 2026."

    def handler(request: httpx.Request) -> httpx.Response:
        return html_response(
            request,
            f"<html><body><main>{evidence} This is an official release.</main></body></html>",
        )

    source_gateway = gateway(cfg, repository, handler)
    backend = FakeResearchBackend(official_payload(evidence))
    runtime = AgenticResearchRuntime(
        cfg,
        repository=repository,
        source_gateway=source_gateway,
    )
    result = runtime.run(job, tmp_path / "jobs" / job["job_id"], backend, 120)
    assert isinstance(backend, ResearchBackend)
    assert backend.calls == 1
    assert result["status"] == "PARTIAL"
    assert result["persisted_count"] == result["read_back_count"] == 1
    assert result["accepted_count"] == 1
    sources = repository.research_sources(result["run_id"])
    assert len(sources) == 1
    assert sources[0]["fetch_status"] == "FETCHED"
    assert sources[0]["verification_status"] == "VERIFIED"
    assert sources[0]["http_status"] == 200
    assert sources[0]["content_type"] == "text/html"
    assert len(sources[0]["content_sha256"]) == 64
    with connect_sqlite(cfg.database_path) as conn:
        invocation_count = conn.execute(
            "SELECT COUNT(*) FROM research_backend_invocations"
        ).fetchone()[0]
        evidence_row = conn.execute(
            """
            SELECT source_id,verification_id,verification_method,
                   verification_reason,source_content_hash
            FROM research_evidence
            """
        ).fetchone()
    assert invocation_count == 1
    assert evidence_row[0] == sources[0]["source_id"]
    assert evidence_row[1].startswith("rverify-")
    assert evidence_row[2] == "exact_normalized"
    assert evidence_row[3] == "verified_exact_normalized_match"
    assert evidence_row[4] == sources[0]["content_sha256"]
    assert result["metrics"]["backend"]["invocations"] == 1
    assert result["metrics"]["sources"]["verified"] == 1


def test_real_no_data_when_fetched_content_does_not_support_anchor(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository, job, _run = ensure_run(cfg, "real-no-data")

    def handler(request: httpx.Request) -> httpx.Response:
        return html_response(
            request,
            "<html><body>This official page contains a release calendar only.</body></html>",
        )

    backend = FakeResearchBackend(
        official_payload("The unemployment rate was 4.2 percent in June 2026.")
    )
    result = AgenticResearchRuntime(
        cfg,
        repository=repository,
        source_gateway=gateway(cfg, repository, handler),
    ).run(job, tmp_path / "jobs" / job["job_id"], backend, 120)
    assert result["status"] == "NO_DATA"
    assert result["accepted_count"] == 0
    assert result["candidate_count"] == 1
    assert "evidence_mismatch" in result["rejected_claims"][0]["warnings"]
    assert result["metrics"]["sources"]["verified"] == 0


def test_tier_one_needs_one_confirmation_but_news_needs_two(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = ensure_run(cfg, "confirmation-policy")
    published = "2026-07-23T09:30:00Z"
    pages = {
        "www.bls.gov": "Official labor outcome confirmed by the agency.",
        "www.reuters.com": "Technology policy announcement moved markets today.",
        "apnews.com": "A separate report confirms the technology policy announcement.",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return html_response(
            request,
            f"<html><body>{pages[request.url.host]} Additional reporting text.</body></html>",
        )

    source_gateway = gateway(cfg, repository, handler)
    requests = [
        {
            "source_url": "https://www.bls.gov/official",
            "publisher": "U.S. Bureau of Labor Statistics",
        },
        {
            "source_url": "https://www.reuters.com/technology/policy",
            "publisher": "Reuters",
        },
        {
            "source_url": "https://apnews.com/article/policy",
            "publisher": "Associated Press",
        },
    ]
    source_gateway.acquire_many(run["run_id"], requests)
    claims = [
        {
            "claim_ref": "tier-one",
            "topic": "macro",
            "field_semantics": "outcome",
            "value": "confirmed",
            "confidence": 0.9,
            "evidence": [
                {
                    "source_url": requests[0]["source_url"],
                    "publisher": requests[0]["publisher"],
                    "evidence_text": pages["www.bls.gov"],
                }
            ],
        },
        {
            "claim_ref": "news-two",
            "topic": "news",
            "field_semantics": "news",
            "value": "policy announcement",
            "published_at": published,
            "confidence": 0.8,
            "evidence": [
                {
                    "source_url": requests[1]["source_url"],
                    "publisher": requests[1]["publisher"],
                    "evidence_text": pages["www.reuters.com"],
                    "published_at": published,
                },
                {
                    "source_url": requests[2]["source_url"],
                    "publisher": requests[2]["publisher"],
                    "evidence_text": pages["apnews.com"],
                    "published_at": published,
                },
            ],
        },
    ]
    verified = source_gateway.verify_claims(run["run_id"], claims)
    persisted = repository.persist_claims(
        repository.get_run(run["run_id"]) or run,
        verified,
    )
    assert persisted["accepted_count"] == 2
    assert {item["topic"] for item in persisted["accepted_claims"]} == {
        "macro",
        "news",
    }

    _repository2, _job2, run2 = ensure_run(cfg, "news-one-source")
    source_gateway.acquire(run2["run_id"], requests[1])
    one_news = source_gateway.verify_claims(run2["run_id"], [claims[1]])[0]
    one_news["evidence"] = one_news["evidence"][:1]
    rejected = repository.persist_claims(
        repository.get_run(run2["run_id"]) or run2,
        [one_news],
    )
    assert rejected["status"] == "NO_DATA"
    assert "insufficient_independent_evidence" in rejected["rejected_claims"][0]["warnings"]
    fred = SourcePolicyService(POLICY).validate(
        {
            "source_url": "https://fred.stlouisfed.org/series/UNRATE",
            "publisher": "Federal Reserve Bank of St. Louis",
        },
        field_semantics="outcome",
    )
    assert fred.accepted and fred.tier == 1


def test_redirect_canonical_dedup_and_dynamic_html(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = ensure_run(cfg, "redirect-dedup")
    body = (
        '<html><head><link rel="canonical" href="/canonical"></head>'
        "<body>Official release content with enough stable visible text.</body></html>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/start":
            return httpx.Response(
                302,
                headers={"location": "https://www.bls.gov/final"},
                request=request,
            )
        if request.url.path == "/dynamic":
            return html_response(
                request,
                "<html><body><script>renderEverything()</script></body></html>",
            )
        return html_response(request, body)

    source_gateway = gateway(cfg, repository, handler)
    redirected = source_gateway.acquire(
        run["run_id"],
        {"source_url": "https://www.bls.gov/start"},
    )
    duplicate = source_gateway.acquire(
        run["run_id"],
        {"source_url": "https://www.bls.gov/copy"},
    )
    dynamic = source_gateway.acquire(
        run["run_id"],
        {"source_url": "https://www.bls.gov/dynamic"},
    )
    assert redirected["final_url"] == "https://www.bls.gov/final"
    assert redirected["canonical_url"] == "https://www.bls.gov/canonical"
    assert redirected["redirect_chain"] == ["https://www.bls.gov/final"]
    assert duplicate["duplicate_of_source_id"] == redirected["source_id"]
    assert dynamic["fetch_status"] == "REJECTED"
    assert dynamic["rejection_reason"] == "insufficient_static_text"


def test_json_pdf_block_timeout_and_ssrf_are_persisted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = ensure_run(cfg, "formats-errors")

    class FakePdfReader:
        def __init__(self, _stream):
            self.pages = [
                SimpleNamespace(
                    extract_text=lambda: "PDF official release contains normalized evidence text."
                )
            ]

    monkeypatch.setattr(
        "app.services.research_source_gateway.PdfReader",
        FakePdfReader,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(".json"):
            return httpx.Response(
                200,
                json={"release": "JSON official release contains evidence text."},
                request=request,
            )
        if request.url.path.endswith(".pdf"):
            return httpx.Response(
                200,
                content=b"%PDF-redacted-fixture",
                headers={"content-type": "application/pdf"},
                request=request,
            )
        if request.url.path == "/blocked":
            return html_response(request, "Forbidden", status=403)
        raise httpx.ReadTimeout("fixture timeout", request=request)

    source_gateway = gateway(cfg, repository, handler)
    json_source = source_gateway.acquire(
        run["run_id"],
        {"source_url": "https://www.bls.gov/release.json"},
    )
    pdf_source = source_gateway.acquire(
        run["run_id"],
        {"source_url": "https://www.bea.gov/release.pdf"},
    )
    blocked = source_gateway.acquire(
        run["run_id"],
        {"source_url": "https://www.census.gov/blocked"},
    )
    timed_out = source_gateway.acquire(
        run["run_id"],
        {"source_url": "https://www.federalreserve.gov/timeout"},
    )
    assert json_source["fetch_status"] == "FETCHED"
    assert json_source["content_type"] == "application/json"
    assert "JSON official release" in json_source["content_text"]
    assert pdf_source["fetch_status"] == "FETCHED"
    assert pdf_source["content_type"] == "application/pdf"
    assert "PDF official release" in pdf_source["content_text"]
    assert blocked["rejection_reason"] == "http_status_403"
    assert timed_out["rejection_reason"] == "fetch_timeout"

    private_gateway = gateway(
        cfg,
        repository,
        handler,
        resolver=lambda _host: ["127.0.0.1"],
    )
    ssrf = private_gateway.acquire(
        run["run_id"],
        {"source_url": "https://www.treasury.gov/private"},
    )
    assert ssrf["rejection_reason"] == "ssrf_non_global_address"

    def robots_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(
                200,
                text="User-agent: *\nDisallow: /blocked-by-robots",
                request=request,
            )
        return html_response(request, "This page must not be fetched.")

    denied = gateway(cfg, repository, robots_handler).acquire(
        run["run_id"],
        {"source_url": "https://www.bls.gov/blocked-by-robots"},
    )
    assert denied["rejection_reason"] == "robots_denied"


def test_unicode_whitespace_punctuation_and_strict_token_matching() -> None:
    document = (
        "The agency’s report states: unemployment—rate was 4.2 percent "
        "in June 2026, after seasonal adjustment."
    )
    exactish = match_evidence(
        "Agency report states the unemployment rate was 4.2 percent for June 2026.",
        document,
        threshold=0.84,
        minimum_tokens=5,
    )
    mismatch = match_evidence(
        "The agency reported inflation was 9.9 percent in June 2026.",
        document,
        threshold=0.88,
        minimum_tokens=5,
    )
    assert exactish.accepted
    assert exactish.method == "token_window"
    assert not mismatch.accepted
    assert mismatch.reason == "evidence_mismatch"


def test_backend_usage_is_idempotent_per_invocation(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    repository, _job, run = ensure_run(cfg, "usage-idempotent")
    invocation = ResearchBackendResult(
        invocation_id="rinvoke-idempotent",
        backend="fake_contract",
        purpose="agentic_research",
        payload={"status": "NO_DATA"},
        usage={
            "input_tokens": 100,
            "output_tokens": 20,
            "cached_tokens": 40,
            "reasoning_tokens": 5,
            "total_tokens": 120,
        },
        duration_ms=50,
    )
    repository.record_backend_invocation(run["run_id"], invocation)
    repository.record_backend_invocation(run["run_id"], invocation)
    metrics = ResearchMetricsService(cfg).snapshot(run["run_id"])
    assert metrics["backend"]["invocations"] == 1
    assert metrics["usage"] == {
        "input_tokens": 100,
        "output_tokens": 20,
        "cached_tokens": 40,
        "reasoning_tokens": 5,
        "total_tokens": 120,
    }
    with connect_sqlite(cfg.database_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM research_backend_invocations").fetchone()[0] == 1


def test_migration_14_adds_gateway_and_semantic_lineage_without_recreating_database(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    result = migrate_database(cfg.database_path)
    assert result["schema_version"] == 14
    with connect_sqlite(cfg.database_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name LIKE 'research_%'
                """
            )
        }
        evidence_columns = {row[1] for row in conn.execute("PRAGMA table_info(research_evidence)")}
        claim_columns = {row[1] for row in conn.execute("PRAGMA table_info(research_claims)")}
    assert {
        "research_sources",
        "research_evidence_verifications",
        "research_backend_invocations",
    }.issubset(tables)
    assert {
        "source_id",
        "verification_id",
        "verification_method",
        "verification_reason",
        "verification_score",
    }.issubset(evidence_columns)
    assert {
        "event_at",
        "release_at",
        "issuer",
        "next_refresh_at",
        "lifecycle_status",
        "post_event_semantics",
    }.issubset(claim_columns)
