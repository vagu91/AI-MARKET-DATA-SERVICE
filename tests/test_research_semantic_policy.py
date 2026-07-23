from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from app.core.config import Settings
from app.core.text_normalization import contains_mojibake, normalize_payload_text, normalize_text
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.agentic_research_runtime import AgenticResearchRuntime
from app.services.ai_research_job_executor import build_agentic_research_prompt
from app.services.ai_research_job_service import AIResearchJobService
from app.services.codex_runtime_contract import agentic_research_output_schema
from app.services.research_backend import ResearchBackendResult
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.research_source_gateway import ResearchSourceGateway, match_evidence
from app.services.source_policy_service import SourcePolicyService


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "source_policy.json"
FIXTURE = ROOT / "tests" / "fixtures" / "mnq_semantic_policy_live_redacted.json"
REFERENCE_NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
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


def ensure_job(cfg: Settings, identity: str) -> dict[str, Any]:
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id=identity,
        request_payload={
            "context_date": "2026-07-23",
            "market_session": "US",
            "database_context": {"data_as_of": "2026-07-23T12:00:00Z"},
        },
        force=True,
    )
    assert created
    return job


class FixtureBackend:
    backend_name = "offline_fixture"

    def __init__(self, payload: dict[str, Any], invocation_id: str) -> None:
        self.payload = payload
        self.invocation_id = invocation_id
        self.calls = 0

    def execute_research(self, **_kwargs: Any) -> ResearchBackendResult:
        self.calls += 1
        return ResearchBackendResult(
            invocation_id=self.invocation_id,
            backend=self.backend_name,
            purpose="agentic_research",
            payload=self.payload,
            usage={
                "input_tokens": 145978,
                "output_tokens": 6337,
                "cached_tokens": 94464,
                "reasoning_tokens": 0,
                "total_tokens": 152315,
            },
            duration_ms=133593,
        )


def fixture_gateway(
    cfg: Settings,
    repository: ResearchRuntimeRepository,
    pages: dict[str, str],
) -> ResearchSourceGateway:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(
                200,
                text="User-agent: *\nAllow: /",
                headers={"content-type": "text/plain"},
                request=request,
            )
        body = pages[str(request.url)]
        return httpx.Response(
            200,
            text=f"<html><body><main>{body}</main></body></html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    return ResearchSourceGateway(
        cfg,
        repository=repository,
        transport=httpx.MockTransport(handler),
        resolver=lambda _host: PUBLIC_IP,
        now=lambda: REFERENCE_NOW,
    )


def test_redacted_live_replay_accepts_official_events_and_is_partial(
    tmp_path: Path,
) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    cfg = settings(tmp_path)
    repository = ResearchRuntimeRepository(cfg, now=lambda: REFERENCE_NOW)
    backend = FixtureBackend(fixture["payload"], "rinvoke-live-redacted")
    job = ensure_job(cfg, "semantic-live-replay")
    result = AgenticResearchRuntime(
        cfg,
        repository=repository,
        source_gateway=fixture_gateway(cfg, repository, fixture["pages"]),
    ).run(job, tmp_path / "jobs" / job["job_id"], backend, 120)

    assert backend.calls == 1
    assert result["status"] == "PARTIAL"
    assert result["persisted_count"] == result["read_back_count"] == 4
    accepted = {item["topic"]: item for item in result["accepted_claims"]}
    assert {
        "macro",
        "fed_rates",
        "events",
        "earnings",
    }.issubset(accepted)
    assert {
        accepted["macro"]["field_semantics"],
        accepted["fed_rates"]["field_semantics"],
        accepted["events"]["field_semantics"],
    } == {"official_calendar_event"}
    assert accepted["earnings"]["field_semantics"] == "earnings_schedule"
    assert accepted["earnings"]["issuer"] == "Microsoft"
    assert accepted["earnings"]["event_at"]
    assert accepted["macro"]["next_refresh_at"] == accepted["macro"]["event_at"]
    assert not contains_mojibake(result)

    rejected = {item["topic"]: item for item in result["rejected_claims"]}
    assert "stale_evidence" in rejected["volatility_positioning"]["warnings"]
    assert "stale_evidence" in rejected["news"]["warnings"]
    with connect_sqlite(cfg.database_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM research_backend_invocations"
            ).fetchone()[0]
            == 1
        )
        assert conn.execute(
            "SELECT COUNT(*) FROM research_claims WHERE validation_status='rejected'"
        ).fetchone()[0] == len(result["rejected_claims"])
        assert conn.execute(
            "SELECT COUNT(*) FROM market_facts WHERE fact_type='agentic_research_claim'"
        ).fetchone()[0] == 4


def test_current_news_keeps_two_domain_requirement_and_real_insufficiency(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository = ResearchRuntimeRepository(cfg, now=lambda: REFERENCE_NOW)
    job = ensure_job(cfg, "current-news-confirmations")
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v2")
    pages = {
        "https://www.reuters.com/technology/current-driver/": (
            "Reuters reports a current technology policy driver affecting large-cap shares."
        ),
        "https://apnews.com/article/current-driver": (
            "Associated Press independently confirms the current technology policy driver."
        ),
    }
    source_gateway = fixture_gateway(cfg, repository, pages)
    source_gateway.acquire_many(
        run["run_id"],
        [
            {"source_url": url, "publisher": publisher}
            for url, publisher in (
                (next(iter(pages)), "Reuters"),
                (list(pages)[1], "Associated Press"),
            )
        ],
    )
    evidence = [
        {
            "source_url": url,
            "publisher": publisher,
            "evidence_text": text,
            "published_at": "2026-07-23T11:30:00+00:00",
        }
        for (url, text), publisher in zip(
            pages.items(),
            ("Reuters", "Associated Press"),
            strict=True,
        )
    ]
    claims = [
        {
            "claim_ref": "confirmed-news",
            "topic": "news",
            "field_semantics": "current_news",
            "value": "current technology policy driver",
            "published_at": "2026-07-23T11:30:00+00:00",
            "confidence": 0.9,
            "topic_status": "SUPPORTED",
            "evidence": evidence,
        },
        {
            "claim_ref": "single-source-news",
            "topic": "conflicts",
            "field_semantics": "current_news",
            "value": "single-source current report",
            "published_at": "2026-07-23T11:30:00+00:00",
            "confidence": 0.8,
            "topic_status": "SUPPORTED",
            "evidence": [evidence[0], dict(evidence[0])],
        },
    ]
    verified = source_gateway.verify_claims(run["run_id"], claims)
    result = repository.persist_claims(repository.get_run(run["run_id"]) or run, verified)
    assert result["accepted_count"] == 1
    rejected = result["rejected_claims"][0]
    assert rejected["field_semantics"] == "current_news"
    assert rejected["warnings"] == ["insufficient_independent_evidence"]


def test_official_issuer_subdomain_is_allowed_with_dns_boundary(
    tmp_path: Path,
) -> None:
    policy = SourcePolicyService(POLICY)
    official = policy.rule_for(
        "https://news.microsoft.com/source/earnings/",
        "Microsoft",
    )
    assert official is not None
    assert official["issuer"] == "Microsoft"
    assert official["issuer_channel"] == "NEWSROOM"
    assert official["tier"] == 1
    assert policy.rule_for("https://evilmicrosoft.com/source/earnings/", "Microsoft") is None

    cfg = settings(tmp_path)
    repository = ResearchRuntimeRepository(cfg, now=lambda: REFERENCE_NOW)
    job = ensure_job(cfg, "evil-domain")
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v2")
    source = ResearchSourceGateway(
        cfg,
        repository=repository,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text="must not fetch", request=request)
        ),
        resolver=lambda _host: (_ for _ in ()).throw(AssertionError("DNS must not run")),
        now=lambda: REFERENCE_NOW,
    ).acquire(
        run["run_id"],
        {
            "source_url": "https://evilmicrosoft.com/source/earnings/",
            "publisher": "Microsoft",
        },
    )
    assert source["rejection_reason"] == "source_policy_rejected"


def test_earnings_schedule_requires_event_and_issuer(tmp_path: Path) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    cfg = settings(tmp_path)
    repository = ResearchRuntimeRepository(cfg, now=lambda: REFERENCE_NOW)
    job = ensure_job(cfg, "earnings-requirements")
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v2")
    sec_url = "https://www.sec.gov/Archives/edgar/data/1018724/filing.htm"
    gateway = fixture_gateway(cfg, repository, {sec_url: fixture["pages"][sec_url]})
    gateway.acquire(run["run_id"], {"source_url": sec_url, "publisher": "U.S. SEC"})
    claim = {
        "claim_ref": "missing-issuer",
        "topic": "earnings",
        "field_semantics": "earnings_schedule",
        "value": "A company scheduled earnings.",
        "event_at": "2026-07-29T20:00:00+00:00",
        "confidence": 0.8,
        "topic_status": "SUPPORTED",
        "evidence": [
            {
                "source_url": sec_url,
                "publisher": "U.S. SEC",
                "evidence_text": (
                    "Amazon closed the sale of 750 million dollars aggregate principal "
                    "amount of floating rate notes due 2029."
                ),
                "published_at": "2026-07-23T11:00:00+00:00",
            }
        ],
    }
    verified = gateway.verify_claims(run["run_id"], [claim])
    result = repository.persist_claims(repository.get_run(run["run_id"]) or run, verified)
    assert result["status"] == "NO_DATA"
    assert "issuer_required" in result["rejected_claims"][0]["warnings"]


def test_documented_not_applicable_can_complete_all_topics_without_facts(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    repository = ResearchRuntimeRepository(cfg, now=lambda: REFERENCE_NOW)
    job = ensure_job(cfg, "documented-not-applicable")
    topics = [
        "macro",
        "fed_rates",
        "events",
        "nasdaq_100",
        "mega_cap_semiconductors",
        "earnings",
        "news",
        "risk",
        "volatility_positioning",
        "conflicts",
    ]
    topic_groups = [
        ["macro", "events"],
        ["fed_rates"],
        ["nasdaq_100"],
        ["mega_cap_semiconductors"],
        ["earnings"],
        ["news"],
        ["risk", "conflicts"],
        ["volatility_positioning"],
    ]
    queries = [
        {
            "query": f"bounded current search for {' and '.join(group)}",
            "purpose": "determine whether a material event exists",
            "topic": group[0],
            "topics": group,
        }
        for group in topic_groups
    ]
    payload = {
        "status": "COMPLETED",
        "plan": {
            "topics": topics,
            "queries": queries,
            "stop_conditions": ["all required topics searched"],
        },
        "searches": [
            {"query": item["query"], "discovered_urls": []}
            for item in queries
        ],
        "acquisition_requests": [],
        "claims": [
            {
                "claim_ref": f"na-{topic}",
                "topic": topic,
                "field_semantics": "current_news",
                "value": "NOT_APPLICABLE",
                "confidence": 1.0,
                "topic_status": "NOT_APPLICABLE",
                "evidence": [],
                "warnings": [],
            }
            for topic in topics
        ],
        "warnings": [],
    }
    backend = FixtureBackend(payload, "rinvoke-not-applicable")
    result = AgenticResearchRuntime(
        cfg,
        repository=repository,
        source_gateway=fixture_gateway(cfg, repository, {}),
    ).run(job, tmp_path / "jobs" / job["job_id"], backend, 120)
    assert backend.calls == 1
    assert result["status"] == "SUCCEEDED"
    assert result["persisted_count"] == result["read_back_count"] == len(topics)
    assert result["valid_not_applicable_topics"] == sorted(topics)
    with connect_sqlite(cfg.database_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM market_facts WHERE fact_type='agentic_research_claim'"
        ).fetchone()[0] == 0


def test_unicode_normalization_preserves_real_unicode_and_rejects_mojibake() -> None:
    payload = normalize_payload_text(
        {
            "value": "July 28\u00c3\u00a2\u00c2\u0080\u00c2\u009329",
            "evidence_text": "VIX\u00c3\u0082\u00c2\u00ae and Microsoft\u00e2\u20ac\u2122s caf\u00c3\u00a8",
            "issuer": "\u65e5\u672c\u682a\u5f0f\u4f1a\u793e",
            "clean_apostrophe": "l\u2019annuncio",
            "romanian": "Rom\u00e2nia",
            "portuguese": "\u00c3gua",
        }
    )
    assert payload["value"] == "July 28\u201329"
    assert payload["evidence_text"] == "VIX\u00ae and Microsoft's caf\u00e8"
    assert payload["issuer"] == "\u65e5\u672c\u682a\u5f0f\u4f1a\u793e"
    assert payload["clean_apostrophe"] == "l\u2019annuncio"
    assert payload["romanian"] == "Rom\u00e2nia"
    assert payload["portuguese"] == "\u00c3gua"
    assert not contains_mojibake(payload)
    assert normalize_text("VIX\u00ae") == "VIX\u00ae"
    assert match_evidence(
        "FOMC meeting July 28\u201329 registered VIX\u00ae",
        "FOMC meeting July 28 - 29; registered VIX\u00ae.",
        threshold=0.8,
        minimum_tokens=5,
    ).accepted


def test_schema_policy_prompt_and_migration_expose_new_semantics(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    policy = SourcePolicyService(POLICY)
    assert policy.required_confirmations("official_calendar_event") == 1
    assert policy.required_confirmations("current_news") == 2
    assert policy.semantic_policy("current_market_context")["ttl_minutes"] == 60
    result = migrate_database(cfg.database_path)
    assert result["schema_version"] == 14
    with connect_sqlite(cfg.database_path) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(research_claims)")
        }
    assert {
        "event_at",
        "release_at",
        "issuer",
        "next_refresh_at",
        "lifecycle_status",
        "post_event_semantics",
    }.issubset(columns)

    schema = agentic_research_output_schema()
    semantics = set(
        schema["properties"]["claims"]["items"]["properties"]["field_semantics"]["enum"]
    )
    assert (
        schema["properties"]["plan"]["properties"]["queries"]["items"]["properties"][
            "topics"
        ]["minItems"]
        == 1
    )
    assert {
        "scheduled_event",
        "official_calendar_event",
        "issuer_announcement",
        "earnings_schedule",
        "current_news",
        "current_market_context",
        "exploratory_context",
    }.issubset(semantics)
    assert "official_actual" not in semantics

    job = ensure_job(cfg, "prompt-strategy")
    repository = ResearchRuntimeRepository(cfg, now=lambda: REFERENCE_NOW)
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v2")
    prompt = build_agentic_research_prompt(
        job,
        run,
        {
            "profile_id": "MNQ_MARKET_RESEARCH",
            "required_topics": ["macro", "news", "volatility_positioning"],
        },
        {
            "budget_mode": "observe",
            "max_searches": 8,
            "max_opened_sources": 12,
        },
    )
    assert "Reuters/AP" in prompt
    assert "CFTC/CME/Cboe" in prompt
    assert "Nasdaq/Invesco/issuer newsroom/issuer IR/SEC" in prompt
    assert "warning thresholds" in prompt
    assert "one bounded agentic invocation" in prompt
