from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.main import app
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_trader_consumer_v2_service import build_ai_trader_consumer_v2
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.parallel_research_coordinator import (
    ParallelResearchCoordinator,
    _parent_status,
)
from app.services.research_backend import normalized_backend_input
from app.services.research_domain_contracts import (
    AGENTIC_DOMAIN_FIELDS,
    DOMAIN_TOPICS,
    build_domain_projection,
    compact_domain_projection,
    domain_claim_warnings,
    enrich_domain_claim,
)
from app.services.research_gap_manifest import ResearchGapManifestBuilder
from app.services.research_metrics_service import ResearchMetricsService
from app.services.research_profiles import PROFILES, prompt_context
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.research_source_gateway import ResearchSourceGateway


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = (
    ROOT
    / "tests"
    / "fixtures"
    / "mnq_agentic_domains_20260724_redacted.json"
)
POLICY = ROOT / "config" / "source_policy.json"
NOW = datetime(2026, 7, 24, 15, 0, tzinfo=UTC)
PUBLIC_IP = ["93.184.216.34"]


def fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "environment": "test",
        "enable_ai_researcher": True,
        "research_backend": "codex_cli",
        "research_parallelism": 2,
        "research_gateway_min_text_chars": 20,
        "research_gateway_respect_robots": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def make_run(
    cfg: Settings,
    profile_id: str,
    identity: str,
) -> tuple[dict[str, Any], dict[str, Any], ResearchRuntimeRepository]:
    job, created = AIResearchJobService(cfg).enqueue_explicit(
        job_type=profile_id,
        symbol="MNQ",
        correlation_id=identity,
        request_payload={
            "gap": {
                "topic": PROFILES[profile_id].required_topics[0],
                "missing_fields": list(PROFILES[profile_id].required_fields),
            }
        },
        pending_fields=list(PROFILES[profile_id].required_fields),
        force=True,
    )
    assert created
    repository = ResearchRuntimeRepository(cfg, now=lambda: NOW)
    profile = PROFILES[profile_id]
    run = repository.ensure_run(
        job,
        profile.profile_id,
        profile.prompt_version,
    )
    return job, run, repository


def service_verified_evidence(
    text: str,
    *,
    url: str,
    published_at: str | None,
) -> dict[str, Any]:
    return {
        "source_url": url,
        "canonical_url": url,
        "publisher": None,
        "evidence_text": text,
        "published_at": published_at,
        "retrieved_at": NOW.isoformat(),
        "_service_verification": {
            "accepted": True,
            "reason": "verified_rigorous_token_match",
            "match_method": "exact_normalized",
            "match_score": 1.0,
            "source_id": "source-offline",
            "verification_id": "verification-offline",
        },
    }


def evidence_row(
    *,
    url: str,
    published_at: str | None,
    domain: str,
    tier: int = 1,
) -> dict[str, Any]:
    return {
        "source_url": url,
        "canonical_url": url,
        "source_domain": domain,
        "source_tier": tier,
        "published_at": published_at,
        "retrieved_at": NOW.isoformat(),
        "source_content_hash": f"hash-{domain}",
        "content_checksum": f"evidence-{domain}",
    }


def test_four_profiles_and_residual_gap_manifest_are_provider_neutral(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    manifest = ResearchGapManifestBuilder(
        cfg,
        clock=lambda: NOW,
    ).build(snapshot=None, components={})
    new_topics = [topic for topic in manifest["agent_topics"] if topic in DOMAIN_TOPICS]
    assert new_topics == [
        "options_positioning",
        "market_internals",
        "cross_asset_context",
        "earnings_intelligence",
    ]
    assert manifest["provider_stage"] == {
        "contract_version": "provider_gap_v1",
        "precedence": [
            "api_provider",
            "public_endpoint",
            "agent_web_for_residual_gaps_only",
        ],
        "configured": False,
        "completed": False,
    }
    for item in manifest["items"]:
        if item["topic"] not in DOMAIN_TOPICS:
            continue
        assert set(item["field_states"]) == set(
            AGENTIC_DOMAIN_FIELDS[item["topic"]]
        )
        assert set(item["field_states"].values()) == {"MISSING"}
        assert list(item["missing_fields"]) == list(
            AGENTIC_DOMAIN_FIELDS[item["topic"]]
        )


def test_manifest_distinguishes_satisfied_stale_quarantined_and_not_applicable(
    tmp_path: Path,
) -> None:
    fields = {
        field: {
            "value": "verified",
            "data_as_of": NOW.isoformat(),
            "verification_status": "VERIFIED",
            "acquisition_method": "api_provider",
        }
        for field in AGENTIC_DOMAIN_FIELDS["options_positioning"]
    }
    fields["qqq_put_call_ratio"]["data_as_of"] = "2026-07-20T15:00:00+00:00"
    fields["estimated_gamma_exposure"]["verification_status"] = "QUARANTINED"
    fields["estimated_gamma_concentration"]["field_state"] = "NOT_APPLICABLE"
    manifest = ResearchGapManifestBuilder(
        settings(tmp_path),
        clock=lambda: NOW,
    ).build(
        snapshot=None,
        components={"options_positioning": {"fields": fields}},
    )
    item = next(
        row for row in manifest["items"] if row["topic"] == "options_positioning"
    )
    assert item["field_states"]["total_put_call_ratio"] == "SATISFIED"
    assert item["field_states"]["qqq_put_call_ratio"] == "STALE"
    assert item["field_states"]["estimated_gamma_exposure"] == "QUARANTINED"
    assert (
        item["field_states"]["estimated_gamma_concentration"]
        == "NOT_APPLICABLE"
    )
    assert set(item["missing_fields"]) == {
        "qqq_put_call_ratio",
        "estimated_gamma_exposure",
    }


def test_fresh_cboe_numeric_claim_is_gateway_verified_and_persisted(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    _job, run, repository = make_run(
        cfg,
        "OPTIONS_POSITIONING_RESEARCH",
        "fresh-cboe",
    )
    data = fixture()["options_fresh"]
    evidence = data["evidence"]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=f"<html><body>{evidence['evidence_text']}</body></html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    gateway = ResearchSourceGateway(
        cfg,
        repository=repository,
        transport=httpx.MockTransport(handler),
        resolver=lambda _host: PUBLIC_IP,
        now=lambda: NOW,
    )
    acquired = gateway.acquire(
        str(run["run_id"]),
        {
            "source_url": evidence["source_url"],
            "publisher": evidence["publisher"],
        },
    )
    assert acquired["fetch_status"] == "FETCHED"
    verified = gateway.verify_claims(
        str(run["run_id"]),
        [
            {
                **{key: value for key, value in data.items() if key != "evidence"},
                "confidence": 0.9,
                "topic_status": "SUPPORTED",
                "evidence": [evidence],
            }
        ],
    )
    persisted = repository.persist_claims(run, verified)
    assert persisted["status"] == "SUCCEEDED"
    assert persisted["accepted_count"] == persisted["persisted_count"] == 1
    claim = persisted["accepted_claims"][0]["payload"]
    assert claim["verification_status"] == "VERIFIED"
    assert claim["freshness_status"] == "FRESH"
    assert claim["acquisition_method"] == "agent_web"
    assert claim["evidence_hash"]


def test_stale_missing_timestamp_and_model_only_numbers_are_rejected() -> None:
    data = fixture()
    url = "https://www.nasdaq.com/market-activity"
    cases = [
        (
            {
                **data["options_stale"],
                "verification_status": "VERIFIED",
                "acquisition_method": "agent_web",
            },
            service_verified_evidence(
                "QQQ put call ratio 0.91",
                url="https://www.cboe.com/us/options/",
                published_at=data["options_stale"]["data_as_of"],
            ),
            "domain_evidence_stale",
        ),
        (
            {
                **data["source_without_timestamp"],
                "verification_status": "VERIFIED",
                "acquisition_method": "agent_web",
            },
            service_verified_evidence(
                "Nasdaq decliners 41",
                url=url,
                published_at=None,
            ),
            "domain_data_as_of_required",
        ),
        (
            {
                **data["market_internals"],
                "verification_status": "UNVERIFIED",
                "acquisition_method": "agent_web",
            },
            {
                "evidence_text": data["market_internals"]["evidence_text"],
                "_service_verification": {"accepted": False},
            },
            "domain_value_not_server_verified",
        ),
    ]
    for claim, evidence, expected in cases:
        assert expected in domain_claim_warnings(
            claim,
            [evidence],
            now=NOW,
        )


def test_gamma_without_verified_inputs_projects_null() -> None:
    raw = fixture()["gamma_incomplete"]
    claim = {
        **raw,
        "verification_status": "VERIFIED",
        "acquisition_method": "agent_web",
    }
    warnings = domain_claim_warnings(
        claim,
        [
            service_verified_evidence(
                "Estimated gamma exposure -120000000",
                url="https://www.cboe.com/us/options/",
                published_at=raw["data_as_of"],
            )
        ],
        now=NOW,
    )
    assert "estimated_gamma_inputs_incomplete" in warnings
    projection = build_domain_projection(
        "options_positioning",
        [],
        status="NO_DATA",
        no_data_reason="no_fresh_verified_source",
    )
    assert projection["fields"]["estimated_gamma_exposure"] is None
    assert projection["fields"]["estimated_gamma_concentration"] is None


def test_market_internals_requires_numeric_value_in_verified_evidence() -> None:
    raw = fixture()["market_internals"]
    evidence = service_verified_evidence(
        raw["evidence_text"],
        url="https://www.nasdaq.com/market-activity",
        published_at=raw["data_as_of"],
    )
    claim = enrich_domain_claim(
        raw,
        [
            evidence_row(
                url=evidence["source_url"],
                published_at=raw["data_as_of"],
                domain="nasdaq.com",
            )
        ],
        now=NOW,
    )
    assert domain_claim_warnings(claim, [evidence], now=NOW) == []
    assert "domain_numeric_value_not_in_verified_evidence" in domain_claim_warnings(
        {**claim, "value": "59"},
        [evidence],
        now=NOW,
    )


def test_cross_asset_mixed_fresh_and_stale() -> None:
    fresh, stale = fixture()["cross_asset"]
    for claim, expected in ((fresh, "FRESH"), (stale, "STALE")):
        enriched = enrich_domain_claim(
            claim,
            [
                evidence_row(
                    url="https://fred.stlouisfed.org/series/DGS10",
                    published_at=claim["data_as_of"],
                    domain="fred.stlouisfed.org",
                )
            ],
            now=NOW,
        )
        assert enriched["freshness_status"] == expected


def test_cross_asset_projection_preserves_repeated_metrics_per_instrument() -> None:
    claims = []
    for symbol, value in (("DGS10", "4.31"), ("BTCUSD", "67250")):
        claims.append(
            {
                "topic": "cross_asset_context",
                "metric_id": "value",
                "value": value,
                "symbol": symbol,
                "payload": {
                    "data_as_of": NOW.isoformat(),
                    "verification_status": "VERIFIED",
                    "freshness_status": "FRESH",
                    "quality": "VERIFIED",
                    "acquisition_method": "public_endpoint",
                    "source_domain": "official.example",
                },
            }
        )
    projection = build_domain_projection(
        "cross_asset_context",
        claims,
        status="SUCCEEDED",
    )
    assert {
        (item["symbol"], item["metric_id"], item["value"])
        for item in projection["items"]
    } == {
        ("DGS10", "value", "4.31"),
        ("BTCUSD", "value", "67250"),
    }
    compact = compact_domain_projection(projection)
    assert {item["symbol"] for item in compact["items"]} == {
        "DGS10",
        "BTCUSD",
    }
    assert "evidence_hash" not in json.dumps(compact)


def test_earnings_sec_ir_value_and_gaap_non_gaap_guard() -> None:
    data = fixture()
    verified = data["earnings_verified"]
    evidence = service_verified_evidence(
        verified["evidence_text"],
        url="https://www.sec.gov/Archives/edgar/data/redacted/filing.htm",
        published_at=verified["data_as_of"],
    )
    claim = enrich_domain_claim(
        verified,
        [
            evidence_row(
                url=evidence["source_url"],
                published_at=verified["data_as_of"],
                domain="sec.gov",
            )
        ],
        now=NOW,
    )
    assert domain_claim_warnings(claim, [evidence], now=NOW) == []
    incompatible = {
        **data["earnings_incompatible"],
        "verification_status": "VERIFIED",
        "acquisition_method": "agent_web",
    }
    incompatible_evidence = service_verified_evidence(
        "EPS surprise 0.25",
        url=evidence["source_url"],
        published_at=incompatible["data_as_of"],
    )
    assert "earnings_comparison_basis_incompatible" in domain_claim_warnings(
        incompatible,
        [incompatible_evidence],
        now=NOW,
    )


def test_no_data_policy_and_parent_mixed_status_are_non_failures() -> None:
    assert _parent_status(["NO_DATA"] * 4) == "NO_DATA"
    assert _parent_status(["SUCCEEDED", "NO_DATA", "NO_DATA", "NO_DATA"]) == (
        "SUCCEEDED"
    )
    assert _parent_status(["SUCCEEDED", "FAILED", "NO_DATA", "NO_DATA"]) == (
        "PARTIAL"
    )


def test_all_four_domain_runs_emit_explicit_no_data_contract(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    for profile_id in (
        "OPTIONS_POSITIONING_RESEARCH",
        "MARKET_INTERNALS_RESEARCH",
        "CROSS_ASSET_CONTEXT_RESEARCH",
        "EARNINGS_INTELLIGENCE_RESEARCH",
    ):
        _job, run, repository = make_run(
            cfg,
            profile_id,
            f"no-data-{profile_id}",
        )
        result = repository.persist_claims(run, [])
        assert result["status"] == "NO_DATA"
        assert result["reason"] == result["no_data_reason"] == (
            "no_fresh_verified_source"
        )
        assert result["value"] is None
        assert result["searched_at"]
        assert result["sources_attempted"] == []


def test_parent_creates_exactly_four_specialized_residual_children(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    manifest = ResearchGapManifestBuilder(
        cfg,
        clock=lambda: NOW,
    ).build(snapshot=None, components={})
    manifest["items"] = [
        item for item in manifest["items"] if item["topic"] in DOMAIN_TOPICS
    ]
    manifest["agent_topics"] = [
        topic for topic in manifest["agent_topics"] if topic in DOMAIN_TOPICS
    ]
    parent = ParallelResearchCoordinator(cfg).create_parent(
        manifest,
        correlation_id="four-domain-parent",
    )
    assert parent["concurrency_limit"] == 2
    assert [job["specialized_topic"] for job in parent["child_jobs"]] == sorted(
        DOMAIN_TOPICS
    )
    assert {
        job["profile_id"] for job in parent["child_jobs"]
    } == {
        "OPTIONS_POSITIONING_RESEARCH",
        "MARKET_INTERNALS_RESEARCH",
        "CROSS_ASSET_CONTEXT_RESEARCH",
        "EARNINGS_INTELLIGENCE_RESEARCH",
    }
    for job in parent["child_jobs"]:
        gap = job["request_payload"]["gap"]
        assert job["pending_fields"] == list(gap["missing_fields"])


def test_codex_and_openai_share_the_same_normalized_domain_contract() -> None:
    profile = PROFILES["OPTIONS_POSITIONING_RESEARCH"]
    request = {
        "gap": {
            "topic": "options_positioning",
            "missing_fields": ["qqq_put_call_ratio"],
        }
    }
    model_profile = prompt_context(profile, request, {"budget_mode": "observe"})
    normalized = normalized_backend_input(
        job={"job_id": "job", "symbol": "MNQ", "request_payload": request},
        run={"run_id": "run"},
        profile=model_profile,
        effective_budget={"budget_mode": "observe"},
    )
    assert normalized["contract_version"] == "research_backend_v1"
    assert normalized["acquisition_methods"] == [
        "agent_web",
        "public_endpoint",
        "api_provider",
    ]
    assert normalized["provider_precedence"][-1] == (
        "agent_web_for_residual_gaps_only"
    )


def test_snapshot_debug_and_compact_consumer_are_schema_21_and_under_90kb(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    snapshots = MarketContextSnapshotRepository(cfg)
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="fixture",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "market_schedule": {"context_date": "2026-07-24"},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    _job, run, repository = make_run(
        cfg,
        "OPTIONS_POSITIONING_RESEARCH",
        "snapshot-domain",
    )
    payload = fixture()["options_fresh"]
    evidence = payload["evidence"]
    verified = [
        {
            **{key: value for key, value in payload.items() if key != "evidence"},
            "confidence": 0.9,
            "evidence": [
                service_verified_evidence(
                    evidence["evidence_text"],
                    url=evidence["source_url"],
                    published_at=evidence["published_at"],
                )
            ],
        }
    ]
    repository.persist_research_source(
        str(run["run_id"]),
        {
            "source_id": "source-offline",
            "requested_url": evidence["source_url"],
            "final_url": evidence["source_url"],
            "canonical_url": evidence["source_url"],
            "source_domain": "cboe.com",
            "source_tier": 1,
            "publisher": "Cboe",
            "fetch_status": "FETCHED",
            "verification_status": "VERIFIED",
            "retrieved_at": NOW.isoformat(),
            "content_sha256": "a" * 64,
            "content_bytes": 100,
            "content_text": evidence["evidence_text"],
            "stage_status": "CONTENT_EXTRACTED",
        },
    )
    result = repository.persist_claims(run, verified)
    assert result["status"] == "SUCCEEDED"
    saved = snapshots.save_next(
        symbol="MNQ",
        refresh_mode="worker_db_only_materialization",
        debug_payload=snapshots.latest_components("MNQ"),
        ai_enrichment={"status": "SUCCEEDED"},
        source_job_id=str(run["job_id"]),
        job_ids=[str(run["job_id"])],
        research_run_id=str(run["run_id"]),
    )
    debug = saved["debug_payload"]
    assert debug["options_positioning"]["fields"]["total_put_call_ratio"][
        "value"
    ] == "0.82"
    consumer = saved["consumer_payload"]
    assert consumer["schema_version"] == "2.1"
    assert consumer["agentic_domains"]["options_positioning"]["fields"][
        "total_put_call_ratio"
    ]["value"] == "0.82"
    assert "evidence_text" not in json.dumps(consumer)
    assert len(
        json.dumps(consumer, separators=(",", ":"), default=str).encode("utf-8")
    ) < 90_000


def test_refresh_false_snapshot_read_is_zero_write(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    snapshots = MarketContextSnapshotRepository(cfg)
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="fixture",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "market_schedule": {"context_date": "2026-07-24"},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    with connect_sqlite(cfg.database_path) as conn:
        before = conn.total_changes
        rows_before = conn.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0]
    first = snapshots.latest("MNQ")
    second = snapshots.latest("MNQ")
    with connect_sqlite(cfg.database_path) as conn:
        rows_after = conn.execute(
            "SELECT COUNT(*) FROM ai_research_jobs"
        ).fetchone()[0]
        after = conn.total_changes
    assert first == second
    assert rows_before == rows_after == 0
    assert before == after == 0


def test_no_trading_order_or_execution_endpoints_were_added() -> None:
    paths = set(app.openapi()["paths"])
    forbidden = ("trading", "order", "execution")
    assert not [
        path for path in paths if any(token in path.lower() for token in forbidden)
    ]


def test_schema_19_remains_compatible_without_additional_migration(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    first = migrate_database(cfg.database_path)
    second = migrate_database(cfg.database_path)
    assert len(MIGRATIONS) == 19
    assert first["schema_version"] == second["schema_version"] == 19
    assert second["applied"] == []


def test_retry_recovery_idempotency_and_bounded_parallelism(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, ai_job_lease_seconds=30)
    service = AIResearchJobService(cfg)
    repository = AIResearchJobRepository(cfg)
    job, _created = service.enqueue_explicit(
        job_type="OPTIONS_POSITIONING_RESEARCH",
        symbol="MNQ",
        correlation_id="retry-domain",
        request_payload={"gap": {"topic": "options_positioning"}},
        force=True,
    )
    acquired = repository.acquire_next("worker-retry")
    assert acquired and acquired["job_id"] == job["job_id"]
    retried = repository.retry_or_fail(
        job["job_id"],
        "worker-retry",
        error="transient_fixture",
        delays=[0],
        retryable=True,
    )
    assert retried["status"] == "RETRY_SCHEDULED"
    reacquired = repository.acquire_next("worker-recovery")
    assert reacquired and reacquired["job_id"] == job["job_id"]
    with connect_sqlite(cfg.database_path) as conn:
        conn.execute(
            "UPDATE ai_research_jobs SET lease_expires_at=? WHERE job_id=?",
            ("2000-01-01T00:00:00+00:00", job["job_id"]),
        )
        conn.commit()
    assert repository.recover_abandoned() == 1

    manifest = ResearchGapManifestBuilder(
        cfg,
        clock=lambda: NOW,
    ).build(snapshot=None, components={})
    coordinator = ParallelResearchCoordinator(cfg)
    first = coordinator.create_parent(
        manifest,
        correlation_id="idempotent-parent",
    )
    second = coordinator.create_parent(
        manifest,
        correlation_id="idempotent-parent",
    )
    assert first["parent_run_id"] == second["parent_run_id"]
    assert second["created"] is False
    children = [
        job
        for job in first["child_jobs"]
        if job.get("specialized_topic") in DOMAIN_TOPICS
    ]
    started = time.perf_counter()
    output = coordinator.execute_children(
        children,
        lambda child: (
            time.sleep(0.08),
            child["specialized_topic"],
        )[1],
    )
    elapsed = time.perf_counter() - started
    assert set(output) == set(DOMAIN_TOPICS)
    assert elapsed < 0.28


def test_compact_projection_never_contains_full_evidence() -> None:
    projection = build_domain_projection(
        "market_internals",
        [
            {
                "topic": "market_internals",
                "metric_id": "advancers",
                "value": "58",
                "unit": "components",
                "payload": {
                    "data_as_of": NOW.isoformat(),
                    "verification_status": "VERIFIED",
                    "freshness_status": "FRESH",
                    "quality": "VERIFIED",
                    "evidence": [{"evidence_text": "private verbose evidence"}],
                },
            }
        ],
        status="SUCCEEDED",
    )
    compact = compact_domain_projection(projection)
    assert compact["fields"]["advancers"]["value"] == "58"
    assert "evidence" not in json.dumps(compact)


def test_metrics_expose_per_domain_distributions(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    _job, run, repository = make_run(
        cfg,
        "MARKET_INTERNALS_RESEARCH",
        "domain-metrics",
    )
    raw = fixture()["market_internals"]
    url = "https://www.nasdaq.com/market-activity"
    repository.persist_research_source(
        str(run["run_id"]),
        {
            "source_id": "source-offline",
            "requested_url": url,
            "final_url": url,
            "canonical_url": url,
            "source_domain": "nasdaq.com",
            "source_tier": 1,
            "publisher": "Nasdaq",
            "fetch_status": "FETCHED",
            "verification_status": "VERIFIED",
            "retrieved_at": NOW.isoformat(),
            "content_sha256": "b" * 64,
            "content_bytes": 100,
            "content_text": raw["evidence_text"],
            "stage_status": "CONTENT_EXTRACTED",
        },
    )
    evidence = service_verified_evidence(
        raw["evidence_text"],
        url=url,
        published_at=raw["data_as_of"],
    )
    repository.persist_claims(
        run,
        [{**raw, "confidence": 0.9, "evidence": [evidence]}],
    )
    metrics = ResearchMetricsService(cfg).snapshot(
        str(run["run_id"]),
        persist=False,
    )
    assert metrics["freshness_distribution"] == {"FRESH": 1}
    assert metrics["acquisition_method_distribution"] == {"agent_web": 1}
    assert metrics["accepted_claims"] == 1
    assert metrics["fetched_sources"] == 1


def test_consumer_builder_keeps_schema_21_with_empty_domains() -> None:
    consumer = build_ai_trader_consumer_v2(
        {
            "symbol": "MNQ",
            "generated_at_utc": NOW.isoformat(),
            "market_schedule": {"context_date": "2026-07-24"},
        }
    )
    assert consumer["schema_version"] == "2.1"
    assert set(consumer["agentic_domains"]) == set(DOMAIN_TOPICS)
