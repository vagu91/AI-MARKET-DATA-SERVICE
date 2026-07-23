from __future__ import annotations

import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.api import routes
from app.infrastructure.persistence.migrations import _split_sql, migrate_database
from app.infrastructure.persistence.schema import MIGRATIONS
from app.models.ai_jobs import MarketResearchRunRequest
from app.models.common import Freshness, Impact, ProviderMetadata, ProviderResult, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.services.agentic_research_runtime import AgenticResearchRuntime
from app.services.ai_research_capability_service import AIResearchCapabilityService
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.ai_research_job_service import AIResearchJobService
from app.services.ai_research_worker import AIResearchWorker
from app.services.deterministic_actual_resolver import DeterministicActualResolver
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.market_fact_repository import MarketFactRepository, connect_market_db
from app.services.official_actual_semantics import OFFICIAL_METRICS, derive_official_actual
from app.services.research_budget import ResearchBudgetExceeded
from app.services.research_runtime_repository import ResearchRuntimeRepository
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.temporal_domain_service import canonical_event_key


POLICY = Path(__file__).resolve().parents[1] / "config" / "source_policy.json"


def settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": POLICY,
        "ai_job_workspace_root": tmp_path / "jobs",
        "enable_ai_researcher": True,
        "ai_worker_enabled": False,
        "research_single_invocation_enabled": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def observations(values: list[str], *, year: int = 2025, start_month: int = 7) -> list[dict]:
    output = []
    for index, value in enumerate(values):
        absolute = start_month - 1 + index
        output.append(
            {
                "period": f"{year + absolute // 12}-{absolute % 12 + 1:02d}",
                "value": value,
                "release_vintage": f"v{index + 1}",
            }
        )
    return output


def derive(metric_id: str, values: list[str], period: str, **series) -> dict:
    spec = OFFICIAL_METRICS[metric_id]
    return derive_official_actual(
        spec,
        {"observations": observations(values), **series},
        expected_period=period,
        retrieved_at="2026-07-22T10:00:00+00:00",
        release_timestamp="2026-07-22T08:30:00+00:00",
    )


def observe_sources(repository: ResearchRuntimeRepository, run: dict, urls: list[str]) -> None:
    step, _ = repository.begin_step(
        run["run_id"],
        "OPEN_SOURCE",
        3,
        {},
        backend="fake",
        tool="fake_open",
    )
    repository.record_tool_events(
        run["run_id"],
        step["step_id"],
        [
            {
                "event_type": "open_source",
                "source_url": url,
                "canonical_url": url,
                "observed_at": "2026-07-22T10:00:00Z",
                "content_hash": f"hash-{index}",
                "http_status": 200,
            }
            for index, url in enumerate(urls)
        ],
    )
    repository.complete_step(step["step_id"], {"status": "COMPLETED"})


def test_cpi_mom_is_derived_from_two_index_levels() -> None:
    actual = derive("headline_cpi_mom", ["300.000", "301.500"], "2025-08")
    assert actual["value"] == "0.5"
    assert actual["source_series_id"] == "CUSR0000SA0"
    assert actual["transformation"] == "pct_change_mom" and actual["seasonal_adjustment"] == "SA"
    assert actual["current_level"] == "301.500" and actual["comparison_level"] == "300.000"


def test_cpi_yoy_uses_thirteen_nsa_observations() -> None:
    values = ["300"] + [str(300 + index / 10) for index in range(1, 12)] + ["309"]
    actual = derive("headline_cpi_yoy", values, "2026-07")
    assert actual["value"] == "3.0"
    assert actual["source_series_id"] == "CUUR0000SA0"
    assert actual["calculation_lineage"]["observation_count"] == 13


def test_nfp_is_monthly_delta_not_payroll_level() -> None:
    actual = derive("nonfarm_payrolls_change", ["158000", "158175"], "2025-08")
    assert actual["value"] == "175"
    assert actual["unit"] == "thousands of jobs" and actual["transformation"] == "delta"


def test_gdp_uses_official_annualized_rate_not_chained_dollar_level() -> None:
    spec = OFFICIAL_METRICS["real_gdp_annualized_qoq"]
    actual = derive_official_actual(
        spec,
        {
            "observations": [
                {"period": "2026Q1", "value": "3.1", "release_vintage": "third-estimate"}
            ]
        },
        expected_period="2026-Q1",
        retrieved_at="2026-06-25T12:30:00Z",
        release_timestamp="2026-06-25T12:30:00Z",
    )
    assert actual["value"] == "3.1"
    assert actual["source_series_id"] == "BEA:GDP"
    assert actual["transformation"] == "official_annualized_qoq_rate"


def test_pce_uses_price_index_not_nominal_spending() -> None:
    actual = derive("headline_pce_mom", ["120.000", "120.240"], "2025-08")
    assert actual["value"] == "0.2"
    assert actual["source_series_id"] == "BEA:PCE_PRICE_INDEX"


def test_period_mismatch_and_insufficient_observations_fail_closed() -> None:
    with pytest.raises(ValueError, match="period_mismatch"):
        derive("headline_cpi_mom", ["300", "301"], "2026-01")
    with pytest.raises(ValueError, match="insufficient_official_observations"):
        derive("headline_cpi_yoy", ["300", "301"], "2025-08")


def test_latest_release_vintage_is_used_and_revision_is_preserved() -> None:
    spec = OFFICIAL_METRICS["headline_cpi_mom"]
    actual = derive_official_actual(
        spec,
        {
            "observations": [
                {"period": "2026-05", "value": "300", "release_vintage": "initial"},
                {"period": "2026-06", "value": "301.2", "release_vintage": "initial"},
                {"period": "2026-06", "value": "301.5", "release_vintage": "revised"},
            ]
        },
        expected_period="2026-06",
        retrieved_at="2026-07-22T10:00:00Z",
        release_timestamp="2026-07-22T08:30:00Z",
    )
    assert actual["value"] == "0.5" and actual["release_vintage"] == "revised"
    assert actual["warnings"] == ["official_observation_revised"]


def event(
    release: datetime, *, baseline_unit: str = "percent", baseline_period: str = "2026-06"
) -> EconomicEvent:
    return EconomicEvent(
        event_id="cpi-release",
        name="CPI (MoM)",
        country="US",
        category="CPI",
        metric_id="headline_cpi_mom",
        normalized_event_family="CPI",
        reference_period="2026-06",
        frequency="monthly",
        date=release.date().isoformat(),
        time_utc=release,
        impact=Impact.HIGH,
        source="BLS",
        source_url="https://www.bls.gov/cpi/",
        reliability=0.99,
        event_risk_level=Impact.HIGH,
        enrichment=EventEnrichment(
            forecast="0.4",
            consensus="0.4",
            previous="0.2",
            metrics=[
                {
                    "metric_id": "headline_cpi_mom",
                    "period": baseline_period,
                    "frequency": "MoM",
                    "unit": baseline_unit,
                    "consensus": "0.4",
                }
            ],
        ),
    )


def bls_result(retrieved_at: datetime) -> ProviderResult:
    return ProviderResult(
        metadata=ProviderMetadata(
            source="BLS",
            provider_type=ProviderType.API,
            retrieved_at=retrieved_at,
            data_as_of=datetime(2026, 6, 1, tzinfo=UTC),
            freshness=Freshness.RECENT,
            reliability=0.99,
        ),
        data={
            "CUSR0000SA0": {
                "units": "index",
                "frequency": "monthly",
                "seasonal_adjustment": "SA",
                "source": "BLS",
                "source_url": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                "canonical_url": "https://www.bls.gov/developers/api_signature_v2.htm",
                "source_domain": "bls.gov",
                "provider_adapter": "BLS_OFFICIAL_API",
                "official_adapter": True,
                "observations": [
                    {"period": "2026-05", "value": "300.000", "release_vintage": "initial"},
                    {"period": "2026-06", "value": "301.500", "release_vintage": "initial"},
                ],
            }
        },
    )


@pytest.mark.parametrize(
    ("baseline_unit", "baseline_period", "compatible", "warning"),
    [
        ("percent", "2026-06", True, None),
        ("thousands of jobs", "2026-06", False, "surprise_unit_mismatch"),
        ("percent", "2026-05", False, "surprise_period_mismatch"),
    ],
)
def test_worker_persists_semantic_actual_and_only_computes_compatible_surprise(
    tmp_path: Path,
    monkeypatch,
    baseline_unit: str,
    baseline_period: str,
    compatible: bool,
    warning: str | None,
) -> None:
    cfg = settings(tmp_path)
    release = datetime.now(UTC) - timedelta(minutes=1)
    item = event(release, baseline_unit=baseline_unit, baseline_period=baseline_period)
    key = canonical_event_key(item)
    facts = MarketFactRepository(cfg)
    facts.upsert_economic_event(item, key)
    job = AIResearchJobService(cfg).enqueue_temporal_refreshes([item])[0]

    async def fetch(_self):
        return bls_result(datetime.now(UTC))

    monkeypatch.setattr("app.services.deterministic_actual_resolver.BlsProvider.fetch", fetch)
    assert AIResearchWorker(cfg, facts=facts, worker_id="semantic-worker").process_once()
    completed = AIResearchJobRepository(cfg).get(job["job_id"])
    assert completed["status"] == "SUCCEEDED", completed["last_error"]
    with connect_market_db(cfg) as conn:
        row = conn.execute(
            "SELECT * FROM economic_events_history WHERE canonical_event_key=?", (key,)
        ).fetchone()
        candidate = conn.execute(
            "SELECT * FROM event_value_candidates WHERE canonical_event_key=?", (key,)
        ).fetchone()
    assert row["actual"] == "0.5" and bool(row["actual_semantic_compatible"]) is compatible
    assert (
        row["actual_metric_id"] == "headline_cpi_mom"
        and candidate["source_series_id"] == "CUSR0000SA0"
    )
    if compatible:
        assert row["surprise_value"] == "0.1" and row["surprise_direction"] == "above_consensus"
    else:
        assert row["surprise_value"] is None and warning in json.loads(
            row["semantic_warnings_json"]
        )


def test_unsupported_official_series_returns_terminal_no_data(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    release = datetime.now(UTC) - timedelta(minutes=1)
    item = event(release)
    item.metric_id = "core_ppi_mom"
    item.category = "PPI"
    item.name = "Core PPI (MoM)"
    facts = MarketFactRepository(cfg)
    facts.upsert_economic_event(item, canonical_event_key(item))
    job = AIResearchJobService(cfg).enqueue_temporal_refreshes([item])[0]
    AIResearchWorker(cfg, facts=facts, worker_id="unsupported-worker").process_once()
    restored = AIResearchJobRepository(cfg).get(job["job_id"])
    assert restored["status"] == "NO_DATA"
    assert "official_metric_unsupported:core_ppi_mom" in restored["last_error"]


def write_fake_codex(tmp_path: Path, *, web: bool = True) -> Path:
    script = tmp_path / "fake_codex.py"
    script.write_text(
        """
import json, sys
from pathlib import Path
args = sys.argv[1:]
if '--version' in args:
    print('codex-cli 9.9.9-fake'); raise SystemExit(0)
if 'login' in args and 'status' in args:
    print('Logged in using fake test auth'); raise SystemExit(0)
if '--help' in args:
    print('exec --sandbox --cd --skip-git-repo-check --ephemeral --ignore-user-config '
          '--ignore-rules --output-schema --output-last-message --color --json """
        + ("--search" if web else "")
        + """')
    raise SystemExit(0)
prompt = sys.stdin.read()
phase = prompt.split('PHASE\\n', 1)[1].split('\\n', 1)[0]
if phase == 'SEARCH':
    print(json.dumps({'type': 'item.completed', 'item': {'type': 'web_search', 'query': 'bounded query', 'urls': ['https://www.bloomberg.com/cpi-a', 'https://www.ft.com/cpi-b']}}))
if phase == 'OPEN_SOURCE':
    for url, digest in [('https://www.bloomberg.com/cpi-a', 'hash-bloomberg'), ('https://www.ft.com/cpi-b', 'hash-ft')]:
        print(json.dumps({'type': 'item.completed', 'item': {'type': 'web_open', 'url': url, 'content_hash': digest, 'http_status': 200, 'observed_at': '2026-07-22T10:00:00Z'}}))
if phase == 'VALIDATE':
    payload = {'status': 'SUCCEEDED', 'claims': [{
        'topic': 'missing_fields', 'field_semantics': 'forecast', 'value': '0.3',
        'metric_id': 'headline_cpi_mom', 'period': '2026-06', 'frequency': 'monthly',
        'unit': 'percent', 'event_key': None, 'event_at': None, 'release_at': None,
        'issuer': None, 'symbol': 'MNQ', 'valid_from': None,
        'valid_until': None, 'published_at': None, 'retrieved_at': '2026-07-22T10:00:00Z',
        'confidence': 0.9, 'topic_status': 'SUPPORTED', 'warnings': [],
        'evidence': [
            {'query': 'bounded query', 'source_url': 'https://www.bloomberg.com/cpi-a', 'canonical_url': None, 'publisher': 'Bloomberg', 'evidence_text': 'Survey forecast is 0.3 percent.', 'published_at': None, 'retrieved_at': '2026-07-22T10:00:00Z'},
            {'query': 'bounded query', 'source_url': 'https://www.ft.com/cpi-b', 'canonical_url': None, 'publisher': 'Financial Times', 'evidence_text': 'Independent economist survey reports 0.3 percent.', 'published_at': None, 'retrieved_at': '2026-07-22T10:00:00Z'}
        ]
    }], 'missing_topics': [], 'blocking_gaps': [], 'warnings': []}
elif phase == 'PLAN':
    payload = {'status': 'COMPLETED', 'topics': ['missing_fields'], 'queries': [
        {'query': 'bounded query', 'purpose': 'find forecast', 'topic': 'missing_fields'}
    ], 'stop_conditions': ['two independent sources'], 'warnings': []}
elif phase == 'SEARCH':
    payload = {'status': 'COMPLETED', 'searches': [
        {'query': 'bounded query', 'discovered_urls': ['https://www.bloomberg.com/cpi-a', 'https://www.ft.com/cpi-b']}
    ], 'sources': [
        {'query': 'bounded query', 'source_url': 'https://www.bloomberg.com/cpi-a', 'title': 'CPI A', 'publisher': 'Bloomberg'}
    ], 'warnings': []}
elif phase == 'OPEN_SOURCE':
    payload = {'status': 'COMPLETED', 'sources': [
        {'source_url': 'https://www.bloomberg.com/cpi-a', 'canonical_url': None, 'redirect_url': None, 'publisher': 'Bloomberg', 'published_at': None, 'retrieved_at': '2026-07-22T10:00:00Z', 'http_status': 200, 'source_status': 'OPENED', 'evidence_available': True, 'content_hash': 'hash-bloomberg'}
    ], 'warnings': []}
elif phase == 'EXTRACT':
    payload = {'status': 'COMPLETED', 'claims': [], 'warnings': []}
elif phase == 'CROSS_CHECK':
    payload = {'status': 'COMPLETED', 'claims': [], 'warnings': []}
else:
    raise SystemExit(2)
output_path = Path(args[args.index('--output-last-message') + 1])
output_path.write_text(json.dumps(payload), encoding='utf-8')
print(json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': json.dumps(payload)}}))
""",
        encoding="utf-8",
    )
    command = tmp_path / "fake_codex.cmd"
    command.write_text(f'@echo off\npython "{script}" %*\n', encoding="utf-8")
    return command


def test_capability_ready_to_smoke_and_degraded_with_fake_executable(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ready_root.mkdir()
    ready_command = write_fake_codex(ready_root, web=True)
    cfg = settings(
        tmp_path / "ready-db",
        codex_cli_command=str(ready_command),
        ai_worker_enabled=True,
        ai_research_web_access_enabled=True,
    )
    report = AIResearchCapabilityService(cfg).probe()
    assert (
        report["status"] == "READY_TO_SMOKE"
        and report["executable_version"] == "codex-cli 9.9.9-fake"
    )
    assert report["authentication_available"] and report["structured_output_supported"]
    assert report["web_search_available"] is False and report["live_web_verified"] is False
    AIResearchCapabilityService(cfg).record_live_verification(
        {
            "observed_search_count": 1,
            "opened_source_count": 1,
            "source_domains": ["example.gov"],
            "run_id": "offline-fixture-proof",
        }
    )
    verified = AIResearchCapabilityService(cfg).probe()
    assert verified["status"] == "LIVE_VERIFIED"
    assert verified["web_search_available"] is True and verified["live_web_verified"] is True

    unavailable_root = tmp_path / "unavailable"
    unavailable_root.mkdir()
    unavailable_command = write_fake_codex(unavailable_root, web=False)
    unavailable = settings(
        tmp_path / "unavailable-db",
        codex_cli_command=str(unavailable_command),
        ai_worker_enabled=True,
        ai_research_web_access_enabled=True,
    )
    assert AIResearchCapabilityService(unavailable).probe()["status"] == "WEB_UNAVAILABLE"


def test_fake_agent_runs_all_persistent_steps_and_persists_claim_evidence(tmp_path: Path) -> None:
    fake_root = tmp_path / "fake"
    fake_root.mkdir()
    command = write_fake_codex(fake_root)
    cfg = settings(
        tmp_path,
        codex_cli_command=str(command),
        ai_worker_enabled=True,
        ai_research_web_access_enabled=True,
    )
    AIResearchCapabilityService(cfg).record_live_verification(
        {
            "observed_search_count": 1,
            "opened_source_count": 2,
            "source_domains": ["bloomberg.com", "ft.com"],
            "run_id": "fixture-live-run",
        }
    )
    release = datetime.now(UTC) + timedelta(hours=1)
    item = event(release)
    item.enrichment.forecast = None
    key = canonical_event_key(item)
    facts = MarketFactRepository(cfg)
    facts.upsert_economic_event(item, key)
    job = AIResearchJobService(cfg).enqueue_missing_events([item])[0]
    assert AIResearchWorker(cfg, facts=facts, worker_id="fake-agent-worker").process_once()
    completed = AIResearchJobRepository(cfg).get(job["job_id"])
    assert completed["status"] == "SUCCEEDED", completed["last_error"]
    with sqlite3.connect(cfg.database_path) as conn:
        steps = conn.execute(
            "SELECT step_name,status FROM research_run_steps ORDER BY ordinal"
        ).fetchall()
        claim = conn.execute("SELECT validation_status FROM research_claims").fetchone()
        evidence_count = conn.execute("SELECT COUNT(*) FROM research_evidence").fetchone()[0]
    assert steps == [
        (name, "COMPLETED")
        for name in (
            "PLAN",
            "SEARCH",
            "OPEN_SOURCE",
            "EXTRACT",
            "CROSS_CHECK",
            "VALIDATE",
            "PERSIST",
            "READ_BACK",
            "MATERIALIZE",
            "COMPLETE",
        )
    ]
    assert claim[0] == "accepted" and evidence_count == 2
    assert completed["result_payload"]["accepted_results"], [
        item.get("validation_reasons") for item in completed["result_payload"]["rejected_results"]
    ]
    assert completed["result_payload"]["event_projection"]["persisted_count"] == 1
    assert facts._event_history_row(key)["forecast"] == "0.3"


class ResumableExecutor:
    def __init__(self) -> None:
        self.calls: dict[str, int] = {}
        self.failed = False

    def execute_step(self, **kwargs):
        step = kwargs["step_name"]
        self.calls[step] = self.calls.get(step, 0) + 1
        if step == "CROSS_CHECK" and not self.failed:
            self.failed = True
            raise RuntimeError("simulated crash")
        return {"claims": []} if step == "VALIDATE" else {"status": "COMPLETED", "_tool_events": []}


def test_agentic_runtime_resumes_after_mid_research_crash(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    service = AIResearchJobService(cfg)
    job, _ = service.enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="resume",
        request_payload={"database_context": {}},
    )
    runtime = AgenticResearchRuntime(cfg)
    executor = ResumableExecutor()
    with pytest.raises(RuntimeError, match="simulated crash"):
        runtime.run(job, tmp_path / "workspace", executor, 10)
    result = runtime.run(job, tmp_path / "workspace", executor, 10)
    assert result["status"] == "NO_DATA"
    assert executor.calls["PLAN"] == 1 and executor.calls["CROSS_CHECK"] == 2


@pytest.mark.parametrize("terminal", ["SUCCEEDED", "NO_DATA", "REJECTED", "FAILED"])
def test_job_storm_is_prevented_after_terminal_in_same_run_window(
    tmp_path: Path, terminal: str
) -> None:
    now = [datetime(2026, 7, 22, 10, 15, tzinfo=UTC)]
    cfg = settings(tmp_path)
    repo = AIResearchJobRepository(cfg, clock=lambda: now[0])
    service = AIResearchJobService(cfg, repository=repo, clock=lambda: now[0])
    kwargs = {
        "job_type": "MNQ_MARKET_RESEARCH",
        "symbol": "MNQ",
        "request_payload": {"database_context": {"snapshot": 1}},
    }
    first, _ = service.enqueue_explicit(correlation_id="first", **kwargs)
    repo.acquire_next("worker")
    repo.complete(first["job_id"], "worker", status=terminal, result_payload={"status": terminal})

    def enqueue(index: int):
        return service.enqueue_explicit(correlation_id=f"auto-{index}", **kwargs)

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(enqueue, range(48)))
    assert {item[0]["job_id"] for item in results} == {first["job_id"]}
    assert not any(item[1] for item in results)
    forced, forced_created = service.enqueue_explicit(correlation_id="forced", force=True, **kwargs)
    assert forced_created and forced["job_id"] != first["job_id"]


def test_new_run_window_allows_new_generation(tmp_path: Path) -> None:
    now = [datetime(2026, 7, 22, 10, 15, tzinfo=UTC)]
    cfg = settings(tmp_path, ai_run_window_general_market_minutes=30)
    repo = AIResearchJobRepository(cfg, clock=lambda: now[0])
    service = AIResearchJobService(cfg, repository=repo, clock=lambda: now[0])
    kwargs = {"job_type": "MNQ_MARKET_RESEARCH", "symbol": "MNQ", "request_payload": {"x": 1}}
    first, _ = service.enqueue_explicit(correlation_id="one", **kwargs)
    repo.acquire_next("worker")
    repo.complete(first["job_id"], "worker", status="NO_DATA", result_payload={})
    now[0] += timedelta(minutes=31)
    second, created = service.enqueue_explicit(correlation_id="two", **kwargs)
    assert created and second["generation"] != first["generation"]


def test_scheduler_fingerprint_returns_not_required_and_supports_event_triggers(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, research_daily_budget_runs=10)
    MarketContextSnapshotRepository(cfg).save_next(
        symbol="MNQ",
        refresh_mode="test",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": "2026-07-22T10:00:00Z",
            "market_schedule": {"context_date": "2026-07-22", "market_session_status": "open"},
            "event_calendar": {},
            "news_context": {},
            "quality": {},
            "data_quality": {},
        },
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    scheduler = ResearchSchedulerService(cfg)
    first = scheduler.evaluate("premarket")
    second = scheduler.evaluate("premarket")
    assert first["decision"] == "QUEUED"
    assert (
        second["decision"] == "NOT_REQUIRED" and second["reason"] == "input_fingerprint_unchanged"
    )


def test_scheduler_pre_event_and_post_release_enqueue_exact_event_work(tmp_path: Path) -> None:
    cfg = settings(tmp_path, research_daily_budget_runs=10, research_max_concurrent_jobs=4)
    future = event(datetime.now(UTC) + timedelta(hours=1))
    future.enrichment.forecast = None
    released = event(datetime.now(UTC) - timedelta(minutes=2))
    released.event_id = "released-cpi"
    released.reference_period = "2026-05"
    released.enrichment.metrics[0]["period"] = "2026-05"
    MarketContextSnapshotRepository(cfg).save_next(
        symbol="MNQ",
        refresh_mode="test",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "market_schedule": {"context_date": "2026-07-22", "market_session_status": "open"},
            "event_calendar": {
                "critical_macro_events": [
                    future.model_dump(mode="json"),
                    released.model_dump(mode="json"),
                ],
                "fed_communications": [],
                "other_economic_events": [],
            },
            "news_context": {},
            "quality": {},
            "data_quality": {},
        },
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    scheduler = ResearchSchedulerService(cfg)
    pre = scheduler.evaluate("pre_event")
    post = scheduler.evaluate("post_release")
    assert pre["decision"] == post["decision"] == "QUEUED"
    repo = AIResearchJobRepository(cfg)
    assert repo.get(pre["job_id"])["job_type"] == "MISSING_EVENT_RESEARCH"
    assert repo.get(post["job_id"])["job_type"] == "RELEASE_ACTUAL_REFRESH"


def test_scheduler_ignores_new_snapshot_revision_with_same_substantive_content(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, research_daily_budget_runs=10)
    snapshots = MarketContextSnapshotRepository(cfg)
    debug = {
        "symbol": "MNQ",
        "generated_at_utc": "2026-07-22T10:00:00Z",
        "market_schedule": {"context_date": "2026-07-22", "market_session_status": "open"},
        "event_calendar": {},
        "news_context": {},
        "quality": {},
        "data_quality": {},
    }
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="test",
        debug_payload=debug,
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    scheduler = ResearchSchedulerService(cfg)
    first = scheduler.evaluate("premarket")
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="test",
        debug_payload={**debug, "generated_at_utc": "2026-07-22T10:01:00Z"},
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    second = scheduler.evaluate("premarket")
    assert first["decision"] == "QUEUED"
    assert (
        second["decision"] == "NOT_REQUIRED" and second["reason"] == "input_fingerprint_unchanged"
    )


def test_event_outside_pre_event_window_does_not_enqueue(tmp_path: Path) -> None:
    cfg = settings(
        tmp_path,
        research_daily_budget_runs=10,
        research_pre_event_window_minutes=60,
    )
    far_future = event(datetime.now(UTC) + timedelta(hours=3))
    far_future.enrichment.forecast = None
    MarketContextSnapshotRepository(cfg).save_next(
        symbol="MNQ",
        refresh_mode="test",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "market_schedule": {"context_date": "2026-07-22", "market_session_status": "open"},
            "event_calendar": {
                "critical_macro_events": [far_future.model_dump(mode="json")],
                "fed_communications": [],
                "other_economic_events": [],
            },
            "news_context": {},
            "quality": {},
            "data_quality": {},
        },
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    decision = ResearchSchedulerService(cfg).evaluate("pre_event")
    assert decision["decision"] == "NOT_REQUIRED" and decision["reason"] == "no_eligible_event_work"
    assert AIResearchJobRepository(cfg).latest(limit=10) == []


def test_async_market_research_api_enqueues_once_without_running_agent(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    MarketContextSnapshotRepository(cfg).save_next(
        symbol="MNQ",
        refresh_mode="test",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": "2026-07-22T10:00:00Z",
            "market_schedule": {"context_date": "2026-07-22", "market_session_status": "open"},
        },
        ai_enrichment={"status": "NOT_REQUIRED", "job_ids": []},
    )
    dependency = SimpleNamespace(settings=cfg)
    request = MarketResearchRunRequest(correlation_id="offline-api-test")
    first = asyncio.run(routes.enqueue_mnq_market_research(request, dependency))
    second = asyncio.run(routes.enqueue_mnq_market_research(request, dependency))
    assert first["created"] is True and second["created"] is False
    assert first["run_id"] == second["run_id"] and first["job_id"] == second["job_id"]
    assert asyncio.run(routes.mnq_market_research_status(dependency))["status"] == "PENDING"
    with sqlite3.connect(cfg.database_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM ai_research_jobs").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM research_run_steps").fetchone()[0] == 0


def test_additive_migration_from_v8_preserves_rows_and_adds_runtime(tmp_path: Path) -> None:
    database = tmp_path / "schema8.sqlite"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT,applied_at TEXT)"
        )
        for version, (name, sql) in enumerate(MIGRATIONS[:8], start=1):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)", (version, name, "2026-07-22")
            )
        conn.execute("PRAGMA user_version=8")
        conn.execute(
            "INSERT INTO market_news(news_key,title,source_url,retrieved_at) VALUES ('preserved','Preserved','https://example.com','2026-01-01')"
        )
        conn.commit()
    assert migrate_database(database)["schema_version"] == 14
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute("SELECT title FROM market_news WHERE news_key='preserved'").fetchone()[0]
            == "Preserved"
        )
        tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert {"research_runs", "research_run_steps", "research_claims", "research_evidence"} <= tables


def test_additive_migration_from_v9_preserves_rows_and_adds_verified_runtime(
    tmp_path: Path,
) -> None:
    database = tmp_path / "schema9.sqlite"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT,applied_at TEXT)"
        )
        for version, (name, sql) in enumerate(MIGRATIONS[:9], start=1):
            for statement in _split_sql(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)", (version, name, "2026-07-22")
            )
        conn.execute("PRAGMA user_version=9")
        conn.execute(
            "INSERT INTO market_news(news_key,title,source_url,retrieved_at) VALUES ('v9-preserved','V9','https://example.com','2026-01-01')"
        )
        conn.commit()
    assert migrate_database(database)["schema_version"] == 14
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute("SELECT title FROM market_news WHERE news_key='v9-preserved'").fetchone()[
                0
            ]
            == "V9"
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(research_runs)")}
    assert {"search_count", "opened_source_count", "usage_json", "cost_json"} <= columns


def test_tool_usage_and_cost_are_persisted_only_when_cli_provides_them(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    service = AIResearchJobService(cfg)
    repository = ResearchRuntimeRepository(cfg)
    job, _ = service.enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="usage-provided",
        request_payload={"database_context": {}},
    )
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v1")
    step, _ = repository.begin_step(
        run["run_id"],
        "SEARCH",
        2,
        {},
        backend="fixture",
        tool="fixture",
    )
    repository.record_tool_events(
        run["run_id"],
        step["step_id"],
        [{"event_type": "search", "query": "bounded query"}],
        usage={"input_tokens": 12, "output_tokens": 7, "cost_usd": 0.004},
    )
    with sqlite3.connect(cfg.database_path) as conn:
        usage_json, cost_json = conn.execute(
            "SELECT usage_json,cost_json FROM research_runs WHERE run_id=?", (run["run_id"],)
        ).fetchone()
    assert json.loads(usage_json) == {"input_tokens": 12, "output_tokens": 7, "cost_usd": 0.004}
    assert json.loads(cost_json) == {"cost_usd": 0.004}

    no_usage_job, _ = service.enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="usage-unavailable",
        request_payload={"database_context": {"revision": 2}},
        force=True,
    )
    no_usage_run = repository.ensure_run(
        no_usage_job,
        "MNQ_MARKET_RESEARCH",
        "mnq_market_research_v1",
    )
    no_usage_step, _ = repository.begin_step(
        no_usage_run["run_id"],
        "SEARCH",
        2,
        {},
        backend="fixture",
        tool="fixture",
    )
    repository.record_tool_events(
        no_usage_run["run_id"],
        no_usage_step["step_id"],
        [{"event_type": "search", "query": "another bounded query"}],
    )
    with sqlite3.connect(cfg.database_path) as conn:
        row = conn.execute(
            "SELECT usage_json,cost_json FROM research_runs WHERE run_id=?",
            (no_usage_run["run_id"],),
        ).fetchone()
    assert row == (None, None)


def test_evidence_deduplicates_exact_rows_and_requires_distinct_content(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    service = AIResearchJobService(cfg)
    repository = ResearchRuntimeRepository(cfg)
    job, _ = service.enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="dedup",
        request_payload={"database_context": {"revision": 1}},
    )
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v1")
    base = {
        "topic": "macro",
        "field_semantics": "consensus",
        "value": "0.3",
        "metric_id": "headline_cpi_mom",
        "period": "2026-06",
        "frequency": "monthly",
        "unit": "percent",
    }
    bloomberg = {
        "source_url": "https://www.bloomberg.com/cpi",
        "publisher": "Bloomberg",
        "evidence_text": "Bloomberg survey consensus is 0.3 percent.",
        "retrieved_at": "2026-07-22T10:00:00Z",
    }
    ft = {
        "source_url": "https://www.ft.com/cpi",
        "publisher": "Financial Times",
        "evidence_text": "FT survey independently reports 0.3 percent.",
        "retrieved_at": "2026-07-22T10:00:00Z",
    }
    observe_sources(repository, run, [bloomberg["source_url"], ft["source_url"]])
    accepted = repository.persist_claims(run, [{**base, "evidence": [bloomberg, bloomberg, ft]}])
    assert accepted["accepted_count"] == 1 and accepted["evidence_count"] == 2

    second_job, _ = service.enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="syndicated",
        request_payload={"database_context": {"revision": 2}},
        force=True,
    )
    second_run = repository.ensure_run(second_job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v1")
    observe_sources(repository, second_run, [bloomberg["source_url"], ft["source_url"]])
    syndicated_text = "The same syndicated consensus text is 0.3 percent."
    rejected = repository.persist_claims(
        second_run,
        [
            {
                **base,
                "evidence": [
                    {**bloomberg, "evidence_text": syndicated_text},
                    {**ft, "evidence_text": syndicated_text},
                ],
            }
        ],
    )
    assert rejected["accepted_count"] == 0
    assert "insufficient_independent_evidence" in rejected["rejected_claims"][0]["warnings"]


def test_claim_evidence_recalculates_tier_and_rejects_invented_confirmation(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    job, _ = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="evidence",
        request_payload={"database_context": {}},
    )
    repository = ResearchRuntimeRepository(cfg)
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v1")
    claim = {
        "topic": "macro",
        "field_semantics": "consensus",
        "value": "0.3",
        "metric_id": "headline_cpi_mom",
        "period": "2026-06",
        "frequency": "monthly",
        "unit": "percent",
        "confirmation_count": 99,
        "source_tier": 1,
        "evidence": [
            {
                "source_url": "https://www.investing.com/cpi",
                "publisher": "Investing",
                "evidence_text": "Consensus is 0.3 percent.",
                "retrieved_at": "2026-07-22T10:00:00Z",
            }
        ],
    }
    observe_sources(repository, run, ["https://www.investing.com/cpi"])
    result = repository.persist_claims(run, [claim])
    assert result["status"] == "NO_DATA" and result["accepted_count"] == 0
    assert "insufficient_independent_evidence" in result["rejected_claims"][0]["warnings"]
    with sqlite3.connect(cfg.database_path) as conn:
        evidence = conn.execute(
            "SELECT source_tier,source_domain FROM research_evidence"
        ).fetchone()
    assert evidence == (4, "investing.com")


def test_partial_when_only_some_claims_pass_evidence_policy(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    job, _ = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="partial",
        request_payload={"database_context": {}},
    )
    repository = ResearchRuntimeRepository(cfg)
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v1")
    valid = {
        "topic": "risk",
        "field_semantics": "outcome",
        "value": "verified context",
        "evidence": [
            {
                "source_url": "https://www.reuters.com/markets/a",
                "publisher": "Reuters",
                "evidence_text": "Verified market context.",
                "retrieved_at": "2026-07-22T10:00:00Z",
            }
        ],
    }
    invalid = {
        "topic": "risk",
        "field_semantics": "news",
        "value": "unsupported",
        "evidence": [
            {
                "source_url": "https://unknown.example/a",
                "publisher": "Unknown",
                "evidence_text": "Unsupported claim.",
                "retrieved_at": "2026-07-22T10:00:00Z",
            }
        ],
    }
    observe_sources(repository, run, ["https://www.reuters.com/markets/a"])
    result = repository.persist_claims(run, [valid, invalid])
    assert result["status"] == "PARTIAL" and result["accepted_count"] == 1


def test_fred_fallback_is_never_promoted_to_official_bls_actual(
    tmp_path: Path, monkeypatch
) -> None:
    cfg = settings(tmp_path)
    release = datetime.now(UTC) - timedelta(hours=3)
    item = event(release)
    key = canonical_event_key(item)
    job = AIResearchJobService(cfg).enqueue_temporal_refreshes([item])[0]

    async def fred_fallback(_self):
        return ProviderResult(
            metadata=ProviderMetadata(
                source="FRED fallback for unavailable BLS transport",
                provider_type=ProviderType.API,
                retrieved_at=datetime.now(UTC),
                data_as_of=datetime(2026, 6, 1, tzinfo=UTC),
                freshness=Freshness.RECENT,
                reliability=0.9,
                is_fallback=True,
            ),
            data={
                "CUSR0000SA0": {
                    "observations": observations(["300", "301.5"], year=2026, start_month=5),
                    "frequency": "monthly",
                    "seasonal_adjustment": "SA",
                    "units": "index",
                    "source": "FRED",
                    "source_url": "https://api.stlouisfed.org/fred/series/observations",
                    "canonical_url": "https://fred.stlouisfed.org/series/CPIAUCSL",
                    "source_domain": "fred.stlouisfed.org",
                    "provider_adapter": "FRED_FALLBACK_API",
                    "official_adapter": False,
                }
            },
        )

    monkeypatch.setattr(
        "app.services.deterministic_actual_resolver.BlsProvider.fetch", fred_fallback
    )
    result = DeterministicActualResolver(cfg)(job, tmp_path / "workspace", 10)
    assert result["status"] == "OFFICIAL_FEED_DELAYED" and result["retryable"] is True
    assert "FRED_FALLBACK_API" in result["error"]
    with sqlite3.connect(cfg.database_path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM event_value_candidates WHERE canonical_event_key=?", (key,)
            ).fetchone()[0]
            == 0
        )


def test_official_feed_delay_remains_retryable_beyond_old_two_hour_window(tmp_path: Path) -> None:
    cfg = settings(tmp_path, official_feed_delay_hours=24)
    release = datetime.now(UTC) - timedelta(hours=3)
    item = event(release)
    key = canonical_event_key(item)
    facts = MarketFactRepository(cfg)
    facts.upsert_economic_event(item, key)
    job = AIResearchJobService(cfg).enqueue_temporal_refreshes([item])[0]

    def delayed(_job, _workspace, _timeout):
        return {
            "status": "OFFICIAL_FEED_DELAYED",
            "retryable": True,
            "results": [],
            "error": "period_mismatch",
        }

    worker = AIResearchWorker(
        cfg, facts=facts, actual_resolver=delayed, worker_id="delayed-official"
    )
    assert worker.process_once()
    restored = AIResearchJobRepository(cfg).get(job["job_id"])
    assert restored["status"] == "RETRY_SCHEDULED"
    assert restored["retry_class"] == "OFFICIAL_ACTUAL"
    assert datetime.fromisoformat(restored["retry_deadline_at"]) > datetime.now(UTC) + timedelta(
        hours=20
    )
    assert restored["last_retry_reason"].startswith("OFFICIAL_FEED_DELAYED:")
    assert facts._event_history_row(key)["temporal_status"] == "AWAITING_ACTUAL"


class NullEvidenceVerifier:
    def verify(self, evidence):
        return None


class ClaimExecutor:
    def __init__(self, *, observed: bool) -> None:
        self.observed = observed

    def execute_step(self, **kwargs):
        step = kwargs["step_name"]
        if step == "OPEN_SOURCE" and self.observed:
            return {
                "status": "COMPLETED",
                "_tool_events": [
                    {
                        "event_type": "open_source",
                        "source_url": url,
                        "canonical_url": url,
                        "content_hash": f"hash-{index}",
                        "http_status": 200,
                        "observed_at": "2026-07-22T10:00:00Z",
                    }
                    for index, url in enumerate(
                        ["https://www.bloomberg.com/verified", "https://www.ft.com/verified"]
                    )
                ],
            }
        if step == "VALIDATE":
            return {
                "claims": [
                    {
                        "topic": "macro",
                        "field_semantics": "consensus",
                        "value": "0.3",
                        "metric_id": "headline_cpi_mom",
                        "period": "2026-06",
                        "frequency": "monthly",
                        "unit": "percent",
                        "evidence": [
                            {
                                "source_url": "https://www.bloomberg.com/verified",
                                "publisher": "Bloomberg",
                                "evidence_text": "Verified consensus is 0.3 percent.",
                            },
                            {
                                "source_url": "https://www.ft.com/verified",
                                "publisher": "Financial Times",
                                "evidence_text": "Independent verified consensus is 0.3 percent.",
                            },
                        ],
                    }
                ]
            }
        return {"status": "COMPLETED", "_tool_events": []}


@pytest.mark.parametrize(("observed", "accepted_count"), [(False, 0), (True, 1)])
def test_ai_evidence_requires_actual_open_source_telemetry(
    tmp_path: Path,
    observed: bool,
    accepted_count: int,
) -> None:
    cfg = settings(tmp_path)
    job, _ = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id=f"observed-{observed}",
        request_payload={"database_context": {}},
    )
    runtime = AgenticResearchRuntime(cfg, verifier=NullEvidenceVerifier())
    result = runtime.run(job, tmp_path / "workspace", ClaimExecutor(observed=observed), 30)
    assert result["accepted_count"] == accepted_count
    if observed:
        assert result["status"] == "PARTIAL" and result["evidence_count"] == 2
    else:
        assert result["status"] == "NO_DATA"
        assert "source_not_observed_or_opened" in result["rejected_claims"][0]["warnings"]


def _topic_claim(topic: str) -> dict:
    return {
        "topic": topic,
        "field_semantics": "outcome",
        "value": f"verified {topic}",
        "evidence": [
            {
                "source_url": "https://www.reuters.com/markets/topic-coverage",
                "publisher": "Reuters",
                "evidence_text": f"Verified evidence for required topic {topic}.",
                "retrieved_at": "2026-07-22T10:00:00Z",
            }
        ],
    }


def test_topic_completeness_is_server_calculated_for_partial_and_success(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    service = AIResearchJobService(cfg)
    repository = ResearchRuntimeRepository(cfg)
    job, _ = service.enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="one-topic",
        request_payload={"database_context": {"version": 1}},
    )
    run = repository.ensure_run(job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v1")
    observe_sources(repository, run, ["https://www.reuters.com/markets/topic-coverage"])
    partial = repository.persist_claims(run, [_topic_claim("macro")])
    assert partial["status"] == "PARTIAL" and partial["coverage_score"] == 0.1
    assert len(partial["missing_topics"]) == 9 and partial["completed_topics"] == ["macro"]

    full_job, _ = service.enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="all-topics",
        request_payload={"database_context": {"version": 2}},
        force=True,
    )
    full_run = repository.ensure_run(full_job, "MNQ_MARKET_RESEARCH", "mnq_market_research_v1")
    observe_sources(repository, full_run, ["https://www.reuters.com/markets/topic-coverage"])
    complete = repository.persist_claims(
        full_run, [_topic_claim(topic) for topic in full_run["required_topics"]]
    )
    assert complete["status"] == "SUCCEEDED" and complete["coverage_score"] == 1.0
    assert complete["missing_topics"] == []
    assert (
        complete["accepted_count"]
        == complete["persisted_count"]
        == complete["read_back_count"]
        == 10
    )


class AdvancingClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class DeadlineExecutor:
    def __init__(self, clock: AdvancingClock) -> None:
        self.clock = clock
        self.watchdogs: list[int] = []

    def execute_step(self, **kwargs):
        self.watchdogs.append(kwargs["watchdog_seconds"])
        self.clock.value += 2.0
        return {"status": "COMPLETED", "_tool_events": []}


def test_agentic_deadline_is_global_not_reapplied_per_phase(tmp_path: Path) -> None:
    cfg = settings(tmp_path)
    job, _ = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="deadline",
        request_payload={"database_context": {}},
    )
    clock = AdvancingClock()
    executor = DeadlineExecutor(clock)
    result = AgenticResearchRuntime(cfg, monotonic=clock, verifier=NullEvidenceVerifier()).run(
        job,
        tmp_path / "deadline",
        executor,
        5,
    )
    assert result["status"] == "CHECKPOINTED"
    assert result["continuation_required"] is True
    assert executor.watchdogs == [5, 3, 1]


class ToolBudgetExecutor:
    def execute_step(self, **kwargs):
        return {
            "status": "COMPLETED",
            "queries": ["model-array-does-not-count"] * 99,
            "_tool_events": [{"event_type": "search", "query": kwargs["step_name"]}],
        }


def test_observed_tool_budget_is_cumulative_across_run(tmp_path: Path) -> None:
    cfg = settings(
        tmp_path,
        research_max_searches=2,
        research_budget_mode="enforce",
    )
    job, _ = AIResearchJobService(cfg).enqueue_explicit(
        job_type="MNQ_MARKET_RESEARCH",
        symbol="MNQ",
        correlation_id="budget",
        request_payload={"database_context": {}},
    )
    runtime = AgenticResearchRuntime(cfg, verifier=NullEvidenceVerifier())
    with pytest.raises(
        ResearchBudgetExceeded,
        match="research_budget_exceeded:searches",
    ):
        runtime.run(job, tmp_path / "budget", ToolBudgetExecutor(), 30)
    run = runtime.repository.latest("MNQ")
    assert run["search_count"] == 3
    assert run["steps"][2]["diagnostic"]["category"] == "BUDGET_EXCEEDED"
