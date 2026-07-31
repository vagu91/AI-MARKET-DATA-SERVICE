from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.core.config import Settings
from app.models.common import Impact
from app.models.events import EconomicEvent
from app.providers.ai_researcher_provider import (
    AIResearcherProvider,
    build_codex_research_prompt,
    parse_json_from_stdout,
)
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.event_enrichment_service import EventEnrichmentService
from app.services.market_fact_repository import MarketFactRepository


def _attestation(
    command: list[str],
    output_path: Path,
    *,
    stdout: str = "",
    stderr: str = "",
    exit_code: int = 0,
) -> dict[str, object]:
    output = output_path.read_bytes() if output_path.is_file() else b""
    return {
        "process_observed": True,
        "process_id": 123,
        "exit_code": exit_code,
        "stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr.encode()).hexdigest(),
        "output_sha256": hashlib.sha256(output).hexdigest(),
        "output_size_bytes": len(output),
        "output_observed": output_path.is_file(),
        "command_sha256": hashlib.sha256(
            json.dumps(command).encode()
        ).hexdigest(),
        "process_terminated": True,
    }


def settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_path": tmp_path / "market.sqlite",
        "codex_workspace_dir": tmp_path / "workspace",
        "enable_ai_researcher": True,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def payload(fact_key="US:CPI:2099-07-14:consumer_price_index:macro_event_enrichment"):
    return {
        "generated_at": "2099-07-10T00:00:00+00:00",
        "results": [
            {
                "fact_key": fact_key,
                "country": "US",
                "date": "2099-07-14",
                "time_utc": "2099-07-14T12:30:00+00:00",
                "category": "CPI",
                "event_name": "Consumer Price Index",
                "period": "July 2099",
                "metric_id": "headline_cpi_mom",
                "forecast": "0.3%",
                "previous": "0.2%",
                "consensus": None,
                "actual": None,
                "unit": "percent",
                "frequency": "monthly",
                "source": "Reuters",
                "source_url": "https://reuters.test/cpi",
                "extracted_text": None,
                "evidence_text": "Reuters reported the July 2099 headline CPI forecast at 0.3% and previous at 0.2%.",
                "reliability": 0.7,
                "confidence": 0.7,
                "valid_until": "2099-07-14T12:30:00+00:00",
                "notes": None,
                "warnings": [],
                "metrics": [],
                "fomc_context": None,
            }
        ],
    }


def event() -> EconomicEvent:
    return EconomicEvent(
        event_id="evt-cpi",
        name="Consumer Price Index",
        country="US",
        category="CPI",
        date="2099-07-14",
        time_utc=datetime(2099, 7, 14, 12, 30, tzinfo=UTC),
        time_local=datetime(2099, 7, 14, 14, 30, tzinfo=UTC),
        impact=Impact.HIGH,
        source="BLS",
        source_url="https://bls.test",
        reliability=0.9,
        event_risk_level=Impact.HIGH,
    )


def research_events(count=5):
    events = []
    categories = ["CPI", "PPI", "NFP", "GDP", "PCE"]
    for index in range(count):
        category = categories[index]
        fact_key = f"US:{category}:2099-07-{14 + index}:fixture_{category.lower()}:macro_event_enrichment"
        events.append(
            {
                "fact_key": fact_key,
                "country": "US",
                "date": f"2099-07-{14 + index}",
                "time_utc": f"2099-07-{14 + index}T12:30:00+00:00",
                "category": category,
                "event_name": f"{category} fixture",
                "valid_until": f"2099-07-{14 + index}T12:30:00+00:00",
            }
        )
    return events


def test_build_codex_research_prompt_contains_full_template_input_and_schema():
    template = "\n".join(
        [
            "Sei AI Researcher data-only per AI-MARKET-DATA-SERVICE.",
            "Devi ricercare forecast, previous, consensus e actual.",
            "source_url valid_until",
        ]
    )
    research_input = {"generated_at": "2099-07-10T00:00:00+00:00", "events": research_events(5)}
    prompt = build_codex_research_prompt(template, research_input)
    assert len(prompt) > 2000
    assert "forecast" in prompt
    assert "previous" in prompt
    assert "consensus" in prompt
    assert "source_url" in prompt
    assert "evidence_text" in prompt
    assert "valid_until" in prompt
    assert '"results"' in prompt
    assert "Restituisci esclusivamente JSON" in prompt
    for item in research_input["events"]:
        assert item["fact_key"] in prompt


def test_parse_stdout_pure_json_and_fenced_json():
    pure, error = parse_json_from_stdout(json.dumps(payload()))
    assert error is None
    assert pure["results"][0]["forecast"] == "0.3%"

    fenced, error = parse_json_from_stdout("```json\n" + json.dumps(payload()) + "\n```")
    assert error is None
    assert fenced["results"][0]["source_url"] == "https://reuters.test/cpi"


def test_codex_command_contains_skip_flag_and_writes_stdout_json(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg = settings(
        tmp_path,
        codex_cli_command="codex",
        codex_workspace_dir=Path("relative-legacy-workspace"),
        timeout_ai_research_seconds=20,
        codex_research_timeout_seconds=60,
    )
    provider = AIResearcherProvider(cfg)
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        output_index = command.index("--output-last-message")
        Path(command[output_index + 1]).write_text(
            json.dumps(payload()),
            encoding="utf-8",
        )
        stdout = json.dumps(payload())
        completed = subprocess.CompletedProcess(
            command,
            0,
            stdout=stdout,
            stderr="",
        )
        return (
            completed,
            _attestation(
                command,
                kwargs["output_path"],
                stdout=stdout,
            ),
            None,
        )

    monkeypatch.setattr(
        "app.providers.ai_researcher_provider._run_attested_codex_subprocess",
        fake_run,
    )
    monkeypatch.setattr("app.providers.ai_researcher_provider._resolve_command", lambda command: [command])
    facts, status = provider._codex_cli([{"fact_key": "x"}])

    assert facts
    assert "exec" in calls[0][0]
    exec_index = calls[0][0].index("exec")
    assert calls[0][0][exec_index : exec_index + 2] == ["exec", "--skip-git-repo-check"]
    for flag in (
        "--ignore-user-config",
        "--ignore-rules",
        "--search",
        "--json",
        "--output-schema",
        "--output-last-message",
    ):
        assert flag in calls[0][0]
    assert calls[0][0][-1] == "-"
    full_prompt = calls[0][1]["prompt"]
    assert len(full_prompt) > 2000
    assert "forecast" in full_prompt
    assert "previous" in full_prompt
    assert "consensus" in full_prompt
    assert "source_url" in full_prompt
    assert "valid_until" in full_prompt
    assert "restituisci esclusivamente json" in full_prompt.lower()
    assert full_prompt not in calls[0][0]
    assert calls[0][1]["timeout_seconds"] == 18.0
    assert calls[0][1]["timeout_seconds"] < cfg.timeout_ai_research_seconds
    cwd = Path(calls[0][1]["cwd"])
    assert cwd.is_absolute() and cwd == cfg.codex_workspace_dir.resolve()
    for path_flag in ("--cd", "--output-schema", "--output-last-message"):
        value = Path(calls[0][0][calls[0][0].index(path_flag) + 1])
        assert value.is_absolute()
        if path_flag != "--cd":
            assert value.parent == cwd
    assert (cfg.codex_workspace_dir / "research_output.json").exists()
    assert status["exit_code"] == 0


def test_legacy_codex_adapter_attests_real_local_process_offline(
    tmp_path,
    monkeypatch,
):
    expected_payload = json.dumps(payload())

    def local_command(_prefix, *, output_path, **_kwargs):
        script = (
            "import pathlib,sys;"
            f"pathlib.Path(sys.argv[1]).write_text({expected_payload!r},encoding='utf-8');"
            "print('offline-codex-fixture')"
        )
        return [sys.executable, "-c", script, str(output_path)]

    monkeypatch.setattr(
        "app.providers.ai_researcher_provider.build_codex_exec_command",
        local_command,
    )
    monkeypatch.setattr(
        "app.providers.ai_researcher_provider.validate_isolated_command",
        lambda *_args, **_kwargs: None,
    )
    provider = AIResearcherProvider(
        settings(
            tmp_path,
            codex_cli_command=sys.executable,
            timeout_ai_research_seconds=10,
            codex_research_timeout_seconds=5,
        )
    )

    facts, status = provider._codex_cli([{"fact_key": "x"}])

    output_path = Path(status["output_path"])
    assert facts
    assert status["process_observed"] is True
    assert type(status["process_id"]) is int and status["process_id"] > 0
    assert status["process_terminated"] is True
    assert status["exit_code"] == 0
    assert status["output_observed"] is True
    assert status["output_size_bytes"] == output_path.stat().st_size
    assert status["output_sha256"] == hashlib.sha256(
        output_path.read_bytes()
    ).hexdigest()
    for field in (
        "stdout_sha256",
        "stderr_sha256",
        "command_sha256",
    ):
        assert len(status[field]) == 64
    assert not any(
        thread.name.startswith("legacy-codex-stdin-")
        for thread in threading.enumerate()
    )


async def test_legacy_codex_adapter_attests_and_reaps_real_local_timeout(
    tmp_path,
    monkeypatch,
):
    def local_command(_prefix, *, output_path, **_kwargs):
        del output_path
        return [
            sys.executable,
            "-c",
            "import time; print('spawned', flush=True); time.sleep(30)",
        ]

    monkeypatch.setattr(
        "app.providers.ai_researcher_provider.build_codex_exec_command",
        local_command,
    )
    monkeypatch.setattr(
        "app.providers.ai_researcher_provider.validate_isolated_command",
        lambda *_args, **_kwargs: None,
    )
    provider = AIResearcherProvider(
        settings(
            tmp_path,
            codex_cli_command=sys.executable,
            timeout_ai_research_seconds=2,
            codex_research_timeout_seconds=1,
        )
    )

    facts, status = await provider.research([{"fact_key": "x"}])

    assert facts == []
    assert status["failure_reason"] == "codex_cli_timeout"
    assert status["process_observed"] is True
    assert type(status["process_id"]) is int and status["process_id"] > 0
    assert status["process_terminated"] is True
    assert status["output_observed"] is False
    assert status["output_size_bytes"] == 0
    assert status["output_sha256"] == hashlib.sha256(b"").hexdigest()
    for field in (
        "stdout_sha256",
        "stderr_sha256",
        "command_sha256",
    ):
        assert len(status[field]) == 64
    assert not any(
        thread.name.startswith("legacy-codex-stdin-")
        for thread in threading.enumerate()
    )


def test_generic_non_json_response_reports_did_not_execute_research(tmp_path, monkeypatch):
    provider = AIResearcherProvider(settings(tmp_path))

    def fake_run(command, **kwargs):
        stdout = "Ricevuto. Opererò come AI Researcher data-only per AI-MARKET-DATA-SERVICE."
        completed = subprocess.CompletedProcess(
            command,
            0,
            stdout=stdout,
            stderr="",
        )
        return (
            completed,
            _attestation(
                command,
                kwargs["output_path"],
                stdout=stdout,
            ),
            None,
        )

    monkeypatch.setattr(
        "app.providers.ai_researcher_provider._run_attested_codex_subprocess",
        fake_run,
    )
    monkeypatch.setattr("app.providers.ai_researcher_provider._resolve_command", lambda command: [command])
    facts, status = provider._codex_cli(research_events(5))
    assert facts == []
    assert status["failure_reason"] == "codex_did_not_execute_research"
    assert status["prompt_length_chars"] > 2000
    assert status["prompt_contains_input"] is True
    assert status["input_event_count"] == 5


def test_codex_non_zero_exit_reports_failure_reason(tmp_path, monkeypatch):
    provider = AIResearcherProvider(settings(tmp_path))

    def fake_run(command, **kwargs):
        completed = subprocess.CompletedProcess(
            command,
            2,
            stdout="",
            stderr="boom",
        )
        return (
            completed,
            _attestation(
                command,
                kwargs["output_path"],
                stderr="boom",
                exit_code=2,
            ),
            None,
        )

    monkeypatch.setattr(
        "app.providers.ai_researcher_provider._run_attested_codex_subprocess",
        fake_run,
    )
    monkeypatch.setattr("app.providers.ai_researcher_provider._resolve_command", lambda command: [command])
    facts, status = provider._codex_cli([{"fact_key": "x"}])
    assert facts == []
    assert status["failure_reason"] == "codex_cli_non_zero_exit"


async def test_batch_of_five_events_is_one_codex_call_and_next_run_db_hit(tmp_path, monkeypatch):
    cfg = settings(tmp_path)
    calls = 0

    def fake_run(command, **kwargs):
        nonlocal calls
        calls += 1
        fact_key = EnrichmentOrchestrator(cfg).fact_key(event())
        stdout = json.dumps(payload(fact_key))
        completed = subprocess.CompletedProcess(
            command,
            0,
            stdout=stdout,
            stderr="",
        )
        return (
            completed,
            _attestation(
                command,
                kwargs["output_path"],
                stdout=stdout,
            ),
            None,
        )

    monkeypatch.setattr(
        "app.providers.ai_researcher_provider._run_attested_codex_subprocess",
        fake_run,
    )
    monkeypatch.setattr("app.providers.ai_researcher_provider._resolve_command", lambda command: [command])

    class EmptyProvider:
        async def enrich_events(self, events, country, start, end):
            return events, {}

    orchestrator = EnrichmentOrchestrator(cfg, event_enrichment_service=EmptyProvider())
    await orchestrator.enrich_events(
        events=[event(), event().model_copy(update={"event_id": "evt-cpi-2"})],
        country="US",
        start=datetime.now(UTC),
        end=datetime.now(UTC) + timedelta(days=7),
        trigger="test",
    )
    assert calls == 0
    queued = orchestrator.ai_jobs.repository.latest(symbol="MNQ")
    assert queued == []

    await orchestrator.enrich_events(
        events=[event()],
        country="US",
        start=datetime.now(UTC),
        end=datetime.now(UTC) + timedelta(days=7),
        trigger="test",
    )
    assert calls == 0
    assert orchestrator.ai_jobs.repository.latest(symbol="MNQ") == []


def test_no_data_ai_result_creates_negative_cache(tmp_path):
    cfg = settings(tmp_path)
    provider = AIResearcherProvider(cfg)
    fact_key = EnrichmentOrchestrator(cfg).fact_key(event())
    no_data = payload(fact_key)
    item = no_data["results"][0]
    item.update({"forecast": None, "previous": None, "source": None, "source_url": None, "reliability": 0, "confidence": 0})
    facts, status = provider.load_payload(no_data)
    assert facts[0]["status"] == "no_data_available"
    MarketFactRepository(cfg).upsert_fact(facts[0])
    stored = MarketFactRepository(cfg).get_fact(fact_key)
    assert stored["valid_until"] is not None


def test_metric_only_ai_result_counts_as_valid_fact(tmp_path):
    provider = AIResearcherProvider(settings(tmp_path))
    data = payload()
    item = data["results"][0]
    item.update(
        {
            "forecast": None,
            "previous": None,
            "consensus": None,
            "actual": None,
            "source": None,
            "source_url": None,
            "metrics": [
                {
                    "metric_id": "headline_cpi_mom",
                    "frequency": "MoM",
                    "unit": "percent",
                    "period": "July 2099",
                    "previous": 0.5,
                    "source": "BLS",
                    "source_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                    "evidence_text": "BLS reported the July 2099 headline CPI previous value at 0.5 percent.",
                    "valid_until": item["valid_until"],
                }
            ],
            "evidence_text": "BLS reported the July 2099 headline CPI previous value at 0.5 percent.",
        }
    )
    facts, status = provider.load_payload(data)
    assert status["results_used"] == 1
    assert facts[0]["status"] == "active"
    assert facts[0]["fact_type"] == "macro_event_enrichment"
    assert facts[0]["previous"] == 0.5
    assert facts[0]["source"] == "BLS"
    assert facts[0]["source_url"] == "https://www.bls.gov/news.release/cpi.nr0.htm"
    raw = facts[0]["raw_payload_json"]
    assert raw["provider_type"] == "AI_RESEARCHER_CODEX_CLI"
    assert raw["evidence"]
    assert raw["validation"]["status"] == "accepted"
    assert raw["metrics"][0]["validation"]["status"] == "accepted"
    assert raw["metrics"][0]["evidence"]


def test_metric_only_numeric_values_require_metric_evidence(tmp_path):
    provider = AIResearcherProvider(settings(tmp_path))
    data = payload()
    item = data["results"][0]
    item.update(
        {
            "forecast": None,
            "previous": None,
            "consensus": None,
            "actual": None,
            "evidence_text": None,
            "extracted_text": None,
            "metrics": [
                {
                    "metric_id": "headline_cpi_mom",
                    "frequency": "MoM",
                    "unit": "percent",
                    "previous": 0.5,
                    "source": "BLS",
                    "source_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                    "valid_until": item["valid_until"],
                    "reliability": 0.9,
                    "confidence": 0.9,
                }
            ],
        }
    )

    facts, status = provider.load_payload(data)

    assert facts == []
    assert status["results_rejected"] == 1
    assert status["warnings"] == [f"rejected_missing_evidence:{item['fact_key']}"]


def test_metric_zero_value_is_preserved_in_top_level_fact(tmp_path):
    provider = AIResearcherProvider(settings(tmp_path))
    data = payload()
    item = data["results"][0]
    item.update({"forecast": None, "previous": None, "metrics": []})
    item["metrics"].append(
        {
            "metric_id": "headline_cpi_mom",
            "frequency": "MoM",
            "unit": "percent",
            "previous": 0.0,
            "source": "BLS",
            "source_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
            "evidence_text": "BLS reported a zero percent prior-period change.",
            "valid_until": item["valid_until"],
            "reliability": 0.9,
            "confidence": 0.9,
        }
    )

    facts, _ = provider.load_payload(data)

    assert facts[0]["previous"] == 0.0


def test_cpi_primary_mom_metric_is_promoted_with_matching_provenance(tmp_path):
    provider = AIResearcherProvider(settings(tmp_path))
    data = payload()
    item = data["results"][0]
    item.update(
        {
            "previous": 4.2,
            "source": "YoY source",
            "source_url": "https://research.test/yoy",
            "metrics": [
                {
                    "metric_id": "headline_cpi_yoy",
                    "previous": 4.2,
                    "unit": "percent",
                    "source": "YoY source",
                    "source_url": "https://research.test/yoy",
                    "evidence_text": "Headline CPI was 4.2 percent year over year.",
                    "reliability": 0.8,
                    "confidence": 0.8,
                },
                {
                    "metric_id": "headline_cpi_mom",
                    "previous": 0.5,
                    "unit": "percent",
                    "source": "MoM source",
                    "source_url": "https://research.test/mom",
                    "evidence_text": "Headline CPI was 0.5 percent month over month.",
                    "reliability": 0.9,
                    "confidence": 0.9,
                },
            ],
        }
    )

    facts, status = provider.load_payload(data)

    assert status["results_used"] == 1
    assert facts[0]["previous"] == 0.5
    assert facts[0]["source"] == "MoM source"
    assert facts[0]["source_url"] == "https://research.test/mom"


def test_source_name_containing_trading_substring_is_not_rejected(tmp_path):
    provider = AIResearcherProvider(settings(tmp_path))
    data = payload()
    item = data["results"][0]
    item.update(
        {
            "source": "Investopedia",
            "source_url": "https://www.investopedia.com/cpi-release",
            "evidence_text": "Investopedia reported the cited official CPI prior value.",
            "previous": 0.5,
        }
    )

    facts, status = provider.load_payload(data)

    assert status["status"] == "success"
    assert facts[0]["source"] == "Investopedia"


def test_source_url_containing_target_is_not_rejected(tmp_path):
    provider = AIResearcherProvider(settings(tmp_path))
    data = payload()
    item = data["results"][0]
    item.update(
        {
            "source": "Wall Street Journal",
            "source_url": "https://www.wsj.com/economy/central-banking/feds-preferred-inflation-gauge-climbs-above-target-range-b2220751",
            "evidence_text": "The cited article documents the reported macro result.",
            "previous": 0.5,
        }
    )

    facts, status = provider.load_payload(data)

    assert status["status"] == "success"
    assert facts[0]["source_url"] == item["source_url"]


async def test_provider_failure_cache_skips_immediate_retry(tmp_path):
    cfg = settings(tmp_path)
    cache = ProviderCacheRepository(cfg.database_path)
    calls = 0

    class FailingProvider:
        source = "DailyFX Economic Calendar"
        enabled = True

        async def fetch_for_events(self, events, country, start, end):
            nonlocal calls
            calls += 1
            return [], ["DailyFX 403"]

    service = EventEnrichmentService(cache, [FailingProvider()])
    await service.enrich_events([event()], "US", datetime.now(UTC), datetime.now(UTC) + timedelta(days=1))
    await service.enrich_events([event()], "US", datetime.now(UTC), datetime.now(UTC) + timedelta(days=1))
    assert calls == 1
