from __future__ import annotations

import asyncio
import gc
import hashlib
import json
import sqlite3
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.services.provider_capability_audit import (
    AuditFilters,
    DatabaseBundleGuard,
    HealthStatus,
    ProbeExecutionError,
    ProbeOutcome,
    ProviderCapabilityAuditEngine,
    _bound_exchange_supports_claim,
    _bound_freshness_valid,
    _bound_schema_valid,
    _bound_semantic_valid,
    _normalize_outcome,
    build_probe_requests,
    build_runtime_adapter_probe_requests,
    select_capability_targets,
    validate_capability_result_derivations,
)
from app.services.provider_capability_registry import (
    PROVIDER_REGISTRY,
    provider_by_id,
)
from app.services.codex_runtime_contract import CodexCLIError
from app.services.research_backend import ResearchBackendResult
from app.services.source_policy_service import SourcePolicyService
from scripts import provider_capability_audit as audit_script
from scripts.validate_senior_analyst_payload import (
    _capture_attestation_valid,
    _normalized_check_derivation_errors,
    _terminal_runtime_failure_without_transport,
)


def _claim_candidate(evidence_text: str) -> dict[str, Any]:
    return {
        "evidence_text": evidence_text,
        "metric_id": "headline_pce_mom",
        "frequency": "monthly",
        "unit": "percent",
        "occurrence_id": "provider-audit:pce:2026-06",
        "reference_period": "2026-06",
    }


@pytest.mark.parametrize(
    ("publisher", "url"),
    [
        ("BEA", "https://www.bea.gov/news/2026/pce"),
        (
            "Board of Governors of the Federal Reserve System",
            "https://www.federalreserve.gov/newsevents.htm",
        ),
        (
            "U.S. Securities and Exchange Commission",
            "https://www.sec.gov/Archives/example",
        ),
    ],
)
def test_official_publisher_aliases_bind_to_policy_hosts(
    publisher: str,
    url: str,
) -> None:
    assert SourcePolicyService().publisher_matches_url(publisher, url)


def _registration(
    *,
    credentials: tuple[str, ...] = (),
    timeout: float = 2.0,
    adapter_path: str = "tests.fake:DirectProvider",
    runtime_adapter: bool = False,
    additional_adapter_paths: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "provider_id": "DIRECT",
        "provider_type": "OFFICIAL_API",
        "adapter_path": adapter_path,
        "capabilities": (
            {
                "dataset_id": "macro",
                "metric_id": "headline_pce_yoy",
                "supported_fields": ("actual",),
                "frequency": "monthly",
                "transformation": "identity",
                "probe_id": "probe.direct.macro",
                "field_validator_id": "validate.official_actual.v1",
            },
        ),
        "allowed_roles": ("PRIMARY", "FALLBACK"),
        "credential_requirements": credentials,
        "timeout": timeout,
        "max_attempts": 1,
        "probe_id": "probe.direct",
        "probe_enabled": True,
        "runtime_adapter": runtime_adapter,
        "additional_adapter_paths": additional_adapter_paths,
    }


def _settings(tmp_path: Path, **updates: Any) -> Settings:
    values = {
        "database_path": tmp_path / "audit.sqlite",
        "diagnostics_dir": tmp_path / "diagnostics",
        "backups_dir": tmp_path / "backups",
        "logs_dir": tmp_path / "logs",
        "temp_dir": tmp_path / "temp",
        "environment": "test",
        **updates,
    }
    return Settings(**values)


def test_capability_dispatch_uses_dataset_specific_runtime_surfaces() -> None:
    class Adapter:
        async def quotes(self, symbols, *, force=False):
            return symbols, force

        async def relevant_option_chains(self, symbol, **kwargs):
            return symbol, kwargs

        async def company_news(self, symbol, *, start, end):
            return symbol, start, end

        async def fetch_for_events(self, events, country, start, end):
            return events, country, start, end

        async def fetch(self):
            raise AssertionError("generic fetch must not be selected")

    adapter = Adapter()
    tradier_method, tradier_kwargs = audit_script._select_probe_method(  # noqa: SLF001
        adapter,
        SimpleNamespace(
            provider_id="TRADIER",
            provider_type="LICENSED_MARKET_DATA",
            dataset_id="market_internals",
            metric_id="advance_decline_ratio",
            frequency="intraday",
        ),
    )
    assert tradier_method == adapter.quotes
    assert tradier_kwargs["force"] is True
    assert len(tradier_kwargs["symbols"]) > 1

    finnhub_method, finnhub_kwargs = audit_script._select_probe_method(  # noqa: SLF001
        adapter,
        SimpleNamespace(
            provider_id="FINNHUB",
            provider_type="STRUCTURED_VENDOR",
            dataset_id="current_news",
            metric_id="news_candidate",
            frequency="intraday",
        ),
    )
    assert finnhub_method == adapter.company_news
    assert finnhub_kwargs["symbol"]
    assert finnhub_kwargs["start"] <= finnhub_kwargs["end"]

    targeted_method, targeted_kwargs = audit_script._select_probe_method(  # noqa: SLF001
        adapter,
        SimpleNamespace(
            provider_id="TARGETED_SEARCH_EVENT",
            provider_type="SEARCH_SNIPPET",
            dataset_id="macro_calendar",
            metric_id="occurrence_enrichment",
            frequency="event",
        ),
    )
    assert targeted_method == adapter.fetch_for_events
    assert targeted_kwargs["country"] == "US"
    assert len(targeted_kwargs["events"]) == 1
    assert targeted_kwargs["events"][0].category == "PCE"


def test_census_dispatch_uses_explicit_registered_probe_query_ids() -> None:
    class Adapter:
        async def fetch(self, *, period=None, datasets=None):
            return period, datasets

    housing_starts = SimpleNamespace(
        provider_id="CENSUS",
        provider_type="OFFICIAL_GOVERNMENT",
        dataset_id="macro_calendar",
        metric_id="CENSUS:RESCONST:HOUSING_STARTS",
        frequency="monthly",
        capability=SimpleNamespace(probe_query_id="RESCONST"),
    )
    building_permits = SimpleNamespace(
        provider_id="CENSUS",
        provider_type="OFFICIAL_GOVERNMENT",
        dataset_id="macro_calendar",
        metric_id="CENSUS:RESCONST:BUILDING_PERMITS",
        frequency="monthly",
        capability=SimpleNamespace(probe_query_id="RESCONST"),
    )

    method, kwargs = audit_script._select_probe_method(  # noqa: SLF001
        Adapter(),
        housing_starts,
        targets=(housing_starts, building_permits),
    )

    assert method is not None
    assert kwargs["datasets"] == ("RESCONST",)
    assert kwargs["period"]


@pytest.mark.asyncio
async def test_census_period_correlation_is_recomputed_by_validator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CensusProvider:
        def __init__(self, settings: Settings) -> None:
            self.settings = settings

        async def fetch(
            self,
            *,
            period: str | None = None,
            datasets: tuple[str, ...] | None = None,
        ) -> dict[str, Any]:
            assert period
            assert datasets == ("RESCONST",)

            def handler(_: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={})

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                response = await client.get(
                    "https://api.example.test/census-period"
                )
                return response.json()

    census = provider_by_id("CENSUS")
    capability = next(
        item
        for item in census.capabilities
        if item.metric_id == "CENSUS:RESCONST:BUILDING_PERMITS"
    )
    registration = replace(
        census,
        adapter_path="tests.fake:CensusProvider",
        capabilities=(capability,),
    )
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: CensusProvider,
    )
    settings = _settings(tmp_path)
    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
    )
    row = execution.report["results"][0]
    acquisition = execution.report["acquisitions"][0]
    correlation = acquisition["request_correlations"][
        row["capability_id"]
    ]

    assert correlation["reference_parameter"] == "period"
    assert correlation["expected_reference_period"] == "2026-07"
    errors = validate_capability_result_derivations(
        row,
        capability,
        registration,
        require_bound_evidence=True,
        normalized_response={},
        acquisition=acquisition,
    )
    assert not any(
        error.endswith("REQUEST_CORRELATION_IDENTITY_MISMATCH")
        for error in errors
    )

    tampered = json.loads(json.dumps(acquisition))
    tampered["request_correlations"][row["capability_id"]][
        "expected_reference_period"
    ] = "2026-06"
    forged_errors = validate_capability_result_derivations(
        row,
        capability,
        registration,
        require_bound_evidence=True,
        normalized_response={},
        acquisition=tampered,
    )
    assert any(
        error.endswith("REQUEST_CORRELATION_IDENTITY_MISMATCH")
        for error in forged_errors
    )


def test_runtime_adapter_probe_plan_covers_every_constructed_event_adapter(
    tmp_path: Path,
) -> None:
    provider_ids = (
        "FEDERAL_RESERVE",
        "INVESTING_ECONOMIC_CALENDAR",
        "DAILYFX",
        "FOREX_FACTORY",
    )
    registry = tuple(provider_by_id(provider_id) for provider_id in provider_ids)
    settings = _settings(tmp_path)
    targets = select_capability_targets(
        registry,
        AuditFilters(),
        settings=settings,
    )
    capability_requests = build_probe_requests(
        targets,
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )
    runtime_requests = build_runtime_adapter_probe_requests(
        targets,
        capability_requests,
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )
    planned = {request.adapter_path for request in runtime_requests}

    assert {
        "app.providers.federal_reserve:FederalReserveRssProvider",
        "app.providers.event_enrichment:InvestingEnrichmentProvider",
        "app.providers.event_enrichment:PlaywrightInvestingProvider",
        "app.providers.event_enrichment:PlaywrightDailyFXProvider",
        "app.providers.event_enrichment:PlaywrightForexFactoryProvider",
    } <= planned


@pytest.mark.asyncio
async def test_runtime_source_negative_control_emits_derived_field_checks(
    tmp_path: Path,
) -> None:
    class ResearchSourceGateway:
        def acquire_many(self, *_: Any) -> list[Any]:
            return []

    registration = provider_by_id("CODEX_CLI_RESEARCH_BACKEND")
    settings = _settings(tmp_path, research_backend="codex_cli")
    targets = select_capability_targets(
        (registration,),
        AuditFilters(),
        settings=settings,
    )
    capability_requests = build_probe_requests(
        targets,
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )
    request = next(
        item
        for item in build_runtime_adapter_probe_requests(
            targets,
            capability_requests,
            run_id="20260731T120000Z",
            sandbox_root=tmp_path / "sandbox",
            database_snapshot_path=None,
            settings=settings,
        )
        if item.adapter_path
        == "app.services.research_source_gateway:ResearchSourceGateway"
    )

    outcome = await audit_script._runtime_source_component_probe(  # noqa: SLF001
        ResearchSourceGateway(),
        request,
        adapter_path=request.adapter_path,
    )

    assert outcome.transport_status == "SUCCESS"
    assert outcome.checked_at
    assert set(outcome.field_checks) == {
        request.targets[0].field_key(field_name)
        for field_name in request.targets[0].fields
    }
    assert all(
        checks["schema_valid"] is False
        and checks["completeness_valid"] is False
        for checks in outcome.field_checks.values()
    )


@pytest.mark.asyncio
async def test_runtime_adapter_coverage_is_bound_to_each_adapter_acquisition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _DirectProvider)
    registration = _registration(
        runtime_adapter=True,
        additional_adapter_paths=("tests.fake:FallbackProvider",),
    )
    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(_settings(tmp_path)),
        settings=_settings(tmp_path),
    ).run(sandbox_root=tmp_path / "sandbox")

    coverage = execution.report["runtime_adapter_coverage"]
    assert execution.audit_status == "COMPLETED"
    assert coverage["complete"] is True
    assert coverage["coverage_pct"] == 100.0
    assert coverage["adapters_expected"] == 2
    assert execution.report["runtime_adapter_acquisitions_executed"] == 1
    assert execution.report["runtime_adapter_acquisitions"][0][
        "probe_adapter_paths"
    ] == ["tests.fake:FallbackProvider"]


@pytest.mark.asyncio
async def test_provider_id_presence_cannot_forge_runtime_adapter_coverage(
    tmp_path: Path,
) -> None:
    class UnboundExecutor:
        requires_real_dispatch = True

        async def __call__(self, request):
            return ProbeOutcome(
                configured=True,
                transport_status="OK",
                attempts=1,
                evidence={
                    "real_adapter_invoked": True,
                    "probe_dispatch_status": "REAL_ADAPTER",
                    "adapter_path": "tests.fake:WrongAdapter",
                },
            )

    execution = await ProviderCapabilityAuditEngine(
        (
            _registration(
                runtime_adapter=True,
                additional_adapter_paths=("tests.fake:FallbackProvider",),
            ),
        ),
        UnboundExecutor(),
        settings=_settings(tmp_path),
    ).run(sandbox_root=tmp_path / "sandbox")

    assert execution.audit_status == "FAILED"
    assert execution.report["runtime_adapter_coverage"]["coverage_pct"] == 0.0
    assert "RUNTIME_ADAPTER_PROBE_COVERAGE_INCOMPLETE" in execution.report[
        "internal_errors"
    ]


class _DirectProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        async def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "actual": 2.6,
                    "metric_id": "headline_pce_yoy",
                    "frequency": "monthly",
                    "transformation": "identity",
                    "unit": "percent",
                    "freshness": "CURRENT_RELEASE",
                    "occurrence_id": "fred:PCEPI:2026-06",
                    "reference_period": "2026-06",
                    "expected_reference_period": "2026-06",
                    "lifecycle_verified": True,
                    "provider_id": "DIRECT",
                    "lineage": [
                        {
                            "field": "actual",
                            "value_sha256": audit_script._stable_sha256(2.6),  # noqa: SLF001
                            "publisher": "BEA",
                            "distributor": "DIRECT",
                            "acquisition_provider": "DIRECT",
                            "source_url": "https://api.example.test/series",
                        }
                    ],
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            response = await client.get("https://api.example.test/series")
            return response.json()


class _WrongSemanticProvider(_DirectProvider):
    async def fetch(self) -> dict[str, Any]:
        payload = _valid_atomic_payload()
        payload["name"] = "PCE M/M"

        async def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            response = await client.get(
                "https://api.example.test/wrong-semantic-series"
            )
            return response.json()


@pytest.mark.asyncio
async def test_additional_adapter_semantic_failure_is_field_evaluated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback_path = "tests.fake:FallbackProvider"
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda path: (
            _WrongSemanticProvider
            if path == fallback_path
            else _DirectProvider
        ),
    )
    settings = _settings(tmp_path)
    execution = await ProviderCapabilityAuditEngine(
        (
            _registration(
                runtime_adapter=True,
                additional_adapter_paths=(fallback_path,),
            ),
        ),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    assert execution.audit_status == "COMPLETED"
    assert execution.system_health == "DEGRADED"
    assert execution.report["runtime_adapter_coverage"][
        "coverage_pct"
    ] == 100.0
    runtime_result = execution.report["runtime_adapter_results"][0]
    assert runtime_result["evaluation_kind"] == (
        "RUNTIME_ADAPTER_CAPABILITY"
    )
    assert runtime_result["runtime_adapter_path"] == fallback_path
    assert runtime_result["semantic_mapping_valid"] is False
    assert runtime_result["health_status"] == "UNUSABLE"
    assert "SOURCE_GAP" in runtime_result["recommendations"]
    quality = execution.report["runtime_adapter_quality"]
    fallback = next(
        row for row in quality["rows"] if row["adapter_path"] == fallback_path
    )
    assert quality["evaluation_coverage_pct"] == 100.0
    assert quality["all_quality_certified"] is False
    assert fallback["quality_certified"] is False
    assert fallback["semantic_mapping_valid"] is False
    assert execution.capability_matrix[
        "runtime_adapter_capabilities"
    ] == [runtime_result]


@pytest.mark.asyncio
async def test_real_adapter_dispatch_and_exact_http_capture_are_exercised_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _DirectProvider)
    settings = _settings(tmp_path)
    execution = await ProviderCapabilityAuditEngine(
        (_registration(),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    result = execution.report["results"][0]
    acquisition = execution.report["acquisitions"][0]
    assert result["health_status"] == "HEALTHY"
    assert result["quality_score"] == 100
    assert result["real_adapter_invoked"] is True
    assert acquisition["raw_response_sha256"] is not None
    assert execution.report["real_adapter_probes"] == 1


class _NoTransportProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        return _valid_atomic_payload()


class _CaughtTransportFailureProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError(
                "offline controlled transport failure",
                request=request,
            )

        try:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                await client.get("https://api.example.test/unreachable")
        except httpx.TransportError:
            return {
                "status": "provider_error",
                "failure_type": "network_error",
                "actual": None,
            }
        raise AssertionError("controlled transport failure was not observed")


class _FailsBeforeSendProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        raise RuntimeError("adapter failed before constructing an HTTP send")


class _ConstructorFailsProvider:
    def __init__(self, settings: Settings) -> None:
        raise RuntimeError("adapter constructor failed")


@pytest.mark.asyncio
async def test_constructor_failure_has_no_attempt_or_certifiable_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _ConstructorFailsProvider,
    )
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (_registration(),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    acquisition = execution.report["acquisitions"][0]
    result = execution.report["results"][0]
    assert acquisition["attempts"] == 0
    assert acquisition["network_exchange_count"] == 0
    assert acquisition["capture_verified"] is None
    assert acquisition["real_adapter_invoked"] is False
    assert result["eligible_as_primary"] is False
    assert result["eligible_as_fallback"] is False


@pytest.mark.asyncio
async def test_invoke_failure_before_send_does_not_invent_http_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _FailsBeforeSendProvider,
    )
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (_registration(),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    acquisition = execution.report["acquisitions"][0]
    result = execution.report["results"][0]
    assert acquisition["attempts"] == 0
    assert acquisition["network_exchange_count"] == 0
    assert acquisition["capture_verified"] is False
    assert acquisition["capture_mode"] == "NONE"
    assert result["eligible_as_primary"] is False
    assert result["eligible_as_fallback"] is False


@pytest.mark.asyncio
async def test_external_adapter_cannot_certify_without_captured_acquisition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _NoTransportProvider)
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (_registration(),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    result = execution.report["results"][0]
    assert execution.audit_status == "COMPLETED"
    assert result["health_status"] == "UNUSABLE"
    assert result["eligible_as_primary"] is False
    assert "PROBE_ACQUISITION_NOT_CAPTURED" in result["reason_codes"]
    assert all(
        check is None
        for check in result["field_results"]["actual"]["checks"].values()
    )


@pytest.mark.asyncio
async def test_caught_httpx_failure_retains_real_attempt_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _CaughtTransportFailureProvider,
    )
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (_registration(),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    acquisition = execution.report["acquisitions"][0]
    result = execution.report["results"][0]
    assert acquisition["transport_status"] == "DOWN"
    assert acquisition["attempts"] == 1
    assert acquisition["network_exchange_count"] == 0
    assert acquisition["capture_mode"] == "HTTPX"
    assert acquisition["capture_verified"] is True
    assert acquisition["capture_attestation"] == {
        "attempted_send_count": 1,
        "failure_reason_code": "PROVIDER_TRANSPORT_FAILED",
        "response_exchange_count": 0,
    }
    assert _capture_attestation_valid(
        acquisition,
        provider=SimpleNamespace(
            provider_id="DIRECT",
            provider_type="OFFICIAL_API",
            capture_mode="HTTPX",
            configuration_setting=None,
        ),
    )
    assert result["health_status"] == "DOWN"
    assert result["eligible_as_primary"] is False
    assert result["eligible_as_fallback"] is False


def test_subprocess_capture_requires_an_observed_process_result() -> None:
    assert (
        audit_script._subprocess_result_attestation(  # noqa: SLF001
            {"status": "provider_unavailable", "failure_reason": "path_invalid"}
        )
        is None
    )
    assert (
        audit_script._subprocess_result_attestation(  # noqa: SLF001
            {
                "status": "provider_failed",
                "exit_code": 7,
                "duration_ms": 12,
            }
        )
        is None
    )
    digest = "a" * 64
    assert audit_script._subprocess_result_attestation(  # noqa: SLF001
        {
            "status": "provider_failed",
            "exit_code": 7,
            "duration_ms": 12,
            "process_observed": True,
            "process_id": 123,
            "process_terminated": True,
            "output_sha256": digest,
            "stdout_sha256": digest,
            "stderr_sha256": digest,
            "command_sha256": digest,
        }
    ) == {
        "exit_code": 7,
        "process_observed": True,
        "process_id": 123,
        "failure_reason": None,
        "status": "provider_failed",
        "duration_ms": 12,
        "bounded_timeout_observed": False,
        "stdout_sha256": digest,
        "stderr_sha256": digest,
        "command_sha256": digest,
        "declared_output_sha256": digest,
        "process_terminated": True,
    }
    assert (
        audit_script._subprocess_result_attestation(  # noqa: SLF001
            {
                "status": "provider_failed",
                "failure_reason": "codex_cli_timeout",
                "timeout_seconds": 1,
            }
        )
        is None
    )
    assert audit_script._subprocess_result_attestation(  # noqa: SLF001
        {
            "status": "provider_failed",
            "failure_reason": "codex_cli_timeout",
            "timeout_seconds": 1,
            "process_observed": True,
            "process_id": 123,
            "exit_code": None,
            "output_sha256": digest,
            "stdout_sha256": digest,
            "stderr_sha256": digest,
            "command_sha256": digest,
            "process_terminated": True,
        }
    ) == {
        "exit_code": None,
        "process_observed": True,
        "process_id": 123,
        "failure_reason": "codex_cli_timeout",
        "status": "provider_failed",
        "duration_ms": None,
        "bounded_timeout_observed": True,
        "stdout_sha256": digest,
        "stderr_sha256": digest,
        "command_sha256": digest,
        "declared_output_sha256": digest,
        "process_terminated": True,
    }


@pytest.mark.asyncio
async def test_real_legacy_subprocess_timeout_is_terminal_and_attested_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    registration = provider_by_id("AI_RESEARCHER")
    settings = _settings(
        tmp_path,
        enable_ai_researcher=True,
        ai_researcher_mode="codex_cli",
        codex_cli_command=sys.executable,
        codex_workspace_dir=tmp_path / "codex-workspace",
        timeout_ai_research_seconds=2,
        codex_research_timeout_seconds=1,
    )

    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(
        filters=AuditFilters.from_values(
            metrics=("earnings_schedule_context",),
        ),
        sandbox_root=tmp_path / "sandbox",
    )

    acquisition = execution.report["acquisitions"][0]
    result = execution.report["results"][0]
    attestation = acquisition["capture_attestation"]
    assert execution.audit_status == "COMPLETED"
    assert acquisition["transport_status"] == "DOWN"
    assert acquisition["reason_codes"] == ["PROBE_TIMEOUT"]
    assert acquisition["attempts"] == 1
    assert acquisition["latency_ms"] >= 900
    assert acquisition["real_adapter_invoked"] is True
    assert acquisition["probe_dispatch_status"] == "REAL_ADAPTER"
    assert acquisition["capture_mode"] == "SUBPROCESS"
    assert acquisition["capture_verified"] is True
    assert attestation["process_observed"] is True
    assert type(attestation["process_id"]) is int
    assert attestation["process_id"] > 0
    assert attestation["process_terminated"] is True
    assert attestation["bounded_timeout_observed"] is True
    assert _capture_attestation_valid(
        acquisition,
        provider=registration,
    ) is True
    assert result["health_status"] == "DOWN"


@pytest.mark.parametrize(
    ("anchor", "expected"),
    [
        (
            "headline pce month over month monthly actual 2.6 percent 2026-06",
            True,
        ),
        (
            "headline pce momentum monthly actual 2.6 percent 2026-06",
            False,
        ),
        (
            "headline pce month over month monthlyish actual 2.6 percent 2026-06",
            False,
        ),
    ],
)
def test_claim_context_aliases_use_word_and_phrase_boundaries(
    anchor: str,
    expected: bool,
) -> None:
    body = anchor.encode("utf-8")
    candidate = _claim_candidate(anchor)
    exchange = audit_script.CapturedHttpExchange(
        method="GET",
        url="https://bea.gov/release/pce",
        request_headers=(),
        status_code=200,
        response_headers=(),
        response_body=body,
        latency_ms=1,
        attempt=1,
    )

    assert audit_script._exchange_supports_claim(  # noqa: SLF001
        exchange,
        field_name="actual",
        value=2.6,
        candidate=candidate,
    ) is expected
    assert _bound_exchange_supports_claim(
        {"body": body},
        field_name="actual",
        value=2.6,
        candidate=candidate,
    ) is expected


@pytest.mark.parametrize(
    ("value", "observed", "expected"),
    [
        (2, "2,600", False),
        (628, "628,000", False),
        (2.6, "2.60%", True),
        (-0.1, "\u22120.10%", True),
        (2600, "2,600", True),
        (1234.56, "1,234.560%", True),
        (2.6, "12.6", False),
    ],
)
def test_claim_numeric_binding_compares_complete_numeric_tokens(
    value: float,
    observed: str,
    expected: bool,
) -> None:
    anchor = (
        "headline pce month over month monthly actual "
        f"{observed} previous 0 percent 2026-06"
    )
    body = anchor.encode("utf-8")
    candidate = _claim_candidate(anchor)
    exchange = audit_script.CapturedHttpExchange(
        method="GET",
        url="https://bea.gov/release/pce",
        request_headers=(),
        status_code=200,
        response_headers=(),
        response_body=body,
        latency_ms=1,
        attempt=1,
    )

    assert audit_script._exchange_supports_claim(  # noqa: SLF001
        exchange,
        field_name="actual",
        value=value,
        candidate=candidate,
    ) is expected
    assert _bound_exchange_supports_claim(
        {"body": body},
        field_name="actual",
        value=value,
        candidate=candidate,
    ) is expected


def test_claim_numeric_binding_is_field_specific() -> None:
    anchor = (
        "headline pce month over month monthly actual 2.6 percent "
        "previous 2.4 percent 2026-06"
    )
    body = anchor.encode("utf-8")
    candidate = _claim_candidate(anchor)
    exchange = audit_script.CapturedHttpExchange(
        method="GET",
        url="https://bea.gov/release/pce",
        request_headers=(),
        status_code=200,
        response_headers=(),
        response_body=body,
        latency_ms=1,
        attempt=1,
    )

    for field_name, value in (("actual", 2.4), ("previous", 2.6)):
        assert not audit_script._exchange_supports_claim(  # noqa: SLF001
            exchange,
            field_name=field_name,
            value=value,
            candidate=candidate,
        )
        assert not _bound_exchange_supports_claim(
            {"body": body},
            field_name=field_name,
            value=value,
            candidate=candidate,
        )


@pytest.mark.parametrize(
    ("value", "observed", "expected"),
    [
        ("hold", "shareholder", False),
        ("US", "business", False),
        ("beat", "unbeatable", False),
        ("after_close", "after_closeout", False),
        ("after_close", "after close", True),
        ("2026-06", "2026-060", False),
        ("2026-06", "2026/06", True),
        (
            "https://bea.gov/release/pce?a=1&b=2",
            "https://bea.gov/release/pce?b=2&a=1",
            True,
        ),
        (
            "https://bea.gov/release/pce",
            "https://bea.gov/release/pce-extra",
            False,
        ),
    ],
)
def test_claim_string_binding_uses_atomic_boundaries(
    value: str,
    observed: str,
    expected: bool,
) -> None:
    anchor = (
        "headline pce month over month monthly actual "
        f"{observed} previous 0 percent 2026-06"
    )
    body = anchor.encode("utf-8")
    candidate = _claim_candidate(anchor)
    exchange = audit_script.CapturedHttpExchange(
        method="GET",
        url="https://bea.gov/release/pce",
        request_headers=(),
        status_code=200,
        response_headers=(),
        response_body=body,
        latency_ms=1,
        attempt=1,
    )

    assert audit_script._exchange_supports_claim(  # noqa: SLF001
        exchange,
        field_name="actual",
        value=value,
        candidate=candidate,
    ) is expected
    assert _bound_exchange_supports_claim(
        {"body": body},
        field_name="actual",
        value=value,
        candidate=candidate,
    ) is expected


def test_legacy_ai_probe_input_preserves_reference_correlation() -> None:
    class Adapter:
        async def research(self, events: list[dict[str, Any]]) -> Any:
            return events

    target = SimpleNamespace(
        provider_id="AI_RESEARCHER",
        provider_type="AI",
        dataset_id="macro_calendar",
        metric_id="headline_pce_yoy",
        fields=("consensus", "previous"),
        frequency="monthly",
        transformation="identity",
    )
    correlation = {
        "request_key": "a" * 64,
        "provider_id": "AI_RESEARCHER",
        "expected_occurrence_id": "provider-audit:event:2026-06",
        "expected_reference_period": "2026-06",
    }

    method, kwargs = audit_script._select_probe_method(  # noqa: SLF001
        Adapter(),
        target,
        request_correlation=correlation,
    )

    assert method is not None
    event = kwargs["events"][0]
    assert event["occurrence_id"] == correlation["expected_occurrence_id"]
    assert event["reference_period"] == "2026-06"
    assert event["expected_reference_period"] == "2026-06"


@pytest.mark.asyncio
async def test_configured_ai_researcher_is_really_invoked_per_concrete_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = provider_by_id("AI_RESEARCHER")
    observed_batches: list[list[dict[str, Any]]] = []

    class ControlledAIResearcher:
        def __init__(self, settings: Settings) -> None:
            self.settings = settings

        async def research(
            self,
            events: list[dict[str, Any]],
        ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
            observed_batches.append(events)
            return [], {
                "status": "no_data_available",
                "reason_code": "AUDIT_SOURCE_GAP",
            }

    original_load_symbol = audit_script._load_symbol  # noqa: SLF001

    def controlled_load_symbol(path: str) -> Any:
        if path == registration.adapter_path:
            return ControlledAIResearcher
        return original_load_symbol(path)

    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        controlled_load_symbol,
    )
    settings = _settings(
        tmp_path,
        enable_ai_researcher=True,
        ai_researcher_mode="openai_api",
        openai_api_key="offline-audit-key",
    )
    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(
        filters=AuditFilters.from_values(
            providers=("AI_RESEARCHER",),
        ),
        sandbox_root=tmp_path / "sandbox",
    )

    assert execution.audit_status == "COMPLETED"
    assert sorted(len(batch) for batch in observed_batches) == [1, 1, 26]
    assert len(execution.report["results"]) == len(
        registration.capabilities
    )
    assert all(
        acquisition["configured"] is True
        and acquisition["real_adapter_invoked"] is True
        and acquisition["probe_dispatch_status"] == "REAL_ADAPTER"
        and acquisition["attempts"] >= 1
        for acquisition in execution.report["acquisitions"]
    )
    assert all(
        row["health_status"] == "UNUSABLE"
        and row["eligible_as_primary"] is False
        and row["eligible_as_fallback"] is False
        for row in execution.report["results"]
    )
    observed_targets = {
        (event["dataset_id"], event["metric_id"])
        for batch in observed_batches
        for event in batch
    }
    assert observed_targets == {
        (capability.dataset_id, capability.metric_id)
        for capability in registration.capabilities
    }


@pytest.mark.asyncio
async def test_legacy_ai_disabled_is_not_configured_not_fake_terminal(
    tmp_path: Path,
) -> None:
    registration = provider_by_id("AI_RESEARCHER")
    settings = _settings(
        tmp_path,
        enable_ai_researcher=False,
        codex_cli_command="codex",
    )
    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(
        filters=AuditFilters.from_values(
            metrics=("flash_services_pmi",)
        ),
        sandbox_root=tmp_path / "sandbox",
    )

    acquisition = execution.report["acquisitions"][0]
    row = execution.report["results"][0]
    capability = next(
        item
        for item in registration.capabilities
        if item.metric_id == "flash_services_pmi"
    )
    assert acquisition["configured"] is False
    assert acquisition["transport_status"] == "NOT_CONFIGURED"
    assert acquisition["probe_dispatch_status"] is None
    assert row["health_status"] == "NOT_CONFIGURED"
    assert row["eligible_as_primary"] is False
    assert row["eligible_as_fallback"] is False
    assert (
        "PROVIDER_DISABLED_BY_CONFIGURATION"
        in row["observed_reason_codes"]
    )
    assert (
        validate_capability_result_derivations(
            row,
            capability,
            registration,
            acquisition=acquisition,
        )
        == ()
    )


class _AttestedResearchBackend:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def execute_research(
        self,
        *,
        workspace: Path,
        event_observer: Any,
        **_: Any,
    ) -> ResearchBackendResult:
        event_observer(
            {
                "event_type": "search",
                "lifecycle": "started",
                "item_id": "search-1",
                "query": "official release",
            }
        )
        output_path = workspace / "agentic_research_output.json"
        output_path.write_bytes(b'{"status":"NO_DATA"}')
        output_sha256 = hashlib.sha256(output_path.read_bytes()).hexdigest()
        empty_sha256 = hashlib.sha256(b"").hexdigest()
        return ResearchBackendResult(
            invocation_id="codex-audit-1",
            backend="codex_cli",
            purpose="AGENTIC_RESEARCH",
            payload={
                "status": "NO_DATA",
                "plan": {
                    "topics": [],
                    "queries": [],
                    "stop_conditions": [],
                },
                "searches": [],
                "acquisition_requests": [],
                "claims": [],
                "warnings": ["NO_OCCURRENCE_SPECIFIC_EVIDENCE"],
            },
            transport_attestation={
                "process_observed": True,
                "process_id": 123,
                "exit_code": 0,
                "process_terminated": True,
                "stdout_sha256": empty_sha256,
                "stderr_sha256": empty_sha256,
                "output_sha256": output_sha256,
                "command_sha256": "a" * 64,
                "duration_ms": 1,
            },
            output_path=str(output_path.resolve()),
        )


class _SlowAttestedSourceSubprocessProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def audit_probe(
        self,
        request: Any,
    ) -> dict[str, Any]:
        await asyncio.sleep(0.08)
        target = request.targets[0]
        correlation = request.correlation_for(target)
        payload = {
            "status": "COMPLETED",
            "claims": [
                {
                    "topic": "macro",
                    "field_semantics": "forecast",
                    "value": "2.6",
                    "metric_id": target.metric_id,
                    "period": correlation["expected_reference_period"],
                    "frequency": "monthly",
                    "unit": "percent",
                    "event_key": correlation["expected_occurrence_id"],
                    "evidence": [
                        {
                            "source_url": (
                                "https://source.example.test/release/pce"
                            ),
                            "canonical_url": None,
                            "publisher": "BEA",
                            "evidence_text": (
                                "forecast 2.6 percent monthly 2026-07"
                            ),
                        }
                    ],
                }
            ],
        }
        request.sandbox_root.mkdir(parents=True, exist_ok=True)
        output_path = request.sandbox_root / "slow-subprocess-output.json"
        output_path.write_bytes(
            json.dumps(payload, separators=(",", ":")).encode()
        )
        output_sha256 = hashlib.sha256(output_path.read_bytes()).hexdigest()
        empty_sha256 = hashlib.sha256(b"").hexdigest()
        return {
            **payload,
            "process_observed": True,
            "process_id": 124,
            "exit_code": 0,
            "process_terminated": True,
            "stdout_sha256": empty_sha256,
            "stderr_sha256": empty_sha256,
            "output_sha256": output_sha256,
            "command_sha256": "b" * 64,
            "duration_ms": 80,
            "output_path": str(output_path.resolve()),
        }


class _AttestedFailingResearchBackend:
    category = "TIMEOUT"
    failure_reason = "codex_cli_timeout"
    exit_code: int | None = None

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def execute_research(
        self,
        *,
        workspace: Path,
        **_: Any,
    ) -> ResearchBackendResult:
        artifact = workspace / "codex-process-failure.json"
        exact = b'{"process":"observed"}\n'
        artifact.write_bytes(exact)
        process_attestation: dict[str, Any] = {
            "status": "TIMED_OUT" if self.category == "TIMEOUT" else "FAILED",
            "failure_reason": self.failure_reason,
            "process_observed": True,
            "process_id": 125,
            "exit_code": self.exit_code,
            "process_terminated": True,
            "stdout_sha256": hashlib.sha256(b"").hexdigest(),
            "stderr_sha256": hashlib.sha256(b"failure").hexdigest(),
            "command_sha256": "c" * 64,
            "output_path": str(artifact.resolve()),
            "output_sha256": hashlib.sha256(exact).hexdigest(),
            "output_size_bytes": len(exact),
        }
        if self.category == "TIMEOUT":
            process_attestation["timeout_seconds"] = 1
        raise CodexCLIError(
            {
                "category": self.category,
                "retryable": self.category == "TIMEOUT",
                "retry_classification": (
                    "RETRYABLE"
                    if self.category == "TIMEOUT"
                    else "NON_RETRYABLE"
                ),
                "process_attestation": process_attestation,
            }
        )


class _AttestedAuthFailingResearchBackend(_AttestedFailingResearchBackend):
    category = "AUTH_UNAVAILABLE"
    failure_reason = "codex_cli_auth_unavailable"
    exit_code = 1


class _AttestedBudgetFailingResearchBackend:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def execute_research(
        self,
        *,
        workspace: Path,
        **_: Any,
    ) -> ResearchBackendResult:
        artifact = workspace / "codex-budget-process-failure.json"
        exact = b'{"process":"budget-stopped"}\n'
        artifact.write_bytes(exact)
        error = ProbeExecutionError(
            "research backend exceeded the observed audit tool budget",
            status=HealthStatus.UNUSABLE,
            reason_code="RESEARCH_TOOL_BUDGET_EXCEEDED",
        )
        error.dispatch_observation = {
            "schema_version": "provider-audit-dispatch-observation-v1",
            "origin": "BACKEND_EVENT_OBSERVER",
            "backend_class": "PersistentAIJobExecutor",
            "budget_stop_observed": True,
            "events_observed": 2,
            "search_attempts": 2,
            "source_open_attempts": 0,
            "event_sha256": ["a" * 64, "b" * 64],
        }
        error.diagnostic = {
            "category": "POLICY_ABORT",
            "process_attestation": {
                "status": "POLICY_ABORTED",
                "failure_reason": "research_tool_budget_exceeded",
                "process_observed": True,
                "process_id": 126,
                "exit_code": 17,
                "process_terminated": True,
                "stdout_sha256": hashlib.sha256(b"events").hexdigest(),
                "stderr_sha256": hashlib.sha256(b"").hexdigest(),
                "command_sha256": "c" * 64,
                "output_path": str(artifact.resolve()),
                "output_sha256": hashlib.sha256(exact).hexdigest(),
                "output_size_bytes": len(exact),
            },
        }
        raise error


class _UnattestedFailingResearchBackend:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def execute_research(self, **_: Any) -> ResearchBackendResult:
        raise CodexCLIError(
            {
                "category": "TIMEOUT",
                "retryable": True,
                "retry_classification": "RETRYABLE",
            }
        )


@pytest.mark.asyncio
async def test_successful_subprocess_gets_fresh_source_verification_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_registration = provider_by_id("CODEX_CLI_RESEARCH_BACKEND")
    registration = replace(
        base_registration,
        timeout=0.05,
        capabilities=tuple(
            replace(
                capability,
                runtime_source_domains=("source.example.test",),
            )
            if capability.metric_id == "event_missing_fields"
            else capability
            for capability in base_registration.capabilities
        ),
    )
    settings = _settings(tmp_path, research_backend="codex_cli")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _SlowAttestedSourceSubprocessProvider,
    )
    source_calls = 0

    def source_handler(source_request: httpx.Request) -> httpx.Response:
        nonlocal source_calls
        assert source_request.url.host == "source.example.test"
        source_calls += 1
        return httpx.Response(
            200,
            text="forecast 2.6 percent monthly 2026-07",
        )

    executor = audit_script.IsolatedRegistryProbeExecutor(
        settings,
        source_url_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(source_handler),
            follow_redirects=False,
        ),
        source_url_resolver=lambda _: ("93.184.216.34",),
        allow_test_source_urls=True,
    )

    outcome = await executor(request)

    assert source_calls == 1, outcome.evidence["source_url_verifications"]
    assert "PROBE_TIMEOUT" not in outcome.reason_codes
    assert outcome.evidence["capture_verified"] is True
    assert outcome.evidence["capture_attestation"]["process_terminated"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend_type", "expected_status", "expected_reason"),
    [
        (_AttestedFailingResearchBackend, "DOWN", "PROBE_TIMEOUT"),
        (
            _AttestedAuthFailingResearchBackend,
            "AUTH_FAILED",
            "CODEX_CLI_AUTH_UNAVAILABLE",
        ),
    ],
)
async def test_persistent_backend_failure_uses_real_process_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend_type: type[Any],
    expected_status: str,
    expected_reason: str,
) -> None:
    registration = provider_by_id("CODEX_CLI_RESEARCH_BACKEND")
    settings = _settings(tmp_path, research_backend="codex_cli")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: backend_type)

    outcome = await audit_script.IsolatedRegistryProbeExecutor(settings)(
        request
    )

    assert outcome.transport_status == expected_status
    assert expected_reason in outcome.reason_codes
    assert outcome.evidence["real_adapter_invoked"] is True
    assert outcome.evidence["capture_verified"] is True
    attestation = outcome.evidence["capture_attestation"]
    assert attestation["process_observed"] is True
    assert attestation["process_terminated"] is True


@pytest.mark.asyncio
async def test_attested_budget_stop_is_terminal_unusable_not_missing_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = provider_by_id("CODEX_CLI_RESEARCH_BACKEND")
    settings = _settings(tmp_path, research_backend="codex_cli")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _AttestedBudgetFailingResearchBackend,
    )

    outcome = await audit_script.IsolatedRegistryProbeExecutor(settings)(
        request
    )

    assert outcome.transport_status == "UNUSABLE"
    assert "RESEARCH_TOOL_BUDGET_EXCEEDED" in outcome.reason_codes
    assert outcome.evidence["real_adapter_invoked"] is True
    assert outcome.evidence["probe_dispatch_status"] == "REAL_ADAPTER"
    assert outcome.evidence["capture_verified"] is True
    assert outcome.evidence["capture_attestation"]["exit_code"] == 17
    assert outcome.evidence["backend_tool_attempt_count"] == 2
    assert _capture_attestation_valid(
        {
            **outcome.evidence,
            "configured": outcome.configured,
            "transport_status": outcome.transport_status,
            "network_exchange_count": len(outcome.network_exchanges),
            "reason_codes": list(outcome.reason_codes),
            "capture_attestation_sha256": audit_script._stable_sha256(  # noqa: SLF001
                outcome.evidence["capture_attestation"]
            ),
        },
        provider=registration,
    )


@pytest.mark.asyncio
async def test_persistent_backend_pre_spawn_failure_remains_unattested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = provider_by_id("CODEX_CLI_RESEARCH_BACKEND")
    settings = _settings(tmp_path, research_backend="codex_cli")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _UnattestedFailingResearchBackend,
    )

    outcome = await audit_script.IsolatedRegistryProbeExecutor(settings)(
        request
    )

    assert outcome.evidence["real_adapter_invoked"] is False
    assert outcome.evidence["probe_dispatch_status"] == "FAILED"
    assert outcome.evidence["capture_mode"] == "NONE"
    assert outcome.evidence["capture_verified"] is False
    assert outcome.evidence["capture_attestation"] is None


@pytest.mark.asyncio
async def test_codex_subprocess_invocation_consumes_call_budget_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = replace(
        provider_by_id("CODEX_CLI_RESEARCH_BACKEND"),
        audit_source_url_budget=0,
    )
    settings = _settings(tmp_path, research_backend="codex_cli")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _AttestedResearchBackend,
    )

    outcome = await audit_script.IsolatedRegistryProbeExecutor(settings)(
        request
    )

    assert outcome.evidence["backend_invocation_attempt_count"] == 1
    assert outcome.evidence["backend_tool_attempt_count"] == 1
    assert outcome.attempts == 2
    normalized = _normalize_outcome(outcome, request)
    assert normalized.reason_codes == ("PROBE_CALL_BUDGET_EXCEEDED",)

    openai_registration = replace(
        provider_by_id("OPENAI_RESPONSES_RESEARCH"),
        audit_source_url_budget=0,
    )
    openai_target = select_capability_targets(
        (openai_registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    openai_request = build_probe_requests(
        (openai_target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "openai-sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    assert audit_script._backend_invocation_attempt_count(  # noqa: SLF001
        openai_request,
        {"runtime_profile_id": "EVENT_MISSING_FIELDS"},
    ) == 0


class _OverBudgetResearchBackend:
    second_lifecycle = "started"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def execute_research(
        self,
        *,
        event_observer: Any,
        **_: Any,
    ) -> ResearchBackendResult:
        event_observer(
            {
                "event_type": "search",
                "lifecycle": "started",
                "item_id": "search-1",
                "query": "first query",
            }
        )
        event_observer(
            {
                "event_type": "search",
                "lifecycle": self.second_lifecycle,
                "item_id": "search-2",
                "query": "second query",
            }
        )
        raise AssertionError("second unique attempt must fail immediately")


@pytest.mark.asyncio
@pytest.mark.parametrize("second_lifecycle", ["started", "failed"])
async def test_research_tool_budget_fails_on_second_unique_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    second_lifecycle: str,
) -> None:
    registration = replace(
        provider_by_id("CODEX_CLI_RESEARCH_BACKEND"),
        audit_source_url_budget=0,
    )
    settings = _settings(tmp_path, research_backend="codex_cli")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    _OverBudgetResearchBackend.second_lifecycle = second_lifecycle
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _OverBudgetResearchBackend,
    )

    outcome = await audit_script.IsolatedRegistryProbeExecutor(settings)(
        request
    )

    assert outcome.transport_status == "UNUSABLE"
    assert outcome.reason_codes == ("RESEARCH_TOOL_BUDGET_EXCEEDED",)
    assert outcome.attempts == 0
    assert outcome.evidence["real_adapter_invoked"] is False
    assert outcome.evidence["probe_dispatch_status"] == "FAILED"
    assert outcome.evidence["capture_verified"] is False


class _SourceClaimResearchBackend:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def execute_research(
        self,
        *,
        job: dict[str, Any],
        **_: Any,
    ) -> ResearchBackendResult:
        gap = job["request_payload"]["gap"]
        return ResearchBackendResult(
            invocation_id="openai-audit-1",
            backend="openai_api",
            purpose="AGENTIC_RESEARCH",
            payload={
                "status": "COMPLETED",
                "claims": [
                    {
                        "topic": "macro",
                        "field_semantics": "forecast",
                        "value": "2.6",
                        "metric_id": gap["metric_id"],
                        "period": gap["expected_reference_period"],
                        "frequency": "monthly",
                        "unit": "percent",
                        "event_key": gap["expected_occurrence_id"],
                        "evidence": [
                            {
                                "source_url": "https://bea.gov/release/pce",
                                "canonical_url": None,
                                "publisher": "BEA",
                                "evidence_text": (
                                    "forecast 2.6 percent monthly 2026-07"
                                ),
                            }
                        ],
                    }
                ],
            },
        )


@pytest.mark.asyncio
async def test_probe_deadline_includes_post_output_source_get(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = replace(
        provider_by_id("OPENAI_RESPONSES_RESEARCH"),
        timeout=0.05,
    )
    settings = _settings(tmp_path, research_backend="openai_api")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _SourceClaimResearchBackend,
    )

    async def slow_source(_: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return httpx.Response(200, text="never reached")

    executor = audit_script.IsolatedRegistryProbeExecutor(
        settings,
        source_url_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(slow_source),
            follow_redirects=False,
        ),
        source_url_resolver=lambda _: ("93.184.216.34",),
    )
    started = asyncio.get_running_loop().time()
    outcome = await executor(request)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.5
    assert outcome.transport_status == "DOWN"
    assert outcome.reason_codes == ("PROBE_TIMEOUT",)


@pytest.mark.asyncio
async def test_same_source_url_is_fetched_once_per_run_for_atomic_bindings(
    tmp_path: Path,
) -> None:
    registration = provider_by_id("OPENAI_RESPONSES_RESEARCH")
    settings = _settings(tmp_path, research_backend="openai_api")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    correlation = request.correlation_for(target)
    source_url = "https://bea.gov/release/pce"
    reference_period = str(correlation["expected_reference_period"])
    occurrence_id = str(correlation["expected_occurrence_id"])
    forecast_anchor = (
        f"event forecast 2.6 percent monthly {reference_period}"
    )
    previous_anchor = (
        f"event previous 2.4 percent monthly {reference_period}"
    )
    normalized = {
        "claims": [
            {
                "field_semantics": field_name,
                "value": value,
                "metric_id": target.metric_id,
                "period": reference_period,
                "frequency": "monthly",
                "unit": "percent",
                "event_key": occurrence_id,
                "evidence": [
                    {
                        "source_url": source_url,
                        "publisher": "BEA",
                        "evidence_text": anchor,
                    }
                ],
            }
            for field_name, value, anchor in (
                ("forecast", "2.6", forecast_anchor),
                ("previous", "2.4", previous_anchor),
            )
        ]
    }
    calls = 0

    def source_handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            text=f"{forecast_anchor}; {previous_anchor}",
        )

    executor = audit_script.IsolatedRegistryProbeExecutor(
        settings,
        source_url_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(source_handler),
            follow_redirects=False,
        ),
        source_url_resolver=lambda _: ("93.184.216.34",),
    )
    with audit_script.HttpxAuditCapture(request) as capture:
        rows, exchanges = await executor._verify_ai_source_urls(  # noqa: SLF001
            request,
            normalized,
            capture,
        )

    assert calls == 1
    assert len(rows) == 1
    assert len(rows[0]["bindings"]) == 2
    assert rows[0]["verified"] is True
    assert all(
        item["content_claim_match"] is True
        for item in rows[0]["bindings"]
    )
    assert len(exchanges) == 1


@pytest.mark.asyncio
async def test_source_url_cache_revalidates_each_binding_policy(
    tmp_path: Path,
) -> None:
    registration = provider_by_id("OPENAI_RESPONSES_RESEARCH")
    settings = _settings(tmp_path, research_backend="openai_api")
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("event_missing_fields",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    correlation = request.correlation_for(target)
    reference_period = str(correlation["expected_reference_period"])
    anchor = f"event forecast 2.6 percent monthly {reference_period}"
    normalized = {
        "claims": [
            {
                "field_semantics": "forecast",
                "value": "2.6",
                "metric_id": target.metric_id,
                "period": reference_period,
                "frequency": "monthly",
                "unit": "percent",
                "event_key": correlation["expected_occurrence_id"],
                "evidence": [
                    {
                        "source_url": "https://bea.gov/release/pce",
                        "publisher": "BEA",
                        "evidence_text": anchor,
                    }
                ],
            }
        ]
    }
    calls = 0

    def source_handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, text=anchor)

    executor = audit_script.IsolatedRegistryProbeExecutor(
        settings,
        source_url_client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(source_handler),
            follow_redirects=False,
        ),
        source_url_resolver=lambda _: ("93.184.216.34",),
    )
    with audit_script.HttpxAuditCapture(request) as capture:
        first, _ = await executor._verify_ai_source_urls(  # noqa: SLF001
            request,
            normalized,
            capture,
        )
    restricted_target = replace(
        target,
        capability=replace(
            target.capability,
            runtime_source_domains=("sec.gov",),
        ),
    )
    restricted_request = replace(
        request,
        acquisition_id="restricted-acquisition",
        targets=(restricted_target,),
    )
    with audit_script.HttpxAuditCapture(restricted_request) as capture:
        second, _ = await executor._verify_ai_source_urls(  # noqa: SLF001
            restricted_request,
            normalized,
            capture,
        )
    publisher_mismatch = {
        "claims": [
            {
                **normalized["claims"][0],
                "evidence": [
                    {
                        **normalized["claims"][0]["evidence"][0],
                        "publisher": "SEC",
                    }
                ],
            }
        ]
    }
    with audit_script.HttpxAuditCapture(request) as capture:
        third, _ = await executor._verify_ai_source_urls(  # noqa: SLF001
            request,
            publisher_mismatch,
            capture,
        )

    assert calls == 1
    assert first[0]["verified"] is True
    assert second[0]["reused_from_run_cache"] is True
    assert second[0]["verified"] is False
    assert second[0]["bindings"][0]["binding_verification_reason"] == (
        "SOURCE_URL_NOT_IN_CAPABILITY_ALLOWLIST"
    )
    assert third[0]["verified"] is False
    assert third[0]["bindings"][0]["binding_verification_reason"] == (
        "SOURCE_URL_PUBLISHER_HOST_MISMATCH"
    )


class _StatusProvider:
    status_code = 403

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        status_code = self.status_code

        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code, content=b'{"error":"observed"}')

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            response = await client.get("https://api.example.test/series")
            return {"http_status": response.status_code}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "credentials", "settings_update", "expected"),
    [
        (403, (), {}, "UNUSABLE"),
        (403, ("fred_api_key",), {"fred_api_key": "configured"}, "AUTH_FAILED"),
        (429, (), {}, "RATE_LIMITED"),
        (503, (), {}, "DOWN"),
    ],
)
async def test_observed_http_failures_are_classified_without_synthetic_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    credentials: tuple[str, ...],
    settings_update: dict[str, Any],
    expected: str,
) -> None:
    _StatusProvider.status_code = status_code
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _StatusProvider)
    settings = _settings(tmp_path, **settings_update)
    execution = await ProviderCapabilityAuditEngine(
        (_registration(credentials=credentials),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    result = execution.report["results"][0]
    assert result["health_status"] == expected
    assert result["eligible_as_primary"] is False
    assert result["quality_score"] == 0


class _SlowProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        await asyncio.sleep(0.2)
        return {"actual": 1}


@pytest.mark.asyncio
async def test_real_adapter_timeout_is_bounded_and_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _SlowProvider)
    settings = _settings(tmp_path)
    execution = await ProviderCapabilityAuditEngine(
        (_registration(timeout=0.01),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    result = execution.report["results"][0]
    assert result["health_status"] == "DOWN"
    assert "PROBE_TIMEOUT" in result["reason_codes"]


class _PreSpawnHangingSubprocessProvider:
    async def audit_probe(self, _request: Any) -> dict[str, Any]:
        await asyncio.sleep(0.01)
        raise asyncio.TimeoutError


@pytest.mark.asyncio
async def test_subprocess_outer_timeout_without_spawn_is_unproved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        audit_script,
        "_load_symbol",
        lambda _: _PreSpawnHangingSubprocessProvider,
    )
    registration = {
        **_registration(timeout=0.01),
        "provider_type": "AI",
        "capture_mode": "SUBPROCESS",
    }
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    acquisition = execution.report["acquisitions"][0]
    provider = SimpleNamespace(
        provider_id="DIRECT",
        provider_type="AI",
        capture_mode="SUBPROCESS",
        configuration_setting=None,
    )
    assert execution.audit_status == "FAILED"
    assert acquisition["reason_codes"] == ["PROBE_TIMEOUT"]
    assert acquisition["attempts"] == 0
    assert acquisition["network_exchange_count"] == 0
    assert acquisition["real_adapter_invoked"] is False
    assert acquisition["probe_dispatch_status"] == "FAILED"
    assert acquisition["capture_mode"] == "NONE"
    assert acquisition["capture_verified"] is False
    assert acquisition["capture_attestation"] is None
    assert _capture_attestation_valid(acquisition, provider=provider) is False
    assert _terminal_runtime_failure_without_transport(acquisition) is False


class _ExplodingProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> Any:
        raise RuntimeError("failed before any source acquisition")


@pytest.mark.asyncio
async def test_invoked_adapter_runtime_failure_completes_with_degraded_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _ExplodingProvider)
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (_registration(),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    acquisition = execution.report["acquisitions"][0]
    assert execution.audit_status == "FAILED"
    assert "CONFIGURED_PROBE_DISPATCH_NOT_OBSERVED" in execution.report[
        "internal_errors"
    ]
    assert execution.report["configured_dispatches_missing"]
    assert acquisition["real_adapter_invoked"] is False
    assert acquisition["probe_dispatch_status"] == "FAILED"
    assert acquisition["attempts"] == 0
    assert acquisition["reason_codes"] == ["ADAPTER_RUNTIME_FAILED"]


class _PayloadProvider:
    payload: Any = None

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> Any:
        payload = self.payload

        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            response = await client.get("https://api.example.test/atomic-evidence")
            return response.json()


def _valid_atomic_payload(
    *,
    source_url: str = "https://api.example.test/atomic-evidence",
) -> dict[str, Any]:
    return {
        "actual": 2.6,
        "metric_id": "headline_pce_yoy",
        "name": "PCE A/A",
        "frequency": "monthly",
        "transformation": "identity",
        "unit": "percent",
        "freshness": "CURRENT_RELEASE",
        "occurrence_id": "fred:PCEPI:2026-06",
        "reference_period": "2026-06",
        "expected_reference_period": "2026-06",
        "lifecycle_verified": True,
        "provider_id": "DIRECT",
        "lineage": [
            {
                "field": "actual",
                "value_sha256": audit_script._stable_sha256(2.6),  # noqa: SLF001
                "publisher": "BEA",
                "distributor": "FRED",
                "acquisition_provider": "DIRECT",
                "source_url": source_url,
            }
        ],
    }


def test_freshness_requires_expected_occurrence_lifecycle_proof() -> None:
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    target = SimpleNamespace(dataset_id="", frequency="weekly")
    recent_previous_release = {
        "reference_period": "2026-07-17",
        "expected_reference_period": "2026-07-24",
        "freshness": "CURRENT_LATEST_OFFICIAL_RELEASE",
        "retrieved_at": now.isoformat(),
        "lifecycle_verified": True,
    }

    assert (
        audit_script._freshness_check(
            recent_previous_release,
            target=target,
            now=now,
        )
        is False
    )
    current_without_proof = {
        **recent_previous_release,
        "reference_period": "2026-07-24",
        "lifecycle_verified": None,
    }
    assert (
        audit_script._freshness_check(
            current_without_proof,
            target=target,
            now=now,
        )
        is None
    )


def test_field_scoped_semantics_require_frequency_transformation_and_unit() -> None:
    capability = SimpleNamespace(
        metric_id="price_change_weight",
        supported_fields=("change_pct",),
        canonical_metric_ids=(),
        field_validator_id="validate.market_quote.v1",
    )
    target = SimpleNamespace(
        metric_id="price_change_weight",
        frequency="intraday",
        transformation="identity",
        capability=capability,
        field_validator_id=capability.field_validator_id,
    )
    valid = {
        "change_pct": 1.25,
        "metric_id": "price_change_weight",
        "frequency": "intraday",
        "transformation": "identity",
        "unit": "percent",
    }

    assert audit_script._semantic_check(  # noqa: SLF001
        target,
        valid,
        valid,
        field_name="change_pct",
        value=1.25,
    ) is True
    for missing in ("frequency", "transformation", "unit"):
        payload = {key: item for key, item in valid.items() if key != missing}
        assert audit_script._semantic_check(  # noqa: SLF001
            target,
            payload,
            payload,
            field_name="change_pct",
            value=1.25,
        ) is None
    for key, wrong in (
        ("frequency", "daily"),
        ("transformation", "pct_change_yoy"),
        ("unit", "bananas"),
        ("unit", "shares"),
    ):
        payload = {**valid, key: wrong}
        assert audit_script._semantic_check(  # noqa: SLF001
            target,
            payload,
            payload,
            field_name="change_pct",
            value=1.25,
        ) is False
        assert _bound_semantic_valid(
            target=target,
            owner=payload,
            field_name="change_pct",
            value=1.25,
        ) is False


def test_schema_contract_rejects_container_boolean_and_invalid_temporal() -> None:
    capability = SimpleNamespace(
        metric_id="headline_pce_yoy",
        supported_fields=("actual", "released_at"),
        canonical_metric_ids=("headline_pce_yoy",),
        field_validator_id="validate.official_actual.v1",
    )
    target = SimpleNamespace(
        metric_id=capability.metric_id,
        frequency="monthly",
        transformation="pct_change_yoy",
        capability=capability,
        field_validator_id=capability.field_validator_id,
    )
    for wrong in ([], {}, True):
        assert audit_script._field_schema_check(  # noqa: SLF001
            target,
            "actual",
            {"actual": wrong},
            {"actual": wrong},
            wrong,
        ) is False
        assert _bound_schema_valid(
            target=target,
            field_name="actual",
            value=wrong,
        ) is False
    assert audit_script._field_schema_check(  # noqa: SLF001
        target,
        "actual",
        {"actual": None},
        {"actual": None},
        None,
    ) is True
    assert _bound_schema_valid(
        target=target,
        field_name="released_at",
        value="not-a-date",
    ) is False


def test_freshness_never_leaks_from_a_sibling_field_lineage() -> None:
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    target = SimpleNamespace(dataset_id="", frequency="intraday")
    actual = 2.6
    owner = {
        "actual": actual,
        "actual_lineage": {
            "field": "actual",
            "value_sha256": audit_script._stable_sha256(actual),  # noqa: SLF001
            "content_valid_until": "2026-07-30T12:00:00Z",
            "data_as_of": "2026-07-30T11:00:00Z",
        },
        "previous_lineage": {
            "field": "previous",
            "value_sha256": audit_script._stable_sha256(2.4),  # noqa: SLF001
            "content_valid_until": "2026-08-01T12:00:00Z",
            "data_as_of": "2026-07-31T11:00:00Z",
            "freshness": "CURRENT",
        },
    }
    assert audit_script._freshness_check(  # noqa: SLF001
        owner,
        target=target,
        field_name="actual",
        field_value=actual,
        now=now,
    ) is False
    assert _bound_freshness_valid(
        owner,
        target=target,
        field_name="actual",
        value=actual,
        checked_at=now,
    ) is False
    only_valid_sibling = {key: item for key, item in owner.items() if key != "actual_lineage"}
    assert audit_script._freshness_check(  # noqa: SLF001
        only_valid_sibling,
        target=target,
        field_name="actual",
        field_value=actual,
        now=now,
    ) is None


def test_unobserved_field_never_inherits_response_freshness(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    target = select_capability_targets(
        (_registration(),),
        AuditFilters(),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    payload = _valid_atomic_payload()
    del payload["actual"]

    _, field_checks = audit_script._evidence_checks(  # noqa: SLF001
        request,
        payload,
        observed_at=datetime(2026, 7, 31, 12, tzinfo=UTC),
    )

    assert field_checks[target.field_key("actual")]["freshness_valid"] is None


def test_exported_field_evidence_uses_only_field_bound_lifecycle(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    target = select_capability_targets(
        (_registration(),),
        AuditFilters(),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    value = 2.6
    payload = {
        **_valid_atomic_payload(),
        "actual_lineage": {
            "field": "actual",
            "value_sha256": audit_script._stable_sha256(value),  # noqa: SLF001
            "content_valid_until": "2020-01-01T00:00:00Z",
        },
        "previous_lineage": {
            "field": "previous",
            "value_sha256": audit_script._stable_sha256(2.4),  # noqa: SLF001
            "content_valid_until": "2099-01-01T00:00:00Z",
        },
    }

    evidence = audit_script._extract_field_evidence(  # noqa: SLF001
        request,
        payload,
        (),
    )[target.field_key("actual")]

    assert evidence["content_valid_until"] == "2020-01-01T00:00:00Z"
    assert evidence["freshness_verified"] is False


@pytest.mark.parametrize(
    ("frequency", "reference_period"),
    [
        ("monthly", "2025-12"),
        ("quarterly", "2025-Q4"),
    ],
)
def test_older_periodic_release_is_valid_only_with_explicit_latest_proof(
    frequency: str,
    reference_period: str,
) -> None:
    now = datetime(2026, 7, 31, 12, tzinfo=UTC)
    target = SimpleNamespace(dataset_id="", frequency=frequency)
    value = {
        "reference_period": reference_period,
        "expected_reference_period": reference_period,
        "freshness": "CURRENT_LATEST_OFFICIAL_RELEASE",
    }

    assert (
        audit_script._freshness_check(
            value,
            target=target,
            now=now,
        )
        is False
    )
    assert (
        audit_script._freshness_check(
            {**value, "latest_release_verified": True},
            target=target,
            now=now,
        )
        is True
    )


def test_occurrence_requires_exact_request_and_provider_correlation() -> None:
    target = SimpleNamespace(
        provider_id="DIRECT",
        metric_id="headline_pce_yoy",
        frequency="monthly",
    )
    expected = {
        "provider_id": "DIRECT",
        "expected_occurrence_id": "direct:event-a:2026-06",
        "expected_reference_period": "2026-06",
    }
    same_period_wrong_occurrence = {
        "provider_id": "DIRECT",
        "metric_id": "headline_pce_yoy",
        "occurrence_id": "direct:event-b:2026-06",
        "reference_period": "2026-06",
    }
    right_occurrence_wrong_provider = {
        **same_period_wrong_occurrence,
        "provider_id": "OTHER_PROVIDER",
        "occurrence_id": "direct:event-a:2026-06",
    }

    assert audit_script._occurrence_check(  # noqa: SLF001
        target,
        same_period_wrong_occurrence,
        same_period_wrong_occurrence,
        expected_correlation=expected,
    ) is False

    field_owner_with_valid_sibling = {
        "actual": 2.6,
        "provider_id": "DIRECT",
        "metric_id": "headline_pce_yoy",
        "occurrence_id": "direct:event-b:2026-06",
        "reference_period": "2026-06",
        "other_event": {
            "provider_id": "DIRECT",
            "metric_id": "headline_pce_yoy",
            "occurrence_id": "direct:event-a:2026-06",
            "reference_period": "2026-06",
        },
    }
    assert audit_script._occurrence_check(  # noqa: SLF001
        target,
        field_owner_with_valid_sibling,
        field_owner_with_valid_sibling,
        expected_correlation=expected,
        field_name="actual",
    ) is False
    assert audit_script._occurrence_check(  # noqa: SLF001
        target,
        right_occurrence_wrong_provider,
        right_occurrence_wrong_provider,
        expected_correlation=expected,
    ) is False


def test_normalized_replay_retains_registered_provider_identity() -> None:
    capability = SimpleNamespace(
        dataset_id="macro",
        metric_id="headline_pce_yoy",
        supported_fields=("actual",),
        audit_only_fields=(),
        frequency="monthly",
        transformation="identity",
        field_validator_id="validate.official_actual.v1",
        canonical_metric_ids=(),
    )
    provider = SimpleNamespace(
        provider_id="DIRECT",
        provider_type="OFFICIAL_API",
    )
    target = SimpleNamespace(
        provider_id=provider.provider_id,
        provider_type=provider.provider_type,
        dataset_id=capability.dataset_id,
        metric_id=capability.metric_id,
        frequency=capability.frequency,
        transformation=capability.transformation,
        field_validator_id=capability.field_validator_id,
        capability=capability,
        registration=provider,
    )
    target_id = (
        f"{provider.provider_id}|"
        f"{capability.dataset_id}|{capability.metric_id}"
    )
    normalized = {
        "actual": 2.6,
        "metric_id": capability.metric_id,
        "frequency": capability.frequency,
        "transformation": capability.transformation,
        "unit": "percent",
        "occurrence_id": "missing-provider:2026-06",
        "reference_period": "2026-06",
        "expected_reference_period": "2026-06",
        "lifecycle_verified": True,
    }
    observed, value, owner = (
        audit_script._find_target_field_observation(  # noqa: SLF001
            normalized,
            SimpleNamespace(
                target_id=target_id,
                metric_id=capability.metric_id,
            ),
            "actual",
        )
    )
    checked_at = datetime(2026, 7, 31, 12, tzinfo=UTC)
    checks = {
        "transport_valid": True,
        "schema_valid": audit_script._field_schema_check(  # noqa: SLF001
            target,
            "actual",
            normalized,
            owner,
            value,
        ),
        "completeness_valid": observed and value is not None,
        "freshness_valid": audit_script._freshness_check(  # noqa: SLF001
            owner,
            target=target,
            field_name="actual",
            field_value=value,
            now=checked_at,
        ),
        "semantic_mapping_valid": audit_script._semantic_check(  # noqa: SLF001
            target,
            owner,
            normalized,
            field_name="actual",
            value=value,
        ),
        "occurrence_match_valid": audit_script._occurrence_check(  # noqa: SLF001
            target,
            owner,
            normalized,
            expected_correlation={
                "provider_id": provider.provider_id,
                "target_id": target_id,
            },
            field_name="actual",
        ),
        "lineage_valid": audit_script._lineage_check(  # noqa: SLF001
            "actual",
            owner,
            normalized,
        ),
    }

    assert checks["occurrence_match_valid"] is False
    assert (
        _normalized_check_derivation_errors(
            {"field_results": {"actual": {"checks": checks}}},
            capability=capability,
            provider=provider,
            normalized_response=normalized,
            checked_at=checked_at,
        )
        == ()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "expected_check"),
    [
        (lambda _: "wrong-schema", "schema_valid"),
        (lambda value: {**value, "actual": None}, "completeness_valid"),
        (
            lambda value: {
                **value,
                "occurrence_id": "fred:PCEPI:2026-05",
            },
            "occurrence_match_valid",
        ),
        (
            lambda value: {
                **value,
                "reference_period": "2026-05",
            },
            "occurrence_match_valid",
        ),
        (
            lambda value: {
                **value,
                "content_valid_until": "2020-01-01T00:00:00Z",
            },
            "freshness_valid",
        ),
        (
            lambda value: {
                **value,
                "occurrence_id": "fred:PCEPI:1999-01",
                "reference_period": "1999-01",
                "retrieved_at": datetime.now(UTC).isoformat(),
                "freshness": "CURRENT_LATEST_OFFICIAL_RELEASE",
            },
            "freshness_valid",
        ),
        (
            lambda value: {**value, "name": "PCE M/M"},
            "semantic_mapping_valid",
        ),
        (lambda value: {**value, "lineage": []}, "lineage_valid"),
        (
            lambda value: {
                **value,
                "lineage": [{"field": "actual", "source": "DIRECT"}],
                "actual_source": "DIRECT",
            },
            "lineage_valid",
        ),
        (
            lambda value: {
                **value,
                "lineage": [
                    {
                        **value["lineage"][0],
                        "value_sha256": "0" * 64,
                    }
                ],
            },
            "lineage_valid",
        ),
    ],
    ids=[
        "schema",
        "completeness",
        "occurrence",
        "reference-period",
        "freshness",
        "old-official-retrieved-today",
        "semantic",
        "lineage",
        "source-only-lineage",
        "lineage-wrong-value",
    ],
)
async def test_atomic_correctness_failures_are_observed_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Any,
    expected_check: str,
) -> None:
    _PayloadProvider.payload = mutation(_valid_atomic_payload())
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _PayloadProvider)
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (_registration(),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    result = execution.report["results"][0]
    field = result["field_results"]["actual"]
    assert result["health_status"] == "UNUSABLE"
    assert field["checks"][expected_check] is False
    assert result["eligible_as_primary"] is False
    assert result["eligible_as_fallback"] is False


class _CacheAwareProvider:
    def __init__(
        self,
        cache: ProviderCacheRepository,
        settings: Settings,
    ) -> None:
        self.cache = cache
        self.settings = settings

    async def fetch(self) -> Any:
        if self.cache.get("provider:audit:operational-marker") is not None:
            raise AssertionError("operational cache leaked into external probe")

        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=_valid_atomic_payload(
                    source_url="https://api.example.test/cache-isolation"
                ),
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            response = await client.get("https://api.example.test/cache-isolation")
            return response.json()


@pytest.mark.asyncio
async def test_external_probe_uses_empty_request_local_cache_and_preserves_db_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operational = tmp_path / "operational.sqlite"
    ProviderCacheRepository(operational).set(
        "provider:audit:operational-marker",
        {"must_not_be_reused": True},
    )
    # The fixture must be byte-stable before the guard snapshots it. Windows
    # can flush/delete a just-closed WAL/SHM asynchronously, which would look
    # like an application write even though the audit never opened this file.
    gc.collect()
    with sqlite3.connect(operational) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        connection.commit()
        connection.execute("PRAGMA journal_mode=DELETE")
    guard = DatabaseBundleGuard(operational, tmp_path / "snapshot")
    snapshot = guard.create_snapshot()
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _CacheAwareProvider)
    settings = _settings(tmp_path, database_path=operational)

    execution = await ProviderCapabilityAuditEngine(
        (_registration(),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=snapshot,
        database_source_unchanged=guard.source_unchanged,
    )

    assert execution.report["results"][0]["health_status"] == "HEALTHY"
    assert execution.report["real_adapter_probes"] == 1
    assert guard.source_unchanged() is True


@pytest.mark.asyncio
async def test_shared_batch_cannot_certify_metric_missing_from_observed_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capabilities = tuple(
        {
            "dataset_id": "macro",
            "metric_id": metric_id,
            "supported_fields": ("actual",),
            "frequency": "monthly",
                "transformation": "identity",
                "probe_id": "probe.direct.batch",
                "field_validator_id": "validate.official_actual.v1",
                "request_group": "direct-monthly-batch",
        }
        for metric_id in ("headline_pce_yoy", "core_pce_yoy")
    )
    registration = {
        **_registration(),
        "capabilities": capabilities,
    }
    _PayloadProvider.payload = [_valid_atomic_payload()]
    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: _PayloadProvider)
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    by_metric = {
        row["metric_id"]: row for row in execution.report["results"]
    }
    assert execution.report["acquisitions_executed"] == 1
    assert by_metric["headline_pce_yoy"]["health_status"] == "HEALTHY"
    missing = by_metric["core_pce_yoy"]
    assert missing["health_status"] == "UNUSABLE"
    assert missing["field_results"]["actual"]["checks"]["schema_valid"] is False
    assert (
        missing["field_results"]["actual"]["checks"]["completeness_valid"]
        is False
    )


def test_field_observation_is_bound_to_exact_target_id() -> None:
    target = SimpleNamespace(
        target_id="DIRECT|macro|headline_pce_yoy",
        metric_id="headline_pce_yoy",
    )
    normalized = {
        "observations": [
            {
                "target_id": target.target_id,
                "metric_id": target.metric_id,
                "actual": 2.6,
            },
            {
                "target_id": "OTHER|macro|headline_pce_yoy",
                "metric_id": target.metric_id,
                "actual": 99.0,
            },
        ]
    }

    observed, value, owner = (
        audit_script._find_target_field_observation(  # noqa: SLF001
            normalized,
            target,
            "actual",
        )
    )

    assert observed is True
    assert value == 2.6
    assert owner["target_id"] == target.target_id


def test_field_observation_is_stable_after_canonical_json_sorting() -> None:
    target = SimpleNamespace(
        target_id="DIRECT|macro|headline_pce_yoy",
        metric_id="headline_pce_yoy",
    )
    normalized = {
        "z_branch": {
            "metric_id": target.metric_id,
            "actual": 99.0,
        },
        "a_branch": {
            "metric_id": target.metric_id,
            "actual": 2.6,
        },
    }
    persisted = json.loads(
        json.dumps(normalized, sort_keys=True)
    )

    before = audit_script._find_target_field_observation(  # noqa: SLF001
        normalized,
        target,
        "actual",
    )
    after = audit_script._find_target_field_observation(  # noqa: SLF001
        persisted,
        target,
        "actual",
    )

    assert before[0] is True
    assert before[1] == 2.6
    assert after[1] == before[1]
    assert after[2] == before[2]


@pytest.mark.asyncio
async def test_registered_local_projection_hook_invokes_real_transform_offline(
    tmp_path: Path,
) -> None:
    registration = next(
        item
        for item in PROVIDER_REGISTRY
        if item.provider_id == "SENIOR_ANALYST_PROJECTION"
    )
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    acquisition = execution.report["acquisitions"][0]
    assert execution.audit_status == "COMPLETED"
    assert acquisition["real_adapter_invoked"] is True
    assert acquisition["probe_dispatch_status"] == "REAL_ADAPTER"
    assert execution.report["unsupported_isolated_probes"] == 0


@pytest.mark.asyncio
async def test_legacy_earnings_local_probe_attests_fmp_only_wrapper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.providers import earnings_provider as earnings_module

    def forbidden_alpha_fixture(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("dead Alpha Vantage parser must not certify runtime")

    monkeypatch.setattr(
        earnings_module,
        "parse_alpha_vantage_earnings_calendar",
        forbidden_alpha_fixture,
    )
    registration = provider_by_id("LEGACY_EARNINGS_AGGREGATOR")
    settings = _settings(tmp_path)

    execution = await ProviderCapabilityAuditEngine(
        (registration,),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    acquisition = execution.report["acquisitions"][0]
    row = execution.report["results"][0]
    assert acquisition["real_adapter_invoked"] is True
    assert acquisition["probe_dispatch_status"] == "REAL_ADAPTER"
    assert row["health_status"] == "UNUSABLE"
    assert "OCCURRENCE_MATCH_VALID_FAILED" in row["reason_codes"]
    assert row["eligible_as_primary"] is False
    assert row["eligible_as_fallback"] is False
    runtime_row = execution.report["runtime_adapter_results"][0]
    assert runtime_row["runtime_adapter_path"] == registration.adapter_path
    assert runtime_row["health_status"] == "UNUSABLE"
    assert all(
        field_result["checks"]["schema_valid"] is False
        and field_result["checks"]["completeness_valid"] is False
        for field_result in runtime_row["field_results"].values()
    )


@pytest.mark.asyncio
async def test_real_ai_researcher_accepts_audit_input_before_fake_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import subprocess

    from app.providers.ai_researcher_provider import AIResearcherProvider
    from app.services.provider_capability_audit import (
        build_probe_requests,
        select_capability_targets,
    )

    registration = provider_by_id("AI_RESEARCHER")
    settings = _settings(
        tmp_path,
        enable_ai_researcher=True,
        ai_researcher_mode="codex_cli",
        codex_cli_command="codex",
        codex_workspace_dir=tmp_path / "codex-workspace",
    )
    target = select_capability_targets(
        (registration,),
        AuditFilters.from_values(metrics=("flash_services_pmi",)),
        settings=settings,
    )[0]
    request = build_probe_requests(
        (target,),
        run_id="20260731T120000Z",
        sandbox_root=tmp_path / "sandbox",
        database_snapshot_path=None,
        settings=settings,
    )[0]
    provider = AIResearcherProvider(settings)
    method, kwargs = audit_script._select_probe_method(  # noqa: SLF001
        provider,
        target,
        targets=request.targets,
        request_correlation=request.correlation_for(target),
        request_correlations=request.request_correlations,
    )
    subprocess_calls = 0

    def fake_run(command: list[str], **run_kwargs: Any) -> Any:
        nonlocal subprocess_calls
        subprocess_calls += 1
        event = events[0]
        output = {
            "generated_at": event["time_utc"],
            "results": [
                {
                    "fact_key": event["fact_key"],
                    "country": event["country"],
                    "date": event["date"],
                    "time_utc": event["time_utc"],
                    "category": event["category"],
                    "event_name": event["event_name"],
                    "period": event["reference_period"],
                    "metric_id": event["metric_id"],
                    "forecast": "52.0",
                    "previous": "51.0",
                    "consensus": None,
                    "actual": None,
                    "unit": "index_points",
                    "frequency": event["frequency"],
                    "source": "S&P Global Market Intelligence",
                    "source_url": "https://www.pmi.spglobal.com/",
                    "evidence_text": (
                        "S&P Global reported a Flash Services PMI "
                        "forecast of 52.0 and previous value of 51.0."
                    ),
                    "extracted_text": None,
                    "reliability": 0.9,
                    "confidence": 0.9,
                    "valid_until": event["valid_until"],
                    "notes": None,
                    "warnings": [],
                    "metrics": [],
                    "fomc_context": None,
                }
            ],
        }
        output_index = command.index("--output-last-message")
        Path(command[output_index + 1]).write_text(
            json.dumps(output),
            encoding="utf-8",
        )
        stdout = json.dumps(output)
        completed = subprocess.CompletedProcess(
            command,
            0,
            stdout=stdout,
            stderr="",
        )
        output_bytes = run_kwargs["output_path"].read_bytes()
        return (
            completed,
            {
                "process_observed": True,
                "process_id": 123,
                "exit_code": 0,
                "process_terminated": True,
                "stdout_sha256": hashlib.sha256(
                    stdout.encode()
                ).hexdigest(),
                "stderr_sha256": hashlib.sha256(b"").hexdigest(),
                "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
                "output_size_bytes": len(output_bytes),
                "output_observed": True,
                "command_sha256": hashlib.sha256(
                    json.dumps(command).encode()
                ).hexdigest(),
            },
            None,
        )

    monkeypatch.setattr(
        "app.providers.ai_researcher_provider._resolve_command",
        lambda command: [command],
    )
    monkeypatch.setattr(
        "app.providers.ai_researcher_provider._run_attested_codex_subprocess",
        fake_run,
    )

    assert method is not None
    events = kwargs["events"]
    assert events[0]["fact_key"].endswith(request.request_key[:16])
    assert (
        events[0]["occurrence_id"]
        == request.correlation_for(target)["expected_occurrence_id"]
    )
    facts, status = await method(**kwargs)

    assert subprocess_calls == 1
    assert len(facts) == 1, status
    assert facts[0]["fact_key"] == events[0]["fact_key"]
    assert facts[0]["previous"] == "51.0"
    assert status["status"] == "success"
    assert status["results_valid"] == 1
    assert status["prompt_contains_input"] is True


@pytest.mark.asyncio
async def test_composite_adapter_without_source_specific_surface_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NewsProvider:
        def __init__(self, settings: Settings) -> None:
            raise AssertionError("composite adapter must not be constructed")

    monkeypatch.setattr(audit_script, "_load_symbol", lambda _: NewsProvider)
    settings = _settings(tmp_path)
    execution = await ProviderCapabilityAuditEngine(
        (_registration(adapter_path="app.providers.news_provider:NewsProvider"),),
        audit_script.IsolatedRegistryProbeExecutor(settings),
        settings=settings,
    ).run(sandbox_root=tmp_path / "sandbox")

    result = execution.report["results"][0]
    assert result["health_status"] == "UNUSABLE"
    assert "ISOLATED_REAL_PROBE_NOT_IMPLEMENTED" in result["reason_codes"]
    assert execution.report["unsupported_isolated_probes"] == 1
    assert execution.audit_status == "FAILED"
    assert "UNSUPPORTED_CONFIGURED_PROBES" in execution.report["internal_errors"]


def test_http_capture_allows_distinct_leaves_with_declared_call_budget() -> None:
    capture = audit_script.HttpxAuditCapture(
        SimpleNamespace(max_attempts=1, call_budget=2)
    )
    capture._before(httpx.Request("GET", "https://offline.invalid/first"))
    capture._before(httpx.Request("GET", "https://offline.invalid/second"))

    with pytest.raises(
        audit_script.ProbeExecutionError,
        match="acquisition call budget",
    ):
        capture._before(
            httpx.Request("GET", "https://offline.invalid/third")
        )

    assert capture.attempts == 2


def test_http_capture_enforces_retry_limit_per_request_fingerprint() -> None:
    capture = audit_script.HttpxAuditCapture(
        SimpleNamespace(max_attempts=1, call_budget=2)
    )
    request = httpx.Request("GET", "https://offline.invalid/first")
    capture._before(request)

    with pytest.raises(
        audit_script.ProbeExecutionError,
        match="fingerprint retry limit",
    ):
        capture._before(request)

    assert capture.attempts == 1


def test_subprocess_declared_source_requires_audit_transport_capture() -> None:
    value = 2.6
    owner = {
        "actual": value,
        "actual_lineage": {
            "field": "actual",
            "value": value,
            "publisher": "BEA",
            "distributor": "AI_RESEARCHER",
            "acquisition_provider": "AI_RESEARCHER",
            "source_url": "https://offline.invalid/source",
            "source_url_reachable": True,
            "source_content_sha256": "a" * 64,
            "verification_origin": "SOURCE_GATEWAY",
        },
    }

    declared_only = audit_script._field_lineage_binding(
        "actual",
        value,
        owner,
        expected_provider_id="AI_RESEARCHER",
        capture_mode="SUBPROCESS",
        exchanges=(),
    )
    captured_body = b"server-captured-source"
    captured = audit_script._field_lineage_binding(
        "actual",
        value,
        owner,
        expected_provider_id="AI_RESEARCHER",
        capture_mode="SUBPROCESS",
        exchanges=(
            audit_script.CapturedHttpExchange(
                method="GET",
                url="https://offline.invalid/source",
                request_headers=(),
                status_code=200,
                response_headers=(),
                response_body=captured_body,
                latency_ms=0.1,
                attempt=1,
            ),
        ),
    )

    assert declared_only is None
    assert captured is not None
    assert captured["verification_origin"] == "AUDIT_TRANSPORT"


def test_subprocess_output_capture_is_exact_and_sandbox_contained(
    tmp_path: Path,
) -> None:
    sandbox = tmp_path / "sandbox"
    sandbox.mkdir()
    exact = b'{"results":[]}\r\n'
    output = sandbox / "research-output.json"
    output.write_bytes(exact)
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"outside")

    assert audit_script._subprocess_output_bytes(
        {"output_path": str(output)},
        sandbox,
    ) == exact
    assert (
        audit_script._subprocess_output_bytes(
            {"output_path": str(outside)},
            sandbox,
        )
        is None
    )


@pytest.mark.asyncio
async def test_aaii_audit_dispatch_has_no_browser_fallback_surface() -> None:
    calls = 0

    class ControlledAaii:
        settings = SimpleNamespace()

        async def fetch(self) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            return {"status": "not_found"}

    result = await audit_script._invoke_registered_source(
        ControlledAaii(),
        SimpleNamespace(
            provider_id="AAII",
            targets=(SimpleNamespace(),),
            settings=SimpleNamespace(),
        ),
    )

    assert result == {"status": "not_found"}
    assert calls == 1
