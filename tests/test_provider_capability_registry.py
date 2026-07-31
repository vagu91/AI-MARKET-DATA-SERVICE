from __future__ import annotations

import ast
import hashlib
from collections import Counter
from dataclasses import asdict, FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import app.services.provider_capability_registry as registry
from app.core.config import Settings
from app.services.provider_capability_audit import (
    HealthStatus,
    ProbeExecutionError,
)
from app.services.provider_capability_probe_hooks import LocalCapabilityProbeHook
from app.services.provider_capability_registry import (
    DATASET_SOURCE_POLICIES,
    FIELD_VALIDATOR_SCHEMAS,
    MARKET_FACT_REPOSITORY_DATASET_QUERIES,
    OFFICIAL_METRIC_REGISTRATIONS,
    OFFICIAL_SOURCE_PROBE_QUERY_IDS,
    PROVIDER_REGISTRY,
    authoritative_calendar_adapter_identities,
    automatic_ai_delivery_authorized,
    capability_delivery_keys,
    dataset_policy_by_id,
    dataset_runtime_provider_order,
    discover_runtime_adapter_paths,
    provider_by_id,
    render_matrix_json,
    render_matrix_markdown,
    registry_summary,
    runtime_adapter_paths,
    validate_registry,
)


ROOT = Path(__file__).resolve().parents[1]
JSON_MATRIX = (
    ROOT / "docs" / "baselines" / "senior-analyst-data-source-matrix.json"
)
MARKDOWN_MATRIX = ROOT / "docs" / "senior-analyst-data-source-matrix.md"
ABSTRACT_PROVIDER_CLASSES = {
    "BaseProvider",
    "BrowserCalendarEnrichmentProvider",
    "CalendarEnrichmentProvider",
}


def _concrete_provider_adapter_paths() -> set[str]:
    paths: set[str] = set()
    provider_root = ROOT / "app" / "providers"
    for source_path in sorted(provider_root.rglob("*.py")):
        if source_path.name == "__init__.py":
            continue
        module = ".".join(
            source_path.relative_to(ROOT).with_suffix("").parts
        )
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in tree.body:
            if (
                isinstance(node, ast.ClassDef)
                and node.name.endswith("Provider")
                and node.name not in ABSTRACT_PROVIDER_CLASSES
            ):
                paths.add(f"{module}:{node.name}")
    return paths


def test_registry_is_valid_complete_and_importable() -> None:
    assert validate_registry(raise_on_error=False) == ()
    summary = registry_summary()
    assert len(PROVIDER_REGISTRY) == 66
    assert summary["providers_registered"] == 66
    assert sum(
        len(provider.capabilities) for provider in PROVIDER_REGISTRY
    ) == 218
    assert summary["capabilities_registered"] == 218
    assert summary["official_metrics_registered"] == 26
    assert summary["dataset_policies_registered"] == 25
    assert summary["runtime_adapters_registered"] == 57
    assert summary["runtime_adapters_discovered"] == 57
    assert summary["field_validator_schemas_registered"] == 36
    assert summary["request_groups_registered"] == 13
    assert summary["request_group_ids"] == [
        (
            "AI_RESEARCHER:probe.ai.researcher.macro_calendar:"
            "ai_researcher.concrete_capabilities.batch"
        ),
        "BEA:probe.bea.nipa:bea.bea_nipa.gdp.batch",
        "BEA:probe.bea.nipa:bea.bea_nipa.pce.batch",
        "BLS:probe.bls.series:bls.series.batch",
        (
            "CANONICAL_EVENT_REPOSITORY:probe.repository.event_values:"
            "repository.event_values.occurrence_fields"
        ),
        "CBOE:probe.cboe.risk_indices:cboe.risk_indices.batch",
        (
            "CENSUS:probe.census.eits:"
            "census.census_eits.resconst.batch"
        ),
        (
            "FRED:probe.fred.series:"
            "fred.fred_series.fed_funds.batch"
        ),
        (
            "FRED:probe.fred.series:"
            "fred.fred_series.target_range.batch"
        ),
        (
            "FRED:probe.fred.series:"
            "fred.fred_series.treasury_rates.batch"
        ),
        (
            "MACRO_CONSENSUS_RECONCILIATION:"
            "probe.transform.macro_consensus:"
            "macro_consensus_reconciliation."
            "transform_macro_consensus.macro_calendar.batch"
        ),
        (
            "MARKET_FACT_REPOSITORY:"
            "probe.repository.market_facts:"
            "repository.market_facts.dataset_lookups"
        ),
        (
            "RISK_CONTEXT_REPOSITORY:probe.repository.risk_context:"
            "repository.risk_context.latest"
        ),
    ]
    assert summary["provider_registry_coverage"] == 100


def test_authoritative_calendar_identities_follow_schedule_runtime_bindings() -> None:
    identities = authoritative_calendar_adapter_identities()

    assert set(identities) == {"FED", "BLS", "BEA"}
    assert {
        source: (
            row["provider_id"],
            row["adapter_name"],
            row["source"],
            row["query_scope"],
        )
        for source, row in identities.items()
    } == {
        "FED": (
            "FEDERAL_RESERVE",
            "FederalReserveCalendarProvider",
            "Federal Reserve Calendar",
            "fomc_occurrence",
        ),
        "BLS": (
            "BLS",
            "BlsReleaseCalendarProvider",
            "BLS Release Calendar",
            "bls_release_occurrence",
        ),
        "BEA": (
            "BEA",
            "BeaReleaseScheduleProvider",
            "BEA Release Schedule",
            "bea_release_occurrence",
        ),
    }


def test_fallback_policies_require_delivery_capability_overlap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fred_vix = next(
        capability
        for capability in provider_by_id("FRED").capabilities
        if capability.dataset_id == "vix"
    )
    cboe = provider_by_id("CBOE")
    cboe_vix = next(
        capability
        for capability in cboe.capabilities
        if capability.dataset_id == "vix"
    )
    assert fred_vix.delivery_capability_id == "vix"
    assert cboe_vix.delivery_capability_id == "vix"
    assert capability_delivery_keys(fred_vix) == {
        ("vix", "value"),
        ("vix", "data_as_of"),
        ("vix", "lineage"),
    }
    assert capability_delivery_keys(cboe_vix) == {
        ("vix", "value"),
        ("vix", "data_as_of"),
        ("vix", "lineage"),
    }

    mutated_cboe = replace(
        cboe,
        capabilities=tuple(
            replace(
                capability,
                delivery_capability_id=None,
            )
            if capability is cboe_vix
            else capability
            for capability in cboe.capabilities
        ),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        tuple(
            mutated_cboe
            if provider.provider_id == "CBOE"
            else provider
            for provider in PROVIDER_REGISTRY
        ),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        "dataset_policy_fallback_capability_disjoint:"
        "vix:FRED:CBOE"
    ) in errors


def test_flash_pmi_fallback_has_explicit_metric_field_relation() -> None:
    primary = next(
        capability
        for capability in provider_by_id("SPGLOBAL").capabilities
        if capability.dataset_id == "flash_services_pmi"
    )
    fallback = next(
        capability
        for capability in provider_by_id("INVESTING_EVENT_1062").capabilities
        if capability.dataset_id == "flash_services_pmi"
    )

    assert capability_delivery_keys(primary) == {
        ("flash_services_pmi", "actual"),
        ("flash_services_pmi", "lineage"),
    }
    assert capability_delivery_keys(fallback) == capability_delivery_keys(
        primary
    )


def test_cboe_leaf_request_budget_is_central_and_validated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cboe = provider_by_id("CBOE")
    assert cboe.audit_leaf_request_count == 23
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        tuple(
            replace(provider, audit_leaf_request_count=0)
            if provider.provider_id == "CBOE"
            else provider
            for provider in PROVIDER_REGISTRY
        ),
    )

    assert (
        "provider_invalid_audit_leaf_request_count:CBOE:0"
        in validate_registry(raise_on_error=False)
    )


def test_every_provider_declares_the_auditable_capture_mode() -> None:
    local_types = {
        "MANUAL_FILE",
        "RECONCILIATION",
        "REPOSITORY",
        "TRANSFORMATION",
    }
    for provider in PROVIDER_REGISTRY:
        expected = (
            "SUBPROCESS"
            if provider.provider_id
            in {"AI_RESEARCHER", "CODEX_CLI_RESEARCH_BACKEND"}
            else (
                "LOCAL_SANDBOX"
                if provider.provider_type in local_types
                else "HTTPX"
            )
        )
        assert provider.capture_mode == expected, provider.provider_id


def test_every_concrete_runtime_provider_is_registered_exactly_once() -> None:
    source_adapters = _concrete_provider_adapter_paths()
    source_adapters.update(
        {
            (
                "app.services.ai_research_job_executor:"
                "PersistentAIJobExecutor"
            ),
            (
                "app.services.research_backend:"
                "OpenAIResponsesResearchBackend"
            ),
            "app.services.research_source_gateway:ResearchSourceGateway",
            (
                "app.services.evidence_verification_service:"
                "DeterministicEvidenceVerifier"
            ),
            (
                "app.services.agentic_research_runtime:"
                "AgenticResearchRuntime"
            ),
            "app.services.ai_research_worker:AIResearchWorker",
        }
    )
    discovered_adapters = set(discover_runtime_adapter_paths())
    registered_adapters = set(runtime_adapter_paths())

    assert source_adapters == discovered_adapters == registered_adapters
    assert len(runtime_adapter_paths()) == len(registered_adapters)


def test_capability_metrics_are_atomic_and_probe_adapter_overrides_import() -> None:
    for provider in PROVIDER_REGISTRY:
        for capability in provider.capabilities:
            assert "," not in capability.metric_id
            if capability.probe_adapter_path:
                module_name, symbol_name = capability.probe_adapter_path.split(
                    ":",
                    1,
                )
                module = importlib.import_module(module_name)
                assert getattr(module, symbol_name)


def test_market_fact_repository_capabilities_are_dataset_scoped() -> None:
    provider = next(
        item
        for item in PROVIDER_REGISTRY
        if item.provider_id == "MARKET_FACT_REPOSITORY"
    )
    expected_datasets = {
        policy.dataset_id
        for policy in DATASET_SOURCE_POLICIES
        if policy.canonical_repository == provider.provider_id
    }

    assert "*" not in {
        capability.dataset_id for capability in provider.capabilities
    }
    assert {
        capability.dataset_id for capability in provider.capabilities
    } == expected_datasets == set(
        MARKET_FACT_REPOSITORY_DATASET_QUERIES
    )
    assert len(provider.capabilities) == len(expected_datasets) == 19
    assert all(
        capability.metric_id
        == f"canonical_{capability.dataset_id}_record"
        for capability in provider.capabilities
    )
    assert all(
        capability.frequency
        == MARKET_FACT_REPOSITORY_DATASET_QUERIES[
            capability.dataset_id
        ].frequency
        for capability in provider.capabilities
    )


def test_registry_rejects_market_fact_repository_wildcard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_index = next(
        index
        for index, item in enumerate(PROVIDER_REGISTRY)
        if item.provider_id == "MARKET_FACT_REPOSITORY"
    )
    provider = PROVIDER_REGISTRY[provider_index]
    mutated_provider = replace(
        provider,
        capabilities=(
            replace(provider.capabilities[0], dataset_id="*"),
            *provider.capabilities[1:],
        ),
    )
    mutated_registry = list(PROVIDER_REGISTRY)
    mutated_registry[provider_index] = mutated_provider
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        tuple(mutated_registry),
    )

    errors = validate_registry(raise_on_error=False)

    assert "market_fact_repository_wildcard_capability" in errors
    assert (
        "market_fact_repository_dataset_capability_missing:nasdaq_100"
        in errors
    )
    assert (
        "dataset_policy_repository_capability_missing:"
        "nasdaq_100:MARKET_FACT_REPOSITORY"
        in errors
    )


def test_every_capability_has_an_exact_non_wildcard_dataset_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert all(
        capability.dataset_id != "*"
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
    )
    provider_index = next(
        index
        for index, provider in enumerate(PROVIDER_REGISTRY)
        if provider.provider_id == "PROVIDER_CACHE_REPOSITORY"
    )
    provider = PROVIDER_REGISTRY[provider_index]
    broken = replace(
        provider,
        capabilities=(
            replace(provider.capabilities[0], dataset_id="*"),
        ),
    )
    providers = list(PROVIDER_REGISTRY)
    providers[provider_index] = broken
    monkeypatch.setattr(registry, "PROVIDER_REGISTRY", tuple(providers))

    errors = validate_registry(raise_on_error=False)

    assert (
        "capability_wildcard_dataset_not_atomic:"
        "PROVIDER_CACHE_REPOSITORY:provider_cache_entry"
    ) in errors


def test_registry_and_dataset_policy_are_frozen() -> None:
    with pytest.raises(FrozenInstanceError):
        PROVIDER_REGISTRY[0].provider_id = "MUTATED"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        DATASET_SOURCE_POLICIES[0].dataset_id = "mutated"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        OFFICIAL_METRIC_REGISTRATIONS[0].unit = "mutated"  # type: ignore[misc]


def test_uncertified_callable_runtime_leaves_are_ci_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert registry_summary()["uncertified_runtime_leaves_registered"] == 0
    provider_index = next(
        index
        for index, provider in enumerate(PROVIDER_REGISTRY)
        if provider.provider_id == "AAII"
    )
    provider = PROVIDER_REGISTRY[provider_index]
    broken = replace(
        provider,
        uncertified_runtime_leaves=(
            "app.providers.aaii_sentiment_provider:"
            "parse_aaii_sentiment",
        ),
    )
    providers = list(PROVIDER_REGISTRY)
    providers[provider_index] = broken
    monkeypatch.setattr(registry, "PROVIDER_REGISTRY", tuple(providers))

    errors = validate_registry(raise_on_error=False)

    assert (
        "provider_uncertified_runtime_leaf_callable:AAII:"
        "app.providers.aaii_sentiment_provider:parse_aaii_sentiment"
        in errors
    )


def test_target_range_policy_matches_real_fred_series_capabilities() -> None:
    policy = dataset_policy_by_id("target_range")
    fred = provider_by_id("FRED")
    federal_reserve = provider_by_id("FEDERAL_RESERVE")
    target_capabilities = tuple(
        capability
        for capability in fred.capabilities
        if capability.dataset_id == "target_range"
    )

    assert policy.primary_provider == "FRED"
    assert policy.fallback_providers == ()
    assert dataset_runtime_provider_order("target_range", ("FRED",)) == (
        "FRED",
    )
    assert {capability.metric_id for capability in target_capabilities} == {
        "DFEDTARL",
        "DFEDTARU",
    }
    assert {
        capability.supported_fields
        for capability in target_capabilities
    } == {
        (
            "series_id",
            "value",
            "observations",
            "units",
            "frequency",
            "data_as_of",
            "source",
            "source_url",
        )
    }
    assert {
        (capability.dataset_id, capability.metric_id)
        for capability in federal_reserve.capabilities
    } == {("macro_calendar", "fomc_occurrence")}
    assert not {
        "actual",
        "lower_bound",
        "upper_bound",
    }.intersection(
        field_name
        for capability in federal_reserve.capabilities
        for field_name in capability.supported_fields
    )


def test_official_metrics_have_one_exact_raw_relation_and_transform() -> None:
    official_ids = {
        specification.canonical_metric_id
        for specification in OFFICIAL_METRIC_REGISTRATIONS
    }
    assert len(official_ids) == len(OFFICIAL_METRIC_REGISTRATIONS) == 26

    source_relations: Counter[str] = Counter()
    source_provider_ids = {
        specification.provider_id
        for specification in OFFICIAL_METRIC_REGISTRATIONS
    }
    for provider in PROVIDER_REGISTRY:
        for capability in provider.capabilities:
            if provider.provider_id not in source_provider_ids:
                continue
            for canonical_metric_id in capability.canonical_metric_ids:
                source_relations[canonical_metric_id] += 1
                specification = next(
                    item
                    for item in OFFICIAL_METRIC_REGISTRATIONS
                    if item.canonical_metric_id == canonical_metric_id
                )
                assert provider.provider_id == specification.provider_id
                assert capability.dataset_id == specification.dataset_id
                assert capability.metric_id == specification.source_series_id
                assert capability.frequency == specification.frequency
                assert capability.transformation == "identity"
                assert capability.probe_query_id == (
                    OFFICIAL_SOURCE_PROBE_QUERY_IDS.get(
                        specification.source_series_id
                    )
                )
                assert not {
                    "actual",
                    "reference_period",
                    "released_at",
                }.intersection(capability.supported_fields)

    transform_provider = next(
        provider
        for provider in PROVIDER_REGISTRY
        if provider.provider_id == "OFFICIAL_ACTUAL_TRANSFORMATION"
    )
    transform_counts = Counter(
        capability.metric_id
        for capability in transform_provider.capabilities
    )
    assert source_relations == Counter({item: 1 for item in official_ids})
    assert transform_counts == Counter({item: 1 for item in official_ids})

    for capability in transform_provider.capabilities:
        specification = next(
            item
            for item in OFFICIAL_METRIC_REGISTRATIONS
            if item.canonical_metric_id == capability.metric_id
        )
        assert (
            capability.dataset_id,
            capability.frequency,
            capability.transformation,
        ) == (
            specification.dataset_id,
            specification.frequency,
            specification.transformation,
        )


def test_capability_field_identity_is_unique_and_ai_targets_are_bounded() -> None:
    identities = [
        (
            provider.provider_id,
            capability.dataset_id,
            capability.metric_id,
            field_name,
        )
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
        for field_name in capability.supported_fields
    ]
    assert len(identities) == len(set(identities))

    ai_provider = next(
        provider
        for provider in PROVIDER_REGISTRY
        if provider.provider_id == "AI_RESEARCHER"
    )
    canonical_capabilities = [
        capability
        for capability in ai_provider.capabilities
        if capability.canonical_metric_ids
    ]
    official_ids = {
        item.canonical_metric_id
        for item in OFFICIAL_METRIC_REGISTRATIONS
    }
    assert len(canonical_capabilities) == 26
    assert {item.metric_id for item in canonical_capabilities} == official_ids
    assert all(
        item.canonical_metric_ids == (item.metric_id,)
        for item in canonical_capabilities
    )
    assert all(
        {
            "consensus",
            "previous",
            "occurrence_id",
            "reference_period",
            "source",
            "source_url",
            "lineage",
        }.issubset(capability.supported_fields)
        for capability in canonical_capabilities
    )
    flash = next(
        item
        for item in canonical_capabilities
        if item.metric_id == "flash_services_pmi"
    )
    pce_yoy = next(
        item
        for item in canonical_capabilities
        if item.metric_id == "headline_pce_yoy"
    )
    assert flash.dataset_id == "flash_services_pmi"
    assert "actual" not in flash.supported_fields
    assert flash.audit_only_fields == (
        "actual",
        "previous_revised",
        "previous_revised_lineage",
    )
    assert "previous_revised" not in flash.supported_fields
    assert pce_yoy.dataset_id == "macro_calendar"
    assert "previous" in pce_yoy.supported_fields
    assert all(
        "forecast" not in item.supported_fields
        for item in canonical_capabilities
    )
    openai_enrichment = next(
        provider
        for provider in PROVIDER_REGISTRY
        if provider.provider_id == "OPENAI_EVENT_ENRICHMENT"
    )
    assert openai_enrichment.provider_type == "AI"
    assert openai_enrichment.allowed_roles == ("AUDIT_ONLY",)
    assert all(
        not capability.ai_eligible
        and not capability.canonical_metric_ids
        for capability in openai_enrichment.capabilities
    )


def test_automatic_ai_delivery_requires_concrete_policy_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert automatic_ai_delivery_authorized() is False
    assert automatic_ai_delivery_authorized(
        dataset_id="macro_calendar",
        metric_id="headline_pce_yoy",
        fields=("previous",),
    ) is False

    backend = provider_by_id("CODEX_CLI_RESEARCH_BACKEND")
    capability = backend.capabilities[0]
    tampered = replace(capability, ai_eligible=True)
    broken = replace(
        backend,
        capabilities=(tampered, *backend.capabilities[1:]),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        tuple(
            broken if provider is backend else provider
            for provider in PROVIDER_REGISTRY
        ),
    )

    assert any(
        error.startswith(
            "ai_capability_not_concrete_policy_bound:"
            "CODEX_CLI_RESEARCH_BACKEND:ai_research_runtime:"
        )
        for error in validate_registry(raise_on_error=False)
    )


def test_ai_field_certification_does_not_authorize_another_target() -> None:
    from app.services.provider_capability_audit import CORRECTNESS_CHECKS
    from scripts.validate_senior_analyst_payload import (
        _certified_ai_fields,
    )

    certified = _certified_ai_fields(
        {
            "results": [
                {
                    "provider_id": "AI_RESEARCHER",
                    "dataset_id": "macro_calendar",
                    "metric_id": "headline_pce_yoy",
                    "configured": True,
                    "real_adapter_invoked": True,
                    "probe_dispatch_status": "REAL_ADAPTER",
                    "field_results": {
                        "previous": {
                            "health_status": "HEALTHY",
                            "checks": {
                                check: True
                                for check in CORRECTNESS_CHECKS
                            },
                        }
                    },
                }
            ]
        }
    )

    assert certified == set()
    assert (
        "AI_RESEARCHER",
        "macro_calendar",
        "headline_pce_mom",
        "previous",
    ) not in certified
    assert (
        "AI_RESEARCHER",
        "macro_calendar",
        "headline_pce_yoy",
        "consensus",
    ) not in certified


@pytest.mark.asyncio
async def test_local_official_transform_probe_is_target_specific(
    tmp_path: Path,
) -> None:
    provider = next(
        item
        for item in PROVIDER_REGISTRY
        if item.provider_id == "OFFICIAL_ACTUAL_TRANSFORMATION"
    )
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "probe.sqlite",
    )
    hook = LocalCapabilityProbeHook(settings)

    for capability in provider.capabilities:
        target = SimpleNamespace(
            metric_id=capability.metric_id,
            frequency=capability.frequency,
            transformation=capability.transformation,
        )
        result = await hook.audit_probe(
            SimpleNamespace(
                provider_id=provider.provider_id,
                registration=provider,
                targets=(target,),
                run_id="offline-registry-test",
            )
        )

        assert result["metric_id"] == capability.metric_id
        assert result["frequency"] == capability.frequency
        assert result["transformation"] == capability.transformation
        assert result["actual"] == result["value"]
        assert (
            result["lineage"]["actual"]["metric_id"]
            == capability.metric_id
        )


@pytest.mark.asyncio
async def test_local_market_fact_probe_performs_dataset_specific_lookups(
    tmp_path: Path,
) -> None:
    from app.services.market_fact_repository import MarketFactRepository

    current = datetime.now(UTC).replace(microsecond=0)
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "probe.sqlite",
    )
    repository = MarketFactRepository(settings, clock=lambda: current)
    repository.upsert_fact(
        {
            "fact_key": "US:DGS2:latest:official_macro_latest",
            "fact_type": "official_macro_latest",
            "country": "US",
            "category": "DGS2",
            "value": "4.25",
            "source": "FRED",
            "source_url": "https://fred.stlouisfed.org/series/DGS2",
            "retrieved_at": current.isoformat(),
            "release_at": current.isoformat(),
            "valid_until": (current + timedelta(days=1)).isoformat(),
            "next_refresh_at": (
                current + timedelta(hours=12)
            ).isoformat(),
            "raw_payload_json": {
                "series_id": "DGS2",
                "value": 4.25,
                "data_as_of": current.date().isoformat(),
                "lineage": [
                    {
                        "field": "value",
                        "source": "FRED",
                    }
                ],
            },
            "field_lineage_json": [
                {
                    "field": "value",
                    "source": "FRED",
                }
            ],
        }
    )
    provider = next(
        item
        for item in PROVIDER_REGISTRY
        if item.provider_id == "MARKET_FACT_REPOSITORY"
    )
    capabilities = {
        item.dataset_id: item for item in provider.capabilities
    }
    targets = tuple(
        SimpleNamespace(
            dataset_id=dataset_id,
            metric_id=capabilities[dataset_id].metric_id,
        )
        for dataset_id in ("treasury_rates", "cpi")
    )

    result = await LocalCapabilityProbeHook(settings).audit_probe(
        SimpleNamespace(
            provider_id=provider.provider_id,
            registration=provider,
            targets=targets,
            run_id="offline-repository-test",
        )
    )

    assert result["lookup_scope"] == (
        "DATASET_SPECIFIC_CANONICAL_OBSERVATION"
    )
    lookups = {
        item["dataset_id"]: item
        for item in result["dataset_lookups"]
    }
    treasury = lookups["treasury_rates"]
    cpi = lookups["cpi"]
    assert treasury["database_lookup_performed"] is True
    assert treasury["database_record_found"] is True
    assert treasury["database_record_count"] == 1
    assert treasury["records"][0]["fact_key"] == (
        "US:DGS2:latest:official_macro_latest"
    )
    assert treasury["records"][0]["data_as_of"] == (
        current.date().isoformat()
    )
    assert treasury["queried_series_ids"] == [
        "DGS2",
        "DGS10",
        "DGS30",
        "T10Y2Y",
        "T10Y3M",
        "NFCI",
    ]
    assert cpi["database_lookup_performed"] is True
    assert cpi["database_record_found"] is False
    assert cpi["database_record_count"] == 0
    assert cpi["records"] == []
    assert cpi["reason_code"] == (
        "CANONICAL_DATASET_RECORD_NOT_FOUND"
    )


def _local_probe_request(provider: object) -> SimpleNamespace:
    return SimpleNamespace(
        provider_id=provider.provider_id,
        registration=provider,
        run_id="offline-positive-local-probe",
        targets=tuple(
            SimpleNamespace(
                provider_id=provider.provider_id,
                dataset_id=capability.dataset_id,
                metric_id=capability.metric_id,
                fields=capability.supported_fields,
                frequency=capability.frequency,
                transformation=capability.transformation,
            )
            for capability in provider.capabilities
        ),
    )


@pytest.mark.asyncio
async def test_local_transform_probes_emit_every_registered_field_from_real_methods(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "positive-local-probes.sqlite",
        environment="test",
    )
    provider_ids = {
        "PROVIDER_CACHE_REPOSITORY",
        "MARKET_CONTEXT_SNAPSHOT_REPOSITORY",
        "MACRO_CONSENSUS_RECONCILIATION",
        "PROVIDER_FORCE_ACTUAL_RECONCILIATION",
        "REQUEST_PROVIDER_ACCOUNTING",
        "SENIOR_ANALYST_PROJECTION",
    }

    for provider in (
        item
        for item in PROVIDER_REGISTRY
        if item.provider_id in provider_ids
    ):
        request = _local_probe_request(provider)
        result = await LocalCapabilityProbeHook(settings).audit_probe(
            request
        )
        assert result["status"] == "LOCAL_SANDBOX_FIXTURE_EXECUTED"
        assert result["normalization"] == (
            "EXACT_REGISTERED_FIELDS_COPIED_FROM_REAL_ADAPTER_OUTPUT"
        )
        rows = {
            row["metric_id"]: row
            for row in result["capability_results"]
        }
        assert set(rows) == {
            capability.metric_id
            for capability in provider.capabilities
        }
        for capability in provider.capabilities:
            row = rows[capability.metric_id]
            assert row["dataset_id"] == capability.dataset_id
            assert all(
                field_name in row
                and row[field_name] not in (None, "", [], {})
                for field_name in capability.supported_fields
            )
            assert set(row["field_lineage"]) == set(
                capability.supported_fields
            )

        if provider.provider_id == "REQUEST_PROVIDER_ACCOUNTING":
            accounting = rows[
                "request_scoped_provider_accounting"
            ]
            assert accounting["database_lookup"][
                "database_lookup_performed"
            ] is True
            assert accounting["provider_attempts"][0]["called"] is False
            assert accounting["selected_source"] == (
                "PROVIDER_CACHE_REPOSITORY"
            )
        if provider.provider_id == "PROVIDER_FORCE_ACTUAL_RECONCILIATION":
            assert rows["flash_services_pmi"]["actual"] == 53.6
        if provider.provider_id == "SENIOR_ANALYST_PROJECTION":
            projection = rows["senior_analyst_projection"]
            assert projection["analytics"]["vix"]["VIX"]["value"] == 18.5
            assert projection["provider_accounting"]


@pytest.mark.asyncio
async def test_local_projection_probe_fails_closed_when_one_field_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.provider_capability_probe_hooks as hooks

    provider = next(
        item
        for item in PROVIDER_REGISTRY
        if item.provider_id == "SENIOR_ANALYST_PROJECTION"
    )

    def incomplete_projection(*_args: object, **_kwargs: object) -> dict:
        return {
            "analytics": {"vix": {"status": "AVAILABLE"}},
            "readiness": {"status": "PARTIAL"},
            "missing_data": [{"reason_code": "CONTROLLED_GAP"}],
        }

    monkeypatch.setattr(hooks, "_load_symbol", lambda _path: incomplete_projection)
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "missing-local-field.sqlite",
        environment="test",
    )

    with pytest.raises(ProbeExecutionError) as raised:
        await LocalCapabilityProbeHook(settings).audit_probe(
            _local_probe_request(provider)
        )

    assert raised.value.status is HealthStatus.UNUSABLE
    assert raised.value.reason_code == (
        "LOCAL_CAPABILITY_FIELD_NOT_OBSERVED"
    )


def test_runtime_projection_consumes_the_registry_policy() -> None:
    from app.services.senior_analyst_projection_v1 import DATASET_POLICIES

    assert [
        (
            policy.dataset_id,
            policy.section,
            int(policy.max_age.total_seconds()),
            policy.primary_provider,
            policy.fallback_providers,
            policy.provider_strategy,
        )
        for policy in DATASET_POLICIES
    ] == [
        (
            policy.dataset_id,
            policy.section,
            policy.sla_seconds,
            policy.primary_provider,
            policy.fallback_providers,
            policy.provider_strategy,
        )
        for policy in DATASET_SOURCE_POLICIES
    ]


def test_generated_json_and_markdown_are_byte_for_byte_current() -> None:
    from scripts.generate_provider_capability_matrix import (
        _expected_documents,
    )

    _expected_documents()
    assert JSON_MATRIX.read_bytes() == render_matrix_json().encode("utf-8")
    assert MARKDOWN_MATRIX.read_bytes() == render_matrix_markdown().encode(
        "utf-8"
    )


def test_last_live_baseline_requires_external_exact_byte_pin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline_path = tmp_path / "provider-capability-last-live.json"
    monkeypatch.setattr(
        registry,
        "LAST_LIVE_BASELINE_PATH",
        baseline_path,
    )
    monkeypatch.setattr(
        registry,
        "LAST_LIVE_BASELINE_FILE_SHA256",
        None,
    )

    assert registry._last_live_baseline_bytes() == (None, None)  # noqa: SLF001
    assert not any(
        error.startswith("last_live_baseline_")
        for error in validate_registry(raise_on_error=False)
    )

    baseline_path.write_bytes(b"{}\n")
    assert registry._last_live_baseline_bytes() == (  # noqa: SLF001
        None,
        "last_live_baseline_unpinned",
    )

    monkeypatch.setattr(
        registry,
        "LAST_LIVE_BASELINE_FILE_SHA256",
        "not-a-sha256",
    )
    assert registry._last_live_baseline_bytes() == (  # noqa: SLF001
        None,
        "last_live_baseline_pin_invalid",
    )

    monkeypatch.setattr(
        registry,
        "LAST_LIVE_BASELINE_FILE_SHA256",
        "0" * 64,
    )
    assert registry._last_live_baseline_bytes() == (  # noqa: SLF001
        None,
        "last_live_baseline_sha256_mismatch",
    )

    exact_sha256 = hashlib.sha256(baseline_path.read_bytes()).hexdigest()
    monkeypatch.setattr(
        registry,
        "LAST_LIVE_BASELINE_FILE_SHA256",
        exact_sha256,
    )
    assert registry._last_live_baseline_bytes() == (  # noqa: SLF001
        b"{}\n",
        None,
    )

    baseline_path.unlink()
    assert registry._last_live_baseline_bytes() == (  # noqa: SLF001
        None,
        "last_live_baseline_pinned_file_missing",
    )
    assert (
        "last_live_baseline_pinned_file_missing"
        in validate_registry(raise_on_error=False)
    )


def test_matrix_renders_last_live_health_per_field_without_aggregation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.provider_capability_audit import (
        FALLBACK_ROLES,
        PRIMARY_ROLES,
        HealthStatus,
        _health_severity,
        recommendations_for_result,
        stable_sha256,
    )

    target_identity = (
        "CANONICAL_EVENT_REPOSITORY",
        "macro_calendar",
        "occurrence_fields",
    )

    def field_result(
        provider: object,
        *,
        health_status: str = "HEALTHY",
        quality_score: int = 100,
        reason_codes: list[str] | None = None,
    ) -> dict[str, object]:
        health = HealthStatus(health_status)
        roles = {
            role.upper()
            for role in getattr(provider, "allowed_roles")
        }
        eligible_as_primary = (
            health is HealthStatus.HEALTHY
            and (not roles or bool(roles & PRIMARY_ROLES))
        )
        eligible_as_fallback = (
            health in {HealthStatus.HEALTHY, HealthStatus.DEGRADED}
            and (not roles or bool(roles & FALLBACK_ROLES))
        )
        recommendations = recommendations_for_result(
            health,
            roles=roles,
            eligible_as_primary=eligible_as_primary,
            eligible_as_fallback=eligible_as_fallback,
        )
        return {
            "health_status": health_status,
            "quality_score": quality_score,
            "eligible_as_primary": eligible_as_primary,
            "eligible_as_fallback": eligible_as_fallback,
            "recommended_role": recommendations[0],
            "recommendations": recommendations,
            "reason_codes": sorted(reason_codes or []),
            "checked_at": "2026-07-31T12:00:00+00:00",
        }

    capabilities: list[dict[str, object]] = []
    for provider in PROVIDER_REGISTRY:
        for capability in provider.capabilities:
            identity = (
                provider.provider_id,
                capability.dataset_id,
                capability.metric_id,
            )
            terminal_reason = str(
                provider.terminal_audit_reason or ""
            ).strip()
            fields = tuple(
                dict.fromkeys(
                    (
                        *capability.supported_fields,
                        *capability.audit_only_fields,
                    )
                )
            )
            field_results = {
                field_name: field_result(
                    provider,
                    health_status=(
                        "UNUSABLE" if terminal_reason else "HEALTHY"
                    ),
                    quality_score=0 if terminal_reason else 100,
                    reason_codes=(
                        [terminal_reason] if terminal_reason else []
                    ),
                )
                for field_name in fields
            }
            if identity == target_identity:
                field_results["previous"] = field_result(
                    provider,
                    health_status="UNUSABLE",
                    quality_score=20,
                    reason_codes=["SOURCE_GAP"],
                )
            aggregate_health = max(
                (
                    HealthStatus(str(result["health_status"]))
                    for result in field_results.values()
                ),
                key=_health_severity,
            )
            aggregate_primary = all(
                result["eligible_as_primary"] is True
                for result in field_results.values()
            )
            aggregate_fallback = all(
                result["eligible_as_fallback"] is True
                for result in field_results.values()
            )
            roles = {role.upper() for role in provider.allowed_roles}
            capability_row = {
                    "capability_id": "|".join(identity),
                    "provider_id": provider.provider_id,
                    "dataset_id": capability.dataset_id,
                    "metric_id": capability.metric_id,
                    "health_status": aggregate_health.value,
                    "eligible_as_primary": aggregate_primary,
                    "eligible_as_fallback": aggregate_fallback,
                    "recommendations": recommendations_for_result(
                        aggregate_health,
                        roles=roles,
                        eligible_as_primary=aggregate_primary,
                        eligible_as_fallback=aggregate_fallback,
                    ),
                    "reason_codes": sorted(
                        {
                            reason
                            for result in field_results.values()
                            for reason in result["reason_codes"]
                        }
                    ),
                "field_results": field_results,
            }
            if terminal_reason:
                capability_row["terminal_attestation"] = {
                    "attempts": 0,
                    "real_adapter_invoked": False,
                    "probe_dispatch_status": "TERMINAL_UNSUPPORTED",
                    "reason_code": terminal_reason,
                }
            capabilities.append(capability_row)

    report_bytes = b'{"audit_status":"COMPLETED"}\n'
    baseline = {
        "contract": "ProviderCapabilityLastLiveBaseline",
        "schema_version": "1.2",
        "source": {
            "pointer_file": "provider-capability-audit-latest.json",
            "audit_report_sha256": hashlib.sha256(
                report_bytes
            ).hexdigest(),
            "audit_report_size_bytes": len(report_bytes),
            "extracted_capabilities_sha256": stable_sha256(
                capabilities
            ),
        },
        "run_id": "20260731T120000Z",
        "audit_status": "COMPLETED",
        "system_health": "DEGRADED",
        "registry_sha256": stable_sha256(
            [asdict(provider) for provider in PROVIDER_REGISTRY]
        ),
        "providers_tested": len(PROVIDER_REGISTRY),
        "capabilities_tested": len(capabilities),
        "capabilities": capabilities,
    }

    def attest(value: dict[str, object]) -> dict[str, object]:
        unsigned = dict(value)
        unsigned.pop("attestation", None)
        field_rows = sum(
            len(capability["field_results"])
            for capability in unsigned["capabilities"]
        )
        return {
            **unsigned,
            "attestation": {
                "algorithm": "SHA-256",
                "content_sha256": stable_sha256(unsigned),
                "capability_rows": len(unsigned["capabilities"]),
                "field_rows": field_rows,
            },
        }

    baseline = attest(baseline)
    baseline_path = tmp_path / "provider-capability-last-live.json"

    def write_and_pin(
        path: Path,
        value: dict[str, object],
    ) -> None:
        path.write_text(
            json.dumps(value, sort_keys=True),
            encoding="utf-8",
        )
        monkeypatch.setattr(
            registry,
            "LAST_LIVE_BASELINE_FILE_SHA256",
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )

    write_and_pin(baseline_path, baseline)
    monkeypatch.setattr(
        registry,
        "LAST_LIVE_BASELINE_PATH",
        baseline_path,
    )

    payload = json.loads(render_matrix_json())
    provider = next(
        item
        for item in payload["providers"]
        if item["provider_id"] == "CANONICAL_EVENT_REPOSITORY"
    )
    capability = next(
        item
        for item in provider["capabilities"]
        if item["metric_id"] == "occurrence_fields"
    )
    live_by_field = {
        item["field_name"]: item
        for item in capability["field_capabilities"]
    }

    assert live_by_field["consensus"]["health_status"] == "HEALTHY"
    assert (
        live_by_field["consensus"]["recommended_role"]
        == "KEEP_PRIMARY"
    )
    assert live_by_field["previous"]["health_status"] == "UNUSABLE"
    assert (
        live_by_field["previous"]["recommended_role"]
        == "DISABLE_FOR_METRIC"
    )
    markdown = render_matrix_markdown()
    assert (
        "| CANONICAL_EVENT_REPOSITORY | macro_calendar | "
        "occurrence_fields | "
        "consensus |"
    ) in markdown
    assert "RUN_20260731T120000Z: HEALTHY" in markdown
    assert "RUN_20260731T120000Z: UNUSABLE" in markdown

    provenance_tampered = json.loads(json.dumps(baseline))
    provenance_tampered["source"][
        "extracted_capabilities_sha256"
    ] = "0" * 64
    provenance_path = tmp_path / "provenance-tampered.json"
    write_and_pin(provenance_path, attest(provenance_tampered))
    monkeypatch.setattr(
        registry,
        "LAST_LIVE_BASELINE_PATH",
        provenance_path,
    )
    assert (
        json.loads(render_matrix_json())["last_live_capability_audit"][
            "available"
        ]
        is False
    )
    monkeypatch.setattr(
        registry,
        "LAST_LIVE_BASELINE_PATH",
        baseline_path,
    )
    write_and_pin(baseline_path, baseline)

    forged_baseline = json.loads(json.dumps(baseline))
    forged_baseline["run_id"] = "FORGED_NO_LIVE"
    forged_baseline["source"]["audit_report_sha256"] = "0" * 64
    forged_baseline = attest(forged_baseline)
    pinned_live_sha256 = registry.LAST_LIVE_BASELINE_FILE_SHA256
    baseline_path.write_text(
        json.dumps(forged_baseline, sort_keys=True),
        encoding="utf-8",
    )
    assert (
        registry.LAST_LIVE_BASELINE_FILE_SHA256
        == pinned_live_sha256
    )
    assert (
        json.loads(render_matrix_json())["last_live_capability_audit"][
            "available"
        ]
        is False
    )
    write_and_pin(baseline_path, baseline)

    audit_only_tampered = json.loads(json.dumps(baseline))
    audit_only_row = next(
        item
        for item in audit_only_tampered["capabilities"]
        if item["provider_id"] == "AI_RESEARCHER"
    )
    first_ai_field = next(iter(audit_only_row["field_results"].values()))
    first_ai_field["eligible_as_primary"] = True
    audit_only_row["eligible_as_primary"] = True
    audit_only_tampered = attest(audit_only_tampered)
    write_and_pin(baseline_path, audit_only_tampered)
    assert (
        json.loads(render_matrix_json())["last_live_capability_audit"][
            "available"
        ]
        is False
    )
    write_and_pin(baseline_path, baseline)

    achievable_healthy_baseline = json.loads(json.dumps(baseline))
    achievable_healthy_target = next(
        capability
        for capability in achievable_healthy_baseline["capabilities"]
        if (
            capability["provider_id"],
            capability["dataset_id"],
            capability["metric_id"],
        )
        == target_identity
    )
    achievable_healthy_target["field_results"]["consensus"][
        "quality_score"
    ] = 90
    achievable_healthy_baseline = attest(achievable_healthy_baseline)
    write_and_pin(baseline_path, achievable_healthy_baseline)
    assert (
        json.loads(render_matrix_json())["last_live_capability_audit"][
            "available"
        ]
        is False
    )

    for impossible_healthy_score in (85, 95):
        impossible_healthy_baseline = json.loads(json.dumps(baseline))
        impossible_healthy_target = next(
            capability
            for capability in impossible_healthy_baseline["capabilities"]
            if (
                capability["provider_id"],
                capability["dataset_id"],
                capability["metric_id"],
            )
            == target_identity
        )
        impossible_healthy_target["field_results"]["consensus"][
            "quality_score"
        ] = impossible_healthy_score
        impossible_healthy_baseline = attest(impossible_healthy_baseline)
        write_and_pin(baseline_path, impossible_healthy_baseline)
        assert (
            json.loads(render_matrix_json())["last_live_capability_audit"][
                "available"
            ]
            is False
        )

    score_tampered_baseline = json.loads(json.dumps(baseline))
    score_tampered_target = next(
        capability
        for capability in score_tampered_baseline["capabilities"]
        if (
            capability["provider_id"],
            capability["dataset_id"],
            capability["metric_id"],
        )
        == target_identity
    )
    score_tampered_target["field_results"]["consensus"][
        "quality_score"
    ] = 0
    score_tampered_baseline = attest(score_tampered_baseline)
    write_and_pin(baseline_path, score_tampered_baseline)
    score_tampered_payload = json.loads(render_matrix_json())
    assert (
        score_tampered_payload["last_live_capability_audit"]["available"]
        is False
    )

    health_tampered_baseline = json.loads(json.dumps(baseline))
    health_tampered_baseline["system_health"] = "HEALTHY"
    health_tampered_baseline = attest(health_tampered_baseline)
    write_and_pin(baseline_path, health_tampered_baseline)
    health_tampered_payload = json.loads(render_matrix_json())
    assert (
        health_tampered_payload["last_live_capability_audit"]["available"]
        is False
    )

    tampered_baseline = json.loads(json.dumps(baseline))
    tampered_target = next(
        capability
        for capability in tampered_baseline["capabilities"]
        if (
            capability["provider_id"],
            capability["dataset_id"],
            capability["metric_id"],
        )
        == target_identity
    )
    tampered_target["field_results"]["consensus"]["health_status"] = (
        "UNUSABLE"
    )
    tampered_baseline = attest(tampered_baseline)
    write_and_pin(baseline_path, tampered_baseline)
    tampered_payload = json.loads(render_matrix_json())
    assert tampered_payload["last_live_capability_audit"]["available"] is False

    stale_baseline = dict(baseline, registry_sha256="0" * 64)
    write_and_pin(baseline_path, stale_baseline)
    stale_payload = json.loads(render_matrix_json())
    assert stale_payload["last_live_capability_audit"]["available"] is False
    stale_provider = next(
        item
        for item in stale_payload["providers"]
        if item["provider_id"] == "AI_RESEARCHER"
    )
    stale_capability = next(
        item
        for item in stale_provider["capabilities"]
        if item["metric_id"] == "headline_pce_yoy"
    )
    stale_live_by_field = {
        item["field_name"]: item
        for item in stale_capability["field_capabilities"]
    }
    assert (
        stale_live_by_field["consensus"]["last_live_result"]
        == "NO_VERIFIED_LIVE_BASELINE"
    )
    assert (
        stale_live_by_field["consensus"]["health_status"]
        == "NOT_AUDITED"
    )

    incomplete_capabilities = [
        dict(capability)
        for capability in capabilities
    ]
    target_index = next(
        index
        for index, capability in enumerate(incomplete_capabilities)
        if (
            capability["provider_id"],
            capability["dataset_id"],
            capability["metric_id"],
        )
        == target_identity
    )
    incomplete_capabilities[target_index] = {
        **incomplete_capabilities[target_index],
        "field_results": {
            "consensus": field_result(
                next(
                    provider
                    for provider in PROVIDER_REGISTRY
                    if provider.provider_id == "AI_RESEARCHER"
                )
            ),
        },
    }
    incomplete_baseline = dict(
        baseline,
        capabilities=incomplete_capabilities,
    )
    write_and_pin(baseline_path, incomplete_baseline)
    incomplete_payload = json.loads(render_matrix_json())
    assert (
        incomplete_payload["last_live_capability_audit"]["available"]
        is False
    )


def test_unimportable_registered_adapter_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken = replace(
        PROVIDER_REGISTRY[0],
        adapter_path="app.providers.does_not_exist:MissingProvider",
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        (broken, *PROVIDER_REGISTRY[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert any(
        error.startswith("provider_adapter_not_importable:")
        for error in errors
    )


def test_provider_and_capability_without_probe_are_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = PROVIDER_REGISTRY[0]
    capability = replace(original.capabilities[0], probe_id="")
    broken = replace(
        original,
        probe_id="",
        capabilities=(capability, *original.capabilities[1:]),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        (broken, *PROVIDER_REGISTRY[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert f"provider_without_probe:{original.provider_id}" in errors
    assert any(
        error.startswith(
            f"capability_without_probe:{original.provider_id}:"
        )
        for error in errors
    )


def test_unregistered_fallback_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = DATASET_SOURCE_POLICIES[0]
    broken = replace(
        original,
        fallback_providers=(
            *original.fallback_providers,
            "UNREGISTERED_PROVIDER",
        ),
    )
    monkeypatch.setattr(
        registry,
        "DATASET_SOURCE_POLICIES",
        (broken, *DATASET_SOURCE_POLICIES[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        "dataset_policy_unregistered_fallback:"
        f"{original.dataset_id}:UNREGISTERED_PROVIDER"
    ) in errors


def test_ai_fallback_without_capability_certification_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = next(
        item
        for item in DATASET_SOURCE_POLICIES
        if item.dataset_id == "macro_calendar"
    )
    broken = replace(
        policy,
        ai_fallback_providers=("AI_RESEARCHER",),
    )
    monkeypatch.setattr(
        registry,
        "DATASET_SOURCE_POLICIES",
        tuple(
            broken if item is policy else item
            for item in DATASET_SOURCE_POLICIES
        ),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        "dataset_policy_ai_capability_not_certifiable:"
        "macro_calendar:AI_RESEARCHER"
    ) in errors


def test_every_declared_field_has_a_probe_and_validator() -> None:
    for provider in PROVIDER_REGISTRY:
        for capability in provider.capabilities:
            assert capability.supported_fields
            assert "*" not in capability.supported_fields
            assert capability.probe_id
            assert capability.field_validator_id.startswith("validate.")
            assert capability.field_validator_id in FIELD_VALIDATOR_SCHEMAS
            assert set(capability.supported_fields) <= FIELD_VALIDATOR_SCHEMAS[
                capability.field_validator_id
            ]


def test_unregistered_runtime_provider_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovered = discover_runtime_adapter_paths()
    unexpected = "app.providers.new_source:UnregisteredProvider"
    monkeypatch.setattr(
        registry,
        "discover_runtime_adapter_paths",
        lambda: (*discovered, unexpected),
    )

    errors = validate_registry(raise_on_error=False)

    assert f"unregistered_runtime_provider:{unexpected}" in errors


def test_adapter_field_outside_validator_schema_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = PROVIDER_REGISTRY[0]
    capability = replace(
        original.capabilities[0],
        supported_fields=(
            *original.capabilities[0].supported_fields,
            "impossible_adapter_field",
        ),
    )
    broken = replace(
        original,
        capabilities=(capability, *original.capabilities[1:]),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        (broken, *PROVIDER_REGISTRY[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        "capability_fields_not_validated:"
        f"{original.provider_id}:{capability.dataset_id}:"
        f"{capability.metric_id}:impossible_adapter_field"
    ) in errors


def test_request_group_represents_one_real_shared_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    grouped: dict[tuple[str, str, str], list[str]] = {}
    for provider in PROVIDER_REGISTRY:
        for capability in provider.capabilities:
            request_group = capability.request_group or provider.request_group
            if request_group:
                grouped.setdefault(
                    (
                        provider.provider_id,
                        capability.probe_id,
                        request_group,
                    ),
                    [],
                ).append(capability.dataset_id)

    assert len(grouped) == 13
    assert grouped[
        (
            "AI_RESEARCHER",
            "probe.ai.researcher.macro_calendar",
            "ai_researcher.concrete_capabilities.batch",
        )
    ] == [
        capability.dataset_id
        for capability in provider_by_id("AI_RESEARCHER").capabilities
        if capability.probe_id
        == "probe.ai.researcher.macro_calendar"
    ]
    assert grouped[
        ("BLS", "probe.bls.series", "bls.series.batch")
    ] == [
        "cpi",
        "cpi",
        "cpi",
        "cpi",
        "ppi",
        "ppi",
        "nfp",
        "employment",
        "wages",
        "wages",
    ]
    assert grouped[
        (
            "CBOE",
            "probe.cboe.risk_indices",
            "cboe.risk_indices.batch",
        )
    ] == ["vix", "vvix", "risk", "risk", "risk"]
    assert grouped[
        (
            "RISK_CONTEXT_REPOSITORY",
            "probe.repository.risk_context",
            "repository.risk_context.latest",
        )
    ] == ["risk", "vvix"]
    assert grouped[
        (
            "CANONICAL_EVENT_REPOSITORY",
            "probe.repository.event_values",
            "repository.event_values.occurrence_fields",
        )
    ] == ["macro_calendar", "flash_services_pmi"]
    assert grouped[
        (
            "MARKET_FACT_REPOSITORY",
            "probe.repository.market_facts",
            "repository.market_facts.dataset_lookups",
        )
    ] == list(MARKET_FACT_REPOSITORY_DATASET_QUERIES)

    original = PROVIDER_REGISTRY[0]
    singleton = replace(
        original.capabilities[0],
        request_group="not-a-shared-request",
    )
    broken = replace(original, capabilities=(singleton,))
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        (broken, *PROVIDER_REGISTRY[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        "request_group_without_shared_acquisition:"
        f"{original.provider_id}:{singleton.probe_id}:"
        "not-a-shared-request"
    ) in errors


def test_official_source_series_drift_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = next(
        item for item in PROVIDER_REGISTRY if item.provider_id == "BLS"
    )
    capability = next(
        item
        for item in provider.capabilities
        if item.metric_id == "CUSR0000SA0"
    )
    broken_capability = replace(
        capability,
        metric_id="CUSR0000SA0_WRONG",
    )
    broken_provider = replace(
        provider,
        capabilities=tuple(
            broken_capability if item is capability else item
            for item in provider.capabilities
        ),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        tuple(
            broken_provider if item is provider else item
            for item in PROVIDER_REGISTRY
        ),
    )

    errors = validate_registry(raise_on_error=False)

    assert any(
        error.startswith(
            "official_source_relation_drift:headline_cpi_mom:"
        )
        for error in errors
    )


def test_official_transformation_drift_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = next(
        item
        for item in PROVIDER_REGISTRY
        if item.provider_id == "OFFICIAL_ACTUAL_TRANSFORMATION"
    )
    capability = next(
        item
        for item in provider.capabilities
        if item.metric_id == "headline_cpi_mom"
    )
    broken_capability = replace(capability, transformation="level")
    broken_provider = replace(
        provider,
        capabilities=tuple(
            broken_capability if item is capability else item
            for item in provider.capabilities
        ),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        tuple(
            broken_provider if item is provider else item
            for item in PROVIDER_REGISTRY
        ),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        "official_transformation_capability_drift:"
        "headline_cpi_mom:cpi|monthly|level"
    ) in errors


def test_ai_canonical_metric_field_drift_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = next(
        item
        for item in PROVIDER_REGISTRY
        if item.provider_id == "AI_RESEARCHER"
    )
    capability = next(
        item
        for item in provider.capabilities
        if item.metric_id == "headline_pce_yoy"
    )
    broken_capability = replace(
        capability,
        supported_fields=tuple(
            field_name
            for field_name in capability.supported_fields
            if field_name != "previous"
        ),
    )
    broken_provider = replace(
        provider,
        capabilities=tuple(
            broken_capability if item is capability else item
            for item in provider.capabilities
        ),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        tuple(
            broken_provider if item is provider else item
            for item in PROVIDER_REGISTRY
        ),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        "ai_canonical_capability_fields_incomplete:headline_pce_yoy"
        in errors
    )
    assert (
        "ai_canonical_field_capability_missing:"
        "headline_pce_yoy:previous"
    ) in errors
