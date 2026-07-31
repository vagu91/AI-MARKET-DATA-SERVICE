from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from app.services import provider_capability_registry as registry
from app.services.provider_capability_contracts import (
    PROBE_OUTPUT_CONTRACTS,
    capability_schema_sha256,
    validate_capability_output_contracts,
)
from app.services.provider_capability_registry import (
    PROVIDER_REGISTRY,
    capability_source_adapter_paths,
    discover_capability_source_adapter_paths,
    registry_summary,
    validate_registry,
)


def test_every_capability_is_bound_to_an_independent_output_contract() -> None:
    assert len(PROBE_OUTPUT_CONTRACTS) == 76
    assert sum(len(provider.capabilities) for provider in PROVIDER_REGISTRY) == 218
    assert validate_capability_output_contracts(PROVIDER_REGISTRY) == ()
    summary = registry_summary()
    assert summary["probe_output_contracts_registered"] == 76
    assert summary["capability_output_contract_coverage"] == 100
    assert summary["source_adapters_registered"] == 70
    assert summary["source_adapters_discovered"] == 70

    for provider in PROVIDER_REGISTRY:
        grouped: dict[str, list[object]] = {}
        for capability in provider.capabilities:
            grouped.setdefault(capability.probe_id, []).append(capability)
        for probe_id, capabilities in grouped.items():
            contract = PROBE_OUTPUT_CONTRACTS[probe_id]
            assert contract.provider_id == provider.provider_id
            assert contract.capability_schema_sha256 == (capability_schema_sha256(capabilities))


def test_adapter_field_not_in_output_contract_is_blocking_even_if_validator_accepts_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = PROVIDER_REGISTRY[0]
    original = provider.capabilities[0]
    field_name = "adapter_never_emits"
    broken_capability = replace(
        original,
        supported_fields=(*original.supported_fields, field_name),
    )
    broken_provider = replace(
        provider,
        capabilities=(
            broken_capability,
            *provider.capabilities[1:],
        ),
    )
    schemas = dict(registry.FIELD_VALIDATOR_SCHEMAS)
    schemas[original.field_validator_id] = frozenset(
        {*schemas[original.field_validator_id], field_name}
    )
    monkeypatch.setattr(
        registry,
        "FIELD_VALIDATOR_SCHEMAS",
        MappingProxyType(schemas),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        (broken_provider, *PROVIDER_REGISTRY[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert not any(
        error.endswith(field_name) and error.startswith("capability_fields_not_validated:")
        for error in errors
    )
    assert (
        f"capability_output_contract_fields_mismatch:{provider.provider_id}:{original.probe_id}"
    ) in errors
    assert (
        f"capability_output_contract_schema_mismatch:{provider.provider_id}:{original.probe_id}"
    ) in errors


def test_new_capability_assignment_requires_contract_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = PROVIDER_REGISTRY[0]
    original = provider.capabilities[0]
    added = replace(original, metric_id=f"{original.metric_id}_new")
    broken_provider = replace(
        provider,
        capabilities=(*provider.capabilities, added),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        (broken_provider, *PROVIDER_REGISTRY[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        f"capability_output_contract_schema_mismatch:{provider.provider_id}:{original.probe_id}"
    ) in errors


def test_new_probe_without_output_contract_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = PROVIDER_REGISTRY[0]
    original = provider.capabilities[0]
    added = replace(
        original,
        metric_id=f"{original.metric_id}_new_probe",
        probe_id="probe.uncontracted.source",
    )
    broken_provider = replace(
        provider,
        capabilities=(*provider.capabilities, added),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        (broken_provider, *PROVIDER_REGISTRY[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        f"capability_output_contract_missing:{provider.provider_id}:probe.uncontracted.source"
    ) in errors


def test_probe_adapter_path_drift_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = PROVIDER_REGISTRY[0]
    original = provider.capabilities[0]
    broken_capability = replace(
        original,
        probe_adapter_path=("app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider"),
    )
    broken_provider = replace(
        provider,
        capabilities=(
            broken_capability,
            *provider.capabilities[1:],
        ),
    )
    monkeypatch.setattr(
        registry,
        "PROVIDER_REGISTRY",
        (broken_provider, *PROVIDER_REGISTRY[1:]),
    )

    errors = validate_registry(raise_on_error=False)

    assert (
        "capability_output_contract_probe_adapter_mismatch:"
        f"{provider.provider_id}:{original.probe_id}"
    ) in errors
    assert f"provider_source_adapter_contract_mismatch:{provider.provider_id}" in errors


def test_source_discovery_covers_provider_repository_ai_and_service_sources() -> None:
    registered = set(capability_source_adapter_paths())
    discovered = set(discover_capability_source_adapter_paths())

    assert len(registered) == 70
    assert discovered == registered
    assert "app.providers.fred:FredProvider" in discovered
    assert "app.services.market_fact_repository:MarketFactRepository" in discovered
    assert "app.services.research_backend:OpenAIResponsesResearchBackend" in discovered
    assert "app.services.official_actual_semantics:derive_official_actual" in discovered
    assert (
        "app.services.request_provider_accounting:RequestProviderAccountingCollector" in discovered
    )
    assert {
        "app.services.research_source_gateway:ResearchSourceGateway",
        (
            "app.services.evidence_verification_service:"
            "DeterministicEvidenceVerifier"
        ),
        "app.services.agentic_research_runtime:AgenticResearchRuntime",
        "app.services.ai_research_worker:AIResearchWorker",
    } <= discovered


def test_audit_only_fields_have_independent_contract_scope() -> None:
    provider = next(
        item for item in PROVIDER_REGISTRY if item.provider_id == "AI_RESEARCHER"
    )
    capabilities = tuple(
        item
        for item in provider.capabilities
        if item.probe_id == "probe.ai.researcher.macro_calendar"
    )
    contract = PROBE_OUTPUT_CONTRACTS[
        "probe.ai.researcher.macro_calendar"
    ]

    assert contract.audit_only_fields == frozenset(
        {"actual", "previous_revised", "previous_revised_lineage"}
    )
    assert "actual" not in contract.normalized_fields
    assert "previous_revised" not in contract.normalized_fields
    assert contract.terminal_audit_reason is None

    broken = replace(
        capabilities[0],
        audit_only_fields=("actual", "actual"),
    )
    errors = validate_capability_output_contracts(
        (
            replace(
                provider,
                capabilities=(broken, *provider.capabilities[1:]),
            ),
        ),
        contracts={
            key: value
            for key, value in PROBE_OUTPUT_CONTRACTS.items()
            if value.provider_id == provider.provider_id
        },
    )

    assert any(
        item.startswith("capability_audit_only_fields_duplicated:")
        for item in errors
    )


def test_new_discovered_repository_source_without_registration_is_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unexpected = "app.services.new_market_repository:NewMarketRepository"
    discovered = discover_capability_source_adapter_paths()
    monkeypatch.setattr(
        registry,
        "discover_capability_source_adapter_paths",
        lambda: (*discovered, unexpected),
    )

    errors = validate_registry(raise_on_error=False)

    assert f"unregistered_capability_source_adapter:{unexpected}" in errors


def test_orphan_output_contract_is_blocking() -> None:
    template = next(iter(PROBE_OUTPUT_CONTRACTS.values()))
    modified = {
        **PROBE_OUTPUT_CONTRACTS,
        "probe.removed.source": replace(
            template,
            provider_id="REMOVED_SOURCE",
        ),
    }

    errors = validate_capability_output_contracts(
        PROVIDER_REGISTRY,
        contracts=modified,
    )

    assert "orphan_capability_output_contract:probe.removed.source" in errors


def test_contract_map_is_immutable() -> None:
    with pytest.raises(TypeError):
        PROBE_OUTPUT_CONTRACTS["probe.mutable"] = next(  # type: ignore[index]
            iter(PROBE_OUTPUT_CONTRACTS.values())
        )
