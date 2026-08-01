from __future__ import annotations

import ast
import gc
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest

import app.bootstrap.application as application
import app.services.ai_researcher_service as ai_researcher_module
import app.services.multi_source_runtime_service as runtime_module
import app.services.positioning_runtime_service as positioning_module
import app.services.provider_adapter_factory as factory_module
import app.services.provider_capability_registry as registry_module
import app.services.risk_context_runtime_service as risk_module
import app.services.social_sentiment_service as social_module
from app.core.config import Settings
from app.services.provider_adapter_factory import (
    RegisteredProviderAdapterError,
    adapter_path_for,
    create_registered_adapter,
    registered_adapter_paths,
    resolve_registered_adapter,
)
from app.services.provider_capability_registry import dataset_policy_by_id
from app.services.provider_capability_registry import provider_by_id


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_PROVIDER_ORDER = [
    "FRED",
    "BLS",
    "BEA",
    "CENSUS",
    "SPGLOBAL",
    "INVESTING_EVENT_1062",
    "FINNHUB",
    "TRADIER",
    "DAILYFX",
    "FOREX_FACTORY",
    "INVESTING_ECONOMIC_CALENDAR",
    "FXSTREET",
    "MARKETWATCH_CALENDAR",
    "YAHOO_ECONOMIC_CALENDAR",
    "GENERIC_SEARCH_CALENDAR",
    "DAILYFX",
    "FOREX_FACTORY",
    "INVESTING_ECONOMIC_CALENDAR",
    "TARGETED_SEARCH_EVENT",
    "MANUAL_EVENT_ENRICHMENT",
    "FEDERAL_RESERVE",
    "FEDERAL_RESERVE",
    "BLS",
    "BEA",
    "ECONOMIC_CALENDAR_SCRAPER",
    "INVESCO",
    "YAHOO_FINANCE_CHART",
    "LEGACY_EARNINGS_AGGREGATOR",
    "ALPHA_VANTAGE_NEWS_SENTIMENT",
    "CFTC",
    "CBOE",
    "CBOE",
    "CBOE",
]
MULTI_SOURCE_PROVIDER_ORDER = [
    "INVESTING_ECONOMIC_CALENDAR",
    "XTB",
    "INVESTING_HOLIDAYS",
    "MARKETBEAT",
    "CME",
    "INVESTING_FED_RATE_MONITOR",
    "CBOE",
    "NASDAQ",
    "FMP_EARNINGS_CALENDAR",
    "NASDAQ",
    "NASDAQ_MARKET_INFO",
    "NASDAQ_QQQ_OPTIONS",
    "MACROMICRO",
    "POLYMARKET",
]
SERVICE_PROVIDER_ORDERS = [
    (
        ai_researcher_module,
        ai_researcher_module.AIResearcherService,
        ["AI_RESEARCHER"],
    ),
    (
        positioning_module,
        positioning_module.PositioningRuntimeService,
        ["CFTC", "AAII"],
    ),
    (
        risk_module,
        risk_module.RiskContextRuntimeService,
        ["CBOE", "CBOE", "CBOE", "NASDAQ_QQQ_OPTIONS"],
    ),
    (
        social_module,
        social_module.SocialSentimentService,
        ["HACKER_NEWS"],
    ),
]


class RegistrySelectedAdapter:
    def __init__(self, marker: str) -> None:
        self.marker = marker


@pytest.fixture(autouse=True)
def prohibit_http_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_async_http(*_: Any, **__: Any) -> Any:
        raise AssertionError("registry runtime test attempted an HTTP request")

    def no_sync_http(*_: Any, **__: Any) -> Any:
        raise AssertionError("registry runtime test attempted an HTTP request")

    monkeypatch.setattr(httpx.AsyncClient, "send", no_async_http)
    monkeypatch.setattr(httpx.Client, "send", no_sync_http)


@pytest.mark.parametrize(
    ("provider_id", "adapter_name"),
    [
        ("FRED", None),
        ("BLS", "BlsReleaseCalendarProvider"),
        ("BEA", "BeaReleaseScheduleProvider"),
        ("NASDAQ", "NasdaqEarningsProvider"),
        ("CBOE", "CboePutCallProvider"),
    ],
)
def test_factory_resolves_only_primary_or_additional_registered_paths(
    provider_id: str,
    adapter_name: str | None,
) -> None:
    adapter = resolve_registered_adapter(
        provider_id,
        adapter_name=adapter_name,
    )

    assert adapter_path_for(adapter) in registered_adapter_paths(provider_id)


def test_factory_rejects_unregistered_provider_and_adapter() -> None:
    with pytest.raises(
        RegisteredProviderAdapterError,
        match="UNREGISTERED_RUNTIME_PROVIDER",
    ):
        resolve_registered_adapter("NOT_IN_PROVIDER_REGISTRY")

    with pytest.raises(
        RegisteredProviderAdapterError,
        match="REGISTERED_PROVIDER_ADAPTER_NOT_FOUND",
    ):
        resolve_registered_adapter(
            "FRED",
            adapter_name="UnregisteredFredAdapter",
        )


def test_factory_construction_follows_monkeypatched_registry_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = provider_by_id("FRED")
    patched_registration = replace(
        registration,
        adapter_path=f"{__name__}:RegistrySelectedAdapter",
        additional_adapter_paths=(),
    )
    monkeypatch.setattr(
        factory_module,
        "provider_by_id",
        lambda provider_id: (
            patched_registration
            if provider_id == "FRED"
            else provider_by_id(provider_id)
        ),
    )

    instance = factory_module.create_registered_adapter("FRED", "registry-selected")

    assert isinstance(instance, RegistrySelectedAdapter)
    assert instance.marker == "registry-selected"


def test_bootstrap_constructs_provider_classes_exclusively_from_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, str]] = []
    original_create = create_registered_adapter

    def observed_create(
        provider_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        instance = original_create(provider_id, *args, **kwargs)
        path = adapter_path_for(instance)
        assert path in registered_adapter_paths(provider_id)
        observed.append((provider_id, path))
        return instance

    monkeypatch.setattr(
        application,
        "create_registered_adapter",
        observed_create,
    )
    settings = Settings(
        environment="test",
        database_path=tmp_path / "provider-registry-runtime.sqlite",
    )

    state = application.build_application_state(settings)

    assert [provider_id for provider_id, _ in observed] == (BOOTSTRAP_PROVIDER_ORDER)
    tree = ast.parse((ROOT / "app" / "bootstrap" / "application.py").read_text(encoding="utf-8"))
    imported_provider_classes = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and str(node.module or "").startswith("app.providers")
        for alias in node.names
        if alias.name.endswith("Provider")
    ]
    assert imported_provider_classes == []

    del state
    del settings
    gc.collect()


def test_bootstrap_runtime_order_follows_mutated_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registrations = list(registry_module.PROVIDER_REGISTRY)

    def swap(left: str, right: str) -> None:
        left_index = next(
            index
            for index, provider in enumerate(registrations)
            if provider.provider_id == left
        )
        right_index = next(
            index
            for index, provider in enumerate(registrations)
            if provider.provider_id == right
        )
        registrations[left_index], registrations[right_index] = (
            registrations[right_index],
            registrations[left_index],
        )

    swap("FRED", "BLS")
    swap("DAILYFX", "FOREX_FACTORY")
    monkeypatch.setattr(
        registry_module,
        "PROVIDER_REGISTRY",
        tuple(registrations),
    )
    settings = Settings(
        environment="test",
        database_path=tmp_path / "mutated-provider-order.sqlite",
    )

    state = application.build_application_state(settings)

    assert [
        provider.source
        for provider in state["macro_service"].providers
    ] == ["BLS", "FRED", "BEA"]
    assert [
        type(provider).__name__
        for provider in state["event_enrichment_service"].providers[:2]
    ] == [
        "ForexFactoryEnrichmentProvider",
        "DailyFxEnrichmentProvider",
    ]

    del state
    del settings
    gc.collect()


def test_multi_source_constructs_provider_classes_exclusively_from_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, str]] = []
    original_create = create_registered_adapter

    def observed_create(
        provider_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        instance = original_create(provider_id, *args, **kwargs)
        path = adapter_path_for(instance)
        assert path in registered_adapter_paths(provider_id)
        observed.append((provider_id, path))
        return instance

    monkeypatch.setattr(
        runtime_module,
        "create_registered_adapter",
        observed_create,
    )
    settings = Settings(
        environment="test",
        database_path=tmp_path / "multi-source-provider-registry.sqlite",
    )

    service = runtime_module.MultiSourceRuntimeService(settings)

    assert [provider_id for provider_id, _ in observed] == MULTI_SOURCE_PROVIDER_ORDER

    del service
    del settings
    gc.collect()


@pytest.mark.parametrize(
    ("service_module", "service_class", "expected_provider_order"),
    SERVICE_PROVIDER_ORDERS,
)
def test_runtime_services_construct_providers_exclusively_from_registry(
    service_module: Any,
    service_class: type[Any],
    expected_provider_order: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, str]] = []
    original_create = create_registered_adapter

    def observed_create(
        provider_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        instance = original_create(provider_id, *args, **kwargs)
        path = adapter_path_for(instance)
        assert path in registered_adapter_paths(provider_id)
        observed.append((provider_id, path))
        return instance

    monkeypatch.setattr(
        service_module,
        "create_registered_adapter",
        observed_create,
    )
    settings = Settings(
        environment="test",
        database_path=tmp_path / f"{service_class.__name__}.sqlite",
    )

    service = service_class(settings)

    assert [provider_id for provider_id, _ in observed] == expected_provider_order

    del service
    del settings
    gc.collect()


@pytest.mark.parametrize(
    "relative_path",
    [
        "app/services/multi_source_runtime_service.py",
        "app/services/ai_researcher_service.py",
        "app/services/positioning_runtime_service.py",
        "app/services/risk_context_runtime_service.py",
        "app/services/social_sentiment_service.py",
    ],
)
def test_registry_driven_runtime_services_do_not_import_provider_classes(
    relative_path: str,
) -> None:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    imported_provider_classes = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and str(node.module or "").startswith("app.providers")
        for alias in node.names
        if alias.name.endswith("Provider")
    ]

    assert imported_provider_classes == []


@pytest.mark.asyncio
async def test_earnings_runtime_call_order_follows_mutated_registry_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(
        Settings(
            environment="test",
            database_path=tmp_path / "earnings-policy-runtime.sqlite",
        )
    )
    calls: list[str] = []

    async def failed_provider(
        name: str,
        *_: Any,
        **__: Any,
    ) -> dict[str, Any]:
        calls.append(name)
        return {
            "status": "not_found",
            "fetched_count": 0,
            "materialized_count": 0,
        }

    monkeypatch.setattr(service, "_run_provider", failed_provider)

    await service._earnings_chain(refresh="force")
    default_order = list(calls)

    policy = dataset_policy_by_id("earnings")
    swapped = replace(
        policy,
        primary_provider=policy.fallback_providers[0],
        fallback_providers=(policy.primary_provider,),
    )
    monkeypatch.setattr(
        registry_module,
        "dataset_policy_by_id",
        lambda dataset_id: (
            swapped if dataset_id == "earnings" else dataset_policy_by_id(dataset_id)
        ),
    )
    calls.clear()

    await service._earnings_chain(refresh="force")

    assert default_order == ["nasdaq_earnings", "fmp_earnings"]
    assert calls == ["fmp_earnings", "nasdaq_earnings"]


@pytest.mark.asyncio
async def test_market_schedule_runtime_call_order_follows_mutated_registry_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = runtime_module.MultiSourceRuntimeService(
        Settings(
            environment="test",
            database_path=tmp_path / "schedule-policy-runtime.sqlite",
        )
    )
    calls: list[str] = []

    async def failed_provider(
        name: str,
        *_: Any,
        **__: Any,
    ) -> dict[str, Any]:
        calls.append(name)
        return {
            "status": "not_found",
            "fetched_count": 0,
            "materialized_count": 0,
        }

    monkeypatch.setattr(service, "_run_provider", failed_provider)

    await service._market_schedule_chain(refresh="force")
    default_order = list(calls)

    policy = dataset_policy_by_id("market_schedule")
    promoted = policy.fallback_providers[0]
    reordered = replace(
        policy,
        primary_provider=promoted,
        fallback_providers=(
            policy.primary_provider,
            *policy.fallback_providers[1:],
        ),
    )
    monkeypatch.setattr(
        registry_module,
        "dataset_policy_by_id",
        lambda dataset_id: (
            reordered
            if dataset_id == "market_schedule"
            else dataset_policy_by_id(dataset_id)
        ),
    )
    calls.clear()

    await service._market_schedule_chain(refresh="force")

    assert default_order == [
        "nasdaq_market_info",
        "cme_market_schedule",
        "investing_holidays",
        "marketbeat_holidays",
    ]
    assert calls == [
        "cme_market_schedule",
        "nasdaq_market_info",
        "investing_holidays",
        "marketbeat_holidays",
    ]
