from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers import news_provider as news_module
from app.providers.bea import BEA_SERIES, BeaProvider
from app.providers.fred import FRED_SERIES, FredProvider
from app.services.provider_capability_audit import (
    ProviderCapabilityAuditEngine,
)
from app.services.provider_capability_registry import (
    dataset_policy_by_id,
    provider_by_id,
    provider_default_runtime_metric_ids,
    validate_registry,
)
from scripts import provider_capability_audit as audit_script


NEWS_ENABLE_SETTINGS = {
    "ALPHA_VANTAGE_NEWS_SENTIMENT": "alpha_vantage_api_key",
    "GDELT_DOC_API": "news_gdelt_enabled",
    "FEDERAL_RESERVE_RSS": "news_rss_enabled",
    "BLS_RSS": "news_rss_enabled",
    "BEA_RSS": "news_rss_enabled",
    "YAHOO_FINANCE_RSS": "news_rss_enabled",
    "MARKETWATCH_RSS": "news_rss_enabled",
    "GOOGLE_NEWS_RSS": "news_rss_enabled",
}


def _settings(tmp_path, **updates) -> Settings:
    return Settings(
        database_path=tmp_path / "provider-alignment.sqlite3",
        diagnostics_dir=tmp_path / "diagnostics",
        backups_dir=tmp_path / "backups",
        logs_dir=tmp_path / "logs",
        temp_dir=tmp_path / "temp",
        environment="test",
        **updates,
    )


def test_fred_source_frequency_is_distinct_from_dataset_lifecycle() -> None:
    frequencies = {
        capability.metric_id: capability.frequency
        for capability in provider_by_id("FRED").capabilities
    }

    assert frequencies["DFEDTARL"] == "daily"
    assert frequencies["DFEDTARU"] == "daily"
    assert frequencies["FEDFUNDS"] == "monthly"
    assert frequencies["NFCI"] == "weekly"
    assert dataset_policy_by_id("target_range").frequency == "event"
    assert dataset_policy_by_id("fed_funds").frequency == "daily"
    assert dataset_policy_by_id("treasury_rates").frequency == "daily"


@pytest.mark.asyncio
async def test_fred_no_argument_fetch_uses_registry_series_only(tmp_path) -> None:
    requested: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url.params["series_id"]))
        return httpx.Response(
            200,
            json={
                "observations": [
                    {
                        "date": "2026-07-30",
                        "value": "1.25",
                        "realtime_start": "2026-07-31",
                    }
                ]
            },
        )

    provider = FredProvider(
        ProviderCacheRepository(tmp_path / "fred-cache.sqlite3"),
        _settings(
            tmp_path,
            fred_api_key="offline-test-key",
            fred_enabled=True,
            fred_base_url="https://fred.test/api",
        ),
        transport=httpx.MockTransport(handler),
    )

    result = await provider.fetch()
    expected = provider_default_runtime_metric_ids("FRED")

    assert all("," not in series_id for series_id in expected)
    assert set(expected) <= set(FRED_SERIES)
    assert tuple(requested) == expected
    assert set(result.data) == set(expected)
    assert "WALCL" not in requested


def test_bea_no_argument_default_excludes_unregistered_series() -> None:
    expected = provider_default_runtime_metric_ids("BEA")
    supported = {str(spec["series_id"]) for spec in BEA_SERIES}

    assert BeaProvider.runtime_default_series_ids() == expected
    assert set(expected) <= supported
    assert "BEA:GDP_PRICE_INDEX" not in expected
    assert "BEA:PCE" not in expected


def test_registry_validation_blocks_runtime_default_minus_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered = FredProvider.runtime_default_series_ids()
    monkeypatch.setattr(
        FredProvider,
        "runtime_default_series_ids",
        classmethod(lambda cls: (*registered, "WALCL")),
    )

    errors = validate_registry(raise_on_error=False)

    assert "runtime_default_series_not_registered:FRED:WALCL" in errors


@pytest.mark.parametrize(
    ("expected", "observed"),
    (
        ("event", "daily"),
        ("daily", "monthly"),
        ("weekly", "daily"),
    ),
)
def test_semantic_check_rejects_structured_frequency_mismatch(
    expected: str,
    observed: str,
) -> None:
    target = SimpleNamespace(
        frequency=expected,
        metric_id="SERIES_A",
        transformation="identity",
    )
    owner = {
        "metric_id": "SERIES_A",
        "frequency": observed,
        "description": f"unrelated text says {expected}",
    }

    assert audit_script._semantic_check(target, owner, owner) is False  # noqa: SLF001


def test_semantic_check_accepts_matching_structured_frequency() -> None:
    target = SimpleNamespace(
        frequency="weekly",
        metric_id="NFCI",
        transformation="identity",
    )
    owner = {
        "series_id": "NFCI",
        "frequency": "Weekly",
        "transformation": "identity",
    }

    assert audit_script._semantic_check(target, owner, owner) is True  # noqa: SLF001


@pytest.mark.asyncio
async def test_news_runtime_and_audit_share_the_registry_enable_setting(
    tmp_path,
) -> None:
    runtime_specs = news_module._NEWS_PROVIDER_RUNTIME_SPECS  # noqa: SLF001
    settings = _settings(
        tmp_path,
        alpha_vantage_api_key=None,
        news_gdelt_enabled=False,
        news_rss_enabled=False,
    )
    registrations = tuple(
        provider_by_id(provider_id) for provider_id in NEWS_ENABLE_SETTINGS
    )

    assert {
        provider_id: registration.enable_setting
        for provider_id, registration in zip(
            NEWS_ENABLE_SETTINGS,
            registrations,
            strict=True,
        )
    } == NEWS_ENABLE_SETTINGS
    assert {
        provider_id: runtime_specs[provider_id].enabled_field
        for provider_id in NEWS_ENABLE_SETTINGS
    } == NEWS_ENABLE_SETTINGS

    provider = news_module.NewsProvider(
        ProviderCacheRepository(tmp_path / "news-cache.sqlite3"),
        settings,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(500)
        )
    ) as client:
        tasks = provider._provider_tasks(  # noqa: SLF001
            provider_specs=tuple(runtime_specs.values()),
            client=client,
            symbols=["QQQ"],
            query="QQQ",
            requested_limit=1,
            recency_days=1,
            execution_evidence=object(),
        )
    assert tasks == []

    executor_calls = 0

    async def executor(_request):
        nonlocal executor_calls
        executor_calls += 1
        raise AssertionError("disabled provider must not reach its audit probe")

    execution = await ProviderCapabilityAuditEngine(
        registrations,
        executor,
        settings=settings,
    ).run(sandbox_root=tmp_path / "audit-sandbox")

    assert executor_calls == 0
    assert len(execution.report["results"]) == len(NEWS_ENABLE_SETTINGS)
    assert all(
        result["configured"] is False
        and result["health_status"] == "NOT_CONFIGURED"
        and "PROVIDER_DISABLED_BY_CONFIGURATION"
        in result["observed_reason_codes"]
        for result in execution.report["results"]
    )
