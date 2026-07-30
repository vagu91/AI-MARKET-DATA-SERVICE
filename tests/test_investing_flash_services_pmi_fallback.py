from __future__ import annotations

import json
from pathlib import Path

import httpx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.base import ProviderError
from app.providers.investing_flash_services_pmi import (
    INVESTING_BROWSER_USER_AGENT,
    SOURCE,
    InvestingFlashServicesPmiProvider,
)
from app.providers.sp_global_pmi import SERIES_ID
from app.services.deterministic_actual_resolver import DeterministicActualResolver


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "investing_flash_services_pmi_1062.json"
)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "market.sqlite",
        investing_flash_services_pmi_enabled=True,
    )


async def test_investing_fallback_preserves_exact_occurrence_and_field_lineage(
    tmp_path: Path,
) -> None:
    body = FIXTURE.read_bytes()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=body, request=request)
    )
    settings = _settings(tmp_path)
    provider = InvestingFlashServicesPmiProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=transport,
    )
    result = await provider.fetch(
        expected_period="2026-07",
        release_date="2026-07-24",
        expected_release_at="2026-07-24T13:45:00Z",
    )
    series = result.data[SERIES_ID]
    assert series["event_id"] == 1062
    assert series["occurrence_id"] == 552847
    assert series["actual"] == 53.6
    assert series["forecast"] == 51.3
    assert series["previous"] == 51.2
    assert series["publisher"] == "S&P Global"
    assert series["distribution_source"] == "Investing.com"
    assert series["acquisition_provider"] == SOURCE
    assert set(series["field_lineage"]) == {"actual", "forecast", "previous"}


async def test_investing_fallback_uses_verified_endpoint_http_contract(
    tmp_path: Path,
) -> None:
    body = FIXTURE.read_bytes()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=body, request=request)

    settings = _settings(tmp_path)
    provider = InvestingFlashServicesPmiProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=httpx.MockTransport(handler),
    )

    await provider.fetch(
        expected_period="2026-07",
        release_date="2026-07-24",
        expected_release_at="2026-07-24T13:45:00Z",
    )

    assert len(requests) == 1
    request = requests[0]
    assert str(request.url) == settings.investing_flash_services_pmi_url
    assert request.headers["Accept"] == "application/json, text/plain, */*"
    assert request.headers["Origin"] == "https://www.investing.com"
    assert request.headers["Referer"] == "https://www.investing.com/"
    assert request.headers["User-Agent"] == INVESTING_BROWSER_USER_AGENT


async def test_investing_fallback_rejects_wrong_occurrence_or_period(
    tmp_path: Path,
) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["occurrences"][0]["occurrence_id"] = None
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json=payload, request=request)
    )
    settings = _settings(tmp_path)
    provider = InvestingFlashServicesPmiProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=transport,
    )
    try:
        await provider.fetch(
            expected_period="2026-07",
            release_date="2026-07-24",
            expected_release_at="2026-07-24T13:45:00Z",
        )
    except ProviderError as exc:
        assert str(exc) == "investing_flash_services_pmi_occurrence_id_missing"
    else:
        raise AssertionError("provider accepted an occurrence without identity")


async def test_investing_fallback_requires_exact_release_timestamp(
    tmp_path: Path,
) -> None:
    body = FIXTURE.read_bytes()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=body, request=request)
    )
    settings = _settings(tmp_path)
    provider = InvestingFlashServicesPmiProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=transport,
    )
    try:
        await provider.fetch(
            expected_period="2026-07",
            release_date="2026-07-24",
            expected_release_at="2026-07-24T13:46:00Z",
        )
    except ProviderError as exc:
        assert str(exc) == "investing_flash_services_pmi_occurrence_not_found"
    else:
        raise AssertionError("provider accepted a mismatched release timestamp")


class _FailingPrimary:
    async def fetch(self, **kwargs):
        del kwargs
        raise ProviderError("sp_global_public_release_access_restricted")


class _FailingFallback:
    async def fetch(self, **kwargs):
        del kwargs
        raise ProviderError("investing_flash_services_pmi_timeout")


def test_resolver_orders_primary_then_investing_and_preserves_both_forecasts(
    tmp_path: Path,
) -> None:
    body = FIXTURE.read_bytes()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=body, request=request)
    )
    settings = _settings(tmp_path)
    investing = InvestingFlashServicesPmiProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=transport,
    )
    resolver = DeterministicActualResolver(
        settings,
        providers={"SPGLOBAL": _FailingPrimary(), SOURCE: investing},
    )
    result = resolver.resolve_event(
        event_key="xtb:146945:2026-07-24",
        event={
            "occurrence_id": "xtb:146945:2026-07-24",
            "name": "Flash Services PMI",
            "reference_period": "2026-07",
            "release_at": "2026-07-24T13:45:00Z",
            "forecast": 51.5,
            "previous": 51.2,
        },
        temporal_state={"release_at": "2026-07-24T13:45:00Z"},
        persist_candidate=False,
    )
    assert result["status"] == "SUCCEEDED"
    assert result["provider"] == SOURCE
    assert result["provider_call_count"] == 2
    assert result["reason_code"] == "FALLBACK_SELECTED_AFTER_PRIMARY_FAILURE"
    assert [item["result"] for item in result["provider_attempts"]] == [
        "HTTP_403",
        "SUCCESS",
    ]
    candidate = result["results"][0]
    assert candidate["value"] == "53.6"
    assert candidate["actual_is_official"] is False
    assert candidate["acquisition_provider"] == SOURCE
    assert candidate["forecast_observations"] == [
        {
            "value": 51.5,
            "provider": "XTB",
            "occurrence_id": "xtb:146945:2026-07-24",
            "selected": False,
            "reason_code": "CONCURRENT_FORECAST_PRESERVED",
        },
        {
            "value": 51.3,
            "provider": SOURCE,
            "event_id": 1062,
            "occurrence_id": 552847,
            "selected": True,
            "reason_code": "EXACT_OCCURRENCE_MATCH_WITH_SELECTED_ACTUAL",
        },
    ]


def test_persisted_fallback_readback_preserves_forecast_observations(
    tmp_path: Path,
) -> None:
    body = FIXTURE.read_bytes()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=body, request=request)
    )
    settings = _settings(tmp_path)
    investing = InvestingFlashServicesPmiProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=transport,
    )
    resolver = DeterministicActualResolver(
        settings,
        providers={"SPGLOBAL": _FailingPrimary(), SOURCE: investing},
    )
    result = resolver.resolve_event(
        event_key="xtb:146945:2026-07-24",
        event={
            "occurrence_id": "xtb:146945:2026-07-24",
            "name": "Flash Services PMI",
            "reference_period": "2026-07",
            "release_at": "2026-07-24T13:45:00Z",
            "forecast": 51.5,
            "previous": 51.2,
        },
        temporal_state={"release_at": "2026-07-24T13:45:00Z"},
        persist_candidate=True,
    )
    assert result["status"] == "SUCCEEDED"
    candidate = result["results"][0]
    assert candidate["validation_status"] == "accepted"
    assert candidate["forecast"] == 51.3
    assert candidate["forecast_observations"][0]["value"] == 51.5
    assert candidate["forecast_observations"][1]["value"] == 51.3
    assert candidate["provider_accounting"]["selected_source"] == SOURCE


def test_db_first_short_circuits_all_providers(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    resolver = DeterministicActualResolver(
        settings,
        providers={"SPGLOBAL": _FailingPrimary(), SOURCE: _FailingPrimary()},
    )
    persisted = {
        "event_metric_id": "flash_services_pmi",
        "source_series_id": SERIES_ID,
        "value": "53.6",
    }

    class _Candidates:
        def accepted_official_actual(self, event_key):
            assert event_key == "xtb:146945:2026-07-24"
            return persisted

    resolver.candidates = _Candidates()
    result = resolver.resolve_event(
        event_key="xtb:146945:2026-07-24",
        event={"name": "Flash Services PMI"},
        temporal_state={},
    )
    assert result["resolution"] == "persisted_candidate"
    assert result["provider_call_count"] == 0


def test_all_provider_failures_return_null_and_account_for_order(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    resolver = DeterministicActualResolver(
        settings,
        providers={
            "SPGLOBAL": _FailingPrimary(),
            SOURCE: _FailingFallback(),
        },
    )
    result = resolver.resolve_event(
        event_key="xtb:146945:2026-07-24",
        event={
            "name": "Flash Services PMI",
            "reference_period": "2026-07",
            "release_at": "2026-07-24T13:45:00Z",
            "actual": 52.0,
        },
        temporal_state={"release_at": "2026-07-24T13:45:00Z"},
        persist_candidate=False,
    )
    assert result["status"] == "OFFICIAL_FEED_DELAYED"
    assert result["results"] == []
    assert result["provider_call_count"] == 2
    assert [item["provider"] for item in result["provider_attempts"]] == [
        "SPGLOBAL",
        SOURCE,
    ]
    assert result["fallback_reason_code"].startswith(
        "all_flash_services_pmi_providers_failed"
    )
