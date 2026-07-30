from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.models.common import (
    Freshness,
    ProviderResult,
    ProviderType,
)
from app.providers.base import ProviderError, metadata
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


def _sp_global_result(
    failure: str | None = None,
) -> ProviderResult:
    series: dict[str, Any] = {
        "official_adapter": True,
        "provider_adapter": "SPGLOBAL_OFFICIAL_API",
        "source": "SPGLOBAL",
        "source_originator": "S&P Global Market Intelligence",
        "publisher": "S&P Global Market Intelligence",
        "source_url": (
            "https://www.pmi.spglobal.com/Public/Home/"
            "PressRelease/controlled"
        ),
        "canonical_url": (
            "https://www.pmi.spglobal.com/Public/Home/PressRelease"
        ),
        "source_domain": "pmi.spglobal.com",
        "seasonal_adjustment": "SA",
        "observations": [
            {"period": "2026-06", "value": "51.2"},
            {"period": "2026-07", "value": "53.6"},
        ],
    }
    data: dict[str, Any] = {SERIES_ID: series}
    if failure == "series_missing":
        data = {}
    elif failure == "adapter_mismatch":
        series["provider_adapter"] = "UNVERIFIED_ADAPTER"
    elif failure == "period_mismatch":
        series["observations"] = [
            {"period": "2026-05", "value": "50.8"},
            {"period": "2026-06", "value": "51.2"},
        ]
    return ProviderResult(
        metadata=metadata(
            source="SPGLOBAL",
            provider_type=ProviderType.API,
            reliability=0.98,
            data_as_of=datetime(2026, 7, 24, tzinfo=UTC),
            freshness=Freshness.RECENT,
        ),
        data=data,
    )


class _PostHttpPrimary:
    def __init__(
        self,
        failure: str | None,
        call_order: list[str],
    ) -> None:
        self.failure = failure
        self.call_order = call_order

    async def fetch(self, **kwargs: Any) -> ProviderResult:
        del kwargs
        self.call_order.append("SPGLOBAL")
        return _sp_global_result(self.failure)


class _PostHttpFallback:
    def __init__(self, call_order: list[str]) -> None:
        self.call_order = call_order

    async def fetch(self, **kwargs: Any) -> ProviderResult:
        del kwargs
        self.call_order.append(SOURCE)
        return ProviderResult(
            metadata=metadata(
                source=SOURCE,
                provider_type=ProviderType.API,
                reliability=0.8,
                freshness=Freshness.RECENT,
                is_fallback=True,
            ),
            data={},
        )


def _investing_provider(
    settings: Settings,
    call_order: list[str],
) -> InvestingFlashServicesPmiProvider:
    body = FIXTURE.read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        call_order.append(SOURCE)
        return httpx.Response(
            200,
            content=body,
            request=request,
        )

    return InvestingFlashServicesPmiProvider(
        ProviderCacheRepository(settings.database_path),
        settings,
        transport=httpx.MockTransport(handler),
    )


def _pmi_event() -> dict[str, Any]:
    return {
        "occurrence_id": "xtb:146945:2026-07-24",
        "name": "Flash Services PMI",
        "reference_period": "2026-07",
        "release_at": "2026-07-24T13:45:00Z",
        "forecast": 51.5,
        "previous": 51.2,
    }


@pytest.mark.parametrize(
    ("failure", "expected_primary_result"),
    (
        ("series_missing", "official_series_not_available"),
        (
            "adapter_mismatch",
            (
                "official_adapter_required:"
                "observed=UNVERIFIED_ADAPTER"
            ),
        ),
        ("period_mismatch", "period_mismatch"),
    ),
)
def test_post_http_primary_rejection_calls_investing_in_order(
    tmp_path: Path,
    failure: str,
    expected_primary_result: str,
) -> None:
    settings = _settings(tmp_path)
    call_order: list[str] = []
    resolver = DeterministicActualResolver(
        settings,
        providers={
            "SPGLOBAL": _PostHttpPrimary(
                failure,
                call_order,
            ),
            SOURCE: _investing_provider(settings, call_order),
        },
    )

    result = resolver.resolve_event(
        event_key="xtb:146945:2026-07-24",
        event=_pmi_event(),
        temporal_state={
            "release_at": "2026-07-24T13:45:00Z"
        },
        persist_candidate=False,
    )

    assert result["status"] == "SUCCEEDED"
    assert result["provider"] == SOURCE
    assert result["provider_call_count"] == 2
    assert call_order == ["SPGLOBAL", SOURCE]
    assert [
        (
            item["provider"],
            item["called"],
            item["attempts"],
            item["result"],
            item["execution_origin"],
        )
        for item in result["provider_attempts"]
    ] == [
        (
            "SPGLOBAL",
            True,
            1,
            expected_primary_result,
            "PROVIDER_CALL",
        ),
        (SOURCE, True, 1, "SUCCESS", "PROVIDER_CALL"),
    ]
    assert result["results"][0]["acquisition_provider"] == SOURCE


class _RejectFirstCandidate:
    def __init__(self) -> None:
        self.calls = 0
        self.accepted: dict[str, Any] | None = None

    def accepted_official_actual(
        self,
        _event_key: str,
    ) -> dict[str, Any] | None:
        return self.accepted

    def persist_candidate(
        self,
        *,
        candidate: dict[str, Any],
        **_kwargs: Any,
    ) -> dict[str, Any]:
        self.calls += 1
        restored = {
            **candidate,
            "validation_status": (
                "rejected" if self.calls == 1 else "accepted"
            ),
            "warnings": (
                ["controlled_primary_candidate_rejection"]
                if self.calls == 1
                else []
            ),
        }
        if restored["validation_status"] == "accepted":
            self.accepted = restored
        return restored


def test_rejected_primary_candidate_calls_investing_once(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    call_order: list[str] = []
    resolver = DeterministicActualResolver(
        settings,
        providers={
            "SPGLOBAL": _PostHttpPrimary(None, call_order),
            SOURCE: _investing_provider(settings, call_order),
        },
    )
    candidates = _RejectFirstCandidate()
    resolver.candidates = candidates  # type: ignore[assignment]

    result = resolver.resolve_event(
        event_key="xtb:146945:2026-07-24",
        event=_pmi_event(),
        temporal_state={
            "release_at": "2026-07-24T13:45:00Z"
        },
        persist_candidate=True,
    )

    assert result["status"] == "SUCCEEDED"
    assert call_order == ["SPGLOBAL", SOURCE]
    assert candidates.calls == 2
    assert [
        item["result"]
        for item in result["provider_attempts"]
    ] == ["official_candidate_rejected", "SUCCESS"]


class _MissingCandidateReadback:
    def accepted_official_actual(
        self,
        _event_key: str,
    ) -> None:
        return None

    def persist_candidate(
        self,
        *,
        candidate: dict[str, Any],
        **_kwargs: Any,
    ) -> dict[str, Any]:
        return {
            **candidate,
            "validation_status": "accepted",
            "warnings": [],
        }


def test_primary_and_fallback_readback_failures_keep_complete_chain(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    call_order: list[str] = []
    resolver = DeterministicActualResolver(
        settings,
        providers={
            "SPGLOBAL": _PostHttpPrimary(None, call_order),
            SOURCE: _investing_provider(settings, call_order),
        },
    )
    resolver.candidates = _MissingCandidateReadback()  # type: ignore[assignment]

    result = resolver.resolve_event(
        event_key="xtb:146945:2026-07-24",
        event=_pmi_event(),
        temporal_state={
            "release_at": "2026-07-24T13:45:00Z"
        },
        persist_candidate=True,
    )

    assert result["status"] == "OFFICIAL_FEED_DELAYED"
    assert call_order == ["SPGLOBAL", SOURCE]
    assert result["provider_call_count"] == 2
    assert [
        item["result"]
        for item in result["provider_attempts"]
    ] == [
        "official_candidate_read_back_failed",
        "official_candidate_read_back_failed",
    ]


def test_post_http_failures_on_both_providers_are_accounted(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    call_order: list[str] = []
    resolver = DeterministicActualResolver(
        settings,
        providers={
            "SPGLOBAL": _PostHttpPrimary(
                "series_missing",
                call_order,
            ),
            SOURCE: _PostHttpFallback(call_order),
        },
    )

    result = resolver.resolve_event(
        event_key="xtb:146945:2026-07-24",
        event=_pmi_event(),
        temporal_state={
            "release_at": "2026-07-24T13:45:00Z"
        },
        persist_candidate=False,
    )

    assert result["status"] == "OFFICIAL_FEED_DELAYED"
    assert result["results"] == []
    assert result["provider_call_count"] == 2
    assert call_order == ["SPGLOBAL", SOURCE]
    assert [
        (item["provider"], item["called"], item["result"])
        for item in result["provider_attempts"]
    ] == [
        ("SPGLOBAL", True, "official_series_not_available"),
        (SOURCE, True, "official_series_not_available"),
    ]
    assert result["fallback_reason_code"] == (
        "all_flash_services_pmi_providers_failed:"
        "official_series_not_available"
    )


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
