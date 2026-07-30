from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

import app.main as main_module
from app.bootstrap.application import build_application_state
from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.fred import FredProvider
from app.providers.investing_flash_services_pmi import (
    InvestingFlashServicesPmiProvider,
    SOURCE as INVESTING_SOURCE,
)
from app.providers.sp_global_pmi import SpGlobalPmiProvider
from app.services.market_news_repository import MarketNewsRepository
from app.services.senior_analyst_projection_v1 import (
    validate_senior_analyst_payload_v1,
)
from tests.test_pr28_route_provider_force_wiring import (
    FIXTURE,
    _seed_route_state,
    _seed_senior_canonical_facts,
    _settings,
)


ROOT = Path(__file__).resolve().parents[1]
INVESTING_FIXTURE = (
    ROOT
    / "tests"
    / "fixtures"
    / "investing_flash_services_pmi_1062.json"
)


def _install_pmi_application(
    monkeypatch: pytest.MonkeyPatch,
    cfg: Settings,
    *,
    investing_succeeds: bool,
    call_order: list[str],
) -> None:
    controlled = json.loads(FIXTURE.read_text(encoding="utf-8"))[
        "controlled_http_boundary"
    ]
    investing_payload = json.loads(
        INVESTING_FIXTURE.read_text(encoding="utf-8")
    )

    def fred_http(request: httpx.Request) -> httpx.Response:
        series_id = str(request.url.params.get("series_id") or "")
        return httpx.Response(
            200,
            json={
                "observations": controlled["fred"].get(
                    series_id,
                    controlled["fred"]["default"],
                )
            },
            request=request,
        )

    def sp_http(request: httpx.Request) -> httpx.Response:
        call_order.append("SPGLOBAL")
        return httpx.Response(403, text="forbidden", request=request)

    def investing_http(request: httpx.Request) -> httpx.Response:
        call_order.append(INVESTING_SOURCE)
        if investing_succeeds:
            return httpx.Response(
                200,
                json=investing_payload,
                request=request,
            )
        return httpx.Response(
            503,
            text="controlled unavailable",
            request=request,
        )

    def production_state(settings: Settings) -> dict[str, Any]:
        cache = ProviderCacheRepository(settings.database_path)
        return build_application_state(
            settings,
            deterministic_provider_overrides={
                "fred": FredProvider(
                    cache,
                    settings,
                    transport=httpx.MockTransport(fred_http),
                ),
                "spglobal": SpGlobalPmiProvider(
                    cache,
                    settings,
                    transport=httpx.MockTransport(sp_http),
                ),
                "investing_flash_services_pmi": (
                    InvestingFlashServicesPmiProvider(
                        cache,
                        settings,
                        transport=httpx.MockTransport(investing_http),
                    )
                ),
            },
        )

    monkeypatch.setattr(main_module, "get_settings", lambda: cfg)
    monkeypatch.setattr(
        main_module,
        "build_application_state",
        production_state,
    )
    monkeypatch.setattr(
        main_module,
        "maybe_run_startup_cleanup",
        lambda _settings: None,
    )


def _seed_current_news(cfg: Settings) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    MarketNewsRepository(cfg).upsert_news(
        {
            "title": "Controlled Federal Reserve release",
            "summary": (
                "Current request-scoped news fixture for the production route."
            ),
            "source": "Federal Reserve",
            "source_url": (
                "https://www.federalreserve.gov/newsevents/"
                "pressreleases/test.htm"
            ),
            "published_at": (now - timedelta(minutes=1)).isoformat(),
            "retrieved_at": now.isoformat(),
            "valid_until": (now + timedelta(hours=6)).isoformat(),
            "next_refresh_at": (now + timedelta(hours=6)).isoformat(),
            "topics": ["Federal Reserve", "macro"],
            "provider_type": "RSS",
            "is_official": True,
            "source_verification_status": "VERIFIED",
            "reliability": 0.9,
        }
    )


def _run_real_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    investing_succeeds: bool,
) -> tuple[dict[str, Any], list[str]]:
    cfg = _settings(tmp_path)
    call_order: list[str] = []
    _install_pmi_application(
        monkeypatch,
        cfg,
        investing_succeeds=investing_succeeds,
        call_order=call_order,
    )

    with respx.mock(
        assert_all_mocked=False,
        assert_all_called=False,
    ) as network:
        network.route().respond(404)
        with TestClient(main_module.app) as client:
            _seed_route_state(
                cfg,
                main_module.app.state.event_service,
                seed_stale_lifecycle=True,
            )
            _seed_senior_canonical_facts(cfg)
            _seed_current_news(cfg)
            response = client.get(
                "/market-context/mnq"
                "?refresh=force&view=consumer"
                "&audience=senior_analyst_v1"
            )
            assert response.status_code == 200, response.text
    return response.json(), call_order


def _pmi_row(payload: dict[str, Any]) -> dict[str, Any]:
    return next(
        row
        for row in payload["provider_accounting"]
        if row["dataset_id"] == "flash_services_pmi"
    )


def test_real_route_uses_investing_after_sp_global_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, call_order = _run_real_route(
        tmp_path,
        monkeypatch,
        investing_succeeds=True,
    )
    row = _pmi_row(payload)

    assert call_order == ["SPGLOBAL", INVESTING_SOURCE]
    assert row["database_lookup_performed"] is True
    assert row["database_record_found"] is True
    assert row["database_record_expired"] is True
    assert row["primary_provider"]["called"] is True
    assert row["fallbacks"][0]["called"] is True
    assert row["acquisition_selected_source"] == INVESTING_SOURCE
    assert row["acquisition_reason_code"] == (
        "FLASH_SERVICES_PMI_DELIVERABLE_ACQUIRED"
    )
    assert row["selected_source"] == INVESTING_SOURCE
    assert row["selected_value_present"] is True
    assert row["delivered_value"][0]["value"] == 53.6
    assert payload["request"]["same_request_provider_accounting"] is True
    validation = validate_senior_analyst_payload_v1(
        payload,
        require_recent_response=True,
    )
    assert validation["checks"]["provider_accounting_valid"] is True


def test_real_route_all_pmi_providers_failed_delivers_null(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, call_order = _run_real_route(
        tmp_path,
        monkeypatch,
        investing_succeeds=False,
    )
    row = _pmi_row(payload)

    assert call_order == ["SPGLOBAL", INVESTING_SOURCE]
    assert row["database_lookup_performed"] is True
    assert row["database_record_found"] is True
    assert row["database_record_expired"] is True
    assert row["primary_provider"]["called"] is True
    assert row["fallbacks"][0]["called"] is True
    assert row["selected_value_present"] is False
    assert row["selected_source"] is None
    assert row["delivered_value"] is None
    assert row["acquisition_reason_code"] == (
        "FLASH_SERVICES_PMI_VALUE_NOT_AVAILABLE"
    )
    assert row["reason_code"] == "FINAL_PAYLOAD_VALUE_NOT_AVAILABLE"
    assert payload["request"]["same_request_provider_accounting"] is True
    validation = validate_senior_analyst_payload_v1(
        payload,
        require_recent_response=True,
    )
    assert validation["checks"]["provider_accounting_valid"] is True
