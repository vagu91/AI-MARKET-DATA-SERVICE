from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
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
from app.services.lifecycle_due_resolver import (
    DeterministicLifecycleDueResolver,
)
from app.services.provider_force_actual_reconciliation_service import (
    ProviderForceActualReconciliationService,
    _actual_provider_chain_complete,
    _request_scoped_provider_attempts,
)
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
)
from app.services.senior_analyst_projection_v1 import (
    DATASET_POLICIES,
    validate_senior_analyst_payload_v1,
)
from tests.test_pr28_route_provider_force_wiring import (
    FIXTURE,
    PMI_ID,
    _seed_no_data_state,
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
    seed_backoff: bool = False,
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
            if seed_backoff:
                _seed_no_data_state(cfg, "BACKOFF")
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


def _pmi_contract() -> dict[str, Any]:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    event = next(
        dict(item)
        for item in fixture["initial_events"]
        if item["occurrence_id"] == PMI_ID
    )
    event["metric_id"] = "flash_services_pmi"
    return {
        "event_calendar": {
            "critical_macro_events": [event],
            "fed_communications": [],
            "other_economic_events": [],
        }
    }


def _pmi_collector(now: datetime) -> RequestProviderAccountingCollector:
    policy = next(
        item
        for item in DATASET_POLICIES
        if item.dataset_id == "flash_services_pmi"
    )
    return RequestProviderAccountingCollector(
        request_id="pmi-request",
        correlation_id="pmi-request",
        request_started_at=now - timedelta(minutes=1),
        policies=[policy],
        clock=lambda: now,
    )


def _backoff_lifecycle(
    now: datetime,
    *,
    actual_resolution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = _pmi_contract()["event_calendar"][
        "critical_macro_events"
    ][0]
    payload = {
        **event,
        "valid_until": (now + timedelta(hours=1)).isoformat(),
        "next_refresh_at": (now + timedelta(hours=1)).isoformat(),
    }
    if actual_resolution is not None:
        payload["actual_resolution"] = actual_resolution
    return {
        "entity_type": "macro_actual",
        "entity_key": PMI_ID,
        "freshness_state": "NO_DATA_BACKOFF",
        "work_status": "BACKOFF",
        "valid_until": payload["valid_until"],
        "next_refresh_at": payload["next_refresh_at"],
        "next_retry_at": (now + timedelta(hours=1)).isoformat(),
        "negative_cache_expires_at": (
            now + timedelta(hours=1)
        ).isoformat(),
        "payload": payload,
    }


class _UnexpectedPmiResolver:
    def __init__(self) -> None:
        self.calls = 0

    def resolve(self, _item: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        raise AssertionError("provider resolver must not be called")


class _ObservedPmiAdapter:
    performs_io = True

    def __init__(self) -> None:
        self.calls = 0

    def resolve(self, _item: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        return {
            "status": "DEFERRED",
            "reason": "controlled_provider_timeout",
            "reason_code": "controlled_provider_timeout",
            "provider": "SPGLOBAL",
            "provider_call_count": 1,
            "provider_attempts": [
                {
                    "provider": "SPGLOBAL",
                    "attempts": 1,
                    "result": "TIMEOUT",
                }
            ],
            "provider_request_attempted": True,
        }


def _accounting_service(
    tmp_path: Path,
    *,
    now: datetime,
    collector: RequestProviderAccountingCollector,
    lifecycle: dict[str, Any] | None,
    force_refresh: bool,
    resolver: _UnexpectedPmiResolver,
) -> ProviderForceActualReconciliationService:
    service = ProviderForceActualReconciliationService(
        _settings(tmp_path),
        lifecycle_resolver=resolver,
        clock=lambda: now,
        accounting_collector=collector,
        force_refresh=force_refresh,
    )
    service.lifecycle = SimpleNamespace(
        list_items=lambda: [lifecycle] if lifecycle else []
    )
    service.facts = SimpleNamespace(
        economic_event_records=lambda **_kwargs: []
    )
    service.telemetry = SimpleNamespace(emit=lambda *_args, **_kwargs: None)
    return service


def test_backoff_emit_decision_is_propagated_to_request_audit(
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    collector = _pmi_collector(now)
    resolver = _UnexpectedPmiResolver()
    service = _accounting_service(
        tmp_path,
        now=now,
        collector=collector,
        lifecycle=_backoff_lifecycle(now),
        force_refresh=False,
        resolver=resolver,
    )

    result = service.prepare(_pmi_contract())

    assert resolver.calls == 0
    audit = result["audit"]["occurrences"][0]
    assert audit["mapping_selected"] == "flash_services_pmi"
    assert audit["eligibility"] == "BACKOFF_ACTIVE"
    assert audit["request_id"] == collector.request_id
    assert audit["correlation_id"] == collector.correlation_id
    assert [item["provider"] for item in audit["provider_attempts"]] == [
        "SPGLOBAL",
        INVESTING_SOURCE,
    ]
    assert all(
        item["called"] is False
        and item["attempts"] == 0
        and item["execution_origin"] == "OBSERVED_SKIP"
        and item["not_called_reason"] == "NEXT_RETRY_IN_FUTURE"
        and item["request_id"] == collector.request_id
        and item["correlation_id"] == collector.correlation_id
        and item["observed_at"] == now.isoformat()
        for item in audit["provider_attempts"]
    )
    manifest = collector.manifest(request_completed_at=now)
    assert manifest["evidence_status"] == "INCOMPLETE"
    row = manifest["datasets"][0]
    assert row["evidence_status"] == "INCOMPLETE"
    assert row["primary_provider"]["called"] is False
    assert row["fallbacks"][0]["called"] is False


@pytest.mark.parametrize(
    (
        "state",
        "expected_eligibility",
        "expected_reason",
    ),
    (
        (
            "FUTURE",
            "FUTURE_NOT_DUE",
            "OCCURRENCE_NOT_PUBLISHED",
        ),
        (
            "FRESH_NO_DATA",
            "FRESH_NO_DATA",
            "NO_DATA_STILL_FRESH",
        ),
        (
            "EXHAUSTED_NO_DATA",
            "EXHAUSTED_NO_DATA",
            "TERMINAL_NO_DATA",
        ),
    ),
)
def test_correlated_observed_skip_chain_completes_pmi_accounting(
    tmp_path: Path,
    state: str,
    expected_eligibility: str,
    expected_reason: str,
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    collector = _pmi_collector(now)
    resolver = _UnexpectedPmiResolver()
    contract = _pmi_contract()
    lifecycle: dict[str, Any] | None = None
    if state == "FUTURE":
        event = contract["event_calendar"][
            "critical_macro_events"
        ][0]
        future = (now + timedelta(hours=1)).isoformat()
        event["release_at"] = future
        event["time_utc"] = future
    else:
        lifecycle = _backoff_lifecycle(now)
        lifecycle.pop("negative_cache_expires_at", None)
        lifecycle.pop("next_retry_at", None)
        lifecycle["freshness_state"] = state
        lifecycle["work_status"] = state
        if state == "FRESH_NO_DATA":
            lifecycle["freshness_state"] = "NO_DATA_FRESH"

    service = _accounting_service(
        tmp_path,
        now=now,
        collector=collector,
        lifecycle=lifecycle,
        force_refresh=True,
        resolver=resolver,
    )

    result = service.prepare(contract)

    assert resolver.calls == 0
    audit = result["audit"]["occurrences"][0]
    assert audit["eligibility"] == expected_eligibility
    assert audit["reason_code"] == expected_reason
    assert audit["provider_call_count"] == 0
    assert [
        item["provider"]
        for item in audit["provider_attempts"]
    ] == ["SPGLOBAL", INVESTING_SOURCE]
    assert all(
        item["called"] is False
        and item["attempts"] == 0
        and item["execution_origin"]
        in {"OBSERVED_SKIP", "CACHE_DECISION"}
        and item["not_called_reason"]
        and item["request_id"] == collector.request_id
        and item["correlation_id"] == collector.correlation_id
        for item in audit["provider_attempts"]
    )
    manifest = collector.manifest(request_completed_at=now)
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    assert manifest["datasets"][0]["evidence_status"] == (
        "ACQUISITION_COMPLETE"
    )


def test_missing_provider_attempt_evidence_never_completes_pmi_accounting() -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    collector = _pmi_collector(now)
    attempts = _request_scoped_provider_attempts(
        [
            {
                "provider": "SPGLOBAL",
                "called": False,
                "attempts": 0,
                "result": "NOT_CALLED",
                "not_called_reason": "PROVIDER_ATTEMPT_EVIDENCE_MISSING",
                "execution_origin": "OBSERVED_SKIP",
            },
            {
                "provider": INVESTING_SOURCE,
                "called": False,
                "attempts": 0,
                "result": "NOT_CALLED",
                "not_called_reason": "PROVIDER_ATTEMPT_EVIDENCE_MISSING",
                "execution_origin": "OBSERVED_SKIP",
            },
        ],
        request_id=collector.request_id,
        correlation_id=collector.correlation_id,
        observed_at=now,
    )

    assert not _actual_provider_chain_complete(
        attempts,
        provider_calls=0,
        request_id=collector.request_id,
        correlation_id=collector.correlation_id,
    )
    collector.record(
        "flash_services_pmi",
        acquisition_id=(
            "flash_services_pmi_actual_resolution:controlled-pmi"
        ),
        shared_dataset_ids=("flash_services_pmi",),
        database_lookup_performed=True,
        database_lookup_reason="CONTROLLED_DB_LOOKUP",
        database_record_found=True,
        database_data_as_of=(now - timedelta(days=1)).isoformat(),
        database_content_valid_until=(now + timedelta(hours=1)).isoformat(),
        database_refresh_due_at=(now + timedelta(hours=1)).isoformat(),
        database_lifecycle_status="NO_DATA_BACKOFF",
        database_record_expired=True,
        database_freshness_evaluation="INVALID_LIFECYCLE",
        primary_provider=attempts[0],
        fallbacks=[attempts[1]],
        acquisition_selected_source=None,
        acquisition_reason_code=(
            "REQUEST_ACQUISITION_EVIDENCE_INCOMPLETE"
        ),
        observed_at=now,
    )
    manifest = collector.manifest(request_completed_at=now)
    assert manifest["evidence_status"] == "INCOMPLETE"
    assert manifest["datasets"][0]["evidence_status"] == "INCOMPLETE"


def test_pmi_accounting_selects_latest_released_not_future_occurrence(
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    collector = _pmi_collector(now)
    resolver = _UnexpectedPmiResolver()
    contract = _pmi_contract()
    past = contract["event_calendar"]["critical_macro_events"][0]
    past_release = (now - timedelta(hours=1)).isoformat()
    content_valid_until = (now + timedelta(hours=2)).isoformat()
    refresh_due_at = (now + timedelta(hours=1)).isoformat()
    reference_period = now.strftime("%Y-%m")
    past.update(
        {
            "release_at": past_release,
            "time_utc": past_release,
            "reference_period": reference_period,
            "actual": 51.4,
            "actual_source": "S&P Global",
            "actual_is_official": True,
            "freshness_state": "CURRENT_RELEASE",
            "content_valid_until": content_valid_until,
            "refresh_due_at": refresh_due_at,
            "source_lineage": [
                {
                    "occurrence_id": PMI_ID,
                    "source": "S&P Global",
                    "publisher": "S&P Global",
                    "source_url": (
                        "https://www.pmi.spglobal.com/Public/"
                        "Home/PressRelease"
                    ),
                    "source_field": "actual",
                    "source_series_id": (
                        "SPGLOBAL:US:FLASH_SERVICES_PMI"
                    ),
                    "metric_id": "flash_services_pmi",
                    "frequency": "monthly",
                    "transformation": "level",
                    "reference_period": reference_period,
                    "value": 51.4,
                    "freshness": "CURRENT_RELEASE",
                    "content_valid_until": content_valid_until,
                    "refresh_due_at": refresh_due_at,
                    "validation_status": "accepted",
                }
            ],
        }
    )
    future = {
        **past,
        "occurrence_id": "controlled-future-pmi",
        "event_id": "controlled-future-pmi",
        "release_at": (now + timedelta(hours=1)).isoformat(),
        "time_utc": (now + timedelta(hours=1)).isoformat(),
        "actual": None,
        "actual_source": None,
    }
    contract["event_calendar"]["critical_macro_events"].append(future)
    lifecycle = {
        "entity_type": "macro_actual",
        "entity_key": PMI_ID,
        "freshness_state": "CURRENT",
        "work_status": "RESOLVED",
        "valid_until": content_valid_until,
        "next_refresh_at": refresh_due_at,
        "payload": {
                **past,
                "actual_resolution": {
                    "occurrence_id": PMI_ID,
                    "mapping_selected": "flash_services_pmi",
                    "reason_code": "CONTROLLED_DATABASE_ACTUAL",
                },
        },
    }
    service = _accounting_service(
        tmp_path,
        now=now,
        collector=collector,
        lifecycle=lifecycle,
        force_refresh=False,
        resolver=resolver,
    )

    result = service.prepare(contract)

    assert resolver.calls == 0
    audits = {
        item["occurrence_id"]: item
        for item in result["audit"]["occurrences"]
    }
    assert audits[PMI_ID]["eligibility"] == "VALID_DATABASE_RECORD"
    assert audits["controlled-future-pmi"]["eligibility"] == (
        "FUTURE_NOT_DUE"
    )
    manifest = collector.manifest(request_completed_at=now)
    row = manifest["datasets"][0]
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    assert row["acquisition_id"] == (
        f"flash_services_pmi_actual_resolution:{PMI_ID}"
    )
    assert row["database_freshness_evaluation"] == "VALID"
    assert row["primary_provider"]["execution_origin"] == (
        "CACHE_DECISION"
    )


def test_same_request_failure_evidence_is_reused_without_duplicate_calls(
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    collector = _pmi_collector(now)
    attempts = [
        {
            "provider": "SPGLOBAL",
            "called": True,
            "attempts": 1,
            "result": "HTTP_403",
            "execution_origin": "PROVIDER_CALL",
            "request_id": collector.request_id,
            "correlation_id": collector.correlation_id,
            "observed_at": now.isoformat(),
        },
        {
            "provider": INVESTING_SOURCE,
            "called": True,
            "attempts": 1,
            "result": "investing_flash_services_pmi_timeout",
            "execution_origin": "PROVIDER_CALL",
            "request_id": collector.request_id,
            "correlation_id": collector.correlation_id,
            "observed_at": now.isoformat(),
        },
    ]
    prior_audit = {
        "occurrence_id": PMI_ID,
        "request_id": collector.request_id,
        "correlation_id": collector.correlation_id,
        "mapping_selected": "flash_services_pmi",
        "provider_attempted": INVESTING_SOURCE,
        "provider_call_count": 2,
        "provider_attempts": attempts,
        "reason_code": "all_flash_services_pmi_providers_failed",
        "attempted_at": now.isoformat(),
    }
    resolver = _UnexpectedPmiResolver()
    service = _accounting_service(
        tmp_path,
        now=now,
        collector=collector,
        lifecycle=_backoff_lifecycle(
            now,
            actual_resolution=prior_audit,
        ),
        force_refresh=True,
        resolver=resolver,
    )

    result = service.prepare(_pmi_contract())

    assert resolver.calls == 0
    audit = result["audit"]["occurrences"][0]
    assert audit["reconciliation_outcome"] == (
        "SAME_REQUEST_PROVIDER_EVIDENCE_REUSED"
    )
    assert audit["provider_attempts"] == attempts
    row = collector.manifest()["datasets"][0]
    assert row["evidence_status"] == "ACQUISITION_COMPLETE"
    assert row["acquisition_reason_code"] == (
        "FLASH_SERVICES_PMI_ALL_PROVIDERS_FAILED"
    )
    assert row["primary_provider"]["attempts"] == 1
    assert row["fallbacks"][0]["attempts"] == 1


def test_atomic_force_mode_bypasses_only_the_scoped_negative_cache(
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    adapter = _ObservedPmiAdapter()
    resolver = DeterministicLifecycleDueResolver(
        _settings(tmp_path),
        clock=lambda: now,
        adapters={"macro_actual": adapter},
    )
    item = {
        "entity_type": "macro_actual",
        "entity_key": PMI_ID,
        "freshness_state": "NO_DATA_BACKOFF",
        "next_retry_at": (now + timedelta(hours=1)).isoformat(),
        "negative_cache_expires_at": (
            now + timedelta(hours=1)
        ).isoformat(),
        "payload": {},
        "resolution_mode": "prepare_atomic_provider_force",
        "force_refresh": True,
    }

    forced = resolver.resolve(item)

    assert adapter.calls == 1
    assert forced["provider_negative_cache_hit"] is False
    assert forced["provider_negative_cache_bypassed"] is True
    assert forced["provider_attempts"][0]["result"] == "TIMEOUT"

    ordinary = resolver.resolve(
        {
            **item,
            "force_refresh": False,
        }
    )

    assert adapter.calls == 1
    assert ordinary["reason"] == "provider_negative_cache_active"
    assert ordinary["provider_negative_cache_hit"] is True
    assert ordinary["provider_negative_cache_bypassed"] is False


def test_request_scoped_pmi_attempt_telemetry_is_redacted() -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    attempts = _request_scoped_provider_attempts(
        [
            {
                "provider": "SPGLOBAL",
                "attempts": 1,
                "result": "api" + "_key=controlled-placeholder",
            }
        ],
        request_id="pmi-request",
        correlation_id="pmi-request",
        observed_at=now,
    )

    assert attempts == [
        {
            "provider": "SPGLOBAL",
            "called": True,
            "attempts": 1,
            "result": "REDACTED_PROVIDER_RESULT",
            "not_called_reason": None,
            "execution_origin": "PROVIDER_CALL",
            "request_id": "pmi-request",
            "correlation_id": "pmi-request",
            "observed_at": now.isoformat(),
        }
    ]


def test_post_http_primary_rejection_chain_is_accounting_complete() -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    collector = _pmi_collector(now)
    attempts = _request_scoped_provider_attempts(
        [
            {
                "provider": "SPGLOBAL",
                "called": True,
                "attempts": 1,
                "result": "official_series_not_available",
                "execution_origin": "PROVIDER_CALL",
            },
            {
                "provider": INVESTING_SOURCE,
                "called": True,
                "attempts": 1,
                "result": "SUCCESS",
                "execution_origin": "PROVIDER_CALL",
            },
        ],
        request_id=collector.request_id,
        correlation_id=collector.correlation_id,
        observed_at=now,
    )

    assert _actual_provider_chain_complete(
        attempts,
        provider_calls=2,
        request_id=collector.request_id,
        correlation_id=collector.correlation_id,
    )
    collector.record(
        "flash_services_pmi",
        acquisition_id="post_http_primary_rejection",
        shared_dataset_ids=("flash_services_pmi",),
        database_lookup_performed=True,
        database_lookup_reason="CONTROLLED_DB_LOOKUP",
        database_record_found=False,
        database_data_as_of=None,
        database_content_valid_until=None,
        database_record_expired=False,
        database_freshness_evaluation="NOT_FOUND",
        primary_provider=attempts[0],
        fallbacks=[attempts[1]],
        acquisition_selected_source=INVESTING_SOURCE,
        acquisition_reason_code=(
            "FLASH_SERVICES_PMI_DELIVERABLE_ACQUIRED"
        ),
        observed_at=now,
    )
    manifest = collector.manifest(request_completed_at=now)
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    assert manifest["datasets"][0]["evidence_status"] == (
        "ACQUISITION_COMPLETE"
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
    assert row["selected_source"] == INVESTING_SOURCE, json.dumps(
        {
            "events": payload["analytics"]["calendar"].get(
                "latest_released_events"
            ),
            "missing_data": [
                item
                for item in payload.get("missing_data") or []
                if "flash" in str(item).casefold()
                or PMI_ID in str(item)
            ],
            "row": row,
        },
        default=str,
        sort_keys=True,
    )
    assert row["selected_value_present"] is True
    assert row["delivered_value"]["payload_path"] == [
        "analytics.calendar.latest_released_events",
        "analytics.calendar.active_event_windows",
        "analytics.calendar.next_24h_events",
        "analytics.calendar.next_7d_high_impact_events",
    ]
    assert row["delivered_value"]["item_count"] == sum(
        len(payload["analytics"]["calendar"].get(key) or [])
        for key in (
            "latest_released_events",
            "active_event_windows",
            "next_24h_events",
            "next_7d_high_impact_events",
        )
    )
    assert len(row["delivered_value"]["content_sha256"]) == 64
    assert any(
        event.get("actual") == 53.6
        for event in payload["analytics"]["calendar"].get(
            "latest_released_events"
        )
        or []
    )
    assert payload["request"]["same_request_provider_accounting"] is True
    validation = validate_senior_analyst_payload_v1(
        payload,
        require_recent_response=True,
    )
    assert validation["checks"]["provider_accounting_valid"] is True


def test_real_force_route_bypasses_prior_request_negative_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, call_order = _run_real_route(
        tmp_path,
        monkeypatch,
        investing_succeeds=True,
        seed_backoff=True,
    )
    row = _pmi_row(payload)

    assert call_order == ["SPGLOBAL", INVESTING_SOURCE]
    assert row["database_lifecycle_status"] == "NO_DATA_BACKOFF"
    assert row["database_record_expired"] is True
    assert row["database_freshness_evaluation"] == "INVALID_LIFECYCLE"
    attempts = [row["primary_provider"], *row["fallbacks"]]
    assert [item["provider"] for item in attempts] == [
        "SPGLOBAL",
        INVESTING_SOURCE,
    ]
    assert all(item["called"] is True for item in attempts)
    assert all(item["attempts"] == 1 for item in attempts)
    assert all(
        item["execution_origin"] == "PROVIDER_CALL"
        and item["request_id"] == row["request_id"]
        and item["correlation_id"] == row["correlation_id"]
        and item["observed_at"]
        for item in attempts
    )
    assert row["evidence_status"] == "COMPLETE"
    assert payload["request"]["same_request_provider_accounting"] is True

    pmi_event = next(
        item
        for item in payload["analytics"]["calendar"][
            "latest_released_events"
        ]
        if item["metric_id"] == "flash_services_pmi"
    )
    assert pmi_event["occurrence_id"] == PMI_ID
    assert pmi_event["actual"] == 53.6
    assert pmi_event["consensus"] == 51.3
    assert pmi_event["previous"] == 51.2
    assert {
        item["field"]: item.get("occurrence_id")
        for item in pmi_event["lineage"]
    } == {
        "actual": PMI_ID,
        "forecast": PMI_ID,
        "previous": PMI_ID,
    }


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
    assert [
        item["provider"]
        for item in [row["primary_provider"], *row["fallbacks"]]
    ] == ["SPGLOBAL", INVESTING_SOURCE]
    assert all(
        item["request_id"] == row["request_id"]
        and item["correlation_id"] == row["correlation_id"]
        and item["observed_at"]
        for item in [row["primary_provider"], *row["fallbacks"]]
    )
    assert row["evidence_status"] == "COMPLETE"
    assert row["selected_value_present"] is False
    assert row["selected_source"] is None
    assert row["delivered_value"] is None
    assert row["acquisition_reason_code"] == (
        "FLASH_SERVICES_PMI_ALL_PROVIDERS_FAILED"
    )
    assert row["reason_code"] == (
        "INVALID_DATABASE_LIFECYCLE_AND_NO_VALID_REPLACEMENT"
    )
    assert payload["request"]["same_request_provider_accounting"] is True
    validation = validate_senior_analyst_payload_v1(
        payload,
        require_recent_response=True,
    )
    assert validation["checks"]["provider_accounting_valid"] is True
