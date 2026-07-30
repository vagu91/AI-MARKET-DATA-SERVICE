from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.api.routes as routes
from app.api.deps import (
    get_deterministic_provider_runtime,
    get_enrichment_orchestrator,
    get_event_service,
    get_event_window_service,
    get_lifecycle_due_resolver,
    get_macro_service,
    get_nasdaq_data_service,
)
from app.core.config import Settings
from app.models.common import (
    Freshness,
    ProviderMetadata,
    ProviderType,
)
from app.models.macro import MacroLatestResponse, MacroSeries
from app.services.data_freshness_service import DataFreshnessService
from app.services.diagnostics_service import DiagnosticsService
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
    provider_attempt,
)
from app.services.provider_force_actual_reconciliation_service import (
    ProviderForceActualReconciliationService,
)
from app.services.senior_analyst_projection_v1 import (
    DATASET_POLICIES,
    build_senior_analyst_payload_v1,
    validate_senior_analyst_payload_v1,
)


def _settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "accounting-route.sqlite",
        source_policy_path="config/source_policy.json",
        ai_job_workspace_root=tmp_path / "jobs",
        temp_dir=tmp_path / "temp",
        diagnostics_dir=tmp_path / "diagnostics",
        backups_dir=tmp_path / "backups",
        logs_dir=tmp_path / "logs",
        enable_scheduler=False,
        ai_worker_enabled=False,
        enable_ai_researcher=False,
    )


def _collector(
    policies=DATASET_POLICIES,
) -> RequestProviderAccountingCollector:
    now = datetime.now(UTC)
    return RequestProviderAccountingCollector(
        request_id="runtime-accounting",
        correlation_id="runtime-accounting",
        request_started_at=now - timedelta(seconds=1),
        policies=policies,
    )


class _Facts:
    def __init__(self, rows):
        self.rows = rows

    def get_valid_facts_by_type(
        self,
        _fact_type,
        *,
        allow_stale=False,
    ):
        return list(self.rows) if allow_stale else []


class BlsProvider:
    pass


class _MacroService:
    def __init__(self, response):
        self.response = response
        self.providers = [BlsProvider()]
        self.calls = 0

    async def latest(self):
        self.calls += 1
        return self.response


def _macro_service(
    settings: Settings,
    *,
    rows,
    response: MacroLatestResponse,
) -> DiagnosticsService:
    service = object.__new__(DiagnosticsService)
    service.settings = settings
    service.freshness = DataFreshnessService(settings)
    service.facts = _Facts(rows)
    service.macro_service = _MacroService(response)
    service._save_macro = lambda _macro: len(_macro.series)
    return service


def _macro_fact(*, valid_until: datetime) -> dict:
    return {
        "fact_key": "US:CUSR0000SA0:latest:official_macro_latest",
        "category": "CUSR0000SA0",
        "source": "BLS",
        "retrieved_at": datetime.now(UTC).isoformat(),
        "release_at": "2026-06",
        "valid_until": valid_until.isoformat(),
        "next_refresh_at": valid_until.isoformat(),
        "lifecycle_status": "CURRENT",
        "raw_payload": {
            "series_id": "CUSR0000SA0",
            "data_as_of": "2026-06",
            "next_refresh_at": valid_until.isoformat(),
        },
    }


def _valid_macro_facts(*, valid_until: datetime) -> list[dict]:
    series = (
        ("DGS2", "FRED"),
        ("DFF", "FRED"),
        ("DFEDTARL", "FEDERAL_RESERVE"),
        ("CUSR0000SA0", "BLS"),
        ("WPUFD4", "BLS"),
        ("BEA:PCE", "BEA"),
        ("BEA:GDP", "BEA"),
        ("LNS14000000", "BLS"),
        ("CES0500000003", "BLS"),
        ("CES0000000001", "BLS"),
        ("ICSA", "FRED"),
    )
    return [
        {
            **_macro_fact(valid_until=valid_until),
            "fact_key": (
                f"US:{series_id}:latest:official_macro_latest"
            ),
            "category": series_id,
            "source": source,
            "raw_payload": {
                "series_id": series_id,
                "data_as_of": "2026-07-29",
                "next_refresh_at": valid_until.isoformat(),
            },
        }
        for series_id, source in series
    ]


def test_valid_db_record_observably_skips_provider(tmp_path) -> None:
    settings = _settings(tmp_path)
    service = _macro_service(
        settings,
        rows=_valid_macro_facts(
            valid_until=datetime.now(UTC) + timedelta(hours=1)
        ),
        response=MacroLatestResponse(),
    )
    collector = _collector(
        [
            policy
            for policy in DATASET_POLICIES
            if policy.dataset_id
            in {
                "treasury_rates",
                "fed_funds",
                "target_range",
                "cpi",
                "ppi",
                "pce",
                "gdp",
                "employment",
                "wages",
                "nfp",
                "jobless_claims",
            }
        ]
    )

    import asyncio

    asyncio.run(
        service._macro_db_first(
            force=True,
            accounting_collector=collector,
        )
    )

    assert service.macro_service.calls == 0
    cpi = next(
        row
        for row in collector.manifest()["datasets"]
        if row["dataset_id"] == "cpi"
    )
    assert cpi["database_freshness_evaluation"] == "VALID"
    assert cpi["database_lifecycle_status"] == "CURRENT"
    assert cpi["primary_provider"]["called"] is False
    assert (
        cpi["primary_provider"]["not_called_reason"]
        == "VALID_DATABASE_RECORD_SELECTED"
    )
    assert cpi["primary_provider"]["execution_origin"] == "CACHE_DECISION"


def test_expired_db_record_calls_primary_provider(tmp_path) -> None:
    settings = _settings(tmp_path)
    metadata = ProviderMetadata(
        source="BLS",
        provider_type=ProviderType.API,
        freshness=Freshness.RECENT,
        reliability=1.0,
    )
    response = MacroLatestResponse(
        series=[
            MacroSeries(
                series_id="CUSR0000SA0",
                name="CPI",
                value=321.0,
                data_as_of="2026-06",
                source="BLS",
                metadata=metadata,
            )
        ],
        provider_results=[metadata],
    )
    service = _macro_service(
        settings,
        rows=[
            _macro_fact(
                valid_until=datetime.now(UTC) - timedelta(seconds=1)
            )
        ],
        response=response,
    )
    collector = _collector(
        [
            policy
            for policy in DATASET_POLICIES
            if policy.dataset_id
            in {
                "treasury_rates",
                "fed_funds",
                "target_range",
                "cpi",
                "ppi",
                "pce",
                "gdp",
                "employment",
                "wages",
                "nfp",
                "jobless_claims",
            }
        ]
    )

    import asyncio

    asyncio.run(
        service._macro_db_first(
            force=True,
            accounting_collector=collector,
        )
    )

    assert service.macro_service.calls == 1
    cpi = next(
        row
        for row in collector.manifest()["datasets"]
        if row["dataset_id"] == "cpi"
    )
    assert cpi["database_freshness_evaluation"] == (
        "EXPIRED_CONTENT_VALID_UNTIL"
    )
    assert cpi["primary_provider"]["called"] is True
    assert cpi["primary_provider"]["execution_origin"] == "PROVIDER_CALL"


class _ControlledProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch(self, dataset_id: str) -> str:
        self.calls.append(dataset_id)
        return f"CONTROLLED:{dataset_id}"


def _record_observed_call(
    collector,
    *,
    policy,
    source,
) -> None:
    collector.record(
        policy.dataset_id,
        acquisition_id=f"controlled:{policy.dataset_id}",
        shared_dataset_ids=(policy.dataset_id,),
        database_lookup_performed=False,
        database_lookup_reason="CONTROLLED_PROVIDER_ONLY_PATH",
        database_record_found=None,
        database_data_as_of=None,
        database_content_valid_until=None,
        database_record_expired=None,
        database_freshness_evaluation="NOT_LOOKED_UP",
        primary_provider=provider_attempt(
            policy.primary_provider,
            called=True,
            attempts=1,
            result="SUCCESS",
            execution_origin="PROVIDER_CALL",
        ),
        fallbacks=[
            provider_attempt(
                provider,
                called=False,
                attempts=0,
                result="NOT_CALLED",
                not_called_reason="PRIMARY_PROVIDER_SUCCEEDED",
                execution_origin="OBSERVED_SKIP",
            )
            for provider in policy.fallback_providers
        ],
        acquisition_selected_source=source,
        acquisition_reason_code="CONTROLLED_PROVIDER_RESULT_OBSERVED",
    )


def test_route_plumbing_cannot_pass_with_provider_only_manual_accounting(
    tmp_path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    provider = _ControlledProvider()
    deterministic_ids = {
        "market_internals",
        "options_positioning",
    }

    class ControlledDiagnostics:
        def __init__(self, *_args, **_kwargs):
            self.force_generation_plan = {}

        async def full_model(self, **kwargs):
            collector = kwargs["accounting_collector"]
            for policy in DATASET_POLICIES:
                if policy.dataset_id in deterministic_ids:
                    continue
                source = await provider.fetch(policy.dataset_id)
                _record_observed_call(
                    collector,
                    policy=policy,
                    source=source,
                )
            return {
                "symbol": "MNQ",
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "sections": {},
            }

    class ControlledDeterministicRuntime:
        async def enrich_market_context(
            self,
            contract,
            *,
            refresh,
            accounting_collector,
        ):
            assert refresh == "force"
            for dataset_id in sorted(deterministic_ids):
                policy = next(
                    item
                    for item in DATASET_POLICIES
                    if item.dataset_id == dataset_id
                )
                source = await provider.fetch(dataset_id)
                _record_observed_call(
                    accounting_collector,
                    policy=policy,
                    source=source,
                )
            return contract

    def materialize(contract, **kwargs):
        attached = routes._attach_request_scoped_accounting(
            contract,
            request_id=kwargs["request_id"],
            request_started_at=kwargs["request_started_at"],
        )
        return build_senior_analyst_payload_v1(
            attached,
            request_id=kwargs["request_id"],
            request_refresh_mode=kwargs["refresh"],
        )

    monkeypatch.setattr(routes, "DiagnosticsService", ControlledDiagnostics)
    monkeypatch.setattr(routes, "_materialize_market_context", materialize)
    monkeypatch.setattr(
        routes,
        "_emit_force_finalization",
        lambda *_args, **_kwargs: None,
    )

    api = FastAPI()
    api.include_router(routes.router)
    orchestrator = SimpleNamespace(settings=settings)
    api.dependency_overrides[get_macro_service] = object
    api.dependency_overrides[get_event_service] = object
    api.dependency_overrides[get_event_window_service] = object
    api.dependency_overrides[get_nasdaq_data_service] = object
    api.dependency_overrides[get_enrichment_orchestrator] = (
        lambda: orchestrator
    )
    api.dependency_overrides[get_deterministic_provider_runtime] = (
        ControlledDeterministicRuntime
    )
    api.dependency_overrides[get_lifecycle_due_resolver] = object

    with TestClient(api) as client:
        response = client.get(
            "/market-context/mnq"
            "?refresh=force&view=consumer"
            "&audience=senior_analyst_v1"
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["request"]["same_request_provider_accounting"] is False
    assert len(payload["provider_accounting"]) == len(DATASET_POLICIES)
    assert set(provider.calls) == {
        policy.dataset_id for policy in DATASET_POLICIES
    }
    validation = validate_senior_analyst_payload_v1(
        payload,
        require_recent_response=True,
    )
    assert validation["checks"]["provider_accounting_valid"] is False


def test_one_missing_dataset_keeps_live_gate_closed() -> None:
    collector = _collector()
    for policy in DATASET_POLICIES[:-1]:
        _record_observed_call(
            collector,
            policy=policy,
            source=f"CONTROLLED:{policy.dataset_id}",
        )
    source = {
        "symbol": "MNQ",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "sections": {},
        "request_scoped_provider_accounting": collector.manifest(),
    }
    payload = build_senior_analyst_payload_v1(
        source,
        request_id="runtime-accounting",
        request_refresh_mode="force",
    )

    result = validate_senior_analyst_payload_v1(
        payload,
        require_recent_response=True,
    )

    assert result["status"] == "FAIL"
    assert result["checks"]["provider_accounting_valid"] is False


def _flash_contract(*, actual=None, source=None) -> dict:
    return {
        "event_calendar": {
            "critical_macro_events": [
                {
                    "occurrence_id": "xtb:146945:2026-07-24",
                    "name": "Flash Services PMI",
                    "metric_id": "flash_services_pmi",
                    "reference_period": "2026-07",
                    "release_at": "2026-07-24T13:45:00+00:00",
                    "actual": actual,
                    "actual_source": source,
                }
            ]
        }
    }


def test_flash_pmi_runtime_evidence_preserves_fallback_order_and_selection() -> None:
    policy = next(
        item
        for item in DATASET_POLICIES
        if item.dataset_id == "flash_services_pmi"
    )
    collector = _collector([policy])
    service = object.__new__(ProviderForceActualReconciliationService)
    service.accounting_collector = collector
    service.lifecycle = type(
        "EmptyLifecycleRepository",
        (),
        {"list_items": lambda _self: []},
    )()

    service._record_flash_pmi_accounting(
        contract=_flash_contract(
            actual=53.6,
            source="INVESTING_EVENT_1062",
        ),
        audits=[
            {
                "occurrence_id": "xtb:146945:2026-07-24",
                "mapping_selected": "flash_services_pmi",
                "provider_call_count": 2,
                "lifecycle_before": {
                    "freshness_state": "STALE",
                    "valid_until": (
                        datetime.now(UTC) - timedelta(minutes=5)
                    ).isoformat(),
                    "next_refresh_at": (
                        datetime.now(UTC) - timedelta(minutes=5)
                    ).isoformat(),
                },
                "provider_attempts": [
                    {
                        "provider": "SPGLOBAL",
                        "attempts": 1,
                        "result": "HTTP_403",
                    },
                    {
                        "provider": "INVESTING_EVENT_1062",
                        "attempts": 1,
                        "result": "SUCCESS",
                    },
                ],
            }
        ],
    )

    manifest = collector.manifest()
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    row = manifest["datasets"][0]
    assert row["primary_provider"]["called"] is True
    assert [item["provider"] for item in row["fallbacks"]] == [
        "INVESTING_EVENT_1062"
    ]
    assert row["fallbacks"][0]["called"] is True
    assert row["acquisition_selected_source"] == "INVESTING_EVENT_1062"


def test_flash_pmi_all_provider_failures_leave_value_null() -> None:
    policy = next(
        item
        for item in DATASET_POLICIES
        if item.dataset_id == "flash_services_pmi"
    )
    collector = _collector([policy])
    service = object.__new__(ProviderForceActualReconciliationService)
    service.accounting_collector = collector
    service.lifecycle = type(
        "EmptyLifecycleRepository",
        (),
        {"list_items": lambda _self: []},
    )()

    service._record_flash_pmi_accounting(
        contract=_flash_contract(),
        audits=[
            {
                "occurrence_id": "xtb:146945:2026-07-24",
                "mapping_selected": "flash_services_pmi",
                "provider_call_count": 2,
                "lifecycle_before": {
                    "freshness_state": "STALE",
                    "valid_until": (
                        datetime.now(UTC) - timedelta(minutes=5)
                    ).isoformat(),
                    "next_refresh_at": (
                        datetime.now(UTC) - timedelta(minutes=5)
                    ).isoformat(),
                },
                "provider_attempts": [
                    {
                        "provider": "SPGLOBAL",
                        "attempts": 1,
                        "result": "HTTP_403",
                    },
                    {
                        "provider": "INVESTING_EVENT_1062",
                        "attempts": 1,
                        "result": "TIMEOUT",
                    },
                ],
            }
        ],
    )

    row = collector.manifest()["datasets"][0]
    assert row["primary_provider"]["called"] is True
    assert row["fallbacks"][0]["called"] is True
    assert row["database_record_found"] is True
    assert row["database_record_expired"] is True
    assert row["database_freshness_evaluation"] != "VALID"
    assert row["acquisition_selected_source"] is None
    assert row["acquisition_reason_code"] == (
        "FLASH_SERVICES_PMI_VALUE_NOT_AVAILABLE"
    )
