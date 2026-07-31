from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import ProviderError, metadata
from app.providers.sp_global_pmi import SERIES_ID, SpGlobalPmiProvider
from app.services.deterministic_actual_resolver import DeterministicActualResolver
from app.services.event_driven_lifecycle_service import DatumLifecycle
from app.services.lifecycle_due_resolver import (
    MacroActualLifecycleProviderAdapter,
    _official_actual_datum,
)
from app.services.official_actual_semantics import normalize_reference_period
from app.services.provider_force_actual_reconciliation_service import (
    ProviderForceActualReconciliationService,
    _persisted_actual_field_lineage,
    _same_acquisition_identity,
)


RELEASED = datetime(2026, 7, 24, 14, 0, tzinfo=UTC)
NOW = datetime(2026, 7, 28, 8, 0, tzinfo=UTC)


def settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "environment": "test",
        "database_path": tmp_path / "pr28.sqlite",
        "source_policy_path": Path(__file__).resolve().parents[1] / "config" / "source_policy.json",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


class OfficialFred:
    source = "FRED"

    async def fetch(self, *, series_ids):
        assert series_ids == ["HSN1F"]
        return ProviderResult(
            metadata=metadata(
                source="FRED",
                provider_type=ProviderType.API,
                reliability=0.98,
                freshness=Freshness.RECENT,
            ),
            data={
                "HSN1F": {
                    "series_id": "HSN1F",
                    "official_adapter": True,
                    "provider_adapter": "FRED_OFFICIAL_API",
                    "source": "FRED",
                    "source_url": "https://fred.stlouisfed.org/series/HSN1F",
                    "canonical_url": "https://fred.stlouisfed.org/series/HSN1F",
                    "source_domain": "fred.stlouisfed.org",
                    "frequency": "monthly",
                    "units": "thousands_annual_rate",
                    "seasonal_adjustment": "SAAR",
                    "observations": [
                        {"period": "2026-05", "value": "618"},
                        {"period": "2026-06", "value": "628"},
                    ],
                }
            },
        )


def test_new_home_sales_exact_occurrence_uses_official_series_and_localized_period(
    tmp_path: Path,
) -> None:
    resolver = DeterministicActualResolver(
        settings(tmp_path), providers={"FRED": OfficialFred()}
    )
    result = resolver.resolve_event(
        event_key="xtb:146392:2026-07-24",
        event={
            "event_id": "xtb:146392:2026-07-24",
            "name": "Vendita case nuove",
            "reference_period": "Giugno",
        },
        temporal_state={"release_at": RELEASED.isoformat()},
    )
    assert result["status"] == "SUCCEEDED"
    candidate = result["results"][0]
    assert (candidate["value"], candidate["previous"], candidate["reference_period"]) == (
        "628",
        "618",
        "2026-06",
    )
    assert candidate["source_series_id"] == "HSN1F"
    assert candidate["provider_adapter"] == "FRED_OFFICIAL_API"


def test_official_derived_actual_preserves_series_and_transformation() -> None:
    occurrence_id = "xtb:145296:2026-07-30"
    datum = _official_actual_datum(
        {
            "occurrence_id": occurrence_id,
            "name": "PCE A/A",
            "frequency": "monthly",
        },
        candidate={
            "value": "2.6",
            "previous": "2.5",
            "event_metric_id": "headline_pce_yoy",
            "source_series_id": "BEA:PCE_PRICE_INDEX",
            "transformation": "pct_change_yoy",
            "reference_period": "2026-06",
            "previous_reference_period": "2026-05",
            "frequency": "monthly",
            "unit": "percent",
            "source": "BEA",
            "publisher": "Bureau of Economic Analysis",
            "source_url": (
                "https://apps.bea.gov/iTable/?reqid=19&step=2"
            ),
            "validation_status": "accepted",
        },
        canonical_key=occurrence_id,
        release=datetime(2026, 7, 30, 12, 30, tzinfo=UTC),
    )

    actual = datum["enrichment"]["field_lineage"]["actual"]
    assert actual["occurrence_id"] == occurrence_id
    assert actual["value"] == "2.6"
    assert actual["source_series_id"] == "BEA:PCE_PRICE_INDEX"
    assert actual["transformation"] == "pct_change_yoy"
    previous = datum["enrichment"]["field_lineage"]["previous"]
    assert previous["occurrence_id"] == occurrence_id
    assert previous["value"] == "2.5"
    assert previous["previous_reference_period"] == "2026-05"


@pytest.mark.parametrize(
    ("localized", "expected"),
    [("Giugno", "2026-06"), ("Luglio", "2026-07")],
)
def test_localized_month_normalization_is_host_locale_independent(
    localized: str, expected: str
) -> None:
    assert normalize_reference_period(
        localized, frequency="monthly", release_date=RELEASED
    ) == expected


@pytest.mark.asyncio
async def test_sp_global_release_parser_preserves_actual_previous_and_lineage(
    tmp_path: Path,
) -> None:
    html = """
    <html><body>
    Flash US Services PMI Business Activity Index: 53.6 (June: 51.2)
    </body></html>
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, request=request)

    cfg = settings(
        tmp_path,
        sp_global_pmi_release_url=(
            "https://www.pmi.spglobal.com/Public/Home/PressRelease/fixture-id"
        ),
    )
    provider = SpGlobalPmiProvider(
        ProviderCacheRepository(cfg.database_path),
        cfg,
        transport=httpx.MockTransport(handler),
    )
    result = await provider.fetch(
        expected_period="2026-07", release_date="2026-07-24"
    )
    series = result.data[SERIES_ID]
    assert [row["value"] for row in series["observations"]] == ["51.2", "53.6"]
    assert [row["period"] for row in series["observations"]] == ["2026-06", "2026-07"]
    assert series["raw_lineage_redacted"]["content_sha256"]
    assert series["source_url"].startswith("https://www.pmi.spglobal.com/")


@pytest.mark.asyncio
async def test_sp_global_access_restriction_and_period_mismatch_fail_closed(
    tmp_path: Path,
) -> None:
    async def forbidden(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, request=request)

    cfg = settings(
        tmp_path,
        sp_global_pmi_release_url=(
            "https://www.pmi.spglobal.com/Public/Home/PressRelease/restricted"
        ),
    )
    provider = SpGlobalPmiProvider(
        ProviderCacheRepository(cfg.database_path),
        cfg,
        transport=httpx.MockTransport(forbidden),
    )
    with pytest.raises(ProviderError, match="access_restricted"):
        await provider.fetch(expected_period="2026-07", release_date="2026-07-24")


def test_flash_services_exact_occurrence_accepts_only_official_sp_global_candidate(
    tmp_path: Path,
) -> None:
    html = (
        "Flash US Services PMI Business Activity Index: "
        "53.6 (June: 51.2)"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, request=request)

    cfg = settings(
        tmp_path,
        sp_global_pmi_release_url=(
            "https://www.pmi.spglobal.com/Public/Home/PressRelease/official"
        ),
    )
    provider = SpGlobalPmiProvider(
        ProviderCacheRepository(cfg.database_path),
        cfg,
        transport=httpx.MockTransport(handler),
    )
    result = DeterministicActualResolver(
        cfg, providers={"SPGLOBAL": provider}
    ).resolve_event(
        event_key="xtb:146945:2026-07-24",
        event={
            "event_id": "xtb:146945:2026-07-24",
            "reference_period": "Luglio",
        },
        temporal_state={"release_at": "2026-07-24T13:45:00+00:00"},
    )
    assert result["status"] == "SUCCEEDED"
    candidate = result["results"][0]
    assert (
        candidate["value"],
        candidate["previous"],
        candidate["reference_period"],
        candidate["validation_status"],
    ) == ("53.6", "51.2", "2026-07", "accepted")
    assert candidate["raw_lineage_redacted"]["content_sha256"]


def test_stale_calendar_can_resolve_by_stable_occurrence_without_reappearing(
    tmp_path: Path,
) -> None:
    class EmptyCalendar:
        last_provider_results = []

        async def list_events(self, **kwargs):
            del kwargs
            return []

    class OfficialResolver:
        def resolve_event(self, **kwargs):
            assert kwargs["event_key"] == "xtb:146945:2026-07-24"
            return {
                "status": "SUCCEEDED",
                "results": [
                    {
                        "field": "actual",
                        "value": "53.6",
                        "previous": "51.2",
                        "event_metric_id": "flash_services_pmi",
                        "reference_period": "2026-07",
                        "previous_reference_period": "2026-06",
                        "frequency": "monthly",
                        "transformation": "level",
                        "unit": "index_points",
                        "source": "SPGLOBAL",
                        "publisher": "S&P Global Market Intelligence",
                        "source_url": "https://www.pmi.spglobal.com/Public/Home/PressRelease/id",
                        "canonical_url": "https://www.pmi.spglobal.com/Public/Home/PressRelease/id",
                        "provider_adapter": "SPGLOBAL_OFFICIAL_API",
                        "retrieved_at": NOW.isoformat(),
                        "validation_timestamp": NOW.isoformat(),
                    }
                ],
            }

    adapter = MacroActualLifecycleProviderAdapter(
        settings=settings(tmp_path, event_calendar_catchup_enabled=True),
        event_service=EmptyCalendar(),
        actual_resolver=OfficialResolver(),
        clock=lambda: NOW,
    )
    result = adapter.resolve(
        {
            "entity_key": "xtb:146945:2026-07-24",
            "event_at": "2026-07-24T13:45:00+00:00",
            "payload": {
                "event_id": "xtb:146945:2026-07-24",
                "occurrence_id": "xtb:146945:2026-07-24",
                "name": "Indice PMI dei servizi",
                "reference_period": "Luglio",
                "frequency": "monthly",
                "forecast": 51.5,
                "previous": 51.2,
                "release_at": "2026-07-24T13:45:00+00:00",
            },
            "fields_attempted": ["actual"],
        }
    )
    assert result["status"] == "RESOLVED"
    assert result["datum"]["canonical_event_key"] == "xtb:146945:2026-07-24"
    assert result["datum"]["actual"] == "53.6"
    assert result["datum"]["previous"] == "51.2"
    assert result["datum"]["forecast"] == 51.5
    assert result["datum"]["reference_period"] == "2026-07"
    assert result["datum"]["actual_is_official"] is True
    field_lineage = result["datum"]["enrichment"]["field_lineage"]
    assert field_lineage["actual"]["occurrence_id"] == (
        "xtb:146945:2026-07-24"
    )
    assert field_lineage["actual"]["value"] == "53.6"
    assert field_lineage["actual"]["field_semantics"] == "actual"
    assert field_lineage["actual"]["transformation"] == "level"
    assert field_lineage["previous"]["occurrence_id"] == (
        "xtb:146945:2026-07-24"
    )
    assert field_lineage["previous"]["value"] == "51.2"
    assert field_lineage["previous"]["previous_reference_period"] == (
        "2026-06"
    )


HOME_OCCURRENCE = "xtb:146392:2026-07-24"


def _new_home_persisted_payload(
    *,
    audit_occurrence: str = HOME_OCCURRENCE,
    complete_lineage: bool = True,
    lineage_source_url: str = (
        "https://fred.stlouisfed.org/series/HSN1F"
    ),
) -> dict:
    lineage = {
        "occurrence_id": HOME_OCCURRENCE,
        "source": "FRED",
        "publisher": "FRED",
        "source_url": lineage_source_url,
        "source_field": "actual",
        "source_series_id": "HSN1F",
        "metric_id": "new_home_sales",
        "frequency": "monthly",
        "transformation": "level",
        "reference_period": "2026-06",
        "value": "628",
        "freshness": "CURRENT_RELEASE",
        "content_valid_until": (
            NOW + timedelta(days=30)
        ).isoformat(),
        "refresh_due_at": (
            NOW + timedelta(days=20)
        ).isoformat(),
        "validation_status": "accepted",
    }
    if not complete_lineage:
        for field in (
            "transformation",
            "content_valid_until",
            "refresh_due_at",
        ):
            lineage.pop(field)
    return {
        "occurrence_id": HOME_OCCURRENCE,
        "event_id": HOME_OCCURRENCE,
        "name": "New Home Sales",
        "country": "US",
        "metric_id": "new_home_sales",
        "frequency": "monthly",
        "reference_period": "2026-06",
        "release_at": RELEASED.isoformat(),
        "actual": "628",
        "actual_source": "FRED",
        "actual_source_url": (
            "https://fred.stlouisfed.org/series/HSN1F"
        ),
        "actual_is_official": True,
        "freshness_state": "CURRENT_RELEASE",
        "content_valid_until": (
            NOW + timedelta(days=30)
        ).isoformat(),
        "refresh_due_at": (
            NOW + timedelta(days=20)
        ).isoformat(),
        "source_lineage": [lineage],
        "actual_resolution": {
            "occurrence_id": audit_occurrence,
            "mapping_selected": "new_home_sales",
            "provider_attempted": "FRED",
            "provider_call_count": 1,
            "provider_attempts": [
                {
                    "provider": "FRED",
                    "called": True,
                    "result": "SUCCESS",
                }
            ],
            "actual_still_missing": False,
        },
    }


def _reuse_service(
    payload: dict,
    *,
    resolver=None,
) -> ProviderForceActualReconciliationService:
    service = object.__new__(ProviderForceActualReconciliationService)
    service.clock = lambda: NOW
    service.lifecycle = SimpleNamespace(
        list_items=lambda: [
            {
                "entity_type": "macro_actual",
                "entity_key": HOME_OCCURRENCE,
                "freshness_state": "CURRENT_RELEASE",
                "work_status": "COMPLETED",
                "payload": payload,
            }
        ]
    )
    service.facts = SimpleNamespace(
        economic_event_records=lambda **_kwargs: []
    )
    service.lifecycle_resolver = SimpleNamespace(
        resolve=(
            resolver
            if resolver is not None
            else lambda _item: pytest.fail(
                "valid canonical actual must not call a provider"
            )
        )
    )
    service.accounting_collector = None
    service.force_refresh = True
    service.request_id = "request-new-home"
    service.correlation_id = "request-new-home"
    service.coverage_write_count = 0
    service.generation_id = "generation-new-home"
    service.telemetry = SimpleNamespace(emit=lambda *_args, **_kwargs: None)
    return service


def _fresh_new_home_lifecycle() -> DatumLifecycle:
    return DatumLifecycle(
        entity_type="macro_actual",
        entity_key=HOME_OCCURRENCE,
        trigger_class="TRIGGER",
        freshness_state="FRESH",
        observed_at=NOW.isoformat(),
        data_as_of="2026-06-01T00:00:00+00:00",
        published_at=RELEASED.isoformat(),
        event_at=RELEASED.isoformat(),
        valid_from=RELEASED.isoformat(),
        valid_until=(NOW + timedelta(days=30)).isoformat(),
        next_refresh_at=(NOW + timedelta(days=20)).isoformat(),
        next_retry_at=None,
        superseded_by=None,
        refresh_reason="official_macro_actual_resolved",
        materiality_fingerprint="new-home-provider-test",
        source_lineage=(),
        acquisition_method="api_provider",
        retry_class=None,
        retry_policy={},
        negative_cache_key=None,
        negative_cache_expires_at=None,
        session_state=None,
        triggering_event=None,
        fields_attempted=("actual",),
        attempt_count=1,
    )


def test_official_lineage_must_match_candidate_acquisition_provider() -> None:
    candidate = {
        "source": "FRED",
        "source_series_id": "HSN1F",
        "transformation": "level",
        "frequency": "monthly",
        "reference_period": "2026-06",
        "acquisition_provider": "FRED",
        "distribution_source": "FRED",
    }
    lineage = {
        "source": "FRED",
        "source_series_id": "HSN1F",
        "transformation": "level",
        "frequency": "monthly",
        "reference_period": "2026-06",
        "acquisition_provider": "UNRELATED_PROVIDER",
        "distributor": "UNRELATED_PROVIDER",
    }

    assert _same_acquisition_identity(
        lineage,
        evidence=candidate,
        candidate=candidate,
        field="actual",
    ) is False


def test_current_registered_pmi_fallback_lineage_is_reusable() -> None:
    valid_until = (NOW + timedelta(days=30)).isoformat()
    refresh_due_at = (NOW + timedelta(days=20)).isoformat()
    payload = {
        "occurrence_id": "xtb:pmi:2026-07",
        "actual": 53.6,
        "actual_source": "INVESTING_EVENT_1062",
        "actual_is_official": False,
        "reference_period": "2026-07",
        "source_lineage": [
            {
                "occurrence_id": "xtb:pmi:2026-07",
                "source_field": "actual",
                "metric_id": "flash_services_pmi",
                "value": 53.6,
                "source": "S&P Global",
                "publisher": "S&P Global",
                "acquisition_provider": "INVESTING_EVENT_1062",
                "distributor": "Investing.com",
                "source_url": (
                    "https://endpoints.investing.com/pd-instruments/"
                    "v1/calendars/economic/events/1062/occurrences"
                ),
                "canonical_url": (
                    "https://www.pmi.spglobal.com/Public/"
                    "Home/PressRelease"
                ),
                "source_series_id": (
                    "SPGLOBAL:US:FLASH_SERVICES_PMI"
                ),
                "transformation": "level",
                "frequency": "monthly",
                "reference_period": "2026-07",
                "validation_status": "accepted",
                "freshness": "CURRENT_RELEASE",
                "content_valid_until": valid_until,
                "refresh_due_at": refresh_due_at,
            }
        ],
    }

    lineage = _persisted_actual_field_lineage(
        payload,
        persisted_audit={
            "occurrence_id": "xtb:pmi:2026-07",
            "mapping_selected": "flash_services_pmi",
        },
        occurrence_id="xtb:pmi:2026-07",
        mapping_selected="flash_services_pmi",
        delivered_actual=53.6,
        now=NOW,
    )

    assert lineage is not None
    assert lineage["acquisition_provider"] == "INVESTING_EVENT_1062"


def _fresh_new_home_resolver(calls: list[dict]):
    def resolve(item: dict) -> dict:
        calls.append(item)
        candidate = {
            "value": "628",
            "actual": "628",
            "event_metric_id": "new_home_sales",
            "metric_id": "new_home_sales",
            "source_series_id": "HSN1F",
            "transformation": "level",
            "reference_period": "2026-06",
            "frequency": "monthly",
            "unit": "thousands_annual_rate",
            "source": "FRED",
            "publisher": "FRED",
            "acquisition_provider": "FRED",
            "distribution_source": "FRED",
            "source_url": (
                "https://fred.stlouisfed.org/series/HSN1F"
            ),
            "validation_status": "accepted",
        }
        datum = _official_actual_datum(
            item["payload"],
            candidate=candidate,
            canonical_key=HOME_OCCURRENCE,
            release=RELEASED,
        )
        return {
            "status": "RESOLVED",
            "reason": "official_macro_actual_resolved",
            "reason_code": "OFFICIAL_ACTUAL_RESOLVED",
            "datum": datum,
            "candidate": candidate,
            "candidate_validation": "accepted",
            "provider": "FRED",
            "source_series": "HSN1F",
            "provider_call_count": 1,
            "provider_request_attempted": True,
            "provider_attempts": [
                {
                    "provider": "FRED",
                    "called": True,
                    "attempts": 1,
                    "result": "SUCCESS",
                }
            ],
            "lifecycle": _fresh_new_home_lifecycle(),
        }

    return resolve


def _new_home_contract() -> dict:
    return {
        "event_calendar": {
            "critical_macro_events": [
                {
                    "occurrence_id": HOME_OCCURRENCE,
                    "event_id": HOME_OCCURRENCE,
                    "name": "New Home Sales",
                    "country": "US",
                    "metric_id": "new_home_sales",
                    "frequency": "monthly",
                    "reference_period": "2026-06",
                    "release_at": RELEASED.isoformat(),
                    "actual": 628,
                    "actual_source": "FRED",
                    "actual_is_official": True,
                }
            ]
        }
    }


def test_valid_db_reuse_restores_exact_occurrence_actual_lineage() -> None:
    prepared = _reuse_service(
        _new_home_persisted_payload()
    ).prepare(_new_home_contract())

    event = prepared["contract"]["event_calendar"][
        "critical_macro_events"
    ][0]
    actual_lineage = event["enrichment"]["field_lineage"][
        "actual"
    ]
    assert actual_lineage["occurrence_id"] == HOME_OCCURRENCE
    assert actual_lineage["value"] == "628"
    assert actual_lineage["source"] == "FRED"
    assert actual_lineage["source_series_id"] == "HSN1F"
    assert actual_lineage["metric_id"] == "new_home_sales"
    assert actual_lineage["frequency"] == "monthly"
    assert actual_lineage["transformation"] == "level"
    assert actual_lineage["freshness"] == "CURRENT_RELEASE"
    assert actual_lineage["validation_status"] == "accepted"
    assert actual_lineage["content_valid_until"] == (
        NOW + timedelta(days=30)
    ).isoformat()
    assert actual_lineage["refresh_due_at"] == (
        NOW + timedelta(days=20)
    ).isoformat()
    projected = prepared["contract"]["macro_actuals"]["items"][0]
    assert projected["actual"] == 628
    assert projected["actual_source"] == "FRED"
    assert projected["field_lineage"]["actual"] == actual_lineage


def test_incomplete_db_actual_lineage_enters_provider_resolution() -> None:
    calls: list[dict] = []
    prepared = _reuse_service(
        _new_home_persisted_payload(complete_lineage=False),
        resolver=_fresh_new_home_resolver(calls),
    ).prepare(_new_home_contract())

    event = prepared["contract"]["event_calendar"][
        "critical_macro_events"
    ][0]
    assert len(calls) == 1
    assert calls[0]["payload"]["actual"] is None
    assert calls[0]["payload"]["actual_source"] is None
    assert calls[0]["payload"]["actual_is_official"] is None
    assert calls[0]["payload"]["source_lineage"] == []

    actual_lineage = event["enrichment"]["field_lineage"]["actual"]
    assert event["actual"] == "628"
    assert actual_lineage["occurrence_id"] == HOME_OCCURRENCE
    assert actual_lineage["value"] == "628"
    assert actual_lineage["source"] == "FRED"
    assert actual_lineage["transformation"] == "level"
    assert actual_lineage["freshness"] == "CURRENT_RELEASE"
    assert actual_lineage["freshness_state"] == "CURRENT_RELEASE"
    assert actual_lineage["validation_status"] == "accepted"
    assert actual_lineage["content_valid_until"] == (
        NOW + timedelta(days=30)
    ).isoformat()
    assert actual_lineage["refresh_due_at"] == (
        NOW + timedelta(days=20)
    ).isoformat()

    audit = prepared["audit"]["occurrences"][0]
    assert audit["occurrence_id"] == HOME_OCCURRENCE
    assert audit["resolver_invoked"] is True
    assert prepared["audit"]["resolver_invocation_count"] == 1
    assert audit["reclaim_reason"] == (
        "CANONICAL_ACTUAL_FIELD_EVIDENCE_INCOMPLETE"
    )


def test_wrong_source_domain_db_actual_enters_provider_resolution() -> None:
    calls: list[dict] = []
    prepared = _reuse_service(
        _new_home_persisted_payload(
            lineage_source_url="https://evil.example/series/HSN1F",
        ),
        resolver=_fresh_new_home_resolver(calls),
    ).prepare(_new_home_contract())

    assert len(calls) == 1
    assert calls[0]["payload"]["actual"] is None
    event = prepared["contract"]["event_calendar"][
        "critical_macro_events"
    ][0]
    assert event["actual"] == "628"
    assert prepared["audit"]["occurrences"][0]["reclaim_reason"] == (
        "CANONICAL_ACTUAL_FIELD_EVIDENCE_INCOMPLETE"
    )


def test_nested_expired_db_actual_enters_provider_resolution() -> None:
    calls: list[dict] = []
    payload = _new_home_persisted_payload()
    payload["source_lineage"][0]["lifecycle"] = {
        "status": "EXPIRED",
        "content_valid_until": (
            NOW - timedelta(minutes=1)
        ).isoformat(),
        "refresh_due_at": (
            NOW - timedelta(minutes=1)
        ).isoformat(),
    }

    prepared = _reuse_service(
        payload,
        resolver=_fresh_new_home_resolver(calls),
    ).prepare(_new_home_contract())

    assert len(calls) == 1
    assert calls[0]["payload"]["actual"] is None
    assert prepared["audit"]["occurrences"][0]["reclaim_reason"] == (
        "CANONICAL_ACTUAL_FIELD_EVIDENCE_INCOMPLETE"
    )


def test_db_actual_audit_from_another_occurrence_is_not_reused() -> None:
    calls: list[dict] = []
    prepared = _reuse_service(
        _new_home_persisted_payload(
            audit_occurrence="xtb:other:2026-07-24"
        ),
        resolver=_fresh_new_home_resolver(calls),
    ).prepare(_new_home_contract())

    assert len(calls) == 1
    event = prepared["contract"]["event_calendar"][
        "critical_macro_events"
    ][0]
    actual_lineage = event["enrichment"]["field_lineage"]["actual"]
    assert actual_lineage["occurrence_id"] == HOME_OCCURRENCE
    assert prepared["audit"]["occurrences"][0][
        "resolver_invoked"
    ] is True
    assert prepared["audit"]["resolver_invocation_count"] == 1
