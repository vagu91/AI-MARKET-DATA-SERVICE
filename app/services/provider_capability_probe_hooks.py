from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from app.core.config import Settings
from app.services.provider_capability_audit import (
    HealthStatus,
    ProbeExecutionError,
    ProbeRequest,
)


class LocalCapabilityProbeHook:
    """Calls registered repositories and pure transforms on audit-only inputs."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def audit_probe(self, request: ProbeRequest) -> Any:
        provider_id = request.provider_id
        adapter_path = str(_value(request.registration, "adapter_path", "")).strip()
        adapter_type = _load_symbol(adapter_path)
        current = datetime.now(UTC).replace(microsecond=0)

        if provider_id == "PROVIDER_CACHE_REPOSITORY":
            repository = adapter_type(self.settings.database_path)
            cache_key = (
                "provider-capability-audit:"
                f"{request.run_id}:controlled-entry"
            )
            valid_until = current + timedelta(minutes=30)
            repository.set(
                cache_key,
                {
                    "fixture": "LOCAL_SANDBOX_CONTROLLED_ENTRY",
                    "run_id": request.run_id,
                },
                provider_name="PROVIDER_CAPABILITY_AUDIT",
                valid_until=valid_until.isoformat(),
                stale_until=(current + timedelta(hours=1)).isoformat(),
                status="valid_cache",
            )
            entry = repository.get_entry(cache_key)
            return _normalized_local_probe_result(
                request,
                current=current,
                raw_output=entry,
                fields_by_metric={
                    "provider_cache_entry": {
                        field_name: _required_field(
                            entry,
                            field_name,
                            provider_id=provider_id,
                        )
                        for field_name in request.targets[0].fields
                    }
                },
                fixture_id="CONTROLLED_PROVIDER_CACHE_SET_AND_GET",
            )
        if provider_id == "MARKET_FACT_REPOSITORY":
            from app.services.provider_capability_registry import (
                MARKET_FACT_REPOSITORY_DATASET_QUERIES,
            )

            repository = adapter_type(self.settings)
            dataset_lookups: list[dict[str, Any]] = []
            for target in request.targets:
                dataset_id = str(_value(target, "dataset_id", "")).strip()
                metric_id = str(_value(target, "metric_id", "")).strip()
                query = MARKET_FACT_REPOSITORY_DATASET_QUERIES.get(
                    dataset_id
                )
                if query is None:
                    raise ProbeExecutionError(
                        (
                            "market fact repository target has no explicit "
                            "dataset query"
                        ),
                        status=HealthStatus.UNUSABLE,
                        reason_code=(
                            "MARKET_FACT_DATASET_QUERY_NOT_REGISTERED"
                        ),
                    )
                rows = [
                    row
                    for fact_type in query.fact_types
                    for row in repository.get_valid_facts_by_type(
                        fact_type,
                        allow_stale=True,
                    )
                ]
                matching_rows = _matching_market_fact_rows(
                    rows,
                    series_ids=query.series_ids,
                )
                records = [
                    _market_fact_probe_record(row)
                    for row in matching_rows
                ]
                dataset_lookups.append(
                    {
                        "dataset_id": dataset_id,
                        "metric_id": metric_id,
                        "frequency": query.frequency,
                        "database_lookup_performed": True,
                        "database_record_found": bool(records),
                        "database_record_count": len(records),
                        "queried_fact_types": list(query.fact_types),
                        "queried_series_ids": list(query.series_ids),
                        "records": records,
                        "reason_code": (
                            "CANONICAL_DATASET_RECORD_FOUND"
                            if records
                            else "CANONICAL_DATASET_RECORD_NOT_FOUND"
                        ),
                    }
                )
            return {
                "repository": provider_id,
                "lookup_scope": "DATASET_SPECIFIC_CANONICAL_OBSERVATION",
                "dataset_lookups": dataset_lookups,
            }
        if provider_id == "MARKET_NEWS_REPOSITORY":
            return {"items": adapter_type(self.settings).current(days=7, limit=1)}
        if provider_id == "FED_EXPECTATIONS_REPOSITORY":
            return {"latest": adapter_type(self.settings).latest()}
        if provider_id == "RISK_CONTEXT_REPOSITORY":
            return {"latest": adapter_type(self.settings).latest()}
        if provider_id == "EVENT_CALENDAR_COVERAGE_REPOSITORY":
            return adapter_type(self.settings).matrix(
                start_date=current.date(),
                end_date=(current + timedelta(days=7)).date(),
                provider_name="PROVIDER_CAPABILITY_AUDIT",
                query_scope="AUDIT_READ_ONLY_SNAPSHOT",
                now=current,
            )
        if provider_id == "MARKET_CONTEXT_SNAPSHOT_REPOSITORY":
            repository = adapter_type(self.settings)
            snapshot_id = (
                "provider-audit-"
                f"{_safe_identifier(request.run_id)}"
            )
            saved = repository.save(
                snapshot_id=snapshot_id,
                revision=1,
                symbol="MNQ",
                refresh_mode="provider_capability_audit",
                debug_payload={
                    "generated_at_utc": current.isoformat(),
                    "symbol": "MNQ",
                    "fixture_scope": "LOCAL_SANDBOX_CONTROLLED_SNAPSHOT",
                },
                consumer_payload={
                    "data_as_of": current.isoformat(),
                    "symbol": "MNQ",
                    "fixture_scope": "LOCAL_SANDBOX_CONTROLLED_SNAPSHOT",
                },
                ai_status="NOT_REQUIRED",
            )
            latest = repository.latest("MNQ")
            if (
                latest is None
                or latest.get("snapshot_id") != saved.get("snapshot_id")
                or latest.get("checksum") != saved.get("checksum")
            ):
                raise ProbeExecutionError(
                    "controlled market-context snapshot read-back failed",
                    status=HealthStatus.UNUSABLE,
                    reason_code="LOCAL_SNAPSHOT_READ_BACK_FAILED",
                )
            return _normalized_local_probe_result(
                request,
                current=current,
                raw_output=latest,
                fields_by_metric={
                    "immutable_market_context_snapshot": {
                        "snapshot_id": _required_field(
                            latest,
                            "snapshot_id",
                            provider_id=provider_id,
                        ),
                        "snapshot_revision": _required_field(
                            latest,
                            "revision",
                            provider_id=provider_id,
                        ),
                        "generated_at": _required_field(
                            latest,
                            "generated_at",
                            provider_id=provider_id,
                        ),
                        "checksum": _required_field(
                            latest,
                            "checksum",
                            provider_id=provider_id,
                        ),
                    }
                },
                fixture_id="CONTROLLED_IMMUTABLE_SNAPSHOT_SAVE_AND_READ",
            )
        if provider_id == "CANONICAL_EVENT_REPOSITORY":
            return {
                "accepted_official_actual": adapter_type(
                    self.settings
                ).accepted_official_actual("provider-audit:missing-occurrence")
            }
        if provider_id == "LEGACY_EARNINGS_AGGREGATOR":
            from app.infrastructure.persistence.provider_cache_repository import (
                ProviderCacheRepository,
            )
            from app.models.common import (
                Freshness,
                ProviderMetadata,
                ProviderResult,
                ProviderType,
            )

            event_date = (current + timedelta(days=7)).date().isoformat()
            event = {
                "symbol": "NVDA",
                "event_date": event_date,
                "timing": "UNKNOWN",
                "eps_estimate": 1.23,
                "revenue_estimate": 42_000_000_000.0,
                "data_as_of": current.isoformat(),
                "content_valid_until": (
                    current + timedelta(hours=24)
                ).isoformat(),
                "refresh_due_at": (
                    current + timedelta(hours=24)
                ).isoformat(),
                "source": "Financial Modeling Prep Earnings Calendar",
                "publisher": "Financial Modeling Prep",
                "distributor": "Financial Modeling Prep",
                "acquisition_provider": "FMP_EARNINGS_CALENDAR",
                "source_url": (
                    "https://financialmodelingprep.com/stable/"
                    "earnings-calendar"
                ),
                "lineage": [
                    {
                        "field": field_name,
                        "acquisition_provider": "FMP_EARNINGS_CALENDAR",
                    }
                    for field_name in request.targets[0].fields
                ],
            }

            class ControlledFmpProvider:
                calls = 0

                def __init__(self, cache: Any, settings: Settings) -> None:
                    del cache, settings

                async def fetch(self) -> ProviderResult:
                    type(self).calls += 1
                    return ProviderResult(
                        metadata=ProviderMetadata(
                            source=(
                                "Financial Modeling Prep Earnings Calendar"
                            ),
                            provider_type=ProviderType.API,
                            retrieved_at=current,
                            data_as_of=current,
                            freshness=Freshness.RECENT,
                            reliability=0.82,
                        ),
                        data={"status": "found", "events": [event]},
                    )

            wrapper = adapter_type(
                ProviderCacheRepository(self.settings.database_path),
                self.settings,
                fmp_provider_factory=ControlledFmpProvider,
            )
            result = await wrapper.fetch()
            events = (
                result.data.get("events")
                if isinstance(result.data, Mapping)
                else None
            )
            if (
                ControlledFmpProvider.calls != 1
                or not isinstance(events, list)
                or len(events) != 1
                or not isinstance(events[0], Mapping)
                or events[0].get("acquisition_provider")
                != "FMP_EARNINGS_CALENDAR"
            ):
                raise ProbeExecutionError(
                    "legacy earnings wrapper did not delegate only to FMP",
                    status=HealthStatus.UNUSABLE,
                    reason_code=(
                        "LEGACY_EARNINGS_FMP_ONLY_DELEGATION_NOT_OBSERVED"
                    ),
                )
            return _normalized_local_probe_result(
                request,
                current=current,
                raw_output=result.model_dump(mode="json"),
                fields_by_metric={
                    "earnings_event": {
                        field_name: _required_field(
                            events[0],
                            field_name,
                            provider_id=provider_id,
                        )
                        for field_name in request.targets[0].fields
                    }
                },
                fixture_id=(
                    "CONTROLLED_LEGACY_WRAPPER_FMP_ONLY_DELEGATION"
                ),
            )
        if provider_id == "OFFICIAL_ACTUAL_TRANSFORMATION":
            from app.services.official_actual_semantics import OFFICIAL_METRICS

            target = request.targets[0]
            specification = OFFICIAL_METRICS.get(target.metric_id)
            if specification is None:
                raise ProbeExecutionError(
                    "official transform target is not registered",
                    status=HealthStatus.UNUSABLE,
                    reason_code="OFFICIAL_TRANSFORM_TARGET_NOT_REGISTERED",
                )
            retrieved_at = current.isoformat()
            reference_period = _probe_reference_period(
                current,
                frequency=specification.frequency,
            )
            result = adapter_type(
                specification,
                _official_series_fixture(
                    specification,
                    current=current,
                ),
                expected_period=reference_period,
                retrieved_at=retrieved_at,
                release_timestamp=None,
            )
            lineage = _value(result, "lineage", {})
            actual_lineage = _value(lineage, "actual", {})
            expected = {
                "metric_id": target.metric_id,
                "frequency": target.frequency,
                "transformation": target.transformation,
                "reference_period": reference_period,
            }
            observed = {
                key: _value(result, key)
                for key in expected
            }
            if observed != expected or not _value(result, "actual") or not all(
                _value(actual_lineage, key) == expected[key]
                for key in expected
            ):
                raise ProbeExecutionError(
                    "official transform output does not match its target",
                    status=HealthStatus.UNUSABLE,
                    reason_code="OFFICIAL_TRANSFORM_OUTPUT_MISMATCH",
                )
            return result
        if provider_id == "MACRO_CONSENSUS_RECONCILIATION":
            reference_period = current.strftime("%Y-%m")
            items = [
                _macro_consensus_fixture_item(
                    target,
                    current=current,
                    reference_period=reference_period,
                )
                for target in request.targets
            ]
            merged = adapter_type(
                {
                    "source": "PROVIDER_CAPABILITY_AUDIT_FIXTURE",
                    "retrieved_at": current.isoformat(),
                    "valid_until": (
                        current + timedelta(minutes=5)
                    ).isoformat(),
                    "items": items,
                }
            )
            observed = {
                str(item.get("metric_id")): item
                for item in (merged or {}).get("items") or []
                if isinstance(item, Mapping)
            }
            return _normalized_local_probe_result(
                request,
                current=current,
                raw_output=merged,
                fields_by_metric={
                    target.metric_id: {
                        field_name: _required_field(
                            observed.get(target.metric_id),
                            field_name,
                            provider_id=provider_id,
                        )
                        for field_name in target.fields
                    }
                    for target in request.targets
                },
                fixture_id="CONTROLLED_MACRO_CONSENSUS_MERGE",
            )
        if provider_id == "PROVIDER_FORCE_ACTUAL_RECONCILIATION":
            occurrence_id = (
                "provider-audit:flash-services-pmi:"
                f"{current.strftime('%Y-%m')}"
            )
            reference_period = current.strftime("%Y-%m")
            valid_until = current + timedelta(hours=6)
            lineage = _field_lineage_fixture(
                request.targets[0].fields,
                provider_id="SPGLOBAL",
                occurrence_id=occurrence_id,
                reference_period=reference_period,
            )
            service = adapter_type(
                self.settings,
                lifecycle_resolver=_ControlledProviderForceResolver(
                    current=current,
                    occurrence_id=occurrence_id,
                    reference_period=reference_period,
                    valid_until=valid_until,
                    lineage=lineage,
                ),
                clock=lambda: current,
                generation_id=f"provider-audit-{request.run_id}",
                force_refresh=False,
            )
            prepared = service.prepare(
                {
                    "event_calendar": {
                        "critical_macro_events": [
                            {
                                "occurrence_id": occurrence_id,
                                "event_id": occurrence_id,
                                "metric_id": "flash_services_pmi",
                                "name": "Flash Services PMI",
                                "frequency": "monthly",
                                "reference_period": reference_period,
                                "release_at": (
                                    current - timedelta(minutes=15)
                                ).isoformat(),
                                "actual": None,
                                "consensus": 52.0,
                                "previous": 51.2,
                                "lineage": lineage,
                                "expected_occurrence_id": occurrence_id,
                                "expected_reference_period": reference_period,
                                "lifecycle_verified": True,
                            }
                        ],
                        "fed_communications": [],
                        "other_economic_events": [],
                    }
                }
            )
            event = _single_prepared_event(prepared)
            return _normalized_local_probe_result(
                request,
                current=current,
                raw_output=prepared,
                fields_by_metric={
                    "flash_services_pmi": {
                        field_name: _required_field(
                            event,
                            field_name,
                            provider_id=provider_id,
                        )
                        for field_name in request.targets[0].fields
                    }
                },
                fixture_id="CONTROLLED_PROVIDER_FORCE_RESOLUTION",
            )
        if provider_id == "REQUEST_PROVIDER_ACCOUNTING":
            from app.infrastructure.persistence.provider_cache_repository import (
                ProviderCacheRepository,
            )
            from app.services.request_provider_accounting import (
                provider_attempt,
            )

            fixture_dataset_id = "request_accounting"
            fixture_policy = SimpleNamespace(
                dataset_id=fixture_dataset_id,
                primary_provider="PROVIDER_CAPABILITY_AUDIT",
                fallback_providers=(),
                canonical_repository_required=True,
                max_age=timedelta(hours=1),
                provider_strategy="FALLBACK",
            )
            request_identity = f"provider-audit-{request.run_id}"
            collector = adapter_type(
                request_id=request_identity,
                correlation_id=request_identity,
                request_started_at=current - timedelta(seconds=1),
                policies=(fixture_policy,),
                clock=lambda: current,
            )
            cache = ProviderCacheRepository(self.settings.database_path)
            cache_key = (
                "provider-capability-audit:"
                f"{request.run_id}:accounting-canonical-record"
            )
            valid_until = current + timedelta(hours=1)
            refresh_due_at = current + timedelta(minutes=30)
            cache.set(
                cache_key,
                {
                    "data_as_of": current.isoformat(),
                    "fixture": "REQUEST_ACCOUNTING_DB_FIRST",
                },
                provider_name="PROVIDER_CAPABILITY_AUDIT",
                valid_until=valid_until.isoformat(),
                stale_until=(current + timedelta(hours=2)).isoformat(),
                status="valid_cache",
            )
            database_record = cache.get_entry(cache_key)
            if database_record is None:
                raise ProbeExecutionError(
                    "controlled accounting cache lookup returned no record",
                    status=HealthStatus.UNUSABLE,
                    reason_code="LOCAL_ACCOUNTING_DB_LOOKUP_FAILED",
                )
            collector.record(
                fixture_dataset_id,
                acquisition_id=(
                    "provider-capability-audit:"
                    f"{request.run_id}:db-first"
                ),
                shared_dataset_ids=(fixture_dataset_id,),
                database_lookup_performed=True,
                database_lookup_reason=(
                    "CONTROLLED_CANONICAL_CACHE_LOOKUP_OBSERVED"
                ),
                database_record_found=True,
                database_data_as_of=current.isoformat(),
                database_content_valid_until=database_record[
                    "valid_until"
                ],
                database_refresh_due_at=refresh_due_at.isoformat(),
                database_lifecycle_status="CURRENT",
                database_record_expired=False,
                database_freshness_evaluation="VALID",
                primary_provider=provider_attempt(
                    fixture_policy.primary_provider,
                    called=False,
                    attempts=0,
                    result="NOT_CALLED_DATABASE_SELECTED",
                    not_called_reason="VALID_CANONICAL_RECORD",
                    execution_origin="CACHE_DECISION",
                ),
                fallbacks=(),
                acquisition_selected_source="PROVIDER_CACHE_REPOSITORY",
                acquisition_reason_code=(
                    "VALID_CANONICAL_RECORD_SELECTED"
                ),
                observed_at=current,
            )
            manifest = collector.manifest(request_completed_at=current)
            rows = manifest.get("datasets") or []
            row = rows[0] if len(rows) == 1 else None
            normalized_fields = {
                "request_id": _required_field(
                    manifest,
                    "request_id",
                    provider_id=provider_id,
                ),
                "correlation_id": _required_field(
                    manifest,
                    "correlation_id",
                    provider_id=provider_id,
                ),
                "database_lookup": {
                    key: _required_field(
                        row,
                        key,
                        provider_id=provider_id,
                    )
                    for key in (
                        "database_lookup_performed",
                        "database_lookup_reason",
                        "database_record_found",
                        "database_data_as_of",
                        "database_content_valid_until",
                        "database_refresh_due_at",
                        "database_record_expired",
                        "database_freshness_evaluation",
                    )
                },
                "provider_attempts": [
                    _required_field(
                        row,
                        "primary_provider",
                        provider_id=provider_id,
                    ),
                    *list((row or {}).get("fallbacks") or []),
                ],
                "selected_source": _required_field(
                    row,
                    "acquisition_selected_source",
                    provider_id=provider_id,
                ),
                "reason_code": _required_field(
                    manifest,
                    "reason_code",
                    provider_id=provider_id,
                ),
            }
            return _normalized_local_probe_result(
                request,
                current=current,
                raw_output=manifest,
                fields_by_metric={
                    "request_scoped_provider_accounting": (
                        normalized_fields
                    )
                },
                fixture_id="CONTROLLED_REQUEST_ACCOUNTING_DB_FIRST",
            )
        if provider_id == "SENIOR_ANALYST_PROJECTION":
            projected = adapter_type(
                _senior_analyst_projection_fixture(current),
                now=current,
                request_id=f"provider-audit-{request.run_id}",
                request_refresh_mode="audit",
            )
            return _normalized_local_probe_result(
                request,
                current=current,
                raw_output=projected,
                fields_by_metric={
                    "senior_analyst_projection": {
                        field_name: _required_field(
                            projected,
                            field_name,
                            provider_id=provider_id,
                        )
                        for field_name in request.targets[0].fields
                    }
                },
                fixture_id="CONTROLLED_SENIOR_ANALYST_PROJECTION",
            )
        raise ProbeExecutionError(
            "local capability hook has no registered implementation",
            status=HealthStatus.UNUSABLE,
            reason_code="LOCAL_CAPABILITY_PROBE_NOT_IMPLEMENTED",
        )


class _ControlledProviderForceResolver:
    """Deterministic provider boundary for the local reconciliation probe."""

    def __init__(
        self,
        *,
        current: datetime,
        occurrence_id: str,
        reference_period: str,
        valid_until: datetime,
        lineage: list[dict[str, Any]],
    ) -> None:
        self.current = current
        self.occurrence_id = occurrence_id
        self.reference_period = reference_period
        self.valid_until = valid_until
        self.lineage = lineage
        self.calls = 0

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        if item.get("entity_key") != self.occurrence_id:
            raise ProbeExecutionError(
                "controlled resolver received the wrong occurrence",
                status=HealthStatus.UNUSABLE,
                reason_code="LOCAL_PROVIDER_FORCE_OCCURRENCE_MISMATCH",
            )
        datum = {
            **dict(item.get("payload") or {}),
            "actual": 53.6,
            "consensus": 52.0,
            "previous": 51.2,
            "occurrence_id": self.occurrence_id,
            "metric_id": "flash_services_pmi",
            "frequency": "monthly",
            "reference_period": self.reference_period,
            "source": "SPGLOBAL",
            "publisher": "S&P Global Market Intelligence",
            "distributor": "SPGLOBAL",
            "source_url": (
                "https://www.pmi.spglobal.com/Public/Home/PressRelease/"
                "provider-capability-audit"
            ),
            "data_as_of": self.reference_period,
            "valid_until": self.valid_until.isoformat(),
            "next_refresh_at": self.valid_until.isoformat(),
            "lineage": self.lineage,
            "expected_occurrence_id": self.occurrence_id,
            "expected_reference_period": self.reference_period,
            "lifecycle_verified": True,
        }
        return {
            "status": "RESOLVED",
            "datum": datum,
            "candidate": {
                "validation_status": "accepted",
                "policy_version": "provider-capability-audit-v1",
            },
            "provider": "SPGLOBAL",
            "source_series": "SPGLOBAL:FLASH_SERVICES_PMI",
            "provider_call_count": 1,
            "provider_request_attempted": True,
            "provider_attempts": [
                {
                    "provider": "SPGLOBAL",
                    "attempts": 1,
                    "result": "SUCCESS",
                }
            ],
            "provider_http_outcome": "SUCCESS",
            "candidate_validation": "accepted",
            "reason_code": "OFFICIAL_ACTUAL_RESOLVED",
        }


def _macro_consensus_fixture_item(
    target: Any,
    *,
    current: datetime,
    reference_period: str,
) -> dict[str, Any]:
    occurrence_id = (
        f"provider-audit:{target.metric_id}:{reference_period}"
    )
    values = {
        "consensus": 2.5,
        "previous": 2.4,
        "occurrence_id": occurrence_id,
        "reference_period": reference_period,
    }
    values["lineage"] = _field_lineage_fixture(
        target.fields,
        provider_id="PROVIDER_CAPABILITY_AUDIT_FIXTURE",
        occurrence_id=occurrence_id,
        reference_period=reference_period,
    )
    return {
        "metric_id": target.metric_id,
        "frequency": "event",
        "event_name": f"Controlled {target.metric_id} occurrence",
        "release_at": current.isoformat(),
        "retrieved_at": current.isoformat(),
        "valid_until": (current + timedelta(minutes=5)).isoformat(),
        "refresh_due_at": (current + timedelta(minutes=5)).isoformat(),
        "freshness": "CURRENT_RELEASE",
        "expected_occurrence_id": occurrence_id,
        "expected_reference_period": reference_period,
        "lifecycle_verified": True,
        "source": "PROVIDER_CAPABILITY_AUDIT_FIXTURE",
        "reliability": 1.0,
        **values,
    }


def _field_lineage_fixture(
    fields: Any,
    *,
    provider_id: str,
    occurrence_id: str,
    reference_period: str,
) -> list[dict[str, Any]]:
    return [
        {
            "field": str(field_name),
            "provider": provider_id,
            "acquisition_provider": provider_id,
            "occurrence_id": occurrence_id,
            "reference_period": reference_period,
            "verification": "CONTROLLED_LOCAL_RUNTIME_OUTPUT",
        }
        for field_name in fields
    ]


def _single_prepared_event(prepared: Any) -> Mapping[str, Any]:
    contract = (
        prepared.get("contract")
        if isinstance(prepared, Mapping)
        else None
    )
    calendar = (
        contract.get("event_calendar")
        if isinstance(contract, Mapping)
        else None
    )
    events = (
        calendar.get("critical_macro_events")
        if isinstance(calendar, Mapping)
        else None
    )
    if (
        not isinstance(events, list)
        or len(events) != 1
        or not isinstance(events[0], Mapping)
    ):
        raise ProbeExecutionError(
            "provider-force fixture did not emit exactly one occurrence",
            status=HealthStatus.UNUSABLE,
            reason_code="LOCAL_PROVIDER_FORCE_OUTPUT_MISSING",
        )
    return events[0]


def _senior_analyst_projection_fixture(
    current: datetime,
) -> dict[str, Any]:
    valid_until = current + timedelta(hours=1)
    common = {
        "status": "AVAILABLE",
        "freshness": "CURRENT",
        "data_as_of": current.isoformat(),
        "content_valid_until": valid_until.isoformat(),
        "refresh_due_at": valid_until.isoformat(),
    }
    return {
        "contract": "ai_trader_market_context_sync",
        "schema_version": "1.0",
        "symbol": "MNQ",
        "snapshot_id": "provider-capability-audit-snapshot",
        "snapshot_revision": 1,
        "generated_at": current.isoformat(),
        "sections": {
            "macro": {
                "snapshot": {
                    "inflation": {},
                    "growth": {},
                    "labor": {},
                    "rates_and_yields": {},
                }
            },
            "event_calendar": {},
            "fed": {},
            "nasdaq": {},
            "market_internals": {},
            "news": {},
            "vix": {
                "vix": {
                    **common,
                    "value": 18.5,
                    "source": "FRED",
                    "lineage": [
                        {
                            "field": "value",
                            "source": "FRED",
                        }
                    ],
                },
                "vvix": {
                    **common,
                    "value": 92.0,
                    "source": "CBOE",
                    "lineage": [
                        {
                            "field": "value",
                            "source": "CBOE",
                        }
                    ],
                },
            },
            "risk": {},
            "rates": {},
            "positioning": {},
            "earnings": {},
            "options_positioning": {},
            "market_schedule": {},
        },
    }


def _normalized_local_probe_result(
    request: ProbeRequest,
    *,
    current: datetime,
    raw_output: Any,
    fields_by_metric: Mapping[str, Mapping[str, Any]],
    fixture_id: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for target in request.targets:
        values = fields_by_metric.get(target.metric_id)
        if not isinstance(values, Mapping):
            raise ProbeExecutionError(
                "local probe did not emit its registered metric",
                status=HealthStatus.UNUSABLE,
                reason_code="LOCAL_CAPABILITY_METRIC_NOT_OBSERVED",
            )
        missing = [
            field_name
            for field_name in target.fields
            if field_name not in values
            or not _positive_fixture_value(values[field_name])
        ]
        if missing:
            raise ProbeExecutionError(
                "local probe omitted registered fields: "
                + ",".join(sorted(missing)),
                status=HealthStatus.UNUSABLE,
                reason_code="LOCAL_CAPABILITY_FIELD_NOT_OBSERVED",
            )
        occurrence_id = str(
            values.get("occurrence_id")
            or (
                f"provider-audit:{target.metric_id}:"
                f"{current.date().isoformat()}"
            )
        )
        reference_period = str(
            values.get("reference_period")
            or current.date().isoformat()
        )
        lineage = values.get("lineage")
        if not isinstance(lineage, list):
            lineage = _field_lineage_fixture(
                target.fields,
                provider_id=request.provider_id,
                occurrence_id=occurrence_id,
                reference_period=reference_period,
            )
        target_id = (
            f"{request.provider_id}|{target.dataset_id}|"
            f"{target.metric_id}"
        )
        field_lineage = {
            field_name: {
                "field": field_name,
                "value_sha256": _stable_json_sha256(
                    values[field_name]
                ),
                "publisher": request.provider_id,
                "distributor": "LOCAL_SANDBOX",
                "acquisition_provider": request.provider_id,
                "verification_origin": "REGISTERED_ADAPTER_OUTPUT",
                "occurrence_id": occurrence_id,
                "reference_period": reference_period,
            }
            for field_name in target.fields
        }
        rows.append(
            {
                "target_id": target_id,
                "provider_id": request.provider_id,
                "dataset_id": target.dataset_id,
                "metric_id": target.metric_id,
                "frequency": target.frequency,
                "transformation": target.transformation,
                "data_as_of": current.isoformat(),
                "valid_until": (
                    current + timedelta(minutes=5)
                ).isoformat(),
                "refresh_due_at": (
                    current + timedelta(minutes=5)
                ).isoformat(),
                "freshness": "CURRENT",
                "expected_occurrence_id": occurrence_id,
                "expected_reference_period": reference_period,
                "lifecycle_verified": True,
                "raw_result_sha256": _stable_json_sha256(raw_output),
                **dict(values),
                "lineage": lineage,
                "field_lineage": field_lineage,
            }
        )
    return {
        "status": "LOCAL_SANDBOX_FIXTURE_EXECUTED",
        "fixture_id": fixture_id,
        "normalization": (
            "EXACT_REGISTERED_FIELDS_COPIED_FROM_REAL_ADAPTER_OUTPUT"
        ),
        "raw_result_sha256": _stable_json_sha256(raw_output),
        "capability_results": rows,
    }


def _required_field(
    value: Any,
    field_name: str,
    *,
    provider_id: str,
) -> Any:
    if not isinstance(value, Mapping) or field_name not in value:
        raise ProbeExecutionError(
            f"{provider_id} did not emit {field_name}",
            status=HealthStatus.UNUSABLE,
            reason_code="LOCAL_CAPABILITY_FIELD_NOT_OBSERVED",
        )
    observed = value[field_name]
    if not _positive_fixture_value(observed):
        raise ProbeExecutionError(
            f"{provider_id} emitted an empty {field_name}",
            status=HealthStatus.UNUSABLE,
            reason_code="LOCAL_CAPABILITY_FIELD_NOT_OBSERVED",
        )
    return observed


def _positive_fixture_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (Mapping, list, tuple, set)):
        return bool(value)
    return True


def _safe_identifier(value: Any) -> str:
    return "".join(
        character
        for character in str(value)
        if character.isalnum() or character in {"-", "_"}
    ) or "run"


def _load_symbol(path: str) -> Any:
    module_name, separator, symbol_name = path.replace(":", ".").rpartition(".")
    if not separator:
        raise ImportError(f"adapter path is not dotted: {path}")
    return getattr(importlib.import_module(module_name), symbol_name)


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _matching_market_fact_rows(
    rows: list[dict[str, Any]],
    *,
    series_ids: tuple[str, ...],
) -> list[dict[str, Any]]:
    expected = {
        str(item).strip().upper()
        for item in series_ids
        if str(item).strip()
    }
    selected: dict[str, dict[str, Any]] = {}
    for row in rows:
        if expected and not expected.intersection(
            _market_fact_series_identities(row)
        ):
            continue
        fact_key = str(row.get("fact_key") or "").strip()
        identity = fact_key or _stable_json_sha256(row)
        selected[identity] = row
    return [selected[key] for key in sorted(selected)]


def _market_fact_series_identities(
    row: Mapping[str, Any],
) -> set[str]:
    raw_payload = row.get("raw_payload")
    raw = raw_payload if isinstance(raw_payload, Mapping) else {}
    return {
        str(value).strip().upper()
        for value in (
            row.get("category"),
            raw.get("series_id"),
            raw.get("canonical_series_id"),
            raw.get("category"),
        )
        if value not in (None, "")
    }


def _market_fact_probe_record(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    raw_payload = row.get("raw_payload")
    raw = raw_payload if isinstance(raw_payload, Mapping) else {}
    lineage = (
        raw.get("lineage")
        or raw.get("field_lineage")
        or _decoded_json(row.get("field_lineage_json"))
    )
    return {
        "fact_key": row.get("fact_key"),
        "fact_type": row.get("fact_type"),
        "value": (
            row.get("value")
            if row.get("value") is not None
            else raw.get("value")
        ),
        "data_as_of": raw.get("data_as_of"),
        "release_at": row.get("release_at"),
        "valid_until": row.get("valid_until"),
        "next_refresh_at": row.get("next_refresh_at"),
        "lineage": lineage,
        "retrieved_at": row.get("retrieved_at"),
        "source": row.get("source"),
        "status": row.get("status"),
        "raw_payload_sha256": (
            _stable_json_sha256(raw_payload)
            if raw_payload is not None
            else None
        ),
    }


def _decoded_json(value: Any) -> Any:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _stable_json_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _probe_reference_period(
    current: datetime,
    *,
    frequency: str,
    offset: int = 0,
) -> str:
    if frequency == "monthly":
        month_index = current.year * 12 + current.month - 1 - offset
        year, month_zero = divmod(month_index, 12)
        return f"{year:04d}-{month_zero + 1:02d}"
    if frequency == "quarterly":
        quarter = (current.month - 1) // 3 + 1
        quarter_index = current.year * 4 + quarter - 1 - offset
        year, quarter_zero = divmod(quarter_index, 4)
        return f"{year:04d}-Q{quarter_zero + 1}"
    raise ValueError(f"unsupported official fixture frequency: {frequency}")


def _official_series_fixture(
    specification: Any,
    *,
    current: datetime,
) -> dict[str, Any]:
    observations = [
        {
            "period": _probe_reference_period(
                current,
                frequency=specification.frequency,
                offset=offset,
            ),
            "value": str(100 + specification.comparison_lag - offset),
            "release_vintage": "AUDIT_FIXTURE",
        }
        for offset in range(specification.comparison_lag, -1, -1)
    ]
    return {
        "series_id": specification.source_series_id,
        "source": specification.provider_id,
        "source_url": specification.canonical_url,
        "frequency": specification.frequency,
        "seasonal_adjustment": specification.seasonal_adjustment,
        "units": specification.unit,
        "data_as_of": observations[-1]["period"],
        "observations": observations,
    }


__all__ = ["LocalCapabilityProbeHook"]
