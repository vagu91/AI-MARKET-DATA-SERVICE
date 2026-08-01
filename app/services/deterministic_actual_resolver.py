from __future__ import annotations

import asyncio
import importlib
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.investing_flash_services_pmi import (
    SOURCE as INVESTING_FLASH_SOURCE,
)
from app.services.event_value_candidate_repository import EventValueCandidateRepository
from app.services.macro_consensus_service import candidate_metric_id
from app.services.official_actual_semantics import (
    OFFICIAL_METRICS,
    UNSUPPORTED_OFFICIAL_METRICS,
    derive_official_actual,
    metric_semantics_mismatch_reason,
    normalize_reference_period,
)
from app.services.provider_capability_registry import (
    dataset_policy_by_id,
    dataset_runtime_provider_order,
    provider_by_id,
)


FLASH_SERVICES_PMI_DATASET_ID = "flash_services_pmi"
_FLASH_SERVICES_PMI_DISPATCH_IDS = frozenset(
    {"SPGLOBAL", INVESTING_FLASH_SOURCE}
)
_ACTUAL_ONLY_OFFICIAL_METRICS = frozenset({"new_home_sales"})


def _actual_field_scoped_candidate(
    candidate: dict[str, Any],
) -> dict[str, Any]:
    scoped = dict(candidate)
    metric_id = str(
        scoped.get("event_metric_id") or scoped.get("metric_id") or ""
    )
    if metric_id in _ACTUAL_ONLY_OFFICIAL_METRICS:
        scoped.pop("previous", None)
        scoped.pop("previous_reference_period", None)
    return scoped


def _declared_flash_services_pmi_provider_order() -> tuple[str, ...]:
    policy = dataset_policy_by_id(FLASH_SERVICES_PMI_DATASET_ID)
    return (
        policy.primary_provider,
        *policy.fallback_providers,
    )


def _flash_services_pmi_provider_order() -> tuple[str, ...]:
    return dataset_runtime_provider_order(
        FLASH_SERVICES_PMI_DATASET_ID,
        _FLASH_SERVICES_PMI_DISPATCH_IDS,
    )


def _registered_adapter(provider_id: str) -> Any:
    registration = provider_by_id(provider_id)
    module_name, separator, qualname = registration.adapter_path.partition(
        ":"
    )
    if not separator:
        raise RuntimeError(
            f"registered adapter path is invalid:{provider_id}"
        )
    value: Any = importlib.import_module(module_name)
    for part in qualname.split("."):
        value = getattr(value, part)
    return value


# These public aliases preserve the monkeypatch surface used by existing tests,
# but their classes and paths now come from the central provider registry.
BlsProvider = _registered_adapter("BLS")
BeaProvider = _registered_adapter("BEA")
CensusProvider = _registered_adapter("CENSUS")
FredProvider = _registered_adapter("FRED")
SpGlobalPmiProvider = _registered_adapter("SPGLOBAL")
InvestingFlashServicesPmiProvider = _registered_adapter(
    INVESTING_FLASH_SOURCE
)


PROVIDERS = {
    "BLS": BlsProvider,
    "BEA": BeaProvider,
    "CENSUS": CensusProvider,
    "FRED": FredProvider,
    "SPGLOBAL": SpGlobalPmiProvider,
    INVESTING_FLASH_SOURCE: InvestingFlashServicesPmiProvider,
}


def official_actual_mapping(event: dict[str, Any]) -> dict[str, str] | None:
    metric_id = _semantic_metric_id(event)
    spec = OFFICIAL_METRICS.get(metric_id or "")
    if spec is None:
        return None
    return {
        "metric_id": spec.event_metric_id,
        "provider": spec.provider,
        "source_series": spec.source_series_id,
        "frequency": spec.frequency,
        "unit": spec.unit,
    }


class DeterministicActualResolver:
    """Resolve event-semantic actuals from official observations, never raw macro levels."""

    def __init__(
        self,
        settings: Settings,
        *,
        providers: Mapping[str, Any] | None = None,
    ) -> None:
        self.settings = settings
        self.candidates = EventValueCandidateRepository(settings)
        self.cache = ProviderCacheRepository(settings.database_path)
        self.providers = dict(providers or PROVIDERS)

    def __call__(self, job: dict[str, Any], workspace: Path, timeout_seconds: int) -> dict[str, Any]:
        del workspace, timeout_seconds
        event_key = str(job.get("event_key") or "")
        request = job.get("request_payload") or {}
        event = request.get("event") or {}
        temporal = request.get("temporal_state") or {}
        return self.resolve_event(
            event_key=event_key,
            event=event,
            temporal_state=temporal,
            expected_period=request.get("expected_period"),
        )

    def resolve_event(
        self,
        *,
        event_key: str,
        event: dict[str, Any],
        temporal_state: dict[str, Any],
        expected_period: Any = None,
        persist_candidate: bool = True,
    ) -> dict[str, Any]:
        existing = (
            self.candidates.accepted_official_actual(event_key)
            if event_key and persist_candidate
            else None
        )
        if existing is not None:
            existing = _actual_field_scoped_candidate(existing)
            return {
                "status": "SUCCEEDED",
                "results": [existing],
                "resolution": "persisted_candidate",
                "provider_call_count": 0,
                "mapping_selected": existing.get("event_metric_id"),
                "source_series": existing.get("source_series_id"),
            }
        metric_id = _semantic_metric_id(event)
        if metric_id in UNSUPPORTED_OFFICIAL_METRICS:
            return {
                "status": "NO_DATA", "results": [],
                "error": f"official_metric_unsupported:{metric_id}:{UNSUPPORTED_OFFICIAL_METRICS[metric_id]}",
            }
        spec = OFFICIAL_METRICS.get(metric_id or "")
        if spec is None:
            return {
                "status": "NO_DATA", "results": [],
                "error": f"official_metric_unsupported:{metric_id or 'UNKNOWN'}",
            }
        is_flash_services_pmi = (
            spec.event_metric_id == FLASH_SERVICES_PMI_DATASET_ID
        )
        flash_provider_order: tuple[str, ...] = ()
        if is_flash_services_pmi:
            declared_order: tuple[str, ...] = ()
            try:
                declared_order = (
                    _declared_flash_services_pmi_provider_order()
                )
                flash_provider_order = (
                    _flash_services_pmi_provider_order()
                )
            except (KeyError, RuntimeError) as exc:
                return _flash_pmi_runtime_policy_invalid(
                    declared_order=declared_order,
                    error=exc,
                    source_series=spec.source_series_id,
                )
        active_provider_id = (
            flash_provider_order[0]
            if is_flash_services_pmi
            else spec.provider
        )
        expected_period = (
            event.get("reference_period")
            or event.get("period")
            or expected_period
        )
        release_timestamp = (
            temporal_state.get("release_at")
            or event.get("release_at")
            or event.get("time_utc")
        )
        release_date = _release_date(release_timestamp)
        expected_period = normalize_reference_period(
            expected_period,
            frequency=spec.frequency,
            release_date=release_date,
        )
        if expected_period is None:
            return {
                "status": "NO_DATA",
                "retryable": False,
                "results": [],
                "error": "official_reference_period_missing",
            }
        if is_flash_services_pmi and release_date is None:
            return {
                "status": "NO_DATA",
                "retryable": False,
                "results": [],
                "error": "official_release_date_missing",
            }
        provider_config = self.providers.get(active_provider_id)
        if provider_config is None:
            if is_flash_services_pmi:
                return _flash_pmi_provider_adapter_unavailable(
                    provider_order=flash_provider_order,
                    unavailable_provider=active_provider_id,
                    source_series=spec.source_series_id,
                )
            return {
                "status": "NO_DATA",
                "results": [],
                "error": "official_provider_adapter_unavailable",
            }
        provider = _provider_instance(
            provider_config,
            cache=self.cache,
            settings=self.settings,
        )
        fallback_used = False
        primary_failure: str | None = None

        def fetch_flash_pmi_fallback(
            failure: str,
        ) -> tuple[Any | None, dict[str, Any] | None]:
            fallback_provider_id = flash_provider_order[1]
            fallback_config = self.providers.get(fallback_provider_id)
            if fallback_config is None:
                return None, _flash_pmi_fallback_adapter_unavailable(
                    failure=failure,
                    primary_provider=flash_provider_order[0],
                    fallback_provider=fallback_provider_id,
                    source_series=spec.source_series_id,
                )
            fallback = _provider_instance(
                fallback_config,
                cache=self.cache,
                settings=self.settings,
            )
            try:
                fallback_result = _fetch_flash_services_pmi_provider(
                    fallback_provider_id,
                    fallback,
                    expected_period=expected_period,
                    release_date=release_date,
                    release_timestamp=release_timestamp,
                )
            except Exception as fallback_exc:
                return None, _flash_pmi_all_failed(
                    primary_failure=failure,
                    fallback_failure=(
                        str(fallback_exc)
                        or type(fallback_exc).__name__
                    ),
                    primary_provider=flash_provider_order[0],
                    fallback_provider=fallback_provider_id,
                    source_series=spec.source_series_id,
                )
            return fallback_result, None

        try:
            if is_flash_services_pmi:
                result = _fetch_flash_services_pmi_provider(
                    active_provider_id,
                    provider,
                    expected_period=expected_period,
                    release_date=release_date,
                    release_timestamp=release_timestamp,
                )
            elif spec.provider == "CENSUS":
                dataset = spec.source_series_id.split(":", 2)[1]
                result = asyncio.run(
                    provider.fetch(
                        period=expected_period,
                        datasets=[dataset],
                    )
                )
            elif spec.provider == "FRED":
                result = asyncio.run(
                    provider.fetch(series_ids=[spec.source_series_id])
                )
            else:
                result = asyncio.run(provider.fetch())
        except Exception as exc:
            detail = _redacted_provider_result(
                str(exc)
                or f"official_provider_unavailable:{type(exc).__name__}"
            )
            primary_failure = (
                detail
                if detail
                else f"official_provider_unavailable:{type(exc).__name__}"
            )
            if not is_flash_services_pmi:
                if detail == "sp_global_public_release_access_restricted":
                    return _feed_delayed(
                        "sp_global_public_release_access_restricted",
                        provider=spec.provider,
                        source_series=spec.source_series_id,
                        provider_http_outcome="HTTP_403",
                    )
                return _feed_delayed(
                    f"official_provider_unavailable:{type(exc).__name__}",
                    provider=spec.provider,
                    source_series=spec.source_series_id,
                )
            result, delayed = fetch_flash_pmi_fallback(
                primary_failure
            )
            if delayed is not None:
                return delayed
            fallback_used = True
            active_provider_id = flash_provider_order[1]

        def retry_flash_pmi_after_result_failure(
            failure: str,
        ) -> tuple[bool, dict[str, Any] | None]:
            nonlocal active_provider_id, fallback_used, primary_failure, result
            if not is_flash_services_pmi:
                return False, None
            if fallback_used:
                return False, _flash_pmi_all_failed(
                    primary_failure=(
                        primary_failure
                        or (
                            f"{flash_provider_order[0]}_"
                            "PROVIDER_RESULT_REJECTED"
                        )
                    ),
                    fallback_failure=failure,
                    primary_provider=flash_provider_order[0],
                    fallback_provider=flash_provider_order[1],
                    source_series=spec.source_series_id,
                )
            primary_failure = failure
            fallback_result, delayed = fetch_flash_pmi_fallback(
                primary_failure
            )
            if delayed is not None:
                return False, delayed
            result = fallback_result
            fallback_used = True
            active_provider_id = flash_provider_order[1]
            return True, None

        while True:
            rows = result.data if isinstance(result.data, dict) else {}
            series = rows.get(spec.source_series_id)
            if not isinstance(series, dict):
                failure = "official_series_not_available"
                retried, delayed = (
                    retry_flash_pmi_after_result_failure(failure)
                )
                if delayed is not None:
                    return delayed
                if retried:
                    continue
                return _feed_delayed(failure)
            expected_adapter = (
                INVESTING_FLASH_SOURCE
                if active_provider_id == INVESTING_FLASH_SOURCE
                else f"{active_provider_id}_OFFICIAL_API"
            )
            official_adapter_valid = (
                active_provider_id != INVESTING_FLASH_SOURCE
                and result.metadata.source == active_provider_id
                and series.get("official_adapter") is True
                and series.get("provider_adapter") == expected_adapter
            )
            fallback_adapter_valid = (
                active_provider_id == INVESTING_FLASH_SOURCE
                and result.metadata.source == INVESTING_FLASH_SOURCE
                and series.get("provider_adapter")
                == INVESTING_FLASH_SOURCE
                and int(series.get("event_id") or 0) == 1062
                and series.get("occurrence_id") is not None
            )
            if not official_adapter_valid and not fallback_adapter_valid:
                failure = (
                    "official_adapter_required:observed="
                    f"{series.get('provider_adapter') or result.metadata.source}"
                )
                retried, delayed = (
                    retry_flash_pmi_after_result_failure(failure)
                )
                if delayed is not None:
                    return delayed
                if retried:
                    continue
                return _feed_delayed(failure)
            source_adjustment = str(
                series.get("seasonal_adjustment") or ""
            ).upper()
            if (
                source_adjustment
                and source_adjustment != spec.seasonal_adjustment
            ):
                failure = "seasonal_adjustment_mismatch"
                retried, delayed = (
                    retry_flash_pmi_after_result_failure(failure)
                )
                if delayed is not None:
                    return delayed
                if retried:
                    continue
                return {
                    "status": "NO_DATA",
                    "results": [],
                    "error": failure,
                }
            retrieved_at = result.metadata.retrieved_at.isoformat()
            try:
                candidate = _actual_field_scoped_candidate(
                    derive_official_actual(
                        spec,
                        series,
                        expected_period=expected_period,
                        retrieved_at=retrieved_at,
                        release_timestamp=release_timestamp,
                    )
                )
            except ValueError as exc:
                failure = _redacted_provider_result(str(exc))
                retried, delayed = (
                    retry_flash_pmi_after_result_failure(failure)
                )
                if delayed is not None:
                    return delayed
                if retried:
                    continue
                if failure in {
                    "period_mismatch",
                    "insufficient_official_observations",
                    "official_observations_missing",
                }:
                    return _feed_delayed(failure)
                return {
                    "status": "NO_DATA",
                    "retryable": False,
                    "results": [],
                    "error": failure,
                }
            source_url = str(series.get("source_url") or "")
            canonical_url = str(
                series.get("canonical_url") or spec.canonical_url
            )
            candidate.update(
                {
                    "source": str(
                        series.get("source_originator")
                        or series.get("source")
                        or result.metadata.source
                    ),
                    "publisher": (
                        series.get("publisher")
                        or series.get("source_originator")
                        or result.metadata.source
                    ),
                    "distribution_source": series.get(
                        "distribution_source"
                    ),
                    "acquisition_provider": (
                        series.get("acquisition_provider")
                        or result.metadata.source
                    ),
                    "source_url": source_url,
                    "canonical_url": canonical_url,
                    "source_domain": series.get("source_domain"),
                    "provider_adapter": series.get(
                        "provider_adapter"
                    ),
                    "evidence_text": (
                        f"Official {result.metadata.source} adapter "
                        f"{expected_adapter}, series "
                        f"{spec.source_series_id}; "
                        f"{spec.transformation} for "
                        f"{candidate['reference_period']}."
                    ),
                    "reliability": result.metadata.reliability,
                    "confidence": result.metadata.reliability,
                    "published_at": (
                        release_timestamp
                        or datetime.now(UTC)
                        .replace(microsecond=0)
                        .isoformat()
                    ),
                    "released_at": (
                        series.get("release_timestamp")
                        or release_timestamp
                    ),
                    "validation_timestamp": retrieved_at,
                    "raw_lineage_redacted": series.get(
                        "raw_lineage_redacted"
                    ),
                }
            )
            investing_selected = (
                is_flash_services_pmi
                and active_provider_id == INVESTING_FLASH_SOURCE
            )
            if investing_selected:
                xtb_forecast = event.get(
                    "consensus", event.get("forecast")
                )
                investing_forecast = series.get("forecast")
                candidate.update(
                    {
                        "actual_is_official": False,
                        "event_id": series.get("event_id"),
                        "occurrence_id": series.get("occurrence_id"),
                        "forecast": investing_forecast,
                        "consensus": investing_forecast,
                        "previous": series.get("previous"),
                        "field_lineage": (
                            series.get("field_lineage") or {}
                        ),
                        "forecast_observations": [
                            {
                                "value": xtb_forecast,
                                "provider": "XTB",
                                "occurrence_id": (
                                    event.get("occurrence_id")
                                    or event.get("event_id")
                                ),
                                "selected": False,
                                "reason_code": (
                                    "CONCURRENT_FORECAST_PRESERVED"
                                ),
                            },
                            {
                                "value": investing_forecast,
                                "provider": INVESTING_FLASH_SOURCE,
                                "event_id": series.get("event_id"),
                                "occurrence_id": series.get(
                                    "occurrence_id"
                                ),
                                "selected": True,
                                "reason_code": (
                                    "EXACT_OCCURRENCE_MATCH_WITH_"
                                    "SELECTED_ACTUAL"
                                ),
                            },
                        ],
                        "canonical_forecast_selection_reason": (
                            "EXACT_OCCURRENCE_MATCH_WITH_"
                            "SELECTED_ACTUAL"
                        ),
                        "provider_accounting": {
                            "primary": _observed_provider_call(
                                flash_provider_order[0],
                                (
                                    _primary_attempt_result(
                                        primary_failure
                                    )
                                    if fallback_used
                                    else "SUCCESS"
                                ),
                            ),
                            "fallbacks": (
                                [
                                    _observed_provider_call(
                                        active_provider_id,
                                        "SUCCESS",
                                    )
                                ]
                                if fallback_used
                                else []
                            ),
                            "selected_source": active_provider_id,
                            "reason_code": (
                                (
                                    "FALLBACK_SELECTED_AFTER_"
                                    "PRIMARY_FAILURE"
                                )
                                if fallback_used
                                else "PRIMARY_SELECTED"
                            ),
                        },
                    }
                )
            restored = (
                self.candidates.persist_candidate(
                    event_key=event_key,
                    candidate=candidate,
                    release_at=release_timestamp,
                    expected_metric_id=spec.event_metric_id,
                    expected_period=expected_period,
                    expected_unit=spec.unit,
                )
                if persist_candidate
                else _validate_unpersisted_candidate(
                    self.candidates,
                    candidate,
                    release_at=release_timestamp,
                    expected_metric_id=spec.event_metric_id,
                    expected_period=expected_period,
                    expected_unit=spec.unit,
                )
            )
            if restored["validation_status"] != "accepted":
                failure = "official_candidate_rejected"
                retried, delayed = (
                    retry_flash_pmi_after_result_failure(failure)
                )
                if delayed is not None:
                    return delayed
                if retried:
                    continue
                return {
                    "status": "NO_DATA",
                    "results": [],
                    "error": failure,
                    "candidate": restored,
                }
            accepted = (
                self.candidates.accepted_official_actual(event_key)
                if persist_candidate
                else restored
            )
            if accepted is None:
                failure = "official_candidate_read_back_failed"
                retried, delayed = (
                    retry_flash_pmi_after_result_failure(failure)
                )
                if delayed is not None:
                    return delayed
                if retried:
                    continue
                return {
                    "status": "FAILED",
                    "results": [],
                    "error": failure,
                    "provider": active_provider_id,
                    "source_series": spec.source_series_id,
                    "provider_call_count": 1,
                    "provider_attempts": [
                        _observed_provider_call(
                            active_provider_id,
                            "SUCCESS",
                        )
                    ],
                }
            accepted = _actual_field_scoped_candidate(accepted)
            break
        return {
            "status": "SUCCEEDED",
            "results": [accepted],
            "resolution": "official_provider",
            "mapping_selected": spec.event_metric_id,
            "source_series": spec.source_series_id,
            "provider": active_provider_id,
            "provider_call_count": 2 if fallback_used else 1,
            "provider_attempts": (
                [
                    _observed_provider_call(
                        flash_provider_order[0],
                        _primary_attempt_result(primary_failure),
                    ),
                    _observed_provider_call(
                        active_provider_id,
                        "SUCCESS",
                    ),
                ]
                if fallback_used
                else [
                    _observed_provider_call(
                        active_provider_id,
                        "SUCCESS",
                    )
                ]
            ),
            "reason_code": (
                "FALLBACK_SELECTED_AFTER_PRIMARY_FAILURE"
                if fallback_used
                else None
            ),
            "candidate_validation": accepted.get("validation_status"),
        }


def _semantic_metric_id(event: dict[str, Any]) -> str | None:
    occurrence = str(
        event.get("occurrence_id")
        or event.get("event_id")
        or event.get("canonical_event_key")
        or ""
    ).casefold()
    if "xtb:146392:2026-07-24" in occurrence:
        return "new_home_sales"
    if "xtb:146945:2026-07-24" in occurrence:
        return "flash_services_pmi"
    name = str(event.get("name") or event.get("event_name") or "").casefold()
    if "new home sales" in name or "vendite di nuove abitazioni" in name:
        return "new_home_sales"
    if "flash services pmi" in name or "pmi servizi flash" in name:
        return "flash_services_pmi"
    explicit = str(event.get("metric_id") or "")
    if explicit in OFFICIAL_METRICS or explicit in UNSUPPORTED_OFFICIAL_METRICS:
        if _metric_semantics_mismatch(event, explicit):
            return None
        return explicit
    for metric in (event.get("enrichment") or {}).get("metrics") or []:
        if isinstance(metric, dict):
            metric_id = str(metric.get("metric_id") or "")
            if metric_id in OFFICIAL_METRICS or metric_id in UNSUPPORTED_OFFICIAL_METRICS:
                if _metric_semantics_mismatch(event, metric_id, metric=metric):
                    continue
                return metric_id
    candidate = candidate_metric_id(
        {
            "metric_id": explicit,
            "event_name": event.get("name") or event.get("event_name"),
            "frequency": event.get("frequency"),
            "evaluation_method": event.get("evaluation_method"),
        }
    )
    if candidate and _metric_semantics_mismatch(event, candidate):
        return None
    return candidate


def _metric_semantics_mismatch(
    event: dict[str, Any],
    metric_id: str,
    *,
    metric: dict[str, Any] | None = None,
) -> bool:
    metric = metric or {}
    return bool(
        metric_semantics_mismatch_reason(
            metric_id,
            name=event.get("name") or event.get("event_name"),
            frequency_hint=" ".join(
                str(item or "")
                for item in (
                    event.get("frequency"),
                    event.get("evaluation_method"),
                    metric.get("frequency"),
                    metric.get("evaluation_method"),
                )
            ),
        )
    )


def _provider_instance(
    provider_config: Any,
    *,
    cache: ProviderCacheRepository,
    settings: Settings,
) -> Any:
    return (
        provider_config(cache, settings)
        if isinstance(provider_config, type)
        else provider_config
    )


def _fetch_flash_services_pmi_provider(
    provider_id: str,
    provider: Any,
    *,
    expected_period: str,
    release_date: Any,
    release_timestamp: Any,
) -> Any:
    if provider_id == "SPGLOBAL":
        return asyncio.run(
            provider.fetch(
                expected_period=expected_period,
                release_date=release_date.isoformat(),
            )
        )
    if provider_id == INVESTING_FLASH_SOURCE:
        return asyncio.run(
            provider.fetch(
                expected_period=expected_period,
                release_date=release_date.isoformat(),
                expected_release_at=release_timestamp,
            )
        )
    raise RuntimeError(
        "FLASH_SERVICES_PMI_RUNTIME_ADAPTER_UNMAPPED:"
        f"{provider_id}"
    )


def _flash_pmi_runtime_policy_invalid(
    *,
    declared_order: tuple[str, ...],
    error: Exception,
    source_series: str,
) -> dict[str, Any]:
    reason = "flash_services_pmi_runtime_policy_mapping_invalid"
    return {
        "status": "NO_DATA",
        "retryable": False,
        "results": [],
        "error": reason,
        "reason_code": reason,
        "policy_error": _redacted_provider_result(error),
        "source_series": source_series,
        "provider_call_count": 0,
        "provider_attempts": [
            _observed_provider_skip(
                provider_id,
                "RUNTIME_ADAPTER_MAPPING_UNAVAILABLE",
            )
            for provider_id in declared_order
        ],
    }


def _flash_pmi_provider_adapter_unavailable(
    *,
    provider_order: tuple[str, ...],
    unavailable_provider: str,
    source_series: str,
) -> dict[str, Any]:
    reason = (
        "flash_services_pmi_provider_adapter_unavailable:"
        f"{unavailable_provider}"
    )
    return {
        "status": "NO_DATA",
        "retryable": False,
        "results": [],
        "error": reason,
        "reason_code": reason,
        "provider": unavailable_provider,
        "source_series": source_series,
        "provider_call_count": 0,
        "provider_attempts": [
            _observed_provider_skip(
                provider_id,
                (
                    "PROVIDER_ADAPTER_UNAVAILABLE"
                    if provider_id == unavailable_provider
                    else "POLICY_CHAIN_BLOCKED_BY_UNAVAILABLE_ADAPTER"
                ),
            )
            for provider_id in provider_order
        ],
    }


def _flash_pmi_fallback_adapter_unavailable(
    *,
    failure: Any,
    primary_provider: str,
    fallback_provider: str,
    source_series: str,
) -> dict[str, Any]:
    primary_result = _primary_attempt_result(failure)
    return {
        **_feed_delayed(
            _redacted_provider_result(failure),
            provider=primary_provider,
            source_series=source_series,
            provider_http_outcome=(
                "HTTP_403"
                if primary_result == "HTTP_403"
                else None
            ),
        ),
        "fallback_reason_code": (
            "flash_services_pmi_fallback_adapter_unavailable:"
            f"{fallback_provider}"
        ),
        "provider_attempts": [
            _observed_provider_call(
                primary_provider,
                primary_result,
            ),
            _observed_provider_skip(
                fallback_provider,
                "FLASH_SERVICES_PMI_FALLBACK_ADAPTER_UNAVAILABLE",
            ),
        ],
    }


def _feed_delayed(
    reason: str,
    *,
    provider: str | None = None,
    source_series: str | None = None,
    provider_http_outcome: str | None = None,
) -> dict[str, Any]:
    return {
        "status": "OFFICIAL_FEED_DELAYED", "retryable": True, "results": [],
        "error": reason, "delay_reason": reason,
        "reason_code": reason,
        "provider": provider,
        "source_series": source_series,
        "provider_http_outcome": provider_http_outcome,
        "provider_call_count": 1,
    }


def _observed_provider_call(
    provider: str,
    result: Any,
) -> dict[str, Any]:
    return {
        "provider": provider,
        "called": True,
        "attempts": 1,
        "result": _redacted_provider_result(result),
        "not_called_reason": None,
        "execution_origin": "PROVIDER_CALL",
    }


def _observed_provider_skip(
    provider: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "provider": provider,
        "called": False,
        "attempts": 0,
        "result": "NOT_CALLED",
        "not_called_reason": reason,
        "execution_origin": "OBSERVED_SKIP",
    }


def _primary_attempt_result(value: Any) -> str:
    reason = _redacted_provider_result(value)
    if reason == "sp_global_public_release_access_restricted":
        return "HTTP_403"
    return reason


def _flash_pmi_all_failed(
    *,
    primary_failure: Any,
    fallback_failure: Any,
    primary_provider: str,
    fallback_provider: str,
    source_series: str,
) -> dict[str, Any]:
    primary_result = _primary_attempt_result(primary_failure)
    fallback_result = _redacted_provider_result(fallback_failure)
    return {
        **_feed_delayed(
            _redacted_provider_result(primary_failure),
            provider=primary_provider,
            source_series=source_series,
            provider_http_outcome=(
                "HTTP_403"
                if primary_result == "HTTP_403"
                else None
            ),
        ),
        "provider_call_count": 2,
        "fallback_reason_code": (
            "all_flash_services_pmi_providers_failed:"
            f"{fallback_result}"
        ),
        "provider_attempts": [
            _observed_provider_call(
                primary_provider,
                primary_result,
            ),
            _observed_provider_call(
                fallback_provider,
                fallback_result,
            ),
        ],
    }


def _redacted_provider_result(value: Any) -> str:
    text = str(value or "UNKNOWN").strip()
    upper = text.upper()
    if any(
        marker in upper
        for marker in (
            "TOKEN",
            "API_KEY",
            "AUTHORIZATION",
            "PASSWORD",
            "SECRET",
        )
    ):
        return "REDACTED_PROVIDER_RESULT"
    return text[:240]


def _release_date(value: Any):
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _validate_unpersisted_candidate(
    repository: EventValueCandidateRepository,
    candidate: dict[str, Any],
    *,
    release_at: Any,
    expected_metric_id: str,
    expected_period: str,
    expected_unit: str,
) -> dict[str, Any]:
    item = dict(candidate)
    decision = repository.policy.validate(
        item,
        field_semantics="actual",
        numerical=True,
    )
    reasons = list(decision.reasons)
    release = _parse_datetime(release_at)
    if release is not None and datetime.now(UTC) < release:
        reasons.append("future_actual_rejected")
    if str(item.get("metric_id") or "").upper() != expected_metric_id.upper():
        reasons.append("metric_id_mismatch")
    if _normalized_period(item.get("period")) != _normalized_period(expected_period):
        reasons.append("period_mismatch")
    if _normalized_token(item.get("unit")) != _normalized_token(expected_unit):
        reasons.append("unit_mismatch")
    try:
        Decimal(str(item.get("value")).replace(",", ""))
    except (InvalidOperation, ValueError):
        reasons.append("actual_not_numerical")
    return {
        **item,
        "source_domain": decision.domain,
        "source_tier": decision.tier,
        "source_classification": decision.classification,
        "policy_version": decision.policy_version,
        "validation_status": (
            "accepted" if decision.accepted and not reasons else "rejected"
        ),
        "warnings": sorted(set(reasons)),
    }


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _normalized_period(value: Any) -> str:
    text = str(value or "").strip().lower().replace("/", "-")
    return text[:7] if len(text) >= 7 else text


def _normalized_token(value: Any) -> str:
    return "_".join(str(value or "").strip().lower().replace("-", "_").split())
