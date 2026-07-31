from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.bea import BeaProvider
from app.providers.bls import BlsProvider
from app.providers.census import CensusProvider
from app.providers.fred import FredProvider
from app.providers.investing_flash_services_pmi import (
    SOURCE as INVESTING_FLASH_SOURCE,
    InvestingFlashServicesPmiProvider,
)
from app.providers.sp_global_pmi import SpGlobalPmiProvider
from app.services.event_value_candidate_repository import EventValueCandidateRepository
from app.services.macro_consensus_service import candidate_metric_id
from app.services.official_actual_semantics import (
    OFFICIAL_METRICS,
    UNSUPPORTED_OFFICIAL_METRICS,
    derive_official_actual,
    metric_semantics_mismatch_reason,
    normalize_reference_period,
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
        provider_config = self.providers.get(spec.provider)
        if provider_config is None:
            return {"status": "NO_DATA", "results": [], "error": "official_provider_adapter_unavailable"}
        provider = (
            provider_config(self.cache, self.settings)
            if isinstance(provider_config, type)
            else provider_config
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
        fallback_used = False
        primary_failure: str | None = None

        def fetch_flash_pmi_fallback(
            failure: str,
        ) -> tuple[Any | None, dict[str, Any] | None]:
            fallback_config = self.providers.get(
                INVESTING_FLASH_SOURCE
            )
            if fallback_config is None:
                return None, {
                    **_feed_delayed(
                        failure,
                        provider=spec.provider,
                        source_series=spec.source_series_id,
                        provider_http_outcome=(
                            "HTTP_403"
                            if failure
                            == "sp_global_public_release_access_restricted"
                            else None
                        ),
                    ),
                    "fallback_reason_code": (
                        "investing_flash_services_pmi_fallback_unavailable"
                    ),
                    "provider_attempts": [
                        _observed_provider_call(
                            spec.provider,
                            _primary_attempt_result(failure),
                        ),
                        _observed_provider_skip(
                            INVESTING_FLASH_SOURCE,
                            (
                                "INVESTING_FLASH_SERVICES_PMI_"
                                "FALLBACK_UNAVAILABLE"
                            ),
                        ),
                    ],
                }
            fallback = (
                fallback_config(self.cache, self.settings)
                if isinstance(fallback_config, type)
                else fallback_config
            )
            try:
                fallback_result = asyncio.run(
                    fallback.fetch(
                        expected_period=expected_period,
                        release_date=release_date.isoformat(),
                        expected_release_at=release_timestamp,
                    )
                )
            except Exception as fallback_exc:
                return None, _flash_pmi_all_failed(
                    primary_failure=failure,
                    fallback_failure=(
                        str(fallback_exc)
                        or type(fallback_exc).__name__
                    ),
                    provider=spec.provider,
                    source_series=spec.source_series_id,
                )
            return fallback_result, None

        try:
            if spec.provider == "CENSUS":
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
            elif spec.provider == "SPGLOBAL":
                if release_date is None:
                    return {
                        "status": "NO_DATA",
                        "retryable": False,
                        "results": [],
                        "error": "official_release_date_missing",
                    }
                result = asyncio.run(
                    provider.fetch(
                        expected_period=expected_period,
                        release_date=release_date.isoformat(),
                    )
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
            if spec.event_metric_id != "flash_services_pmi":
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

        def retry_flash_pmi_after_result_failure(
            failure: str,
        ) -> tuple[bool, dict[str, Any] | None]:
            nonlocal fallback_used, primary_failure, result
            if spec.event_metric_id != "flash_services_pmi":
                return False, None
            if fallback_used:
                return False, _flash_pmi_all_failed(
                    primary_failure=(
                        primary_failure
                        or "SPGLOBAL_PROVIDER_RESULT_REJECTED"
                    ),
                    fallback_failure=failure,
                    provider=spec.provider,
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
            expected_adapter = f"{spec.provider}_OFFICIAL_API"
            official_adapter_valid = (
                result.metadata.source == spec.provider
                and series.get("official_adapter") is True
                and series.get("provider_adapter") == expected_adapter
            )
            fallback_adapter_valid = (
                fallback_used
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
                candidate = derive_official_actual(
                    spec,
                    series,
                    expected_period=expected_period,
                    retrieved_at=retrieved_at,
                    release_timestamp=release_timestamp,
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
            if fallback_used:
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
                                spec.provider,
                                _primary_attempt_result(
                                    primary_failure
                                ),
                            ),
                            "fallbacks": [
                                _observed_provider_call(
                                    INVESTING_FLASH_SOURCE,
                                    "SUCCESS",
                                )
                            ],
                            "selected_source": (
                                INVESTING_FLASH_SOURCE
                            ),
                            "reason_code": (
                                "FALLBACK_SELECTED_AFTER_"
                                "PRIMARY_FAILURE"
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
                    "provider": spec.provider,
                    "source_series": spec.source_series_id,
                    "provider_call_count": 1,
                    "provider_attempts": [
                        _observed_provider_call(
                            spec.provider,
                            "SUCCESS",
                        )
                    ],
                }
            break
        return {
            "status": "SUCCEEDED",
            "results": [accepted],
            "resolution": "official_provider",
            "mapping_selected": spec.event_metric_id,
            "source_series": spec.source_series_id,
            "provider": (
                INVESTING_FLASH_SOURCE if fallback_used else spec.provider
            ),
            "provider_call_count": 2 if fallback_used else 1,
            "provider_attempts": (
                [
                    _observed_provider_call(
                        spec.provider,
                        _primary_attempt_result(primary_failure),
                    ),
                    _observed_provider_call(
                        INVESTING_FLASH_SOURCE,
                        "SUCCESS",
                    ),
                ]
                if fallback_used
                else [
                    _observed_provider_call(
                        spec.provider,
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
    provider: str,
    source_series: str,
) -> dict[str, Any]:
    primary_result = _primary_attempt_result(primary_failure)
    fallback_result = _redacted_provider_result(fallback_failure)
    return {
        **_feed_delayed(
            _redacted_provider_result(primary_failure),
            provider=provider,
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
            _observed_provider_call(provider, primary_result),
            _observed_provider_call(
                INVESTING_FLASH_SOURCE,
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
