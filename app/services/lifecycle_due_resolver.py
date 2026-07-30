from __future__ import annotations

import asyncio
import inspect
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Mapping, Protocol

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.event_driven_lifecycle_service import compute_datum_lifecycle
from app.services.official_actual_semantics import normalize_reference_period
from app.services.research_agent_enablement import research_agent_enablement
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_domain_service import canonical_event_key


class TemporaryLifecycleProviderError(RuntimeError):
    """A deterministic provider failed in a way that may succeed after backoff."""


class LifecycleProviderAdapter(Protocol):
    def resolve(self, item: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class StaticLifecycleProviderAdapter:
    """Deterministic adapter useful for committed/static provider results."""

    status: str
    datum: dict[str, Any] | None = None
    reason: str | None = None
    performs_io: bool = False

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        del item
        return {
            "status": self.status,
            "datum": dict(self.datum or {}),
            "reason": self.reason or "static_provider_result",
            "provider_request_attempted": False,
            "provider_request_completed": False,
            "provider_request_failed": False,
        }


class CallableLifecycleProviderAdapter:
    """Adapts an existing provider/service callable to the lifecycle contract."""

    def __init__(
        self,
        acquire: Callable[[dict[str, Any]], Any],
        *,
        select: Callable[[Any, dict[str, Any]], dict[str, Any] | None],
        name: str,
    ) -> None:
        self.acquire = acquire
        self.select = select
        self.name = name
        self.performs_io = True

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        output = self.acquire(item)
        if inspect.isawaitable(output):
            output = asyncio.run(output)
        datum = self.select(output, item)
        request_telemetry = _provider_request_telemetry(output)
        if not datum:
            if _has_temporary_provider_failure(output):
                raise TemporaryLifecycleProviderError(
                    f"{self.name}_temporary_failure"
                )
            return {
                "status": "NO_DATA",
                "reason": f"{self.name}_exhausted",
                **request_telemetry,
            }
        missing_fields = _missing_requested_fields(
            datum,
            list(item.get("fields_attempted") or []),
        )
        return {
            "status": "PARTIAL" if missing_fields else "RESOLVED",
            "reason": f"{self.name}_resolved",
            "datum": datum,
            "missing_fields": missing_fields,
            **request_telemetry,
        }


class ExactOccurrenceActualProviderAdapter:
    """Select an actual only from a structured, exact-occurrence observation.

    The adapter deliberately treats occurrence identity, release minute,
    reference period, and field lineage as part of the value.  It therefore
    cannot promote a forecast/previous field or a differently-perioded release
    merely because the event title looks similar.
    """

    performs_io = True

    def __init__(
        self,
        acquire: Callable[[dict[str, Any]], Any],
        *,
        name: str = "exact_occurrence_actual",
    ) -> None:
        self.acquire = acquire
        self.name = name

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        output = self.acquire(item)
        if inspect.isawaitable(output):
            output = asyncio.run(output)
        observations = (
            list(output)
            if isinstance(output, (list, tuple))
            else list((output or {}).get("observations") or [])
            if isinstance(output, dict)
            else []
        )
        payload = (
            dict(item.get("payload") or {})
            if isinstance(item.get("payload"), dict)
            else {}
        )
        expected_key = str(
            payload.get("occurrence_id")
            or payload.get("event_id")
            or payload.get("canonical_event_key")
            or canonical_event_key(payload)
        )
        expected_release = parse_datetime(
            item.get("event_at")
            or payload.get("release_at")
            or payload.get("time_utc")
        )
        frequency = str(payload.get("frequency") or "monthly").lower()
        expected_period = normalize_reference_period(
            payload.get("reference_period") or payload.get("period"),
            frequency=frequency,
            release_date=expected_release,
        )
        if not expected_key or expected_release is None or not expected_period:
            return self._no_data("expected_occurrence_semantics_missing")

        identity_matches = [
            dict(row)
            for row in observations
            if isinstance(row, dict)
            and str(
                row.get("occurrence_id")
                or row.get("canonical_event_key")
                or canonical_event_key(row)
            )
            == expected_key
        ]
        if not identity_matches:
            identity_matches = [
                dict(row)
                for row in observations
                if isinstance(row, dict)
                and _semantic_occurrence_match(
                    row,
                    payload=payload,
                    expected_release=expected_release,
                )
            ]
        if not identity_matches:
            return self._no_data("exact_occurrence_not_found")
        release_matches = [
            row
            for row in identity_matches
            if _same_release_minute(row, expected_release)
        ]
        if not release_matches:
            return self._no_data("exact_occurrence_release_mismatch")
        period_matches = [
            row
            for row in release_matches
            if normalize_reference_period(
                row.get("reference_period") or row.get("period"),
                frequency=frequency,
                release_date=expected_release,
            )
            == expected_period
        ]
        if not period_matches:
            return self._no_data("exact_occurrence_reference_period_mismatch")
        semantic_values = {
            (
                str(row.get("actual")),
                str(row.get("forecast")),
                str(row.get("previous")),
                _normalized_unit(row.get("unit")),
            )
            for row in period_matches
        }
        revision: dict[str, Any] | None = None
        if len(semantic_values) > 1:
            comparison_values = {
                (
                    str(row.get("forecast")),
                    str(row.get("previous")),
                    _normalized_unit(row.get("unit")),
                )
                for row in period_matches
            }
            retrieved = [
                parse_datetime(row.get("retrieved_at"))
                for row in period_matches
            ]
            if (
                len(comparison_values) != 1
                or any(value is None for value in retrieved)
                or len(set(retrieved)) != len(retrieved)
                or any(value < expected_release for value in retrieved if value)
            ):
                return self._no_data(
                    "exact_occurrence_observation_ambiguous"
                )
            ordered = sorted(
                zip(retrieved, period_matches),
                key=lambda item: item[0],
            )
            observation = ordered[-1][1]
            revision = {
                "from": ordered[-2][1].get("actual"),
                "to": observation.get("actual"),
                "observation_count": len(ordered),
                "selected_retrieved_at": observation.get("retrieved_at"),
            }
        else:
            observation = max(
                period_matches,
                key=lambda row: (
                    parse_datetime(row.get("retrieved_at"))
                    or expected_release
                ),
            )
        if (
            payload.get("forecast") not in (None, "")
            and payload.get("previous") not in (None, "")
            and observation.get("forecast") == payload.get("previous")
            and observation.get("previous") == payload.get("forecast")
            and payload.get("forecast") != payload.get("previous")
        ):
            return self._no_data("forecast_previous_fields_swapped")
        observed_frequency = str(
            observation.get("frequency") or frequency
        ).lower()
        if observed_frequency != frequency:
            return self._no_data("exact_occurrence_frequency_mismatch")
        expected_unit = _normalized_unit(payload.get("unit"))
        observed_unit = _normalized_unit(observation.get("unit"))
        if expected_unit and observed_unit and expected_unit != observed_unit:
            return self._no_data("exact_occurrence_unit_mismatch")
        if str(
            observation.get("validation_status") or "accepted"
        ).lower() in {
            "rejected",
            "invalid",
            "quarantined",
            "stale",
            "expired",
        }:
            return self._no_data("exact_occurrence_validation_rejected")
        retrieved_at = parse_datetime(observation.get("retrieved_at"))
        if retrieved_at is not None and retrieved_at < expected_release:
            return self._no_data("exact_occurrence_validation_stale")
        raw_lineage = observation.get("field_lineage") or observation.get(
            "lineage"
        )
        lineage = (
            dict(raw_lineage or {})
            if isinstance(raw_lineage, dict)
            else {}
        )
        actual_lineage = (
            dict(lineage.get("actual") or {})
            if isinstance(lineage.get("actual"), dict)
            else {}
        )
        source_field = str(actual_lineage.get("source_field") or "").lower()
        if source_field not in {"actual", "current"}:
            return self._no_data("actual_field_lineage_invalid")
        for field in ("forecast", "previous"):
            if observation.get(field) in (None, ""):
                continue
            proof = lineage.get(field) or {}
            if (
                not isinstance(proof, dict)
                or str(proof.get("source_field") or "").lower() != field
            ):
                return self._no_data(f"{field}_field_lineage_invalid")
        value = observation.get("actual")
        if value in (None, ""):
            return self._no_data("actual_value_missing")

        datum = {
            **payload,
            "canonical_event_key": expected_key,
            "occurrence_id": expected_key,
            "release_at": expected_release.isoformat(),
            "time_utc": expected_release.isoformat(),
            "reference_period": expected_period,
            "period": expected_period,
            "frequency": frequency,
            "actual": value,
            "forecast": (
                observation.get("forecast")
                if observation.get("forecast") not in (None, "")
                else payload.get("forecast")
            ),
            "consensus": payload.get("consensus"),
            "previous": (
                observation.get("previous")
                if observation.get("previous") not in (None, "")
                else payload.get("previous")
            ),
            "unit": observation.get("unit") or payload.get("unit"),
            "release_status": "REVISED" if revision else "PUBLISHED",
            "revision": revision or payload.get("revision"),
            "source": (
                observation.get("source")
                or observation.get("source_originator")
            ),
            "publisher": (
                observation.get("publisher")
                or observation.get("source_originator")
            ),
            "source_originator": observation.get("source_originator"),
            "distribution_source": observation.get("distribution_source"),
            "source_url": observation.get("source_url"),
            "canonical_url": (
                observation.get("canonical_url")
                or observation.get("source_url")
            ),
            "retrieved_at": observation.get("retrieved_at"),
            "validation_status": (
                observation.get("validation_status") or "accepted"
            ),
            "removal_status": (
                None
                if observation.get("occurrence_confirmed") is True
                else payload.get("removal_status")
            ),
            "comparison_lineage": (
                {
                    **(
                        dict(payload.get("comparison_lineage") or {})
                        if isinstance(
                            payload.get("comparison_lineage"), dict
                        )
                        else {}
                    ),
                    "confirmation_source": (
                        observation.get("source")
                        or observation.get("source_originator")
                    ),
                }
                if observation.get("occurrence_confirmed") is True
                else payload.get("comparison_lineage")
            ),
            "field_lineage": {
                **(
                    dict(payload.get("field_lineage") or {})
                    if isinstance(payload.get("field_lineage"), dict)
                    else {}
                ),
                **lineage,
            },
        }
        missing_fields = _missing_requested_fields(
            datum,
            list(item.get("fields_attempted") or []),
        )
        return {
            "status": "PARTIAL" if missing_fields else "RESOLVED",
            "reason": f"{self.name}_resolved",
            "datum": datum,
            "missing_fields": missing_fields,
            "provider_request_attempted": True,
            "provider_request_completed": True,
            "provider_request_failed": False,
        }

    def _no_data(self, reason: str) -> dict[str, Any]:
        return {
            "status": "NO_DATA",
            "reason": f"{self.name}_{reason}",
            "provider_request_attempted": True,
            "provider_request_completed": True,
            "provider_request_failed": False,
        }


class MacroActualLifecycleProviderAdapter:
    """Resolve a past macro occurrence through its official observation feed."""

    def __init__(
        self,
        *,
        settings: Settings,
        event_service: Any,
        actual_resolver: Any,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.event_service = event_service
        self.actual_resolver = actual_resolver
        self.clock = clock or (lambda: datetime.now(UTC))
        self.source_policy = SourcePolicyService(settings.source_policy_path)

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        now = self.clock()
        payload = (
            dict(item.get("payload") or {})
            if isinstance(item.get("payload"), dict)
            else {}
        )
        release = parse_datetime(
            item.get("event_at")
            or payload.get("release_at")
            or payload.get("time_utc")
        )
        if release is None:
            return {
                "status": "NO_DATA",
                "reason": "macro_actual_occurrence_time_missing",
            }
        if release > now:
            return {
                "status": "NO_DATA",
                "reason": "macro_actual_occurrence_not_released",
            }
        atomic_provider_force = (
            str(item.get("resolution_mode") or "")
            == "prepare_atomic_provider_force"
        )
        lookback_start = now - (
            timedelta(
                days=int(
                    self.settings.event_calendar_catchup_lookback_days
                )
            )
            if (
                self.settings.event_calendar_catchup_enabled
                or atomic_provider_force
            )
            else timedelta(
                hours=int(self.settings.lifecycle_startup_catchup_hours)
            )
        )
        if release < lookback_start:
            return {
                "status": "NO_DATA",
                "reason": "macro_actual_occurrence_outside_lookback",
            }

        persisted_occurrence = str(
            payload.get("occurrence_id") or payload.get("event_id") or ""
        )
        expected_key = (
            persisted_occurrence
            if persisted_occurrence.casefold().startswith("xtb:")
            else str(
                payload.get("canonical_event_key")
                or canonical_event_key(payload)
            )
        )
        entity_key = str(item.get("entity_key") or "")
        if entity_key.startswith("event:") and entity_key != expected_key:
            return {
                "status": "NO_DATA",
                "reason": "macro_actual_item_identity_mismatch",
            }
        exact = (
            {
                **payload,
                "occurrence_id": expected_key,
                "canonical_event_key": expected_key,
            }
            if atomic_provider_force
            else None
        )
        if exact is None:
            tolerance = timedelta(minutes=1)
            output = asyncio.run(
                self.event_service.list_events(
                    country=str(payload.get("country") or "US"),
                    start=max(lookback_start, release - tolerance),
                    end=min(now, release + tolerance),
                    enrich=False,
                )
            )
            rows = [_model_dump(row) for row in output]
            exact = next(
                (
                    row
                    for row in rows
                    if canonical_event_key(row) == expected_key
                    and _same_release_minute(row, release)
                ),
                None,
            )
        if exact is None:
            provider_results = [
                _model_dump(result)
                for result in getattr(
                    self.event_service,
                    "last_provider_results",
                    [],
                )
            ]
            if _has_temporary_provider_failure(
                {"provider_results": provider_results}
            ):
                raise TemporaryLifecycleProviderError(
                    "macro_actual_calendar_provider_temporary_failure"
                )
            # The calendar feed is allowed to age out a released occurrence.
            # Its persisted occurrence payload remains the identity anchor; only
            # an admitted official post-release source may supply the actual.
            exact = {
                **payload,
                "occurrence_id": expected_key,
                "canonical_event_key": expected_key,
            }

        direct = _exact_calendar_actual_datum(
            payload,
            exact=exact,
            canonical_key=expected_key,
            release=release,
            source_accepted=not self.source_policy.invalid_sources(
                exact,
                allow_test_reserved=(
                    self.settings.environment.lower() == "test"
                ),
            ),
        )
        if direct is not None:
            missing_fields = _missing_requested_fields(
                direct,
                list(item.get("fields_attempted") or []),
            )
            return {
                "status": "PARTIAL" if missing_fields else "RESOLVED",
                "reason": "exact_calendar_actual_resolved",
                "datum": direct,
                "missing_fields": missing_fields,
                "provider_request_attempted": True,
                "provider_request_completed": True,
                "provider_request_failed": False,
            }

        resolution = self.actual_resolver.resolve_event(
            event_key=expected_key,
            event={
                **exact,
                "canonical_event_key": expected_key,
                "metric_id": (
                    payload.get("metric_id")
                    or exact.get("metric_id")
                ),
                "reference_period": (
                    payload.get("reference_period")
                    or payload.get("period")
                    or exact.get("reference_period")
                    or exact.get("period")
                ),
            },
            temporal_state={"release_at": release.isoformat()},
            expected_period=(
                payload.get("reference_period")
                or payload.get("period")
            ),
            persist_candidate=(
                str(item.get("resolution_mode") or "")
                != "prepare_atomic_provider_force"
            ),
        )
        status = str(resolution.get("status") or "NO_DATA").upper()
        if status == "OFFICIAL_FEED_DELAYED" or resolution.get(
            "retryable"
        ) is True:
            fallback_reason = resolution.get(
                "fallback_reason_code"
            )
            return {
                "status": "DEFERRED",
                "reason": str(
                    fallback_reason
                    or resolution.get("error")
                    or "official_macro_actual_feed_delayed"
                ),
                "reason_code": (
                    fallback_reason
                    or resolution.get("reason_code")
                ),
                "fallback_reason_code": fallback_reason,
                "provider": resolution.get("provider"),
                "source_series": resolution.get("source_series"),
                "provider_http_outcome": resolution.get(
                    "provider_http_outcome"
                ),
                "provider_call_count": int(
                    resolution.get("provider_call_count") or 0
                ),
                "provider_attempts": [
                    dict(item)
                    for item in resolution.get(
                        "provider_attempts"
                    )
                    or []
                    if isinstance(item, dict)
                ],
                "retryable": True,
                "provider_request_attempted": True,
                "provider_request_completed": False,
                "provider_request_failed": True,
            }
        if status in {"FAILED", "TEMPORARY_ERROR"}:
            return {
                "status": "DEFERRED",
                "reason": str(
                    resolution.get("error")
                    or "official_macro_actual_resolution_failed"
                ),
            }
        candidates = [
            candidate
            for candidate in resolution.get("results") or []
            if isinstance(candidate, dict)
            and candidate.get("value") not in (None, "")
        ]
        if status != "SUCCEEDED" or not candidates:
            return {
                "status": "NO_DATA",
                "reason": str(
                    resolution.get("error")
                    or "official_macro_actual_not_published"
                ),
            }

        candidate = candidates[0]
        datum = _official_actual_datum(
            exact,
            candidate=candidate,
            canonical_key=expected_key,
            release=release,
        )
        missing_fields = _missing_requested_fields(
            datum,
            list(item.get("fields_attempted") or []),
        )
        return {
            "status": "PARTIAL" if missing_fields else "RESOLVED",
            "reason": "official_macro_actual_resolved",
            "reason_code": resolution.get("reason_code"),
            "fallback_reason_code": resolution.get(
                "fallback_reason_code"
            ),
            "datum": datum,
            "missing_fields": missing_fields,
            "provider_request_attempted": True,
            "provider_request_completed": True,
            "provider_request_failed": False,
            "candidate": candidate,
            "mapping_selected": resolution.get("mapping_selected"),
            "source_series": resolution.get("source_series"),
            "provider": resolution.get("provider"),
            "provider_call_count": int(
                resolution.get("provider_call_count") or 0
            ),
            "provider_attempts": [
                dict(item)
                for item in resolution.get("provider_attempts") or []
                if isinstance(item, dict)
            ],
            "candidate_validation": resolution.get(
                "candidate_validation"
            ),
        }


class DeterministicLifecycleDueResolver:
    """Revalidate committed data, then use configured deterministic adapters.

    Only provider exhaustion can fall through to a mapped and enabled research
    agent. Temporary provider failures are negative-cached and never invoke AI
    in the same scan.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
        adapters: Mapping[
            str,
            LifecycleProviderAdapter | Callable[[dict[str, Any]], dict[str, Any]],
        ]
        | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.adapters = {
            str(entity_type).lower(): adapter
            for entity_type, adapter in dict(adapters or {}).items()
        }
        self.source_policy = SourcePolicyService(settings.source_policy_path)

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        now = self.clock()
        entity_type = str(item.get("entity_type") or "unknown").lower()
        entity_key = str(item.get("entity_key") or "")
        datum = item.get("payload")
        telemetry: dict[str, Any] = {
            "committed_payload_hit": False,
            "committed_payload_reason": None,
            "provider_request_attempted": False,
            "provider_request_completed": False,
            "provider_request_failed": False,
            "provider_cache_hit": False,
            "provider_negative_cache_hit": False,
        }
        if entity_type == "schedule_only":
            return {
                "status": "NOT_APPLICABLE",
                "reason": "schedule_only_occurrence_has_no_outcome_resolver",
                "ai_eligible": False,
                "agent_status": "DISABLED",
                "execution_status": "NOT_REQUESTED",
                **telemetry,
            }
        if isinstance(datum, dict) and datum:
            telemetry["committed_payload_hit"] = True
            committed = compute_datum_lifecycle(
                entity_type,
                entity_key,
                datum,
                settings=self.settings,
                now=now,
                attempt_count=int(item.get("attempt_count") or 0),
                session_state=item.get("session_state"),
                triggering_event=item.get("triggering_event"),
                refresh_reason="deterministic_committed_payload_revalidated",
            )
            committed_reason = _committed_payload_reason(
                item,
                datum,
                lifecycle=committed,
                source_policy=self.source_policy,
                allow_test_reserved=self.settings.environment.lower() == "test",
            )
            telemetry["committed_payload_reason"] = committed_reason
            if committed_reason == "committed_payload_operationally_fresh":
                return {
                    "status": "RESOLVED",
                    "reason": committed_reason,
                    "datum": datum,
                    "lifecycle": committed,
                    "next_refresh_at": committed.next_refresh_at,
                    "ai_eligible": False,
                    **telemetry,
                }

        cached_until = parse_datetime(
            item.get("negative_cache_expires_at") or item.get("next_retry_at")
        )
        if (
            str(item.get("freshness_state") or "") == "NO_DATA_BACKOFF"
            and cached_until is not None
            and cached_until > now
        ):
            return {
                "status": "DEFERRED",
                "reason": "provider_negative_cache_active",
                "ai_eligible": False,
                "next_retry_at": cached_until.isoformat(),
                **{
                    **telemetry,
                    "provider_negative_cache_hit": True,
                },
            }

        adapter = self.adapters.get(entity_type)
        if adapter is None:
            return self._exhausted(
                item,
                status="EXHAUSTED",
                reason="deterministic_provider_not_configured",
                telemetry=telemetry,
            )
        provider_request_attempted = bool(
            getattr(adapter, "performs_io", True)
        )
        telemetry["provider_request_attempted"] = provider_request_attempted
        try:
            result = (
                adapter.resolve(item)
                if hasattr(adapter, "resolve")
                else adapter(item)
            )
            if inspect.isawaitable(result):
                result = asyncio.run(result)
        except TemporaryLifecycleProviderError as exc:
            return self._temporary_failure(
                item,
                reason=str(exc) or "provider_temporary_error",
                telemetry={
                    **telemetry,
                    "provider_request_failed": provider_request_attempted,
                },
            )
        except (TimeoutError, ConnectionError) as exc:
            return self._temporary_failure(
                item,
                reason=f"provider_temporary_error:{type(exc).__name__}",
                telemetry={
                    **telemetry,
                    "provider_request_failed": provider_request_attempted,
                },
            )

        if not isinstance(result, dict):
            return self._temporary_failure(
                item,
                reason="provider_contract_invalid",
                telemetry={
                    **telemetry,
                    "provider_request_failed": provider_request_attempted,
                },
            )
        provider_request_attempted = bool(
            result.get(
                "provider_request_attempted",
                provider_request_attempted,
            )
        )
        telemetry.update(
            {
                "provider_request_attempted": provider_request_attempted,
                "provider_request_completed": bool(
                    result.get(
                        "provider_request_completed",
                        provider_request_attempted,
                    )
                ),
                "provider_request_failed": bool(
                    result.get("provider_request_failed")
                ),
                "provider_cache_hit": bool(
                    result.get("provider_cache_hit")
                ),
            }
        )
        status = str(result.get("status") or "NO_DATA").upper()
        if status in {
            "DEFERRED",
            "TEMPORARY_ERROR",
            "TIMEOUT",
            "RATE_LIMITED",
            "UNAVAILABLE",
        }:
            failure_telemetry = {
                **telemetry,
                "provider_request_completed": False,
                "provider_request_failed": provider_request_attempted,
                **{
                    key: result.get(key)
                    for key in (
                        "reason_code",
                        "provider",
                        "source_series",
                        "provider_http_outcome",
                        "provider_call_count",
                        "provider_attempts",
                        "retryable",
                    )
                    if result.get(key) is not None
                },
            }
            return self._temporary_failure(
                item,
                reason=str(result.get("reason") or status.lower()),
                telemetry=failure_telemetry,
            )
        if status in {"EXHAUSTED", "NOT_FOUND", "NO_DATA", "NOT_CONFIGURED"}:
            exhausted_telemetry = {
                **telemetry,
                **{
                    key: result.get(key)
                    for key in (
                        "reason_code",
                        "provider",
                        "source_series",
                        "provider_http_outcome",
                        "provider_call_count",
                        "provider_attempts",
                        "retryable",
                    )
                    if result.get(key) is not None
                },
            }
            return self._exhausted(
                item,
                status="NO_DATA" if status == "NO_DATA" else "EXHAUSTED",
                reason=str(result.get("reason") or "deterministic_provider_exhausted"),
                telemetry=exhausted_telemetry,
            )
        if status not in {"RESOLVED", "FRESH", "SUCCEEDED", "PARTIAL"}:
            return self._temporary_failure(
                item,
                reason=f"provider_status_invalid:{status}",
                telemetry={
                    **telemetry,
                    "provider_request_completed": False,
                    "provider_request_failed": provider_request_attempted,
                },
            )
        provider_datum = result.get("datum")
        if not isinstance(provider_datum, dict) or not provider_datum:
            return self._exhausted(
                item,
                status="NO_DATA",
                reason="deterministic_provider_returned_no_data",
                telemetry=telemetry,
            )
        lifecycle = compute_datum_lifecycle(
            entity_type,
            entity_key,
            provider_datum,
            settings=self.settings,
            now=now,
            attempt_count=int(item.get("attempt_count") or 0),
            session_state=item.get("session_state"),
            triggering_event=item.get("triggering_event"),
            refresh_reason="deterministic_provider_resolved",
        )
        if (
            entity_type == "macro_actual"
            and provider_datum.get("actual") not in (None, "")
        ):
            # Keep the cadence-derived content and refresh deadlines.  A
            # recent retrieval must never make an old official release fresh.
            lifecycle = replace(
                lifecycle,
                next_retry_at=None,
                retry_class=None,
                negative_cache_key=None,
                negative_cache_expires_at=None,
                refresh_reason="official_macro_actual_resolved",
            )
        if lifecycle.freshness_state != "FRESH":
            return self._exhausted(
                item,
                status="NO_DATA",
                reason="deterministic_provider_result_not_fresh",
                telemetry=telemetry,
            )
        if status == "PARTIAL":
            decision = research_agent_enablement(
                self.settings,
                topic=_topic_for_entity(entity_type),
            )
            return {
                **result,
                "status": "PARTIAL",
                "datum": provider_datum,
                "lifecycle": lifecycle,
                "missing_fields": list(
                    result.get("missing_fields")
                    or _missing_requested_fields(
                        provider_datum,
                        list(item.get("fields_attempted") or []),
                    )
                ),
                "ai_eligible": bool(decision["agent_enabled"]),
                "agent_status": (
                    "ENABLED" if decision["agent_enabled"] else "DISABLED"
                ),
                "execution_status": (
                    "ELIGIBLE"
                    if decision["agent_enabled"]
                    else "NOT_REQUESTED"
                ),
                **telemetry,
            }
        return {
            **result,
            "status": "RESOLVED",
            "datum": provider_datum,
            "lifecycle": lifecycle,
            "next_refresh_at": lifecycle.next_refresh_at,
            "ai_eligible": False,
            **telemetry,
        }

    def _exhausted(
        self,
        item: dict[str, Any],
        *,
        status: str,
        reason: str,
        telemetry: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        decision = research_agent_enablement(
            self.settings,
            topic=_topic_for_entity(str(item.get("entity_type") or "")),
        )
        event_at = parse_datetime(item.get("event_at"))
        retry_deadline_exhausted = bool(
            event_at is not None
            and self.clock()
            >= event_at
            + timedelta(
                hours=int(self.settings.lifecycle_retry_deadline_hours)
            )
        )
        terminal = retry_deadline_exhausted
        payload = (
            dict(item.get("payload") or {})
            if isinstance(item.get("payload"), dict)
            else {}
        )
        payload.setdefault("event_at", item.get("event_at"))
        payload.setdefault("actual", None)
        lifecycle = compute_datum_lifecycle(
            str(item.get("entity_type") or "unknown"),
            str(item.get("entity_key") or ""),
            payload,
            settings=self.settings,
            now=self.clock(),
            attempt_count=int(item.get("attempt_count") or 0) + 1,
            no_data=not terminal,
            fields_attempted=list(item.get("fields_attempted") or []),
            session_state=item.get("session_state"),
            triggering_event=item.get("triggering_event"),
            retry_class="NO_DATA",
            refresh_reason=reason,
        )
        if terminal:
            lifecycle = replace(
                lifecycle,
                freshness_state="EXHAUSTED_NO_DATA",
                next_refresh_at=None,
                next_retry_at=None,
                retry_class="EXHAUSTED_NO_DATA",
                negative_cache_key=None,
                negative_cache_expires_at=None,
                refresh_reason=reason,
            )
        return {
            "status": status,
            "reason": reason,
            "lifecycle": lifecycle,
            "next_retry_at": lifecycle.next_retry_at,
            "ai_eligible": bool(
                decision["agent_enabled"] and not terminal
            ),
            "agent_status": (
                "ENABLED" if decision["agent_enabled"] else "DISABLED"
            ),
            "execution_status": (
                "ELIGIBLE"
                if decision["agent_enabled"] and not terminal
                else "NOT_REQUESTED"
            ),
            "data_outcome": (
                "NO_DATA" if terminal else "PENDING"
            ),
            "retry_deadline_exhausted": terminal,
            "enablement": decision,
            **dict(telemetry or {}),
        }

    def _temporary_failure(
        self,
        item: dict[str, Any],
        *,
        reason: str,
        telemetry: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        lifecycle = compute_datum_lifecycle(
            str(item.get("entity_type") or "unknown"),
            str(item.get("entity_key") or ""),
            {"refresh_reason": reason},
            settings=self.settings,
            now=self.clock(),
            attempt_count=int(item.get("attempt_count") or 0) + 1,
            no_data=True,
            fields_attempted=list(item.get("fields_attempted") or []),
            session_state=item.get("session_state"),
            triggering_event=item.get("triggering_event"),
            retry_class="PROVIDER_TEMPORARY",
            refresh_reason=reason,
        )
        return {
            "status": "DEFERRED",
            "reason": reason,
            "datum": {
                "reason": reason,
                "fields_attempted": list(item.get("fields_attempted") or []),
            },
            "lifecycle": lifecycle,
            "next_retry_at": lifecycle.next_retry_at,
            "ai_eligible": False,
            **dict(telemetry or {}),
        }


def existing_lifecycle_provider_adapters(
    *,
    macro_service: Any,
    event_service: Any,
    nasdaq_data_service: Any,
    settings: Settings | None = None,
    official_actual_resolver: Any | None = None,
    clock: Callable[[], datetime] | None = None,
    cftc_provider: Any | None = None,
    cboe_risk_indices_provider: Any | None = None,
    cboe_vix_futures_provider: Any | None = None,
    cboe_put_call_provider: Any | None = None,
) -> dict[str, LifecycleProviderAdapter]:
    """Wire only provider services that already exist in application bootstrap."""

    def macro(_: dict[str, Any]) -> Any:
        return macro_service.latest()

    async def schedule_events(_: dict[str, Any]) -> Any:
        rows = await event_service.upcoming(country="US", days=14)
        return {
            "events": [_model_dump(row) for row in rows],
            "provider_results": [
                _model_dump(result)
                for result in getattr(
                    event_service,
                    "last_provider_results",
                    [],
                )
            ],
        }

    def nasdaq(method: str) -> Callable[[dict[str, Any]], Any]:
        return lambda _: getattr(nasdaq_data_service, method)()

    macro_actual_adapter: LifecycleProviderAdapter = (
        MacroActualLifecycleProviderAdapter(
            settings=settings,
            event_service=event_service,
            actual_resolver=official_actual_resolver,
            clock=clock,
        )
        if settings is not None and official_actual_resolver is not None
        else StaticLifecycleProviderAdapter(
            status="NOT_CONFIGURED",
            reason="official_macro_actual_resolver_not_configured",
        )
    )
    adapters: dict[str, LifecycleProviderAdapter] = {
        "macro_snapshot": CallableLifecycleProviderAdapter(
            macro,
            select=_select_macro_snapshot,
            name="macro_service",
        ),
        "vix": CallableLifecycleProviderAdapter(
            macro,
            select=_select_vix,
            name="fred_vix",
        ),
        "macro_schedule": CallableLifecycleProviderAdapter(
            schedule_events,
            select=_select_event,
            name="event_service",
        ),
        "macro_actual": macro_actual_adapter,
        "nasdaq_100": CallableLifecycleProviderAdapter(
            nasdaq("qqq_holdings"),
            select=_select_model,
            name="qqq_holdings_provider",
        ),
        "mega_cap_semiconductors": CallableLifecycleProviderAdapter(
            nasdaq("mega_cap_snapshot"),
            select=_select_model,
            name="mega_cap_snapshot_provider",
        ),
        "earnings": CallableLifecycleProviderAdapter(
            nasdaq("earnings"),
            select=_select_earnings,
            name="earnings_provider",
        ),
        "earnings_schedule": CallableLifecycleProviderAdapter(
            nasdaq("earnings"),
            select=_select_earnings,
            name="earnings_provider",
        ),
        "earnings_actual": CallableLifecycleProviderAdapter(
            nasdaq("earnings"),
            select=_select_earnings,
            name="earnings_provider",
        ),
        "news": CallableLifecycleProviderAdapter(
            nasdaq("latest_news"),
            select=_select_model,
            name="news_provider",
        ),
        "breaking_news": CallableLifecycleProviderAdapter(
            nasdaq("latest_news"),
            select=_select_model,
            name="news_provider",
        ),
    }
    if cftc_provider is not None:
        cot_adapter = CallableLifecycleProviderAdapter(
            lambda _: cftc_provider.fetch_nasdaq(),
            select=_select_found_payload,
            name="cftc_cot_provider",
        )
        adapters["cot"] = cot_adapter
        adapters["cot_positioning"] = cot_adapter
        adapters["cot_publication"] = cot_adapter
    if cboe_risk_indices_provider is not None:
        adapters["vvix"] = CallableLifecycleProviderAdapter(
            lambda _: cboe_risk_indices_provider.fetch(),
            select=lambda output, item: _select_cboe_index(
                output,
                item,
                key="vvix",
            ),
            name="cboe_vvix_provider",
        )
        adapters["skew"] = CallableLifecycleProviderAdapter(
            lambda _: cboe_risk_indices_provider.fetch(),
            select=lambda output, item: _select_cboe_index(
                output,
                item,
                key="skew",
            ),
            name="cboe_skew_provider",
        )
    if cboe_vix_futures_provider is not None:
        adapters["vix_futures"] = CallableLifecycleProviderAdapter(
            lambda _: cboe_vix_futures_provider.fetch(),
            select=_select_found_payload,
            name="cboe_vix_futures_provider",
        )
    if cboe_put_call_provider is not None:
        adapters["put_call"] = CallableLifecycleProviderAdapter(
            lambda _: cboe_put_call_provider.fetch(),
            select=_select_found_payload,
            name="cboe_put_call_provider",
        )
    return adapters


def _model_dump(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    dump = getattr(value, "model_dump", None)
    return dump(mode="json") if callable(dump) else {}


def _same_release_minute(
    event: dict[str, Any],
    expected: datetime,
) -> bool:
    observed = parse_datetime(
        event.get("release_at") or event.get("time_utc")
    )
    return bool(
        observed is not None
        and observed.replace(second=0, microsecond=0)
        == expected.replace(second=0, microsecond=0)
    )


def _exact_calendar_actual_datum(
    payload: dict[str, Any],
    *,
    exact: dict[str, Any],
    canonical_key: str,
    release: datetime,
    source_accepted: bool,
) -> dict[str, Any] | None:
    if not source_accepted or exact.get("actual") in (None, ""):
        return None
    expected_frequency = str(payload.get("frequency") or "monthly").lower()
    frequency = str(exact.get("frequency") or expected_frequency).lower()
    if frequency != expected_frequency:
        return None
    validation_status = str(
        exact.get("validation_status") or "accepted"
    ).lower()
    if validation_status in {
        "rejected",
        "invalid",
        "quarantined",
        "stale",
        "expired",
    }:
        return None
    retrieved_at = parse_datetime(exact.get("retrieved_at"))
    if retrieved_at is not None and retrieved_at < release:
        return None
    expected_period = normalize_reference_period(
        payload.get("reference_period") or payload.get("period"),
        frequency=frequency,
        release_date=release,
    )

    observed_period = normalize_reference_period(
        exact.get("reference_period") or exact.get("period"),
        frequency=frequency,
        release_date=release,
    )
    if not expected_period or observed_period != expected_period:
        return None
    raw_lineage = exact.get("field_lineage") or exact.get("lineage") or {}
    lineage = dict(raw_lineage) if isinstance(raw_lineage, dict) else {}
    actual_lineage = lineage.get("actual") or {}
    if (
        not isinstance(actual_lineage, dict)
        or str(actual_lineage.get("source_field") or "").lower()
        not in {"actual", "current"}
    ):
        return None
    for field in ("forecast", "previous"):
        if exact.get(field) in (None, ""):
            continue
        field_proof = lineage.get(field) or {}
        if (
            not isinstance(field_proof, dict)
            or str(field_proof.get("source_field") or "").lower() != field
        ):
            return None
    expected_unit = _normalized_unit(payload.get("unit"))
    observed_unit = _normalized_unit(exact.get("unit"))
    if expected_unit and observed_unit and expected_unit != observed_unit:
        return None
    source = exact.get("source") or exact.get("source_originator")
    source_url = exact.get("source_url") or exact.get("canonical_url")
    if not source or not source_url:
        return None
    return {
        **payload,
        "canonical_event_key": canonical_key,
        "occurrence_id": canonical_key,
        "release_at": release.isoformat(),
        "time_utc": release.isoformat(),
        "reference_period": observed_period,
        "period": observed_period,
        "frequency": frequency,
        "actual": exact["actual"],
        "forecast": (
            exact.get("forecast")
            if exact.get("forecast") not in (None, "")
            else payload.get("forecast")
        ),
        "consensus": (
            exact.get("consensus")
            if exact.get("consensus") not in (None, "")
            else payload.get("consensus")
        ),
        "previous": (
            exact.get("previous")
            if exact.get("previous") not in (None, "")
            else payload.get("previous")
        ),
        "unit": exact.get("unit") or payload.get("unit"),
        "release_status": "PUBLISHED",
        "source": source,
        "publisher": exact.get("publisher") or source,
        "source_originator": exact.get("source_originator") or source,
        "distribution_source": exact.get("distribution_source"),
        "source_url": source_url,
        "canonical_url": exact.get("canonical_url") or source_url,
        "retrieved_at": exact.get("retrieved_at"),
        "validation_status": exact.get("validation_status") or "accepted",
        "field_lineage": {
            **(
                dict(payload.get("field_lineage") or {})
                if isinstance(payload.get("field_lineage"), dict)
                else {}
            ),
            **lineage,
        },
    }


def _semantic_occurrence_match(
    event: dict[str, Any],
    *,
    payload: dict[str, Any],
    expected_release: datetime,
) -> bool:
    def normalized_name(value: Any) -> str:
        return " ".join(
            re.findall(r"[a-z0-9]+", str(value or "").casefold())
        )

    expected_name = normalized_name(
        payload.get("name")
        or payload.get("event_name")
        or payload.get("title")
    )
    observed_name = normalized_name(
        event.get("name")
        or event.get("event_name")
        or event.get("title")
    )
    expected_country = str(payload.get("country") or "").upper()
    observed_country = str(event.get("country") or "").upper()
    return bool(
        expected_name
        and expected_name == observed_name
        and expected_country
        and expected_country == observed_country
        and _same_release_minute(event, expected_release)
    )


def _normalized_unit(value: Any) -> str:
    text = str(value or "").strip().casefold().replace("-", "_")
    aliases = {
        "k": "thousands_annual_rate",
        "thousand": "thousands_annual_rate",
        "thousands": "thousands_annual_rate",
        "index_points": "index",
        "points": "index",
    }
    return aliases.get(text, text)


def _official_actual_datum(
    event: dict[str, Any],
    *,
    candidate: dict[str, Any],
    canonical_key: str,
    release: datetime,
) -> dict[str, Any]:
    value = candidate.get("value")
    source = candidate.get("source") or candidate.get("publisher")
    source_url = (
        candidate.get("source_url")
        or candidate.get("canonical_url")
    )
    distributor = (
        candidate.get("distribution_source")
        or candidate.get("acquisition_provider")
        or event.get("source")
        or event.get("provider")
    )
    distributor_url = (
        candidate.get("distribution_source_url")
        or candidate.get("source_url")
        or event.get("source_url")
    )
    acquisition_source = (
        candidate.get("acquisition_provider")
        or candidate.get("distribution_source")
        or source
    )
    actual_is_official = bool(
        candidate.get("actual_is_official", True)
    )
    actual_lineage = {
        "source": source,
        "publisher": candidate.get("publisher"),
        "distributor": distributor,
        "distributor_url": distributor_url,
        "source_url": source_url,
        "canonical_url": candidate.get("canonical_url"),
        "source_tier": candidate.get("source_tier") or 1,
        "source_classification": (
            candidate.get("source_classification")
            or (
                "official_source"
                if actual_is_official
                else "secondary_market_source"
            )
        ),
        "provider_adapter": candidate.get("provider_adapter"),
        "metric_id": (
            candidate.get("event_metric_id")
            or candidate.get("metric_id")
        ),
        "source_series_id": candidate.get("source_series_id"),
        "reference_period": (
            candidate.get("reference_period")
            or candidate.get("period")
        ),
        "retrieved_at": candidate.get("retrieved_at"),
        "released_at": candidate.get("released_at") or candidate.get("release_timestamp"),
        "validation_timestamp": candidate.get("validation_timestamp"),
        "frequency": candidate.get("frequency"),
        "unit": candidate.get("unit"),
        "raw_lineage_redacted": candidate.get("raw_lineage_redacted"),
        "validation_status": (
            candidate.get("validation_status") or "accepted"
        ),
        "source_field": "actual",
    }
    enrichment = (
        dict(event.get("enrichment") or {})
        if isinstance(event.get("enrichment"), dict)
        else {}
    )
    field_lineage = dict(enrichment.get("field_lineage") or {})
    persisted_lineage = (
        event.get("lineage")
        if isinstance(event.get("lineage"), dict)
        else {}
    )
    persisted_fields = dict(
        persisted_lineage.get("field_lineage") or {}
    )
    if "forecast" not in field_lineage:
        scheduled_forecast = (
            persisted_fields.get("forecast")
            or persisted_fields.get("consensus")
        )
        if isinstance(scheduled_forecast, dict):
            field_lineage["forecast"] = {
                **scheduled_forecast,
                "field_semantics": "forecast",
            }
    field_lineage["actual"] = actual_lineage
    if candidate.get("previous") not in (None, ""):
        field_lineage["previous"] = {
            **actual_lineage,
            "field_semantics": "previous",
            "source_field": "previous",
            "value": candidate.get("previous"),
            "reference_period": candidate.get(
                "previous_reference_period"
            ),
            "derivation": "previous_official_series_observation",
        }
    enrichment.update(
        {
            "actual": value,
            "forecast": event.get("forecast"),
            "previous": (
                candidate.get("previous")
                if candidate.get("previous") not in (None, "")
                else event.get("previous")
            ),
            "source": source,
            "source_url": source_url,
            "field_lineage": field_lineage,
        }
    )
    return {
        **event,
        "canonical_event_key": canonical_key,
        "release_at": release.isoformat(),
        "time_utc": release.isoformat(),
        "actual": value,
        "previous": (
            candidate.get("previous")
            if candidate.get("previous") not in (None, "")
            else event.get("previous")
        ),
        "previous_revised": candidate.get("previous_revised"),
        "forecast": event.get("forecast"),
        "metric_id": (
            candidate.get("event_metric_id")
            or candidate.get("metric_id")
            or event.get("metric_id")
        ),
        "reference_period": (
            candidate.get("reference_period")
            or candidate.get("period")
            or event.get("reference_period")
            or event.get("period")
        ),
        "data_as_of": (
            candidate.get("retrieved_at")
            or candidate.get("published_at")
            or release.isoformat()
        ),
        "published_at": (
            candidate.get("published_at") or release.isoformat()
        ),
        "released_at": (
            candidate.get("released_at")
            or candidate.get("release_timestamp")
            or release.isoformat()
        ),
        "validation_timestamp": (
            candidate.get("validation_timestamp")
            or candidate.get("retrieved_at")
        ),
        # A newly admitted official actual starts a fresh lifecycle.  Do not
        # inherit the pre-release occurrence's expired cache/retry horizon.
        "valid_until": candidate.get("valid_until"),
        "next_refresh_at": candidate.get("next_refresh_at"),
        "next_retry_at": None,
        "frequency": candidate.get("frequency"),
        "unit": candidate.get("unit"),
        "source": source,
        "publisher": candidate.get("publisher") or source,
        "source_url": source_url,
        "distributor": distributor,
        "distributor_url": distributor_url,
        "acquisition_provider": acquisition_source,
        "actual_source": acquisition_source,
        "actual_source_url": source_url,
        "source_lineage": [actual_lineage],
        "comparison_lineage": {
            **dict(event.get("comparison_lineage") or {}),
            "scheduled_previous": event.get("previous"),
            "scheduled_previous_lineage": persisted_fields.get(
                "previous"
            ),
            "official_previous": candidate.get("previous"),
            "official_previous_semantics": (
                "previous_official_series_observation"
            ),
        },
        "acquisition_method": "api_provider",
        "actual_is_official": actual_is_official,
        "awaiting_actual": False,
        "status": "RELEASED",
        "release_status": "RELEASED",
        "enrichment": enrichment,
    }


def _select_model(output: Any, _: dict[str, Any]) -> dict[str, Any] | None:
    value = _model_dump(output)
    return value or None


def _select_macro_snapshot(
    output: Any,
    _: dict[str, Any],
) -> dict[str, Any] | None:
    value = _model_dump(output)
    series = value.get("series") or []
    return value if series else None


def _select_vix(output: Any, _: dict[str, Any]) -> dict[str, Any] | None:
    value = _model_dump(output)
    for series in value.get("series") or []:
        if str(series.get("series_id") or "").upper() == "VIXCLS":
            return dict(series)
    fred_results = [
        _model_dump(raw)
        for raw in value.get("provider_results") or []
        if "FRED" in str(_model_dump(raw).get("source") or "").upper()
    ]
    if fred_results and all(result.get("errors") for result in fred_results):
        raise TemporaryLifecycleProviderError("fred_vix_temporary_failure")
    return None


def _select_event(
    output: Any,
    item: dict[str, Any],
) -> dict[str, Any] | None:
    entity_key = str(item.get("entity_key") or "")
    rows = [
        _model_dump(row)
        for row in (
            output.get("events") or []
            if isinstance(output, dict)
            else output or []
        )
    ]
    return next(
        (
            row
            for row in rows
            if entity_key
            in {
                str(row.get("canonical_event_key") or ""),
                str(row.get("event_id") or ""),
                str(row.get("source_event_id") or ""),
            }
        ),
        None,
    )


def _select_earnings(
    output: Any,
    item: dict[str, Any],
) -> dict[str, Any] | None:
    value = _model_dump(output)
    rows = (
        value.get("events")
        or value.get("upcoming")
        or value.get("earnings")
        or []
    )
    target = str(item.get("entity_key") or "").upper()
    return next(
        (
            dict(row)
            for row in rows
            if isinstance(row, dict)
            and _earnings_key(row) == target
        ),
        None,
    )


def _select_found_payload(
    output: Any,
    _: dict[str, Any],
) -> dict[str, Any] | None:
    value = _model_dump(output)
    if str(value.get("status") or "").lower() not in {
        "found",
        "valid",
        "available",
        "partial",
    }:
        return None
    return {
        **value,
        "acquisition_method": "api_provider",
    }


def _select_cboe_index(
    output: Any,
    _: dict[str, Any],
    *,
    key: str,
) -> dict[str, Any] | None:
    value = _model_dump(output)
    index = (value.get("indices") or {}).get(key)
    if not isinstance(index, dict) or index.get("current_price") in (None, ""):
        return None
    return {
        **index,
        "value": index.get("current_price"),
        "data_as_of": index.get("provider_timestamp")
        or index.get("retrieved_at"),
        "valid_until": index.get("valid_until")
        or value.get("valid_until"),
        "acquisition_method": "api_provider",
        "source_lineage": [
            {
                "source": index.get("source") or value.get("source"),
                "source_url": index.get("source_url")
                or value.get("source_url"),
                "provider_type": "OFFICIAL_EXCHANGE",
            }
        ],
    }


def _earnings_key(value: dict[str, Any]) -> str:
    issuer = str(
        value.get("ticker")
        or value.get("symbol")
        or value.get("issuer")
        or value.get("company")
        or ""
    ).upper()
    day = str(
        value.get("event_at")
        or value.get("earnings_date")
        or value.get("date")
        or ""
    )[:10]
    return f"{issuer}:{day}".strip(":")


def _has_temporary_provider_failure(output: Any) -> bool:
    value = _model_dump(output)
    provider_results = value.get("provider_results") or []
    attempted = 0
    failed = 0
    for raw in provider_results:
        result = _model_dump(raw)
        attempted += 1
        if result.get("errors"):
            failed += 1
    if attempted and failed == attempted:
        return True
    quality = value.get("data_quality") or value.get("quality") or {}
    errors = quality.get("errors") if isinstance(quality, dict) else []
    status = str(value.get("status") or "").lower()
    return bool(errors) and status in {
        "",
        "failed",
        "provider_failed",
        "unavailable",
        "not_found",
    }


def _missing_requested_fields(
    datum: dict[str, Any],
    requested: list[str],
) -> list[str]:
    return sorted(
        {
            str(field)
            for field in requested
            if str(field)
            and datum.get(str(field)) in (None, "", [], {})
        }
    )


_NO_DATA_ENVELOPE_STATUSES = frozenset(
    {
        "DISABLED",
        "NOT_CONFIGURED",
        "NOT_FOUND",
        "NO_DATA",
        "NO_DATA_AVAILABLE",
        "PROVIDER_FAILED",
        "QUARANTINED",
        "REJECTED",
    }
)
_NON_OPERATIONAL_PAYLOAD_KEYS = frozenset(
    {
        "fields_attempted",
        "negative_cache_expires_at",
        "negative_cache_key",
        "next_refresh_at",
        "next_retry_at",
        "reason",
        "refresh_reason",
        "retry_class",
        "searched_at",
        "session_state",
        "sources_attempted",
        "status",
        "triggering_event",
        "valid_from",
        "valid_until",
    }
)


def _committed_payload_reason(
    item: dict[str, Any],
    datum: dict[str, Any],
    *,
    lifecycle: Any,
    source_policy: SourcePolicyService,
    allow_test_reserved: bool,
) -> str:
    status = str(datum.get("status") or "").upper()
    if status in _NO_DATA_ENVELOPE_STATUSES or (
        datum.get("value") is None
        and str(datum.get("reason") or "").lower()
        in {
            "no_fresh_verified_source",
            "no_data",
            "not_found",
            "provider_failed",
        }
    ):
        return "committed_payload_no_data_envelope"
    if not _committed_payload_has_operational_value(item, datum):
        return "committed_payload_temporally_valid_but_not_operational"
    if not _committed_payload_source_verified(
        datum,
        source_policy=source_policy,
        allow_test_reserved=allow_test_reserved,
    ):
        return "committed_payload_source_unverified"
    if str(lifecycle.freshness_state) == "FRESH":
        return "committed_payload_operationally_fresh"
    return "committed_payload_temporally_stale"


def _committed_payload_has_operational_value(
    item: dict[str, Any],
    datum: dict[str, Any],
) -> bool:
    requested = [
        str(field)
        for field in item.get("fields_attempted") or []
        if str(field)
    ]
    if requested:
        return any(
            datum.get(field) not in (None, "", [], {})
            for field in requested
        )
    return any(
        key not in _NON_OPERATIONAL_PAYLOAD_KEYS
        and value not in (None, "", [], {}, False)
        for key, value in datum.items()
    )


def _committed_payload_source_verified(
    datum: dict[str, Any],
    *,
    source_policy: SourcePolicyService,
    allow_test_reserved: bool,
) -> bool:
    validation = (
        datum.get("validation")
        if isinstance(datum.get("validation"), dict)
        else {}
    )
    if (
        str(datum.get("source_classification") or "").lower()
        == "invalid_source"
        or str(datum.get("source_audit_status") or "").upper()
        in {"QUARANTINED", "REJECTED"}
        or str(datum.get("verification_status") or "").upper()
        in {"QUARANTINED", "REJECTED", "UNVERIFIED"}
        or str(validation.get("status") or "").lower() == "rejected"
    ):
        return False
    raw_lineage = datum.get("source_lineage") or datum.get("lineage") or []
    if isinstance(raw_lineage, dict):
        raw_lineage = [raw_lineage]
    lineage = [
        dict(entry)
        for entry in raw_lineage
        if isinstance(entry, dict)
    ]
    if datum.get("source") or datum.get("provider"):
        lineage.append(datum)
    for entry in lineage:
        classification = str(
            entry.get("source_classification") or ""
        ).lower()
        verification = str(
            entry.get("verification_status")
            or entry.get("validation_status")
            or ""
        ).upper()
        if classification == "invalid_source" or verification in {
            "QUARANTINED",
            "REJECTED",
            "UNVERIFIED",
        }:
            continue
        source_url = entry.get("source_url") or entry.get("canonical_url")
        if source_url and source_policy.validate_url(
            str(source_url),
            allow_test_reserved=allow_test_reserved,
        ).accepted:
            return True
        if (
            entry.get("source")
            or entry.get("provider")
            or entry.get("publisher")
        ) and (
            verification in {"ACCEPTED", "VERIFIED", "SUCCEEDED", "VALID"}
            or classification
            in {"official_source", "primary_source", "verified_source"}
            or entry.get("provider_type")
        ):
            return True
    return False


def _provider_request_telemetry(output: Any) -> dict[str, bool]:
    value = _model_dump(output)
    candidates = [
        value,
        value.get("metadata") if isinstance(value, dict) else None,
        value.get("data_quality") if isinstance(value, dict) else None,
        value.get("diagnostics") if isinstance(value, dict) else None,
    ]
    counts: list[int] = []
    cache_used = False
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        cache_used = cache_used or bool(candidate.get("cache_used"))
        for field in ("actual_network_calls", "provider_calls"):
            if candidate.get(field) is None:
                continue
            try:
                counts.append(max(int(candidate[field]), 0))
            except (TypeError, ValueError):
                continue
    attempted = any(count > 0 for count in counts) if counts else True
    cache_hit = bool(cache_used and not attempted)
    return {
        "provider_request_attempted": attempted,
        "provider_request_completed": attempted,
        "provider_request_failed": False,
        "provider_cache_hit": cache_hit,
    }


def _topic_for_entity(entity_type: str) -> str:
    normalized = str(entity_type or "").lower()
    if normalized in {"vix", "vvix", "vix_futures", "put_call", "skew"}:
        return "vix_risk"
    if normalized in {"cot", "cot_publication"}:
        return "cot_positioning"
    if normalized.startswith("earnings"):
        return (
            "earnings_intelligence"
            if normalized == "earnings_intelligence"
            else "earnings"
        )
    if normalized in {
        "options_positioning",
        "market_internals",
        "cross_asset_context",
        "geopolitical_regulatory_risk",
        "nasdaq_100",
        "mega_cap_semiconductors",
    }:
        return normalized
    if normalized in {"macro_actual", "macro_schedule", "macro_snapshot"}:
        return "macro_events"
    if normalized.startswith("fomc") or normalized == "fed_rates":
        return "fed_rates"
    if normalized in {"breaking_news", "news"}:
        return "news"
    return normalized
