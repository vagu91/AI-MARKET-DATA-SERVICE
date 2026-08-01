from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, Callable, Iterable

from app.services.data_freshness_service import (
    CanonicalFreshnessPolicy,
    evaluate_canonical_freshness,
    parse_datetime,
)


EVIDENCE_ORIGIN = "NORMAL_APPLICATION_REQUEST"
ACQUISITION_COMPLETE = "ACQUISITION_COMPLETE"
EVIDENCE_INCOMPLETE = "INCOMPLETE"
ATTEMPT_EXECUTION_ORIGINS = {
    "PROVIDER_CALL",
    "OBSERVED_SKIP",
    "CACHE_DECISION",
}
OBSERVED_LIFECYCLE_SKIP_REASON_CODES = frozenset(
    {
        "NO_DATA_STILL_FRESH",
        "OCCURRENCE_NOT_PUBLISHED",
        "TERMINAL_NO_DATA",
    }
)


class RequestProviderAccountingCollector:
    """Collect acquisition evidence for one market-context request."""

    def __init__(
        self,
        *,
        request_id: str,
        correlation_id: str,
        request_started_at: datetime | str,
        policies: Iterable[Any],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not request_id or correlation_id != request_id:
            raise ValueError("request_accounting_correlation_mismatch")
        started = parse_datetime(request_started_at)
        if started is None:
            raise ValueError("request_accounting_started_at_invalid")
        self.request_id = request_id
        self.correlation_id = correlation_id
        self.request_started_at = started
        self.clock = clock or (lambda: datetime.now(UTC))
        self.policies = {
            str(policy.dataset_id): policy
            for policy in policies
        }
        self._rows: dict[str, dict[str, Any]] = {}
        self._duplicates: set[str] = set()

    def record(
        self,
        dataset_id: str,
        *,
        acquisition_id: str,
        shared_dataset_ids: Iterable[str],
        database_lookup_performed: bool,
        database_lookup_reason: str,
        database_record_found: bool | None,
        database_data_as_of: Any,
        database_content_valid_until: Any,
        database_record_expired: bool | None,
        database_freshness_evaluation: str,
        primary_provider: dict[str, Any],
        fallbacks: Iterable[dict[str, Any]],
        acquisition_selected_source: Any,
        acquisition_reason_code: str,
        database_refresh_due_at: Any = None,
        database_lifecycle_status: Any = None,
        database_lookup_summary: dict[str, Any] | None = None,
        capability_acquisitions: Iterable[
            dict[str, Any]
        ] | None = None,
        observed_at: datetime | str | None = None,
        evidence_complete: bool = True,
    ) -> None:
        policy = self.policies.get(dataset_id)
        if policy is None:
            raise KeyError(dataset_id)
        if dataset_id in self._rows:
            self._duplicates.add(dataset_id)
            return
        observed = parse_datetime(observed_at or self.clock())
        row = {
            "dataset_id": dataset_id,
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "evidence_origin": EVIDENCE_ORIGIN,
            "evidence_status": ACQUISITION_COMPLETE,
            "observed_at": observed.isoformat() if observed else None,
            "acquisition_id": acquisition_id,
            "shared_acquisition_dataset_ids": sorted(
                {
                    str(item)
                    for item in shared_dataset_ids
                    if str(item) in self.policies
                }
            ),
            "database_lookup_performed": database_lookup_performed,
            "database_lookup_reason": database_lookup_reason,
            "database_record_found": database_record_found,
            "database_data_as_of": database_data_as_of,
            "database_content_valid_until": database_content_valid_until,
            "database_refresh_due_at": database_refresh_due_at,
            "database_lifecycle_status": database_lifecycle_status,
            "database_record_expired": database_record_expired,
            "database_freshness_evaluation": (
                database_freshness_evaluation
            ),
            "database_lookup_summary": (
                deepcopy(database_lookup_summary)
                if isinstance(database_lookup_summary, dict)
                else None
            ),
            "capability_acquisitions": [
                deepcopy(item)
                for item in (capability_acquisitions or [])
            ],
            "primary_provider": deepcopy(primary_provider),
            "fallbacks": [
                deepcopy(item)
                for item in fallbacks
            ],
            "acquisition_selected_source": deepcopy(
                acquisition_selected_source
            ),
            "acquisition_reason_code": acquisition_reason_code,
        }
        if (
            not evidence_complete
            or not self._row_valid(row, policy=policy)
        ):
            row["evidence_status"] = EVIDENCE_INCOMPLETE
        self._rows[dataset_id] = row

    def manifest(
        self,
        *,
        request_completed_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        completed = parse_datetime(request_completed_at or self.clock())
        rows = [
            deepcopy(
                self._rows.get(dataset_id)
                or self._missing_row(dataset_id)
            )
            for dataset_id in self.policies
        ]
        complete = bool(
            completed
            and completed >= self.request_started_at
            and not self._duplicates
            and len(self._rows) == len(self.policies)
            and all(
                self._row_valid(
                    row,
                    policy=self.policies[row["dataset_id"]],
                    completed_at=completed,
                )
                for row in rows
            )
            and _shared_acquisition_links_valid(
                rows,
                governed_dataset_ids=set(self.policies),
            )
        )
        return {
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "request_started_at": self.request_started_at.isoformat(),
            "request_completed_at": (
                completed.isoformat()
                if completed
                else None
            ),
            "evidence_origin": EVIDENCE_ORIGIN,
            "evidence_status": (
                ACQUISITION_COMPLETE
                if complete
                else EVIDENCE_INCOMPLETE
            ),
            "reason_code": (
                "REQUEST_ACQUISITION_EVIDENCE_COMPLETE"
                if complete
                else "REQUEST_ACQUISITION_EVIDENCE_INCOMPLETE"
            ),
            "datasets": rows,
        }

    def _row_valid(
        self,
        row: dict[str, Any],
        *,
        policy: Any,
        completed_at: datetime | None = None,
    ) -> bool:
        observed = parse_datetime(row.get("observed_at"))
        if (
            row.get("request_id") != self.request_id
            or row.get("correlation_id") != self.correlation_id
            or row.get("evidence_origin") != EVIDENCE_ORIGIN
            or row.get("evidence_status") != ACQUISITION_COMPLETE
            or not row.get("acquisition_id")
            or not observed
            or observed < self.request_started_at
            or (completed_at and observed > completed_at)
            or row.get("dataset_id")
            not in row.get("shared_acquisition_dataset_ids", [])
            or not row.get("database_lookup_reason")
            or not row.get("database_freshness_evaluation")
            or not row.get("acquisition_reason_code")
        ):
            return False
        lookup_performed = row.get("database_lookup_performed")
        found = row.get("database_record_found")
        expired = row.get("database_record_expired")
        capability_scoped = (
            str(getattr(policy, "provider_strategy", "")).upper()
            == "FAN_IN"
            and bool(row.get("capability_acquisitions"))
        )
        if type(lookup_performed) is not bool:
            return False
        if (
            getattr(policy, "canonical_repository_required", False)
            and lookup_performed is not True
        ):
            return False
        if capability_scoped:
            if (
                lookup_performed is not True
                or found is not None
                or expired is not None
                or row.get("database_data_as_of") is not None
                or row.get("database_content_valid_until") is not None
                or row.get("database_refresh_due_at") is not None
                or row.get("database_lifecycle_status") is not None
                or row.get("database_freshness_evaluation")
                != "CAPABILITY_SCOPED"
            ):
                return False
        elif lookup_performed:
            if type(found) is not bool or type(expired) is not bool:
                return False
            if found and (
                not row.get("database_data_as_of")
                or not row.get("database_content_valid_until")
                or not row.get("database_refresh_due_at")
            ):
                return False
            if not _canonical_database_evidence_valid(
                row,
                policy=policy,
                observed_at=observed,
                request_started_at=self.request_started_at,
            ):
                return False
        elif (
            found is not None
            or expired is not None
            or row.get("database_data_as_of") is not None
            or row.get("database_content_valid_until") is not None
            or row.get("database_refresh_due_at") is not None
            or row.get("database_lifecycle_status") is not None
            or row.get("database_freshness_evaluation")
            != "NOT_LOOKED_UP"
        ):
            return False
        primary = row.get("primary_provider")
        fallbacks = row.get("fallbacks")
        if (
            not isinstance(primary, dict)
            or not isinstance(fallbacks, list)
            or primary.get("provider") != policy.primary_provider
            or [
                item.get("provider")
                for item in fallbacks
                if isinstance(item, dict)
            ]
            != list(policy.fallback_providers)
        ):
            return False
        return bool(
            all(
                _attempt_valid(item)
                for item in [primary, *fallbacks]
            )
            and _provider_flow_valid(
                row,
                policy=policy,
                governed_dataset_ids=set(self.policies),
            )
        )

    def _missing_row(self, dataset_id: str) -> dict[str, Any]:
        policy = self.policies[dataset_id]
        return {
            "dataset_id": dataset_id,
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "evidence_origin": EVIDENCE_ORIGIN,
            "evidence_status": EVIDENCE_INCOMPLETE,
            "observed_at": None,
            "acquisition_id": None,
            "shared_acquisition_dataset_ids": [],
            "database_lookup_performed": None,
            "database_lookup_reason": (
                "ACQUISITION_OBSERVATION_NOT_EMITTED"
            ),
            "database_record_found": None,
            "database_data_as_of": None,
            "database_content_valid_until": None,
            "database_refresh_due_at": None,
            "database_lifecycle_status": None,
            "database_record_expired": None,
            "database_freshness_evaluation": None,
            "database_lookup_summary": None,
            "capability_acquisitions": [],
            "primary_provider": {
                "provider": policy.primary_provider,
                "called": None,
                "attempts": None,
                "result": "EVIDENCE_NOT_AVAILABLE",
                "not_called_reason": None,
                "execution_origin": None,
            },
            "fallbacks": [
                {
                    "provider": provider,
                    "called": None,
                    "attempts": None,
                    "result": "EVIDENCE_NOT_AVAILABLE",
                    "not_called_reason": None,
                    "execution_origin": None,
                }
                for provider in policy.fallback_providers
            ],
            "acquisition_selected_source": None,
            "acquisition_reason_code": (
                "REQUEST_SCOPED_EVIDENCE_NOT_AVAILABLE"
            ),
        }


def provider_attempt(
    provider: str,
    *,
    called: bool,
    attempts: int,
    result: str,
    not_called_reason: str | None = None,
    execution_origin: str,
) -> dict[str, Any]:
    return {
        "provider": provider,
        "called": called,
        "attempts": attempts,
        "result": _redacted_result(result),
        "not_called_reason": not_called_reason,
        "execution_origin": execution_origin,
    }


def _attempt_valid(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    called = value.get("called")
    attempts = value.get("attempts")
    origin = value.get("execution_origin")
    if (
        not value.get("provider")
        or type(called) is not bool
        or not isinstance(attempts, int)
        or isinstance(attempts, bool)
        or attempts < 0
        or not value.get("result")
        or origin not in ATTEMPT_EXECUTION_ORIGINS
    ):
        return False
    if called:
        return bool(
            attempts >= 1
            and origin == "PROVIDER_CALL"
            and not value.get("not_called_reason")
        )
    return bool(
        attempts == 0
        and value.get("not_called_reason")
        and origin in {"OBSERVED_SKIP", "CACHE_DECISION"}
    )


def _canonical_database_evidence_valid(
    row: dict[str, Any],
    *,
    policy: Any,
    observed_at: datetime,
    request_started_at: datetime | None = None,
) -> bool:
    if row.get("database_lookup_performed") is not True:
        return not getattr(
            policy,
            "canonical_repository_required",
            False,
        )
    if (
        str(getattr(policy, "dataset_id", ""))
        == "macro_calendar"
        and (
            request_started_at is None
            or not _calendar_database_lookup_summary_valid(
                row.get("database_lookup_summary"),
                request_id=str(row.get("request_id") or ""),
                correlation_id=str(
                    row.get("correlation_id") or ""
                ),
                request_started_at=request_started_at,
                request_observed_at=observed_at,
            )
        )
    ):
        return False
    found = row.get("database_record_found")
    expired = row.get("database_record_expired")
    evaluation = str(
        row.get("database_freshness_evaluation") or ""
    )
    if found is False:
        return bool(
            expired is False
            and evaluation == "NOT_FOUND"
            and row.get("database_data_as_of") is None
            and row.get("database_content_valid_until") is None
            and row.get("database_refresh_due_at") is None
            and row.get("database_lifecycle_status") is None
        )
    if found is not True:
        return False
    result = evaluate_canonical_freshness(
        row,
        policy=CanonicalFreshnessPolicy(
            max_age=policy.max_age,
            data_reference_mode=_data_reference_mode(policy),
        ),
        observed_at=observed_at,
    )
    return bool(
        result.complete
        and result.expired is expired
        and result.evaluation == evaluation
        and result.usable is (not expired)
    )


def _calendar_database_lookup_summary_valid(
    value: Any,
    *,
    request_id: str,
    correlation_id: str,
    request_started_at: datetime,
    request_observed_at: datetime,
) -> bool:
    if not isinstance(value, dict):
        return False
    integer_fields = (
        "providers_expected",
        "providers_evaluated",
        "observations_expected",
        "observations_evaluated",
        "observations_missing",
        "records_terminal",
        "records_missing_or_incomplete",
        "records_expired",
    )
    if any(
        type(value.get(field)) is not int
        or value[field] < 0
        for field in integer_fields
    ):
        return False
    providers_expected = value["providers_expected"]
    providers = value["providers_evaluated"]
    expected = value["observations_expected"]
    evaluated = value["observations_evaluated"]
    missing = value["observations_missing"]
    terminal = value["records_terminal"]
    incomplete = value["records_missing_or_incomplete"]
    expired = value["records_expired"]
    window_start = parse_datetime(value.get("window_start"))
    window_end = parse_datetime(value.get("window_end"))
    provider_ids_expected = value.get(
        "provider_ids_expected"
    )
    provider_ids_evaluated = value.get(
        "provider_ids_evaluated"
    )
    provider_names_expected = value.get(
        "provider_names_expected"
    )
    provider_names_evaluated = value.get(
        "provider_names_evaluated"
    )
    unexpected_provider_names = value.get(
        "unexpected_provider_names"
    )
    refresh_required_ids = value.get(
        "provider_refresh_required_ids"
    )
    refresh_required_names = value.get(
        "provider_refresh_required_names"
    )
    provider_attempts = value.get("provider_attempts")
    from app.services.provider_capability_registry import (
        authoritative_calendar_adapter_identities,
    )

    canonical_identities = {
        str(identity["provider_id"]): identity
        for identity in (
            authoritative_calendar_adapter_identities().values()
        )
    }
    registered_provider_ids = sorted(canonical_identities)
    registered_provider_names = sorted(
        str(identity["source"])
        for identity in canonical_identities.values()
    )
    if (
        not isinstance(provider_ids_expected, list)
        or not isinstance(provider_ids_evaluated, list)
        or provider_ids_expected != registered_provider_ids
        or provider_ids_evaluated != registered_provider_ids
        or not isinstance(provider_names_expected, list)
        or not isinstance(provider_names_evaluated, list)
        or not all(
            isinstance(item, str) and item
            for item in provider_names_expected
        )
        or not all(
            isinstance(item, str) and item
            for item in provider_names_evaluated
        )
        or provider_names_expected != registered_provider_names
        or provider_names_evaluated != provider_names_expected
        or len(provider_names_expected)
        != len(registered_provider_ids)
        or unexpected_provider_names != []
        or not isinstance(refresh_required_ids, list)
        or not all(
            isinstance(item, str) and item
            for item in refresh_required_ids
        )
        or refresh_required_ids != sorted(
            set(refresh_required_ids)
        )
        or not set(refresh_required_ids).issubset(
            registered_provider_ids
        )
        or not isinstance(refresh_required_names, list)
        or not all(
            isinstance(item, str) and item
            for item in refresh_required_names
        )
        or refresh_required_names != sorted(
            set(refresh_required_names)
        )
        or not set(refresh_required_names).issubset(
            provider_names_expected
        )
        or len(refresh_required_ids)
        != len(refresh_required_names)
        or {
            str(canonical_identities[provider_id]["source"])
            for provider_id in refresh_required_ids
        }
        != set(refresh_required_names)
        or not isinstance(provider_attempts, list)
        or len(provider_attempts) != len(
            registered_provider_ids
        )
    ):
        return False
    attempts_by_id: dict[str, dict[str, Any]] = {}
    for attempt in provider_attempts:
        if not isinstance(attempt, dict):
            return False
        provider_id = str(
            attempt.get("provider_id") or ""
        )
        provider_name = str(
            attempt.get("provider_name") or ""
        )
        called = attempt.get("called")
        attempts = attempt.get("attempts")
        successes = attempt.get("successful_attempts")
        failures = attempt.get("failed_attempts")
        result = str(attempt.get("result") or "")
        attempt_started_at = parse_datetime(
            attempt.get("started_at")
        )
        attempt_observed_at = parse_datetime(
            attempt.get("observed_at")
        )
        if (
            provider_id in attempts_by_id
            or provider_id not in registered_provider_ids
            or provider_name
            != str(canonical_identities[provider_id]["source"])
            or attempt.get("query_scope") != "country=US"
            or attempt.get("request_id") != request_id
            or attempt.get("correlation_id")
            != correlation_id
            or request_id != correlation_id
            or attempt_started_at is None
            or attempt_observed_at is None
            or attempt_started_at < request_started_at
            or attempt_observed_at < attempt_started_at
            or attempt_observed_at > request_observed_at
            or type(called) is not bool
            or type(attempts) is not int
            or type(successes) is not int
            or type(failures) is not int
            or min(attempts, successes, failures) < 0
            or successes + failures != attempts
            or called is (attempts == 0)
            or not result
            or (
                called
                and provider_id not in refresh_required_ids
            )
            or (
                not called
                and provider_id in refresh_required_ids
            )
            or (
                called
                and (
                    (
                        result == "SUCCESS"
                        and not (
                            successes == attempts
                            and failures == 0
                        )
                    )
                    or (
                        result == "FAILED"
                        and not (
                            failures == attempts
                            and successes == 0
                        )
                    )
                    or (
                        result == "PARTIAL"
                        and not (
                            successes > 0
                            and failures > 0
                        )
                    )
                    or result
                    not in {"SUCCESS", "FAILED", "PARTIAL"}
                )
            )
            or (
                not called
                and (
                    result != "NOT_CALLED"
                    or attempt.get("not_called_reason")
                    != "VALID_DATABASE_COVERAGE"
                )
            )
            or (called and attempt.get("not_called_reason"))
        ):
            return False
        attempts_by_id[provider_id] = attempt
    if {
        str(attempt.get("provider_name"))
        for attempt in attempts_by_id.values()
    } != set(provider_names_expected):
        return False
    inclusive_days = (
        (window_end.date() - window_start.date()).days
        + 1
        if window_start is not None
        and window_end is not None
        else 0
    )
    return bool(
        providers_expected == len(registered_provider_ids)
        and providers == providers_expected
        and expected > 0
        and expected == providers_expected * inclusive_days
        and evaluated == expected
        and missing == 0
        and terminal + incomplete == evaluated
        and expired <= terminal
        and window_start is not None
        and window_end is not None
        and window_start <= window_end
    )


def _data_reference_mode(policy: Any) -> str:
    dataset_id = str(getattr(policy, "dataset_id", ""))
    if dataset_id == "macro_calendar":
        return "event_occurrence"
    if dataset_id in {
        "nasdaq_100",
        "mega_cap_quotes",
        "market_internals",
        "vix",
        "vvix",
        "risk",
        "fomc_expectations",
        "earnings",
        "options_positioning",
        "market_schedule",
    }:
        return "point_in_time"
    return "official_release"


def _provider_flow_valid(
    row: dict[str, Any],
    *,
    policy: Any,
    governed_dataset_ids: set[str] | None = None,
) -> bool:
    attempts = [
        row.get("primary_provider"),
        *(row.get("fallbacks") or []),
    ]
    database_valid = bool(
        row.get("database_record_found")
        and not row.get("database_record_expired")
        and row.get("database_freshness_evaluation") == "VALID"
    )
    if database_valid:
        return all(
            (
                attempt.get("called") is False
                and attempt.get("execution_origin") == "CACHE_DECISION"
            )
            or _shared_post_selection_call_valid(
                row,
                attempt,
                policy=policy,
                governed_dataset_ids=governed_dataset_ids,
            )
            for attempt in attempts
        )
    if (
        str(getattr(policy, "dataset_id", ""))
        == "flash_services_pmi"
        and _observed_lifecycle_skip_flow_valid(attempts)
    ):
        return True

    strategy = str(
        getattr(policy, "provider_strategy", "FALLBACK")
    ).upper()
    if strategy == "FAN_IN":
        if str(getattr(policy, "dataset_id", "")) == (
            "market_schedule"
        ):
            return _fan_in_capability_flow_valid(
                row,
                policy=policy,
            )
        return all(
            attempt.get("called") is True
            or (
                attempt.get("called") is False
                and attempt.get("execution_origin")
                in {"OBSERVED_SKIP", "CACHE_DECISION"}
            )
            for attempt in attempts
        )
    if strategy == "CASCADE":
        called_seen = False
        for attempt in attempts:
            if attempt.get("called") is True:
                called_seen = True
                continue
            reason = str(
                attempt.get("not_called_reason") or ""
            ).upper()
            if not called_seen and not any(
                token in reason
                for token in (
                    "NOT_CONFIGURED",
                    "NEGATIVE_CACHE",
                )
            ):
                return False
            if called_seen and not any(
                token in reason
                for token in (
                    "PRIOR_PROVIDER_SUCCEEDED",
                    "NOT_REQUIRED",
                    "NOT_CONFIGURED",
                    "NEGATIVE_CACHE",
                )
            ):
                return False
        return called_seen or all(
            "NOT_CONFIGURED"
            in str(
                attempt.get("not_called_reason") or ""
            ).upper()
            for attempt in attempts
        )

    prior_succeeded = False
    prior_called_and_failed = False
    for index, attempt in enumerate(attempts):
        called = attempt.get("called") is True
        if index == 0:
            if not called:
                reason = str(
                    attempt.get("not_called_reason") or ""
                ).upper()
                if (
                    not policy.fallback_providers
                    or "NOT_CONFIGURED" not in reason
                ):
                    return False
                prior_called_and_failed = True
                continue
        elif prior_succeeded:
            if called:
                if not _shared_post_selection_call_valid(
                    row,
                    attempt,
                    policy=policy,
                    governed_dataset_ids=governed_dataset_ids,
                ):
                    return False
            continue
        elif not prior_called_and_failed or not called:
            return False

        succeeded = _attempt_succeeded(attempt)
        prior_succeeded = succeeded
        prior_called_and_failed = called and not succeeded
    return True


def _fan_in_capability_flow_valid(
    row: dict[str, Any],
    *,
    policy: Any,
) -> bool:
    from app.services.provider_capability_registry import (
        provider_by_id,
    )

    provider_order = (
        policy.primary_provider,
        *policy.fallback_providers,
    )
    expected_groups: dict[str, list[str]] = {}
    for provider_id in provider_order:
        try:
            capabilities = tuple(
                capability
                for capability in provider_by_id(
                    provider_id
                ).capabilities
                if capability.dataset_id == policy.dataset_id
            )
        except KeyError:
            return False
        if len(capabilities) != 1:
            return False
        metric_id = str(capabilities[0].metric_id or "").strip()
        if not metric_id:
            return False
        expected_groups.setdefault(metric_id, []).append(
            provider_id
        )

    acquisitions = row.get("capability_acquisitions")
    if (
        not isinstance(acquisitions, list)
        or len(acquisitions) != len(expected_groups)
    ):
        return False
    observed_at = parse_datetime(row.get("observed_at"))
    if observed_at is None:
        return False
    attempts = {
        str(attempt.get("provider")): attempt
        for attempt in [
            row.get("primary_provider"),
            *(row.get("fallbacks") or []),
        ]
        if isinstance(attempt, dict)
        and attempt.get("provider")
    }
    if set(attempts) != set(provider_order):
        return False

    observed_metrics: set[str] = set()
    selected_providers: list[str] = []
    for acquisition in acquisitions:
        if not isinstance(acquisition, dict):
            return False
        metric_id = str(
            acquisition.get("capability_metric_id") or ""
        ).strip()
        provider_ids = acquisition.get("provider_ids")
        lookups = acquisition.get("database_lookups")
        if (
            metric_id in observed_metrics
            or metric_id not in expected_groups
            or provider_ids != expected_groups[metric_id]
            or not isinstance(lookups, list)
            or len(lookups) != len(provider_ids)
        ):
            return False
        observed_metrics.add(metric_id)
        selected = acquisition.get("selected_provider")
        if selected is not None and selected not in provider_ids:
            return False
        if selected is not None:
            selected_providers.append(str(selected))

        selection_reached = False
        prior_failed_or_skipped = False
        for index, (provider_id, lookup) in enumerate(
            zip(provider_ids, lookups, strict=True)
        ):
            attempt = attempts[provider_id]
            if (
                not isinstance(lookup, dict)
                or lookup.get("provider") != provider_id
                or not _capability_database_lookup_valid(
                    lookup,
                    policy=policy,
                    observed_at=observed_at,
                )
            ):
                return False
            if selection_reached:
                if (
                    lookup.get("performed") is not False
                    or attempt.get("called") is not False
                    or attempt.get("execution_origin")
                    != "OBSERVED_SKIP"
                    or "PRIOR_" not in str(
                        attempt.get("not_called_reason") or ""
                    ).upper()
                ):
                    return False
                continue

            if lookup.get("performed") is not True:
                return False
            database_valid = bool(
                lookup.get("found") is True
                and lookup.get("expired") is False
                and str(
                    lookup.get("freshness") or ""
                ).upper()
                == "VALID"
            )
            if database_valid:
                if (
                    attempt.get("called") is not False
                    or attempt.get("execution_origin")
                    != "CACHE_DECISION"
                    or selected != provider_id
                ):
                    return False
                selection_reached = True
                continue

            called = attempt.get("called") is True
            if called:
                if (
                    index > 0
                    and not prior_failed_or_skipped
                ):
                    return False
                if _attempt_succeeded(attempt):
                    if selected != provider_id:
                        return False
                    selection_reached = True
                else:
                    prior_failed_or_skipped = True
                continue

            reason = str(
                attempt.get("not_called_reason") or ""
            ).upper()
            if (
                attempt.get("execution_origin") != "OBSERVED_SKIP"
                or not any(
                    token in reason
                    for token in (
                        "DISABLED",
                        "NOT_CONFIGURED",
                        "PREREQUISITE",
                    )
                )
            ):
                return False
            prior_failed_or_skipped = True

        if selection_reached != (selected is not None):
            return False

    if observed_metrics != set(expected_groups):
        return False
    selected_summary = row.get("acquisition_selected_source")
    return bool(
        (selected_providers and selected_summary == "MIXED")
        or (not selected_providers and selected_summary is None)
    )


def _capability_database_lookup_valid(
    lookup: dict[str, Any],
    *,
    policy: Any,
    observed_at: datetime,
) -> bool:
    performed = lookup.get("performed")
    reason_code = str(lookup.get("reason_code") or "").strip()
    if type(performed) is not bool:
        return False
    if not performed:
        return bool(
            reason_code
            and lookup.get("found") is None
            and lookup.get("expired") is None
            and lookup.get("data_as_of") is None
            and lookup.get("content_valid_until") is None
            and lookup.get("refresh_due_at") is None
            and lookup.get("lifecycle_status") is None
            and lookup.get("freshness") == "NOT_LOOKED_UP"
        )
    evidence = {
        "database_lookup_performed": True,
        "database_record_found": lookup.get("found"),
        "database_data_as_of": lookup.get("data_as_of"),
        "database_content_valid_until": lookup.get(
            "content_valid_until"
        ),
        "database_refresh_due_at": lookup.get(
            "refresh_due_at"
        ),
        "database_lifecycle_status": lookup.get(
            "lifecycle_status"
        ),
        "database_record_expired": lookup.get("expired"),
        "database_freshness_evaluation": lookup.get(
            "freshness"
        ),
    }
    if not _canonical_database_evidence_valid(
        evidence,
        policy=policy,
        observed_at=observed_at,
    ):
        return False
    if lookup.get("found") is False:
        return reason_code == "CANONICAL_RECORD_NOT_FOUND"
    result = evaluate_canonical_freshness(
        evidence,
        policy=CanonicalFreshnessPolicy(
            max_age=policy.max_age,
            data_reference_mode=_data_reference_mode(policy),
        ),
        observed_at=observed_at,
    )
    return result.reason_code == reason_code


def _shared_post_selection_call_valid(
    row: dict[str, Any],
    attempt: dict[str, Any],
    *,
    policy: Any,
    governed_dataset_ids: set[str] | None,
) -> bool:
    governed = {
        str(dataset_id)
        for dataset_id in (governed_dataset_ids or set())
        if str(dataset_id)
    }
    shared = {
        str(dataset_id)
        for dataset_id in (
            row.get("shared_acquisition_dataset_ids") or []
        )
        if str(dataset_id) in governed
    }
    dataset_id = str(getattr(policy, "dataset_id", ""))
    selected = str(
        row.get("acquisition_selected_source") or ""
    ).strip().upper()
    delivery_selected = str(
        row.get("selected_source") or ""
    ).strip().upper()
    primary = str(getattr(policy, "primary_provider", "")).upper()
    return bool(
        row.get("acquisition_id")
        and dataset_id in shared
        and len(shared) >= 2
        and attempt.get("called") is True
        and isinstance(attempt.get("attempts"), int)
        and not isinstance(attempt.get("attempts"), bool)
        and int(attempt.get("attempts") or 0) > 0
        and attempt.get("execution_origin") == "PROVIDER_CALL"
        and str(attempt.get("provider") or "").upper() != primary
        and selected == primary
        and (not delivery_selected or delivery_selected == primary)
    )


def _shared_acquisition_links_valid(
    rows: Iterable[dict[str, Any]],
    *,
    governed_dataset_ids: set[str],
) -> bool:
    governed = {str(dataset_id) for dataset_id in governed_dataset_ids}
    by_dataset = {
        str(row.get("dataset_id")): row
        for row in rows
        if isinstance(row, dict) and row.get("dataset_id")
    }
    for dataset_id, row in by_dataset.items():
        shared = {
            str(item)
            for item in (
                row.get("shared_acquisition_dataset_ids") or []
            )
            if str(item) in governed
        }
        if dataset_id not in shared:
            return False
        if len(shared) < 2:
            continue
        acquisition_id = row.get("acquisition_id")
        for linked_dataset_id in shared:
            linked = by_dataset.get(linked_dataset_id)
            if (
                linked is None
                or linked.get("acquisition_id") != acquisition_id
                or {
                    str(item)
                    for item in (
                        linked.get("shared_acquisition_dataset_ids")
                        or []
                    )
                    if str(item) in governed
                }
                != shared
            ):
                return False
    return True


def _observed_lifecycle_skip_flow_valid(
    attempts: Iterable[Any],
) -> bool:
    observed = list(attempts)
    if not observed:
        return False
    reasons = {
        str(attempt.get("not_called_reason") or "").strip().upper()
        for attempt in observed
        if isinstance(attempt, dict)
    }
    return bool(
        len(reasons) == 1
        and reasons <= OBSERVED_LIFECYCLE_SKIP_REASON_CODES
        and all(
            isinstance(attempt, dict)
            and attempt.get("called") is False
            and attempt.get("attempts") == 0
            and attempt.get("execution_origin") == "OBSERVED_SKIP"
            for attempt in observed
        )
    )


def _attempt_succeeded(attempt: dict[str, Any]) -> bool:
    if attempt.get("called") is not True:
        return False
    result = str(attempt.get("result") or "").upper()
    if result.startswith("SCHEDULE_CATCH_UP_"):
        return result == "SCHEDULE_CATCH_UP_COMPLETED"
    return bool(
        any(
            token in result
            for token in (
                "SUCCESS",
                "FOUND",
                "AVAILABLE",
                "VALID",
                "PARTIAL",
            )
        )
        and not any(
            token in result
            for token in (
                "FAIL",
                "ERROR",
                "NO_DATA",
                "NOT_AVAILABLE",
                "UNAVAILABLE",
                "NOT_FOUND",
                "TIMEOUT",
            )
        )
    )


def _redacted_result(value: Any) -> str:
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
