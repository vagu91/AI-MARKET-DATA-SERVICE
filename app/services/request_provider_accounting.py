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
        if type(lookup_performed) is not bool:
            return False
        if (
            getattr(policy, "canonical_repository_required", False)
            and lookup_performed is not True
        ):
            return False
        if lookup_performed:
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
) -> bool:
    if row.get("database_lookup_performed") is not True:
        return not getattr(
            policy,
            "canonical_repository_required",
            False,
        )
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
            attempt.get("called") is False
            and attempt.get("execution_origin") == "CACHE_DECISION"
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
                return False
            continue
        elif not prior_called_and_failed or not called:
            return False

        succeeded = _attempt_succeeded(attempt)
        prior_succeeded = succeeded
        prior_called_and_failed = called and not succeeded
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
